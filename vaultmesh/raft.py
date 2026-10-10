"""A single member of a Raft consensus group (Ongaro & Ousterhout, "In Search of an
Understandable Consensus Algorithm", 2014).

Implemented:
  * Leader election with randomised timeouts and the "up-to-date log" voting rule.
  * Log replication with the consistency check, conflict truncation and fast back-off.
  * Commitment by majority, only for entries of the leader's own term (a no-op is appended
    on election so earlier entries commit safely).
  * Group commit: concurrent client writes are batched into one disk fsync.
  * Linearizable reads with the ReadIndex protocol (no log write needed for a read).
  * At most one in-flight AppendEntries per follower, with a "dirty" flag so nothing is lost.

Concurrency model: the node runs on ONE asyncio event loop, so no locks are needed. Every
state change happens in plain (non-async) methods that never ``await`` in the middle; network
calls happen in tasks, and their replies are checked against the current term so stale
replies are ignored.
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
from dataclasses import dataclass
from enum import Enum
from typing import Any, Coroutine

from .messages import AppendRequest, AppendResponse, VoteRequest, VoteResponse
from .state import PersistentState
from .transport import Transport
from .wal import LogEntry, WriteAheadLog

log = logging.getLogger("raft")


class Role(str, Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class NotLeaderError(Exception):
    """Raised when a client request reaches a node that is not the current leader."""

    def __init__(self, leader_id: str | None) -> None:
        super().__init__(f"not the leader; leader is {leader_id}" if leader_id else "no leader known yet")
        self.leader_id = leader_id


@dataclass
class RaftConfig:
    election_timeout_min: float = 0.30   # seconds; random timeout in [min, max] avoids split votes
    election_timeout_max: float = 0.60
    heartbeat_interval: float = 0.075    # must be well below the minimum election timeout
    max_batch_entries: int = 512         # entries per AppendEntries RPC
    fsync: bool = True                   # disable only in tests

    @staticmethod
    def fast_for_tests() -> "RaftConfig":
        return RaftConfig(0.15, 0.30, 0.04, 256, False)


@dataclass
class _ReadWaiter:
    requested_at: float
    term: int
    future: asyncio.Future
    read_index: int = -1


@dataclass
class _Peer:
    next_index: int = 1
    match_index: int = 0
    last_acked_send_time: float = 0.0   # newest send time a reply in our term was received for
    in_flight: bool = False
    dirty: bool = False                 # something new to send once the in-flight RPC returns


class RaftNode:
    def __init__(
        self,
        node_id: str,
        peers: list[str],
        transport: Transport,
        data_dir: str,
        state_machine: Any,
        config: RaftConfig | None = None,
    ) -> None:
        self.id = node_id
        self.peer_ids = list(peers)
        self.transport = transport
        self.state_machine = state_machine
        self.config = config or RaftConfig()
        self.majority = (len(peers) + 1) // 2 + 1

        self.log = WriteAheadLog(os.path.join(data_dir, "raft.wal"), self.config.fsync)
        self.meta = PersistentState(os.path.join(data_dir, "raft.meta"), self.config.fsync)
        if self.log.recovered_from_torn_write:
            log.warning("%s: truncated a torn write at the end of the WAL", self.id)

        # volatile state (rebuilt after a restart)
        self.role = Role.FOLLOWER
        self.leader_id: str | None = None
        self.commit_index = 0
        self.last_applied = 0

        # leader state
        self._peers: dict[str, _Peer] = {p: _Peer() for p in peers}
        self._pending_writes: dict[int, asyncio.Future] = {}
        self._pending_reads: list[_ReadWaiter] = []
        self._write_queue: list[tuple[bytes, asyncio.Future]] = []
        self._flush_scheduled = False

        self._loop: asyncio.AbstractEventLoop | None = None
        self._election_timer: asyncio.TimerHandle | None = None
        self._heartbeat_task: asyncio.Task | None = None
        self._tasks: set[asyncio.Task] = set()
        self._stopped = False

    # ================================================================== lifecycle
    async def start(self) -> None:
        self._loop = asyncio.get_running_loop()
        log.info("%s: starting at term=%d with %d log entries", self.id, self.meta.term, self.log.last_index())
        self._reset_election_timer()

    async def stop(self) -> None:
        self._stopped = True
        if self._election_timer:
            self._election_timer.cancel()
        self._fail_all_pending()
        tasks = list(self._tasks)
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.log.close()

    def _spawn(self, coro: Coroutine) -> asyncio.Task:
        task = self._loop.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    # ================================================================== election
    def _reset_election_timer(self) -> None:
        if self._stopped:
            return
        if self._election_timer:
            self._election_timer.cancel()
        timeout = random.uniform(self.config.election_timeout_min, self.config.election_timeout_max)
        self._election_timer = self._loop.call_later(timeout, self._start_election)

    def _start_election(self) -> None:
        if self._stopped or self.role == Role.LEADER:
            return
        term = self.meta.term + 1
        self.meta.update(term, self.id)          # vote for ourselves, persisted
        self.role = Role.CANDIDATE
        self.leader_id = None
        log.info("%s: election timeout, starting election for term %d", self.id, term)
        self._reset_election_timer()             # if this election splits, try again later
        votes = {self.id}
        if len(votes) >= self.majority:
            self._become_leader()
            return
        req = VoteRequest(term, self.id, self.log.last_index(), self.log.last_term())
        for peer in self.peer_ids:
            self._spawn(self._request_vote(peer, req, votes))

    async def _request_vote(self, peer: str, req: VoteRequest, votes: set[str]) -> None:
        try:
            resp = await self.transport.request_vote(peer, req)
        except Exception:
            return  # peer down or unreachable: it simply does not vote
        if resp.term > self.meta.term:
            self._become_follower(resp.term)
            return
        if self.role != Role.CANDIDATE or self.meta.term != req.term or not resp.granted:
            return
        votes.add(peer)
        if len(votes) >= self.majority:
            self._become_leader()

    def handle_request_vote(self, req: VoteRequest) -> VoteResponse:
        if self._stopped:
            return VoteResponse(self.meta.term, False)
        if req.term > self.meta.term:
            self._become_follower(req.term)
        # Election restriction (paper 5.4.1): only vote for a candidate whose log is at least as
        # up-to-date as ours, so a new leader always holds every committed entry.
        log_ok = (req.last_log_term > self.log.last_term()) or (
            req.last_log_term == self.log.last_term() and req.last_log_index >= self.log.last_index()
        )
        free_to_vote = self.meta.voted_for in (None, req.candidate_id)
        grant = req.term == self.meta.term and free_to_vote and log_ok
        if grant:
            self.meta.update(self.meta.term, req.candidate_id)  # persisted BEFORE replying
            self._reset_election_timer()
        return VoteResponse(self.meta.term, grant)

    def _become_leader(self) -> None:
        self.role = Role.LEADER
        self.leader_id = self.id
        if self._election_timer:
            self._election_timer.cancel()
        nxt = self.log.last_index() + 1
        self._peers = {p: _Peer(next_index=nxt) for p in self.peer_ids}
        # A no-op in our own term lets us commit entries from earlier terms (paper 5.4.2)
        # and is required before ReadIndex reads are safe.
        self.log.append([LogEntry(self.meta.term, b"")])
        log.info("%s: became LEADER for term %d", self.id, self.meta.term)
        self._heartbeat_task = self._spawn(self._heartbeat_loop(self.meta.term))
        self._advance_commit_index()

    def _become_follower(self, term: int) -> None:
        if term > self.meta.term:
            self.meta.update(term, None)
        if self.role == Role.LEADER:
            log.info("%s: stepping down (saw term %d)", self.id, term)
            if self._heartbeat_task:
                self._heartbeat_task.cancel()
            self._fail_all_pending()
        if self.role != Role.FOLLOWER:
            self.leader_id = None
        self.role = Role.FOLLOWER
        self._reset_election_timer()

    def _fail_all_pending(self) -> None:
        err = NotLeaderError(None)
        for fut in self._pending_writes.values():
            if not fut.done():
                fut.set_exception(err)
        for _, fut in self._write_queue:
            if not fut.done():
                fut.set_exception(err)
        for w in self._pending_reads:
            if not w.future.done():
                w.future.set_exception(err)
        self._pending_writes.clear()
        self._write_queue.clear()
        self._pending_reads.clear()

    # ================================================================== replication (leader)
    async def _heartbeat_loop(self, term: int) -> None:
        while not self._stopped and self.role == Role.LEADER and self.meta.term == term:
            self._broadcast_append_entries()
            await asyncio.sleep(self.config.heartbeat_interval)

    def _broadcast_append_entries(self) -> None:
        for peer in self.peer_ids:
            self._replicate_to(peer)

    def _replicate_to(self, peer: str) -> None:
        state = self._peers[peer]
        if state.in_flight:
            state.dirty = True  # send again as soon as the current RPC returns
            return
        state.in_flight = True
        self._spawn(self._append_loop(peer))

    async def _append_loop(self, peer: str) -> None:
        state = self._peers[peer]
        try:
            while not self._stopped and self.role == Role.LEADER:
                state.dirty = False
                nxt = state.next_index
                req = AppendRequest(
                    term=self.meta.term,
                    leader_id=self.id,
                    prev_log_index=nxt - 1,
                    prev_log_term=self.log.term_at(nxt - 1),
                    entries=self.log.slice(nxt, self.config.max_batch_entries),
                    leader_commit=self.commit_index,
                )
                sent_at = self._loop.time()
                try:
                    resp = await self.transport.append_entries(peer, req)
                except Exception:
                    return  # unreachable: the next heartbeat retries
                more = self._on_append_response(peer, req, sent_at, resp)
                if not (more or state.dirty):
                    return
        finally:
            state.in_flight = False

    def _on_append_response(self, peer: str, req: AppendRequest, sent_at: float, resp: AppendResponse) -> bool:
        """Returns True if this follower still needs more entries."""
        if resp.term > self.meta.term:
            self._become_follower(resp.term)
            return False
        if self.role != Role.LEADER or req.term != self.meta.term:
            return False  # stale reply from an older term
        state = self._peers[peer]
        # Any reply in our term proves the peer still accepts us as leader (used by ReadIndex).
        state.last_acked_send_time = max(state.last_acked_send_time, sent_at)
        if resp.success:
            match = req.prev_log_index + len(req.entries)
            state.match_index = max(state.match_index, match)
            state.next_index = max(state.next_index, match + 1)
            self._advance_commit_index()
            more = state.next_index <= self.log.last_index()
        else:
            # Log mismatch: jump back using the follower's hint, never below what it already acked.
            hinted = min(state.next_index - 1, resp.last_log_index + 1)
            state.next_index = max(state.match_index + 1, hinted, 1)
            more = True
        self._check_pending_reads()
        return more

    def _advance_commit_index(self) -> None:
        """Commit the highest index stored on a majority, if it belongs to the current term."""
        for n in range(self.log.last_index(), self.commit_index, -1):
            if self.log.term_at(n) != self.meta.term:
                break  # terms only decrease going backwards
            replicas = 1 + sum(1 for p in self._peers.values() if p.match_index >= n)
            if replicas >= self.majority:
                self.commit_index = n
                self._apply_committed()
                return

    # ================================================================== replication (follower)
    def handle_append_entries(self, req: AppendRequest) -> AppendResponse:
        if self._stopped or req.term < self.meta.term:
            return AppendResponse(self.meta.term, False, self.log.last_index())  # reject an old leader
        if req.term > self.meta.term or self.role != Role.FOLLOWER:
            self._become_follower(req.term)
        self.leader_id = req.leader_id
        self._reset_election_timer()

        # Consistency check: we must already hold the entry just before the new ones.
        if req.prev_log_index > self.log.last_index():
            return AppendResponse(self.meta.term, False, self.log.last_index())
        if self.log.term_at(req.prev_log_index) != req.prev_log_term:
            return AppendResponse(self.meta.term, False, req.prev_log_index - 1)

        for i, entry in enumerate(req.entries):
            index = req.prev_log_index + 1 + i
            if index <= self.log.last_index():
                if self.log.term_at(index) == entry.term:
                    continue                      # already have it
                self.log.truncate_from(index)     # conflicting suffix from an old leader
            self.log.append(req.entries[i:])
            break

        last_new = req.prev_log_index + len(req.entries)
        if req.leader_commit > self.commit_index:
            self.commit_index = max(self.commit_index, min(req.leader_commit, last_new))
            self._apply_committed()
        return AppendResponse(self.meta.term, True, self.log.last_index())

    # ================================================================== applying
    def _apply_committed(self) -> None:
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log.get(self.last_applied)
            result = self.state_machine.apply(entry.command) if entry.command else None
            fut = self._pending_writes.pop(self.last_applied, None)
            if fut is not None and not fut.done():
                fut.set_result(result)
        self._check_pending_reads()

    # ================================================================== client API
    async def submit(self, command: bytes) -> Any:
        """Replicate a command; returns the state machine's result once committed and applied."""
        if self.role != Role.LEADER or self._stopped:
            raise NotLeaderError(self.leader_id)
        fut = self._loop.create_future()
        self._write_queue.append((command, fut))
        if not self._flush_scheduled:
            self._flush_scheduled = True
            self._loop.call_soon(self._flush_write_queue)
        return await fut

    def _flush_write_queue(self) -> None:
        """Group commit: every write queued in this event-loop tick goes to disk in one fsync."""
        self._flush_scheduled = False
        if not self._write_queue or self.role != Role.LEADER:
            return
        first = self.log.last_index() + 1
        self.log.append([LogEntry(self.meta.term, cmd) for cmd, _ in self._write_queue])
        for i, (_, fut) in enumerate(self._write_queue):
            self._pending_writes[first + i] = fut
        self._write_queue.clear()
        self._advance_commit_index()   # a single-node cluster commits right away
        self._broadcast_append_entries()

    async def read_barrier(self) -> None:
        """ReadIndex: returns once a read of the state machine is linearizable.

        1. remember commit_index, 2. confirm we are still leader through a heartbeat round acked
        by a majority, 3. wait until the state machine has applied up to that index.
        """
        if self.role != Role.LEADER or self._stopped:
            raise NotLeaderError(self.leader_id)
        waiter = _ReadWaiter(self._loop.time(), self.meta.term, self._loop.create_future())
        self._pending_reads.append(waiter)
        self._check_pending_reads()
        if not waiter.future.done():
            self._broadcast_append_entries()
        await waiter.future

    def _check_pending_reads(self) -> None:
        if not self._pending_reads:
            return
        own_term_committed = self.log.term_at(self.commit_index) == self.meta.term
        still_waiting = []
        for w in self._pending_reads:
            if w.future.done():
                continue
            if self.role != Role.LEADER or self.meta.term != w.term:
                w.future.set_exception(NotLeaderError(self.leader_id))
                continue
            if own_term_committed:
                if w.read_index < 0:
                    w.read_index = self.commit_index
                acks = 1 + sum(1 for p in self._peers.values() if p.last_acked_send_time >= w.requested_at)
                if acks >= self.majority and self.last_applied >= w.read_index:
                    w.future.set_result(None)
                    continue
            still_waiting.append(w)
        self._pending_reads = still_waiting

    # ================================================================== introspection
    def status(self) -> dict:
        return {
            "id": self.id,
            "role": self.role.value,
            "term": self.meta.term,
            "leader": self.leader_id,
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "log_size": self.log.last_index(),
            "peers": self.peer_ids,
        }
