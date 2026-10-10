import asyncio

import pytest

from vaultmesh.kv import Command
from vaultmesh.messages import AppendRequest
from vaultmesh.raft import NotLeaderError, Role
from vaultmesh.wal import LogEntry

from .conftest import Cluster


def put(key, value):
    return Command.put(key, value).encode()


async def test_elects_exactly_one_leader(cluster):
    leader = await cluster.wait_for_leader()
    await asyncio.sleep(0.5)  # stays stable: heartbeats suppress new elections
    leaders = [n for n in cluster.net.nodes.values() if n.role == Role.LEADER]
    assert leaders == [leader]
    term = leader.meta.term
    assert all(n.meta.term == term for n in cluster.net.nodes.values())


async def test_writes_replicate_to_every_node(cluster):
    leader = await cluster.wait_for_leader()
    for i in range(50):
        await leader.submit(put(f"k{i}", f"v{i}"))
    await cluster.wait_until(lambda: all(len(kv) == 50 for kv in cluster.kvs.values()))
    assert all(kv.get("k49") == "v49" for kv in cluster.kvs.values())


async def test_concurrent_writes_are_group_committed(cluster):
    leader = await cluster.wait_for_leader()
    before = leader.log.last_index()
    results = await asyncio.gather(*(leader.submit(put(f"c{i}", "x")) for i in range(200)))
    assert len(results) == 200
    assert leader.log.last_index() == before + 200
    await cluster.wait_until(lambda: all(len(kv) == 200 for kv in cluster.kvs.values()))


async def test_follower_rejects_client_writes(cluster):
    leader = await cluster.wait_for_leader()
    follower = next(n for n in cluster.net.nodes.values() if n is not leader)
    await cluster.wait_until(lambda: follower.leader_id == leader.id)
    with pytest.raises(NotLeaderError) as err:
        await follower.submit(put("a", "b"))
    assert err.value.leader_id == leader.id  # tells the client where to go


async def test_leader_failover_keeps_committed_data(cluster):
    old = await cluster.wait_for_leader()
    await old.submit(put("before", "failover"))

    cluster.net.isolate(old.id)                               # leader's network dies
    survivors = [i for i in cluster.ids if i != old.id]
    new = await cluster.wait_for_leader(among=survivors)
    assert new.id != old.id and new.meta.term > old.meta.term

    await new.submit(put("after", "failover"))
    await new.read_barrier()
    assert cluster.kvs[new.id].get("before") == "failover"   # nothing committed was lost
    assert cluster.kvs[new.id].get("after") == "failover"

    cluster.net.heal(old.id)                                  # old leader comes back
    await cluster.wait_until(lambda: old.role == Role.FOLLOWER and cluster.kvs[old.id].get("after") == "failover")


async def test_isolated_minority_leader_cannot_commit(cluster):
    old = await cluster.wait_for_leader()
    cluster.net.isolate(old.id)
    pending = asyncio.ensure_future(old.submit(put("lost", "write")))
    with pytest.raises((asyncio.TimeoutError, NotLeaderError)):
        await asyncio.wait_for(asyncio.shield(pending), 0.6)  # no majority -> never commits

    survivors = [i for i in cluster.ids if i != old.id]
    new = await cluster.wait_for_leader(among=survivors)
    await new.submit(put("winner", "yes"))
    cluster.net.heal(old.id)

    # The old leader learns about the higher term, steps down and drops its uncommitted entry.
    await cluster.wait_until(lambda: old.role == Role.FOLLOWER and cluster.kvs[old.id].get("winner") == "yes")
    with pytest.raises(NotLeaderError):
        await pending
    assert all(kv.get("lost") is None for kv in cluster.kvs.values())


async def test_linearizable_read_after_write(cluster):
    leader = await cluster.wait_for_leader()
    await leader.submit(put("x", "1"))
    await leader.read_barrier()
    assert cluster.kvs[leader.id].get("x") == "1"


async def test_isolated_leader_cannot_serve_strong_reads(cluster):
    leader = await cluster.wait_for_leader()
    cluster.net.isolate(leader.id)
    with pytest.raises((asyncio.TimeoutError, NotLeaderError)):
        await asyncio.wait_for(leader.read_barrier(), 0.6)  # could be stale, so it refuses


async def test_full_cluster_restart_recovers_from_wal(tmp_path):
    c = await Cluster(tmp_path, 3).start()
    leader = await c.wait_for_leader()
    for i in range(30):
        await leader.submit(put(f"k{i}", str(i)))
    await c.stop()                                   # every node "crashes"

    c2 = await Cluster(tmp_path, 3).start()          # same data directories
    try:
        new = await c2.wait_for_leader()
        await new.read_barrier()
        assert c2.kvs[new.id].get("k29") == "29"
        await c2.wait_until(lambda: all(len(kv) == 30 for kv in c2.kvs.values()))
    finally:
        await c2.stop()


async def test_crashed_follower_catches_up(cluster):
    leader = await cluster.wait_for_leader()
    victim = next(i for i in cluster.ids if i != leader.id)
    await cluster.stop_node(victim)
    for i in range(100):
        await leader.submit(put(f"k{i}", "v"))      # majority of 2 still commits
    await cluster.start_node(victim)                 # restart from its WAL
    await cluster.wait_until(lambda: len(cluster.kvs[victim]) == 100)


async def test_five_node_cluster_survives_two_failures(cluster5):
    leader = await cluster5.wait_for_leader()
    others = [i for i in cluster5.ids if i != leader.id]
    await cluster5.stop_node(others[0])
    await cluster5.stop_node(others[1])
    await asyncio.wait_for(leader.submit(put("still", "works")), 2)


def test_follower_truncates_conflicting_entries(tmp_path):
    """A follower holding entries from a deposed leader replaces them with the new leader's."""
    async def scenario():
        c = Cluster(tmp_path, 3)
        node = await c.start_node("n1")
        node._election_timer.cancel()
        node.log.append([LogEntry(1, b""), LogEntry(1, put("a", "old")), LogEntry(1, put("b", "old"))])
        node.meta.update(1, None)

        resp = node.handle_append_entries(AppendRequest(
            term=2, leader_id="n2", prev_log_index=1, prev_log_term=1,
            entries=[LogEntry(2, put("a", "new"))], leader_commit=2))
        assert resp.success
        assert node.log.last_index() == 2
        assert node.log.term_at(2) == 2
        assert c.kvs["n1"].get("a") == "new"
        assert c.kvs["n1"].get("b") is None

        stale = node.handle_append_entries(AppendRequest(1, "n3", 0, 0, [], 0))
        assert not stale.success and stale.term == 2   # rejects a leader from an old term
        await node.stop()

    asyncio.run(scenario())


async def test_simulated_crash_fails_over_and_old_leader_rejoins(cluster):
    """The dashboard's "Crash leader" button: leader goes silent, a new one is elected,
    and when the old leader comes back it steps down and catches up."""
    old = await cluster.wait_for_leader()
    await old.submit(put("before", "crash"))
    old.pause(1.0)
    assert old.paused

    survivors = [i for i in cluster.ids if i != old.id]
    new = await cluster.wait_for_leader(among=survivors)
    await new.submit(put("during", "crash"))

    await cluster.wait_until(lambda: not old.paused)
    await cluster.wait_until(lambda: old.role == Role.FOLLOWER and old.leader_id == new.id
                             and cluster.kvs[old.id].get("during") == "crash")
    assert old.meta.term == new.meta.term
