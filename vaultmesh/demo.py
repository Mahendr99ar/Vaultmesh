"""Single-process demo mode for small free hosts (e.g. Render's 512 MB free plan).

Runs the whole cluster -- every Raft node of every shard plus the gateway -- inside ONE Python
process and ONE event loop:

* Each node is the normal :class:`RaftNode` with its normal write-ahead log on disk.
* Nodes exchange Raft RPCs through :class:`LocalTransport` (direct calls with a small simulated
  network delay) instead of HTTP, which keeps CPU and memory low. A node that is paused by the
  "crash leader" button is unreachable, exactly like on a real network.
* Each node still serves its normal HTTP API on 127.0.0.1, and the gateway talks to the nodes over
  real HTTP, so routing, leader discovery and retries work exactly as in the multi-process setup.

    python -m vaultmesh demo --port 8080
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import shutil
import signal
import socket

import uvicorn

from .gateway import create_gateway_app
from .kv import KVStateMachine
from .messages import AppendRequest, AppendResponse, VoteRequest, VoteResponse
from .node_server import create_node_app
from .raft import RaftConfig, RaftNode

log = logging.getLogger("demo")
NETWORK_DELAY = 0.002  # seconds, one way


class LocalTransport:
    """In-process 'network' between nodes of the same process."""

    def __init__(self, registry: dict[str, RaftNode], src: str) -> None:
        self.registry, self.src = registry, src

    async def _reach(self, peer_id: str) -> RaftNode:
        await asyncio.sleep(NETWORK_DELAY)
        node = self.registry.get(peer_id)
        if node is None or node._stopped or node.paused:
            raise ConnectionError(f"{self.src} -> {peer_id}: unreachable")
        return node

    async def request_vote(self, peer_id: str, req: VoteRequest) -> VoteResponse:
        return (await self._reach(peer_id)).handle_request_vote(req)

    async def append_entries(self, peer_id: str, req: AppendRequest) -> AppendResponse:
        return (await self._reach(peer_id)).handle_append_entries(req)

    async def close(self) -> None:
        pass


def _free_port() -> int:
    """Ask the OS for a free port, so the demo never clashes with another cluster on this machine."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _Server(uvicorn.Server):
    """Several uvicorn servers share one loop, so none of them may grab the signal handlers."""

    @contextlib.contextmanager
    def capture_signals(self):
        yield


async def run(port: int, num_shards: int, data_dir: str) -> None:
    shutil.rmtree(data_dir, ignore_errors=True)
    registry: dict[str, RaftNode] = {}
    servers: list[_Server] = []
    shards: dict[str, list[str]] = {}

    for s in range(1, num_shards + 1):
        members = [(f"s{s}n{n}", _free_port()) for n in range(1, 4)]
        shards[f"shard{s}"] = [f"127.0.0.1:{p}" for _, p in members]
        for node_id, node_port in members:
            peers = {pid: f"127.0.0.1:{pp}" for pid, pp in members if pid != node_id}
            kv = KVStateMachine()
            raft = RaftNode(node_id, list(peers), LocalTransport(registry, node_id),
                            os.path.join(data_dir, node_id), kv, RaftConfig())
            registry[node_id] = raft
            app = create_node_app(raft, kv, peers)
            servers.append(_Server(uvicorn.Config(app, host="127.0.0.1", port=node_port,
                                                  log_level="warning", access_log=False)))

    gateway = create_gateway_app(shards)
    servers.append(_Server(uvicorn.Config(gateway, host="0.0.0.0", port=port,
                                          log_level="warning", access_log=False)))
    log.info("VaultMesh demo: %d shards x 3 nodes in one process, dashboard on port %d", num_shards, port)
    print(f"VaultMesh demo running: {num_shards} shards x 3 nodes. Open http://localhost:{port}/", flush=True)
    print("Press Ctrl+C to stop.", flush=True)

    # Graceful shutdown on Ctrl+C (and on SIGTERM, which hosts like Render send on redeploy):
    # ask every server to exit so each runs its shutdown hooks (Raft nodes stop, HTTP clients
    # close) instead of being cancelled mid-flight.
    loop = asyncio.get_running_loop()

    def request_exit(*_):
        loop.call_soon_threadsafe(lambda: [setattr(srv, "should_exit", True) for srv in servers])

    signal.signal(signal.SIGINT, request_exit)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, request_exit)

    await asyncio.gather(*(srv.serve() for srv in servers))
    print("VaultMesh demo stopped.", flush=True)


def main(port: int, num_shards: int, data_dir: str) -> None:
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(run(port, num_shards, data_dir))
