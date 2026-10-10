"""Test helpers: an in-memory network that can drop nodes and partition the cluster."""

from __future__ import annotations

import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from vaultmesh.kv import KVStateMachine  # noqa: E402
from vaultmesh.raft import RaftConfig, RaftNode, Role  # noqa: E402


class InMemoryNetwork:
    def __init__(self) -> None:
        self.nodes: dict[str, RaftNode] = {}
        self.isolated: set[str] = set()   # nodes cut off from everyone

    def transport_for(self, node_id: str) -> "_Transport":
        return _Transport(self, node_id)

    def isolate(self, node_id: str) -> None:
        self.isolated.add(node_id)

    def heal(self, node_id: str | None = None) -> None:
        if node_id is None:
            self.isolated.clear()
        else:
            self.isolated.discard(node_id)

    async def deliver(self, src: str, dst: str, call):
        await asyncio.sleep(0.002)  # a little network delay
        node = self.nodes.get(dst)
        if (src in self.isolated or dst in self.isolated or node is None or node._stopped
                or node.paused):  # a paused (simulated-crash) node answers nothing
            raise ConnectionError(f"{src} -> {dst} unreachable")
        return call(node)


class _Transport:
    def __init__(self, net: InMemoryNetwork, src: str) -> None:
        self.net, self.src = net, src

    async def request_vote(self, peer_id, req):
        return await self.net.deliver(self.src, peer_id, lambda n: n.handle_request_vote(req))

    async def append_entries(self, peer_id, req):
        return await self.net.deliver(self.src, peer_id, lambda n: n.handle_append_entries(req))

    async def close(self):
        pass


class Cluster:
    def __init__(self, tmp_path, size: int = 3) -> None:
        self.tmp_path = tmp_path
        self.ids = [f"n{i}" for i in range(1, size + 1)]
        self.net = InMemoryNetwork()
        self.kvs: dict[str, KVStateMachine] = {}

    async def start_node(self, node_id: str) -> RaftNode:
        kv = KVStateMachine()
        peers = [p for p in self.ids if p != node_id]
        node = RaftNode(node_id, peers, self.net.transport_for(node_id),
                        str(self.tmp_path / node_id), kv, RaftConfig.fast_for_tests())
        self.net.nodes[node_id] = node
        self.kvs[node_id] = kv
        await node.start()
        return node

    async def start(self) -> "Cluster":
        for node_id in self.ids:
            await self.start_node(node_id)
        return self

    async def stop_node(self, node_id: str) -> None:
        await self.net.nodes[node_id].stop()

    async def stop(self) -> None:
        for node in self.net.nodes.values():
            if not node._stopped:
                await node.stop()

    def node(self, node_id: str) -> RaftNode:
        return self.net.nodes[node_id]

    async def wait_for_leader(self, among: list[str] | None = None, timeout: float = 5.0) -> RaftNode:
        among = among or self.ids
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            leaders = [self.node(i) for i in among
                       if not self.node(i)._stopped and self.node(i).role == Role.LEADER]
            if len(leaders) == 1:
                return leaders[0]
            await asyncio.sleep(0.02)
        raise AssertionError("no single leader elected in time")

    async def wait_until(self, predicate, timeout: float = 5.0) -> None:
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            if predicate():
                return
            await asyncio.sleep(0.02)
        raise AssertionError("condition not met in time")


@pytest.fixture
async def cluster(tmp_path):
    c = await Cluster(tmp_path, 3).start()
    yield c
    await c.stop()


@pytest.fixture
async def cluster5(tmp_path):
    c = await Cluster(tmp_path, 5).start()
    yield c
    await c.stop()
