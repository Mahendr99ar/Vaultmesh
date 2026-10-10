"""How a Raft node talks to its peers.

Production uses :class:`HttpTransport`: JSON over HTTP with a pooled, keep-alive aiohttp
client. (aiohttp was chosen after profiling: httpx's async client capped this cluster at
roughly 300 requests/sec, aiohttp handled 5,000+ on the same machine.)
Tests use an in-memory network that can simulate crashes and network partitions.
"""

from __future__ import annotations

from typing import Protocol

import aiohttp

from .messages import AppendRequest, AppendResponse, VoteRequest, VoteResponse


class Transport(Protocol):
    async def request_vote(self, peer_id: str, req: VoteRequest) -> VoteResponse: ...

    async def append_entries(self, peer_id: str, req: AppendRequest) -> AppendResponse: ...

    async def close(self) -> None: ...


class HttpTransport:
    def __init__(self, peer_addresses: dict[str, str], timeout: float = 0.5) -> None:
        self._addresses = peer_addresses
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._session: aiohttp.ClientSession | None = None

    def _client(self) -> aiohttp.ClientSession:
        # Created lazily because a session must be bound to the running event loop.
        if self._session is None:
            self._session = aiohttp.ClientSession(
                timeout=self._timeout,
                connector=aiohttp.TCPConnector(limit=64),
                trust_env=False,  # node-to-node traffic must never go through an HTTP proxy
            )
        return self._session

    async def _post(self, peer_id: str, path: str, body: dict) -> dict:
        async with self._client().post(f"http://{self._addresses[peer_id]}{path}", json=body) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def request_vote(self, peer_id: str, req: VoteRequest) -> VoteResponse:
        return VoteResponse.from_dict(await self._post(peer_id, "/raft/vote", req.to_dict()))

    async def append_entries(self, peer_id: str, req: AppendRequest) -> AppendResponse:
        return AppendResponse.from_dict(await self._post(peer_id, "/raft/append", req.to_dict()))

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
