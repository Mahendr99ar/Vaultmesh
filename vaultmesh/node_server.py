"""HTTP API for one storage node (FastAPI).

Raft RPCs (node to node):
    POST /raft/vote            RequestVote
    POST /raft/append          AppendEntries

Client API (normally called through the gateway):
    GET    /kv/{key}           linearizable read (add ?consistency=stale for a fast local read)
    PUT    /kv/{key}           body: {"value": "..."}
    DELETE /kv/{key}
    GET    /status             role, term, leader, log and commit indexes
    GET    /health
    POST   /admin/pause        body: {"seconds": 1-10}; simulate a crash (demo)

A non-leader answers client calls with HTTP 503 and the header ``X-Raft-Leader: host:port``
so the caller can retry against the real leader.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from .kv import Command, KVStateMachine
from .messages import AppendRequest, VoteRequest
from .raft import NotLeaderError, RaftNode

WRITE_TIMEOUT = 3.0
READ_TIMEOUT = 2.0


MAX_PAUSE_SECONDS = 10.0


class PutBody(BaseModel):
    value: str


class PauseBody(BaseModel):
    seconds: float = 4.0


def create_node_app(raft: RaftNode, kv: KVStateMachine, peer_addresses: dict[str, str]) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await raft.start()
        yield
        await raft.stop()
        await raft.transport.close()

    app = FastAPI(title=f"VaultMesh node {raft.id}", lifespan=lifespan)

    @app.middleware("http")
    async def crashed_nodes_are_silent(request: Request, call_next):
        # During a simulated crash the node answers nothing except /status, so peers and the
        # gateway see the same failures they would see from a dead machine.
        if raft.paused and request.url.path.startswith(("/raft/", "/kv/")):
            return JSONResponse(status_code=503, content={"error": "node_down"})
        return await call_next(request)

    def not_leader(err: NotLeaderError) -> JSONResponse:
        leader_addr = peer_addresses.get(err.leader_id) if err.leader_id else None
        headers = {"X-Raft-Leader": leader_addr} if leader_addr else {}
        return JSONResponse(
            status_code=503,
            content={"error": "not_leader", "leader_id": err.leader_id, "leader_address": leader_addr},
            headers=headers,
        )

    # ------------------------------------------------------------- Raft RPCs
    @app.post("/raft/vote")
    async def raft_vote(request: Request):
        return raft.handle_request_vote(VoteRequest.from_dict(await request.json())).to_dict()

    @app.post("/raft/append")
    async def raft_append(request: Request):
        return raft.handle_append_entries(AppendRequest.from_dict(await request.json())).to_dict()

    # ------------------------------------------------------------- client API
    @app.get("/kv/{key:path}")
    async def get_key(key: str, consistency: str = "strong"):
        if consistency == "stale":
            value = kv.get(key)  # may be slightly behind the leader, but never blocks
        else:
            try:
                await asyncio.wait_for(raft.read_barrier(), READ_TIMEOUT)
            except NotLeaderError as e:
                return not_leader(e)
            except asyncio.TimeoutError:
                raise HTTPException(504, "could not confirm leadership with a majority")
            value = kv.get(key)
        if value is None:
            raise HTTPException(404, f"key {key!r} not found")
        return {"key": key, "value": value, "node": raft.id}

    @app.put("/kv/{key:path}")
    async def put_key(key: str, body: PutBody):
        return await _write(Command.put(key, body.value), key)

    @app.delete("/kv/{key:path}")
    async def delete_key(key: str):
        return await _write(Command.delete(key), key)

    async def _write(cmd: Command, key: str):
        try:
            previous = await asyncio.wait_for(raft.submit(cmd.encode()), WRITE_TIMEOUT)
        except NotLeaderError as e:
            return not_leader(e)
        except asyncio.TimeoutError:
            raise HTTPException(504, "write not committed in time (no majority reachable?)")
        return {"key": key, "op": cmd.op, "previous": previous, "node": raft.id,
                "index": raft.commit_index}

    # ------------------------------------------------------------- introspection
    @app.get("/status")
    async def status():
        return {**raft.status(), "keys": len(kv)}

    @app.get("/health")
    async def health():
        return {"ok": True}

    @app.post("/admin/pause")
    async def pause(body: PauseBody):
        seconds = max(1.0, min(MAX_PAUSE_SECONDS, body.seconds))
        raft.pause(seconds)
        return {"node": raft.id, "paused_for": seconds, "was_leader": raft.role.value == "leader"}

    return app
