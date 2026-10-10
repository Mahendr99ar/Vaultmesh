"""Gateway (router) in front of all shards.

* Uses a consistent hash ring to map every key to a shard (a 3-node Raft group).
* Remembers each shard's leader; on a 503 it follows the ``X-Raft-Leader`` hint, and on a dead
  node it tries the next replica with exponential backoff, so clients never see a failover.
* ``GET /cluster`` collects ``/status`` from every node in parallel; ``GET /`` serves a live
  dashboard.
* ``POST /admin/crash`` simulates a crash of a shard's leader for a few seconds (demo button).
  It refuses unless all three replicas are healthy, so a shard can never lose its majority.
"""

from __future__ import annotations

import asyncio
import json
import time
from contextlib import asynccontextmanager
from importlib import resources
from urllib.parse import quote

import aiohttp
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response

from .ring import ConsistentHashRing

MAX_ATTEMPTS = 8


def _error_code(content: bytes) -> str | None:
    try:
        return json.loads(content).get("error")
    except (ValueError, AttributeError):
        return None


def create_gateway_app(shards: dict[str, list[str]], vnodes: int = 128) -> FastAPI:
    ring = ConsistentHashRing(list(shards), vnodes=vnodes)
    leader_cache: dict[str, str] = {}
    box: dict[str, aiohttp.ClientSession] = {}

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        box["s"] = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=4.0, sock_connect=0.5),
            connector=aiohttp.TCPConnector(limit=256),
            trust_env=False,
        )
        yield
        await box["s"].close()

    app = FastAPI(title="VaultMesh gateway", lifespan=lifespan)

    def next_node(nodes: list[str], current: str) -> str:
        return nodes[(nodes.index(current) + 1) % len(nodes)] if current in nodes else nodes[0]

    async def forward(shard: str, method: str, path: str, body: bytes, params: dict) -> Response:
        nodes = shards[shard]
        target = leader_cache.get(shard, nodes[0])
        last_error = "no attempt made"
        for attempt in range(MAX_ATTEMPTS):
            try:
                async with box["s"].request(
                    method, f"http://{target}{path}", data=body or None, params=params,
                    headers={"content-type": "application/json"} if body else None,
                ) as resp:
                    status, content = resp.status, await resp.read()
                    leader_hint = resp.headers.get("X-Raft-Leader")
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:   # node down or unreachable
                last_error = f"{target}: {type(e).__name__}"
                leader_cache.pop(shard, None)
                target = next_node(nodes, target)
                await asyncio.sleep(min(0.05 * 2 ** attempt, 0.8))
                continue

            code = _error_code(content) if status == 503 else None
            if code == "node_down":                         # crashed node: treat like a dead machine
                last_error = f"{target} is down"
                leader_cache.pop(shard, None)
                target = next_node(nodes, target)
                await asyncio.sleep(min(0.05 * 2 ** attempt, 0.8))
                continue

            if code == "not_leader":
                last_error = f"{target} is not leader"
                if leader_hint and leader_hint != target:
                    target = leader_hint                   # follow the redirect immediately
                    leader_cache[shard] = leader_hint
                    continue
                leader_cache.pop(shard, None)              # election in progress: wait, try another
                target = next_node(nodes, target)
                await asyncio.sleep(min(0.05 * 2 ** attempt, 0.8))
                continue

            leader_cache[shard] = target
            return Response(
                content=content, status_code=status, media_type="application/json",
                headers={"X-Shard": shard, "X-Served-By": target, "X-Attempts": str(attempt + 1)},
            )
        return JSONResponse(status_code=503, content={"error": "shard_unavailable", "shard": shard,
                                                      "detail": last_error})

    @app.api_route("/kv/{key:path}", methods=["GET", "PUT", "DELETE"])
    async def kv(key: str, request: Request):
        shard = ring.node_for(key)
        return await forward(shard, request.method, f"/kv/{quote(key, safe=':')}",
                             await request.body(), dict(request.query_params))

    @app.get("/ring-map")
    async def ring_map():
        return {"shards": list(shards), "points": [[round(p, 6), s] for p, s in ring.points()]}

    @app.get("/ring/{key:path}")
    async def which_shard(key: str):
        shard = ring.node_for(key)
        return {"key": key, "shard": shard, "position": ring.position(key), "nodes": shards[shard],
                "cached_leader": leader_cache.get(shard)}

    async def status_of(addr: str) -> dict:
        t0 = time.perf_counter()
        try:
            async with box["s"].get(f"http://{addr}/status",
                                    timeout=aiohttp.ClientTimeout(total=0.5)) as r:
                data = await r.json()
            return {"address": addr, "up": True,
                    "latency_ms": round((time.perf_counter() - t0) * 1000, 1), **data}
        except Exception:
            return {"address": addr, "up": False}

    @app.get("/cluster")
    async def cluster():
        return {"shards": {shard: await asyncio.gather(*(status_of(a) for a in nodes))
                           for shard, nodes in shards.items()}}

    @app.post("/admin/crash")
    async def crash(request: Request):
        body = await request.json()
        shard = body.get("shard")
        seconds = float(body.get("seconds", 4))
        if shard not in shards:
            return JSONResponse(status_code=404, content={"error": f"unknown shard {shard!r}"})
        statuses = await asyncio.gather(*(status_of(a) for a in shards[shard]))
        if not all(s["up"] and not s.get("paused") for s in statuses):
            return JSONResponse(status_code=409, content={
                "error": "shard_degraded",
                "detail": "A replica in this shard is already down. Wait until all three are back."})
        leaders = [s for s in statuses if s.get("role") == "leader"]
        if len(leaders) != 1:
            return JSONResponse(status_code=409, content={
                "error": "no_leader", "detail": "This shard is electing a leader right now. Try again."})
        leader = leaders[0]
        async with box["s"].post(f"http://{leader['address']}/admin/pause", json={"seconds": seconds}) as r:
            result = await r.json()
        return {"shard": shard, "crashed": leader["id"], "address": leader["address"],
                "term": leader["term"], "seconds": result["paused_for"]}

    @app.get("/health")
    async def health():
        return {"ok": True}

    @app.get("/", response_class=HTMLResponse)
    async def dashboard():
        return resources.files("vaultmesh").joinpath("static/dashboard.html").read_text(encoding="utf-8")

    return app
