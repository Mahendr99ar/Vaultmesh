"""Load generator: measures throughput and latency percentiles through the gateway.

    python -m vaultmesh bench --target http://localhost:8080 --ops 5000 --concurrency 64
"""

from __future__ import annotations

import asyncio
import random
import string
import time

import aiohttp


def _pct(sorted_values: list[float], p: float) -> float:
    if not sorted_values:
        return 0.0
    k = min(len(sorted_values) - 1, int(round(p / 100 * (len(sorted_values) - 1))))
    return sorted_values[k]


async def _phase(session: aiohttp.ClientSession, target: str, name: str,
                 requests: list[tuple[str, str, dict | None]], concurrency: int) -> dict:
    latencies: list[float] = []
    errors = 0
    sem = asyncio.Semaphore(concurrency)

    async def one(method: str, path: str, body: dict | None) -> None:
        nonlocal errors
        async with sem:
            t0 = time.perf_counter()
            try:
                async with session.request(method, target + path, json=body) as r:
                    await r.read()
                    ok = r.status in (200, 404)
            except (aiohttp.ClientError, asyncio.TimeoutError):
                ok = False
            latencies.append((time.perf_counter() - t0) * 1000)
            if not ok:
                errors += 1

    start = time.perf_counter()
    await asyncio.gather(*(one(*r) for r in requests))
    elapsed = time.perf_counter() - start
    latencies.sort()
    return {
        "phase": name, "ops": len(requests), "errors": errors,
        "ops_per_sec": round(len(requests) / elapsed),
        "p50_ms": round(_pct(latencies, 50), 2), "p95_ms": round(_pct(latencies, 95), 2),
        "p99_ms": round(_pct(latencies, 99), 2),
    }


async def run(target: str, ops: int, concurrency: int, value_size: int) -> list[dict]:
    target = target.rstrip("/")
    value = "".join(random.choices(string.ascii_letters, k=value_size))
    keys = [f"bench:{i}" for i in range(ops)]
    connector = aiohttp.TCPConnector(limit=concurrency)
    async with aiohttp.ClientSession(connector=connector, timeout=aiohttp.ClientTimeout(total=10),
                                     trust_env=False) as session:
        writes = [("PUT", f"/kv/{k}", {"value": value}) for k in keys]
        reads = [("GET", f"/kv/{k}", None) for k in keys]
        stale = [("GET", f"/kv/{k}?consistency=stale", None) for k in keys]
        return [
            await _phase(session, target, "write (PUT)", writes, concurrency),
            await _phase(session, target, "read (linearizable)", reads, concurrency),
            await _phase(session, target, "read (stale/local)", stale, concurrency),
        ]


def main(target: str, ops: int, concurrency: int, value_size: int) -> None:
    results = asyncio.run(run(target, ops, concurrency, value_size))
    header = f"{'phase':<22}{'ops':>7}{'errors':>8}{'ops/sec':>10}{'p50 ms':>9}{'p95 ms':>9}{'p99 ms':>9}"
    print(header)
    print("-" * len(header))
    for r in results:
        print(f"{r['phase']:<22}{r['ops']:>7}{r['errors']:>8}{r['ops_per_sec']:>10}"
              f"{r['p50_ms']:>9}{r['p95_ms']:>9}{r['p99_ms']:>9}")
