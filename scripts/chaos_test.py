"""Chaos test: kill a shard's leader during live traffic and prove no acknowledged write is lost.

Local cluster:  start it (python scripts/local_cluster.py --clean), then in another terminal

    python scripts/chaos_test.py --rounds 3

Docker cluster (e.g. on your EC2 server, from the project folder):

    python3 scripts/chaos_test.py --docker --rounds 3

Each round:
  1. writes continuously through the gateway,
  2. hard-kills (SIGKILL) the current leader of shard1, like pulling the power cable,
  3. measures how long writes to that shard were unavailable (failover time),
  4. restarts the killed node so it rejoins from its write-ahead log,
  5. reads back EVERY write that was acknowledged and checks it is there.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import subprocess
import sys
import time

import aiohttp

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "data")


class LocalController:
    """Kills/restarts node processes started by scripts/local_cluster.py."""

    def __init__(self) -> None:
        self.path = os.path.join(DATA, "cluster.json")
        with open(self.path) as f:
            self.layout = json.load(f)

    def _node(self, node_id: str) -> dict:
        return next(n for n in self.layout["nodes"] if n["id"] == node_id)

    def kill(self, node_id: str) -> None:
        pid = self._node(node_id)["pid"]
        if sys.platform == "win32":
            subprocess.run(["taskkill", "/F", "/PID", str(pid)], capture_output=True)
        else:
            os.kill(pid, signal.SIGKILL)

    def restart(self, node_id: str) -> None:
        node = self._node(node_id)
        log = open(os.path.join(DATA, "logs", f"{node_id}.log"), "ab")
        node["pid"] = subprocess.Popen(node["cmd"], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT).pid
        with open(self.path, "w") as f:
            json.dump(self.layout, f, indent=2)


class DockerController:
    """Kills/restarts containers of the docker compose cluster (service name = node id)."""

    def kill(self, node_id: str) -> None:
        subprocess.run(["docker", "compose", "kill", "-s", "SIGKILL", node_id], cwd=ROOT,
                       check=True, capture_output=True)

    def restart(self, node_id: str) -> None:
        subprocess.run(["docker", "compose", "start", node_id], cwd=ROOT, check=True, capture_output=True)


class Client:
    """Tiny wrapper so the test reads simply: status code + parsed JSON."""

    def __init__(self, base: str) -> None:
        self.base = base.rstrip("/")
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5), trust_env=False)

    async def call(self, method: str, path: str, body: dict | None = None, timeout: float | None = None):
        kw = {"timeout": aiohttp.ClientTimeout(total=timeout)} if timeout else {}
        async with self.session.request(method, self.base + path, json=body, **kw) as r:
            return r.status, await r.json(content_type=None)

    async def close(self) -> None:
        await self.session.close()


ERRORS = (aiohttp.ClientError, asyncio.TimeoutError)


async def shard1_leader(client: Client) -> str:
    for _ in range(100):
        status = (await client.call("GET", "/cluster"))[1]["shards"]["shard1"]
        leaders = [n for n in status if n.get("up") and n.get("role") == "leader"]
        if len(leaders) == 1:
            return leaders[0]["id"]
        await asyncio.sleep(0.1)
    raise RuntimeError("shard1 has no leader")


async def find_shard1_keys(client: Client, prefix: str, n: int) -> list[str]:
    keys, i = [], 0
    while len(keys) < n:
        k = f"{prefix}:{i}"
        if (await client.call("GET", f"/ring/{k}"))[1]["shard"] == "shard1":
            keys.append(k)
        i += 1
    return keys


async def one_round(client: Client, ctl, rnd: int, writers: int) -> dict:
    victim = await shard1_leader(client)
    keys = await find_shard1_keys(client, f"chaos{rnd}", 4000)
    acked: dict[str, str] = {}
    failures: list[float] = []
    stop = asyncio.Event()
    key_iter = iter(keys)

    async def writer():
        while not stop.is_set():
            try:
                k = next(key_iter)
            except StopIteration:
                return
            t = time.perf_counter()
            try:
                code, _ = await client.call("PUT", f"/kv/{k}", {"value": k})
                if code == 200:
                    acked[k] = k
                else:
                    failures.append(t)
            except ERRORS:
                failures.append(t)

    tasks = [asyncio.create_task(writer()) for _ in range(writers)]
    await asyncio.sleep(1.0)                      # steady traffic
    killed_at = time.perf_counter()
    ctl.kill(victim)
    print(f"  round {rnd}: killed leader {victim}")

    # Failover is over when a write to shard1 succeeds again.
    probe_keys = await find_shard1_keys(client, f"probe{rnd}", 200)
    recovered_at = None
    for k in probe_keys:
        try:
            code, _ = await client.call("PUT", f"/kv/{k}", {"value": "p"}, timeout=0.5)
            if code == 200:
                recovered_at = time.perf_counter()
                break
        except ERRORS:
            pass
        await asyncio.sleep(0.01)
    await asyncio.sleep(1.0)
    stop.set()
    await asyncio.gather(*tasks)

    # Restart the dead node; it recovers from its WAL and catches up from the new leader.
    ctl.restart(victim)

    lost = []
    for k, v in acked.items():
        code, body = await client.call("GET", f"/kv/{k}")
        if code != 200 or body["value"] != v:
            lost.append(k)
    return {
        "round": rnd, "killed": victim, "acked_writes": len(acked),
        "failover_ms": round((recovered_at - killed_at) * 1000) if recovered_at else None,
        "lost_acked_writes": len(lost),
    }


async def main(rounds: int, writers: int, gateway: str, docker: bool) -> None:
    ctl = DockerController() if docker else LocalController()
    client = Client(gateway)
    results = []
    try:
        for rnd in range(1, rounds + 1):
            results.append(await one_round(client, ctl, rnd, writers))
            print(f"  -> {results[-1]}")
            await asyncio.sleep(2.0)              # let the restarted node rejoin
    finally:
        await client.close()
    ok = all(r["lost_acked_writes"] == 0 for r in results)
    times = [r["failover_ms"] for r in results if r["failover_ms"] is not None]
    print()
    print(f"Rounds: {rounds}   acknowledged writes: {sum(r['acked_writes'] for r in results)}   "
          f"lost: {sum(r['lost_acked_writes'] for r in results)}")
    if times:
        print(f"Failover time: min {min(times)} ms, avg {sum(times) // len(times)} ms, max {max(times)} ms")
    print("RESULT:", "PASS - no acknowledged write was lost" if ok else "FAIL")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rounds", type=int, default=3)
    ap.add_argument("--writers", type=int, default=16)
    ap.add_argument("--gateway", default="http://localhost:8080")
    ap.add_argument("--docker", action="store_true", help="cluster runs under docker compose")
    a = ap.parse_args()
    asyncio.run(main(a.rounds, a.writers, a.gateway, a.docker))
