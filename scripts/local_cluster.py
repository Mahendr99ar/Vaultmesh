"""Run a full VaultMesh cluster on one machine (works on Windows, macOS and Linux).

    python scripts/local_cluster.py            # 2 shards x 3 nodes + gateway on :8080
    python scripts/local_cluster.py --shards 3 # 3 shards x 3 nodes
    python scripts/local_cluster.py --clean    # wipe data/ first

Press Ctrl+C to stop everything. Process info is written to data/cluster.json so that
scripts/chaos_test.py can kill and restart individual nodes.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "data")
LOGS = os.path.join(DATA, "logs")


def plan(num_shards: int, gateway_port: int) -> dict:
    shards = {}
    nodes = []
    for s in range(1, num_shards + 1):
        members = [(f"s{s}n{n}", 9000 + s * 100 + n) for n in range(1, 4)]
        shards[f"shard{s}"] = [f"127.0.0.1:{port}" for _, port in members]
        for node_id, port in members:
            peers = ",".join(f"{pid}=127.0.0.1:{pport}" for pid, pport in members if pid != node_id)
            nodes.append({
                "id": node_id, "shard": f"shard{s}", "port": port,
                "cmd": [sys.executable, "-m", "vaultmesh", "node", "--id", node_id, "--port", str(port),
                        "--data", os.path.join(DATA, node_id), "--peers", peers],
            })
    shard_arg = ";".join(f"{name}={','.join(addrs)}" for name, addrs in shards.items())
    gateway = {"id": "gateway", "port": gateway_port,
               "cmd": [sys.executable, "-m", "vaultmesh", "gateway", "--port", str(gateway_port),
                       "--shards", shard_arg]}
    return {"shards": shards, "nodes": nodes, "gateway": gateway}


def spawn(cmd: list[str], log_name: str) -> subprocess.Popen:
    log = open(os.path.join(LOGS, f"{log_name}.log"), "ab")
    return subprocess.Popen(cmd, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)


def _is_our_process(pid: int) -> bool:
    """True only if the pid is still a Python process (never kill an unrelated program
    that happens to reuse an old pid)."""
    if sys.platform == "win32":
        out = subprocess.run(["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
                             capture_output=True, text=True).stdout.lower()
        return "python" in out
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            return b"vaultmesh" in f.read()
    except OSError:
        return False


def stop_previous_cluster() -> None:
    """Stop a cluster left running from an earlier start, e.g. when its terminal window was
    closed instead of pressing Ctrl+C (on Windows the node processes keep running and keep
    the log files and ports busy)."""
    path = os.path.join(DATA, "cluster.json")
    if not os.path.exists(path):
        return
    try:
        with open(path) as f:
            old = json.load(f)
    except (OSError, ValueError):
        return
    pids = [n.get("pid") for n in old.get("nodes", [])] + [old.get("gateway", {}).get("pid")]
    stopped = 0
    for pid in filter(None, pids):
        if pid == os.getpid() or not _is_our_process(pid):
            continue
        if sys.platform == "win32":
            subprocess.run(["taskkill", "/F", "/PID", str(pid)], capture_output=True)
        else:
            try:
                os.kill(pid, signal.SIGTERM)
            except OSError:
                continue
        stopped += 1
    if stopped:
        print(f"Stopped {stopped} processes left over from the previous run.")
        time.sleep(1.5)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--shards", type=int, default=2)
    ap.add_argument("--gateway-port", type=int, default=8080)
    ap.add_argument("--clean", action="store_true")
    args = ap.parse_args()

    stop_previous_cluster()
    if args.clean and os.path.isdir(DATA):
        for attempt in range(5):
            try:
                shutil.rmtree(DATA)
                break
            except PermissionError:
                if attempt == 4:
                    sys.exit("Cannot delete the data folder because another program is using it.\n"
                             "Close other VaultMesh windows, or run:  taskkill /F /IM python.exe")
                time.sleep(1)
    os.makedirs(LOGS, exist_ok=True)

    layout = plan(args.shards, args.gateway_port)
    procs: dict[str, subprocess.Popen] = {}
    for node in layout["nodes"]:
        procs[node["id"]] = spawn(node["cmd"], node["id"])
        node["pid"] = procs[node["id"]].pid
    procs["gateway"] = spawn(layout["gateway"]["cmd"], "gateway")
    layout["gateway"]["pid"] = procs["gateway"].pid

    with open(os.path.join(DATA, "cluster.json"), "w") as f:
        json.dump(layout, f, indent=2)

    print(f"Started {len(layout['nodes'])} nodes in {args.shards} shards + gateway.")
    print(f"Dashboard: http://localhost:{args.gateway_port}/   Logs: data/logs/")
    print("Press Ctrl+C to stop.")

    def shutdown(*_):
        print("\nStopping cluster...")
        for p in procs.values():
            if p.poll() is None:
                p.terminate()
        for p in procs.values():
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()
