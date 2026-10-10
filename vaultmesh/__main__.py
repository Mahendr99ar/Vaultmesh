"""Command-line entry point.

Start a storage node:
    python -m vaultmesh node --id s1n1 --port 9101 --data data/s1n1 \
        --peers s1n2=localhost:9102,s1n3=localhost:9103

Start the gateway:
    python -m vaultmesh gateway --port 8080 \
        --shards "shard1=localhost:9101,localhost:9102,localhost:9103;shard2=localhost:9201,localhost:9202,localhost:9203"

Run everything in one process (small free hosts such as Render):
    python -m vaultmesh demo --port 8080      # port defaults to $PORT, then 8080

Run the benchmark:
    python -m vaultmesh bench --target http://localhost:8080 --ops 5000 --concurrency 64
"""

from __future__ import annotations

import argparse
import logging
import os

import uvicorn


def _parse_peers(text: str) -> dict[str, str]:
    peers = {}
    for item in filter(None, (p.strip() for p in text.split(","))):
        node_id, addr = item.split("=", 1)
        peers[node_id.strip()] = addr.strip()
    return peers


def _parse_shards(text: str) -> dict[str, list[str]]:
    shards = {}
    for item in filter(None, (s.strip() for s in text.split(";"))):
        name, nodes = item.split("=", 1)
        shards[name.strip()] = [n.strip() for n in nodes.split(",") if n.strip()]
    return shards


def main() -> None:
    parser = argparse.ArgumentParser(prog="vaultmesh", description="Sharded Raft key-value store")
    sub = parser.add_subparsers(dest="cmd", required=True)

    node = sub.add_parser("node", help="run one storage node")
    node.add_argument("--id", required=True)
    node.add_argument("--host", default="0.0.0.0")
    node.add_argument("--port", type=int, required=True)
    node.add_argument("--data", required=True, help="directory for the WAL and Raft metadata")
    node.add_argument("--peers", default="", help="comma list of id=host:port for the OTHER nodes")
    node.add_argument("--no-fsync", action="store_true", help="faster but unsafe; for demos only")

    gw = sub.add_parser("gateway", help="run the routing gateway")
    gw.add_argument("--host", default="0.0.0.0")
    gw.add_argument("--port", type=int, default=8080)
    gw.add_argument("--shards", required=True, help='"shard1=h:p,h:p,h:p;shard2=..."')

    demo = sub.add_parser("demo", help="run all shards and the gateway in one process")
    demo.add_argument("--port", type=int, default=int(os.environ.get("PORT", "8080")))
    demo.add_argument("--shards", type=int, default=2)
    demo.add_argument("--data", default="data/demo")

    bench = sub.add_parser("bench", help="load-test a running cluster")
    bench.add_argument("--target", default="http://localhost:8080")
    bench.add_argument("--ops", type=int, default=5000)
    bench.add_argument("--concurrency", type=int, default=64)
    bench.add_argument("--value-size", type=int, default=100)

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    # Never log per-request lines: with heartbeats every 75 ms that floods the disk.
    logging.getLogger("aiohttp.access").setLevel(logging.WARNING)

    if args.cmd == "node":
        from .kv import KVStateMachine
        from .node_server import create_node_app
        from .raft import RaftConfig, RaftNode
        from .transport import HttpTransport

        peers = _parse_peers(args.peers)
        kv = KVStateMachine()
        raft = RaftNode(args.id, list(peers), HttpTransport(peers), args.data, kv,
                        RaftConfig(fsync=not args.no_fsync))
        app = create_node_app(raft, kv, peers)
        uvicorn.run(app, host=args.host, port=args.port, log_level="warning")

    elif args.cmd == "gateway":
        from .gateway import create_gateway_app

        app = create_gateway_app(_parse_shards(args.shards))
        uvicorn.run(app, host=args.host, port=args.port, log_level="warning")

    elif args.cmd == "demo":
        from .demo import main as demo_main

        demo_main(args.port, args.shards, args.data)

    elif args.cmd == "bench":
        from .bench import main as bench_main

        bench_main(args.target, args.ops, args.concurrency, args.value_size)


if __name__ == "__main__":
    main()
