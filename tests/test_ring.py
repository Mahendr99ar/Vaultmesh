from collections import Counter

from vaultmesh.ring import ConsistentHashRing

KEYS = [f"user:{i}" for i in range(20_000)]


def test_same_key_always_maps_to_same_shard():
    ring = ConsistentHashRing(["shard1", "shard2", "shard3"])
    assert all(ring.node_for(k) == ring.node_for(k) for k in KEYS[:500])


def test_keys_are_spread_evenly():
    ring = ConsistentHashRing(["shard1", "shard2", "shard3", "shard4"], vnodes=128)
    counts = Counter(ring.node_for(k) for k in KEYS)
    expected = len(KEYS) / 4
    for shard, n in counts.items():
        assert abs(n - expected) / expected < 0.15, (shard, n)


def test_adding_a_shard_moves_only_about_one_nth_of_keys():
    ring = ConsistentHashRing(["shard1", "shard2", "shard3"])
    before = {k: ring.node_for(k) for k in KEYS}
    ring.add("shard4")
    moved = [k for k in KEYS if ring.node_for(k) != before[k]]
    # Ideal is 1/4 = 25%. Modulo hashing would move about 75%.
    assert 0.15 < len(moved) / len(KEYS) < 0.35
    assert all(ring.node_for(k) == "shard4" for k in moved)  # keys only move TO the new shard


def test_remove_shard():
    ring = ConsistentHashRing(["a", "b", "c"])
    ring.remove("b")
    assert ring.nodes == {"a", "c"}
    assert {ring.node_for(k) for k in KEYS[:2000]} == {"a", "c"}
