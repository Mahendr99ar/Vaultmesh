"""Consistent hashing ring with virtual nodes.

Each shard is placed on the ring ``vnodes`` times at pseudo-random positions. A key belongs
to the first shard position clockwise from the key's hash. Adding or removing a shard only
moves about 1/N of the keys, instead of nearly all of them as with ``hash(key) % N``.
Lookups are O(log V) with binary search over the sorted positions.
"""

from __future__ import annotations

import bisect
import hashlib


def _hash(value: str) -> int:
    return int.from_bytes(hashlib.md5(value.encode("utf-8")).digest()[:8], "big")


class ConsistentHashRing:
    def __init__(self, nodes: list[str] | None = None, vnodes: int = 128) -> None:
        self.vnodes = vnodes
        self._positions: list[int] = []      # sorted hash positions
        self._owners: dict[int, str] = {}    # position -> shard name
        for n in nodes or []:
            self.add(n)

    def add(self, node: str) -> None:
        for i in range(self.vnodes):
            pos = _hash(f"{node}#{i}")
            if pos in self._owners:
                continue
            self._owners[pos] = node
            bisect.insort(self._positions, pos)

    def remove(self, node: str) -> None:
        for i in range(self.vnodes):
            pos = _hash(f"{node}#{i}")
            if self._owners.get(pos) == node:
                del self._owners[pos]
                self._positions.remove(pos)

    def node_for(self, key: str) -> str:
        if not self._positions:
            raise LookupError("ring is empty")
        i = bisect.bisect_left(self._positions, _hash(key))
        if i == len(self._positions):
            i = 0  # wrap around the ring
        return self._owners[self._positions[i]]

    @property
    def nodes(self) -> set[str]:
        return set(self._owners.values())
