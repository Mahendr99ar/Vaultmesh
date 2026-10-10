"""Durable Raft metadata: ``current_term`` and ``voted_for``.

Raft requires these to survive crashes; otherwise a restarted node could vote twice in the
same term and two leaders could be elected. Updates use the classic atomic-file pattern:
write a temp file, fsync it, then atomically rename it over the old one (``os.replace``).
"""

from __future__ import annotations

import json
import os


class PersistentState:
    def __init__(self, path: str, fsync: bool = True) -> None:
        self._path = path
        self._fsync = fsync
        self.term = 0
        self.voted_for: str | None = None
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            self.term = data["term"]
            self.voted_for = data["voted_for"]

    def update(self, term: int, voted_for: str | None) -> None:
        if term == self.term and voted_for == self.voted_for:
            return
        tmp = self._path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"term": term, "voted_for": voted_for}, f)
            f.flush()
            if self._fsync:
                os.fsync(f.fileno())
        os.replace(tmp, self._path)  # atomic on Linux, macOS and Windows
        self.term = term
        self.voted_for = voted_for
