"""The key-value store that Raft replicates.

Writes are encoded as :class:`Command` bytes, placed in the Raft log, and applied to
:class:`KVStateMachine` on every node in the same order, so all replicas end up identical.
"""

from __future__ import annotations

import json
from dataclasses import dataclass


@dataclass(frozen=True)
class Command:
    op: str            # "put" or "delete"
    key: str
    value: str | None = None

    @staticmethod
    def put(key: str, value: str) -> "Command":
        return Command("put", key, value)

    @staticmethod
    def delete(key: str) -> "Command":
        return Command("delete", key)

    def encode(self) -> bytes:
        return json.dumps([self.op, self.key, self.value], separators=(",", ":")).encode("utf-8")

    @staticmethod
    def decode(data: bytes) -> "Command":
        op, key, value = json.loads(data)
        return Command(op, key, value)


class KVStateMachine:
    """In-memory dict, rebuilt after a restart by replaying the write-ahead log."""

    def __init__(self) -> None:
        self._data: dict[str, str] = {}

    def apply(self, command: bytes) -> str | None:
        """Apply one committed command; returns the key's previous value."""
        cmd = Command.decode(command)
        if cmd.op == "put":
            previous = self._data.get(cmd.key)
            self._data[cmd.key] = cmd.value
            return previous
        if cmd.op == "delete":
            return self._data.pop(cmd.key, None)
        raise ValueError(f"unknown op {cmd.op!r}")

    def get(self, key: str) -> str | None:
        return self._data.get(key)

    def __len__(self) -> int:
        return len(self._data)

    def snapshot(self) -> dict[str, str]:
        return dict(self._data)
