"""Crash-safe, append-only write-ahead log (WAL) for Raft entries.

On-disk record format (big-endian):

    [length: 4 bytes][crc32: 4 bytes][payload: length bytes]
    payload = [term: 8 bytes][command length: 4 bytes][command bytes]

Every append is followed by ``os.fsync`` before the node acknowledges it, so an entry a node
has said "yes" to survives a crash or power cut. On startup the file is scanned; a half-written
("torn") record at the end, detected by a bad length or CRC, is cut off.

Entries are also cached in memory so reads never touch the disk.
"""

from __future__ import annotations

import base64
import os
import struct
import zlib
from dataclasses import dataclass

_HEADER = struct.Struct(">II")  # length, crc32
_ENTRY = struct.Struct(">QI")   # term, command length


@dataclass(frozen=True)
class LogEntry:
    """One entry of the replicated log. Its index is its position in the log (1-based)."""

    term: int
    command: bytes = b""  # empty command = no-op

    def encode(self) -> bytes:
        return _ENTRY.pack(self.term, len(self.command)) + self.command

    @staticmethod
    def decode(payload: bytes) -> "LogEntry":
        term, length = _ENTRY.unpack_from(payload, 0)
        start = _ENTRY.size
        return LogEntry(term, bytes(payload[start:start + length]))

    def to_dict(self) -> dict:
        return {"term": self.term, "command": base64.b64encode(self.command).decode("ascii")}

    @staticmethod
    def from_dict(d: dict) -> "LogEntry":
        return LogEntry(d["term"], base64.b64decode(d["command"]))


class WriteAheadLog:
    """Not thread-safe; the owning RaftNode runs on a single asyncio event loop."""

    def __init__(self, path: str, fsync: bool = True) -> None:
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_BINARY", 0)  # O_BINARY matters on Windows
        self._fd = os.open(path, flags, 0o644)
        self._fsync = fsync
        self._entries: list[LogEntry] = []  # _entries[i] has index i + 1
        self._offsets: list[int] = []       # byte offset of each record in the file
        self._end = 0
        self.recovered_from_torn_write = False
        self._recover()

    # ------------------------------------------------------------------ recovery
    def _recover(self) -> None:
        size = os.fstat(self._fd).st_size
        os.lseek(self._fd, 0, os.SEEK_SET)
        data = self._read_exact(size)
        pos = 0
        while pos + _HEADER.size <= size:
            length, crc = _HEADER.unpack_from(data, pos)
            body_start = pos + _HEADER.size
            if length < _ENTRY.size or body_start + length > size:
                break  # torn header or body
            payload = data[body_start:body_start + length]
            if zlib.crc32(payload) != crc:
                break  # corrupted record
            self._offsets.append(pos)
            self._entries.append(LogEntry.decode(payload))
            pos = body_start + length
        if pos < size:
            os.ftruncate(self._fd, pos)
            os.fsync(self._fd)
            self.recovered_from_torn_write = True
        self._end = pos

    def _read_exact(self, n: int) -> bytes:
        chunks, remaining = [], n
        while remaining > 0:
            chunk = os.read(self._fd, remaining)
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    # ------------------------------------------------------------------ writes
    def append(self, entries: list[LogEntry]) -> int:
        """Append a batch with ONE write + ONE fsync (group commit). Returns the new last index."""
        if not entries:
            return self.last_index()
        buf = bytearray()
        offset = self._end
        for entry in entries:
            payload = entry.encode()
            self._offsets.append(offset)
            buf += _HEADER.pack(len(payload), zlib.crc32(payload)) + payload
            offset += _HEADER.size + len(payload)
        os.lseek(self._fd, self._end, os.SEEK_SET)
        view = memoryview(buf)
        while view:
            written = os.write(self._fd, view)
            view = view[written:]
        if self._fsync:
            os.fsync(self._fd)
        self._entries.extend(entries)
        self._end = offset
        return self.last_index()

    def truncate_from(self, index: int) -> None:
        """Delete every entry with index >= ``index`` (a follower dropping a conflicting suffix)."""
        if index < 1:
            raise ValueError("index must be >= 1")
        if index > self.last_index():
            return
        i = index - 1
        cut = self._offsets[i]
        os.ftruncate(self._fd, cut)
        if self._fsync:
            os.fsync(self._fd)
        del self._entries[i:]
        del self._offsets[i:]
        self._end = cut

    # ------------------------------------------------------------------ reads
    def last_index(self) -> int:
        return len(self._entries)

    def last_term(self) -> int:
        return self.term_at(self.last_index())

    def term_at(self, index: int) -> int:
        """Term of the entry at ``index``; index 0 is a virtual sentinel with term 0."""
        return 0 if index == 0 else self._entries[index - 1].term

    def get(self, index: int) -> LogEntry:
        return self._entries[index - 1]

    def slice(self, from_index: int, max_entries: int) -> list[LogEntry]:
        return self._entries[from_index - 1:from_index - 1 + max_entries]

    def close(self) -> None:
        os.close(self._fd)
