"""The two Raft RPCs and their replies, serialised as JSON over HTTP."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field

from .wal import LogEntry


@dataclass(frozen=True)
class VoteRequest:
    """Sent by a candidate asking for a vote."""

    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(d: dict) -> "VoteRequest":
        return VoteRequest(**d)


@dataclass(frozen=True)
class VoteResponse:
    term: int
    granted: bool

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(d: dict) -> "VoteResponse":
        return VoteResponse(**d)


@dataclass(frozen=True)
class AppendRequest:
    """Sent by the leader to replicate entries. An empty ``entries`` list is a heartbeat."""

    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: list[LogEntry] = field(default_factory=list)
    leader_commit: int = 0

    def to_dict(self) -> dict:
        return {
            "term": self.term,
            "leader_id": self.leader_id,
            "prev_log_index": self.prev_log_index,
            "prev_log_term": self.prev_log_term,
            "entries": [e.to_dict() for e in self.entries],
            "leader_commit": self.leader_commit,
        }

    @staticmethod
    def from_dict(d: dict) -> "AppendRequest":
        return AppendRequest(
            term=d["term"],
            leader_id=d["leader_id"],
            prev_log_index=d["prev_log_index"],
            prev_log_term=d["prev_log_term"],
            entries=[LogEntry.from_dict(e) for e in d["entries"]],
            leader_commit=d["leader_commit"],
        )


@dataclass(frozen=True)
class AppendResponse:
    """``last_log_index`` is a hint on failure so the leader can back up many entries at once."""

    term: int
    success: bool
    last_log_index: int

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(d: dict) -> "AppendResponse":
        return AppendResponse(**d)
