from vaultmesh.state import PersistentState
from vaultmesh.wal import LogEntry, WriteAheadLog


def test_append_and_reload(tmp_path):
    path = str(tmp_path / "raft.wal")
    wal = WriteAheadLog(path)
    wal.append([LogEntry(1, b"a"), LogEntry(1, b"b")])
    wal.append([LogEntry(2, b"c")])
    wal.close()

    reopened = WriteAheadLog(path)
    assert reopened.last_index() == 3
    assert reopened.last_term() == 2
    assert [reopened.get(i).command for i in (1, 2, 3)] == [b"a", b"b", b"c"]
    assert reopened.term_at(0) == 0
    reopened.close()


def test_torn_write_is_truncated_on_recovery(tmp_path):
    """Simulates a crash in the middle of writing a record."""
    path = str(tmp_path / "raft.wal")
    wal = WriteAheadLog(path)
    wal.append([LogEntry(1, b"committed-1"), LogEntry(1, b"committed-2")])
    wal.close()
    with open(path, "ab") as f:
        f.write(b"\x00\x00\x00\x40\xde\xad")  # header of a record whose body never made it to disk

    recovered = WriteAheadLog(path)
    assert recovered.recovered_from_torn_write
    assert recovered.last_index() == 2
    recovered.append([LogEntry(2, b"after-crash")])  # the log stays usable
    recovered.close()
    assert WriteAheadLog(path).last_index() == 3


def test_corrupted_record_detected_by_crc(tmp_path):
    path = str(tmp_path / "raft.wal")
    wal = WriteAheadLog(path)
    wal.append([LogEntry(1, b"good"), LogEntry(1, b"will-be-corrupted")])
    wal.close()
    with open(path, "r+b") as f:
        f.seek(-3, 2)
        f.write(b"XYZ")  # flip bytes inside the last record
    recovered = WriteAheadLog(path)
    assert recovered.last_index() == 1
    assert recovered.get(1).command == b"good"


def test_truncate_from_removes_suffix_durably(tmp_path):
    path = str(tmp_path / "raft.wal")
    wal = WriteAheadLog(path)
    wal.append([LogEntry(1, b"1"), LogEntry(1, b"2"), LogEntry(2, b"3"), LogEntry(2, b"4")])
    wal.truncate_from(3)
    assert wal.last_index() == 2
    wal.append([LogEntry(3, b"new3")])
    wal.close()
    reopened = WriteAheadLog(path)
    assert [reopened.get(i).command for i in (1, 2, 3)] == [b"1", b"2", b"new3"]
    assert reopened.term_at(3) == 3


def test_persistent_state_survives_restart(tmp_path):
    path = str(tmp_path / "raft.meta")
    s = PersistentState(path)
    s.update(7, "n2")
    again = PersistentState(path)
    assert (again.term, again.voted_for) == (7, "n2")
