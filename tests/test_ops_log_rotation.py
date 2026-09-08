"""Size-based rotation + retention for heartbeat_history.jsonl (ROADMAP item 12).

The runner appends the full heartbeat (~27 KB) every 30 s; the live file reached
5.8 GB on the SSD. The policy rides on RotatingJsonlSink (which already owns the
handle lifecycle around a roll) extended with `max_files` retention and a
non-fatal `on_rotate_error="warn"` mode. Collectors keep the old defaults:
no pruning, fail-loud roll.
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path

import pytest

from crypto_collector import ops
from crypto_collector.ops import OpsRunner
from crypto_collector.storage import RotatingJsonlSink


def _row(i: int) -> dict[str, object]:
    return {"i": i, "payload": "abcdefghij"}


def test_defaults_keep_every_part_and_raise_on_failed_roll(tmp_path: Path) -> None:
    sink = RotatingJsonlSink(tmp_path, "messages.jsonl", max_bytes=40)
    for i in range(12):
        sink.write(_row(i))
    parts = sorted(p.name for p in tmp_path.glob("messages.*.jsonl"))
    assert len(parts) >= 5  # nothing pruned without max_files
    assert sink.max_files is None


def test_max_files_prunes_oldest_parts_by_index_not_name(tmp_path: Path) -> None:
    # Pre-existing parts 1..10 plus foreign files that must never be touched.
    for n in range(1, 11):
        (tmp_path / f"messages.{n}.jsonl").write_text("{}\n", encoding="utf-8")
    foreign = [tmp_path / "messages.archive.jsonl", tmp_path / "messages.00_backup.jsonl", tmp_path / "other.1.jsonl"]
    for path in foreign:
        path.write_text("{}\n", encoding="utf-8")

    sink = RotatingJsonlSink(tmp_path, "messages.jsonl", max_bytes=40, max_files=3)
    for i in range(3):  # ~35 bytes each -> rolls before the 2nd and 3rd write -> parts 11, 12
        sink.write(_row(i))

    numbered = sorted(int(p.name.split(".")[1]) for p in tmp_path.glob("messages.*.jsonl") if p.name.split(".")[1].isdigit())
    # Newest three by INDEX survive (10, 11, 12) - a plain name sort ("1" < "10" < "2"
    # < ... < "9") would have kept 7, 8, 9 and deleted the newest parts.
    assert numbered == [10, 11, 12]
    assert all(path.exists() for path in foreign)
    assert (tmp_path / "messages.jsonl").exists()


def test_next_part_index_ignores_foreign_and_unicode_digit_names(tmp_path: Path) -> None:
    (tmp_path / "messages.4.jsonl").write_text("{}\n", encoding="utf-8")
    (tmp_path / "messages.².jsonl").write_text("{}\n", encoding="utf-8")  # superscript two: isdigit() but not int()-able
    (tmp_path / "messages.archive.jsonl").write_text("{}\n", encoding="utf-8")
    sink = RotatingJsonlSink(tmp_path, "messages.jsonl", max_bytes=10)
    sink.write(_row(1))
    sink.write(_row(2))
    assert (tmp_path / "messages.5.jsonl").exists()


def test_warn_mode_defers_a_blocked_roll_and_retries_after_backoff(tmp_path: Path, monkeypatch, caplog) -> None:
    clock = {"t": 1000.0}
    sink = RotatingJsonlSink(
        tmp_path,
        "messages.jsonl",
        max_bytes=40,
        max_files=2,
        on_rotate_error="warn",
        rotate_retry_seconds=600.0,
        time_fn=lambda: clock["t"],
    )
    sink.write(_row(0))
    real_replace = os.replace
    calls = {"n": 0}

    def blocked(src, dst):  # noqa: ANN001
        calls["n"] += 1
        raise PermissionError("held by another process")

    monkeypatch.setattr(os, "replace", blocked)
    with caplog.at_level("WARNING"):
        sink.write(_row(1))  # exceeds cap -> roll attempted -> blocked -> deferred
        sink.write(_row(2))  # inside the backoff window -> no second attempt
    assert calls["n"] == 1
    assert "deferred" in caplog.text and "Traceback" not in caplog.text
    # Rows kept flowing into the active file.
    lines = (tmp_path / "messages.jsonl").read_text(encoding="utf-8").splitlines()
    assert [json.loads(line)["i"] for line in lines] == [0, 1, 2]

    monkeypatch.setattr(os, "replace", real_replace)
    clock["t"] += 601.0
    sink.write(_row(3))  # backoff elapsed -> roll succeeds
    assert (tmp_path / "messages.1.jsonl").exists()
    assert [json.loads(line)["i"] for line in (tmp_path / "messages.jsonl").read_text(encoding="utf-8").splitlines()] == [3]


def test_raise_mode_propagates_a_blocked_roll(tmp_path: Path, monkeypatch) -> None:
    sink = RotatingJsonlSink(tmp_path, "messages.jsonl", max_bytes=40)
    sink.write(_row(0))

    def blocked(src, dst):  # noqa: ANN001
        raise PermissionError("held")

    monkeypatch.setattr(os, "replace", blocked)
    with pytest.raises(PermissionError):
        sink.write(_row(1))


def test_invalid_on_rotate_error_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        RotatingJsonlSink(tmp_path, "messages.jsonl", on_rotate_error="ignore")


def test_roll_works_with_a_batched_open_handle(tmp_path: Path) -> None:
    """The sink closes its handle before the rename, so rotation does not depend on
    the reopen-per-write posture (the review's Windows/POSIX divergence case)."""
    sink = RotatingJsonlSink(tmp_path, "messages.jsonl", max_bytes=40, max_files=2, fsync_interval_events=50)
    for i in range(6):
        sink.write(_row(i))
    sink.close()
    parts = sorted(p.name for p in tmp_path.glob("messages.*.jsonl"))
    assert 1 <= len(parts) <= 2
    assert (tmp_path / "messages.jsonl").exists()


def test_runner_history_rotates_oversized_file_into_part_1(tmp_path: Path, monkeypatch) -> None:
    """End to end: an existing oversized heartbeat_history.jsonl (the live 5.8 GB
    case) becomes part 1 on the first heartbeat and the new row starts a fresh file."""
    monkeypatch.setattr(ops, "HEARTBEAT_HISTORY_MAX_BYTES", 100)
    monkeypatch.setattr(ops, "HEARTBEAT_HISTORY_KEEP", 2)
    ops_root = tmp_path / "ops"
    ops_root.mkdir()
    live = ops_root / "heartbeat_history.jsonl"
    live.write_bytes(b"y" * 500)

    runner = OpsRunner(ops_root, runner_name="t", collector_concurrency=1)
    assert runner.heartbeat_history.max_files == 2
    runner._emit_heartbeat({"status": "running", "last_seen": datetime.now(tz=UTC).isoformat()})

    part = ops_root / "heartbeat_history.1.jsonl"
    assert part.exists() and part.stat().st_size == 500
    rows = [json.loads(line) for line in live.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(rows) == 1 and rows[0]["status"] == "running"
    assert json.loads((ops_root / "heartbeat.json").read_text(encoding="utf-8"))["status"] == "running"
