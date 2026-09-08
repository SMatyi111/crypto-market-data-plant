"""Size-based rotation of heartbeat_history.jsonl (ROADMAP item 12).

The runner appends the full heartbeat (~27 KB) every 30 s; the live file reached
5.8 GB on the SSD. Rotation must be bounded, best-effort (never take the runner
down), and must never touch the live file's name pattern from the wrong side.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from crypto_collector import ops
from crypto_collector.ops import OpsRunner, rotate_jsonl_if_oversized


def _fill(path: Path, size: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * size)


def test_rotation_is_a_noop_below_the_cap(tmp_path: Path) -> None:
    live = tmp_path / "heartbeat_history.jsonl"
    _fill(live, 10)
    assert rotate_jsonl_if_oversized(live, max_bytes=100, keep=3) is None
    assert live.exists()
    assert sorted(tmp_path.iterdir()) == [live]


def test_rotation_is_a_noop_when_file_missing(tmp_path: Path) -> None:
    assert rotate_jsonl_if_oversized(tmp_path / "heartbeat_history.jsonl", max_bytes=1, keep=1) is None


def test_rotation_renames_with_stamp_and_prunes_oldest(tmp_path: Path) -> None:
    live = tmp_path / "heartbeat_history.jsonl"
    # Two pre-existing rotated files, older than anything we will create.
    _fill(tmp_path / "heartbeat_history.20260101_000000.jsonl", 5)
    _fill(tmp_path / "heartbeat_history.20260201_000000.jsonl", 5)
    _fill(live, 200)

    rotated = rotate_jsonl_if_oversized(
        live, max_bytes=100, keep=2, now=datetime(2026, 9, 8, 12, 0, 0, tzinfo=UTC)
    )

    assert rotated == tmp_path / "heartbeat_history.20260908_120000.jsonl"
    assert rotated.exists() and rotated.stat().st_size == 200
    assert not live.exists()  # the next write recreates it (JsonlSink appends)
    remaining = sorted(p.name for p in tmp_path.iterdir())
    # keep=2: the newest two rotated files survive, the 2026-01 one is pruned.
    assert remaining == [
        "heartbeat_history.20260201_000000.jsonl",
        "heartbeat_history.20260908_120000.jsonl",
    ]


def test_rotation_never_collides_within_one_second(tmp_path: Path) -> None:
    live = tmp_path / "heartbeat_history.jsonl"
    stamp = datetime(2026, 9, 8, 12, 0, 0, tzinfo=UTC)
    _fill(live, 200)
    first = rotate_jsonl_if_oversized(live, max_bytes=100, keep=5, now=stamp)
    _fill(live, 200)
    second = rotate_jsonl_if_oversized(live, max_bytes=100, keep=5, now=stamp)
    assert first != second
    assert first is not None and second is not None
    assert first.exists() and second.exists()


def test_prune_glob_ignores_unrelated_and_live_files(tmp_path: Path) -> None:
    live = tmp_path / "heartbeat_history.jsonl"
    unrelated = tmp_path / "job_runs.jsonl"
    _fill(unrelated, 5)
    _fill(live, 200)
    rotate_jsonl_if_oversized(live, max_bytes=100, keep=0)
    # keep=0 prunes every rotated file but must leave unrelated logs alone.
    assert unrelated.exists()
    assert [p.name for p in tmp_path.iterdir()] == ["job_runs.jsonl"]


def test_rotation_failure_is_swallowed(tmp_path: Path, monkeypatch) -> None:
    live = tmp_path / "heartbeat_history.jsonl"
    _fill(live, 200)

    def boom(self: Path, target: Path) -> Path:  # noqa: ARG001
        raise OSError("held by another process")

    monkeypatch.setattr(Path, "rename", boom)
    assert rotate_jsonl_if_oversized(live, max_bytes=100, keep=3) is None
    assert live.exists()


def test_runner_heartbeat_rotates_oversized_history_before_appending(tmp_path: Path, monkeypatch) -> None:
    """End to end through _emit_heartbeat: an oversized history is rotated and the
    new heartbeat lands as the first row of a fresh file."""
    ops_root = tmp_path / "ops"
    runner = OpsRunner(ops_root, runner_name="t", collector_concurrency=1)
    monkeypatch.setattr(ops, "HEARTBEAT_HISTORY_MAX_BYTES", 100)
    monkeypatch.setattr(ops, "HEARTBEAT_HISTORY_KEEP", 2)
    live = ops_root / "heartbeat_history.jsonl"
    live.write_bytes(b"y" * 500)

    runner._emit_heartbeat({"status": "running", "last_seen": datetime.now(tz=UTC).isoformat()})

    rotated = sorted(ops_root.glob("heartbeat_history.*.jsonl"))
    assert len(rotated) == 1 and rotated[0].stat().st_size == 500
    rows = [json.loads(line) for line in live.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(rows) == 1 and rows[0]["status"] == "running"
    assert json.loads((ops_root / "heartbeat.json").read_text(encoding="utf-8"))["status"] == "running"
