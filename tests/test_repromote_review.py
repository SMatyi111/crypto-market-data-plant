"""Apply-path guarantees pinned by the PR #63 review.

Every case here is one where the first cut of the tool could have destroyed
curated rows or wedged a run in an unrepairable state.
"""

from __future__ import annotations

import json
import os
import shutil
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.dataset as ds

from crypto_collector.ops import _promoted_rows_by_run
from crypto_collector.promotion import promote_replayable_runs
from crypto_collector.repromote import repromote_short_runs

# Helpers duplicated from tests/test_repromote.py on purpose: the suite runs as
# `pytest -q` on CI with `tests/` as a plain directory, so cross-test-module
# imports depend on the import mode. Keep them in sync by hand.
OLD = datetime(2026, 4, 6, 0, 0, tzinfo=UTC)


def _row(i: int, *, source: str = "binance", product: str = "BTCUSDT") -> dict[str, object]:
    from datetime import timedelta

    return {
        "source": source,
        "event_time": (OLD + timedelta(seconds=i)).isoformat(),
        "received_at": (OLD + timedelta(seconds=i, milliseconds=100)).isoformat(),
        "instrument": {"instrument_id": f"spot:{source}:{product}"},
        "product": product,
        "trade_id": i,
    }


def _write_run(run_dir: Path, rows: list[dict[str, object]], *, replayable: bool = True) -> None:
    (run_dir / "clean").mkdir(parents=True, exist_ok=True)
    (run_dir / "metrics").mkdir(parents=True, exist_ok=True)
    (run_dir / "clean" / "events.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
    (run_dir / "metrics" / "replay_summary.json").write_text(
        json.dumps({"replayable": replayable, "findings": [], "event_count": len(rows)}), encoding="utf-8"
    )


def _append_rows(run_dir: Path, rows: list[dict[str, object]]) -> None:
    with (run_dir / "clean" / "events.jsonl").open("a", encoding="utf-8") as handle:
        for r in rows:
            handle.write(json.dumps(r) + "\n")
    # Keep the summary current, as the collector's segment-close rewrite would.
    _set_summary(run_dir, event_count=len((run_dir / "clean" / "events.jsonl").read_text(encoding="utf-8").splitlines()))


def _curated_rows_for(target_root: Path, run_path: str) -> list[dict[str, object]]:
    dataset = ds.dataset(target_root, format="parquet", partitioning="hive")
    return [r for r in dataset.to_table().to_pylist() if r["source_run_path"] == run_path]


def _truncated_setup(tmp_path: Path, *, lane: str = "binance_trades"):
    source_root = tmp_path / "raw" / "market" / lane
    target_root = tmp_path / "curated" / "trades_replayable"
    run_dir = source_root / OLD.strftime("%Y%m%d_%H%M%S")
    rows = [_row(i) for i in range(10)]
    _write_run(run_dir, rows[:4])
    report = promote_replayable_runs(source_root, target_root, limit=10, max_age_hours=24 * 365 * 100)
    assert report.promoted_row_count == 4
    _append_rows(run_dir, rows[4:])
    return source_root, target_root, run_dir


def _summary_path(run_dir: Path) -> Path:
    return run_dir / "metrics" / "replay_summary.json"


def _set_summary(run_dir: Path, **fields: object) -> None:
    payload = json.loads(_summary_path(run_dir).read_text(encoding="utf-8"))
    payload.update(fields)
    _summary_path(run_dir).write_text(json.dumps(payload), encoding="utf-8")


def test_stale_replay_summary_is_skipped_and_curated_untouched(tmp_path: Path) -> None:
    """A summary whose event_count covers only the prefix was scored on the live
    segment - it says nothing about the tail, so the run is not re-promoted."""
    _, target_root, run_dir = _truncated_setup(tmp_path)
    _set_summary(run_dir, event_count=4)  # raw now has 10
    report = repromote_short_runs(target_root=target_root, apply=True)
    assert report.runs[0].action == "skipped_stale_replay_summary"
    assert report.runs[0].summary_event_count == 4
    assert len(_curated_rows_for(target_root, str(run_dir))) == 4
    # Once re-scored on the full segment the run is repairable.
    _set_summary(run_dir, event_count=10)
    assert repromote_short_runs(target_root=target_root, apply=True).repromoted_count == 1


def test_torn_final_raw_line_is_tolerated(tmp_path: Path) -> None:
    _, target_root, run_dir = _truncated_setup(tmp_path)
    with (run_dir / "clean" / "events.jsonl").open("a", encoding="utf-8") as handle:
        handle.write('{"trade_id": 99, "source": "binance", "event_ti')  # torn tail, no newline
    report = repromote_short_runs(target_root=target_root, apply=True)
    (run,) = report.runs
    assert run.action == "repromote" and run.torn_tail is True
    assert run.raw_rows == 11 and run.new_rows == 10
    assert sorted(r["trade_id"] for r in _curated_rows_for(target_root, str(run_dir))) == list(range(10))


def test_unparseable_middle_raw_line_leaves_curated_untouched(tmp_path: Path) -> None:
    """Raw is validated BEFORE any delete: a corrupt row in the middle fails the run
    with the original 4 curated rows still in place."""
    _, target_root, run_dir = _truncated_setup(tmp_path)
    events = run_dir / "clean" / "events.jsonl"
    lines = events.read_text(encoding="utf-8").splitlines()
    lines[6] = "{not json"
    events.write_text("\n".join(lines) + "\n", encoding="utf-8")
    report = repromote_short_runs(target_root=target_root, apply=True)
    (run,) = report.runs
    assert run.action == "failed" and "unparseable clean row 7" in (run.error or "")
    assert report.status == "error"
    assert len(_curated_rows_for(target_root, str(run_dir))) == 4
    assert len((target_root / "_promotion_index.jsonl").read_text(encoding="utf-8").splitlines()) == 1


def test_interrupted_apply_is_repaired_on_the_next_pass(tmp_path: Path) -> None:
    """Simulate a crash between delete and write: the run is absent from curated
    while the index still says 4. The complete file scan finds nothing to delete
    and the run is simply re-promoted."""
    _, target_root, run_dir = _truncated_setup(tmp_path)
    for part in list(target_root.rglob("*.parquet")):
        os.remove(part)
    assert _curated_rows_for(target_root, str(run_dir)) == [] if list(target_root.rglob("*.parquet")) else True
    report = repromote_short_runs(target_root=target_root, apply=True)
    (run,) = report.runs
    assert run.action == "repromote" and run.curated_rows == 0 and run.index_mismatch is True
    assert len(_curated_rows_for(target_root, str(run_dir))) == 10


def test_partially_deleted_run_is_repaired_on_the_next_pass(tmp_path: Path) -> None:
    """Same, but one of several part-files survived the interruption: the leftover
    is found by the full scan and replaced together with the rest."""
    source_root = tmp_path / "raw" / "market" / "binance_trades"
    target_root = tmp_path / "curated" / "trades_replayable"
    run_dir = source_root / "20260406_000000"
    rows = [_row(i, product="BTCUSDT") for i in range(4)] + [_row(i, product="ETHUSDT") for i in range(4, 8)]
    _write_run(run_dir, rows[:6])  # 4 BTC + 2 ETH -> two partitions -> two part-files
    from crypto_collector.promotion import promote_replayable_runs

    promote_replayable_runs(source_root, target_root, limit=10, max_age_hours=24 * 365 * 100)
    _append_rows(run_dir, rows[6:])
    parts = sorted(target_root.rglob("*.parquet"))
    assert len(parts) == 2
    os.remove(parts[0])  # interrupted after the first delete
    report = repromote_short_runs(target_root=target_root, apply=True)
    (run,) = report.runs
    assert run.action == "repromote" and len(run.curated_files) == 1
    assert sorted(r["trade_id"] for r in _curated_rows_for(target_root, str(run_dir))) == list(range(8))


def test_remove_not_replayable_is_idempotent(tmp_path: Path) -> None:
    _, target_root, run_dir = _truncated_setup(tmp_path)
    _set_summary(run_dir, replayable=False, event_count=10)
    first = repromote_short_runs(target_root=target_root, apply=True)
    assert first.removed_count == 1
    second = repromote_short_runs(target_root=target_root, apply=True)
    assert second.candidate_count == 0 and second.removed_count == 0
    dry = repromote_short_runs(target_root=target_root)
    assert dry.candidate_count == 0
    assert len((target_root / "_promotion_index.jsonl").read_text(encoding="utf-8").splitlines()) == 2


def test_hot_skeleton_without_events_falls_back_to_cold(tmp_path: Path) -> None:
    _, target_root, run_dir = _truncated_setup(tmp_path)
    cold_root = tmp_path / "cold" / "raw" / "market"
    cold_run = cold_root / run_dir.parent.name / run_dir.name
    cold_run.parent.mkdir(parents=True)
    shutil.copytree(run_dir, cold_run)
    (run_dir / "clean" / "events.jsonl").unlink()  # offload interrupted mid-rmtree
    no_cold = repromote_short_runs(target_root=target_root)
    assert no_cold.runs and no_cold.runs[0].action == "skipped_raw_missing"
    report = repromote_short_runs(target_root=target_root, cold_root=cold_root, apply=True)
    assert report.repromoted_count == 1
    assert len(_curated_rows_for(target_root, str(run_dir))) == 10


def test_health_index_reader_keeps_latest_row_per_run(tmp_path: Path) -> None:
    index = tmp_path / "_promotion_index.jsonl"
    run_a = str(tmp_path / "raw" / "market" / "binance_trades" / "20260406_000000")
    run_b = str(tmp_path / "raw" / "market" / "binance_trades" / "20260406_003000")
    other = str(tmp_path / "raw" / "market" / "okx_trades" / "20260406_000000")
    lines = [
        {"run_path": run_a, "promoted_at": "2026-04-06T00:35:00+00:00", "promoted_rows": 4},
        {"run_path": run_a, "promoted_at": "2026-09-08T12:00:00+00:00", "promoted_rows": 10, "repromoted": True},
        {"run_path": run_b, "promoted_at": "2026-04-06T01:05:00+00:00", "promoted_rows": 7},
        {"run_path": run_b, "promoted_at": "2026-09-08T12:00:01+00:00", "promoted_rows": 0, "removed_not_replayable": True},
        {"run_path": other, "promoted_at": "2026-04-06T01:05:00+00:00", "promoted_rows": 3},
    ]
    index.write_text("".join(json.dumps(row) + "\n" for row in lines), encoding="utf-8")
    assert _promoted_rows_by_run(index) == {run_a: 10}


def test_limit_caps_candidates_before_raw_scanning(tmp_path: Path) -> None:
    _, target_root, run_dir = _truncated_setup(tmp_path)
    source_root = run_dir.parent
    second = source_root / "20260406_003000"
    rows = [_row(i) for i in range(10)]
    _write_run(second, rows[:3])
    from crypto_collector.promotion import promote_replayable_runs

    promote_replayable_runs(source_root, target_root, limit=10, max_age_hours=24 * 365 * 100)
    _append_rows(second, rows[3:])
    report = repromote_short_runs(target_root=target_root, limit=1, now=datetime.now(tz=UTC))
    assert report.candidate_count == 1
    assert report.index_runs == 2


def test_raw_offloaded_between_scan_and_apply_is_re_resolved(tmp_path: Path, monkeypatch) -> None:
    """Live 2026-09-08: the offload job moved a candidate to the cold tier during the
    13-minute dataset scan; the run must be repaired from the cold copy, not skipped."""
    from crypto_collector import repromote as mod

    _, target_root, run_dir = _truncated_setup(tmp_path)
    cold_root = tmp_path / "cold" / "raw" / "market"
    cold_run = cold_root / run_dir.parent.name / run_dir.name
    real_scan = mod.build_run_file_map

    def scan_then_offload(root: Path):
        result = real_scan(root)
        cold_run.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(run_dir), str(cold_run))  # offload lands mid-repair
        return result

    monkeypatch.setattr(mod, "build_run_file_map", scan_then_offload)
    report = repromote_short_runs(target_root=target_root, cold_root=cold_root, apply=True)
    (run,) = report.runs
    assert run.action == "repromote" and run.raw_dir == str(cold_run)
    assert len(_curated_rows_for(target_root, str(run_dir))) == 10


def test_raw_moved_after_summary_read_fails_closed_with_curated_intact(tmp_path: Path, monkeypatch) -> None:
    """If the move lands after the summary read, parsing raw fails and the run is
    reported failed - with the original curated part-files still in place."""
    from crypto_collector import repromote as mod

    _, target_root, run_dir = _truncated_setup(tmp_path)
    cold_root = tmp_path / "cold" / "raw" / "market"
    real_read = mod.read_clean_rows

    def move_then_read(path: Path):
        cold_run = cold_root / run_dir.parent.name / run_dir.name
        cold_run.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(run_dir), str(cold_run))
        return real_read(path)  # hot path -> FileNotFoundError

    monkeypatch.setattr(mod, "read_clean_rows", move_then_read)
    report = repromote_short_runs(target_root=target_root, cold_root=cold_root, apply=True)
    (run,) = report.runs
    assert run.action == "failed" and report.status == "error"
    assert len(_curated_rows_for(target_root, str(run_dir))) == 4
    assert len((target_root / "_promotion_index.jsonl").read_text(encoding="utf-8").splitlines()) == 1


def test_raw_moved_mid_scan_without_cold_root_is_reported_as_raw_missing(tmp_path: Path, monkeypatch) -> None:
    from crypto_collector import repromote as mod

    _, target_root, run_dir = _truncated_setup(tmp_path)
    real_scan = mod.build_run_file_map

    def scan_then_offload(root: Path):
        result = real_scan(root)
        shutil.move(str(run_dir), str(tmp_path / "elsewhere"))
        return result

    monkeypatch.setattr(mod, "build_run_file_map", scan_then_offload)
    report = repromote_short_runs(target_root=target_root, apply=True)
    (run,) = report.runs
    assert run.action == "skipped_raw_missing"  # not the misleading missing-summary label
    assert len(_curated_rows_for(target_root, str(run_dir))) == 4
