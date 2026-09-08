"""repromote-short-runs: repair curated runs promoted from a partial segment.

Fixtures reproduce the real failure shape: a run is promoted while its raw
`clean/events.jsonl` is still growing, so the promotion index records fewer rows
than raw ends up holding. The tool must find such runs, replace exactly their
curated part-files, and append a superseding index row - and refuse to touch
anything it cannot attribute cleanly.
"""

from __future__ import annotations

import json
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pyarrow.dataset as ds

from crypto_collector.cli import run_repromote_short_runs
from crypto_collector.promotion import promote_replayable_runs
from crypto_collector.repromote import build_run_file_map, latest_index_rows, repromote_short_runs
from crypto_collector.research_manifest import read_promotion_index_with_stats
from crypto_collector.storage import ParquetDatasetSink

OLD = datetime(2026, 4, 6, 0, 0, tzinfo=UTC)


def _row(i: int, *, source: str = "binance", product: str = "BTCUSDT") -> dict[str, object]:
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
    (run_dir / "clean" / "events.jsonl").write_text(
        "".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8"
    )
    (run_dir / "metrics" / "replay_summary.json").write_text(
        json.dumps({"replayable": replayable, "findings": []}), encoding="utf-8"
    )


def _append_rows(run_dir: Path, rows: list[dict[str, object]]) -> None:
    with (run_dir / "clean" / "events.jsonl").open("a", encoding="utf-8") as handle:
        for r in rows:
            handle.write(json.dumps(r) + "\n")


def _curated_rows_for(target_root: Path, run_path: str) -> list[dict[str, object]]:
    dataset = ds.dataset(target_root, format="parquet", partitioning="hive")
    return [r for r in dataset.to_table().to_pylist() if r["source_run_path"] == run_path]


def _truncated_setup(tmp_path: Path, *, lane: str = "binance_trades", run_name: str = OLD.strftime("%Y%m%d_%H%M%S")):
    """Promote a run while it holds 4 rows, then let raw grow to 10: the index says
    4, raw says 10 - the live defect in miniature."""
    source_root = tmp_path / "raw" / "market" / lane
    target_root = tmp_path / "curated" / "trades_replayable"
    run_dir = source_root / run_name
    rows = [_row(i) for i in range(10)]
    _write_run(run_dir, rows[:4])
    report = promote_replayable_runs(source_root, target_root, limit=10, max_age_hours=24 * 365 * 100)
    assert report.promoted_row_count == 4
    _append_rows(run_dir, rows[4:])
    return source_root, target_root, run_dir


def test_dry_run_reports_short_run_and_changes_nothing(tmp_path: Path) -> None:
    source_root, target_root, run_dir = _truncated_setup(tmp_path)
    before = sorted(p.name for p in target_root.rglob("*.parquet"))

    report = repromote_short_runs(target_root=target_root)

    assert report.mode == "dry_run"
    assert report.candidate_count == 1
    (run,) = report.runs
    assert run.action == "would_repromote"
    assert run.promoted_rows == 4 and run.raw_rows == 10 and run.curated_rows == 4
    assert sorted(p.name for p in target_root.rglob("*.parquet")) == before
    assert len((target_root / "_promotion_index.jsonl").read_text(encoding="utf-8").splitlines()) == 1


def test_apply_replaces_rows_and_appends_superseding_index_row(tmp_path: Path) -> None:
    source_root, target_root, run_dir = _truncated_setup(tmp_path)
    run_path = str(run_dir)

    report = repromote_short_runs(target_root=target_root, apply=True)

    assert report.status == "ok"
    assert report.repromoted_count == 1 and report.rows_removed == 4 and report.rows_written == 10
    curated = _curated_rows_for(target_root, run_path)
    assert sorted(r["trade_id"] for r in curated) == list(range(10))
    assert all(r["promotion_tag"] == "replayable" for r in curated)
    # Index: the original row stays (append-only, shared with the live promoter);
    # the new row supersedes it for every latest-wins reader.
    index_lines = [json.loads(line) for line in (target_root / "_promotion_index.jsonl").read_text(encoding="utf-8").splitlines()]
    assert len(index_lines) == 2
    assert index_lines[1]["repromoted"] is True
    assert index_lines[1]["promoted_rows"] == 10 and index_lines[1]["previous_promoted_rows"] == 4
    assert latest_index_rows(target_root / "_promotion_index.jsonl")[run_path]["promoted_rows"] == 10
    by_day, stats = read_promotion_index_with_stats(target_root / "_promotion_index.jsonl")
    assert stats["deduped_promoted_run_count"] == 1
    assert sum(day["rows"] for day in by_day.values()) == 10
    # Idempotent: a second pass finds nothing short.
    again = repromote_short_runs(target_root=target_root, apply=True)
    assert again.candidate_count == 0


def test_live_promoter_does_not_repromote_a_repaired_run(tmp_path: Path) -> None:
    source_root, target_root, run_dir = _truncated_setup(tmp_path)
    repromote_short_runs(target_root=target_root, apply=True)
    report = promote_replayable_runs(source_root, target_root, limit=10, max_age_hours=24 * 365 * 100)
    assert report.promoted_run_count == 0
    assert report.runs[0].action == "skipped_promoted"
    assert len(_curated_rows_for(target_root, str(run_dir))) == 10


def test_not_replayable_now_removes_partial_rows(tmp_path: Path) -> None:
    source_root, target_root, run_dir = _truncated_setup(tmp_path)
    (run_dir / "metrics" / "replay_summary.json").write_text(
        json.dumps({"replayable": False, "findings": ["trade_id_gaps"]}), encoding="utf-8"
    )
    report = repromote_short_runs(target_root=target_root, apply=True)
    assert report.removed_count == 1 and report.repromoted_count == 0
    assert _curated_rows_for(target_root, str(run_dir)) == []
    latest = latest_index_rows(target_root / "_promotion_index.jsonl")[str(run_dir)]
    assert latest["promoted_rows"] == 0 and latest["removed_not_replayable"] is True


def test_cold_tier_fallback_keeps_hot_run_key(tmp_path: Path) -> None:
    source_root, target_root, run_dir = _truncated_setup(tmp_path)
    cold_root = tmp_path / "cold" / "raw" / "market"
    cold_run = cold_root / run_dir.parent.name / run_dir.name
    cold_run.parent.mkdir(parents=True)
    shutil.move(str(run_dir), str(cold_run))
    assert not run_dir.exists()

    missing = repromote_short_runs(target_root=target_root, apply=False)
    assert missing.runs and missing.runs[0].action == "skipped_raw_missing"

    report = repromote_short_runs(target_root=target_root, cold_root=cold_root, apply=True)
    assert report.repromoted_count == 1
    curated = _curated_rows_for(target_root, str(run_dir))  # key stays the HOT path
    assert len(curated) == 10
    latest = latest_index_rows(target_root / "_promotion_index.jsonl")[str(run_dir)]
    assert latest["raw_dir"] == str(cold_run)


def test_complete_and_young_runs_are_not_candidates(tmp_path: Path) -> None:
    source_root = tmp_path / "raw" / "market" / "binance_trades"
    target_root = tmp_path / "curated" / "trades_replayable"
    complete = source_root / "20260406_000000"
    _write_run(complete, [_row(i) for i in range(10)])
    promote_replayable_runs(source_root, target_root, limit=10, max_age_hours=24 * 365 * 100)
    # A young short run (still being written) must be left to the live floor.
    young = source_root / datetime.now(tz=UTC).strftime("%Y%m%d_%H%M%S")
    _write_run(young, [_row(i) for i in range(2)])
    promote_replayable_runs(source_root, target_root, limit=10, max_age_hours=24)
    _append_rows(young, [_row(i) for i in range(2, 10)])

    report = repromote_short_runs(target_root=target_root)
    assert report.candidate_count == 0
    assert report.index_runs == 2


def test_shared_part_file_is_never_touched(tmp_path: Path) -> None:
    """A part-file holding two runs' rows (impossible from the promoter, but cheap
    to guard) must make the run a skip, not a partial delete."""
    target_root = tmp_path / "curated" / "trades_replayable"
    source_root = tmp_path / "raw" / "market" / "binance_trades"
    run_a = source_root / "20260406_000000"
    run_b = source_root / "20260406_003000"
    _write_run(run_a, [_row(i) for i in range(10)])
    _write_run(run_b, [_row(i) for i in range(10, 20)])
    sink = ParquetDatasetSink(target_root, batch_size=1000)
    for run in (run_a, run_b):
        for r in [_row(i) for i in range(3)]:  # 3 rows each, one shared flush
            r = dict(r)
            r["source_run_path"] = str(run)
            sink.write(r)
    sink.flush()
    (target_root / "_promotion_index.jsonl").write_text(
        "".join(
            json.dumps({"run_path": str(run), "promoted_at": OLD.isoformat(), "promoted_rows": 3}) + "\n"
            for run in (run_a, run_b)
        ),
        encoding="utf-8",
    )
    _, _, shared = build_run_file_map(target_root)
    assert len(shared) == 1

    report = repromote_short_runs(target_root=target_root, apply=True)
    assert report.candidate_count == 2
    assert {run.action for run in report.runs} == {"skipped_shared_part_file"}
    assert report.status == "warn"
    assert len(list(target_root.rglob("*.parquet"))) == 1  # untouched


def test_index_file_row_mismatch_is_reported_but_repaired(tmp_path: Path) -> None:
    """The file scan is complete (whole dataset), so a stale index count is
    informational - the run's files are all known and can be replaced safely."""
    source_root, target_root, run_dir = _truncated_setup(tmp_path)
    index = target_root / "_promotion_index.jsonl"
    row = json.loads(index.read_text(encoding="utf-8").splitlines()[0])
    row["promoted_rows"] = 5  # files hold 4
    index.write_text(json.dumps(row) + "\n", encoding="utf-8")
    report = repromote_short_runs(target_root=target_root, apply=True)
    assert report.runs[0].action == "repromote"
    assert report.runs[0].index_mismatch is True
    assert report.rows_removed == 4
    assert len(_curated_rows_for(target_root, str(run_dir))) == 10


def test_lane_filter_and_limit(tmp_path: Path) -> None:
    source_a, target_root, run_a = _truncated_setup(tmp_path, lane="binance_trades")
    # Second truncated lane sharing the same curated root.
    source_b = tmp_path / "raw" / "market" / "okx_trades"
    run_b = source_b / "20260406_010000"
    rows = [_row(i, source="okx") for i in range(10)]
    _write_run(run_b, rows[:5])
    promote_replayable_runs(source_b, target_root, limit=10, max_age_hours=24 * 365 * 100)
    _append_rows(run_b, rows[5:])

    only_b = repromote_short_runs(target_root=target_root, lanes=["okx_trades"])
    assert [run.lane for run in only_b.runs] == ["okx_trades"]
    limited = repromote_short_runs(target_root=target_root, limit=1)
    assert limited.candidate_count == 1


def test_cli_text_report(tmp_path: Path, capsys) -> None:
    _, target_root, _ = _truncated_setup(tmp_path)
    run_repromote_short_runs(
        SimpleNamespace(
            target_root=target_root,
            lane=None,
            cold_root=None,
            min_ratio=0.98,
            min_age_hours=1.0,
            limit=1000,
            apply=False,
            format="text",
        )
    )
    out = capsys.readouterr().out
    assert "mode=dry_run" in out and "would_repromote=1" in out and "--apply" in out
