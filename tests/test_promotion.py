from __future__ import annotations

import json
from pathlib import Path

import pyarrow.dataset as ds

from crypto_collector.promotion import promote_replayable_runs


def test_promote_replayable_runs_writes_curated_dataset_and_index(tmp_path: Path) -> None:
    source_root = tmp_path / "raw" / "market" / "binance_depth"
    target_root = tmp_path / "curated" / "market_replayable"
    run_dir = source_root / "20990101_000000"
    clean_dir = run_dir / "clean"
    metrics_dir = run_dir / "metrics"
    clean_dir.mkdir(parents=True)
    metrics_dir.mkdir(parents=True)
    (clean_dir / "events.jsonl").write_text(
        json.dumps(
            {
                "source": "binance",
                "event_time": "2026-04-06T00:00:00+00:00",
                "received_at": "2026-04-06T00:00:00.100000+00:00",
                "instrument": {"instrument_id": "spot:binance:BTCUSDT"},
                "product": "BTCUSDT",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (metrics_dir / "replay_summary.json").write_text(
        json.dumps({"replayable": True, "findings": []}),
        encoding="utf-8",
    )

    report = promote_replayable_runs(source_root, target_root, limit=10, max_age_hours=24 * 365 * 100)

    assert report.status == "ok"
    assert report.promoted_run_count == 1
    assert report.promoted_row_count == 1
    dataset = ds.dataset(target_root, format="parquet", partitioning="hive")
    rows = dataset.to_table().to_pylist()
    assert len(rows) == 1
    assert rows[0]["source"] == "binance"
    assert rows[0]["source_run_path"] == str(run_dir)
    index_rows = (target_root / "_promotion_index.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(index_rows) == 1


def test_promote_replayable_runs_skips_unreplayable_and_existing(tmp_path: Path) -> None:
    source_root = tmp_path / "raw" / "market" / "binance_depth"
    target_root = tmp_path / "curated" / "market_replayable"
    run_dir = source_root / "20990101_000001"
    clean_dir = run_dir / "clean"
    metrics_dir = run_dir / "metrics"
    clean_dir.mkdir(parents=True)
    metrics_dir.mkdir(parents=True)
    (clean_dir / "events.jsonl").write_text("{}", encoding="utf-8")
    (metrics_dir / "replay_summary.json").write_text(
        json.dumps({"replayable": False, "findings": ["snapshot_anchor_gap"]}),
        encoding="utf-8",
    )

    first_report = promote_replayable_runs(source_root, target_root, limit=10, max_age_hours=24 * 365 * 100)
    assert first_report.status == "warn"
    assert first_report.promoted_run_count == 0
    assert first_report.runs[0].action == "skipped_unreplayable"

    # Seed an already promoted replayable run and make sure it is skipped cleanly.
    replayable_run = source_root / "20990101_000002"
    replayable_clean_dir = replayable_run / "clean"
    replayable_metrics_dir = replayable_run / "metrics"
    replayable_clean_dir.mkdir(parents=True)
    replayable_metrics_dir.mkdir(parents=True)
    (replayable_clean_dir / "events.jsonl").write_text(
        json.dumps(
            {
                "source": "binance",
                "event_time": "2026-04-06T00:00:00+00:00",
                "received_at": "2026-04-06T00:00:00.100000+00:00",
                "instrument": {"instrument_id": "spot:binance:BTCUSDT"},
                "product": "BTCUSDT",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (replayable_metrics_dir / "replay_summary.json").write_text(
        json.dumps({"replayable": True, "findings": []}),
        encoding="utf-8",
    )

    seeded = promote_replayable_runs(source_root, target_root, limit=10, max_age_hours=24 * 365 * 100)
    assert seeded.promoted_run_count == 1

    second = promote_replayable_runs(source_root, target_root, limit=10, max_age_hours=24 * 365 * 100)
    actions = {run.run_path: run.action for run in second.runs}
    assert actions[str(replayable_run)] == "skipped_promoted"


def test_promote_replayable_runs_skips_quarantined_runs(tmp_path: Path) -> None:
    source_root = tmp_path / "raw" / "market" / "binance_depth"
    target_root = tmp_path / "curated" / "market_replayable"
    quarantine_root = tmp_path / "quarantine" / "market" / "binance_depth"
    run_dir = source_root / "20990101_000003"
    clean_dir = run_dir / "clean"
    metrics_dir = run_dir / "metrics"
    clean_dir.mkdir(parents=True)
    metrics_dir.mkdir(parents=True)
    quarantine_root.mkdir(parents=True)
    (clean_dir / "events.jsonl").write_text("{}", encoding="utf-8")
    (metrics_dir / "replay_summary.json").write_text(
        json.dumps({"replayable": True, "findings": []}),
        encoding="utf-8",
    )
    (quarantine_root / "_quarantine_index.jsonl").write_text(
        json.dumps({"run_path": str(run_dir), "quarantine_dir": str(quarantine_root / run_dir.name)}) + "\n",
        encoding="utf-8",
    )

    report = promote_replayable_runs(
        source_root,
        target_root,
        limit=10,
        max_age_hours=24 * 365 * 100,
        quarantine_index_path=quarantine_root / "_quarantine_index.jsonl",
    )

    assert report.status == "warn"
    assert report.promoted_run_count == 0
    assert report.runs[0].action == "skipped_quarantined"


def test_promote_replayable_runs_reports_quarantined_before_unreplayable(tmp_path: Path) -> None:
    source_root = tmp_path / "raw" / "market" / "binance_depth"
    target_root = tmp_path / "curated" / "market_replayable"
    quarantine_root = tmp_path / "quarantine" / "market" / "binance_depth"
    run_dir = source_root / "20990101_000004"
    metrics_dir = run_dir / "metrics"
    metrics_dir.mkdir(parents=True)
    quarantine_root.mkdir(parents=True)
    (metrics_dir / "replay_summary.json").write_text(
        json.dumps({"replayable": False, "findings": ["snapshot_anchor_gap"]}),
        encoding="utf-8",
    )
    (quarantine_root / "_quarantine_index.jsonl").write_text(
        json.dumps({"run_path": str(run_dir), "quarantine_dir": str(quarantine_root / run_dir.name)}) + "\n",
        encoding="utf-8",
    )

    report = promote_replayable_runs(
        source_root,
        target_root,
        limit=10,
        max_age_hours=24 * 365 * 100,
        quarantine_index_path=quarantine_root / "_quarantine_index.jsonl",
    )

    assert report.status == "warn"
    assert report.promoted_run_count == 0
    assert report.runs[0].action == "skipped_quarantined"
    assert "quarantined_run" in report.runs[0].findings


def _write_promotable_run(source_root: Path, name: str, products: list[str]) -> Path:
    run_dir = source_root / name
    clean_dir = run_dir / "clean"
    metrics_dir = run_dir / "metrics"
    clean_dir.mkdir(parents=True)
    metrics_dir.mkdir(parents=True)
    (clean_dir / "events.jsonl").write_text(
        "".join(
            json.dumps(
                {
                    "source": "binance",
                    "event_time": "2026-04-06T00:00:00+00:00",
                    "instrument": {"instrument_id": "spot:binance:BTCUSDT"},
                    "product": product,
                }
            )
            + "\n"
            for product in products
        ),
        encoding="utf-8",
    )
    (metrics_dir / "replay_summary.json").write_text(
        json.dumps({"replayable": True, "findings": []}), encoding="utf-8"
    )
    return run_dir


def test_failed_run_discards_buffer_no_duplicates_and_no_cascade(
    tmp_path: Path, monkeypatch
) -> None:
    """A per-run failure mid-write must DISCARD the run's buffered rows: the run has no
    index entry so the next pass re-promotes it in full — under the old best-effort
    flush the partial rows were persisted too, duplicating them on the retry. The
    discard also keeps a poisoned buffer from cascading into later runs in the pass."""
    from crypto_collector.storage import ParquetDatasetSink

    source_root = tmp_path / "raw" / "market" / "binance_trades"
    target_root = tmp_path / "curated" / "trades_replayable"
    # Newest run (processed first) fails mid-run; the older run must still promote.
    _write_promotable_run(source_root, "20990101_000001", ["GOODROW", "POISON"])
    _write_promotable_run(source_root, "20990101_000000", ["OLDROW"])

    original_write = ParquetDatasetSink.write

    def poisoned_write(self, row):
        if row.get("product") == "POISON":
            raise RuntimeError("simulated mid-run write failure")
        return original_write(self, row)

    monkeypatch.setattr(ParquetDatasetSink, "write", poisoned_write)
    report = promote_replayable_runs(
        source_root, target_root, limit=10, max_age_hours=24 * 365 * 100
    )
    assert report.failed_count == 1
    assert report.promoted_run_count == 1

    dataset = ds.dataset(target_root, format="parquet", partitioning="hive")
    rows = dataset.to_table().to_pylist()
    # Only the clean older run's row — the failed run's GOODROW was discarded, not flushed.
    assert [row["product"] for row in rows] == ["OLDROW"]
    index_lines = (target_root / "_promotion_index.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(index_lines) == 1

    # Retry without the poison: the failed run promotes in full, exactly once.
    monkeypatch.setattr(ParquetDatasetSink, "write", original_write)
    retry = promote_replayable_runs(
        source_root, target_root, limit=10, max_age_hours=24 * 365 * 100
    )
    assert retry.failed_count == 0
    assert retry.promoted_run_count == 1
    rows = ds.dataset(target_root, format="parquet", partitioning="hive").to_table().to_pylist()
    assert sorted(row["product"] for row in rows) == ["GOODROW", "OLDROW", "POISON"]


def test_torn_replay_summary_skips_run_not_whole_pass(tmp_path: Path) -> None:
    """A half-written replay_summary.json (collector finalizing concurrently, or a
    killed scorer) must skip that run as missing-summary — previously the unguarded
    json.loads aborted the entire promotion pass for every lane behind it."""
    source_root = tmp_path / "raw" / "market" / "binance_trades"
    target_root = tmp_path / "curated" / "trades_replayable"
    torn = _write_promotable_run(source_root, "20990101_000001", ["TORN"])
    (torn / "metrics" / "replay_summary.json").write_text('{"replayable": tru', encoding="utf-8")
    _write_promotable_run(source_root, "20990101_000000", ["WHOLE"])

    report = promote_replayable_runs(
        source_root, target_root, limit=10, max_age_hours=24 * 365 * 100
    )
    assert report.promoted_run_count == 1
    actions = {run.run_path: run.action for run in report.runs}
    assert actions[str(torn)] == "skipped_missing_replay"
