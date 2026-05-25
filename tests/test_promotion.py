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
