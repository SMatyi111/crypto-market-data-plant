from __future__ import annotations

import json
from pathlib import Path

from crypto_collector.quarantine import quarantine_bad_runs


def test_quarantine_bad_runs_writes_index_and_diagnostics(tmp_path: Path) -> None:
    source_root = tmp_path / "raw" / "market" / "binance_depth"
    quarantine_root = tmp_path / "quarantine" / "market" / "binance_depth"
    run_dir = source_root / "20990101_000000"
    clean_dir = run_dir / "clean"
    raw_dir = run_dir / "raw"
    metrics_dir = run_dir / "metrics"
    clean_dir.mkdir(parents=True)
    raw_dir.mkdir(parents=True)
    metrics_dir.mkdir(parents=True)
    (clean_dir / "events.jsonl").write_text(json.dumps({"a": 1}) + "\n", encoding="utf-8")
    (raw_dir / "messages.jsonl").write_text(json.dumps({"b": 2}) + "\n", encoding="utf-8")
    (metrics_dir / "summary.jsonl").write_text(json.dumps({"raw_messages": 1}) + "\n", encoding="utf-8")
    (metrics_dir / "replay_summary.json").write_text(
        json.dumps({"replayable": False, "findings": ["snapshot_anchor_gap"]}),
        encoding="utf-8",
    )

    report = quarantine_bad_runs(source_root, quarantine_root, limit=10, max_age_hours=24 * 365 * 100)

    assert report.status == "ok"
    assert report.quarantined_count == 1
    diagnostics_path = quarantine_root / "20990101_000000" / "diagnostics.json"
    assert diagnostics_path.exists()
    index_path = quarantine_root / "_quarantine_index.jsonl"
    assert index_path.exists()


def test_quarantine_bad_runs_skips_replayable_runs(tmp_path: Path) -> None:
    source_root = tmp_path / "raw" / "market" / "binance_depth"
    quarantine_root = tmp_path / "quarantine" / "market" / "binance_depth"
    run_dir = source_root / "20990101_000001"
    metrics_dir = run_dir / "metrics"
    metrics_dir.mkdir(parents=True)
    (metrics_dir / "replay_summary.json").write_text(
        json.dumps({"replayable": True, "findings": []}),
        encoding="utf-8",
    )

    report = quarantine_bad_runs(source_root, quarantine_root, limit=10, max_age_hours=24 * 365 * 100)

    assert report.status == "warn"
    assert report.quarantined_count == 0
    assert report.runs[0].action == "skipped_replayable"
