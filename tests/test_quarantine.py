from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from crypto_collector.quarantine import quarantine_aged_unaccounted_run, quarantine_bad_runs


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


def test_aged_unaccounted_tolerates_torn_replay_summary(tmp_path: Path) -> None:
    """Regression: an empty/torn replay_summary.json (killed scorer, power-cut
    zero-length promotion) used to raise JSONDecodeError out of the backstop and
    abort the caller's whole offload pass. It must classify as missing instead."""
    run_dir = tmp_path / "raw" / "okx_trades" / "20200101_000000"
    (run_dir / "metrics").mkdir(parents=True)
    (run_dir / "metrics" / "replay_summary.json").write_text("", encoding="utf-8")
    quarantine_index = tmp_path / "quarantine" / "okx_trades" / "_quarantine_index.jsonl"

    report = quarantine_aged_unaccounted_run(
        run_dir,
        quarantine_index_path=quarantine_index,
        checked_at=datetime.now(tz=UTC),
    )

    assert report.error is None
    assert report.action == "quarantined_aged_unaccounted"
    assert report.findings == ["aged_unaccounted", "missing_replay_summary"]
    assert report.replayable is None
    index_row = json.loads(quarantine_index.read_text(encoding="utf-8"))
    assert index_row["classification"] == "aged_unaccounted"


def test_aged_unaccounted_ignores_scalar_findings_in_replay_summary(tmp_path: Path) -> None:
    """A damaged-but-valid summary with a scalar findings value must not explode
    into per-character findings in the durable quarantine index."""
    run_dir = tmp_path / "raw" / "okx_trades" / "20200101_000000"
    (run_dir / "metrics").mkdir(parents=True)
    (run_dir / "metrics" / "replay_summary.json").write_text(
        json.dumps({"replayable": False, "findings": "gap_detected"}), encoding="utf-8"
    )
    quarantine_index = tmp_path / "quarantine" / "okx_trades" / "_quarantine_index.jsonl"

    report = quarantine_aged_unaccounted_run(
        run_dir,
        quarantine_index_path=quarantine_index,
        checked_at=datetime.now(tz=UTC),
    )

    assert report.error is None
    assert report.findings == ["aged_unaccounted"]


def test_aged_unaccounted_diagnostics_stream_only_bounded_samples(tmp_path: Path) -> None:
    source_root = tmp_path / "raw" / "market" / "binance_depth"
    run_dir = source_root / "20200101_000000"
    for folder, filename in (
        ("raw", "messages.jsonl"),
        ("clean", "events.jsonl"),
        ("metrics", "summary.jsonl"),
    ):
        path = run_dir / folder / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            "".join(json.dumps({"row": index}) + "\n" for index in range(100)),
            encoding="utf-8",
        )
    quarantine_index = tmp_path / "quarantine" / "binance_depth" / "_quarantine_index.jsonl"

    report = quarantine_aged_unaccounted_run(
        run_dir,
        quarantine_index_path=quarantine_index,
        checked_at=datetime.now(tz=UTC),
    )

    assert report.error is None
    diagnostics = json.loads(
        (quarantine_index.parent / run_dir.name / "diagnostics.json").read_text(
            encoding="utf-8"
        )
    )
    assert len(diagnostics["raw_sample"]) == 5
    assert len(diagnostics["clean_sample"]) == 5
    assert len(diagnostics["metrics_summary"]) == 20
    assert diagnostics["classification"]["findings"] == [
        "aged_unaccounted",
        "missing_replay_summary",
    ]
