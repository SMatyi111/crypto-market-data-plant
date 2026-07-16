"""Text replay-verdict tests: the envelope-integrity gating bar, the non-gating
source-clock diagnostics (the probe's ~16h stale outlier must promote), quiet-run
accounting (no_events summaries even without a clean events file), and the
backfill catch-up path."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from crypto_collector.quarantine import quarantine_bad_runs
from crypto_collector.replay import backfill_replay_summaries, replay_text_run

T0 = datetime(2026, 7, 15, 12, 0, 0, tzinfo=UTC)


def _row(
    idx: int,
    *,
    source_id: str | None = None,
    content_hash: str | None = None,
    ingestion: datetime | None = None,
    source_ts: datetime | None | str = "default",
    event_type: str = "new",
    metadata: dict | None = None,
) -> dict:
    ingestion = ingestion or (T0 + timedelta(seconds=idx))
    if source_ts == "default":
        source_ts = ingestion - timedelta(seconds=30)
    return {
        "source": "rss",
        "product": "feedx",
        "channel": "text",
        "event_type": event_type,
        "source_id": source_id if source_id is not None else f"id{idx}",
        "source_ts": source_ts.isoformat() if isinstance(source_ts, datetime) else source_ts,
        "ingestion_ts": ingestion.isoformat(),
        "received_at": ingestion.isoformat(),
        "content_hash": content_hash if content_hash is not None else f"hash{idx}",
        "raw_item": "<item/>",
        "metadata": metadata or {},
    }


def _write_run(tmp_path: Path, rows: list[dict] | None, name: str = "20260715_120000") -> Path:
    run_dir = tmp_path / "text_rss" / name
    (run_dir / "metrics").mkdir(parents=True)
    if rows is not None:
        (run_dir / "clean").mkdir()
        (run_dir / "clean" / "events.jsonl").write_text(
            "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
        )
    return run_dir


def test_replay_text_run_happy_path(tmp_path: Path) -> None:
    rows = [_row(0), _row(1, event_type="edit"), _row(2)]
    run_dir = _write_run(tmp_path, rows)
    summary = replay_text_run(run_dir)
    assert summary.replayable is True
    assert summary.findings == []
    assert summary.event_count == 3
    assert summary.new_count == 2
    assert summary.edit_count == 1
    assert summary.gap_detection == "none_native"
    assert summary.mode == "text_poll"
    assert summary.source == "rss"
    assert summary.max_source_lag_seconds == 30.0
    written = json.loads((run_dir / "metrics" / "replay_summary.json").read_text(encoding="utf-8"))
    assert written["replayable"] is True
    assert written["replay_type"] == "text"


def test_stale_source_ts_outlier_reports_but_promotes(tmp_path: Path) -> None:
    # The probe's real-world case: Cointelegraph claimed a publish time ~16h before
    # ingestion. The claim is preserved and DIAGNOSED, never gating.
    stale = _row(1, source_ts=(T0 + timedelta(seconds=1)) - timedelta(hours=16))
    run_dir = _write_run(tmp_path, [_row(0), stale])
    summary = replay_text_run(run_dir)
    assert summary.replayable is True
    assert "stale_source_ts" in summary.findings
    assert summary.stale_source_ts_count == 1
    assert summary.max_source_lag_seconds == 16 * 3600.0


def test_future_missing_and_unparseable_source_ts_are_non_gating(tmp_path: Path) -> None:
    rows = [
        _row(0, source_ts=(T0 + timedelta(seconds=0)) + timedelta(hours=1)),  # future claim
        _row(1, source_ts=None),  # missing claim
        _row(2, source_ts=None, metadata={"source_ts_unparseable": True, "source_ts_raw": "junk"}),
    ]
    summary = replay_text_run(_write_run(tmp_path, rows))
    assert summary.replayable is True
    assert summary.future_source_ts_count == 1
    assert summary.missing_source_ts_count == 1
    assert summary.unparseable_source_ts_count == 1
    for finding in ("future_source_ts", "missing_source_ts", "unparseable_source_ts"):
        assert finding in summary.findings


def test_duplicate_envelope_keys_reported_not_gating(tmp_path: Path) -> None:
    # A content revert (A -> B -> A) re-emits an earlier (source, id, hash) key.
    rows = [
        _row(0, source_id="idA", content_hash="hA"),
        _row(1, source_id="idA", content_hash="hB", event_type="edit"),
        _row(2, source_id="idA", content_hash="hA", event_type="edit"),
    ]
    summary = replay_text_run(_write_run(tmp_path, rows))
    assert summary.replayable is True
    assert summary.duplicate_key_count == 1
    assert "duplicate_envelope_keys" in summary.findings


def test_missing_envelope_fields_block(tmp_path: Path) -> None:
    summary = replay_text_run(_write_run(tmp_path, [_row(0), _row(1, content_hash="")]))
    assert summary.replayable is False
    assert "missing_envelope_fields" in summary.findings
    assert summary.missing_envelope_count == 1


def test_non_monotonic_ingestion_ts_blocks(tmp_path: Path) -> None:
    rows = [_row(0), _row(1, ingestion=T0 - timedelta(seconds=5))]
    summary = replay_text_run(_write_run(tmp_path, rows))
    assert summary.replayable is False
    assert "non_monotonic_ingestion_ts" in summary.findings


def test_empty_run_without_events_file_still_writes_summary(tmp_path: Path) -> None:
    # A quiet poll window: no clean/events.jsonl at all. The run must still score
    # (no_events, unreplayable) so quarantine + offload accounting closes.
    run_dir = _write_run(tmp_path, None)
    summary = replay_text_run(run_dir)
    assert summary.replayable is False
    assert summary.findings == ["no_events"]
    assert (run_dir / "metrics" / "replay_summary.json").exists()

    # ... and the standard quarantine job then moves it out of the promotion path.
    report = quarantine_bad_runs(
        tmp_path / "text_rss",
        tmp_path / "quarantine" / "text_rss",
        limit=10,
        max_age_hours=24 * 365 * 10,
    )
    assert report.quarantined_count == 1


def test_backfill_text_replay_scores_eventless_runs(tmp_path: Path) -> None:
    lane_root = tmp_path / "text_rss"
    _write_run(tmp_path, [_row(0)], name="20260715_120000")
    _write_run(tmp_path, None, name="20260715_123000")  # crash/quiet orphan

    from crypto_collector.replay import replay_text_run as scorer

    report = backfill_replay_summaries(
        lane_root,
        limit=10,
        max_age_hours=24 * 365 * 10,
        replay_fn=lambda run_dir, write_summary=True: scorer(run_dir, write_summary=write_summary),
        require_events=False,
    )
    assert report.created_count == 2
    assert report.failed_count == 0
    quiet = json.loads(
        (lane_root / "20260715_123000" / "metrics" / "replay_summary.json").read_text(encoding="utf-8")
    )
    assert quiet["replayable"] is False
    assert quiet["findings"] == ["no_events"]

    # Default (market) semantics unchanged: require_events=True still skips it.
    (lane_root / "20260715_123000" / "metrics" / "replay_summary.json").unlink()
    report = backfill_replay_summaries(
        lane_root,
        limit=10,
        max_age_hours=24 * 365 * 10,
        replay_fn=lambda run_dir, write_summary=True: scorer(run_dir, write_summary=write_summary),
    )
    assert any(run.action == "skipped_missing_events" for run in report.runs)
