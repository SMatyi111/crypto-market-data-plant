from __future__ import annotations

import json
from pathlib import Path

from crypto_collector.replay import backfill_replay_summaries, replay_depth_run


def test_replay_depth_run_reconstructs_book_and_writes_summary(tmp_path: Path) -> None:
    run_path = tmp_path / "binance_depth" / "20260406_000000"
    clean_path = run_path / "clean"
    clean_path.mkdir(parents=True)
    snapshot_path = run_path / "snapshots"
    snapshot_path.mkdir(parents=True)
    (snapshot_path / "book_snapshot.json").write_text(
        json.dumps(
            {
                "source": "binance",
                "product": "BTCUSDT",
                "received_at": "2026-04-06T00:00:00+00:00",
                "snapshot": {
                    "lastUpdateId": 0,
                    "bids": [],
                    "asks": [],
                },
            }
        ),
        encoding="utf-8",
    )
    events = [
        {
            "source": "binance",
            "product": "BTCUSDT",
            "event_time": "2026-04-06T00:00:00+00:00",
            "received_at": "2026-04-06T00:00:00.100000+00:00",
            "first_update_id": 1,
            "final_update_id": 2,
            "instrument": {"instrument_id": "spot:binance:BTCUSDT"},
            "bids": [[100.0, 1.0]],
            "asks": [[101.0, 2.0]],
        },
        {
            "source": "binance",
            "product": "BTCUSDT",
            "event_time": "2026-04-06T00:00:01+00:00",
            "received_at": "2026-04-06T00:00:01.100000+00:00",
            "first_update_id": 3,
            "final_update_id": 4,
            "instrument": {"instrument_id": "spot:binance:BTCUSDT"},
            "bids": [[100.0, 0.0], [99.0, 3.0]],
            "asks": [[101.0, 1.5], [102.0, 1.0]],
        },
    ]
    (clean_path / "events.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in events),
        encoding="utf-8",
    )

    summary = replay_depth_run(run_path, max_levels=5)

    assert summary.replayable is True
    assert summary.event_count == 2
    assert summary.gap_count == 0
    assert summary.snapshot_gap_count == 0
    assert summary.best_bid == 99.0
    assert summary.best_ask == 101.0
    assert summary.spread == 2.0
    assert summary.top_bids == [[99.0, 3.0]]
    assert summary.top_asks == [[101.0, 1.5], [102.0, 1.0]]
    assert summary.summary_path is not None
    assert Path(summary.summary_path).exists()


def test_replay_depth_run_flags_gaps_invalid_ranges_and_crossed_book(tmp_path: Path) -> None:
    run_path = tmp_path / "binance_depth" / "20260406_000001"
    clean_path = run_path / "clean"
    clean_path.mkdir(parents=True)
    events = [
        {
            "source": "binance",
            "product": "BTCUSDT",
            "event_time": "2026-04-06T00:00:00+00:00",
            "received_at": "2026-04-06T00:00:00.100000+00:00",
            "first_update_id": 10,
            "final_update_id": 12,
            "instrument": {"instrument_id": "spot:binance:BTCUSDT"},
            "bids": [[100.0, 1.0]],
            "asks": [[101.0, 1.0]],
        },
        {
            "source": "binance",
            "product": "BTCUSDT",
            "event_time": "2026-04-06T00:00:01+00:00",
            "received_at": "2026-04-06T00:00:01.100000+00:00",
            "first_update_id": 14,
            "final_update_id": 14,
            "instrument": {"instrument_id": "spot:binance:BTCUSDT"},
            "bids": [[102.0, 1.0]],
            "asks": [],
        },
        {
            "source": "binance",
            "product": "BTCUSDT",
            "event_time": "2026-04-06T00:00:02+00:00",
            "received_at": "2026-04-06T00:00:02.100000+00:00",
            "first_update_id": 13,
            "final_update_id": 12,
            "instrument": {"instrument_id": "spot:binance:BTCUSDT"},
            "bids": [],
            "asks": [],
        },
    ]
    (clean_path / "events.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in events),
        encoding="utf-8",
    )

    summary = replay_depth_run(run_path, max_levels=5, write_summary=False)

    assert summary.replayable is False
    assert summary.gap_count == 1
    assert summary.invalid_range_count == 1
    assert summary.crossed_book_count >= 1
    assert "gaps_detected" in summary.findings
    assert "invalid_update_ranges" in summary.findings
    assert "crossed_book_states" in summary.findings


def test_replay_depth_run_flags_empty_runs(tmp_path: Path) -> None:
    run_path = tmp_path / "binance_depth" / "20260406_000002"
    clean_path = run_path / "clean"
    clean_path.mkdir(parents=True)
    (clean_path / "events.jsonl").write_text("", encoding="utf-8")

    summary = replay_depth_run(run_path, write_summary=False)

    assert summary.replayable is False
    assert summary.event_count == 0
    assert "no_events" in summary.findings


def test_replay_depth_run_does_not_double_count_snapshot_gap_as_sequence_gap(tmp_path: Path) -> None:
    """A snapshot anchor gap must increment snapshot_gap_count only, not also gap_count."""
    run_path = tmp_path / "binance_depth" / "20260406_000099"
    clean_path = run_path / "clean"
    snapshot_path = run_path / "snapshots"
    clean_path.mkdir(parents=True)
    snapshot_path.mkdir(parents=True)
    (snapshot_path / "book_snapshot.json").write_text(
        json.dumps(
            {
                "source": "binance",
                "product": "BTCUSDT",
                "received_at": "2026-04-06T00:00:00+00:00",
                "snapshot": {
                    "lastUpdateId": 100,
                    "bids": [["100.0", "1.0"]],
                    "asks": [["101.0", "1.0"]],
                },
            }
        ),
        encoding="utf-8",
    )
    events = [
        # First post-snapshot event jumps past snapshot_last+1 -> snapshot anchor gap.
        {
            "source": "binance",
            "product": "BTCUSDT",
            "event_time": "2026-04-06T00:00:01+00:00",
            "received_at": "2026-04-06T00:00:01+00:00",
            "first_update_id": 110,
            "final_update_id": 115,
            "instrument": {"instrument_id": "spot:binance:BTCUSDT"},
            "bids": [],
            "asks": [],
        },
        # Next event is contiguous with the anchor -> must not count as a normal gap.
        {
            "source": "binance",
            "product": "BTCUSDT",
            "event_time": "2026-04-06T00:00:02+00:00",
            "received_at": "2026-04-06T00:00:02+00:00",
            "first_update_id": 116,
            "final_update_id": 120,
            "instrument": {"instrument_id": "spot:binance:BTCUSDT"},
            "bids": [],
            "asks": [],
        },
    ]
    (clean_path / "events.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in events),
        encoding="utf-8",
    )

    summary = replay_depth_run(run_path, write_summary=False)

    assert summary.snapshot_gap_count == 1
    assert summary.gap_count == 0
    assert summary.reordered_count == 0


def test_replay_depth_run_flags_snapshot_anchor_gap(tmp_path: Path) -> None:
    run_path = tmp_path / "binance_depth" / "20260406_000003"
    clean_path = run_path / "clean"
    snapshot_path = run_path / "snapshots"
    clean_path.mkdir(parents=True)
    snapshot_path.mkdir(parents=True)
    (snapshot_path / "book_snapshot.json").write_text(
        json.dumps(
            {
                "source": "binance",
                "product": "BTCUSDT",
                "received_at": "2026-04-06T00:00:00+00:00",
                "snapshot": {
                    "lastUpdateId": 100,
                    "bids": [["100.0", "1.0"]],
                    "asks": [["101.0", "1.0"]],
                },
            }
        ),
        encoding="utf-8",
    )
    (clean_path / "events.jsonl").write_text(
        json.dumps(
            {
                "source": "binance",
                "product": "BTCUSDT",
                "event_time": "2026-04-06T00:00:01+00:00",
                "received_at": "2026-04-06T00:00:01.100000+00:00",
                "first_update_id": 105,
                "final_update_id": 106,
                "instrument": {"instrument_id": "spot:binance:BTCUSDT"},
                "bids": [[102.0, 1.0]],
                "asks": [],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    summary = replay_depth_run(run_path, write_summary=False)

    assert summary.replayable is False
    assert summary.snapshot_last_update_id == 100
    assert summary.snapshot_gap_count == 1
    assert "snapshot_anchor_gap" in summary.findings


def test_backfill_replay_summaries_creates_missing_summary(tmp_path: Path) -> None:
    source_root = tmp_path / "raw" / "market" / "binance_depth"
    run_path = source_root / "20990101_000010"
    clean_path = run_path / "clean"
    clean_path.mkdir(parents=True)
    (clean_path / "events.jsonl").write_text(
        json.dumps(
            {
                "source": "binance",
                "product": "BTCUSDT",
                "event_time": "2026-04-06T00:00:00+00:00",
                "received_at": "2026-04-06T00:00:00.100000+00:00",
                "first_update_id": 1,
                "final_update_id": 1,
                "instrument": {"instrument_id": "spot:binance:BTCUSDT"},
                "bids": [[100.0, 1.0]],
                "asks": [[101.0, 1.0]],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    report = backfill_replay_summaries(source_root, limit=10, max_age_hours=24 * 365 * 100)

    assert report.status == "ok"
    assert report.created_count == 1
    assert report.updated_count == 0
    assert report.failed_count == 0
    assert report.runs[0].action == "created"
    assert (run_path / "metrics" / "replay_summary.json").exists()


def test_backfill_replay_summaries_skips_existing_without_overwrite(tmp_path: Path) -> None:
    source_root = tmp_path / "raw" / "market" / "binance_depth"
    run_path = source_root / "20990101_000011"
    metrics_path = run_path / "metrics"
    clean_path = run_path / "clean"
    clean_path.mkdir(parents=True)
    metrics_path.mkdir(parents=True)
    (clean_path / "events.jsonl").write_text("", encoding="utf-8")
    (metrics_path / "replay_summary.json").write_text(
        json.dumps({"replayable": True, "findings": []}),
        encoding="utf-8",
    )

    report = backfill_replay_summaries(
        source_root,
        limit=10,
        max_age_hours=24 * 365 * 100,
        overwrite=False,
    )

    assert report.status == "warn"
    assert report.created_count == 0
    assert report.updated_count == 0
    assert report.skipped_count == 1
    assert report.runs[0].action == "skipped_existing"
