from __future__ import annotations

import json
from pathlib import Path

from crypto_collector.replay import (
    _kraken_book_crc32,
    backfill_replay_summaries,
    replay_depth_run,
    replay_depth_stream_run,
    replay_trades_run,
    replay_trades_stream_run,
)


# Real Kraken v2 `book` snapshot (BTC/USD, depth 10) captured from the live socket
# 2026-06-01, frozen as a golden CRC32 vector. If the checksum algorithm ever drifts
# from Kraken's spec, this fails — independent of any code that produced the data.
_KRAKEN_GOLDEN_CHECKSUM = 4017594139
_KRAKEN_GOLDEN_ASKS = [
    [71753.2, 0.827788], [71753.3, 0.49278855], [71754.9, 1.114559], [71757.4, 0.43581603],
    [71760.5, 0.0641], [71763.1, 0.4], [71764.8, 0.03490782], [71768.9, 0.001393],
    [71770.1, 0.09999999], [71770.3, 0.67008108],
]
_KRAKEN_GOLDEN_BIDS = [
    [71741.4, 0.0001], [71741.3, 0.06855], [71738.9, 5.1e-05], [71735.3, 5.1e-05],
    [71732.5, 1.39340999], [71732.3, 0.85375328], [71732.2, 0.663889], [71732.0, 1.114563],
    [71731.8, 5.1e-05], [71730.8, 5.039e-05],
]


def test_kraken_book_crc32_matches_real_capture() -> None:
    """Golden vector: the CRC32 of a real Kraken BTC/USD snapshot (price 1dp, qty 8dp)
    must reproduce the checksum Kraken sent."""
    bids = {p: q for p, q in _KRAKEN_GOLDEN_BIDS}
    asks = {p: q for p, q in _KRAKEN_GOLDEN_ASKS}
    assert _kraken_book_crc32(bids, asks, 1, 8) == _KRAKEN_GOLDEN_CHECKSUM


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


def _write_trades_run(
    run_path: Path,
    rows: list[dict],
) -> None:
    clean_path = run_path / "clean"
    clean_path.mkdir(parents=True)
    (clean_path / "events.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in rows),
        encoding="utf-8",
    )


def _trade_row(
    *,
    trade_id: int,
    price: float = 100.0,
    size: float = 0.5,
    exchange_time: str = "2026-04-06T00:00:00+00:00",
    received_at: str = "2026-04-06T00:00:00.100000+00:00",
) -> dict:
    return {
        "source": "binance",
        "product": "BTCUSDT",
        "channel": "trades",
        "event_type": "trade",
        "exchange_time": exchange_time,
        "received_at": received_at,
        "side": "buy",
        "price": price,
        "size": size,
        "trade_id": str(trade_id),
        "sequence": trade_id,
        "raw_type": "trade",
        "metadata": {"instrument_id": "spot:binance:BTCUSDT"},
    }


def test_replay_trades_run_marks_clean_stream_replayable(tmp_path: Path) -> None:
    run_path = tmp_path / "binance_trades" / "20260406_000000"
    _write_trades_run(
        run_path,
        [_trade_row(trade_id=i, price=100.0 + i * 0.01) for i in range(1, 6)],
    )

    summary = replay_trades_run(run_path)

    assert summary.replayable is True, summary
    assert summary.findings == []
    assert summary.event_count == 5
    assert summary.first_trade_id == 1
    assert summary.last_trade_id == 5
    assert summary.trade_id_gap_count == 0
    assert summary.non_monotonic_count == 0
    # summary file written to disk
    summary_path = run_path / "metrics" / "replay_summary.json"
    assert summary_path.exists()
    on_disk = json.loads(summary_path.read_text(encoding="utf-8"))
    assert on_disk["replay_type"] == "trades"
    assert on_disk["replayable"] is True


def test_replay_trades_run_flags_trade_id_gap(tmp_path: Path) -> None:
    run_path = tmp_path / "binance_trades" / "20260406_000001"
    _write_trades_run(
        run_path,
        [
            _trade_row(trade_id=1),
            _trade_row(trade_id=2),
            _trade_row(trade_id=10),  # gap: missing 3..9
        ],
    )

    summary = replay_trades_run(run_path)

    assert summary.replayable is False
    assert "trade_id_gaps" in summary.findings
    assert summary.trade_id_gap_count == 1
    assert summary.trade_id_gap_total_missing == 7


def test_replay_trades_run_flags_non_monotonic_trade_id(tmp_path: Path) -> None:
    run_path = tmp_path / "binance_trades" / "20260406_000002"
    _write_trades_run(
        run_path,
        [
            _trade_row(trade_id=5),
            _trade_row(trade_id=3),  # backwards from 5
            _trade_row(trade_id=2),  # backwards from 3
            _trade_row(trade_id=10),  # forwards (gap) but monotonic
        ],
    )

    summary = replay_trades_run(run_path)

    assert summary.replayable is False
    assert "non_monotonic_trade_ids" in summary.findings
    # 5→3 backwards, 3→2 backwards → 2 non-monotonic transitions
    assert summary.non_monotonic_count == 2


def test_replay_trades_run_flags_invalid_price_and_size(tmp_path: Path) -> None:
    run_path = tmp_path / "binance_trades" / "20260406_000003"
    _write_trades_run(
        run_path,
        [
            _trade_row(trade_id=1, price=100.0, size=1.0),
            _trade_row(trade_id=2, price=0.0, size=1.0),  # zero price
            _trade_row(trade_id=3, price=100.0, size=-0.5),  # negative size
        ],
    )

    summary = replay_trades_run(run_path)

    assert summary.replayable is False
    assert "invalid_prices" in summary.findings
    assert "invalid_sizes" in summary.findings
    assert summary.invalid_price_count == 1
    assert summary.invalid_size_count == 1


def test_replay_trades_run_flags_excessive_clock_skew(tmp_path: Path) -> None:
    run_path = tmp_path / "binance_trades" / "20260406_000004"
    # received_at is 2 minutes after exchange_time → 120_000ms skew, exceeds 60_000 default
    _write_trades_run(
        run_path,
        [
            _trade_row(
                trade_id=1,
                exchange_time="2026-04-06T00:00:00+00:00",
                received_at="2026-04-06T00:02:00+00:00",
            ),
        ],
    )

    summary = replay_trades_run(run_path, max_clock_skew_ms=60_000.0)

    assert summary.replayable is False
    assert "excessive_clock_skew" in summary.findings
    assert summary.excessive_clock_skew_count == 1
    assert summary.max_clock_skew_ms is not None
    assert summary.max_clock_skew_ms >= 119_000  # ~120s skew


def test_replay_trades_run_with_no_events_is_unreplayable(tmp_path: Path) -> None:
    run_path = tmp_path / "binance_trades" / "20260406_000005"
    _write_trades_run(run_path, [])

    summary = replay_trades_run(run_path)

    assert summary.replayable is False
    assert summary.event_count == 0
    assert "no_events" in summary.findings


def test_replay_trades_run_writes_summary_compatible_with_quarantine_chain(tmp_path: Path) -> None:
    """The trades replay summary must use the same {replayable, findings} shape as
    `replay_depth_run` so quarantine_bad_runs and promote_replayable_runs can act on
    it unchanged."""
    run_path = tmp_path / "binance_trades" / "20260406_000006"
    _write_trades_run(
        run_path,
        [
            _trade_row(trade_id=1),
            _trade_row(trade_id=100),  # huge gap → unreplayable
        ],
    )

    replay_trades_run(run_path)

    summary_json = json.loads(
        (run_path / "metrics" / "replay_summary.json").read_text(encoding="utf-8")
    )
    # The two keys the curation chain reads
    assert isinstance(summary_json["replayable"], bool)
    assert isinstance(summary_json["findings"], list)
    assert summary_json["replayable"] is False
    assert "trade_id_gaps" in summary_json["findings"]


# --- Phase 2 #3b: non-sequence depth replay (Coinbase level2, STANDARDS 4.3) ---


def _write_stream_depth_run(run_path: Path, rows: list[dict]) -> None:
    clean_path = run_path / "clean"
    clean_path.mkdir(parents=True)
    (clean_path / "events.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in rows),
        encoding="utf-8",
    )


def _stream_snapshot_row(
    *,
    bids: list[list[float]],
    asks: list[list[float]],
    received_at: str = "2026-04-06T00:00:00.100000+00:00",
) -> dict:
    # The in-stream snapshot carries the full book and, like the live Coinbase feed,
    # has no exchange `event_time` — so it's skipped from the monotonic-time check.
    return {
        "source": "coinbase",
        "product": "BTC-USD",
        "channel": "depth",
        "event_type": "snapshot",
        "event_time": None,
        "received_at": received_at,
        "first_update_id": None,
        "final_update_id": None,
        "instrument": {"instrument_id": "spot:coinbase:BTCUSD"},
        "bids": bids,
        "asks": asks,
    }


def _stream_l2update_row(
    *,
    bids: list[list[float]] | None = None,
    asks: list[list[float]] | None = None,
    event_time: str = "2026-04-06T00:00:01+00:00",
    received_at: str = "2026-04-06T00:00:01.100000+00:00",
) -> dict:
    return {
        "source": "coinbase",
        "product": "BTC-USD",
        "channel": "depth",
        "event_type": "l2update",
        "event_time": event_time,
        "received_at": received_at,
        "first_update_id": None,
        "final_update_id": None,
        "instrument": {"instrument_id": "spot:coinbase:BTCUSD"},
        "bids": bids or [],
        "asks": asks or [],
    }


def test_replay_depth_stream_run_clean_snapshot_is_replayable_none_native(tmp_path: Path) -> None:
    """A single in-stream snapshot followed by monotonic l2updates is structurally
    clean, so it's `replayable` — but tagged `none_native` because gaplessness can't
    be proven without a sequence (STANDARDS 4.3)."""
    run_path = tmp_path / "coinbase_depth" / "20260406_000000"
    _write_stream_depth_run(
        run_path,
        [
            _stream_snapshot_row(bids=[[100.0, 1.0]], asks=[[101.0, 2.0]]),
            _stream_l2update_row(
                bids=[[100.0, 0.0], [99.0, 3.0]],
                event_time="2026-04-06T00:00:01+00:00",
            ),
            _stream_l2update_row(
                asks=[[101.0, 1.5]],
                event_time="2026-04-06T00:00:02+00:00",
            ),
        ],
    )

    summary = replay_depth_stream_run(run_path)

    assert summary.replayable is True, summary.findings
    assert summary.gap_detection == "none_native"
    assert summary.mode == "stream_snapshot"
    assert summary.event_count == 3
    # Gaplessness is unprovable: the sequence-only counters stay zero/neutral.
    assert summary.gap_count == 0
    assert summary.snapshot_gap_count == 0
    assert summary.reordered_count == 0
    assert summary.invalid_range_count == 0
    assert summary.findings == []
    # The summary the curation chain reads is written with the downgraded tag.
    summary_json = json.loads(
        (run_path / "metrics" / "replay_summary.json").read_text(encoding="utf-8")
    )
    assert summary_json["replayable"] is True
    assert summary_json["gap_detection"] == "none_native"


def _bybit_stream_row(
    *,
    event_type: str,
    update_id: int | None,
    bids: list[list[float]] | None = None,
    asks: list[list[float]] | None = None,
    event_time: str | None = None,
) -> dict:
    # Mirrors a normalized Bybit orderbook event: the dense per-symbol id lives in
    # metadata["bybit_update_id"], not the event-level first/final_update_id.
    metadata: dict = {}
    if update_id is not None:
        metadata["bybit_update_id"] = update_id
    return {
        "source": "bybit",
        "product": "BTCUSDT",
        "channel": "depth",
        "event_type": event_type,
        "event_time": event_time,
        "received_at": "2026-04-06T00:00:00.100000+00:00",
        "first_update_id": None,
        "final_update_id": None,
        "instrument": {"instrument_id": "spot:bybit:BTCUSDT"},
        "bids": bids or [],
        "asks": asks or [],
        "metadata": metadata,
    }


def test_replay_depth_stream_run_sequence_contiguous_is_gap_proof(tmp_path: Path) -> None:
    """With sequence_metadata_key set (Bybit), a contiguous +1 update id across the run
    upgrades the verdict from none_native to a provable `sequence` gap proof."""
    run_path = tmp_path / "bybit_depth" / "20260406_000000"
    _write_stream_depth_run(
        run_path,
        [
            _bybit_stream_row(event_type="snapshot", update_id=100, bids=[[100.0, 1.0]], asks=[[101.0, 2.0]]),
            _bybit_stream_row(event_type="delta", update_id=101, bids=[[100.0, 0.0]], event_time="2026-04-06T00:00:01+00:00"),
            _bybit_stream_row(event_type="delta", update_id=102, asks=[[101.0, 1.5]], event_time="2026-04-06T00:00:02+00:00"),
        ],
    )

    summary = replay_depth_stream_run(run_path, sequence_metadata_key="bybit_update_id")

    assert summary.replayable is True, summary.findings
    assert summary.gap_detection == "sequence"
    assert summary.mode == "stream_snapshot_sequence"
    assert summary.first_update_id == 100
    assert summary.last_update_id == 102
    assert summary.gap_count == 0
    assert summary.reordered_count == 0
    assert summary.findings == []


def test_replay_depth_stream_run_sequence_gap_blocks_promotion(tmp_path: Path) -> None:
    """A jump in the update id (a dropped message) is now provable, so it flags
    `update_id_gaps` and is NOT replayable."""
    run_path = tmp_path / "bybit_depth" / "20260406_000001"
    _write_stream_depth_run(
        run_path,
        [
            _bybit_stream_row(event_type="snapshot", update_id=100, bids=[[100.0, 1.0]], asks=[[101.0, 2.0]]),
            _bybit_stream_row(event_type="delta", update_id=104, asks=[[101.0, 1.5]], event_time="2026-04-06T00:00:02+00:00"),
        ],
    )

    summary = replay_depth_stream_run(run_path, sequence_metadata_key="bybit_update_id")

    assert summary.replayable is False
    assert summary.gap_detection == "sequence"
    assert "update_id_gaps" in summary.findings
    assert summary.gap_count == 1


def test_replay_depth_stream_run_sequence_reorder_blocks_promotion(tmp_path: Path) -> None:
    """A backwards/duplicate update id (reorder or service reset) flags
    `non_monotonic_update_id` and blocks promotion."""
    run_path = tmp_path / "bybit_depth" / "20260406_000002"
    _write_stream_depth_run(
        run_path,
        [
            _bybit_stream_row(event_type="snapshot", update_id=100, bids=[[100.0, 1.0]], asks=[[101.0, 2.0]]),
            _bybit_stream_row(event_type="delta", update_id=100, asks=[[101.0, 1.5]], event_time="2026-04-06T00:00:02+00:00"),
        ],
    )

    summary = replay_depth_stream_run(run_path, sequence_metadata_key="bybit_update_id")

    assert summary.replayable is False
    assert "non_monotonic_update_id" in summary.findings
    assert summary.reordered_count == 1


def test_replay_depth_stream_run_without_sequence_key_stays_none_native(tmp_path: Path) -> None:
    """The same events WITHOUT a sequence_metadata_key keep the none_native verdict —
    the upgrade is strictly opt-in, so Coinbase/Kraken depth are unaffected."""
    run_path = tmp_path / "bybit_depth" / "20260406_000003"
    _write_stream_depth_run(
        run_path,
        [
            _bybit_stream_row(event_type="snapshot", update_id=100, bids=[[100.0, 1.0]], asks=[[101.0, 2.0]]),
            _bybit_stream_row(event_type="delta", update_id=104, asks=[[101.0, 1.5]], event_time="2026-04-06T00:00:02+00:00"),
        ],
    )

    summary = replay_depth_stream_run(run_path)  # no key

    assert summary.gap_detection == "none_native"
    assert summary.mode == "stream_snapshot"
    assert summary.replayable is True  # gap is invisible without the sequence
    assert "update_id_gaps" not in summary.findings


def _kraken_stream_row(
    *,
    event_type: str,
    checksum: int | None,
    bids: list[list[float]] | None = None,
    asks: list[list[float]] | None = None,
    event_time: str | None = None,
) -> dict:
    metadata: dict = {}
    if checksum is not None:
        metadata["kraken_checksum"] = checksum
    return {
        "source": "kraken",
        "product": "BTC/USD",
        "channel": "depth",
        "event_type": event_type,
        "event_time": event_time,
        "received_at": "2026-04-06T00:00:00.100000+00:00",
        "first_update_id": None,
        "final_update_id": None,
        "instrument": {"instrument_id": "spot:kraken:BTCUSD"},
        "bids": bids or [],
        "asks": asks or [],
        "metadata": metadata,
    }


def test_replay_depth_stream_run_checksum_validates_clean(tmp_path: Path) -> None:
    """With checksum_metadata_key + precision (Kraken), a run whose every per-frame CRC
    matches the reconstructed top-10 book is `gap_detection="checksum"` and replayable."""
    # Compute the venue checksum each frame would carry, from the post-apply book state.
    cs_snap = _kraken_book_crc32({100.0: 1.0}, {101.0: 2.0}, 1, 8)
    cs_upd = _kraken_book_crc32({100.0: 1.0, 99.0: 3.0}, {101.0: 1.5}, 1, 8)
    run_path = tmp_path / "kraken_depth" / "20260406_000000"
    _write_stream_depth_run(
        run_path,
        [
            _kraken_stream_row(event_type="snapshot", checksum=cs_snap, bids=[[100.0, 1.0]], asks=[[101.0, 2.0]]),
            _kraken_stream_row(
                event_type="update", checksum=cs_upd,
                bids=[[99.0, 3.0]], asks=[[101.0, 1.5]],
                event_time="2026-04-06T00:00:01+00:00",
            ),
        ],
    )

    summary = replay_depth_stream_run(
        run_path,
        checksum_metadata_key="kraken_checksum",
        checksum_price_precision=1,
        checksum_qty_precision=8,
    )

    assert summary.replayable is True, summary.findings
    assert summary.gap_detection == "checksum"
    assert summary.mode == "stream_snapshot_checksum"
    assert summary.findings == []


def test_replay_depth_stream_run_checksum_mismatch_blocks_promotion(tmp_path: Path) -> None:
    """A wrong checksum (a dropped/corrupted update would diverge the local book) flags
    `checksum_mismatch` and blocks promotion."""
    cs_snap = _kraken_book_crc32({100.0: 1.0}, {101.0: 2.0}, 1, 8)
    run_path = tmp_path / "kraken_depth" / "20260406_000001"
    _write_stream_depth_run(
        run_path,
        [
            _kraken_stream_row(event_type="snapshot", checksum=cs_snap, bids=[[100.0, 1.0]], asks=[[101.0, 2.0]]),
            _kraken_stream_row(
                event_type="update", checksum=999999,  # wrong
                bids=[[99.0, 3.0]], asks=[[101.0, 1.5]],
                event_time="2026-04-06T00:00:01+00:00",
            ),
        ],
    )

    summary = replay_depth_stream_run(
        run_path,
        checksum_metadata_key="kraken_checksum",
        checksum_price_precision=1,
        checksum_qty_precision=8,
    )

    assert summary.replayable is False
    assert summary.gap_detection == "checksum"
    assert "checksum_mismatch" in summary.findings


def test_replay_depth_stream_run_no_snapshot_is_not_replayable(tmp_path: Path) -> None:
    """Without the in-stream snapshot anchor the book can't be seeded, so the run is
    not replayable even though the diffs are monotonic."""
    run_path = tmp_path / "coinbase_depth" / "20260406_000001"
    _write_stream_depth_run(
        run_path,
        [
            _stream_l2update_row(bids=[[100.0, 1.0]], event_time="2026-04-06T00:00:01+00:00"),
            _stream_l2update_row(asks=[[101.0, 1.0]], event_time="2026-04-06T00:00:02+00:00"),
        ],
    )

    summary = replay_depth_stream_run(run_path)

    assert summary.replayable is False
    assert "no_snapshot_anchor" in summary.findings
    assert summary.gap_detection == "none_native"


def test_replay_depth_stream_run_second_snapshot_breaks_replay(tmp_path: Path) -> None:
    """A reconnect mid-run yields a *second* in-stream snapshot. Like Binance depth's
    single-anchor invariant, two anchors mean the run isn't one continuous book, so
    it's flagged unreplayable and the worker's next segment starts fresh."""
    run_path = tmp_path / "coinbase_depth" / "20260406_000002"
    _write_stream_depth_run(
        run_path,
        [
            _stream_snapshot_row(bids=[[100.0, 1.0]], asks=[[101.0, 2.0]]),
            _stream_l2update_row(bids=[[99.0, 3.0]], event_time="2026-04-06T00:00:01+00:00"),
            _stream_snapshot_row(
                bids=[[100.0, 1.0]],
                asks=[[101.0, 2.0]],
                received_at="2026-04-06T00:00:02.100000+00:00",
            ),
        ],
    )

    summary = replay_depth_stream_run(run_path)

    assert summary.replayable is False
    assert "multiple_snapshot_anchors" in summary.findings
    assert "snapshot_not_first_event" in summary.findings


def test_replay_depth_stream_run_non_monotonic_event_time_breaks_replay(tmp_path: Path) -> None:
    """Diff timestamps must not go backwards. The snapshot has no exchange time so it's
    skipped; the second l2update predates the first, which is the violation."""
    run_path = tmp_path / "coinbase_depth" / "20260406_000003"
    _write_stream_depth_run(
        run_path,
        [
            _stream_snapshot_row(bids=[[100.0, 1.0]], asks=[[101.0, 2.0]]),
            _stream_l2update_row(bids=[[99.0, 3.0]], event_time="2026-04-06T00:00:05+00:00"),
            _stream_l2update_row(asks=[[101.0, 1.5]], event_time="2026-04-06T00:00:02+00:00"),
        ],
    )

    summary = replay_depth_stream_run(run_path)

    assert summary.replayable is False
    assert "non_monotonic_event_time" in summary.findings


def test_replay_depth_stream_run_with_no_events_is_unreplayable(tmp_path: Path) -> None:
    run_path = tmp_path / "coinbase_depth" / "20260406_000004"
    _write_stream_depth_run(run_path, [])

    summary = replay_depth_stream_run(run_path)

    assert summary.replayable is False
    assert summary.event_count == 0
    assert "no_events" in summary.findings
    assert "no_snapshot_anchor" in summary.findings
    assert summary.gap_detection == "none_native"


def test_replay_depth_stream_run_reports_crossed_book_without_blocking(tmp_path: Path) -> None:
    """A crossed book (best bid >= best ask) is surfaced as a finding for visibility
    but, consistent with `replay_depth_run`, does not by itself block promotion."""
    run_path = tmp_path / "coinbase_depth" / "20260406_000005"
    _write_stream_depth_run(
        run_path,
        [
            # Snapshot is already crossed: bid 102 >= ask 101.
            _stream_snapshot_row(bids=[[102.0, 1.0]], asks=[[101.0, 2.0]]),
            _stream_l2update_row(bids=[[100.0, 1.0]], event_time="2026-04-06T00:00:01+00:00"),
        ],
    )

    summary = replay_depth_stream_run(run_path)

    assert summary.crossed_book_count >= 1
    assert "crossed_book_states" in summary.findings
    assert summary.replayable is True  # reported, not gating


# --- Phase 2 #3c: non-sequence trades replay (Bybit spot, STANDARDS 4.3) ---


def _stream_trade_row(
    *,
    price: float = 100.0,
    size: float = 0.5,
    side: str = "buy",
    exchange_time: str = "2026-04-06T00:00:00+00:00",
    received_at: str = "2026-04-06T00:00:00.100000+00:00",
    trade_id: str = "uuid-aaaa",
) -> dict:
    """A Bybit-shaped none_native trade row: a UUID trade_id and NO dense `sequence`,
    so the only ordering signal is the exchange timestamp."""
    return {
        "source": "bybit",
        "product": "BTCUSDT",
        "channel": "trades",
        "event_type": "trade",
        "exchange_time": exchange_time,
        "received_at": received_at,
        "side": side,
        "price": price,
        "size": size,
        "trade_id": trade_id,
        "sequence": None,
        "raw_type": "trade",
        "metadata": {"instrument_id": "spot:bybit:BTCUSDT"},
    }


def test_replay_trades_stream_run_marks_clean_stream_replayable_none_native(tmp_path: Path) -> None:
    """A structurally clean none_native trade run is replayable, but the summary makes
    clear gaplessness is NOT proven: gap_detection='none_native', no trade_id checks."""
    run_path = tmp_path / "bybit_trades" / "20260406_000000"
    _write_trades_run(
        run_path,
        [
            _stream_trade_row(
                trade_id=f"uuid-{i}",
                price=100.0 + i * 0.01,
                exchange_time=f"2026-04-06T00:00:0{i}+00:00",
                received_at=f"2026-04-06T00:00:0{i}.100000+00:00",
            )
            for i in range(1, 6)
        ],
    )

    summary = replay_trades_stream_run(run_path)

    assert summary.replayable is True, summary
    assert summary.findings == []
    assert summary.event_count == 5
    assert summary.gap_detection == "none_native"
    assert summary.mode == "trade_stream_none_native"
    # No dense counter, so trade-id fields are neutral.
    assert summary.first_trade_id is None
    assert summary.last_trade_id is None
    assert summary.trade_id_gap_count == 0
    # Summary file written + curation-chain compatible.
    on_disk = json.loads(
        (run_path / "metrics" / "replay_summary.json").read_text(encoding="utf-8")
    )
    assert on_disk["replay_type"] == "trades"
    assert on_disk["gap_detection"] == "none_native"
    assert on_disk["replayable"] is True


def test_replay_trades_stream_run_does_not_flag_unordered_trade_ids(tmp_path: Path) -> None:
    """The defining difference from replay_trades_run: out-of-order / duplicate UUID
    trade ids are NOT a finding here, because there is no dense counter to gap-check.
    Only the timestamp ordering matters."""
    run_path = tmp_path / "bybit_trades" / "20260406_000001"
    _write_trades_run(
        run_path,
        [
            _stream_trade_row(trade_id="uuid-zzz", exchange_time="2026-04-06T00:00:01+00:00",
                              received_at="2026-04-06T00:00:01.050000+00:00"),
            _stream_trade_row(trade_id="uuid-aaa", exchange_time="2026-04-06T00:00:02+00:00",
                              received_at="2026-04-06T00:00:02.050000+00:00"),
        ],
    )

    summary = replay_trades_stream_run(run_path)

    assert summary.replayable is True, summary
    assert summary.findings == []
    assert summary.trade_id_gap_count == 0
    assert summary.non_monotonic_count == 0


def test_replay_trades_stream_run_flags_non_monotonic_event_time(tmp_path: Path) -> None:
    """Timestamp going backwards is the one ordering violation a none_native trade
    stream can detect, and it blocks replay."""
    run_path = tmp_path / "bybit_trades" / "20260406_000002"
    _write_trades_run(
        run_path,
        [
            _stream_trade_row(exchange_time="2026-04-06T00:00:05+00:00",
                              received_at="2026-04-06T00:00:05.050000+00:00"),
            _stream_trade_row(exchange_time="2026-04-06T00:00:02+00:00",  # backwards
                              received_at="2026-04-06T00:00:02.050000+00:00"),
        ],
    )

    summary = replay_trades_stream_run(run_path)

    assert summary.replayable is False
    assert "non_monotonic_event_time" in summary.findings
    assert summary.non_monotonic_count == 1


def test_replay_trades_stream_run_flags_invalid_price_and_size(tmp_path: Path) -> None:
    run_path = tmp_path / "bybit_trades" / "20260406_000003"
    _write_trades_run(
        run_path,
        [
            _stream_trade_row(price=100.0, size=1.0,
                              exchange_time="2026-04-06T00:00:01+00:00",
                              received_at="2026-04-06T00:00:01.050000+00:00"),
            _stream_trade_row(price=0.0, size=1.0,  # zero price
                              exchange_time="2026-04-06T00:00:02+00:00",
                              received_at="2026-04-06T00:00:02.050000+00:00"),
            _stream_trade_row(price=100.0, size=-0.5,  # negative size
                              exchange_time="2026-04-06T00:00:03+00:00",
                              received_at="2026-04-06T00:00:03.050000+00:00"),
        ],
    )

    summary = replay_trades_stream_run(run_path)

    assert summary.replayable is False
    assert "invalid_prices" in summary.findings
    assert "invalid_sizes" in summary.findings
    assert summary.invalid_price_count == 1
    assert summary.invalid_size_count == 1


def test_replay_trades_stream_run_flags_excessive_clock_skew(tmp_path: Path) -> None:
    run_path = tmp_path / "bybit_trades" / "20260406_000004"
    _write_trades_run(
        run_path,
        [
            _stream_trade_row(
                exchange_time="2026-04-06T00:00:00+00:00",
                received_at="2026-04-06T00:02:00+00:00",  # 120s skew > 60s default
            ),
        ],
    )

    summary = replay_trades_stream_run(run_path, max_clock_skew_ms=60_000.0)

    assert summary.replayable is False
    assert "excessive_clock_skew" in summary.findings
    assert summary.excessive_clock_skew_count == 1


def test_replay_trades_stream_run_with_no_events_is_unreplayable(tmp_path: Path) -> None:
    run_path = tmp_path / "bybit_trades" / "20260406_000005"
    _write_trades_run(run_path, [])

    summary = replay_trades_stream_run(run_path)

    assert summary.replayable is False
    assert summary.event_count == 0
    assert "no_events" in summary.findings
    assert summary.gap_detection == "none_native"
