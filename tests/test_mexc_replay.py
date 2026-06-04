"""Replay-validation tests for MEXC lanes (STANDARDS 4.3 none_native).

End-to-end: build binary protobuf frames, decode + normalize through the real
MEXC path, write a run's clean/events.jsonl, then run the same replay verdict the
curation chain uses. Proves a clean MEXC depth run and trade run are `replayable`
(structurally clean) and tagged `none_native`, and that the run writes the
`metrics/replay_summary.json` contract the quarantine/promote chain reads.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from crypto_collector.collectors.mexc import decode_mexc_frame
from crypto_collector.collectors.mexc_pb import PushDataV3ApiWrapper_pb2 as wrapper_pb2
from crypto_collector.market_normalizers import MexcDepthNormalizer, MexcTradeNormalizer
from crypto_collector.models import RawMessage
from crypto_collector.replay import replay_depth_stream_run, replay_trades_stream_run

_BASE = datetime(2026, 6, 1, 0, 0, 0, tzinfo=UTC)


def _ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _deals_frame(*, deals: list[tuple[str, str, int, int]], send_time: int) -> bytes:
    msg = wrapper_pb2.PushDataV3ApiWrapper()
    msg.channel = "spot@public.aggre.deals.v3.api.pb@100ms@BTCUSDT"
    msg.symbol = "BTCUSDT"
    msg.sendTime = send_time
    for price, quantity, trade_type, t in deals:
        item = msg.publicAggreDeals.deals.add()
        item.price = price
        item.quantity = quantity
        item.tradeType = trade_type
        item.time = t
    return msg.SerializeToString()


def _limit_depth_frame(*, bids, asks, version: str, send_time: int) -> bytes:
    msg = wrapper_pb2.PushDataV3ApiWrapper()
    msg.channel = "spot@public.limit.depth.v3.api.pb@BTCUSDT@20"
    msg.symbol = "BTCUSDT"
    msg.sendTime = send_time
    for price, quantity in asks:
        item = msg.publicLimitDepths.asks.add()
        item.price = price
        item.quantity = quantity
    for price, quantity in bids:
        item = msg.publicLimitDepths.bids.add()
        item.price = price
        item.quantity = quantity
    msg.publicLimitDepths.version = version
    return msg.SerializeToString()


def _write_run(run_path: Path, rows: list[dict]) -> None:
    clean = run_path / "clean"
    clean.mkdir(parents=True)
    (clean / "events.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
    )


def _depth_rows(frames_with_received: list[tuple[bytes, datetime]]) -> list[dict]:
    normalizer = MexcDepthNormalizer()
    rows = []
    for frame, received_at in frames_with_received:
        raw = RawMessage(source="mexc", received_at=received_at, payload=decode_mexc_frame(frame))
        rows.append(normalizer.normalize(raw).to_dict())
    return rows


def _trade_rows(frames_with_received: list[tuple[bytes, datetime]]) -> list[dict]:
    normalizer = MexcTradeNormalizer()
    rows = []
    for frame, received_at in frames_with_received:
        raw = RawMessage(source="mexc", received_at=received_at, payload=decode_mexc_frame(frame))
        rows.extend(event.to_dict() for event in normalizer.normalize_many(raw))
    return rows


# --- depth ---------------------------------------------------------------------


def test_mexc_depth_run_is_replayable_none_native(tmp_path: Path) -> None:
    """A run of full-book limit-depth frames (each a snapshot, monotonic send times)
    is structurally clean -> replayable, tagged none_native."""
    run_path = tmp_path / "mexc_depth" / "20260601_000000"
    frames = [
        (
            _limit_depth_frame(
                bids=[("71741.4", "0.3")],
                asks=[("71760.5", "0.5")],
                version=str(1000 + i),
                send_time=_ms(_BASE + timedelta(seconds=i)),
            ),
            _BASE + timedelta(seconds=i, milliseconds=50),
        )
        for i in range(3)
    ]
    _write_run(run_path, _depth_rows(frames))

    summary = replay_depth_stream_run(run_path)

    assert summary.replayable is True, summary.findings
    assert summary.gap_detection == "none_native"
    assert summary.mode == "stream_snapshot"
    assert summary.event_count == 3
    assert summary.source == "mexc"
    assert summary.findings == []
    # The curation chain reads {replayable, findings} from the on-disk summary.
    on_disk = json.loads((run_path / "metrics" / "replay_summary.json").read_text(encoding="utf-8"))
    assert on_disk["replayable"] is True
    assert on_disk["gap_detection"] == "none_native"


def test_mexc_depth_run_non_monotonic_send_time_blocks_replay(tmp_path: Path) -> None:
    run_path = tmp_path / "mexc_depth" / "20260601_000001"
    frames = [
        (_limit_depth_frame(bids=[("100.0", "1.0")], asks=[("101.0", "2.0")], version="1",
                            send_time=_ms(_BASE + timedelta(seconds=5))), _BASE + timedelta(seconds=5)),
        (_limit_depth_frame(bids=[("100.0", "1.0")], asks=[("101.0", "2.0")], version="2",
                            send_time=_ms(_BASE + timedelta(seconds=2))),  # backwards
         _BASE + timedelta(seconds=2)),
    ]
    _write_run(run_path, _depth_rows(frames))

    summary = replay_depth_stream_run(run_path)

    assert summary.replayable is False
    assert "non_monotonic_event_time" in summary.findings
    assert summary.gap_detection == "none_native"


# --- trades --------------------------------------------------------------------


def test_mexc_trades_run_is_replayable_none_native(tmp_path: Path) -> None:
    """A clean batch of aggregated deals (monotonic times, valid price/size, low skew)
    is replayable and tagged none_native - no trade-id gap checks apply."""
    run_path = tmp_path / "mexc_trades" / "20260601_000000"
    frame = _deals_frame(
        deals=[
            ("71753.2", "0.01", 1, _ms(_BASE)),
            ("71753.3", "0.02", 2, _ms(_BASE + timedelta(milliseconds=10))),
            ("71754.0", "0.03", 1, _ms(_BASE + timedelta(milliseconds=20))),
        ],
        send_time=_ms(_BASE),
    )
    _write_run(run_path, _trade_rows([(frame, _BASE + timedelta(milliseconds=50))]))

    summary = replay_trades_stream_run(run_path)

    assert summary.replayable is True, summary.findings
    assert summary.gap_detection == "none_native"
    assert summary.mode == "trade_stream_none_native"
    assert summary.event_count == 3
    assert summary.source == "mexc"
    # No dense counter -> trade-id fields neutral.
    assert summary.first_trade_id is None
    assert summary.trade_id_gap_count == 0
    on_disk = json.loads((run_path / "metrics" / "replay_summary.json").read_text(encoding="utf-8"))
    assert on_disk["replay_type"] == "trades"
    assert on_disk["gap_detection"] == "none_native"
    assert on_disk["replayable"] is True


def test_mexc_trades_run_non_monotonic_time_blocks_replay(tmp_path: Path) -> None:
    run_path = tmp_path / "mexc_trades" / "20260601_000001"
    frame = _deals_frame(
        deals=[
            ("100.0", "1.0", 1, _ms(_BASE + timedelta(seconds=5))),
            ("100.0", "1.0", 2, _ms(_BASE + timedelta(seconds=2))),  # backwards in time
        ],
        send_time=_ms(_BASE),
    )
    _write_run(run_path, _trade_rows([(frame, _BASE + timedelta(seconds=5))]))

    summary = replay_trades_stream_run(run_path)

    assert summary.replayable is False
    assert "non_monotonic_event_time" in summary.findings
