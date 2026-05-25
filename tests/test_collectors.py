from __future__ import annotations

from pathlib import Path

from crypto_collector.cli import (
    _align_binance_buffered_events,
    _binance_update_window,
    build_parser,
    _is_retryable_connect_error,
)
from crypto_collector.collectors.generic_ws import GenericWebsocketCollector
from crypto_collector.config import CollectorConfig
from crypto_collector.models import RawMessage, utc_now


def make_collector(subscription_style: str) -> GenericWebsocketCollector:
    return GenericWebsocketCollector(
        CollectorConfig(
            source="test",
            output_root=Path("data"),
            product="BTCUSDT",
            channel="depth",
            websocket_url="wss://example.test",
            subscription_style=subscription_style,
        )
    )


def test_binance_ack_is_not_emitted() -> None:
    collector = make_collector("binance")
    assert collector._should_emit({"result": None, "id": 1}) is False


def test_binance_depth_event_is_emitted() -> None:
    collector = make_collector("binance")
    assert collector._should_emit({"e": "depthUpdate", "s": "BTCUSDT"}) is True


def test_coinbase_payload_is_emitted() -> None:
    collector = make_collector("coinbase")
    assert collector._should_emit({"type": "open", "product_id": "BTC-USD"}) is True


def test_binance_update_window_and_alignment_drop_stale_buffered_events() -> None:
    stale = RawMessage(
        source="binance",
        received_at=utc_now(),
        payload={"e": "depthUpdate", "U": 90, "u": 100},
    )
    bridging = RawMessage(
        source="binance",
        received_at=utc_now(),
        payload={"e": "depthUpdate", "U": 101, "u": 105},
    )
    future = RawMessage(
        source="binance",
        received_at=utc_now(),
        payload={"e": "depthUpdate", "U": 106, "u": 110},
    )

    assert _binance_update_window(bridging.payload) == (101, 105)
    aligned = _align_binance_buffered_events([stale, bridging, future], snapshot_last_update_id=100)

    assert aligned == [bridging, future]


def test_retryable_binance_connect_error_detects_handshake_timeout() -> None:
    assert _is_retryable_connect_error(RuntimeError("timed out during opening handshake")) is True
    assert _is_retryable_connect_error(RuntimeError("validation bug")) is False


def test_cli_binance_trades_worker_defaults_to_trade_channel() -> None:
    parser = build_parser()
    args = parser.parse_args(["binance-trades-worker"])
    assert args.channel == "trade"
