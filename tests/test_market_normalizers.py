from __future__ import annotations

from datetime import UTC, datetime

from crypto_collector.market_normalizers import BinanceDepthNormalizer, BinanceTradeNormalizer
from crypto_collector.models import RawMessage
from crypto_collector.quality import MetadataQualityGate, QualityGate


def test_binance_depth_normalizer_parses_levels_and_ids() -> None:
    normalizer = BinanceDepthNormalizer()
    raw = RawMessage(
        source="binance",
        received_at=datetime.now(tz=UTC),
        payload={
            "e": "depthUpdate",
            "E": 1672515782136,
            "s": "BTCUSDT",
            "U": 157,
            "u": 160,
            "b": [["100.1", "1.25"]],
            "a": [["100.2", "0.5"]],
        },
    )
    event = normalizer.normalize(raw)
    assert event.product == "BTCUSDT"
    assert event.instrument is not None
    assert event.instrument.instrument_id == "spot:binance:BTCUSDT"
    assert event.first_update_id == 157
    assert event.final_update_id == 160
    assert event.bids == [[100.1, 1.25]]
    assert event.asks == [[100.2, 0.5]]


def test_metadata_quality_gate_rejects_invalid_update_range() -> None:
    normalizer = BinanceDepthNormalizer()
    gate = MetadataQualityGate()
    raw = RawMessage(
        source="binance",
        received_at=datetime.now(tz=UTC),
        payload={
            "e": "depthUpdate",
            "E": 1672515782136,
            "s": "BTCUSDT",
            "U": 200,
            "u": 100,
            "b": [],
            "a": [],
        },
    )
    event = normalizer.normalize(raw)
    result = gate.validate(event)
    assert result.accepted is False
    assert "invalid_update_range" in result.reasons


def test_binance_trade_normalizer_maps_trade_payload_to_l3_event() -> None:
    normalizer = BinanceTradeNormalizer()
    gate = QualityGate(max_delay_ms=60_000)
    now_ms = int(datetime.now(tz=UTC).timestamp() * 1000)
    raw = RawMessage(
        source="binance",
        received_at=datetime.now(tz=UTC),
        payload={
            "e": "trade",
            "E": now_ms,
            "T": now_ms - 1,
            "s": "BTCUSDT",
            "t": 12345,
            "p": "100.25",
            "q": "0.40",
            "m": False,
        },
    )

    event = normalizer.normalize(raw)
    verdict = gate.validate(event)

    assert event.product == "BTCUSDT"
    assert event.channel == "trades"
    assert event.event_type == "trade"
    assert event.trade_id == "12345"
    assert event.sequence == 12345
    assert event.side == "buy"
    assert event.price == 100.25
    assert event.size == 0.40
    assert event.metadata["instrument_id"] == "spot:binance:BTCUSDT"
    assert verdict.accepted is True


def test_binance_trade_normalizer_preserves_exact_millisecond_in_exchange_time() -> None:
    normalizer = BinanceTradeNormalizer()
    # Pick a millisecond near the current epoch that would round-trip badly via
    # float seconds. With float division we'd get a microsecond offset; with
    # integer microsecond arithmetic the conversion is lossless.
    trade_time_ms = 1_700_000_000_001
    raw = RawMessage(
        source="binance",
        received_at=datetime(2026, 1, 1, tzinfo=UTC),
        payload={
            "e": "trade",
            "E": trade_time_ms,
            "T": trade_time_ms,
            "s": "BTCUSDT",
            "t": 1,
            "p": "1",
            "q": "1",
            "m": False,
        },
    )
    event = normalizer.normalize(raw)
    assert event.exchange_time is not None
    # Microsecond field must be exactly 1ms = 1000us, with zero float noise.
    assert event.exchange_time.microsecond == 1000


def test_binance_trade_normalizer_maps_agg_trade_side_from_buyer_maker_flag() -> None:
    normalizer = BinanceTradeNormalizer()
    now_ms = int(datetime.now(tz=UTC).timestamp() * 1000)
    raw = RawMessage(
        source="binance",
        received_at=datetime.now(tz=UTC),
        payload={
            "e": "aggTrade",
            "E": now_ms,
            "T": now_ms - 1,
            "s": "BTCUSDT",
            "a": 998,
            "p": "99.75",
            "q": "1.25",
            "m": True,
        },
    )

    event = normalizer.normalize(raw)

    assert event.event_type == "aggTrade"
    assert event.trade_id == "998"
    assert event.sequence == 998
    assert event.side == "sell"
