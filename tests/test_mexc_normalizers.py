"""Unit tests for MEXC spot v3 message normalization (protobuf transport).

These build **binary protobuf fixture frames** with the vendored, generated
bindings (the same `PushDataV3ApiWrapper` the live socket sends), decode them via
`decode_mexc_frame`, and assert the normalized output. They validate the
decode + normalize logic end to end; they do NOT prove MEXC's live field numbers
match the vendored `.proto` (that is the documented pre-rollout live-frame
verification gate - see proto/mexc/README.md).
"""

from __future__ import annotations

import base64
import hashlib
from datetime import UTC, datetime, timedelta

from crypto_collector.collectors.mexc import (
    MEXC_DEALS_CHANNEL,
    MEXC_LIMIT_DEPTH_CHANNEL,
    build_deals_topic,
    build_limit_depth_topic,
    decode_mexc_frame,
)
from crypto_collector.collectors.mexc_pb import PushDataV3ApiWrapper_pb2 as wrapper_pb2
from crypto_collector.market_normalizers import MexcDepthNormalizer, MexcTradeNormalizer
from crypto_collector.models import RawMessage
from crypto_collector.quality import MetadataQualityGate, QualityGate

_BASE = datetime(2026, 6, 1, 0, 0, 0, tzinfo=UTC)


def _ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _deals_frame(
    *,
    deals: list[tuple[str, str, int, int]],
    symbol: str = "BTCUSDT",
    send_time: int | None = None,
    channel: str = "spot@public.aggre.deals.v3.api.pb@100ms@BTCUSDT",
) -> bytes:
    msg = wrapper_pb2.PushDataV3ApiWrapper()
    msg.channel = channel
    msg.symbol = symbol
    if send_time is not None:
        msg.sendTime = send_time
    for price, quantity, trade_type, t in deals:
        item = msg.publicAggreDeals.deals.add()
        item.price = price
        item.quantity = quantity
        item.tradeType = trade_type
        item.time = t
    msg.publicAggreDeals.eventType = "spot@public.aggre.deals.v3.api"
    return msg.SerializeToString()


def _limit_depth_frame(
    *,
    bids: list[tuple[str, str]],
    asks: list[tuple[str, str]],
    version: str,
    symbol: str = "BTCUSDT",
    send_time: int | None = None,
    channel: str = "spot@public.limit.depth.v3.api.pb@BTCUSDT@20",
) -> bytes:
    msg = wrapper_pb2.PushDataV3ApiWrapper()
    msg.channel = channel
    msg.symbol = symbol
    if send_time is not None:
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


def _raw(frame: bytes, *, received_at: datetime) -> RawMessage:
    return RawMessage(source="mexc", received_at=received_at, payload=decode_mexc_frame(frame))


# --- decode + provenance -------------------------------------------------------


def test_decode_mexc_frame_shape_and_provenance() -> None:
    frame = _deals_frame(deals=[("71753.2", "0.01", 1, _ms(_BASE))], send_time=_ms(_BASE))
    payload = decode_mexc_frame(frame)

    assert payload["channel"] == "spot@public.aggre.deals.v3.api.pb@100ms@BTCUSDT"
    assert payload["symbol"] == "BTCUSDT"
    # int64 fields come back as decimal strings per proto3 JSON mapping.
    assert payload["sendTime"] == str(_ms(_BASE))
    assert "publicAggreDeals" in payload
    prov = payload["_mexc_decode"]
    assert prov["schema"] == "PushDataV3ApiWrapper"
    assert prov["proto_source"] == "github.com/mexcdevelop/websocket-proto"
    assert prov["decoder_version"] == 1
    assert prov["frame_bytes"] == len(frame)
    assert prov["frame_sha256"] == hashlib.sha256(frame).hexdigest()


def test_decode_mexc_frame_b64_roundtrips_to_original_bytes() -> None:
    """The raw archive stays a true rebuild source: the base64 in provenance must
    decode back to the exact wire frame so a consumer can re-decode from raw alone."""
    frame = _limit_depth_frame(bids=[("100.0", "1.0")], asks=[("101.0", "2.0")], version="42", send_time=_ms(_BASE))
    payload = decode_mexc_frame(frame)
    assert base64.b64decode(payload["_mexc_decode"]["frame_b64"]) == frame


# --- trades --------------------------------------------------------------------


def test_mexc_trade_normalizer_maps_taker_side_and_fans_out_batch() -> None:
    """One frame batches many deals; tradeType 1 -> buy, 2 -> sell (taker side)."""
    frame = _deals_frame(
        deals=[
            ("71753.2", "0.01", 1, _ms(_BASE)),
            ("71750.0", "0.02", 2, _ms(_BASE + timedelta(milliseconds=5))),
        ],
        send_time=_ms(_BASE),
    )
    events = MexcTradeNormalizer().normalize_many(_raw(frame, received_at=_BASE))

    assert len(events) == 2
    buy, sell = events
    assert buy.product == "BTCUSDT"
    assert buy.channel == "trades"
    assert buy.event_type == "trade"
    assert buy.side == "buy"
    assert buy.price == 71753.2
    assert buy.size == 0.01
    assert buy.metadata["instrument_id"] == "spot:mexc:BTCUSDT"
    assert buy.metadata["canonical_symbol"] == "BTC/USDT"
    assert buy.metadata["buyer_is_maker"] is False  # taker bought
    assert sell.side == "sell"
    assert sell.metadata["buyer_is_maker"] is True  # taker sold
    # Provenance link back to the raw protobuf frame.
    assert buy.metadata["mexc_frame_sha256"] == hashlib.sha256(frame).hexdigest()


def test_mexc_trade_normalizer_has_no_sequence_none_native() -> None:
    """The aggregated-deals stream carries no per-trade id, so there is no dense
    `sequence` to gap-check - this is what makes MEXC trades a none_native feed."""
    frame = _deals_frame(deals=[("100.0", "1.0", 1, _ms(_BASE))], send_time=_ms(_BASE))
    (event,) = MexcTradeNormalizer().normalize_many(_raw(frame, received_at=_BASE))
    assert event.sequence is None
    assert event.trade_id is None


def test_mexc_trade_quality_gate_accepts_clean_event() -> None:
    frame = _deals_frame(deals=[("100.25", "0.4", 1, _ms(_BASE))], send_time=_ms(_BASE))
    (event,) = MexcTradeNormalizer().normalize_many(
        _raw(frame, received_at=_BASE + timedelta(milliseconds=50))
    )
    verdict = QualityGate(max_delay_ms=60_000).validate(event)
    assert verdict.accepted is True, verdict.reasons


def test_mexc_trade_normalizer_unknown_trade_type_is_unknown_side_not_error() -> None:
    # tradeType 0 is proto3's default and is omitted on the wire -> side unknown (None),
    # which the gate tolerates (it only rejects a side that is neither buy/sell nor None).
    frame = _deals_frame(deals=[("100.0", "1.0", 0, _ms(_BASE))], send_time=_ms(_BASE))
    (event,) = MexcTradeNormalizer().normalize_many(_raw(frame, received_at=_BASE))
    assert event.side is None
    assert "parse_errors" not in event.metadata
    assert QualityGate().validate(event).accepted is True


# --- depth ---------------------------------------------------------------------


def test_mexc_depth_normalizer_emits_snapshot_with_levels_and_version() -> None:
    frame = _limit_depth_frame(
        bids=[("71741.4", "0.3"), ("71741.3", "0.1")],
        asks=[("71760.5", "0.5")],
        version="999123",
        send_time=_ms(_BASE),
    )
    event = MexcDepthNormalizer().normalize(_raw(frame, received_at=_BASE))

    assert event.product == "BTCUSDT"
    assert event.channel == "depth"
    # Every limit-depth frame is a full top-N book -> a snapshot anchor.
    assert event.event_type == "snapshot"
    assert event.bids == [[71741.4, 0.3], [71741.3, 0.1]]
    assert event.asks == [[71760.5, 0.5]]
    assert event.instrument is not None
    assert event.instrument.instrument_id == "spot:mexc:BTCUSDT"
    # The per-frame version is preserved as gap-detection metadata (STANDARDS 4.3).
    assert event.metadata["mexc_version"] == 999123
    assert event.metadata["mexc_channel"] == "spot@public.limit.depth.v3.api.pb@BTCUSDT@20"


def test_mexc_depth_normalizer_is_none_native_no_update_ids() -> None:
    """No first/final update id (the version is forensic metadata, not a U/u window),
    and the metadata quality gate accepts the event."""
    frame = _limit_depth_frame(bids=[("100.0", "1.0")], asks=[("101.0", "2.0")], version="7", send_time=_ms(_BASE))
    event = MexcDepthNormalizer().normalize(_raw(frame, received_at=_BASE))
    assert event.first_update_id is None
    assert event.final_update_id is None
    assert MetadataQualityGate().validate(event).accepted is True


def test_mexc_depth_normalizer_event_time_from_send_time() -> None:
    frame = _limit_depth_frame(bids=[("100.0", "1.0")], asks=[("101.0", "2.0")], version="1", send_time=_ms(_BASE))
    event = MexcDepthNormalizer().normalize(_raw(frame, received_at=_BASE))
    assert event.event_time == _BASE


# --- topic builders ------------------------------------------------------------


def test_build_topics_compose_full_subscription_strings() -> None:
    assert (
        build_deals_topic(channel=MEXC_DEALS_CHANNEL, symbol="btcusdt", interval="100ms")
        == "spot@public.aggre.deals.v3.api.pb@100ms@BTCUSDT"
    )
    assert (
        build_limit_depth_topic(channel=MEXC_LIMIT_DEPTH_CHANNEL, symbol="btcusdt", depth=20)
        == "spot@public.limit.depth.v3.api.pb@BTCUSDT@20"
    )


def test_mexc_depth_normalizer_quarantines_missing_body_instead_of_empty_book() -> None:
    """Regression: a frame without a publicLimitDepths body (e.g. a misdelivered deals
    frame that passed the emit filter) used to normalize into a CLEAN empty-book
    snapshot — wiping the reconstructed book with no quarantine trail. It must carry a
    parse error so MetadataQualityGate quarantines it."""
    raw = RawMessage(
        source="mexc",
        received_at=_BASE,
        payload={
            "channel": "spot@public.limit.depth.v3.api.pb@BTCUSDT@20",
            "symbol": "BTCUSDT",
            "publicAggreDeals": {"deals": []},  # wrong body for the depth lane
            "sendTime": str(int(_BASE.timestamp() * 1000)),
        },
    )

    event = MexcDepthNormalizer().normalize(raw)

    assert event.bids == [] and event.asks == []
    assert "missing_publicLimitDepths" in event.metadata["parse_errors"]
    assert MetadataQualityGate().validate(event).accepted is False
