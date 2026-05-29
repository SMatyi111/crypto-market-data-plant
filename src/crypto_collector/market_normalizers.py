from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

_EPOCH_UTC = datetime(1970, 1, 1, tzinfo=UTC)

from .asset_registry import resolve_spot_instrument
from .context_models import NormalizedDepthUpdate
from .models import NormalizedL3Event, RawMessage


class BinanceDepthNormalizer:
    def normalize(self, raw: RawMessage) -> NormalizedDepthUpdate:
        payload = raw.payload.get("data", raw.payload)
        parse_errors: list[str] = []
        product = str(payload.get("s") or "UNKNOWN")
        event_time = _parse_timestamp_ms(payload.get("E"), parse_errors)
        return NormalizedDepthUpdate(
            source=raw.source,
            product=product,
            channel="depth",
            event_type=str(payload.get("e") or "depthUpdate"),
            event_time=event_time,
            received_at=raw.received_at,
            first_update_id=_optional_int(payload.get("U"), "first_update_id", parse_errors),
            final_update_id=_optional_int(payload.get("u"), "final_update_id", parse_errors),
            instrument=resolve_spot_instrument(product, venue=raw.source),
            bids=_parse_levels(payload.get("b"), "bids", parse_errors),
            asks=_parse_levels(payload.get("a"), "asks", parse_errors),
            metadata={"parse_errors": parse_errors} if parse_errors else {},
        )


class BinanceTradeNormalizer:
    def normalize(self, raw: RawMessage) -> NormalizedL3Event:
        payload = raw.payload.get("data", raw.payload)
        parse_errors: list[str] = []
        product = str(payload.get("s") or "UNKNOWN")
        event_type = str(payload.get("e") or "trade")
        trade_time = _parse_timestamp_ms(payload.get("T"), parse_errors)
        event_time = _parse_timestamp_ms(payload.get("E"), parse_errors)
        trade_id = _optional_int(payload.get("t") if payload.get("t") is not None else payload.get("a"), "trade_id", parse_errors)
        buyer_is_maker = _optional_bool(payload.get("m"), "buyer_is_maker", parse_errors)
        price = _optional_float(payload.get("p"), "price", parse_errors)
        size = _optional_float(payload.get("q"), "size", parse_errors)
        instrument = resolve_spot_instrument(product, venue=raw.source)

        metadata: dict[str, Any] = {
            "instrument_id": instrument.instrument_id if instrument is not None else None,
            "canonical_symbol": instrument.canonical_symbol if instrument is not None else None,
            "buyer_is_maker": buyer_is_maker,
            "event_time": event_time.isoformat() if event_time is not None else None,
        }
        if parse_errors:
            metadata["parse_errors"] = parse_errors

        return NormalizedL3Event(
            source=raw.source,
            product=product,
            channel="trades",
            event_type=event_type,
            exchange_time=trade_time or event_time,
            received_at=raw.received_at,
            side=_trade_side(buyer_is_maker),
            price=price,
            size=size,
            trade_id=str(trade_id) if trade_id is not None else None,
            sequence=trade_id,
            raw_type=event_type,
            metadata={key: value for key, value in metadata.items() if value is not None},
        )


class CoinbaseTradeNormalizer:
    """Normalize Coinbase Exchange `matches` channel frames (type=match / last_match).

    Two venue quirks the Binance normalizer doesn't have to deal with:

    * Coinbase timestamps are ISO-8601 strings (`time`), not epoch milliseconds.
    * Coinbase `side` is the *maker* order side. To keep `NormalizedL3Event.side`
      meaning the same thing across venues as it does for Binance (the aggressor /
      taker side), we flip it: a resting sell that gets hit means the taker bought.
      `buyer_is_maker` and the raw maker side are preserved in metadata.

    trade_id is a dense, per-product, monotonically increasing counter, so the same
    monotonicity + gap checks the trades replay uses for Binance apply unchanged — a
    gap means we actually dropped trades (e.g. an unclean reconnect), which the
    curation gate should catch.
    """

    def normalize(self, raw: RawMessage) -> NormalizedL3Event:
        payload = raw.payload.get("data", raw.payload)
        parse_errors: list[str] = []
        product = str(payload.get("product_id") or "UNKNOWN")
        event_type = str(payload.get("type") or "match")
        trade_time = _parse_iso_timestamp(payload.get("time"), parse_errors)
        trade_id = _optional_int(payload.get("trade_id"), "trade_id", parse_errors)
        price = _optional_float(payload.get("price"), "price", parse_errors)
        size = _optional_float(payload.get("size"), "size", parse_errors)
        maker_side = payload.get("side")
        taker_side = _coinbase_taker_side(maker_side)
        instrument = resolve_spot_instrument(
            _strip_symbol_separators(product), venue=raw.source
        )

        metadata: dict[str, Any] = {
            "instrument_id": instrument.instrument_id if instrument is not None else None,
            "canonical_symbol": instrument.canonical_symbol if instrument is not None else None,
            "maker_side": maker_side if maker_side in ("buy", "sell") else None,
            "buyer_is_maker": _coinbase_buyer_is_maker(maker_side),
            "coinbase_sequence": _optional_int(payload.get("sequence"), "sequence", parse_errors),
        }
        if parse_errors:
            metadata["parse_errors"] = parse_errors

        return NormalizedL3Event(
            source=raw.source,
            product=product,
            channel="trades",
            event_type=event_type,
            exchange_time=trade_time,
            received_at=raw.received_at,
            side=taker_side,
            price=price,
            size=size,
            trade_id=str(trade_id) if trade_id is not None else None,
            # Sequence the dense per-product trade_id, not Coinbase's global `sequence`
            # cursor: the trades replay + quality gate both expect a per-stream dense
            # counter, and `sequence` is shared across products / message types so it
            # would show false gaps on a single-product trades lane.
            sequence=trade_id,
            raw_type=event_type,
            metadata={key: value for key, value in metadata.items() if value is not None},
        )


def _parse_timestamp_ms(value: Any, errors: list[str]) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        # Use integer microsecond arithmetic instead of float seconds. For L3 trade
        # ordering we want lossless conversion — `int(value) / 1000` introduces
        # representation error that breaks tie-breakers between events sharing a
        # millisecond boundary.
        return _EPOCH_UTC + timedelta(microseconds=int(value) * 1000)
    except (TypeError, ValueError, OverflowError):
        errors.append("invalid_event_time")
        return None


def _optional_int(value: Any, field_name: str, errors: list[str]) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        errors.append(f"invalid_{field_name}")
        return None


def _optional_bool(value: Any, field_name: str, errors: list[str]) -> bool | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return value
    errors.append(f"invalid_{field_name}")
    return None


def _optional_float(value: Any, field_name: str, errors: list[str]) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        errors.append(f"invalid_{field_name}")
        return None


def _trade_side(buyer_is_maker: bool | None) -> str | None:
    if buyer_is_maker is None:
        return None
    return "sell" if buyer_is_maker else "buy"


def _parse_iso_timestamp(value: Any, errors: list[str]) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        text = str(value)
        # datetime.fromisoformat only learned to accept a trailing 'Z' in 3.11; the
        # archive runs on 3.11+ but normalize it anyway so the parser is self-contained.
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
    except (TypeError, ValueError):
        errors.append("invalid_event_time")
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _coinbase_taker_side(maker_side: Any) -> str | None:
    # Coinbase `side` is the maker (resting) order side; the aggressor is the opposite.
    if maker_side == "buy":
        return "sell"
    if maker_side == "sell":
        return "buy"
    return None


def _coinbase_buyer_is_maker(maker_side: Any) -> bool | None:
    if maker_side == "buy":
        return True
    if maker_side == "sell":
        return False
    return None


def _strip_symbol_separators(symbol: str) -> str:
    # Venue symbols carry separators the spot resolver doesn't strip (Coinbase "BTC-USD",
    # Kraken "XBT/USD"). Collapse them so resolve_spot_instrument can split base/quote.
    return symbol.replace("-", "").replace("_", "").replace("/", "")


def _parse_levels(
    values: Any,
    field_name: str,
    errors: list[str],
) -> list[list[float]]:
    if values in (None, ""):
        return []
    if not isinstance(values, list):
        errors.append(f"invalid_{field_name}")
        return []
    levels: list[list[float]] = []
    for item in values:
        try:
            price = float(item[0])
            size = float(item[1])
        except (TypeError, ValueError, IndexError):
            errors.append(f"invalid_{field_name}")
            continue
        levels.append([price, size])
    return levels
