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
