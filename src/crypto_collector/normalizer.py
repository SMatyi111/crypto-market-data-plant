from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from .models import NormalizedL3Event, RawMessage


class GenericL3Normalizer:
    """
    Maps a loosely structured exchange payload into the internal L3 event schema.

    This is intentionally conservative. Real exchange adapters should pre-shape their payloads
    before hitting this normalizer so the internal contract stays stable.
    """

    def normalize(self, raw: RawMessage) -> NormalizedL3Event:
        payload = raw.payload
        parse_errors: list[str] = []
        exchange_time = _parse_timestamp(payload.get("time"), parse_errors)
        return NormalizedL3Event(
            source=raw.source,
            product=str(payload.get("product_id") or payload.get("symbol") or "UNKNOWN"),
            channel=str(payload.get("channel") or "unknown"),
            event_type=str(payload.get("type") or "unknown"),
            exchange_time=exchange_time,
            received_at=raw.received_at,
            side=_optional_str(payload.get("side")),
            price=_optional_float(payload.get("price"), "price", parse_errors),
            size=_optional_float(payload.get("size") or payload.get("remaining_size"), "size", parse_errors),
            order_id=_optional_str(payload.get("order_id")),
            trade_id=_optional_str(payload.get("trade_id")),
            sequence=_optional_int(payload.get("sequence"), "sequence", parse_errors),
            raw_type=_optional_str(payload.get("type")),
            metadata={
                key: value
                for key, value in payload.items()
                if key
                not in {
                    "product_id",
                    "symbol",
                    "channel",
                    "type",
                    "time",
                    "side",
                    "price",
                    "size",
                    "remaining_size",
                    "order_id",
                    "trade_id",
                    "sequence",
                }
            } | ({"parse_errors": parse_errors} if parse_errors else {}),
        )


def _parse_timestamp(value: Any, errors: list[str]) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        if isinstance(value, datetime):
            return value.astimezone(UTC)
        if isinstance(value, (int, float)):
            # Epoch SECONDS — unlike the market normalizers, which treat numerics
            # as epoch milliseconds. Only the mock lane uses this generic adapter;
            # a real venue lane must pre-shape its timestamps (see class docstring)
            # or use a market normalizer, never feed ms values here.
            return datetime.fromtimestamp(value, tz=UTC)
        if isinstance(value, str):
            text = value.replace("Z", "+00:00")
            return datetime.fromisoformat(text).astimezone(UTC)
    except (TypeError, ValueError, OSError, OverflowError):
        # OSError/OverflowError: fromtimestamp on Windows rejects negative or
        # out-of-range values with OSError rather than ValueError.
        errors.append("invalid_time")
    return None


def _optional_float(value: Any, field_name: str, errors: list[str]) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        errors.append(f"invalid_{field_name}")
        return None


def _optional_int(value: Any, field_name: str, errors: list[str]) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        errors.append(f"invalid_{field_name}")
        return None


def _optional_str(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)
