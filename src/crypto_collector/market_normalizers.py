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


class CoinbaseDepthNormalizer:
    """Normalize Coinbase Exchange `level2` / `level2_batch` depth frames.

    Two structural differences from Binance depth, both of which make this a
    **non-sequence** ("none_native") feed under STANDARDS §4.3:

    * The book **snapshot arrives in-stream** (`type: "snapshot"`) on subscribe
      rather than via a separate REST call, so it's emitted as a normal depth event
      with `event_type="snapshot"` and the full book in `bids`/`asks`.
    * Diff frames (`type: "l2update"`) carry **no per-message sequence** (no `U`/`u`
      window). `changes` is `[[side, price, size], ...]` with the *new absolute*
      size at that level (`0` = remove). We fan them out into `bids`/`asks` to match
      the `[[price, size]]` shape the depth replay already applies.

    Because there's no sequence, `first_update_id`/`final_update_id` are always None
    and gaplessness is not provable from the stream — the run is validated by
    `replay_depth_stream_run`, which downgrades `replayable` to structurally-clean-only.
    """

    def normalize(self, raw: RawMessage) -> NormalizedDepthUpdate:
        payload = raw.payload.get("data", raw.payload)
        parse_errors: list[str] = []
        product = str(payload.get("product_id") or "UNKNOWN")
        msg_type = str(payload.get("type") or "l2update")
        # The snapshot frame has no `time`; l2update carries an ISO-8601 `time`.
        event_time = _parse_iso_timestamp(payload.get("time"), parse_errors)
        if msg_type == "snapshot":
            event_type = "snapshot"
            bids = _parse_levels(payload.get("bids"), "bids", parse_errors)
            asks = _parse_levels(payload.get("asks"), "asks", parse_errors)
        else:
            event_type = "l2update"
            bids, asks = _split_l2_changes(payload.get("changes"), parse_errors)
        instrument = resolve_spot_instrument(
            _strip_symbol_separators(product), venue=raw.source
        )
        return NormalizedDepthUpdate(
            source=raw.source,
            product=product,
            channel="depth",
            event_type=event_type,
            event_time=event_time,
            received_at=raw.received_at,
            first_update_id=None,
            final_update_id=None,
            instrument=instrument,
            bids=bids,
            asks=asks,
            metadata={"parse_errors": parse_errors} if parse_errors else {},
        )


class BybitTradeNormalizer:
    """Normalize Bybit v5 spot `publicTrade.{symbol}` frames.

    Two structural differences from the single-event venues:

    * A single frame batches **many** trades in `data: [...]` (up to 1024), so this
      normalizer exposes `normalize_many` and the pipeline fans them out.
    * Bybit's spot trade id (`i`) is a **UUID string**, not a dense per-product
      counter, and `seq` (cross sequence) is shared across batched messages — neither
      supports `delta == 1` gap detection. So `sequence` is left `None` and the run is
      curated by `replay_trades_stream_run` as a non-sequence (`none_native`) feed:
      structurally clean, **not** gap-proof (STANDARDS §4.3).

    `S` is the **taker (aggressor) side** directly (`"Buy"`/`"Sell"`), so unlike
    Coinbase no flip is needed; `buyer_is_maker` is derived for the cross-venue
    convention (taker sold ⇒ the buyer was the maker).
    """

    def normalize_many(self, raw: RawMessage) -> list[NormalizedL3Event]:
        data = raw.payload.get("data")
        if not isinstance(data, list):
            return []
        return [self._normalize_one(item, raw) for item in data]

    def _normalize_one(self, item: Any, raw: RawMessage) -> NormalizedL3Event:
        item = item if isinstance(item, dict) else {}
        parse_errors: list[str] = []
        product = str(item.get("s") or "UNKNOWN")
        trade_time = _parse_timestamp_ms(item.get("T"), parse_errors)
        taker_side = _bybit_taker_side(item.get("S"), parse_errors)
        price = _optional_float(item.get("p"), "price", parse_errors)
        size = _optional_float(item.get("v"), "size", parse_errors)
        trade_id = item.get("i")
        instrument = resolve_spot_instrument(
            _strip_symbol_separators(product), venue=raw.source
        )

        metadata: dict[str, Any] = {
            "instrument_id": instrument.instrument_id if instrument is not None else None,
            "canonical_symbol": instrument.canonical_symbol if instrument is not None else None,
            "buyer_is_maker": (taker_side == "sell") if taker_side is not None else None,
            # Kept for forensics only — NOT used as a dense gap-detection sequence.
            "bybit_cross_sequence": _optional_int(item.get("seq"), "cross_sequence", parse_errors),
        }
        if parse_errors:
            metadata["parse_errors"] = parse_errors

        return NormalizedL3Event(
            source=raw.source,
            product=product,
            channel="trades",
            event_type="trade",
            exchange_time=trade_time,
            received_at=raw.received_at,
            side=taker_side,
            price=price,
            size=size,
            trade_id=str(trade_id) if trade_id not in (None, "") else None,
            # UUID trade id is not a dense counter, so no sequence-gap detection.
            sequence=None,
            raw_type="trade",
            metadata={key: value for key, value in metadata.items() if value is not None},
        )


class KrakenTradeNormalizer:
    """Normalize Kraken v2 `trade` channel frames.

    Like Bybit, one frame batches several trades in `data: [...]`, so this exposes
    `normalize_many`. Unlike Bybit, Kraken v2 `trade_id` is documented as "a sequence
    number, unique per book" — a **dense per-pair counter** — so the standard
    sequence-bearing `replay_trades_run` (STANDARDS §4.2) applies and gaps are
    provable. `side` is the **taker (aggressor) side** directly (`"buy"`/`"sell"`).
    """

    def normalize_many(self, raw: RawMessage) -> list[NormalizedL3Event]:
        data = raw.payload.get("data")
        if not isinstance(data, list):
            return []
        return [self._normalize_one(item, raw) for item in data]

    def _normalize_one(self, item: Any, raw: RawMessage) -> NormalizedL3Event:
        item = item if isinstance(item, dict) else {}
        parse_errors: list[str] = []
        product = str(item.get("symbol") or "UNKNOWN")
        raw_side = item.get("side")
        taker_side = raw_side if raw_side in ("buy", "sell") else None
        if raw_side is not None and taker_side is None:
            parse_errors.append("invalid_side")
        price = _optional_float(item.get("price"), "price", parse_errors)
        size = _optional_float(item.get("qty"), "size", parse_errors)
        trade_id = _optional_int(item.get("trade_id"), "trade_id", parse_errors)
        trade_time = _parse_iso_timestamp(item.get("timestamp"), parse_errors)
        instrument = resolve_spot_instrument(
            _strip_symbol_separators(product), venue=raw.source
        )

        metadata: dict[str, Any] = {
            "instrument_id": instrument.instrument_id if instrument is not None else None,
            "canonical_symbol": instrument.canonical_symbol if instrument is not None else None,
            "buyer_is_maker": (taker_side == "sell") if taker_side is not None else None,
            "ord_type": item.get("ord_type") if item.get("ord_type") in ("limit", "market") else None,
        }
        if parse_errors:
            metadata["parse_errors"] = parse_errors

        return NormalizedL3Event(
            source=raw.source,
            product=product,
            channel="trades",
            event_type="trade",
            exchange_time=trade_time,
            received_at=raw.received_at,
            side=taker_side,
            price=price,
            size=size,
            trade_id=str(trade_id) if trade_id is not None else None,
            # Dense per-pair counter → sequence-gap detection works (STANDARDS §4.2).
            sequence=trade_id,
            raw_type="trade",
            metadata={key: value for key, value in metadata.items() if value is not None},
        )


def _bybit_taker_side(value: Any, errors: list[str]) -> str | None:
    # Bybit `S` is the taker (aggressor) side, capitalized.
    if value == "Buy":
        return "buy"
    if value == "Sell":
        return "sell"
    if value in (None, ""):
        return None
    errors.append("invalid_side")
    return None


def _split_l2_changes(
    values: Any,
    errors: list[str],
) -> tuple[list[list[float]], list[list[float]]]:
    """Split Coinbase `changes` (`[[side, price, size], ...]`) into bid/ask levels.

    `buy` updates the bid side, `sell` the ask side; `size` is the new absolute
    level size (`0` removes the level), matching Binance's `[[price, size]]`
    convention so the same `_apply_levels` book-building logic works downstream.
    """
    bids: list[list[float]] = []
    asks: list[list[float]] = []
    if values in (None, ""):
        return bids, asks
    if not isinstance(values, list):
        errors.append("invalid_changes")
        return bids, asks
    for item in values:
        try:
            side = item[0]
            price = float(item[1])
            size = float(item[2])
        except (TypeError, ValueError, IndexError):
            errors.append("invalid_changes")
            continue
        if side == "buy":
            bids.append([price, size])
        elif side == "sell":
            asks.append([price, size])
        else:
            errors.append("invalid_changes")
    return bids, asks


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
