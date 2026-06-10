from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

_EPOCH_UTC = datetime(1970, 1, 1, tzinfo=UTC)

from .asset_registry import resolve_perp_instrument, resolve_spot_instrument
from .context_models import NormalizedDepthUpdate
from .models import NormalizedL3Event, RawMessage


class BinanceDepthNormalizer:
    """Normalize Binance `depthUpdate` (spot/futures WS diff) or remapped REST depth
    snapshots. `instrument_type="perp"` resolves `perp:binance-futures:SYM` (used by the
    REST futures depth lane) instead of `spot:binance:SYM`; the level/timestamp parsing is
    identical (REST snapshots are remapped to the same `s`/`e`/`E`/`u`/`b`/`a` keys)."""

    def __init__(self, *, instrument_type: str = "spot") -> None:
        self._resolve_instrument = (
            resolve_perp_instrument if instrument_type == "perp" else resolve_spot_instrument
        )

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
            instrument=self._resolve_instrument(product, venue=raw.source),
            bids=_parse_levels(payload.get("b"), "bids", parse_errors),
            asks=_parse_levels(payload.get("a"), "asks", parse_errors),
            metadata={"parse_errors": parse_errors} if parse_errors else {},
        )


class BinanceTradeNormalizer:
    """Normalize Binance `trade` (spot) / `aggTrade` (spot or USDT-M futures) frames.

    The frame shape is identical across spot and futures — both carry a dense per-symbol
    id (`t` for raw trades, `a` for aggregate trades), `m` (buyer-is-maker), `p`/`q` — so
    the only market-dependent behavior is instrument identity: `instrument_type="perp"`
    resolves `perp:binance-futures:SYM` (canonical `BTC/USDT-PERP`) instead of
    `spot:binance:SYM`. (Futures streams only aggregate trades, so its lane uses `a`.)
    """

    def __init__(self, *, instrument_type: str = "spot") -> None:
        self._resolve_instrument = (
            resolve_perp_instrument if instrument_type == "perp" else resolve_spot_instrument
        )

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
        instrument = self._resolve_instrument(product, venue=raw.source)

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


class BinanceFuturesFundingNormalizer:
    """Normalize Binance USDT-M futures `/fapi/v1/premiumIndex` rows into a `funding`
    event. Mark price is carried as the event `price` (a real price); index price, funding
    rate, interest rate and next funding time live in metadata. This is a low-rate METRIC
    lane (not microstructure): `size`/`side`/`trade_id` are absent and it is curated
    none_native via replay_funding_run. Instrument is tagged perp:binance-futures:*."""

    def normalize(self, raw: RawMessage) -> NormalizedL3Event:
        payload = raw.payload.get("data", raw.payload)
        parse_errors: list[str] = []
        product = str(payload.get("symbol") or "UNKNOWN")
        event_time = _parse_timestamp_ms(payload.get("time"), parse_errors)
        mark_price = _optional_float(payload.get("markPrice"), "mark_price", parse_errors)
        instrument = resolve_perp_instrument(product, venue=raw.source)
        metadata: dict[str, Any] = {
            "instrument_id": instrument.instrument_id if instrument is not None else None,
            "canonical_symbol": instrument.canonical_symbol if instrument is not None else None,
            "mark_price": mark_price,
            "index_price": _optional_float(payload.get("indexPrice"), "index_price", parse_errors),
            "funding_rate": _optional_float(payload.get("lastFundingRate"), "funding_rate", parse_errors),
            "interest_rate": _optional_float(payload.get("interestRate"), "interest_rate", parse_errors),
            "estimated_settle_price": _optional_float(
                payload.get("estimatedSettlePrice"), "estimated_settle_price", parse_errors
            ),
            "next_funding_time": _optional_int(payload.get("nextFundingTime"), "next_funding_time", parse_errors),
        }
        if parse_errors:
            metadata["parse_errors"] = parse_errors
        return NormalizedL3Event(
            source=raw.source,
            product=product,
            channel="funding",
            event_type="funding",
            exchange_time=event_time,
            received_at=raw.received_at,
            price=mark_price,
            raw_type="premiumIndex",
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
    """Normalize Coinbase Exchange `level2_50` depth frames (same `snapshot` /
    `l2update` shape the deprecated public `level2` / `level2_batch` channels used).

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

    The v5 `publicTrade` frame is identical for spot and linear (USDT-perp), so the
    only market-dependent behavior is instrument identity: `instrument_type="perp"`
    resolves `perp:bybit:SYM` (canonical `BTC/USDT-PERP`) instead of `spot:bybit:SYM`,
    keeping the curated perp lane distinct from spot.
    """

    def __init__(self, *, instrument_type: str = "spot") -> None:
        self._resolve_instrument = (
            resolve_perp_instrument if instrument_type == "perp" else resolve_spot_instrument
        )

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
        instrument = self._resolve_instrument(
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


class BybitDepthNormalizer:
    """Normalize Bybit v5 spot `orderbook.{depth}.{symbol}` frames.

    Bybit depth arrives as an **in-stream snapshot** (`type:"snapshot"`) on subscribe
    followed by **deltas** (`type:"delta"`); `b`/`a` are `[[price, size]]` arrays (same
    shape as Binance) where a size of `"0"` removes the level. This is a **non-sequence**
    ("none_native") feed under STANDARDS 4.3:

    * The snapshot arrives in-stream rather than via a separate REST call, so it's
      emitted as a normal depth event with `event_type="snapshot"` and the full book.
    * Bybit's orderbook `u` (update id) is monotonic but **not** a guaranteed dense `+1`
      counter for spot, and `seq` (cross sequence) is a cross-topic cursor — neither
      supports `delta == 1` gap detection. Both are kept in metadata for forensics only.

    `first_update_id`/`final_update_id` are therefore always None and gaplessness is not
    provable from the stream; the run is validated by `replay_depth_stream_run`, which
    downgrades `replayable` to structurally-clean-only.

    Spot and linear (USDT-perp) `orderbook` frames are structurally identical, so the
    only market-dependent behavior is instrument identity: `instrument_type="perp"`
    resolves `perp:bybit:SYM` (canonical `BTC/USDT-PERP`) instead of `spot:bybit:SYM`.
    """

    def __init__(self, *, instrument_type: str = "spot") -> None:
        self._resolve_instrument = (
            resolve_perp_instrument if instrument_type == "perp" else resolve_spot_instrument
        )

    def normalize(self, raw: RawMessage) -> NormalizedDepthUpdate:
        payload = raw.payload
        parse_errors: list[str] = []
        data = payload.get("data")
        data = data if isinstance(data, dict) else {}
        product = str(data.get("s") or "UNKNOWN")
        frame_type = payload.get("type")
        event_type = "snapshot" if frame_type == "snapshot" else "delta"
        # `cts` is the matching-engine timestamp (preferred); `ts` is the frame send time.
        event_time = _parse_timestamp_ms(
            payload.get("cts") if payload.get("cts") is not None else payload.get("ts"),
            parse_errors,
        )
        bids = _parse_levels(data.get("b"), "bids", parse_errors)
        asks = _parse_levels(data.get("a"), "asks", parse_errors)
        instrument = self._resolve_instrument(
            _strip_symbol_separators(product), venue=raw.source
        )
        metadata: dict[str, Any] = {
            # Kept for forensics only — NOT used as a dense gap-detection sequence.
            "bybit_update_id": _optional_int(data.get("u"), "update_id", parse_errors),
            "bybit_cross_sequence": _optional_int(data.get("seq"), "cross_sequence", parse_errors),
        }
        if parse_errors:
            metadata["parse_errors"] = parse_errors
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
            metadata={key: value for key, value in metadata.items() if value is not None},
        )


class KrakenDepthNormalizer:
    """Normalize Kraken v2 `book` channel frames.

    Like the Kraken trade feed, `data` is a **list** (one entry per symbol), so this
    exposes `normalize_many`. The book is an **in-stream snapshot** (`type:"snapshot"`)
    on subscribe followed by **updates** (`type:"update"`); `bids`/`asks` are lists of
    `{"price":.., "qty":..}` objects where a `qty` of `0` removes the level.

    Kraken ships a **CRC32 `checksum`** per book message for integrity, but validating it
    requires each pair's exact price/qty decimal precision to rebuild the canonical
    digest string — and the normalized archive stores prices as floats (losing trailing
    zeros), so checksum validation from the archive would be lossy and produce false
    quarantines. So this is curated as a **non-sequence** ("none_native") feed under
    STANDARDS 4.3 (structurally clean, not gap-proof); the `checksum` is preserved in
    metadata for a future checksum-validated gap-proofing pass.
    """

    def normalize_many(self, raw: RawMessage) -> list[NormalizedDepthUpdate]:
        data = raw.payload.get("data")
        if not isinstance(data, list):
            return []
        frame_type = raw.payload.get("type")
        event_type = "snapshot" if frame_type == "snapshot" else "update"
        return [self._normalize_one(item, raw, event_type) for item in data]

    def _normalize_one(
        self, item: Any, raw: RawMessage, event_type: str
    ) -> NormalizedDepthUpdate:
        item = item if isinstance(item, dict) else {}
        parse_errors: list[str] = []
        product = str(item.get("symbol") or "UNKNOWN")
        # The snapshot frame has no `timestamp`; updates carry an RFC3339 `timestamp`.
        event_time = _parse_iso_timestamp(item.get("timestamp"), parse_errors)
        bids = _parse_kraken_book_levels(item.get("bids"), "bids", parse_errors)
        asks = _parse_kraken_book_levels(item.get("asks"), "asks", parse_errors)
        instrument = resolve_spot_instrument(
            _strip_symbol_separators(product), venue=raw.source
        )
        metadata: dict[str, Any] = {
            # Kraken's CRC32 book checksum — preserved for a future checksum-validated
            # gap-proofing pass; NOT validated here (see class docstring).
            "kraken_checksum": _optional_int(item.get("checksum"), "checksum", parse_errors),
        }
        if parse_errors:
            metadata["parse_errors"] = parse_errors
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
            metadata={key: value for key, value in metadata.items() if value is not None},
        )


class MexcTradeNormalizer:
    """Normalize MEXC spot v3 `publicAggreDeals` frames (decoded from protobuf).

    The collector decodes MEXC's binary `PushDataV3ApiWrapper` into a plain dict
    (`collectors/mexc.py`); this normalizer consumes that dict, so it is wire-format
    agnostic. One frame batches several trades in `publicAggreDeals.deals`, so this
    exposes `normalize_many` and the pipeline fans them out.

    Like Bybit spot, MEXC's aggregated-deals stream carries **no per-trade id** —
    each item is just `price`/`quantity`/`tradeType`/`time` — so `sequence` is left
    `None` and the run is curated by `replay_trades_stream_run` as a non-sequence
    (`none_native`) feed: structurally clean, **not** gap-proof (STANDARDS 4.3).

    `tradeType` is the **taker (aggressor) side** directly (`1` = buy, `2` = sell),
    so no flip is needed; `buyer_is_maker` is derived for the cross-venue convention
    (taker sold => the buyer was the maker). int64 `time` arrives as a decimal string
    (proto3 JSON) in epoch milliseconds.
    """

    def normalize_many(self, raw: RawMessage) -> list[NormalizedL3Event]:
        body = raw.payload.get("publicAggreDeals")
        if not isinstance(body, dict):
            return []
        deals = body.get("deals")
        if not isinstance(deals, list):
            return []
        return [self._normalize_one(item, raw) for item in deals]

    def _normalize_one(self, item: Any, raw: RawMessage) -> NormalizedL3Event:
        item = item if isinstance(item, dict) else {}
        parse_errors: list[str] = []
        product = str(raw.payload.get("symbol") or "UNKNOWN")
        trade_time = _parse_timestamp_ms(item.get("time"), parse_errors)
        taker_side = _mexc_taker_side(item.get("tradeType"), parse_errors)
        price = _optional_float(item.get("price"), "price", parse_errors)
        size = _optional_float(item.get("quantity"), "size", parse_errors)
        instrument = resolve_spot_instrument(
            _strip_symbol_separators(product), venue=raw.source
        )

        metadata: dict[str, Any] = {
            "instrument_id": instrument.instrument_id if instrument is not None else None,
            "canonical_symbol": instrument.canonical_symbol if instrument is not None else None,
            "buyer_is_maker": (taker_side == "sell") if taker_side is not None else None,
            # Forensics: the MEXC topic and the raw protobuf frame hash (full frame
            # bytes are preserved in raw/messages.jsonl under _mexc_decode).
            "mexc_channel": _optional_str(raw.payload.get("channel")),
            "mexc_frame_sha256": _mexc_frame_sha256(raw.payload),
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
            # No per-trade id in the aggregated-deals stream -> no sequence-gap detection.
            trade_id=None,
            sequence=None,
            raw_type="trade",
            metadata={key: value for key, value in metadata.items() if value is not None},
        )


class MexcDepthNormalizer:
    """Normalize MEXC spot v3 `publicLimitDepths` frames (decoded from protobuf).

    MEXC's limit-depth stream pushes the **full top-N book on every update** (each
    frame is a complete partial-book snapshot of `asks`/`bids`, levels as
    `{"price","quantity"}`), plus a per-frame `version`. There is no separate REST
    snapshot and no incremental-diff frame, so every frame is emitted with
    `event_type="snapshot"`: the depth replay (`replay_depth_stream_run`) treats each
    as a re-anchor and validates the run as structurally clean.

    This is a **non-sequence** (`none_native`) feed under STANDARDS 4.3. MEXC's
    `version` is preserved in metadata (`mexc_version`) as explicit gap-detection
    metadata, but it is NOT used to prove gaplessness: limit-depth frames are
    independent full books (not a delta chain), and the `version` is not a
    verified dense +1 counter. So `first_update_id`/`final_update_id` stay None and
    `replayable` means structurally-clean-only (a future pass could upgrade this to
    a provable `sequence` guarantee if a dense per-symbol diff id is verified live —
    the same path Bybit depth took).
    """

    def normalize(self, raw: RawMessage) -> NormalizedDepthUpdate:
        payload = raw.payload
        parse_errors: list[str] = []
        body = payload.get("publicLimitDepths")
        body = body if isinstance(body, dict) else {}
        product = str(payload.get("symbol") or "UNKNOWN")
        # The wrapper send time (matching-engine push time) is the per-frame clock;
        # fall back to createTime. Both are epoch-ms decimal strings (proto3 JSON).
        event_time = _parse_timestamp_ms(
            payload.get("sendTime") if payload.get("sendTime") is not None else payload.get("createTime"),
            parse_errors,
        )
        bids = _parse_mexc_book_levels(body.get("bids"), "bids", parse_errors)
        asks = _parse_mexc_book_levels(body.get("asks"), "asks", parse_errors)
        instrument = resolve_spot_instrument(
            _strip_symbol_separators(product), venue=raw.source
        )
        metadata: dict[str, Any] = {
            # Preserved as explicit gap-detection metadata (STANDARDS 4.3); NOT used
            # as a dense gap-proof sequence (see class docstring).
            "mexc_version": _optional_int(body.get("version"), "version", parse_errors),
            "mexc_channel": _optional_str(payload.get("channel")),
            "mexc_frame_sha256": _mexc_frame_sha256(payload),
        }
        if parse_errors:
            metadata["parse_errors"] = parse_errors
        return NormalizedDepthUpdate(
            source=raw.source,
            product=product,
            channel="depth",
            # Every limit-depth frame is a full top-N book -> a snapshot anchor.
            event_type="snapshot",
            event_time=event_time,
            received_at=raw.received_at,
            first_update_id=None,
            final_update_id=None,
            instrument=instrument,
            bids=bids,
            asks=asks,
            metadata={key: value for key, value in metadata.items() if value is not None},
        )


def _mexc_taker_side(value: Any, errors: list[str]) -> str | None:
    # MEXC `tradeType` is the taker (aggressor) side: 1 = buy, 2 = sell. A missing
    # value (proto3 omits the int32 default 0) means unknown, not an error.
    if value in (None, "", 0):
        return None
    if value == 1:
        return "buy"
    if value == 2:
        return "sell"
    errors.append("invalid_side")
    return None


def _mexc_frame_sha256(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None
    provenance = payload.get("_mexc_decode")
    if isinstance(provenance, dict):
        return _optional_str(provenance.get("frame_sha256"))
    return None


def _parse_mexc_book_levels(
    values: Any,
    field_name: str,
    errors: list[str],
) -> list[list[float]]:
    """Parse MEXC depth levels (`[{"price":.., "quantity":..}, ...]`) into the
    `[[price, size]]` shape `_apply_levels` expects. A `quantity` of `0` removes the
    level (consistent with the other depth feeds), though MEXC limit-depth frames are
    full books rather than diffs."""
    if values in (None, ""):
        return []
    if not isinstance(values, list):
        errors.append(f"invalid_{field_name}")
        return []
    levels: list[list[float]] = []
    for item in values:
        try:
            price = float(item["price"])
            size = float(item["quantity"])
        except (TypeError, ValueError, KeyError):
            errors.append(f"invalid_{field_name}")
            continue
        levels.append([price, size])
    return levels


class OkxTradeNormalizer:
    """Normalize OKX v5 `trades` channel frames.

    Like Bybit/Kraken, one frame batches several trades in `data: [...]`, so this
    exposes `normalize_many`. OKX `side` is the **taker (aggressor) side** directly
    (`"buy"`/`"sell"`), so no flip is needed; `buyer_is_maker` is derived for the
    cross-venue convention (taker sold ⇒ the buyer was the maker).

    OKX `tradeId` is a per-instrument counter, but the public `trades` channel does
    not guarantee every fill is delivered (it can conflate), so it is **not** trusted
    as a dense gap-detection sequence: `sequence` is left `None` and the run is curated
    by `replay_trades_stream_run` as a non-sequence (`none_native`) feed — structurally
    clean, not gap-proof (STANDARDS §4.3), same class as Bybit/MEXC trades.

    Spot (`BTC-USDT`) and swap (`BTC-USDT-SWAP`) frames are structurally identical, so
    the only market-dependent behavior is instrument identity: `instrument_type="perp"`
    resolves `perp:okx:SYM` (canonical `BTC/USDT-PERP`) instead of `spot:okx:SYM`.
    """

    def __init__(self, *, instrument_type: str = "spot") -> None:
        self._resolve_instrument = (
            resolve_perp_instrument if instrument_type == "perp" else resolve_spot_instrument
        )

    def normalize_many(self, raw: RawMessage) -> list[NormalizedL3Event]:
        data = raw.payload.get("data")
        if not isinstance(data, list):
            return []
        return [self._normalize_one(item, raw) for item in data]

    def _normalize_one(self, item: Any, raw: RawMessage) -> NormalizedL3Event:
        item = item if isinstance(item, dict) else {}
        parse_errors: list[str] = []
        product = str(item.get("instId") or "UNKNOWN")
        trade_time = _parse_timestamp_ms(item.get("ts"), parse_errors)
        taker_side = _okx_taker_side(item.get("side"), parse_errors)
        price = _optional_float(item.get("px"), "price", parse_errors)
        size = _optional_float(item.get("sz"), "size", parse_errors)
        trade_id = item.get("tradeId")
        instrument = self._resolve_instrument(
            _okx_resolve_symbol(product), venue=raw.source
        )

        metadata: dict[str, Any] = {
            "instrument_id": instrument.instrument_id if instrument is not None else None,
            "canonical_symbol": instrument.canonical_symbol if instrument is not None else None,
            "buyer_is_maker": (taker_side == "sell") if taker_side is not None else None,
            # Kept for forensics only — NOT used as a dense gap-detection sequence.
            "okx_trade_id": str(trade_id) if trade_id not in (None, "") else None,
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
            # The trades channel may conflate fills, so the id is not a dense counter.
            sequence=None,
            raw_type="trade",
            metadata={key: value for key, value in metadata.items() if value is not None},
        )


class OkxDepthNormalizer:
    """Normalize OKX v5 `books` channel frames.

    OKX `books` delivers an in-stream snapshot (`action:"snapshot"`, 400 levels) on
    subscribe followed by incremental updates (`action:"update"`); `bids`/`asks` are
    `[[price, size, _deprecated, num_orders]]` arrays (a size of `"0"` removes the
    level — `_parse_levels` keeps the first two fields). The payload nests a single
    book object in `data: [ {...} ]`.

    Unlike Bybit's `+1` update id, OKX proves continuity with a **linked sequence**:
    each update carries `seqId` and `prevSeqId`, and `prevSeqId(N)` must equal
    `seqId(N-1)`. That maps onto the depth model's `first_update_id`/`final_update_id`
    so `replay_depth_stream_run(chain_sequence=True)` can validate the chain by
    equality (not `delta == 1`): `first_update_id = prevSeqId`, `final_update_id = seqId`.
    A broken link is a provable gap that blocks promotion, so this lane is curated as
    `gap_detection="sequence"` (STANDARDS §4.1). The per-frame CRC32 `checksum` is kept
    in metadata for forensics / a future checksum gate but is not validated here.

    Spot (`BTC-USDT`) and swap (`BTC-USDT-SWAP`) `books` frames are structurally
    identical, so the only market-dependent behavior is instrument identity:
    `instrument_type="perp"` resolves `perp:okx:SYM` instead of `spot:okx:SYM`.
    """

    def __init__(self, *, instrument_type: str = "spot") -> None:
        self._resolve_instrument = (
            resolve_perp_instrument if instrument_type == "perp" else resolve_spot_instrument
        )

    def normalize(self, raw: RawMessage) -> NormalizedDepthUpdate:
        payload = raw.payload
        parse_errors: list[str] = []
        arg = payload.get("arg")
        arg = arg if isinstance(arg, dict) else {}
        product = str(arg.get("instId") or "UNKNOWN")
        # OKX nests one book object in a single-element data list.
        data = payload.get("data")
        book = data[0] if isinstance(data, list) and data and isinstance(data[0], dict) else {}
        action = payload.get("action")
        event_type = "snapshot" if action == "snapshot" else "delta"
        event_time = _parse_timestamp_ms(book.get("ts"), parse_errors)
        bids = _parse_levels(book.get("bids"), "bids", parse_errors)
        asks = _parse_levels(book.get("asks"), "asks", parse_errors)
        seq_id = _optional_int(book.get("seqId"), "seq_id", parse_errors)
        prev_seq_id = _optional_int(book.get("prevSeqId"), "prev_seq_id", parse_errors)
        instrument = self._resolve_instrument(
            _okx_resolve_symbol(product), venue=raw.source
        )
        metadata: dict[str, Any] = {
            "okx_seq_id": seq_id,
            "okx_prev_seq_id": prev_seq_id,
            # CRC32 over the top-25 book; kept for forensics / a future checksum gate.
            "okx_checksum": _optional_int(book.get("checksum"), "checksum", parse_errors),
        }
        if parse_errors:
            metadata["parse_errors"] = parse_errors
        return NormalizedDepthUpdate(
            source=raw.source,
            product=product,
            channel="depth",
            event_type=event_type,
            event_time=event_time,
            received_at=raw.received_at,
            # prevSeqId/seqId form the linked chain validated by chain_sequence replay.
            first_update_id=prev_seq_id,
            final_update_id=seq_id,
            instrument=instrument,
            bids=bids,
            asks=asks,
            metadata={key: value for key, value in metadata.items() if value is not None},
        )


def _okx_resolve_symbol(product: str) -> str:
    """Map an OKX instId to the bare base+quote the resolver expects.

    `BTC-USDT` (spot) and `BTC-USDT-SWAP` (perp) both collapse to `BTCUSDT` so the
    perp resolves to `perp:okx:BTCUSDT` (canonical `BTC/USDT-PERP`), matching the
    Bybit perp convention and keeping a cross-venue `*:BTCUSDT` query uniform."""
    value = product.upper()
    if value.endswith("-SWAP"):
        value = value[: -len("-SWAP")]
    return _strip_symbol_separators(value)


def _okx_taker_side(value: Any, errors: list[str]) -> str | None:
    # OKX `side` is the taker (aggressor) side, lowercase.
    if value in ("buy", "sell"):
        return value
    if value in (None, ""):
        return None
    errors.append("invalid_side")
    return None


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


def _optional_str(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


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


def _parse_kraken_book_levels(
    values: Any,
    field_name: str,
    errors: list[str],
) -> list[list[float]]:
    """Parse Kraken v2 book levels (`[{"price":.., "qty":..}, ...]`) into the
    `[[price, size]]` shape `_apply_levels` expects. A `qty` of `0` removes the level."""
    if values in (None, ""):
        return []
    if not isinstance(values, list):
        errors.append(f"invalid_{field_name}")
        return []
    levels: list[list[float]] = []
    for item in values:
        try:
            price = float(item["price"])
            size = float(item["qty"])
        except (TypeError, ValueError, KeyError):
            errors.append(f"invalid_{field_name}")
            continue
        levels.append([price, size])
    return levels
