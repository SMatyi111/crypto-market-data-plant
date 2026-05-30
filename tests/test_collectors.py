from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

from crypto_collector.cli import (
    _align_binance_buffered_events,
    _binance_update_window,
    _build_source_name,
    _next_utc_midnight,
    _post_reconnect_alignment_holds,
    _reopen_binance_depth_connection,
    build_parser,
    collect_binance_depth_segment,
    collect_bybit_trades_segment,
    collect_coinbase_depth_segment,
    collect_coinbase_trades_segment,
    collect_kraken_trades_segment,
    _is_retryable_connect_error,
)
from crypto_collector.market_normalizers import (
    BybitTradeNormalizer,
    CoinbaseDepthNormalizer,
    CoinbaseTradeNormalizer,
    KrakenTradeNormalizer,
)
from crypto_collector.collectors.generic_ws import (
    GenericWebsocketCollector,
    _backoff_delay,
    _is_retryable_connect_error as _generic_is_retryable_connect_error,
)
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


def test_generic_collector_backoff_grows_exponentially_with_cap() -> None:
    assert _backoff_delay(attempt=1, base=1.0, cap=60.0) == 1.0
    assert _backoff_delay(attempt=2, base=1.0, cap=60.0) == 2.0
    assert _backoff_delay(attempt=3, base=1.0, cap=60.0) == 4.0
    assert _backoff_delay(attempt=10, base=1.0, cap=8.0) == 8.0


class _FakeWebsocket:
    def __init__(self, incoming: list[str]) -> None:
        self._incoming = list(incoming)
        self.sent: list[str] = []

    async def send(self, message: str) -> None:
        self.sent.append(message)

    async def recv(self) -> str:
        if not self._incoming:
            raise AssertionError("no more frames queued")
        return self._incoming.pop(0)


def test_generic_collector_subscribe_waits_for_coinbase_ack_and_buffers_early_frames() -> None:
    import asyncio
    collector = make_collector("coinbase")
    fake = _FakeWebsocket(
        incoming=[
            '{"type": "open", "product_id": "BTC-USD"}',
            '{"type": "subscriptions", "channels": []}',
        ]
    )
    buffered = asyncio.run(collector._subscribe(fake))
    assert fake.sent and "subscribe" in fake.sent[0]
    assert len(buffered) == 1
    assert buffered[0].payload["type"] == "open"


def test_generic_collector_subscribe_raises_on_explicit_error_frame() -> None:
    import asyncio
    import pytest

    collector = make_collector("coinbase")
    fake = _FakeWebsocket(incoming=['{"type": "error", "message": "bad sub"}'])
    with pytest.raises(RuntimeError, match="subscription rejected"):
        asyncio.run(collector._subscribe(fake))


def test_generic_collector_retryable_errors_include_connection_closed() -> None:
    assert _generic_is_retryable_connect_error(TimeoutError("nope")) is True
    assert _generic_is_retryable_connect_error(OSError("connection reset by peer")) is True

    class ConnectionClosed(Exception):
        pass

    assert _generic_is_retryable_connect_error(ConnectionClosed("bye")) is True
    assert _generic_is_retryable_connect_error(ValueError("config bug")) is False


def _depth_payload(*, first: int, final: int, symbol: str = "BTCUSDT") -> dict:
    return {"e": "depthUpdate", "s": symbol, "U": first, "u": final, "b": [], "a": []}


def test_post_reconnect_alignment_holds_when_first_event_bridges_snapshot() -> None:
    bridging = RawMessage(
        source="binance",
        received_at=utc_now(),
        payload=_depth_payload(first=100, final=110),
    )
    # snapshot_last_update_id=99 → bridging.U=100 <= 99+1, so alignment holds
    assert _post_reconnect_alignment_holds([bridging], snapshot_last_update_id=99) is True
    # snapshot_last_update_id=120 → bridging.U=100 <= 120+1, holds (overlap)
    assert _post_reconnect_alignment_holds([bridging], snapshot_last_update_id=120) is True


def test_post_reconnect_alignment_broken_when_first_event_has_gap() -> None:
    gapped = RawMessage(
        source="binance",
        received_at=utc_now(),
        payload=_depth_payload(first=200, final=210),
    )
    # snapshot_last_update_id=99 → gapped.U=200 > 99+1, gap of ~100 update ids
    assert _post_reconnect_alignment_holds([gapped], snapshot_last_update_id=99) is False


def test_post_reconnect_alignment_holds_when_no_buffered_events() -> None:
    # No events seen during the resubscribe window — defer to next streamed event
    assert _post_reconnect_alignment_holds([], snapshot_last_update_id=99) is True


def test_post_reconnect_alignment_ignores_payloads_without_window() -> None:
    raw = RawMessage(
        source="binance",
        received_at=utc_now(),
        payload={"e": "heartbeat"},  # no U/u
    )
    assert _post_reconnect_alignment_holds([raw], snapshot_last_update_id=99) is True


class _ScriptedDepthWebsocket:
    """Async-iterable fake WS for depth tests. Scripts a sequence of incoming frames,
    then either closes cleanly or raises a scripted error."""

    def __init__(
        self,
        frames: list[dict | Exception],
        *,
        close_with: Exception | None = None,
    ) -> None:
        self._frames = list(frames)
        self._close_with = close_with
        self.sent: list[str] = []

    async def send(self, message: str) -> None:
        self.sent.append(message)

    async def recv(self) -> str:
        if not self._frames:
            if self._close_with is not None:
                raise self._close_with
            # No more frames and no scripted error: hang. Callers using `wait_for` will
            # see TimeoutError; async-for callers must use _close_with to terminate.
            await asyncio.sleep(3600)
            raise RuntimeError("unreachable")
        frame = self._frames.pop(0)
        if isinstance(frame, Exception):
            raise frame
        return json.dumps(frame)

    def __aiter__(self):
        return self

    async def __anext__(self) -> str:
        if not self._frames:
            if self._close_with is not None:
                raise self._close_with
            raise StopAsyncIteration
        frame = self._frames.pop(0)
        if isinstance(frame, Exception):
            raise frame
        return json.dumps(frame)


class _FakeConnection:
    """Async context manager that wraps a _ScriptedDepthWebsocket."""

    def __init__(self, websocket: _ScriptedDepthWebsocket) -> None:
        self._websocket = websocket
        self.entered = False
        self.exited = False

    async def __aenter__(self) -> _ScriptedDepthWebsocket:
        self.entered = True
        return self._websocket

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self.exited = True


class _FakeWebsocketsModule:
    """Stand-in for the `websockets` module. Hands out scripted connections in order."""

    def __init__(self, websockets: list[_ScriptedDepthWebsocket]) -> None:
        self._websockets = list(websockets)
        self.opened: list[_FakeConnection] = []

    def connect(self, url: str) -> _FakeConnection:
        if not self._websockets:
            raise AssertionError(f"unexpected extra connection to {url}")
        ws = self._websockets.pop(0)
        connection = _FakeConnection(ws)
        self.opened.append(connection)
        return connection


def test_reopen_binance_depth_connection_buffers_frames_no_snapshot_fetch(monkeypatch) -> None:
    """`_reopen_binance_depth_connection` must NOT call the REST snapshot endpoint;
    that's the whole point of reconnect-in-place. It returns buffered data frames so the
    caller can run its own alignment check."""
    ws = _ScriptedDepthWebsocket(
        frames=[
            {"result": None, "id": 1},  # subscribe ack — filtered by _is_binance_depth_payload
            _depth_payload(first=101, final=105),
            _depth_payload(first=106, final=110),
        ],
    )
    fake_ws_mod = _FakeWebsocketsModule([ws])

    # Tripwire: if _reopen ever calls fetch_binance_order_book_snapshot, fail loud.
    import crypto_collector.cli as cli_mod
    monkeypatch.setattr(
        cli_mod,
        "fetch_binance_order_book_snapshot",
        lambda *a, **k: (_ for _ in ()).throw(
            AssertionError("reopen must not refetch snapshot")
        ),
    )

    connection, websocket, buffered = asyncio.run(
        _reopen_binance_depth_connection(
            websockets=fake_ws_mod,
            websocket_url="wss://example.test",
            product="btcusdt",
            channel="depth@100ms",
            resubscribe_buffer_seconds=0.2,
        )
    )

    assert websocket is ws
    assert len(buffered) == 2
    assert buffered[0].payload["U"] == 101
    assert buffered[1].payload["U"] == 106
    # subscribe was sent
    assert ws.sent and "SUBSCRIBE" in ws.sent[0]


def _install_fake_depth_runtime(monkeypatch, websockets_list, *, snapshot_last_update_id):
    """Wire up `crypto_collector.cli` to use a scripted websockets module + a fake REST
    snapshot. Returns (snapshot_calls, fake_ws_mod) so tests can assert against them."""
    import crypto_collector.cli as cli_mod

    fake_ws_mod = _FakeWebsocketsModule(websockets_list)
    snapshot_calls: list[dict] = []

    def _fake_snapshot(*, symbol, limit, base_url):
        snapshot_calls.append({"symbol": symbol, "limit": limit, "base_url": base_url})
        return {
            "lastUpdateId": snapshot_last_update_id,
            "bids": [["100.0", "1.0"]],
            "asks": [["101.0", "1.0"]],
        }

    monkeypatch.setattr(cli_mod, "fetch_binance_order_book_snapshot", _fake_snapshot)

    class _NoopParquet:
        def __init__(self, *a, **k) -> None: ...
        def write(self, row) -> None: ...
        def flush(self) -> None: ...

    monkeypatch.setattr(cli_mod, "ParquetDatasetSink", _NoopParquet)

    import sys
    fake_module = SimpleNamespace(connect=fake_ws_mod.connect)
    monkeypatch.setitem(sys.modules, "websockets", fake_module)

    return snapshot_calls, fake_ws_mod


def test_collect_depth_segment_reconnects_in_place_when_alignment_holds(tmp_path, monkeypatch) -> None:
    """End-to-end: a clean WS close mid-stream triggers reconnect-in-place. The post-
    reconnect frames bridge the existing snapshot, so the segment continues without
    fetching a new snapshot."""
    # snapshot_last_update_id = 110 means snapshot covers everything up to id 110.
    # Initial WS frames are all stale (u<=110) so _align drops them — pending_raws is empty,
    # async-for is also empty (consumed during snapshot capture). Then clean close.
    # Reconnect → reopen → buffered frames U=111..115, U=116..120 bridge 110 (U=111<=111).
    ws_initial = _ScriptedDepthWebsocket(
        frames=[
            {"result": None, "id": 1},  # ack
            _depth_payload(first=101, final=105),  # stale (u=105<=110)
            _depth_payload(first=106, final=110),  # stale (u=110<=110)
        ]
    )
    ws_reopen = _ScriptedDepthWebsocket(
        frames=[
            {"result": None, "id": 1},  # ack
            _depth_payload(first=111, final=115),  # bridges 110 (U=111=110+1)
            _depth_payload(first=116, final=120),
        ]
    )
    snapshot_calls, _ = _install_fake_depth_runtime(
        monkeypatch, [ws_initial, ws_reopen], snapshot_last_update_id=110
    )

    args = SimpleNamespace(
        symbol="btcusdt",
        speed="100ms",
        count=2,
        output_root=tmp_path,
        snapshot_limit=10,
        snapshot_base_url="https://example.test/depth",
        connect_retries=3,
        retry_backoff_seconds=0.0,
        max_backoff_seconds=0.0,
        resubscribe_buffer_seconds=0.2,
    )

    result = asyncio.run(collect_binance_depth_segment(args))

    assert result["raw_messages"] == 2, result
    assert result["reconnect_count"] == 1, result
    assert result["alignment_break_count"] == 0, result
    # The whole point of reconnect-in-place: only ONE REST snapshot fetch
    assert len(snapshot_calls) == 1


def test_collect_depth_segment_ends_segment_when_alignment_broken(tmp_path, monkeypatch) -> None:
    """If the post-reconnect window has a gap (U > last_seen+1), end the segment cleanly
    so the worker spawns a fresh run with a fresh snapshot. We do NOT refetch the
    snapshot into the same run dir (would violate replay's single-anchor invariant)."""
    ws_initial = _ScriptedDepthWebsocket(
        frames=[
            {"result": None, "id": 1},
            _depth_payload(first=101, final=105),  # stale
            _depth_payload(first=106, final=110),  # stale
        ]
    )
    # Post-reconnect: first event has U=500 — huge gap from last_seen_final_update_id=110
    ws_reopen = _ScriptedDepthWebsocket(
        frames=[
            {"result": None, "id": 1},
            _depth_payload(first=500, final=510),
        ]
    )
    snapshot_calls, _ = _install_fake_depth_runtime(
        monkeypatch, [ws_initial, ws_reopen], snapshot_last_update_id=110
    )

    args = SimpleNamespace(
        symbol="btcusdt",
        speed="100ms",
        count=100,  # we won't reach this; segment ends on alignment break
        output_root=tmp_path,
        snapshot_limit=10,
        snapshot_base_url="https://example.test/depth",
        connect_retries=3,
        retry_backoff_seconds=0.0,
        max_backoff_seconds=0.0,
        resubscribe_buffer_seconds=0.2,
    )

    result = asyncio.run(collect_binance_depth_segment(args))

    assert result["alignment_break_count"] == 1, result
    assert result["reconnect_count"] == 1, result
    # Still only ONE REST snapshot fetch — alignment break ends the segment;
    # the worker loop's next segment is what fetches a fresh snapshot.
    assert len(snapshot_calls) == 1, result
    assert result["raw_messages"] == 0, result  # nothing processed pre-disconnect


# --- Phase 2 #1: per-instrument lane source-name composition --------------


def test_build_source_name_preserves_legacy_layout_when_suffix_empty() -> None:
    """No suffix → no change. Critical: the live BTC collector writes to
    `binance_depth/<timestamp>/` today and we don't want to break that."""
    assert _build_source_name("binance_depth", "") == "binance_depth"
    assert _build_source_name("binance_depth", None) == "binance_depth"
    assert _build_source_name("binance_trades", "  ") == "binance_trades"


def test_build_source_name_adds_lane_suffix_when_set() -> None:
    """New ETH / SOL / etc. lanes opt into per-instrument subdirs by setting
    --source-suffix. Output layout: <output_root>/binance_depth_ethusdt/<ts>/."""
    assert _build_source_name("binance_depth", "ethusdt") == "binance_depth_ethusdt"
    assert _build_source_name("binance_trades", "SOLUSDT") == "binance_trades_solusdt"
    # whitespace stripped
    assert _build_source_name("binance_depth", "  ethusdt  ") == "binance_depth_ethusdt"


def test_build_source_name_sanitizes_unsafe_characters() -> None:
    """A typo in the config shouldn't be able to write to ../../etc. Anything
    outside [a-z0-9_-] is replaced with `_`."""
    assert _build_source_name("binance_depth", "../bad") == "binance_depth____bad"
    assert _build_source_name("binance_depth", "btc/usdt") == "binance_depth_btc_usdt"
    assert _build_source_name("binance_depth", "btc.usdt") == "binance_depth_btc_usdt"


def test_cli_parser_accepts_source_suffix_on_depth_and_trades() -> None:
    parser = build_parser()
    depth_args = parser.parse_args(
        ["binance-depth-worker", "--symbol", "ethusdt", "--source-suffix", "ethusdt"]
    )
    assert depth_args.source_suffix == "ethusdt"
    trades_args = parser.parse_args(
        ["binance-trades-worker", "--symbol", "ethusdt", "--source-suffix", "ethusdt"]
    )
    assert trades_args.source_suffix == "ethusdt"

    # Default is empty — backwards compatibility
    default_depth = parser.parse_args(["binance-depth-worker"])
    assert default_depth.source_suffix == ""
    default_trades = parser.parse_args(["binance-trades-worker"])
    assert default_trades.source_suffix == ""


# --- Phase 2 #2: day-bounded rotation -------------------------------------


def test_next_utc_midnight_returns_first_midnight_strictly_after() -> None:
    """Day-bounded rotation needs the *next* UTC midnight strictly after the segment
    start. A segment started at 23:59:59 UTC must rotate at the very next 00:00,
    one second later, not 24 hours later."""
    assert _next_utc_midnight(datetime(2026, 5, 28, 14, 30, tzinfo=UTC)) == datetime(
        2026, 5, 29, 0, 0, tzinfo=UTC
    )
    # Boundary: 23:59:59 → next is the very next minute
    assert _next_utc_midnight(datetime(2026, 5, 28, 23, 59, 59, tzinfo=UTC)) == datetime(
        2026, 5, 29, 0, 0, tzinfo=UTC
    )
    # Naive datetime is treated as UTC
    assert _next_utc_midnight(datetime(2026, 5, 28, 14, 30)) == datetime(
        2026, 5, 29, 0, 0, tzinfo=UTC
    )


def test_next_utc_midnight_normalizes_non_utc_tz() -> None:
    """Local-time inputs get converted to UTC before computing the next midnight,
    so a New York start at 23:59 (= 04:59 UTC next day) rotates at *that* next
    UTC midnight, not at New York midnight."""
    from datetime import timezone

    nyc = timezone(timedelta(hours=-5))
    # 2026-05-28 23:59 NYC == 2026-05-29 04:59 UTC → next UTC midnight is 2026-05-30
    assert _next_utc_midnight(datetime(2026, 5, 28, 23, 59, tzinfo=nyc)) == datetime(
        2026, 5, 30, 0, 0, tzinfo=UTC
    )


def test_cli_parser_accepts_rotate_at_midnight() -> None:
    parser = build_parser()
    depth_args = parser.parse_args(["binance-depth-worker", "--rotate-at-midnight"])
    assert depth_args.rotate_at_midnight is True
    # Default is False — back-compat preserved for the live BTC collector
    assert parser.parse_args(["binance-depth-worker"]).rotate_at_midnight is False
    trades_args = parser.parse_args(["binance-trades-worker", "--rotate-at-midnight"])
    assert trades_args.rotate_at_midnight is True
    assert parser.parse_args(["binance-trades-worker"]).rotate_at_midnight is False


def test_collect_depth_segment_stops_at_deadline_with_clean_finalize(tmp_path, monkeypatch) -> None:
    """When the wall clock crosses `deadline_utc`, the depth segment stops cleanly
    BEFORE the message count is reached, and the metrics/replay/parquet finalize
    paths all still run (so the run dir is replayable + curatable)."""
    # Pre-populate frames with bridging data so each one increments message_count
    ws_initial = _ScriptedDepthWebsocket(
        frames=[
            {"result": None, "id": 1},  # ack
            _depth_payload(first=99, final=101),  # bridges snapshot lastUpdateId=100
            _depth_payload(first=102, final=103),
            _depth_payload(first=104, final=105),
        ],
        close_with=None,
    )
    _install_fake_depth_runtime(monkeypatch, [ws_initial], snapshot_last_update_id=100)

    # Deadline is ALREADY past — the very first processed event in
    # _process_batch checks _deadline_crossed() and returns True, triggering
    # clean finalize. This avoids racing the asyncio event loop.
    deadline = datetime.now(tz=UTC) - timedelta(seconds=1)

    args = SimpleNamespace(
        symbol="btcusdt",
        speed="100ms",
        count=1000,
        output_root=tmp_path,
        snapshot_limit=10,
        snapshot_base_url="https://example.test/depth",
        connect_retries=3,
        retry_backoff_seconds=0.0,
        max_backoff_seconds=0.0,
        resubscribe_buffer_seconds=0.05,
        deadline_utc=deadline,
    )

    result = asyncio.run(collect_binance_depth_segment(args))

    # The deadline_reached flag is surfaced in the summary
    assert result["deadline_reached"] is True, result
    # We didn't reach the message count — we stopped early on the deadline
    assert result["raw_messages"] < 1000
    # And critically the replay summary was still written (clean shutdown)
    assert "replayable" in result
    assert result["replay_findings"] is not None


# --- Phase 2 #3a: Coinbase trades adapter ---------------------------------


def _coinbase_match(
    *,
    trade_id: int,
    price: str,
    size: str,
    maker_side: str,
    time_iso: str,
    product_id: str = "BTC-USD",
    sequence: int | None = None,
    event_type: str = "match",
) -> dict:
    payload = {
        "type": event_type,
        "trade_id": trade_id,
        "product_id": product_id,
        "price": price,
        "size": size,
        "side": maker_side,
        "time": time_iso,
    }
    if sequence is not None:
        payload["sequence"] = sequence
    return payload


def test_coinbase_trade_normalizer_flips_maker_side_to_taker_side() -> None:
    """Coinbase `side` is the maker order side; the normalized `side` must be the
    aggressor (taker) side so it means the same thing as the Binance normalizer's
    side. maker sell -> taker buy, and buyer_is_maker reflects the maker side."""
    raw = RawMessage(
        source="coinbase",
        received_at=datetime(2026, 5, 28, 12, 0, 0, tzinfo=UTC),
        payload=_coinbase_match(
            trade_id=100,
            price="50000.5",
            size="0.25",
            maker_side="sell",
            time_iso="2026-05-28T12:00:00.000000Z",
        ),
    )

    event = CoinbaseTradeNormalizer().normalize(raw)

    assert event.side == "buy"  # maker sold -> taker bought
    assert event.metadata["maker_side"] == "sell"
    assert event.metadata["buyer_is_maker"] is False
    assert event.price == 50000.5
    assert event.size == 0.25
    assert event.channel == "trades"
    # Dense per-product trade_id is what the replay/quality gate sequence on.
    assert event.trade_id == "100"
    assert event.sequence == 100
    assert event.exchange_time == datetime(2026, 5, 28, 12, 0, 0, tzinfo=UTC)


def test_coinbase_trade_normalizer_maker_buy_is_taker_sell() -> None:
    raw = RawMessage(
        source="coinbase",
        received_at=utc_now(),
        payload=_coinbase_match(
            trade_id=7,
            price="100.0",
            size="1.0",
            maker_side="buy",
            time_iso="2026-05-28T12:00:00Z",
        ),
    )

    event = CoinbaseTradeNormalizer().normalize(raw)

    assert event.side == "sell"  # maker bought -> taker sold
    assert event.metadata["buyer_is_maker"] is True


def test_coinbase_trade_normalizer_resolves_dashed_product_to_instrument() -> None:
    """`resolve_spot_instrument` doesn't strip the Coinbase dash, so the normalizer
    must collapse separators before resolving or the instrument comes back None."""
    raw = RawMessage(
        source="coinbase",
        received_at=utc_now(),
        payload=_coinbase_match(
            trade_id=1,
            price="50000",
            size="0.1",
            maker_side="sell",
            time_iso="2026-05-28T12:00:00Z",
            product_id="BTC-USD",
        ),
    )

    event = CoinbaseTradeNormalizer().normalize(raw)

    assert event.product == "BTC-USD"  # raw venue symbol preserved
    assert event.metadata["instrument_id"] == "spot:coinbase:BTCUSD"
    assert event.metadata["canonical_symbol"] == "BTC/USD"
    assert "parse_errors" not in event.metadata


def test_coinbase_trade_normalizer_flags_invalid_fields() -> None:
    raw = RawMessage(
        source="coinbase",
        received_at=utc_now(),
        payload=_coinbase_match(
            trade_id=5,
            price="not-a-number",
            size="0.1",
            maker_side="sell",
            time_iso="garbage-timestamp",
        ),
    )

    event = CoinbaseTradeNormalizer().normalize(raw)

    assert event.price is None
    assert event.exchange_time is None
    assert "invalid_price" in event.metadata["parse_errors"]
    assert "invalid_event_time" in event.metadata["parse_errors"]


def test_cli_parser_coinbase_trades_worker_defaults() -> None:
    parser = build_parser()
    args = parser.parse_args(["coinbase-trades-worker"])
    assert args.symbol == "BTC-USD"
    assert args.channel == "matches"
    assert args.source_suffix == ""
    assert args.rotate_at_midnight is False

    suffixed = parser.parse_args(
        ["coinbase-trades-worker", "--symbol", "ETH-USD", "--source-suffix", "ethusd"]
    )
    assert suffixed.symbol == "ETH-USD"
    assert suffixed.source_suffix == "ethusd"


def _install_fake_trades_runtime(monkeypatch, ws) -> "_FakeWebsocketsModule":
    """Wire the generic WS collector to a scripted websockets module and stub out the
    Parquet sink so the trades pipeline doesn't touch the real archive."""
    import sys

    import crypto_collector.pipeline as pipeline_mod

    fake_ws_mod = _FakeWebsocketsModule([ws])

    class _NoopParquet:
        def __init__(self, *a, **k) -> None: ...
        def write(self, row) -> None: ...
        def flush(self) -> None: ...

    monkeypatch.setattr(pipeline_mod, "ParquetDatasetSink", _NoopParquet)
    monkeypatch.setitem(
        sys.modules, "websockets", SimpleNamespace(connect=fake_ws_mod.connect)
    )
    return fake_ws_mod


def test_collect_coinbase_trades_segment_writes_clean_events_and_replay_summary(
    tmp_path, monkeypatch
) -> None:
    """End-to-end: a Coinbase matches stream lands in coinbase_trades/<ts>/ as clean
    events and gets a trades replay summary, so the existing quarantine/promote chain
    can curate it exactly like Binance trades."""
    now = utc_now()
    time_iso = now.isoformat().replace("+00:00", "Z")
    ws = _ScriptedDepthWebsocket(
        frames=[
            {"type": "subscriptions", "channels": [{"name": "matches"}]},  # ack
            _coinbase_match(trade_id=500, price="50000.0", size="0.1", maker_side="sell", time_iso=time_iso),
            _coinbase_match(trade_id=501, price="50001.0", size="0.2", maker_side="buy", time_iso=time_iso),
            _coinbase_match(trade_id=502, price="50002.0", size="0.3", maker_side="sell", time_iso=time_iso),
        ]
    )
    _install_fake_trades_runtime(monkeypatch, ws)

    args = SimpleNamespace(
        symbol="BTC-USD",
        channel="matches",
        count=3,
        output_root=tmp_path,
        max_delay_ms=60_000,
        max_future_skew_ms=5_000,
        max_clock_skew_ms=60_000.0,
        source_suffix="",
        deadline_utc=None,
    )

    result = asyncio.run(collect_coinbase_trades_segment(args))

    assert result["clean_events"] == 3, result
    assert result["quarantined_events"] == 0
    assert result["replayable"] is True, result["replay_findings"]

    run_path = Path(result["run_path"])
    assert run_path.parent.name == "coinbase_trades"
    events = (run_path / "clean" / "events.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(events) == 3
    first = json.loads(events[0])
    assert first["source"] == "coinbase"
    assert first["side"] == "buy"  # maker sell -> taker buy
    assert (run_path / "metrics" / "replay_summary.json").exists()


# --- Phase 2 #3b: Coinbase depth (level2) adapter -------------------------


def _coinbase_l2_snapshot(
    *,
    bids: list[list[str]],
    asks: list[list[str]],
    product_id: str = "BTC-USD",
) -> dict:
    # The level2 snapshot frame carries the full book and, unlike l2update, has NO
    # `time` field — that's exactly what the normalizer / replay rely on to tell the
    # snapshot anchor apart from the diffs.
    return {
        "type": "snapshot",
        "product_id": product_id,
        "bids": bids,
        "asks": asks,
    }


def _coinbase_l2update(
    *,
    changes: list[list[str]],
    time_iso: str,
    product_id: str = "BTC-USD",
) -> dict:
    return {
        "type": "l2update",
        "product_id": product_id,
        "time": time_iso,
        "changes": changes,
    }


def test_coinbase_depth_normalizer_snapshot_sets_event_type_and_levels() -> None:
    """The in-stream snapshot must normalize to event_type='snapshot' with the full
    book in bids/asks and NO sequence ids (this is a none_native feed)."""
    raw = RawMessage(
        source="coinbase",
        received_at=utc_now(),
        payload=_coinbase_l2_snapshot(
            bids=[["50000.0", "1.0"], ["49999.0", "2.0"]],
            asks=[["50001.0", "0.5"]],
        ),
    )

    event = CoinbaseDepthNormalizer().normalize(raw)

    assert event.event_type == "snapshot"
    assert event.channel == "depth"
    assert event.bids == [[50000.0, 1.0], [49999.0, 2.0]]
    assert event.asks == [[50001.0, 0.5]]
    # No per-message sequence on this feed.
    assert event.first_update_id is None
    assert event.final_update_id is None
    assert event.event_time is None  # snapshot has no exchange time
    assert event.metadata == {}
    # Dashed product is collapsed before resolving so the instrument isn't None.
    assert event.instrument is not None
    assert event.instrument.instrument_id == "spot:coinbase:BTCUSD"


def test_coinbase_depth_normalizer_l2update_splits_changes_into_bid_ask() -> None:
    """`changes` is [[side, price, size], ...]; buy updates the bid side, sell the ask
    side, and size 0 (a removal) is preserved as a level so replay can drop it."""
    raw = RawMessage(
        source="coinbase",
        received_at=utc_now(),
        payload=_coinbase_l2update(
            changes=[
                ["buy", "50000.0", "1.5"],
                ["sell", "50001.0", "0.0"],  # removal
                ["buy", "49999.0", "2.0"],
            ],
            time_iso="2026-05-28T12:00:01.000000Z",
        ),
    )

    event = CoinbaseDepthNormalizer().normalize(raw)

    assert event.event_type == "l2update"
    assert event.bids == [[50000.0, 1.5], [49999.0, 2.0]]
    assert event.asks == [[50001.0, 0.0]]
    assert event.first_update_id is None
    assert event.final_update_id is None
    assert event.event_time == datetime(2026, 5, 28, 12, 0, 1, tzinfo=UTC)
    assert "parse_errors" not in event.metadata


def test_coinbase_depth_normalizer_flags_invalid_changes() -> None:
    raw = RawMessage(
        source="coinbase",
        received_at=utc_now(),
        payload=_coinbase_l2update(
            changes=[
                ["buy", "not-a-number", "1.0"],  # bad price
                ["sideways", "50000.0", "1.0"],  # unknown side
            ],
            time_iso="2026-05-28T12:00:01Z",
        ),
    )

    event = CoinbaseDepthNormalizer().normalize(raw)

    assert event.bids == []
    assert event.asks == []
    assert event.metadata["parse_errors"].count("invalid_changes") == 2


def test_cli_parser_coinbase_depth_worker_defaults() -> None:
    parser = build_parser()
    args = parser.parse_args(["coinbase-depth-worker"])
    assert args.symbol == "BTC-USD"
    # level2_batch is the unauthenticated public depth feed (plain level2 needs auth).
    assert args.channel == "level2_batch"
    assert args.source_suffix == ""
    assert args.rotate_at_midnight is False

    suffixed = parser.parse_args(
        ["coinbase-depth-worker", "--symbol", "ETH-USD", "--source-suffix", "ethusd"]
    )
    assert suffixed.symbol == "ETH-USD"
    assert suffixed.source_suffix == "ethusd"


def test_collect_coinbase_depth_segment_writes_clean_events_and_replay_summary(
    tmp_path, monkeypatch
) -> None:
    """End-to-end: a Coinbase level2 stream (in-stream snapshot + l2updates) lands in
    coinbase_depth/<ts>/ as clean events and gets a none_native depth replay summary,
    so the existing quarantine/promote chain can curate it like any other lane."""
    now = utc_now()
    t1 = now.isoformat().replace("+00:00", "Z")
    t2 = (now + timedelta(seconds=1)).isoformat().replace("+00:00", "Z")
    ws = _ScriptedDepthWebsocket(
        frames=[
            {"type": "subscriptions", "channels": [{"name": "level2_batch"}]},  # ack
            _coinbase_l2_snapshot(
                bids=[["50000.0", "1.0"]],
                asks=[["50001.0", "2.0"]],
            ),
            _coinbase_l2update(
                changes=[["buy", "50000.0", "0.0"], ["buy", "49999.0", "3.0"]],
                time_iso=t1,
            ),
            _coinbase_l2update(
                changes=[["sell", "50001.0", "1.5"]],
                time_iso=t2,
            ),
        ]
    )
    _install_fake_trades_runtime(monkeypatch, ws)

    args = SimpleNamespace(
        symbol="BTC-USD",
        channel="level2_batch",
        count=3,
        output_root=tmp_path,
        source_suffix="",
        deadline_utc=None,
    )

    result = asyncio.run(collect_coinbase_depth_segment(args))

    assert result["clean_events"] == 3, result
    assert result["quarantined_events"] == 0
    assert result["replayable"] is True, result["replay_findings"]

    run_path = Path(result["run_path"])
    assert run_path.parent.name == "coinbase_depth"
    events = (run_path / "clean" / "events.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(events) == 3
    first = json.loads(events[0])
    assert first["source"] == "coinbase"
    assert first["event_type"] == "snapshot"
    summary = json.loads(
        (run_path / "metrics" / "replay_summary.json").read_text(encoding="utf-8")
    )
    assert summary["gap_detection"] == "none_native"
    assert summary["replayable"] is True


# --- Phase 2 #3c: Bybit trades adapter ------------------------------------


def _bybit_publictrade_frame(
    *,
    trades: list[dict],
    symbol: str = "BTCUSDT",
    msg_type: str = "snapshot",
    ts_ms: int | None = None,
) -> dict:
    """Build a Bybit v5 spot publicTrade frame. One frame batches many trades in
    `data`, and each trade carries `S` (taker side, capitalized), `i` (UUID trade id),
    and `seq` (cross sequence shared across the batch) — none of which is a dense
    per-product counter, hence none_native."""
    return {
        "topic": f"publicTrade.{symbol}",
        "type": msg_type,
        "ts": ts_ms if ts_ms is not None else 0,
        "data": trades,
    }


def _bybit_trade(
    *,
    trade_id: str,
    price: str,
    size: str,
    taker_side: str,
    time_ms: int,
    symbol: str = "BTCUSDT",
    seq: int | None = None,
) -> dict:
    trade = {
        "T": time_ms,
        "s": symbol,
        "S": taker_side,
        "v": size,
        "p": price,
        "i": trade_id,
    }
    if seq is not None:
        trade["seq"] = seq
    return trade


def test_bybit_trade_normalizer_uses_taker_side_directly_and_no_sequence() -> None:
    """Bybit `S` is the taker (aggressor) side already (capitalized), so no flip like
    Coinbase. The UUID trade id must NOT become a dense `sequence` (gaplessness is
    unprovable), and buyer_is_maker is derived (taker sold -> buyer was maker)."""
    raw = RawMessage(
        source="bybit",
        received_at=datetime(2026, 5, 28, 12, 0, 0, tzinfo=UTC),
        payload=_bybit_publictrade_frame(
            trades=[
                _bybit_trade(
                    trade_id="2290000000054b3f0a",
                    price="50000.5",
                    size="0.25",
                    taker_side="Sell",
                    time_ms=1_780_000_000_000,
                    seq=99,
                )
            ]
        ),
    )

    events = BybitTradeNormalizer().normalize_many(raw)

    assert len(events) == 1
    event = events[0]
    assert event.side == "sell"  # taker side used directly (no flip)
    assert event.metadata["buyer_is_maker"] is True  # taker sold -> buyer was maker
    assert event.price == 50000.5
    assert event.size == 0.25
    assert event.channel == "trades"
    # UUID trade id preserved as a string, but sequence stays None (none_native).
    assert event.trade_id == "2290000000054b3f0a"
    assert event.sequence is None
    # cross sequence kept for forensics only.
    assert event.metadata["bybit_cross_sequence"] == 99


def test_bybit_trade_normalizer_fans_out_batched_data() -> None:
    """One frame's `data` array fans out to one event per trade via normalize_many."""
    raw = RawMessage(
        source="bybit",
        received_at=utc_now(),
        payload=_bybit_publictrade_frame(
            trades=[
                _bybit_trade(trade_id="a1", price="100.0", size="1.0", taker_side="Buy", time_ms=1_780_000_000_000),
                _bybit_trade(trade_id="a2", price="101.0", size="2.0", taker_side="Sell", time_ms=1_780_000_000_001),
                _bybit_trade(trade_id="a3", price="102.0", size="3.0", taker_side="Buy", time_ms=1_780_000_000_002),
            ]
        ),
    )

    events = BybitTradeNormalizer().normalize_many(raw)

    assert [e.trade_id for e in events] == ["a1", "a2", "a3"]
    assert [e.side for e in events] == ["buy", "sell", "buy"]
    assert all(e.sequence is None for e in events)


def test_bybit_trade_normalizer_resolves_instrument_and_flags_invalid_side() -> None:
    raw = RawMessage(
        source="bybit",
        received_at=utc_now(),
        payload=_bybit_publictrade_frame(
            trades=[
                _bybit_trade(
                    trade_id="a1",
                    price="50000",
                    size="0.1",
                    taker_side="sideways",  # not Buy/Sell
                    time_ms=1_780_000_000_000,
                )
            ]
        ),
    )

    event = BybitTradeNormalizer().normalize_many(raw)[0]

    assert event.product == "BTCUSDT"
    assert event.metadata["instrument_id"] == "spot:bybit:BTCUSDT"
    assert event.metadata["canonical_symbol"] == "BTC/USDT"
    assert event.side is None
    assert "invalid_side" in event.metadata["parse_errors"]


def test_bybit_trade_normalizer_empty_data_yields_nothing() -> None:
    raw = RawMessage(
        source="bybit",
        received_at=utc_now(),
        payload=_bybit_publictrade_frame(trades=[]),
    )
    assert BybitTradeNormalizer().normalize_many(raw) == []


def test_cli_parser_bybit_trades_worker_defaults() -> None:
    parser = build_parser()
    args = parser.parse_args(["bybit-trades-worker"])
    assert args.symbol == "BTCUSDT"
    assert args.channel == "publicTrade"
    assert args.source_suffix == ""
    assert args.rotate_at_midnight is False

    suffixed = parser.parse_args(
        ["bybit-trades-worker", "--symbol", "ETHUSDT", "--source-suffix", "ethusdt"]
    )
    assert suffixed.symbol == "ETHUSDT"
    assert suffixed.source_suffix == "ethusdt"


def test_collect_bybit_trades_segment_writes_none_native_replay_summary(
    tmp_path, monkeypatch
) -> None:
    """End-to-end: a Bybit publicTrade stream (batched data arrays) lands in
    bybit_trades/<ts>/ as clean events and gets a none_native trades replay summary
    (gap_detection='none_native'), so the existing quarantine/promote chain can curate
    it without claiming gaplessness it can't prove."""
    now = utc_now()
    t_ms = int(now.timestamp() * 1000)
    ws = _ScriptedDepthWebsocket(
        frames=[
            {"success": True, "ret_msg": "subscribe", "op": "subscribe", "conn_id": "x"},  # ack
            _bybit_publictrade_frame(
                trades=[
                    _bybit_trade(trade_id="a1", price="50000.0", size="0.1", taker_side="Buy", time_ms=t_ms),
                    _bybit_trade(trade_id="a2", price="50001.0", size="0.2", taker_side="Sell", time_ms=t_ms),
                ],
            ),
            _bybit_publictrade_frame(
                trades=[
                    _bybit_trade(trade_id="a3", price="50002.0", size="0.3", taker_side="Buy", time_ms=t_ms),
                ],
                msg_type="delta",
            ),
        ]
    )
    _install_fake_trades_runtime(monkeypatch, ws)

    args = SimpleNamespace(
        symbol="BTCUSDT",
        channel="publicTrade",
        count=2,  # frames, not events; 2 frames fan out to 3 trades
        output_root=tmp_path,
        max_delay_ms=60_000,
        max_future_skew_ms=5_000,
        max_clock_skew_ms=60_000.0,
        source_suffix="",
        deadline_utc=None,
    )

    result = asyncio.run(collect_bybit_trades_segment(args))

    assert result["raw_messages"] == 2, result  # 2 frames
    assert result["clean_events"] == 3, result  # fanned out to 3 trades
    assert result["quarantined_events"] == 0
    assert result["replayable"] is True, result["replay_findings"]

    run_path = Path(result["run_path"])
    assert run_path.parent.name == "bybit_trades"
    events = (run_path / "clean" / "events.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(events) == 3
    first = json.loads(events[0])
    assert first["source"] == "bybit"
    assert first["side"] == "buy"
    assert first["sequence"] is None  # none_native — no dense counter
    summary = json.loads(
        (run_path / "metrics" / "replay_summary.json").read_text(encoding="utf-8")
    )
    assert summary["gap_detection"] == "none_native"
    assert summary["mode"] == "trade_stream_none_native"
    assert summary["replayable"] is True


# --- Phase 2 #3c: Kraken trades adapter -----------------------------------


def _kraken_trade_frame(
    *,
    trades: list[dict],
    msg_type: str = "update",
) -> dict:
    """Build a Kraken v2 trade frame. One frame batches several trades in `data`; each
    carries `side` (taker side, lowercase) and `trade_id` (a dense per-pair counter,
    so gap detection works -- this is a sequence-bearing feed unlike Bybit)."""
    return {
        "channel": "trade",
        "type": msg_type,
        "data": trades,
    }


def _kraken_trade(
    *,
    trade_id: int,
    price: float,
    qty: float,
    side: str,
    timestamp_iso: str,
    symbol: str = "BTC/USD",
    ord_type: str = "market",
) -> dict:
    return {
        "symbol": symbol,
        "side": side,
        "price": price,
        "qty": qty,
        "ord_type": ord_type,
        "trade_id": trade_id,
        "timestamp": timestamp_iso,
    }


def test_kraken_trade_normalizer_uses_dense_trade_id_as_sequence() -> None:
    """Kraken v2 `trade_id` is documented as a per-pair sequence number, so unlike
    Bybit it DOES populate `sequence` (gap detection applies). `side` is the taker side
    directly (lowercase), so no flip; buyer_is_maker is derived."""
    raw = RawMessage(
        source="kraken",
        received_at=datetime(2026, 5, 28, 12, 0, 0, tzinfo=UTC),
        payload=_kraken_trade_frame(
            trades=[
                _kraken_trade(
                    trade_id=1001,
                    price=50000.5,
                    qty=0.25,
                    side="sell",
                    timestamp_iso="2026-05-28T12:00:00.000000Z",
                )
            ]
        ),
    )

    events = KrakenTradeNormalizer().normalize_many(raw)

    assert len(events) == 1
    event = events[0]
    assert event.side == "sell"  # taker side used directly
    assert event.metadata["buyer_is_maker"] is True  # taker sold -> buyer was maker
    assert event.price == 50000.5
    assert event.size == 0.25
    assert event.channel == "trades"
    # Dense per-pair trade_id -> both trade_id string and sequence int.
    assert event.trade_id == "1001"
    assert event.sequence == 1001
    assert event.metadata["ord_type"] == "market"
    assert event.exchange_time == datetime(2026, 5, 28, 12, 0, 0, tzinfo=UTC)


def test_kraken_trade_normalizer_fans_out_and_resolves_instrument() -> None:
    raw = RawMessage(
        source="kraken",
        received_at=utc_now(),
        payload=_kraken_trade_frame(
            trades=[
                _kraken_trade(trade_id=1, price=100.0, qty=1.0, side="buy", timestamp_iso="2026-05-28T12:00:00Z"),
                _kraken_trade(trade_id=2, price=101.0, qty=2.0, side="sell", timestamp_iso="2026-05-28T12:00:01Z"),
            ]
        ),
    )

    events = KrakenTradeNormalizer().normalize_many(raw)

    assert [e.sequence for e in events] == [1, 2]
    assert [e.side for e in events] == ["buy", "sell"]
    # Slashed Kraken pair is collapsed before resolving so the instrument isn't None.
    assert events[0].product == "BTC/USD"
    assert events[0].metadata["instrument_id"] == "spot:kraken:BTCUSD"
    assert events[0].metadata["canonical_symbol"] == "BTC/USD"


def test_kraken_trade_normalizer_empty_data_yields_nothing() -> None:
    raw = RawMessage(
        source="kraken",
        received_at=utc_now(),
        payload=_kraken_trade_frame(trades=[]),
    )
    assert KrakenTradeNormalizer().normalize_many(raw) == []


def test_cli_parser_kraken_trades_worker_defaults() -> None:
    parser = build_parser()
    args = parser.parse_args(["kraken-trades-worker"])
    assert args.symbol == "BTC/USD"
    assert args.channel == "trade"
    assert args.source_suffix == ""
    assert args.rotate_at_midnight is False

    suffixed = parser.parse_args(
        ["kraken-trades-worker", "--symbol", "ETH/USD", "--source-suffix", "ethusd"]
    )
    assert suffixed.symbol == "ETH/USD"
    assert suffixed.source_suffix == "ethusd"


def test_collect_kraken_trades_segment_writes_sequence_replay_summary(
    tmp_path, monkeypatch
) -> None:
    """End-to-end: a Kraken v2 trade stream (snapshot + update frames, batched data)
    lands in kraken_trades/<ts>/ as clean events and gets a sequence-bearing trades
    replay summary (gap_detection='sequence'), because Kraken's dense per-pair trade_id
    makes gaplessness provable."""
    now = utc_now()
    t1 = now.isoformat().replace("+00:00", "Z")
    t2 = (now + timedelta(seconds=1)).isoformat().replace("+00:00", "Z")
    ws = _ScriptedDepthWebsocket(
        frames=[
            {"method": "subscribe", "success": True, "result": {"channel": "trade", "symbol": "BTC/USD"}},  # ack
            _kraken_trade_frame(
                trades=[
                    _kraken_trade(trade_id=1001, price=50000.0, qty=0.1, side="buy", timestamp_iso=t1),
                    _kraken_trade(trade_id=1002, price=50001.0, qty=0.2, side="sell", timestamp_iso=t1),
                ],
                msg_type="snapshot",
            ),
            _kraken_trade_frame(
                trades=[
                    _kraken_trade(trade_id=1003, price=50002.0, qty=0.3, side="buy", timestamp_iso=t2),
                ],
                msg_type="update",
            ),
        ]
    )
    _install_fake_trades_runtime(monkeypatch, ws)

    args = SimpleNamespace(
        symbol="BTC/USD",
        channel="trade",
        count=2,  # frames; fan out to 3 trades
        output_root=tmp_path,
        max_delay_ms=60_000,
        max_future_skew_ms=5_000,
        max_clock_skew_ms=60_000.0,
        source_suffix="",
        deadline_utc=None,
    )

    result = asyncio.run(collect_kraken_trades_segment(args))

    assert result["raw_messages"] == 2, result
    assert result["clean_events"] == 3, result
    assert result["quarantined_events"] == 0
    assert result["replayable"] is True, result["replay_findings"]

    run_path = Path(result["run_path"])
    assert run_path.parent.name == "kraken_trades"
    events = (run_path / "clean" / "events.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(events) == 3
    first = json.loads(events[0])
    assert first["source"] == "kraken"
    assert first["side"] == "buy"
    assert first["sequence"] == 1001  # dense per-pair counter
    summary = json.loads(
        (run_path / "metrics" / "replay_summary.json").read_text(encoding="utf-8")
    )
    assert summary["gap_detection"] == "sequence"
    assert summary["mode"] == "trade_stream"
    assert summary["replayable"] is True
    assert summary["last_trade_id"] == 1003
