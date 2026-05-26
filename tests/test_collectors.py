from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

from crypto_collector.cli import (
    _align_binance_buffered_events,
    _binance_update_window,
    _post_reconnect_alignment_holds,
    _reopen_binance_depth_connection,
    build_parser,
    collect_binance_depth_segment,
    _is_retryable_connect_error,
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
