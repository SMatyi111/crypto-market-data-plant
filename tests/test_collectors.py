from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

from crypto_collector.cli import (
    _align_binance_buffered_events,
    _binance_buffer_bridges_snapshot,
    _binance_trades_market,
    _binance_update_window,
    _build_source_name,
    _bybit_instrument_type,
    _bybit_market,
    _bybit_ws_url,
    _capture_binance_snapshot_and_buffer,
    _job_args,
    _okx_instid,
    _okx_instrument_type,
    _okx_market,
    _OKX_WS_URL,
    _next_utc_midnight,
    _post_reconnect_alignment_holds,
    _reopen_binance_depth_connection,
    build_parser,
    collect_binance_depth_segment,
    collect_bybit_depth_segment,
    collect_bybit_trades_segment,
    collect_coinbase_depth_segment,
    collect_coinbase_trades_segment,
    collect_kraken_depth_segment,
    collect_kraken_trades_segment,
    _is_retryable_connect_error,
)
from crypto_collector.pipeline import (
    DEFAULT_FSYNC_INTERVAL_EVENTS,
    DEFAULT_FSYNC_INTERVAL_MS,
)
from crypto_collector.market_normalizers import (
    BinanceTradeNormalizer,
    BybitDepthNormalizer,
    BybitTradeNormalizer,
    CoinbaseDepthNormalizer,
    CoinbaseTradeNormalizer,
    KrakenDepthNormalizer,
    KrakenTradeNormalizer,
    OkxDepthNormalizer,
    OkxTradeNormalizer,
    _okx_resolve_symbol,
)
from crypto_collector.collectors.generic_ws import (
    GenericWebsocketCollector,
    _backoff_delay,
    _is_retryable_connect_error as _generic_is_retryable_connect_error,
)
from crypto_collector.replay import _kraken_book_crc32
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
    assert args.jsonl_fsync is True
    assert args.normalized_parquet is True


def test_cli_binance_trades_worker_can_disable_hot_path_writes() -> None:
    parser = build_parser()
    args = parser.parse_args(
        ["binance-trades-worker", "--no-jsonl-fsync", "--no-normalized-parquet"]
    )
    assert args.channel == "trade"
    assert args.jsonl_fsync is False
    assert args.normalized_parquet is False


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


def test_generic_collector_retryable_errors_cover_websockets13_handshake_classes() -> None:
    """Regression: websockets >= 13 raises InvalidStatus (not the legacy
    InvalidStatusCode) for a non-101 handshake response. The old name-only
    allowlist classified a routine 429/503 during venue maintenance as
    non-retryable and crashed the worker lane on the FIRST attempt. Any
    InvalidHandshake subclass must be retryable — verified against the real
    installed library, plus a structural fake so the test outlives renames."""
    from websockets.exceptions import InvalidMessage, InvalidStatus

    real_status = InvalidStatus(SimpleNamespace(status_code=429))
    assert _generic_is_retryable_connect_error(real_status) is True
    assert _generic_is_retryable_connect_error(InvalidMessage("malformed handshake")) is True

    class InvalidHandshake(Exception):
        pass

    class SomeFutureHandshakeError(InvalidHandshake):
        def __str__(self) -> str:
            return "server rejected WebSocket connection: HTTP 503"

    assert _generic_is_retryable_connect_error(SomeFutureHandshakeError()) is True


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

    def connect(self, url: str, **kwargs: object) -> _FakeConnection:
        # Accept (and record) connect kwargs like max_size so the collector's
        # websockets.connect(..., max_size=...) call works against the fake.
        self.connect_kwargs = kwargs
        if not self._websockets:
            raise AssertionError(f"unexpected extra connection to {url}")
        ws = self._websockets.pop(0)
        connection = _FakeConnection(ws)
        self.opened.append(connection)
        return connection


class _KeepaliveProbeWebsocket:
    """Fake WS for the app-level keepalive tests. `recv` returns one subscription
    ack (drives `_subscribe`); the async iterator can optionally block its first
    data frame until a keepalive ping has actually been sent, so the test asserts
    on observed behavior, not on wall-clock timing."""

    def __init__(self, ack: dict, data_frames: list[dict], *, wait_for_ping: bool = False) -> None:
        self._ack = ack
        self._data_frames = list(data_frames)
        self._wait_for_ping = wait_for_ping
        self.sent: list[str] = []
        self._recv_used = False

    async def send(self, message: str) -> None:
        self.sent.append(message)

    async def recv(self) -> str:
        if not self._recv_used:
            self._recv_used = True
            return json.dumps(self._ack)
        await asyncio.sleep(3600)  # ack already delivered; iterator drives the rest
        raise RuntimeError("unreachable")

    def ping_count(self) -> int:
        return sum(1 for m in self.sent if json.loads(m) == {"op": "ping"})

    def __aiter__(self):
        return self

    async def __anext__(self) -> str:
        if self._wait_for_ping:
            for _ in range(2000):
                if self.ping_count() >= 1:
                    break
                await asyncio.sleep(0.001)
            else:
                raise AssertionError("keepalive ping was never sent")
            self._wait_for_ping = False  # only gate the first frame
        if not self._data_frames:
            raise StopAsyncIteration
        return json.dumps(self._data_frames.pop(0))


def _run_stream(collector: GenericWebsocketCollector, *, limit: int) -> list[RawMessage]:
    async def _drive() -> list[RawMessage]:
        out: list[RawMessage] = []
        async for raw in collector.stream(limit=limit):
            out.append(raw)
        return out

    return asyncio.run(_drive())


def test_collector_sends_app_level_keepalive_ping(monkeypatch) -> None:
    """When ping_message + a positive interval are configured (Bybit), the collector
    sends the app-level ping on the open socket, concurrently with the receive loop,
    after the subscription handshake."""
    import sys

    probe = _KeepaliveProbeWebsocket(
        ack={"op": "subscribe", "success": True},
        data_frames=[{"topic": "orderbook.50.BTCUSDT", "data": {}}],
        wait_for_ping=True,
    )
    fake_mod = _FakeWebsocketsModule([probe])
    monkeypatch.setitem(sys.modules, "websockets", SimpleNamespace(connect=fake_mod.connect))

    collector = GenericWebsocketCollector(
        CollectorConfig(
            source="bybit",
            output_root=Path("data"),
            product="BTCUSDT",
            channel="orderbook.50",
            websocket_url="wss://example.test",
            subscription_style="bybit",
            ping_message={"op": "ping"},
            ping_interval_seconds=0.005,
        )
    )

    emitted = _run_stream(collector, limit=1)

    assert len(emitted) == 1  # the one data frame was forwarded
    assert probe.ping_count() >= 1  # at least one {"op":"ping"} went out
    # The subscribe message is also sent, so the ping is an *addition*, not a swap.
    assert any(json.loads(m) == {"op": "subscribe", "args": ["orderbook.50.BTCUSDT"]} for m in probe.sent)


def test_collector_without_ping_config_sends_no_keepalive(monkeypatch) -> None:
    """Default config (no ping_message) — e.g. the live Binance collector — must send
    only the subscription and never an app-level ping. Guards 'live collector
    unaffected'."""
    import sys

    probe = _KeepaliveProbeWebsocket(
        ack={"result": None, "id": 1},
        data_frames=[{"e": "depthUpdate"}, {"e": "depthUpdate"}],
        wait_for_ping=False,
    )
    fake_mod = _FakeWebsocketsModule([probe])
    monkeypatch.setitem(sys.modules, "websockets", SimpleNamespace(connect=fake_mod.connect))

    collector = GenericWebsocketCollector(
        CollectorConfig(
            source="binance",
            output_root=Path("data"),
            product="BTCUSDT",
            channel="depth",
            websocket_url="wss://example.test",
            subscription_style="binance",
        )
    )

    emitted = _run_stream(collector, limit=2)

    assert len(emitted) == 2
    assert probe.ping_count() == 0
    assert len(probe.sent) == 1  # only the SUBSCRIBE frame, nothing else


# --- Phase 2 #5: data-arrival watchdog (idle timeout) ---------------------


class _StallingWebsocket:
    """Fake WS that acks the subscription (via `recv`, driving `_subscribe`) and then
    forwards any queued data frames before stalling on the async iterator — simulating a
    feed that acks then goes silent-but-connected. Once the queued frames run out,
    `__anext__` blocks forever (never another frame, never a close), so a collector with
    NO idle timeout hangs here; the watchdog must break the wait and end the stream."""

    def __init__(self, ack: dict, data_frames: list[dict] | None = None) -> None:
        self._ack = ack
        self._data_frames = list(data_frames or [])
        self.sent: list[str] = []
        self._ack_delivered = False

    async def send(self, message: str) -> None:
        self.sent.append(message)

    async def recv(self) -> str:
        if not self._ack_delivered:
            self._ack_delivered = True
            return json.dumps(self._ack)
        await asyncio.sleep(3600)  # ack delivered; data flows via the iterator
        raise RuntimeError("unreachable")

    def __aiter__(self):
        return self

    async def __anext__(self) -> str:
        if self._data_frames:
            return json.dumps(self._data_frames.pop(0))
        await asyncio.sleep(3600)  # silent-but-connected: never another frame
        raise RuntimeError("unreachable")


def _install_collector(monkeypatch, websockets_list, **config_kwargs) -> GenericWebsocketCollector:
    """Wire a GenericWebsocketCollector to a scripted websockets module (a list of fake
    sockets handed out in connect order)."""
    import sys

    fake_mod = _FakeWebsocketsModule(websockets_list)
    monkeypatch.setitem(sys.modules, "websockets", SimpleNamespace(connect=fake_mod.connect))
    config = CollectorConfig(
        source=config_kwargs.pop("source", "binance"),
        output_root=Path("data"),
        product="BTCUSDT",
        channel=config_kwargs.pop("channel", "depth"),
        websocket_url="wss://example.test",
        subscription_style=config_kwargs.pop("subscription_style", "binance"),
        **config_kwargs,
    )
    return GenericWebsocketCollector(config=config)


def test_collector_idle_timeout_fires_and_ends_on_silent_feed(monkeypatch) -> None:
    """A feed that acks then sends zero data frames must NOT hang. With the watchdog on,
    the bounded wait times out, idle_timeout_count increments, and the stream ends
    cleanly (returns) rather than blocking forever in recv."""
    ws = _StallingWebsocket(ack={"result": None, "id": 1}, data_frames=[])
    collector = _install_collector(monkeypatch, [ws], idle_timeout_seconds=0.02)

    emitted = _run_stream(collector, limit=5)

    assert emitted == []  # the feed never sent a data frame
    assert collector.idle_timeout_count == 1  # watchdog fired exactly once, then ended


def test_collector_idle_timeout_forwards_frames_then_ends(monkeypatch) -> None:
    """The watchdog only bounds the wait for the NEXT frame: frames that do arrive are
    forwarded normally, and the timeout fires only once the feed goes silent."""
    ws = _StallingWebsocket(
        ack={"result": None, "id": 1},
        data_frames=[{"e": "depthUpdate", "s": "BTCUSDT"}, {"e": "depthUpdate", "s": "BTCUSDT"}],
    )
    collector = _install_collector(monkeypatch, [ws], idle_timeout_seconds=0.02)

    emitted = _run_stream(collector, limit=10)

    assert len(emitted) == 2  # both real frames forwarded before the feed went silent
    assert collector.idle_timeout_count == 1


def test_collector_without_idle_timeout_blocks_on_silent_feed(monkeypatch) -> None:
    """Guards that the watchdog is the thing that breaks the hang: with the default
    config (idle_timeout_seconds=0.0) the same silent feed blocks forever, so a bounded
    outer wait times out. This is exactly the hang the watchdog fixes."""
    import pytest

    ws = _StallingWebsocket(ack={"result": None, "id": 1}, data_frames=[])
    collector = _install_collector(monkeypatch, [ws])  # idle timeout OFF (default)
    assert collector.config.idle_timeout_seconds == 0.0

    async def _drive() -> None:
        async for _ in collector.stream(limit=1):
            pass

    with pytest.raises((asyncio.TimeoutError, TimeoutError)):
        asyncio.run(asyncio.wait_for(_drive(), timeout=0.1))
    assert collector.idle_timeout_count == 0  # never fired — the watchdog was off


def test_collector_idle_timeout_enabled_still_reconnects_on_clean_close(monkeypatch) -> None:
    """With the watchdog on, a clean server close mid-stream (StopAsyncIteration through
    the bounded wait) must still flow through the existing reconnect path — frames arrive
    instantly so the timeout never trips, and the count stays 0. Guards that wrapping the
    iterator in wait_for doesn't break normal clean-close reconnection."""
    ws_initial = _ScriptedDepthWebsocket(
        frames=[{"result": None, "id": 1}, {"e": "depthUpdate", "s": "BTCUSDT"}]
    )  # exhausts -> StopAsyncIteration (clean close)
    ws_reopen = _ScriptedDepthWebsocket(
        frames=[{"result": None, "id": 1}, {"e": "depthUpdate", "s": "BTCUSDT"}]
    )
    collector = _install_collector(
        monkeypatch, [ws_initial, ws_reopen], idle_timeout_seconds=0.5
    )

    emitted = _run_stream(collector, limit=2)

    assert len(emitted) == 2  # one frame from each connection
    assert collector.idle_timeout_count == 0  # clean close, not an idle timeout


def test_collect_depth_segment_idle_timeout_surfaces_metric(tmp_path, monkeypatch) -> None:
    """End-to-end: a silent-but-connected depth feed ends the segment cleanly (no hang),
    the run still finalizes (metrics + replay summary written), and idle_timeout_count is
    surfaced both in the segment result and in metrics/summary.jsonl so the health report
    can see the silent venue."""
    ws = _StallingWebsocket(
        ack={"type": "subscriptions", "channels": [{"name": "level2_50"}]},
        data_frames=[_coinbase_l2_snapshot(bids=[["50000.0", "1.0"]], asks=[["50001.0", "2.0"]])],
    )
    _install_fake_trades_runtime(monkeypatch, ws)

    args = SimpleNamespace(
        symbol="BTC-USD",
        channel="level2_50",
        count=100,  # we won't reach this — the idle timeout ends the segment first
        output_root=tmp_path,
        source_suffix="",
        deadline_utc=None,
        idle_timeout_seconds=0.02,
    )

    result = asyncio.run(collect_coinbase_depth_segment(args))

    # The lone snapshot was forwarded, then the feed went silent and the watchdog ended
    # the segment cleanly instead of hanging.
    assert result["idle_timeout_count"] == 1, result
    assert result["clean_events"] == 1, result
    run_path = Path(result["run_path"])
    summary_rows = [
        json.loads(line)
        for line in (run_path / "metrics" / "summary.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert summary_rows[-1]["idle_timeout_count"] == 1
    # The run still finalized: the replay summary was written despite the idle end.
    assert (run_path / "metrics" / "replay_summary.json").exists()


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


def test_binance_buffer_bridges_snapshot_classifies_three_states() -> None:
    """The bootstrap anchor classifier mirrors replay's snapshot-gap rule exactly."""
    def _raw(first: int, final: int):
        return RawMessage(source="binance", received_at=utc_now(), payload=_depth_payload(first=first, final=final))

    # Bridges: first kept delta (u>L) has U <= L+1.
    assert _binance_buffer_bridges_snapshot([_raw(99, 101)], 100) is True
    # Snapshot ahead of the buffer (all u <= L) -> need more deltas.
    assert _binance_buffer_bridges_snapshot([_raw(90, 95)], 100) is None
    # Buffer ran ahead (earliest kept U > L+1) -> a newer snapshot is needed.
    assert _binance_buffer_bridges_snapshot([_raw(150, 151)], 100) is False
    # Empty buffer -> need more.
    assert _binance_buffer_bridges_snapshot([], 100) is None


def test_capture_keeps_buffering_until_delta_bridges_snapshot(monkeypatch) -> None:
    """Regression for the snapshot_anchor_gap bug: a fast REST snapshot must NOT end
    buffering early. The bridging delta can arrive after the snapshot returns, and we
    must keep reading until it does — otherwise it is dropped and replay flags a gap."""
    import crypto_collector.cli as cli_mod

    monkeypatch.setattr(
        cli_mod,
        "fetch_binance_order_book_snapshot",
        lambda **k: {"lastUpdateId": 100, "bids": [], "asks": []},
    )
    ws = _ScriptedDepthWebsocket(
        frames=[
            {"result": None, "id": 1},             # ack
            _depth_payload(first=99, final=101),   # bridges 100 (U=99 <= 101)
            _depth_payload(first=102, final=104),
        ]
    )

    snapshot, pending = asyncio.run(
        _capture_binance_snapshot_and_buffer(
            ws,
            product="btcusdt",
            snapshot_limit=10,
            snapshot_base_url="https://example.test/depth",
            snapshot_anchor_timeout_seconds=1.0,
        )
    )

    assert snapshot["lastUpdateId"] == 100
    assert pending, "bridging delta must be buffered, not dropped"
    assert pending[0].payload["U"] == 99
    assert _binance_buffer_bridges_snapshot(pending, 100) is True


def test_capture_refetches_newer_snapshot_when_buffer_ran_ahead(monkeypatch) -> None:
    """When the buffer has already advanced past the snapshot (U > L+1), the bootstrap
    must refetch a NEWER snapshot — while still buffering — until one bridges."""
    import crypto_collector.cli as cli_mod

    calls: list[dict] = []
    seq = [100, 149]  # first snapshot is stale vs U=150 buffer; the refetch bridges it

    def _fake(**kwargs):
        calls.append(kwargs)
        return {"lastUpdateId": seq[min(len(calls) - 1, len(seq) - 1)], "bids": [], "asks": []}

    monkeypatch.setattr(cli_mod, "fetch_binance_order_book_snapshot", _fake)
    ws = _ScriptedDepthWebsocket(
        frames=[
            {"result": None, "id": 1},
            _depth_payload(first=150, final=151),  # U=150 > 100+1 -> snapshot too old
            _depth_payload(first=152, final=153),
        ]
    )

    snapshot, pending = asyncio.run(
        _capture_binance_snapshot_and_buffer(
            ws,
            product="btcusdt",
            snapshot_limit=10,
            snapshot_base_url="https://example.test/depth",
            snapshot_anchor_timeout_seconds=1.0,
        )
    )

    assert len(calls) == 2, "expected exactly one refetch to resolve the stale snapshot"
    assert snapshot["lastUpdateId"] == 149
    assert pending and pending[0].payload["U"] == 150
    assert _binance_buffer_bridges_snapshot(pending, 149) is True


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
        # Initial buffer is all-stale by design (forces reconnect-in-place), so cap the
        # snapshot-anchor wait small instead of letting it spin the full default.
        snapshot_anchor_timeout_seconds=0.2,
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
        snapshot_anchor_timeout_seconds=0.2,
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
    # level2_50 is the unauthenticated public depth feed (plain level2/level2_batch
    # now need auth). Verified against the live socket 2026-05-31.
    assert args.channel == "level2_50"
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
            {"type": "subscriptions", "channels": [{"name": "level2_50"}]},  # ack
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
        channel="level2_50",
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


def test_bybit_market_helpers_select_url_and_instrument_type() -> None:
    # spot is the legacy lane; linear is the USDT-perp path. Only the URL suffix and the
    # resolved instrument type differ between them.
    assert _bybit_ws_url("spot") == "wss://stream.bybit.com/v5/public/spot"
    assert _bybit_ws_url("linear") == "wss://stream.bybit.com/v5/public/linear"
    assert _bybit_instrument_type("spot") == "spot"
    assert _bybit_instrument_type("linear") == "perp"
    # _bybit_market defaults to spot, is case-insensitive, and rejects unknown markets
    # (rather than silently collecting an untagged feed).
    import pytest

    assert _bybit_market(SimpleNamespace()) == "spot"
    assert _bybit_market(SimpleNamespace(market="LINEAR")) == "linear"
    with pytest.raises(SystemExit):
        _bybit_market(SimpleNamespace(market="inverse"))


def test_cli_parser_bybit_workers_accept_market_flag() -> None:
    parser = build_parser()
    # Default stays spot for both lanes (preserves the live BTC behavior).
    assert parser.parse_args(["bybit-trades-worker"]).market == "spot"
    assert parser.parse_args(["bybit-depth-worker"]).market == "spot"
    # linear is accepted.
    assert parser.parse_args(["bybit-trades-worker", "--market", "linear"]).market == "linear"
    assert parser.parse_args(["bybit-depth-worker", "--market", "linear"]).market == "linear"
    # argparse rejects anything outside the choices.
    import pytest

    with pytest.raises(SystemExit):
        parser.parse_args(["bybit-depth-worker", "--market", "inverse"])


def test_bybit_trade_normalizer_perp_tags_perp_instrument() -> None:
    raw = RawMessage(
        source="bybit",
        received_at=utc_now(),
        payload=_bybit_publictrade_frame(
            trades=[
                _bybit_trade(
                    trade_id="a1",
                    price="50000",
                    size="0.1",
                    taker_side="Buy",
                    time_ms=1_780_000_000_000,
                )
            ]
        ),
    )

    spot_event = BybitTradeNormalizer().normalize_many(raw)[0]
    perp_event = BybitTradeNormalizer(instrument_type="perp").normalize_many(raw)[0]

    # Same frame, only the instrument identity differs by market.
    assert spot_event.metadata["instrument_id"] == "spot:bybit:BTCUSDT"
    assert spot_event.metadata["canonical_symbol"] == "BTC/USDT"
    assert perp_event.metadata["instrument_id"] == "perp:bybit:BTCUSDT"
    assert perp_event.metadata["canonical_symbol"] == "BTC/USDT-PERP"


def test_bybit_depth_normalizer_perp_tags_perp_instrument() -> None:
    raw = RawMessage(
        source="bybit",
        received_at=utc_now(),
        payload=_bybit_orderbook_frame(
            bids=[["50000.0", "1.0"]],
            asks=[["50001.0", "0.5"]],
            update_id=18521288,
            cts_ms=1_780_000_000_000,
        ),
    )

    spot_event = BybitDepthNormalizer().normalize(raw)
    perp_event = BybitDepthNormalizer(instrument_type="perp").normalize(raw)

    assert spot_event.instrument.instrument_id == "spot:bybit:BTCUSDT"
    assert perp_event.instrument.instrument_id == "perp:bybit:BTCUSDT"
    assert perp_event.instrument.canonical_symbol == "BTC/USDT-PERP"


def test_job_args_threads_bybit_market_through_inprocess_path() -> None:
    # The ops-runner in-process path builds args from the job dict (not argparse), so
    # `market` must be carried there too; default spot when unset.
    trades_default = _job_args(
        SimpleNamespace(job_type="bybit-trades-worker", args={"symbol": "BTCUSDT"})
    )
    assert trades_default.market == "spot"

    depth_linear = _job_args(
        SimpleNamespace(
            job_type="bybit-depth-worker",
            args={"symbol": "BTCUSDT", "market": "linear"},
        )
    )
    assert depth_linear.market == "linear"


# ----------------------------- OKX (spot + linear perp) -----------------------------


def _okx_trades_frame(*, trades: list[dict], inst_id: str = "BTC-USDT") -> dict:
    """OKX v5 `trades` channel frame. One frame batches trades in `data`; each carries
    `side` (taker side, lowercase), `tradeId`, `px`, `sz`, `ts` (ms string)."""
    return {"arg": {"channel": "trades", "instId": inst_id}, "data": trades}


def _okx_trade(*, trade_id, price, size, side, time_ms, inst_id="BTC-USDT") -> dict:
    return {
        "instId": inst_id, "tradeId": trade_id, "px": price, "sz": size,
        "side": side, "ts": time_ms,
    }


def _okx_books_frame(
    *,
    bids: list[list[str]],
    asks: list[list[str]],
    seq_id: int,
    prev_seq_id: int,
    inst_id: str = "BTC-USDT",
    action: str = "snapshot",
    ts_ms: int = 1_780_000_000_000,
    checksum: int = -855196043,
) -> dict:
    """OKX v5 `books` frame. The book object is nested in a single-element `data` list;
    each level is [price, size, deprecated, num_orders]; prevSeqId/seqId form the chain."""
    book = {
        "asks": asks, "bids": bids, "ts": str(ts_ms),
        "checksum": checksum, "seqId": seq_id, "prevSeqId": prev_seq_id,
    }
    return {"arg": {"channel": "books", "instId": inst_id}, "action": action, "data": [book]}


def test_okx_trade_normalizer_uses_taker_side_directly_and_no_sequence() -> None:
    """OKX `side` is the taker side already (lowercase), so no flip. tradeId is kept in
    metadata but NOT used as a dense `sequence` (the trades channel may conflate)."""
    raw = RawMessage(
        source="okx",
        received_at=datetime(2026, 6, 10, tzinfo=UTC),
        payload=_okx_trades_frame(
            trades=[_okx_trade(trade_id="130639474", price="61000.5", size="0.25",
                               side="sell", time_ms="1780000000000")]
        ),
    )
    events = OkxTradeNormalizer().normalize_many(raw)
    assert len(events) == 1
    e = events[0]
    assert e.side == "sell"  # taker side used directly
    assert e.metadata["buyer_is_maker"] is True  # taker sold -> buyer was maker
    assert e.price == 61000.5
    assert e.size == 0.25
    assert e.channel == "trades"
    assert e.trade_id == "130639474"
    assert e.sequence is None  # none_native
    assert e.metadata["okx_trade_id"] == "130639474"
    assert e.metadata["instrument_id"] == "spot:okx:BTCUSDT"


def test_okx_trade_normalizer_fans_out_and_flags_invalid_side() -> None:
    raw = RawMessage(
        source="okx",
        received_at=utc_now(),
        payload=_okx_trades_frame(
            trades=[
                _okx_trade(trade_id="1", price="100", size="1", side="buy", time_ms="1780000000000"),
                _okx_trade(trade_id="2", price="101", size="2", side="sell", time_ms="1780000000001"),
                _okx_trade(trade_id="3", price="102", size="3", side="sideways", time_ms="1780000000002"),
            ]
        ),
    )
    events = OkxTradeNormalizer().normalize_many(raw)
    assert [e.trade_id for e in events] == ["1", "2", "3"]
    assert [e.side for e in events] == ["buy", "sell", None]
    assert "invalid_side" in events[2].metadata["parse_errors"]
    assert all(e.sequence is None for e in events)


def test_okx_trade_normalizer_empty_data_yields_nothing() -> None:
    assert OkxTradeNormalizer().normalize_many(
        RawMessage(source="okx", received_at=utc_now(), payload=_okx_trades_frame(trades=[]))
    ) == []


def test_okx_trade_normalizer_perp_tags_perp_instrument() -> None:
    raw = RawMessage(
        source="okx",
        received_at=utc_now(),
        payload=_okx_trades_frame(
            inst_id="BTC-USDT-SWAP",
            trades=[_okx_trade(trade_id="1", price="61000", size="1", side="buy",
                               time_ms="1780000000000", inst_id="BTC-USDT-SWAP")],
        ),
    )
    perp = OkxTradeNormalizer(instrument_type="perp").normalize_many(raw)[0]
    assert perp.metadata["instrument_id"] == "perp:okx:BTCUSDT"
    assert perp.metadata["canonical_symbol"] == "BTC/USDT-PERP"


def test_okx_depth_normalizer_snapshot_maps_prevseqid_seqid_to_chain() -> None:
    """The in-stream snapshot maps prevSeqId/seqId onto first/final update id so the
    chain validator can check prevSeqId(N) == seqId(N-1); checksum kept in metadata."""
    raw = RawMessage(
        source="okx",
        received_at=utc_now(),
        payload=_okx_books_frame(
            bids=[["61000", "2", "0", "3"], ["60999", "1", "0", "1"]],
            asks=[["61001", "0.5", "0", "2"]],
            seq_id=100, prev_seq_id=-1,
        ),
    )
    e = OkxDepthNormalizer().normalize(raw)
    assert e.event_type == "snapshot"
    assert e.channel == "depth"
    assert e.bids == [[61000.0, 2.0], [60999.0, 1.0]]  # 4-field levels keep [price, size]
    assert e.asks == [[61001.0, 0.5]]
    assert e.first_update_id == -1  # prevSeqId
    assert e.final_update_id == 100  # seqId
    assert e.metadata["okx_seq_id"] == 100
    assert e.metadata["okx_prev_seq_id"] == -1
    assert e.metadata["okx_checksum"] == -855196043
    assert e.instrument.instrument_id == "spot:okx:BTCUSDT"


def test_okx_depth_normalizer_update_preserves_removal_and_perp_tag() -> None:
    raw = RawMessage(
        source="okx",
        received_at=utc_now(),
        payload=_okx_books_frame(
            inst_id="BTC-USDT-SWAP", action="update",
            bids=[["60999", "0", "0", "0"]],  # removal
            asks=[["61001", "2", "0", "1"]],
            seq_id=101, prev_seq_id=100,
        ),
    )
    e = OkxDepthNormalizer(instrument_type="perp").normalize(raw)
    assert e.event_type == "delta"
    assert e.bids == [[60999.0, 0.0]]
    assert e.first_update_id == 100
    assert e.final_update_id == 101
    assert e.instrument.instrument_id == "perp:okx:BTCUSDT"
    assert e.instrument.canonical_symbol == "BTC/USDT-PERP"


def test_okx_resolve_symbol_strips_swap_and_separators() -> None:
    assert _okx_resolve_symbol("BTC-USDT") == "BTCUSDT"
    assert _okx_resolve_symbol("BTC-USDT-SWAP") == "BTCUSDT"


def test_okx_market_and_instid_helpers() -> None:
    import pytest

    assert _okx_instrument_type("spot") == "spot"
    assert _okx_instrument_type("linear") == "perp"
    assert _okx_market(SimpleNamespace()) == "spot"
    assert _okx_market(SimpleNamespace(market="LINEAR")) == "linear"
    with pytest.raises(SystemExit):
        _okx_market(SimpleNamespace(market="inverse"))
    # linear appends -SWAP to the spot base; spot leaves it; idempotent if already -SWAP.
    assert _okx_instid("BTC-USDT", "spot") == "BTC-USDT"
    assert _okx_instid("BTC-USDT", "linear") == "BTC-USDT-SWAP"
    assert _okx_instid("BTC-USDT-SWAP", "linear") == "BTC-USDT-SWAP"
    assert _OKX_WS_URL.startswith("wss://ws.okx.com")


def test_cli_parser_okx_workers_defaults_and_market_flag() -> None:
    parser = build_parser()
    t = parser.parse_args(["okx-trades-worker"])
    assert t.symbol == "BTC-USDT"
    assert t.channel == "trades"
    assert t.market == "spot"
    d = parser.parse_args(["okx-depth-worker", "--market", "linear"])
    assert d.channel == "books"
    assert d.market == "linear"
    import pytest

    with pytest.raises(SystemExit):
        parser.parse_args(["okx-depth-worker", "--market", "inverse"])


def test_job_args_threads_okx_market_through_inprocess_path() -> None:
    # The ops-runner in-process path builds args from the job dict (not argparse), so
    # `market` must be carried there too (the bybit PR #6 drop-trap); default spot.
    trades_default = _job_args(
        SimpleNamespace(job_type="okx-trades-worker", args={"symbol": "BTC-USDT"})
    )
    assert trades_default.market == "spot"
    assert trades_default.channel == "trades"
    depth_linear = _job_args(
        SimpleNamespace(job_type="okx-depth-worker", args={"symbol": "BTC-USDT", "market": "linear"})
    )
    assert depth_linear.market == "linear"
    assert depth_linear.channel == "books"


def _binance_aggtrade_frame(*, symbol="BTCUSDT", agg_id=12345, price="50000", qty="0.1"):
    # Binance USDT-M futures streams aggregate trades: `a` (dense agg id), no raw `t`.
    return {
        "e": "aggTrade",
        "s": symbol,
        "a": agg_id,
        "p": price,
        "q": qty,
        "T": 1_780_000_000_000,
        "E": 1_780_000_000_001,
        "m": False,
    }


def test_binance_trades_market_helper_validates() -> None:
    import pytest

    assert _binance_trades_market(SimpleNamespace()) == "spot"
    assert _binance_trades_market(SimpleNamespace(market="FUTURES")) == "futures"
    with pytest.raises(SystemExit):
        _binance_trades_market(SimpleNamespace(market="coin"))


def test_binance_trade_normalizer_perp_tags_binance_futures_instrument() -> None:
    # Futures frames arrive with source 'binance-futures' so the perp resolver hits the
    # explicit instrument-master record (perp:binance-futures:BTCUSDT), not the generic
    # perp:binance:* fallback.
    raw = RawMessage(
        source="binance-futures",
        received_at=utc_now(),
        payload=_binance_aggtrade_frame(agg_id=987654),
    )
    spot_raw = RawMessage(
        source="binance",
        received_at=utc_now(),
        payload=_binance_aggtrade_frame(agg_id=987654),
    )

    perp_event = BinanceTradeNormalizer(instrument_type="perp").normalize(raw)
    spot_event = BinanceTradeNormalizer().normalize(spot_raw)

    assert perp_event.metadata["instrument_id"] == "perp:binance-futures:BTCUSDT"
    assert perp_event.metadata["canonical_symbol"] == "BTC/USDT-PERP"
    assert spot_event.metadata["instrument_id"] == "spot:binance:BTCUSDT"
    # aggTrade `a` is a dense per-symbol counter, so the lane stays a sequence feed.
    assert perp_event.sequence == 987654
    assert perp_event.event_type == "aggTrade"


def test_cli_parser_binance_trades_worker_accepts_market_flag() -> None:
    import pytest

    parser = build_parser()
    assert parser.parse_args(["binance-trades-worker"]).market == "spot"
    assert (
        parser.parse_args(["binance-trades-worker", "--market", "futures"]).market
        == "futures"
    )
    with pytest.raises(SystemExit):
        parser.parse_args(["binance-trades-worker", "--market", "coin"])


def test_job_args_threads_binance_trades_market_through_inprocess_path() -> None:
    default = _job_args(
        SimpleNamespace(job_type="binance-trades-worker", args={"symbol": "btcusdt"})
    )
    assert default.market == "spot"
    futures = _job_args(
        SimpleNamespace(
            job_type="binance-trades-worker",
            args={"symbol": "btcusdt", "market": "futures"},
        )
    )
    assert futures.market == "futures"


def test_workers_thread_market_through_build_segment_args(tmp_path, monkeypatch) -> None:
    """Regression: the per-worker build_segment_args lambda must copy `market` onto the
    segment namespace. It previously dropped it, so a perp worker silently ran as spot
    (wrong endpoint + lane + instrument). Drive each worker once with a stubbed segment
    fn and assert the market it actually receives."""
    import crypto_collector.cli as cli

    captured: dict[str, str] = {}

    def make_fake(key):
        async def fake(segment_args):
            captured[key] = getattr(segment_args, "market", "MISSING")
            return {"run_path": str(tmp_path / key), "clean_events": 0, "replayable": True}
        return fake

    monkeypatch.setattr(cli, "collect_bybit_trades_segment", make_fake("bybit_trades"))
    monkeypatch.setattr(cli, "collect_bybit_depth_segment", make_fake("bybit_depth"))
    monkeypatch.setattr(cli, "collect_binance_trades_segment", make_fake("binance_trades"))

    def drive(job_type, runner, market):
        args = _job_args(
            SimpleNamespace(
                job_type=job_type,
                args={
                    "symbol": "BTCUSDT",
                    "market": market,
                    "max_segments": 1,
                    "cooldown_seconds": 0.0,
                    "heartbeat_interval_seconds": 0.1,
                    "worker_name": f"{job_type}-mkttest",
                    "output_root": str(tmp_path),
                    "ops_root": str(tmp_path),
                },
            )
        )
        runner(args)

    drive("bybit-trades-worker", cli.run_bybit_trades_worker, "linear")
    drive("bybit-depth-worker", cli.run_bybit_depth_worker, "linear")
    drive("binance-trades-worker", cli.run_binance_trades_worker, "futures")

    assert captured == {
        "bybit_trades": "linear",
        "bybit_depth": "linear",
        "binance_trades": "futures",
    }


def test_workers_thread_fsync_batching_through_build_segment_args(tmp_path, monkeypatch) -> None:
    """Regression: the JSONL durability posture (jsonl_fsync + the batched-fsync cadence)
    must reach every segment. The per-worker build_segment_args lambdas don't enumerate
    these — they're threaded centrally in _run_segmented_worker, the same way `market`
    was fixed — so a config value can't be silently dropped before it reaches the sink."""
    import crypto_collector.cli as cli

    captured: dict[str, dict[str, object]] = {}

    def make_fake(key):
        async def fake(segment_args):
            captured[key] = {
                "jsonl_fsync": getattr(segment_args, "jsonl_fsync", "MISSING"),
                "fsync_interval_events": getattr(segment_args, "fsync_interval_events", "MISSING"),
                "fsync_interval_ms": getattr(segment_args, "fsync_interval_ms", "MISSING"),
            }
            return {"run_path": str(tmp_path / key), "clean_events": 0, "replayable": True}

        return fake

    monkeypatch.setattr(cli, "collect_binance_trades_segment", make_fake("binance_trades"))
    monkeypatch.setattr(cli, "collect_bybit_trades_segment", make_fake("bybit_trades"))

    def drive(job_type, runner, extra_args):
        args = _job_args(
            SimpleNamespace(
                job_type=job_type,
                args={
                    "symbol": "BTCUSDT",
                    "max_segments": 1,
                    "cooldown_seconds": 0.0,
                    "heartbeat_interval_seconds": 0.1,
                    "worker_name": f"{job_type}-fsynctest",
                    "output_root": str(tmp_path),
                    "ops_root": str(tmp_path),
                    **extra_args,
                },
            )
        )
        # The ops dispatcher injects the cadence knobs onto the worker args; mirror that.
        args.fsync_interval_events = extra_args.get("fsync_interval_events")
        args.fsync_interval_ms = extra_args.get("fsync_interval_ms")
        runner(args)

    # binance trades carries explicit batched-cadence args (config-set).
    drive(
        "binance-trades-worker",
        cli.run_binance_trades_worker,
        {"fsync_interval_events": 128, "fsync_interval_ms": 250.0},
    )
    # bybit perp sets nothing -> defaults to fsync ON with the safe pipeline cadence.
    drive("bybit-trades-worker", cli.run_bybit_trades_worker, {})

    assert captured["binance_trades"] == {
        "jsonl_fsync": True,
        "fsync_interval_events": 128,
        "fsync_interval_ms": 250.0,
    }
    assert captured["bybit_trades"] == {
        "jsonl_fsync": True,
        "fsync_interval_events": DEFAULT_FSYNC_INTERVAL_EVENTS,
        "fsync_interval_ms": DEFAULT_FSYNC_INTERVAL_MS,
    }


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
    lands in kraken_trades/<ts>/ with the UPDATE-frame trades clean and the
    SNAPSHOT-frame trades quarantined as `subscribe_replay` — Kraken replays the
    last ~50 historical prints on every subscribe, the previous segment already
    captured them, and promotion has no cross-run row dedup (letting them into
    clean landed duplicate prints in curated trades_replayable). The clean run
    still gets a sequence-bearing replay summary (gap_detection='sequence')
    because Kraken's dense per-pair trade_id makes gaplessness provable."""
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
    assert result["clean_events"] == 1, result
    assert result["quarantined_events"] == 2, result
    assert result["replayable"] is True, result["replay_findings"]

    run_path = Path(result["run_path"])
    assert run_path.parent.name == "kraken_trades"
    events = (run_path / "clean" / "events.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(events) == 1
    first = json.loads(events[0])
    assert first["source"] == "kraken"
    assert first["side"] == "buy"
    assert first["sequence"] == 1003  # dense per-pair counter; update frame only
    quarantined = [
        json.loads(line)
        for line in (run_path / "quarantine" / "events.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
    ]
    assert [row["sequence"] for row in quarantined] == [1001, 1002]
    assert all(row["reasons"] == ["subscribe_replay"] for row in quarantined)
    assert all(row["metadata"]["subscribe_replay"] is True for row in quarantined)
    summary = json.loads(
        (run_path / "metrics" / "replay_summary.json").read_text(encoding="utf-8")
    )
    assert summary["gap_detection"] == "sequence"
    assert summary["mode"] == "trade_stream"
    assert summary["replayable"] is True
    assert summary["last_trade_id"] == 1003


# --- Phase 2 #3c: Bybit depth (orderbook) adapter -------------------------


def _bybit_orderbook_frame(
    *,
    bids: list[list[str]],
    asks: list[list[str]],
    symbol: str = "BTCUSDT",
    msg_type: str = "snapshot",
    update_id: int | None = None,
    seq: int | None = None,
    ts_ms: int = 0,
    cts_ms: int | None = None,
) -> dict:
    """Build a Bybit v5 spot orderbook frame. The snapshot/delta `type` is at the frame
    level; `data.b`/`data.a` are `[[price, size]]` arrays where size '0' removes a level.
    `u` (update id) and `seq` (cross sequence) are kept for forensics only — neither is a
    dense gap-detection counter, hence none_native."""
    data: dict = {"s": symbol, "b": bids, "a": asks}
    if update_id is not None:
        data["u"] = update_id
    if seq is not None:
        data["seq"] = seq
    frame: dict = {
        "topic": f"orderbook.50.{symbol}",
        "type": msg_type,
        "ts": ts_ms,
        "data": data,
    }
    if cts_ms is not None:
        frame["cts"] = cts_ms
    return frame


def test_bybit_depth_normalizer_snapshot_sets_event_type_and_levels() -> None:
    """The in-stream snapshot must normalize to event_type='snapshot' with the full book
    in bids/asks and NO sequence ids (none_native); u/seq live in metadata only."""
    raw = RawMessage(
        source="bybit",
        received_at=utc_now(),
        payload=_bybit_orderbook_frame(
            bids=[["50000.0", "1.0"], ["49999.0", "2.0"]],
            asks=[["50001.0", "0.5"]],
            update_id=18521288,
            seq=7961638724,
            cts_ms=1_780_000_000_000,
        ),
    )

    event = BybitDepthNormalizer().normalize(raw)

    assert event.event_type == "snapshot"
    assert event.channel == "depth"
    assert event.bids == [[50000.0, 1.0], [49999.0, 2.0]]
    assert event.asks == [[50001.0, 0.5]]
    # none_native: no per-message sequence is exposed as first/final update id.
    assert event.first_update_id is None
    assert event.final_update_id is None
    # u/seq kept for forensics only, NOT as a dense gap-detection counter.
    assert event.metadata["bybit_update_id"] == 18521288
    assert event.metadata["bybit_cross_sequence"] == 7961638724
    # cts (matching-engine ts) is used as the exchange time when present.
    assert event.event_time is not None
    assert event.instrument is not None
    assert event.instrument.instrument_id == "spot:bybit:BTCUSDT"


def test_bybit_depth_normalizer_delta_preserves_removal_and_update_id() -> None:
    """Deltas use event_type='delta'; size '0' (a removal) is preserved as a level so
    replay can drop it, and the absent `seq` simply doesn't appear in metadata."""
    raw = RawMessage(
        source="bybit",
        received_at=utc_now(),
        payload=_bybit_orderbook_frame(
            bids=[["50000.0", "0"]],  # removal
            asks=[["50002.0", "1.25"]],
            msg_type="delta",
            update_id=18521290,
        ),
    )

    event = BybitDepthNormalizer().normalize(raw)

    assert event.event_type == "delta"
    assert event.bids == [[50000.0, 0.0]]
    assert event.asks == [[50002.0, 1.25]]
    assert event.first_update_id is None
    assert event.final_update_id is None
    assert event.metadata["bybit_update_id"] == 18521290
    assert "bybit_cross_sequence" not in event.metadata


def test_bybit_depth_normalizer_missing_data_yields_empty_book() -> None:
    raw = RawMessage(
        source="bybit",
        received_at=utc_now(),
        payload={"topic": "orderbook.50.BTCUSDT", "type": "snapshot"},
    )

    event = BybitDepthNormalizer().normalize(raw)

    assert event.product == "UNKNOWN"
    assert event.bids == []
    assert event.asks == []
    assert event.metadata == {}


def test_cli_parser_bybit_depth_worker_defaults() -> None:
    parser = build_parser()
    args = parser.parse_args(["bybit-depth-worker"])
    assert args.symbol == "BTCUSDT"
    # orderbook.<depth>; the symbol is appended to form the full topic.
    assert args.channel == "orderbook.50"
    assert args.source_suffix == ""
    assert args.rotate_at_midnight is False

    suffixed = parser.parse_args(
        ["bybit-depth-worker", "--symbol", "ETHUSDT", "--source-suffix", "ethusdt"]
    )
    assert suffixed.symbol == "ETHUSDT"
    assert suffixed.source_suffix == "ethusdt"


def test_collect_bybit_depth_segment_writes_sequence_replay_summary(
    tmp_path, monkeypatch
) -> None:
    """End-to-end: a Bybit orderbook stream (in-stream snapshot + delta) lands in
    bybit_depth/<ts>/ as clean events. Bybit's data.u increments by exactly 1, so the
    lane is curated as a provable `sequence` gap proof (not none_native): contiguous
    update ids => replayable, gap_detection='sequence'."""
    now = utc_now()
    cts = int(now.timestamp() * 1000)
    ws = _ScriptedDepthWebsocket(
        frames=[
            {"success": True, "ret_msg": "subscribe", "op": "subscribe", "conn_id": "x"},  # ack
            _bybit_orderbook_frame(
                bids=[["50000.0", "1.0"]],
                asks=[["50001.0", "2.0"]],
                msg_type="snapshot",
                update_id=100,
                cts_ms=cts,
            ),
            _bybit_orderbook_frame(
                bids=[["50000.0", "0"]],  # removal
                asks=[["50002.0", "1.5"]],
                msg_type="delta",
                update_id=101,  # contiguous +1
                cts_ms=cts + 1000,
            ),
        ]
    )
    _install_fake_trades_runtime(monkeypatch, ws)

    args = SimpleNamespace(
        symbol="BTCUSDT",
        channel="orderbook.50",
        count=2,  # data frames
        output_root=tmp_path,
        source_suffix="",
        deadline_utc=None,
    )

    result = asyncio.run(collect_bybit_depth_segment(args))

    assert result["clean_events"] == 2, result
    assert result["quarantined_events"] == 0
    assert result["replayable"] is True, result["replay_findings"]

    run_path = Path(result["run_path"])
    assert run_path.parent.name == "bybit_depth"
    events = (run_path / "clean" / "events.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(events) == 2
    first = json.loads(events[0])
    assert first["source"] == "bybit"
    assert first["event_type"] == "snapshot"
    # The event-level first_update_id stays None; the dense id lives in metadata.
    assert first["first_update_id"] is None
    summary = json.loads(
        (run_path / "metrics" / "replay_summary.json").read_text(encoding="utf-8")
    )
    assert summary["gap_detection"] == "sequence"
    assert summary["mode"] == "stream_snapshot_sequence"
    assert summary["first_update_id"] == 100
    assert summary["last_update_id"] == 101
    assert summary["replayable"] is True


def test_collect_bybit_depth_segment_flags_update_id_gap(tmp_path, monkeypatch) -> None:
    """A jump in Bybit's data.u (a dropped message) must be caught now that the lane is
    sequence-bearing: update_id_gaps finding + NOT replayable, so the run is quarantined
    instead of falsely promoted as gapless."""
    now = utc_now()
    cts = int(now.timestamp() * 1000)
    ws = _ScriptedDepthWebsocket(
        frames=[
            {"success": True, "ret_msg": "subscribe", "op": "subscribe", "conn_id": "x"},  # ack
            _bybit_orderbook_frame(
                bids=[["50000.0", "1.0"]],
                asks=[["50001.0", "2.0"]],
                msg_type="snapshot",
                update_id=100,
                cts_ms=cts,
            ),
            _bybit_orderbook_frame(
                bids=[["49999.0", "1.0"]],
                asks=[["50002.0", "1.5"]],
                msg_type="delta",
                update_id=103,  # gap: skipped 101, 102 (dropped messages)
                cts_ms=cts + 1000,
            ),
        ]
    )
    _install_fake_trades_runtime(monkeypatch, ws)

    args = SimpleNamespace(
        symbol="BTCUSDT",
        channel="orderbook.50",
        count=2,
        output_root=tmp_path,
        source_suffix="",
        deadline_utc=None,
    )

    result = asyncio.run(collect_bybit_depth_segment(args))

    run_path = Path(result["run_path"])
    summary = json.loads(
        (run_path / "metrics" / "replay_summary.json").read_text(encoding="utf-8")
    )
    assert summary["gap_detection"] == "sequence"
    assert "update_id_gaps" in summary["findings"]
    assert summary["gap_count"] == 1
    assert summary["replayable"] is False


# --- Phase 2 #3c: Kraken depth (book) adapter -----------------------------


def _kraken_book_level(price: float, qty: float) -> dict:
    return {"price": price, "qty": qty}


def _kraken_book_frame(
    *,
    bids: list[dict],
    asks: list[dict],
    msg_type: str = "snapshot",
    symbol: str = "BTC/USD",
    checksum: int | None = None,
    timestamp_iso: str | None = None,
) -> dict:
    """Build a Kraken v2 book frame. `data` is a list (one entry per symbol); bids/asks
    are `{price, qty}` objects where qty 0 removes a level. `checksum` (CRC32) is kept in
    metadata and validated at replay time for known-precision pairs (BTC/USD)."""
    entry: dict = {"symbol": symbol, "bids": bids, "asks": asks}
    if checksum is not None:
        entry["checksum"] = checksum
    if timestamp_iso is not None:
        entry["timestamp"] = timestamp_iso
    return {"channel": "book", "type": msg_type, "data": [entry]}


def test_kraken_depth_normalizer_snapshot_parses_object_levels() -> None:
    """Kraken book levels are `{price, qty}` objects (not arrays); they must flatten to
    the `[[price, size]]` shape replay expects, with no sequence ids and the CRC32
    checksum preserved in metadata (validated at replay time for known-precision pairs)."""
    raw = RawMessage(
        source="kraken",
        received_at=utc_now(),
        payload=_kraken_book_frame(
            bids=[_kraken_book_level(50000.0, 1.0), _kraken_book_level(49999.0, 2.0)],
            asks=[_kraken_book_level(50001.0, 0.5)],
            msg_type="snapshot",
            checksum=3093594577,
        ),
    )

    events = KrakenDepthNormalizer().normalize_many(raw)

    assert len(events) == 1
    event = events[0]
    assert event.event_type == "snapshot"
    assert event.channel == "depth"
    assert event.bids == [[50000.0, 1.0], [49999.0, 2.0]]
    assert event.asks == [[50001.0, 0.5]]
    assert event.first_update_id is None
    assert event.final_update_id is None
    assert event.event_time is None  # snapshot has no timestamp
    assert event.metadata["kraken_checksum"] == 3093594577
    # Slashed Kraken pair is collapsed before resolving so the instrument isn't None.
    assert event.instrument is not None
    assert event.instrument.instrument_id == "spot:kraken:BTCUSD"


def test_kraken_depth_normalizer_update_handles_qty_zero_removal() -> None:
    raw = RawMessage(
        source="kraken",
        received_at=utc_now(),
        payload=_kraken_book_frame(
            bids=[_kraken_book_level(50000.0, 0.0)],  # qty 0 = removal
            asks=[_kraken_book_level(50002.0, 1.25)],
            msg_type="update",
            checksum=12345,
            timestamp_iso="2026-05-28T12:00:01.000000Z",
        ),
    )

    event = KrakenDepthNormalizer().normalize_many(raw)[0]

    assert event.event_type == "update"
    assert event.bids == [[50000.0, 0.0]]
    assert event.asks == [[50002.0, 1.25]]
    assert event.event_time == datetime(2026, 5, 28, 12, 0, 1, tzinfo=UTC)
    assert event.metadata["kraken_checksum"] == 12345


def test_kraken_depth_normalizer_non_list_data_yields_nothing() -> None:
    raw = RawMessage(
        source="kraken",
        received_at=utc_now(),
        payload={"channel": "book", "type": "snapshot"},
    )
    assert KrakenDepthNormalizer().normalize_many(raw) == []


def test_cli_parser_kraken_depth_worker_defaults() -> None:
    parser = build_parser()
    args = parser.parse_args(["kraken-depth-worker"])
    assert args.symbol == "BTC/USD"
    assert args.channel == "book"
    assert args.source_suffix == ""
    assert args.rotate_at_midnight is False

    suffixed = parser.parse_args(
        ["kraken-depth-worker", "--symbol", "ETH/USD", "--source-suffix", "ethusd"]
    )
    assert suffixed.symbol == "ETH/USD"
    assert suffixed.source_suffix == "ethusd"


def test_collect_kraken_depth_segment_unknown_pair_is_none_native(
    tmp_path, monkeypatch
) -> None:
    """A Kraken pair whose native precision isn't in the table falls back to
    none_native (no checksum validation) — structurally clean but not gap-proof, so the
    scripted (non-real) checksums are simply not checked."""
    now = utc_now()
    t1 = (now + timedelta(seconds=1)).isoformat().replace("+00:00", "Z")
    ws = _ScriptedDepthWebsocket(
        frames=[
            {"method": "subscribe", "success": True, "result": {"channel": "book", "symbol": "ETH/USD"}},  # ack
            _kraken_book_frame(
                bids=[_kraken_book_level(2000.0, 1.0)],
                asks=[_kraken_book_level(2001.0, 2.0)],
                msg_type="snapshot",
                symbol="ETH/USD",
                checksum=111,
            ),
            _kraken_book_frame(
                bids=[_kraken_book_level(2000.0, 0.0), _kraken_book_level(1999.0, 3.0)],
                asks=[_kraken_book_level(2001.0, 1.5)],
                msg_type="update",
                symbol="ETH/USD",
                checksum=222,
                timestamp_iso=t1,
            ),
        ]
    )
    _install_fake_trades_runtime(monkeypatch, ws)

    args = SimpleNamespace(
        symbol="ETH/USD",  # not in _KRAKEN_BOOK_PRECISION -> none_native fallback
        channel="book",
        count=2,  # data frames
        output_root=tmp_path,
        source_suffix="",
        deadline_utc=None,
    )

    result = asyncio.run(collect_kraken_depth_segment(args))

    assert result["clean_events"] == 2, result
    assert result["replayable"] is True, result["replay_findings"]
    run_path = Path(result["run_path"])
    assert run_path.parent.name == "kraken_depth"
    summary = json.loads(
        (run_path / "metrics" / "replay_summary.json").read_text(encoding="utf-8")
    )
    assert summary["gap_detection"] == "none_native"
    assert summary["replayable"] is True


def test_collect_kraken_depth_segment_btcusd_validates_checksum(tmp_path, monkeypatch) -> None:
    """BTC/USD is in the precision table, so its per-frame CRC32 is validated at replay:
    correct checksums => gap_detection='checksum', replayable. The scripted checksums are
    computed with the same (verified) helper, so this exercises the full collect path."""
    now = utc_now()
    t1 = (now + timedelta(seconds=1)).isoformat().replace("+00:00", "Z")
    # Post-apply book state per frame -> the checksum Kraken would send.
    cs_snap = _kraken_book_crc32({50000.0: 1.0}, {50001.0: 2.0}, 1, 8)
    cs_upd = _kraken_book_crc32({49999.0: 3.0}, {50001.0: 1.5}, 1, 8)
    ws = _ScriptedDepthWebsocket(
        frames=[
            {"method": "subscribe", "success": True, "result": {"channel": "book", "symbol": "BTC/USD"}},  # ack
            _kraken_book_frame(
                bids=[_kraken_book_level(50000.0, 1.0)],
                asks=[_kraken_book_level(50001.0, 2.0)],
                msg_type="snapshot",
                checksum=cs_snap,
            ),
            _kraken_book_frame(
                bids=[_kraken_book_level(50000.0, 0.0), _kraken_book_level(49999.0, 3.0)],
                asks=[_kraken_book_level(50001.0, 1.5)],
                msg_type="update",
                checksum=cs_upd,
                timestamp_iso=t1,
            ),
        ]
    )
    _install_fake_trades_runtime(monkeypatch, ws)

    args = SimpleNamespace(
        symbol="BTC/USD",
        channel="book",
        count=2,
        output_root=tmp_path,
        source_suffix="",
        deadline_utc=None,
    )

    result = asyncio.run(collect_kraken_depth_segment(args))

    assert result["clean_events"] == 2, result
    assert result["replayable"] is True, result["replay_findings"]
    run_path = Path(result["run_path"])
    summary = json.loads(
        (run_path / "metrics" / "replay_summary.json").read_text(encoding="utf-8")
    )
    assert summary["gap_detection"] == "checksum"
    assert summary["mode"] == "stream_snapshot_checksum"
    assert summary["replayable"] is True


def test_binance_rest_snapshot_clean_row_builds_snapshot_event() -> None:
    """The REST snapshot must become a clean 'snapshot' event matching the delta schema,
    with update ids pinned to lastUpdateId, so promotion carries a binance snapshot row."""
    from crypto_collector.cli import _binance_rest_snapshot_clean_row
    from crypto_collector.market_normalizers import BinanceDepthNormalizer

    row = _binance_rest_snapshot_clean_row(
        BinanceDepthNormalizer(),
        source="binance",
        product="btcusdt",
        snapshot={
            "lastUpdateId": 42,
            "bids": [["100.0", "1.0"], ["99.0", "2.0"]],
            "asks": [["101.0", "1.5"]],
        },
        snapshot_last_update_id=42,
        received_at=datetime(2026, 6, 9, 0, 0, 0, tzinfo=UTC),
    )

    assert row["event_type"] == "snapshot"
    assert row["first_update_id"] == 42
    assert row["final_update_id"] == 42
    assert row["product"] == "BTCUSDT"
    assert len(row["bids"]) == 2 and len(row["asks"]) == 1
    assert row["bids"][0][0] == 100.0 and row["bids"][1][0] == 99.0
    assert row["asks"][0][0] == 101.0
    # No exchange event time: the REST snapshot carries none, and stamping local
    # wall clock here put a row whose event_time POSTDATED the buffered deltas
    # written after it (head-of-segment event-time inversion).
    assert row["event_time"] is None


class _RawStr(str):
    """Marker: deliver this frame as a RAW string (no json.dumps) — an undecodable wire frame."""


class _RawCapableWebsocket(_ScriptedDepthWebsocket):
    async def __anext__(self) -> str:
        if self._frames and isinstance(self._frames[0], _RawStr):
            return str(self._frames.pop(0))
        return await super().__anext__()

    async def recv(self) -> str:
        if self._frames and isinstance(self._frames[0], _RawStr):
            return str(self._frames.pop(0))
        return await super().recv()


def test_one_undecodable_frame_is_skipped_and_counted_not_fatal(tmp_path, monkeypatch) -> None:
    """Regression: a single non-JSON frame mid-stream used to propagate out of the
    receive loop as a non-retryable error and kill the whole worker process. It must
    be skipped, counted (decode_error_count in metrics), and collection continue."""
    now = utc_now()
    t1 = now.isoformat().replace("+00:00", "Z")
    ws = _RawCapableWebsocket(
        frames=[
            {"method": "subscribe", "success": True, "result": {"channel": "trade", "symbol": "BTC/USD"}},
            _RawStr("this is { not json"),
            _kraken_trade_frame(
                trades=[
                    _kraken_trade(trade_id=2001, price=50000.0, qty=0.1, side="buy", timestamp_iso=t1),
                ],
                msg_type="update",
            ),
        ]
    )
    _install_fake_trades_runtime(monkeypatch, ws)

    args = SimpleNamespace(
        symbol="BTC/USD",
        channel="trade",
        count=1,
        output_root=tmp_path,
        max_delay_ms=60_000,
        max_future_skew_ms=5_000,
        max_clock_skew_ms=60_000.0,
        source_suffix="",
        deadline_utc=None,
    )

    result = asyncio.run(collect_kraken_trades_segment(args))

    assert result["clean_events"] == 1, result
    summary_rows = [
        json.loads(line)
        for line in (Path(result["run_path"]) / "metrics" / "summary.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
    ]
    assert summary_rows[-1]["decode_error_count"] == 1


def test_persistent_decode_failure_ends_segment_cleanly(tmp_path, monkeypatch) -> None:
    """A sustained run of undecodable frames (wire-format drift) must end the segment
    cleanly — finalized metrics, no exception, worker free to reconnect fresh — not
    crash the process and not spin collecting garbage forever."""
    ws = _RawCapableWebsocket(
        frames=[
            {"method": "subscribe", "success": True, "result": {"channel": "trade", "symbol": "BTC/USD"}},
            *[_RawStr(f"garbage frame {i} {{") for i in range(25)],
        ]
    )
    _install_fake_trades_runtime(monkeypatch, ws)

    args = SimpleNamespace(
        symbol="BTC/USD",
        channel="trade",
        count=100,  # never reached: every data frame is garbage
        output_root=tmp_path,
        max_delay_ms=60_000,
        max_future_skew_ms=5_000,
        max_clock_skew_ms=60_000.0,
        source_suffix="",
        deadline_utc=None,
    )

    result = asyncio.run(collect_kraken_trades_segment(args))

    assert result["clean_events"] == 0
    summary_rows = [
        json.loads(line)
        for line in (Path(result["run_path"]) / "metrics" / "summary.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
    ]
    # Ends at the consecutive-failure cap (20), leaving the remaining frames unread.
    assert summary_rows[-1]["decode_error_count"] == 20


def test_workers_thread_normalized_parquet_and_anchor_timeout_centrally(tmp_path, monkeypatch) -> None:
    """Regression for two more lambda-drop instances: `normalized_parquet` (enumerated by
    only ONE build_segment_args lambda, so --no-normalized-parquet was silently inert on
    every other lane) and binance-depth's `snapshot_anchor_timeout_seconds` (configured
    values never reached the collector; anchoring was pinned at 10.0s). Both are now
    threaded centrally in _run_segmented_worker."""
    import crypto_collector.cli as cli

    captured: dict[str, dict[str, object]] = {}

    def make_fake(key):
        async def fake(segment_args):
            captured[key] = {
                "normalized_parquet": getattr(segment_args, "normalized_parquet", "MISSING"),
                "snapshot_anchor_timeout_seconds": getattr(
                    segment_args, "snapshot_anchor_timeout_seconds", "MISSING"
                ),
            }
            return {
                "run_path": str(tmp_path / key),
                "clean_events": 0,
                "replayable": True,
                "connect_attempts": 1,  # read by the depth worker's progress message
            }

        return fake

    monkeypatch.setattr(cli, "collect_binance_trades_segment", make_fake("binance_trades"))
    monkeypatch.setattr(cli, "collect_binance_depth_segment", make_fake("binance_depth"))

    def drive(job_type, runner, extra_args):
        args = _job_args(
            SimpleNamespace(
                job_type=job_type,
                args={
                    "symbol": "BTCUSDT",
                    "max_segments": 1,
                    "cooldown_seconds": 0.0,
                    "heartbeat_interval_seconds": 0.1,
                    "worker_name": f"{job_type}-centraltest",
                    "output_root": str(tmp_path),
                    "ops_root": str(tmp_path),
                    **extra_args,
                },
            )
        )
        # Mirror the dispatcher's central injection for config-only keys.
        if "normalized_parquet" in extra_args:
            args.normalized_parquet = bool(extra_args["normalized_parquet"])
        runner(args)

    drive(
        "binance-trades-worker",
        cli.run_binance_trades_worker,
        {"normalized_parquet": False},
    )
    drive(
        "binance-depth-worker",
        cli.run_binance_depth_worker,
        {"snapshot_anchor_timeout_seconds": 30.0},
    )

    assert captured["binance_trades"]["normalized_parquet"] is False
    assert captured["binance_depth"]["snapshot_anchor_timeout_seconds"] == 30.0
    # And the defaults still flow when nothing is configured.
    assert captured["binance_depth"]["normalized_parquet"] is True


def test_binance_futures_rest_segment_threads_fsync_cadence_to_pipeline(tmp_path, monkeypatch) -> None:
    """Regression: collect_binance_futures_rest_segment built its CollectorPipeline
    without the fsync cadence knobs, so a per-lane durability tune on any of the three
    REST lanes was silently ignored (pipeline fell back to the 64/200 defaults)."""
    import crypto_collector.cli as cli

    captured: dict[str, object] = {}

    class _FakePipeline:
        def __init__(self, **kwargs) -> None:
            captured.update(kwargs)

        async def run(self, limit=None, deadline_utc=None):
            return SimpleNamespace(
                raw_messages=0, clean_events=0, quarantined_events=0, deadline_reached=False
            )

    monkeypatch.setattr(cli, "CollectorPipeline", _FakePipeline)

    args = SimpleNamespace(
        symbol="BTCUSDT",
        stream="funding",  # stateless: no cursor, no aggtrades scan
        output_root=tmp_path,
        count=1,
        jsonl_fsync=True,
        fsync_interval_events=7,
        fsync_interval_ms=55.0,
        normalized_parquet=False,  # keep the test off the real normalized root
        source_suffix="",
        deadline_utc=None,
    )
    result = asyncio.run(cli.collect_binance_futures_rest_segment(args))

    assert captured["fsync_interval_events"] == 7
    assert captured["fsync_interval_ms"] == 55.0
    assert captured["jsonl_fsync"] is True
    assert captured["normalized_root"] is None
    assert result["replayable"] is False  # no clean events written by the fake


def test_run_mock_does_not_write_normalized_parquet_by_default(tmp_path, monkeypatch) -> None:
    """Regression: a bare `mock` invocation used to write synthetic rows into the LIVE
    normalized `market` dataset via the default-root fallback (the durability test had
    been doing exactly that on the plant box). Normalized output is now opt-in."""
    import crypto_collector.cli as cli

    captured: dict[str, object] = {"called": False}

    class _FakePipeline:
        def __init__(self, **kwargs) -> None:
            captured.update(kwargs)
            captured["called"] = True

        async def run(self, limit=None, deadline_utc=None):
            return SimpleNamespace(
                raw_messages=0,
                clean_events=0,
                quarantined_events=0,
                deadline_reached=False,
                to_dict=lambda: {},
            )

    monkeypatch.setattr(cli, "CollectorPipeline", _FakePipeline)
    args = SimpleNamespace(count=1, output_root=tmp_path, product="BTC-USD")
    asyncio.run(cli.run_mock(args))

    assert captured["called"] is True
    assert captured["normalized_root"] is None

    # An explicit normalized_root (ops job arg) still opts in.
    args = SimpleNamespace(
        count=1, output_root=tmp_path, product="BTC-USD", normalized_root=str(tmp_path / "norm")
    )
    asyncio.run(cli.run_mock(args))
    assert captured["normalized_root"] == Path(tmp_path / "norm")


def test_collector_subprocess_timeout_scales_for_poll_jobs() -> None:
    """A hung 60s-cadence kalshi pool job must be reaped on a cadence-scaled timeout,
    not hold its slot (and gap its lane) for the full 7200s segment-sized default."""
    from crypto_collector.cli import (
        _COLLECTOR_SUBPROCESS_TIMEOUT_SECONDS,
        _collector_subprocess_timeout_seconds,
    )
    from crypto_collector.ops import JobSpec

    kalshi = JobSpec(name="k", job_type="kalshi-collect-crypto-quotes", interval_seconds=60)
    assert _collector_subprocess_timeout_seconds(kalshi) == 300.0  # max(300, 4*60)

    worker = JobSpec(name="w", job_type="bybit-trades-worker", interval_seconds=60)
    assert _collector_subprocess_timeout_seconds(worker) == _COLLECTOR_SUBPROCESS_TIMEOUT_SECONDS

    pinned = JobSpec(
        name="p",
        job_type="kalshi-collect-crypto-quotes",
        interval_seconds=60,
        args={"subprocess_timeout_seconds": 42.0},
    )
    assert _collector_subprocess_timeout_seconds(pinned) == 42.0


def test_coinbase_last_match_is_tagged_subscribe_replay() -> None:
    """`last_match` is always the most recent print from BEFORE the subscription — a
    replay the previous segment already captured. The normalizer tags it so the gate
    quarantines it instead of letting it duplicate into curated."""
    raw = RawMessage(
        source="coinbase",
        received_at=utc_now(),
        payload=_coinbase_match(
            trade_id=99,
            price="50000",
            size="0.1",
            maker_side="sell",
            time_iso="2026-05-28T12:00:00Z",
            event_type="last_match",
        ),
    )

    event = CoinbaseTradeNormalizer().normalize(raw)

    assert event.event_type == "last_match"
    assert event.metadata["subscribe_replay"] is True
    from crypto_collector.quality import QualityGate

    assert "subscribe_replay" in QualityGate().validate(event).reasons

    # A live match is NOT tagged.
    live = CoinbaseTradeNormalizer().normalize(
        RawMessage(
            source="coinbase",
            received_at=utc_now(),
            payload=_coinbase_match(
                trade_id=100,
                price="50000",
                size="0.1",
                maker_side="sell",
                time_iso="2026-05-28T12:00:00Z",
            ),
        )
    )
    assert "subscribe_replay" not in live.metadata


def test_kraken_snapshot_frame_trades_are_tagged_subscribe_replay() -> None:
    """Kraken's trade-channel `snapshot` frame replays the last ~50 historical prints
    on every subscribe; the normalizer tags them per-trade so the gate quarantines
    them. Update-frame trades stay untagged."""
    snapshot_frame = _kraken_trade_frame(
        trades=[_kraken_trade(trade_id=1, price=100.0, qty=0.1, side="buy", timestamp_iso="2026-05-28T12:00:00Z")],
        msg_type="snapshot",
    )
    update_frame = _kraken_trade_frame(
        trades=[_kraken_trade(trade_id=2, price=101.0, qty=0.1, side="sell", timestamp_iso="2026-05-28T12:00:01Z")],
        msg_type="update",
    )
    normalizer = KrakenTradeNormalizer()

    replayed = normalizer.normalize_many(
        RawMessage(source="kraken", received_at=utc_now(), payload=snapshot_frame)
    )
    live = normalizer.normalize_many(
        RawMessage(source="kraken", received_at=utc_now(), payload=update_frame)
    )

    assert replayed[0].metadata["subscribe_replay"] is True
    assert "subscribe_replay" not in live[0].metadata


def test_dict_shaped_depth_levels_quarantine_instead_of_crashing() -> None:
    """Regression: a dict-shaped level item made `item[0]` a KeyError, which the parse
    helpers didn't catch — one malformed frame crashed the whole lane instead of
    quarantining the event with invalid_bids/invalid_asks."""
    from crypto_collector.market_normalizers import _parse_levels, _split_l2_changes

    errors: list[str] = []
    assert _parse_levels([{"price": "1", "size": "2"}], "bids", errors) == []
    assert errors == ["invalid_bids"]

    errors = []
    bids, asks = _split_l2_changes([{"side": "buy"}], errors)
    assert (bids, asks) == ([], [])
    assert errors == ["invalid_changes"]
