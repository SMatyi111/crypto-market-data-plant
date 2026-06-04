from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator

from ..config import CollectorConfig
from ..models import RawMessage, utc_now
from .base import BaseCollector

logger = logging.getLogger(__name__)


class GenericWebsocketCollector(BaseCollector):
    def __init__(self, config: CollectorConfig) -> None:
        self.config = config
        # Number of times the data-arrival watchdog fired (no data frame within
        # `idle_timeout_seconds`). Read by the pipeline into metrics/summary.jsonl so
        # the health report can see a silent-but-connected feed. Stays 0 when the
        # watchdog is disabled (the default) or never trips.
        self.idle_timeout_count = 0

    async def stream(self, limit: int | None = None) -> AsyncIterator[RawMessage]:
        try:
            import websockets
        except ImportError as exc:
            raise RuntimeError("Install the 'websockets' package to use the live collector.") from exc

        if not self.config.websocket_url:
            raise ValueError("websocket_url is required for live collection")

        message_count = 0
        attempt = 0
        max_attempts = max(1, int(self.config.connect_retries))
        while True:
            try:
                async with websockets.connect(
                    self.config.websocket_url, max_size=self.config.max_message_bytes
                ) as websocket:
                    pending = await self._subscribe(websocket)
                    attempt = 0
                    # Start the app-level keepalive (if configured) only after the
                    # subscription handshake, so a pong reply can never be mistaken
                    # for the subscribe ack. Always torn down in `finally` — on a
                    # reconnect, an exception, or `return` when `limit` is reached —
                    # so a stale ping task can't outlive its socket.
                    keepalive_task = self._start_keepalive(websocket)
                    try:
                        for raw in pending:
                            payload = raw.payload
                            if not self._should_emit(payload):
                                continue
                            yield raw
                            message_count += 1
                            if limit is not None and message_count >= limit:
                                return
                        # Data-arrival watchdog: when idle_timeout > 0 each wait for the
                        # next frame is bounded, so a feed that acks then goes silent
                        # can't hang the loop forever. idle_timeout <= 0 (the default)
                        # is the exact `async for message in websocket` behavior.
                        idle_timeout = float(self.config.idle_timeout_seconds or 0.0)
                        message_iter = websocket.__aiter__()
                        while True:
                            try:
                                if idle_timeout > 0:
                                    message = await asyncio.wait_for(
                                        message_iter.__anext__(), timeout=idle_timeout
                                    )
                                else:
                                    message = await message_iter.__anext__()
                            except StopAsyncIteration:
                                # Server closed the stream cleanly — leave the loop so
                                # the reconnect path below runs (unchanged behavior).
                                break
                            except (asyncio.TimeoutError, TimeoutError):
                                # No data frame within idle_timeout: the feed acked the
                                # subscription but went silent-but-connected. End the
                                # stream cleanly so the consumer finalizes (writes
                                # metrics + replay summary) and the worker opens a fresh
                                # segment, instead of blocking forever in recv.
                                self.idle_timeout_count += 1
                                logger.warning(
                                    "websocket idle timeout source=%s channel=%s "
                                    "timeout=%.1fs idle_timeout_count=%d; ending segment",
                                    self.config.source,
                                    self.config.channel,
                                    idle_timeout,
                                    self.idle_timeout_count,
                                )
                                return
                            payload = self._decode_frame(message)
                            if not self._should_emit(payload):
                                continue
                            yield RawMessage(
                                source=self.config.source,
                                received_at=utc_now(),
                                payload=payload,
                            )
                            message_count += 1
                            if limit is not None and message_count >= limit:
                                return
                    finally:
                        await self._stop_keepalive(keepalive_task)
                # Server closed the stream cleanly — reconnect so we keep recording.
                logger.warning("websocket closed cleanly; reconnecting source=%s", self.config.source)
            except Exception as exc:  # noqa: BLE001
                attempt += 1
                if attempt >= max_attempts or not _is_retryable_connect_error(exc):
                    raise
                delay = _backoff_delay(
                    attempt=attempt,
                    base=self.config.retry_backoff_seconds,
                    cap=self.config.max_backoff_seconds,
                )
                logger.warning(
                    "websocket reconnect source=%s attempt=%d delay=%.2fs error=%s",
                    self.config.source,
                    attempt,
                    delay,
                    exc,
                )
                await asyncio.sleep(delay)

    def _start_keepalive(self, websocket: object) -> asyncio.Task | None:
        """Spawn the app-level ping task if configured; otherwise return None.

        Sending happens in a separate task while the main loop awaits `recv` —
        the websockets library permits concurrent send + recv. Bybit's pong reply
        carries no `topic`, so `_should_emit` already drops it from the data path.
        """
        interval = float(self.config.ping_interval_seconds or 0.0)
        message = self.config.ping_message
        if interval <= 0 or not message:
            return None
        return asyncio.ensure_future(self._keepalive_loop(websocket, interval, message))

    async def _keepalive_loop(self, websocket: object, interval: float, message: dict) -> None:
        payload = json.dumps(message)
        # CancelledError is a BaseException and propagates out of this `except` —
        # so a normal teardown (cancel) ends the task cleanly, while a dead socket
        # (send raises) just ends the ping loop and lets the main loop reconnect.
        try:
            while True:
                await asyncio.sleep(interval)
                await websocket.send(payload)
        except Exception as exc:  # noqa: BLE001
            logger.debug("keepalive ping ended source=%s error=%s", self.config.source, exc)

    async def _stop_keepalive(self, task: asyncio.Task | None) -> None:
        if task is None:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    def _decode_frame(self, message: object) -> dict:
        """Turn one raw WS frame into a payload dict.

        Default path is `json.loads` — exactly what every JSON venue used before.
        When `config.message_decoder` is set (MEXC), a **binary** frame is handed to
        that callable (protobuf decode) instead; text frames (subscription ack, PONG)
        still go through `json.loads`, so the shared control-frame handling is
        unchanged. Leaving the decoder unset preserves byte-for-byte behavior for
        every other lane, including the live Binance collector.
        """
        decoder = self.config.message_decoder
        if decoder is not None and isinstance(message, (bytes, bytearray)):
            return decoder(bytes(message))
        return json.loads(message)

    async def _subscribe(self, websocket: object) -> list[RawMessage]:
        """Send subscription and wait for an ack. Returns any data frames received during the wait
        so the caller can forward them instead of dropping recorded traffic on the floor."""
        subscription = self._subscription_message()
        if not subscription:
            return []
        await websocket.send(json.dumps(subscription))
        timeout = float(self.config.subscription_ack_timeout_seconds)
        if timeout <= 0:
            return []
        buffered: list[RawMessage] = []
        loop = asyncio.get_event_loop()
        deadline = loop.time() + timeout
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise TimeoutError(
                    f"subscription ack timed out for {self.config.source}:{self.config.channel}"
                )
            raw = await asyncio.wait_for(websocket.recv(), timeout=remaining)
            try:
                payload = self._decode_frame(raw)
            except Exception:  # noqa: BLE001 - malformed frame during ack wait; skip it
                # json.loads raises ValueError/TypeError; the optional binary decoder
                # (MEXC protobuf) can raise its own DecodeError. Either way a single
                # undecodable buffered frame is skipped, not fatal to the handshake.
                continue
            if self._is_subscription_error(payload):
                raise RuntimeError(f"subscription rejected: {payload}")
            if self._is_subscription_ack(payload):
                return buffered
            if isinstance(payload, dict) and self._should_emit(payload):
                buffered.append(
                    RawMessage(
                        source=self.config.source,
                        received_at=utc_now(),
                        payload=payload,
                    )
                )

    def _is_subscription_ack(self, payload: object) -> bool:
        if not isinstance(payload, dict):
            return False
        if self.config.subscription_style == "coinbase":
            return payload.get("type") == "subscriptions"
        if self.config.subscription_style == "binance":
            return payload.get("result") is None and "id" in payload
        if self.config.subscription_style == "bybit":
            # {"success":true,"ret_msg":"subscribe","op":"subscribe",...}; the pong
            # reply uses op:"ping", so key off op:"subscribe" to avoid matching it.
            return payload.get("op") == "subscribe" and payload.get("success") is True
        if self.config.subscription_style == "kraken_v2":
            # {"method":"subscribe","success":true,...}; one ack per symbol.
            return payload.get("method") == "subscribe" and payload.get("success") is True
        if self.config.subscription_style == "mexc":
            # {"id":0,"code":0,"msg":"<topic>"}; code 0 = success. The PONG reply also
            # has code 0 + msg:"PONG", but the keepalive only starts AFTER this
            # handshake, so a pong can't be mistaken for the ack here.
            return payload.get("code") == 0 and "msg" in payload
        return False

    def _is_subscription_error(self, payload: object) -> bool:
        if not isinstance(payload, dict):
            return False
        if self.config.subscription_style == "coinbase":
            return payload.get("type") == "error"
        if self.config.subscription_style == "binance":
            error = payload.get("error")
            if isinstance(error, dict) and error:
                return True
            code = payload.get("code")
            return code not in (None, 0)
        if self.config.subscription_style == "bybit":
            return payload.get("op") == "subscribe" and payload.get("success") is False
        if self.config.subscription_style == "kraken_v2":
            return payload.get("method") == "subscribe" and payload.get("success") is False
        if self.config.subscription_style == "mexc":
            # A non-zero code is an error (e.g. a blocked / unknown subscription).
            code = payload.get("code")
            return "code" in payload and code not in (None, 0)
        return False

    def _subscription_message(self) -> dict[str, object]:
        if self.config.subscription_style == "none":
            return {}
        if self.config.subscription_style == "coinbase":
            return {
                "type": "subscribe",
                "product_ids": [self.config.product],
                "channels": [self.config.channel],
            }
        if self.config.subscription_style == "binance":
            return {
                "method": "SUBSCRIBE",
                "params": [f"{self.config.product.lower()}@{self.config.channel}"],
                "id": 1,
            }
        if self.config.subscription_style == "bybit":
            # Topic is "<channel>.<symbol>", e.g. "publicTrade.BTCUSDT" or
            # "orderbook.50.BTCUSDT" (the depth level is part of the channel).
            return {
                "op": "subscribe",
                "args": [f"{self.config.channel}.{self.config.product}"],
            }
        if self.config.subscription_style == "kraken_v2":
            return {
                "method": "subscribe",
                "params": {
                    "channel": self.config.channel,
                    "symbol": [self.config.product],
                },
            }
        if self.config.subscription_style == "mexc":
            # config.channel carries the full topic the worker built, e.g.
            # "spot@public.aggre.deals.v3.api.pb@100ms@BTCUSDT" (trades) or
            # "spot@public.limit.depth.v3.api.pb@BTCUSDT@20" (limit depth).
            return {"method": "SUBSCRIPTION", "params": [self.config.channel]}
        raise ValueError(f"Unsupported subscription_style: {self.config.subscription_style}")

    def _should_emit(self, payload: object) -> bool:
        if not isinstance(payload, dict):
            return False
        if self.config.subscription_style == "binance":
            if payload.get("result") is None and "id" in payload:
                return False
            if payload.get("e") is None and "data" not in payload:
                return False
        if self.config.subscription_style == "coinbase":
            if payload.get("type") in {"subscriptions", "error"}:
                return False
        if self.config.subscription_style == "bybit":
            # Data frames carry a "topic"; acks/pongs carry "op" and no topic.
            return "topic" in payload
        if self.config.subscription_style == "kraken_v2":
            # Data frames are channel trade/book; drop heartbeat/status/pong + acks.
            return payload.get("channel") in {"trade", "book"} and "data" in payload
        if self.config.subscription_style == "mexc":
            # Decoded protobuf data frames carry "channel" + one of the public bodies;
            # the JSON ack / PONG reply carry code+msg and no channel, so they're dropped.
            return "channel" in payload and (
                "publicAggreDeals" in payload or "publicLimitDepths" in payload
            )
        return True


def _is_retryable_connect_error(exc: BaseException) -> bool:
    if isinstance(exc, (TimeoutError, OSError, asyncio.TimeoutError)):
        return True
    name = type(exc).__name__
    if name in {"ConnectionClosed", "ConnectionClosedError", "ConnectionClosedOK", "InvalidStatusCode"}:
        return True
    text = str(exc).lower()
    return any(
        token in text
        for token in (
            "opening handshake",
            "timed out",
            "connection reset",
            "network is unreachable",
            "no route to host",
            "temporary failure in name resolution",
        )
    )


def _backoff_delay(*, attempt: int, base: float, cap: float) -> float:
    base = max(0.0, float(base))
    cap = max(base, float(cap))
    return min(cap, base * (2 ** (attempt - 1)))
