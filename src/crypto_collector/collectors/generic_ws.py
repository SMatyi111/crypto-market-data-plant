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
                async with websockets.connect(self.config.websocket_url) as websocket:
                    pending = await self._subscribe(websocket)
                    attempt = 0
                    for raw in pending:
                        payload = raw.payload
                        if not self._should_emit(payload):
                            continue
                        yield raw
                        message_count += 1
                        if limit is not None and message_count >= limit:
                            return
                    async for message in websocket:
                        payload = json.loads(message)
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
                payload = json.loads(raw)
            except (TypeError, ValueError):
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
