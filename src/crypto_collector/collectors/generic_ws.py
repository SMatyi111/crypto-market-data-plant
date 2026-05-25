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
                    subscription = self._subscription_message()
                    if subscription:
                        await websocket.send(json.dumps(subscription))
                    attempt = 0
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
        raise ValueError(f"Unsupported subscription_style: {self.config.subscription_style}")

    def _should_emit(self, payload: object) -> bool:
        if not isinstance(payload, dict):
            return False
        if self.config.subscription_style == "binance":
            if payload.get("result") is None and "id" in payload:
                return False
            if payload.get("e") is None and "data" not in payload:
                return False
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
