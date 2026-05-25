from __future__ import annotations

import json
from collections.abc import AsyncIterator

from ..config import CollectorConfig
from ..models import RawMessage, utc_now
from .base import BaseCollector


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
        async with websockets.connect(self.config.websocket_url) as websocket:
            subscription = self._subscription_message()
            if subscription:
                await websocket.send(json.dumps(subscription))
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
                    break

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
