from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

from ..models import RawMessage, utc_now
from .base import BaseCollector


class MockL3Collector(BaseCollector):
    def __init__(self, *, source: str, product: str) -> None:
        self.source = source
        self.product = product

    async def stream(self, limit: int | None = None) -> AsyncIterator[RawMessage]:
        count = limit or 25
        for sequence in range(1, count + 1):
            yield RawMessage(
                source=self.source,
                received_at=utc_now(),
                payload={
                    "type": "open",
                    "product_id": self.product,
                    "channel": "full",
                    "sequence": sequence,
                    "time": utc_now().isoformat(),
                    "side": "buy" if sequence % 2 else "sell",
                    "price": f"{100_000 + sequence:.2f}",
                    "size": "0.0100",
                    "order_id": f"order-{sequence}",
                },
            )
            await asyncio.sleep(0)

