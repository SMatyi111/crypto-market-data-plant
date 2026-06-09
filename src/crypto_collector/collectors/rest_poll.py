from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable

from ..models import RawMessage, utc_now
from .base import BaseCollector

# An async poll function returns (payloads, more_pending):
#   payloads     - new normalizer-ready rows since the last poll (each a dict)
#   more_pending - True when the poll hit its page cap and there may be more rows
#                  available right now, so the collector should re-poll IMMEDIATELY
#                  (catch-up) instead of sleeping. This is how the gapless aggTrades
#                  pager keeps up under load.
PollFn = Callable[[], Awaitable[tuple[list[dict], bool]]]


class RestPollingCollector(BaseCollector):
    """Poll a REST endpoint on a cadence and yield each row as a `RawMessage`, so a REST
    feed plugs into the exact same `CollectorPipeline` (normalizer + quality gate + replay
    + curation) as the WebSocket collectors.

    This exists for venues whose WS market data is unavailable from the host but whose
    REST data API works — concretely Binance USDT-M futures, whose `fstream` WS is blocked
    in some jurisdictions while `fapi` REST stays up. The collector is transport-only; the
    venue-specific fetching/paging lives in the injected `poll` callable, so trades, depth
    and funding lanes share this body and differ only in their poll function + normalizer.

    HTTP itself is synchronous stdlib `urllib` run via `asyncio.to_thread` inside the poll
    callable (same approach as the Binance REST depth snapshot), so the event loop is never
    blocked and no new dependency is needed.
    """

    def __init__(self, *, source: str, poll: PollFn, poll_interval_seconds: float) -> None:
        self.source = source
        self._poll = poll
        self.poll_interval_seconds = max(0.0, float(poll_interval_seconds))
        # Parity with GenericWebsocketCollector: the pipeline reads this into
        # metrics/summary.jsonl. A REST poller has no silent-but-connected failure mode
        # (a dead endpoint raises), so it stays 0.
        self.idle_timeout_count = 0

    async def stream(self, limit: int | None = None) -> AsyncIterator[RawMessage]:
        emitted = 0
        while True:
            payloads, more_pending = await self._poll()
            for payload in payloads:
                yield RawMessage(source=self.source, received_at=utc_now(), payload=payload)
                emitted += 1
                if limit is not None and emitted >= limit:
                    return
            # Sleep between polls unless the pager is still catching up. The pipeline's
            # deadline (max_segment_seconds) is checked after each yielded frame, so a
            # bounded segment still rotates promptly for any lane that yields regularly.
            if not more_pending and self.poll_interval_seconds > 0:
                await asyncio.sleep(self.poll_interval_seconds)
