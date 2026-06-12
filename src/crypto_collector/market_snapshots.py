from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .storage import write_text_atomic

DEFAULT_HTTP_TIMEOUT = 30


def fetch_binance_order_book_snapshot(
    *,
    symbol: str,
    limit: int = 1000,
    base_url: str = "https://api.binance.com/api/v3/depth",
    user_agent: str = "crypto-market-data-plant/0.1.0",
) -> dict[str, Any]:
    query = urlencode({"symbol": symbol.upper(), "limit": limit})
    request = Request(f"{base_url}?{query}", headers={"User-Agent": user_agent})
    with urlopen(request, timeout=DEFAULT_HTTP_TIMEOUT) as response:
        payload = json.loads(response.read().decode("utf-8"))
    # This payload becomes the run's replay anchor — validate the shape before it
    # is persisted, so a proxy error page / partial body fails the segment loudly
    # at fetch time instead of poisoning the anchor for the whole run.
    if not isinstance(payload, dict) or not all(
        key in payload for key in ("lastUpdateId", "bids", "asks")
    ):
        raise ValueError(
            f"binance depth snapshot for {symbol.upper()} missing required keys "
            f"(got: {sorted(payload)[:8] if isinstance(payload, dict) else type(payload).__name__})"
        )
    return payload


def write_snapshot_file(
    path: Path,
    *,
    source: str,
    product: str,
    snapshot: dict[str, Any],
    received_at: datetime | None = None,
) -> None:
    payload = {
        "source": source,
        "product": product,
        "received_at": (received_at or datetime.now(tz=UTC)).isoformat(),
        "snapshot": snapshot,
    }
    # Atomic + fsynced: this sidecar is the run's replay anchor — a torn file (hard
    # kill mid-write) or a power-cut-promoted empty tmp fails the run at scoring.
    write_text_atomic(path, json.dumps(payload, indent=2, sort_keys=True), fsync=True)
