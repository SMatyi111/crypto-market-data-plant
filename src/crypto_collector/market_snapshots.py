from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen


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
        return json.loads(response.read().decode("utf-8"))


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
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
