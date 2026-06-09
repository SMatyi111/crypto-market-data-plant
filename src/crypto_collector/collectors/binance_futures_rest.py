from __future__ import annotations

import asyncio
import json
from typing import Any, Callable
from urllib.parse import urlencode
from urllib.request import Request, urlopen

# Binance USDT-M futures REST. Used instead of the fstream WebSocket where the WS market
# data is blocked (jurisdiction) but the REST data API is reachable. Endpoints:
#   /fapi/v1/aggTrades   - aggregate trades, dense `a` id, fromId paging => GAPLESS
#   /fapi/v1/depth       - full order-book snapshot (lastUpdateId, bids, asks)
#   /fapi/v1/premiumIndex - mark price, index price, funding rate (low-rate metric)
FAPI_BASE = "https://fapi.binance.com"
_HTTP_TIMEOUT_SECONDS = 15
_USER_AGENT = "crypto-market-data-plant/0.1.0"

# Type of the (injectable, for tests) blocking HTTP-GET-JSON callable.
FetchFn = Callable[[str, dict], Any]


def _get_json(path: str, params: dict) -> Any:
    url = f"{FAPI_BASE}{path}?{urlencode(params)}"
    request = Request(url, headers={"User-Agent": _USER_AGENT})
    with urlopen(request, timeout=_HTTP_TIMEOUT_SECONDS) as response:
        return json.loads(response.read().decode("utf-8"))


def make_aggtrades_poll(
    symbol: str,
    *,
    page_limit: int = 1000,
    max_pages_per_poll: int = 5,
    fetch: FetchFn = _get_json,
):
    """Build a gapless aggregate-trades poll. Binance's `a` (aggregate trade id) is a dense
    per-symbol counter, so paging by `fromId = last_a + 1` yields a CONTIGUOUS id stream
    with no gaps — curated as a provable `sequence` feed (replay_trades_run, gap-proof),
    same class as the spot trades lane. The first poll (fromId unset) grabs the most recent
    page and anchors `from_id`; each later poll advances from there.

    REST aggTrade rows carry no `s`/`e`, so we inject them to match the shape
    `BinanceTradeNormalizer` reads (`s`=symbol, `e`="aggTrade", `a`/`p`/`q`/`T`/`m`).
    Returns (rows, more_pending) where more_pending=True means the last page was full
    (still catching up) so the collector should re-poll without sleeping.
    """
    sym = symbol.upper()
    state: dict[str, int | None] = {"from_id": None}

    async def poll() -> tuple[list[dict], bool]:
        rows: list[dict] = []
        last_full = False
        for _ in range(max_pages_per_poll):
            params: dict[str, Any] = {"symbol": sym, "limit": page_limit}
            if state["from_id"] is not None:
                params["fromId"] = state["from_id"]
            batch = await asyncio.to_thread(fetch, "/fapi/v1/aggTrades", params)
            if not batch:
                last_full = False
                break
            for item in batch:
                item["s"] = sym
                item["e"] = "aggTrade"
            rows.extend(batch)
            state["from_id"] = int(batch[-1]["a"]) + 1
            last_full = len(batch) >= page_limit
            if not last_full:
                break
        return rows, last_full

    return poll


def make_depth_poll(symbol: str, *, limit: int = 1000, fetch: FetchFn = _get_json):
    """Build a depth-snapshot poll. Each poll fetches a full order book and emits ONE
    snapshot event, remapped to the Binance depth-frame shape `BinanceDepthNormalizer`
    reads (`s`, `e`="snapshot", `E`, `u`, `b`/`a`). Every poll is an independent full book
    (no delta chain), so this is a `none_native` lane validated by replay_depth_stream_run
    as a structurally-clean per-poll book — the same model as the MEXC limit-depth lane.
    Lower temporal fidelity than a WS L2 delta stream (book sampled per poll, not per
    update), but it is the best Binance-futures depth obtainable without the WS feed.
    """
    sym = symbol.upper()

    async def poll() -> tuple[list[dict], bool]:
        snap = await asyncio.to_thread(fetch, "/fapi/v1/depth", {"symbol": sym, "limit": limit})
        payload = {
            "s": sym,
            "e": "snapshot",
            "E": snap.get("E") if snap.get("E") is not None else snap.get("T"),
            "u": snap.get("lastUpdateId"),
            "b": snap.get("bids", []),
            "a": snap.get("asks", []),
        }
        return [payload], False

    return poll


def make_funding_poll(symbol: str, *, fetch: FetchFn = _get_json):
    """Build a mark-price / funding-rate poll over /fapi/v1/premiumIndex. The response is
    a low-rate metric (mark price ticks ~per second, funding rate every 8h), so REST
    polling loses no fidelity. The native premiumIndex dict is passed straight through to
    `BinanceFuturesFundingNormalizer`.
    """
    sym = symbol.upper()

    async def poll() -> tuple[list[dict], bool]:
        row = await asyncio.to_thread(fetch, "/fapi/v1/premiumIndex", {"symbol": sym})
        return [row], False

    return poll
