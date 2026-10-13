"""Bybit linear universe snapshot - raw-only reference lane (STANDARDS 4.15).

One run per day archives the exact upstream bytes of three logical payload families:
the linear instruments-info pages (bounded cursor pagination), the spot instruments-info
page (no pagination on spot) and one linear tickers snapshot. That is the dated,
point-in-time record of (a) contract rules and delisting times for every linear
symbol, (b) the spot-pair eligibility set and (c) the turnover rank of every linear
symbol, which the alt research panel's membership rule and any later rebalance must be
read from. The tickers payload also carries open interest value, the funding *quote*
and next funding time, mark and index price once a day: a coarse reference only, never
a settled funding rate and never an executable quote.

Design rules (parent findings, 2026-10-13):

- Requests are counted, not payload families: up to ``MAX_LINEAR_PAGES`` linear pages
  plus one spot and one tickers request. Hitting the page bound with a non-empty cursor
  makes the run INCOMPLETE: the bytes that arrived are archived, the summary says why,
  and the job fails loudly. The current two-page universe is not hard-coded as a truth.
- ``retCode`` must be a strict integer 0 (bool is rejected), the category must match,
  the list must be a list of objects with string symbols, no symbol may repeat across
  pages, a repeated cursor is a failure, spot must not return a cursor.
- Every request has a 1 MiB body cap and a 10 s socket timeout from the shared
  skeleton; the whole run has a wall budget after which remaining requests are
  recorded as failures without being sent.
- Panel symbols absent from the instruments or tickers payload are RECORDED as absent
  in the summary (last-known membership for this run), never fabricated and never a
  validation failure: a delisted contract must not break the daily job forever. The
  lane deletes nothing and enables no replacement; membership is prospective evidence
  from each dated run, never inferred history.

The shared skeleton (``raw_snapshot.py``) owns the on-disk contract. All network
requests happen before the skeleton runs, through a per-run cache keyed by URL, so
the skeleton makes no second request and still records url, bytes, sha256 and
validation per payload. The skeleton's per-payload ``fetched_at`` is therefore the
capture time; the actual request times are in ``payload_fetched_at``.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Callable
from urllib.parse import quote

from ..models import utc_now
from .raw_snapshot import (
    FetchFn,
    RawSnapshotResult,
    capture_raw_snapshot,
    fetch_bytes,
    require_safe_tokens,
)


BASE_URL = "https://api.bybit.com"
SOURCE_NAME = "bybit_linear_universe"
USER_AGENT = "crypto-market-data-plant-bybit-universe/0.1"
DEFAULT_PANEL = ("XLMUSDT", "GRAMUSDT", "TRXUSDT", "MEGAUSDT")
PAGE_LIMIT = 500
MAX_LINEAR_PAGES = 3  # bounded maximum, not a universe size claim
MAX_RESPONSE_BYTES = 1024 * 1024
TIMEOUT_SECONDS = 10.0
WALL_BUDGET_SECONDS = 120.0
LINEAR_URL = f"{BASE_URL}/v5/market/instruments-info?category=linear&limit={PAGE_LIMIT}"
SPOT_URL = f"{BASE_URL}/v5/market/instruments-info?category=spot"
TICKERS_URL = f"{BASE_URL}/v5/market/tickers?category=linear"


def fetch_url(url: str, *, timeout_seconds: float = TIMEOUT_SECONDS) -> bytes:
    return fetch_bytes(
        url,
        user_agent=USER_AGENT,
        timeout_seconds=timeout_seconds,
        max_response_bytes=MAX_RESPONSE_BYTES,
    )


def _strict_int(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _check_page(parsed: Any, category: str) -> tuple[list[dict[str, Any]], str | None, str | None]:
    """Return (rows, next_cursor, failure). Failure strings never include payload content."""
    if not isinstance(parsed, dict):
        return [], None, "payload is not a JSON object"
    code = _strict_int(parsed.get("retCode"))
    if code is None:
        return [], None, "retCode is not a strict integer"
    if code != 0:
        return [], None, f"retCode {code} is not 0"
    result = parsed.get("result")
    if not isinstance(result, dict) or result.get("category") != category:
        return [], None, f"result.category is not {category}"
    rows = result.get("list")
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        return [], None, "result.list is not a list of objects"
    if not all(isinstance(row.get("symbol"), str) and row["symbol"] for row in rows):
        return [], None, "a row has no string symbol"
    cursor = result.get("nextPageCursor")
    if cursor is not None and not isinstance(cursor, str):
        return [], None, "nextPageCursor is not a string"
    return rows, (cursor or None), None


def _cursor_url(cursor: str) -> str:
    return f"{LINEAR_URL}&cursor={quote(cursor, safe='')}"


def _parse_bytes(raw: bytes, category: str) -> tuple[list[dict[str, Any]], str | None, str | None]:
    try:
        parsed = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return [], None, "payload is not valid JSON"
    return _check_page(parsed, category)


def snapshot_bybit_linear_universe(
    output_root: Path | str,
    *,
    panel_symbols: tuple[str, ...] = DEFAULT_PANEL,
    fetch: FetchFn = fetch_url,
    clock: Callable[[], Any] = utc_now,
    monotonic: Callable[[], float] = time.monotonic,
    max_linear_pages: int = MAX_LINEAR_PAGES,
    wall_budget_seconds: float = WALL_BUDGET_SECONDS,
) -> RawSnapshotResult:
    """Capture one dated universe snapshot into its own run dir (raw-only, 4.15)."""
    require_safe_tokens(panel_symbols, "panel_symbols")
    if max_linear_pages < 1:
        raise ValueError("max_linear_pages must be at least 1")
    started = monotonic()
    cache: dict[str, bytes | OSError] = {}
    order: list[tuple[str, str]] = []
    payload_fetched_at: dict[str, str] = {}

    def get(name: str, url: str) -> bytes | None:
        order.append((name, url))
        if monotonic() - started > wall_budget_seconds:
            cache[url] = OSError(f"wall budget of {wall_budget_seconds:.0f}s exhausted before {name}")
            return None
        payload_fetched_at[name] = clock().isoformat()
        try:
            raw = fetch(url)
        except OSError as exc:
            cache[url] = exc
            return None
        cache[url] = raw
        return raw

    # Linear instruments: bounded cursor pagination, every request counted.
    linear_rows: list[dict[str, Any]] = []
    cursor: str | None = None
    seen_cursors: set[str] = set()
    pages = 0
    pagination_complete = False
    for page in range(1, max_linear_pages + 1):
        pages = page
        raw = get(f"instruments_linear_p{page}", LINEAR_URL if cursor is None else _cursor_url(cursor))
        if raw is None:
            break
        rows, next_cursor, failure = _parse_bytes(raw, "linear")
        if failure is not None:
            break
        linear_rows.extend(rows)
        if next_cursor is None:
            pagination_complete = True
            break
        if next_cursor in seen_cursors:
            break  # the validator reports the repeated cursor
        seen_cursors.add(next_cursor)
        cursor = next_cursor
    get("instruments_spot", SPOT_URL)
    tickers_raw = get("tickers_linear", TICKERS_URL)

    # Last-known panel membership from THIS run only; absence is recorded, not failed.
    by_symbol = {row["symbol"]: row for row in linear_rows}
    ticker_symbols: set[str] = set()
    if tickers_raw is not None:
        rows, _, failure = _parse_bytes(tickers_raw, "linear")
        if failure is None:
            ticker_symbols = {row["symbol"] for row in rows}
    membership: dict[str, dict[str, Any]] = {}
    for symbol in panel_symbols:
        row = by_symbol.get(symbol)
        membership[symbol] = {
            "present_in_instruments": row is not None,
            "status": row.get("status") if row is not None else None,
            "delivery_time": row.get("deliveryTime") if row is not None else None,
            "funding_interval": row.get("fundingInterval") if row is not None else None,
            "present_in_tickers": symbol in ticker_symbols,
        }
    absent = [symbol for symbol in panel_symbols if not membership[symbol]["present_in_instruments"]]

    last_linear = f"instruments_linear_p{pages}"
    seen_symbols: set[str] = set()
    seen_page_cursors: set[str] = set()

    def validate(name: str, parsed: Any) -> tuple[int | None, str | None]:
        if name.startswith("instruments_linear_p"):
            rows, next_cursor, failure = _check_page(parsed, "linear")
            if failure is not None:
                return None, f"{name}: {failure}"
            duplicates = [row["symbol"] for row in rows if row["symbol"] in seen_symbols]
            seen_symbols.update(row["symbol"] for row in rows)
            if duplicates:
                return len(rows), f"{name}: {len(duplicates)} symbol(s) repeated across pages"
            if next_cursor is not None and next_cursor in seen_page_cursors:
                return len(rows), f"{name}: repeated cursor"
            if next_cursor is not None:
                seen_page_cursors.add(next_cursor)
            if name == last_linear and next_cursor is not None:
                return len(rows), (
                    f"{name}: pagination incomplete after {pages} page(s) "
                    f"(bounded maximum {max_linear_pages}); universe INCOMPLETE"
                )
            return len(rows), None
        if name == "instruments_spot":
            rows, next_cursor, failure = _check_page(parsed, "spot")
            if failure is not None:
                return None, f"{name}: {failure}"
            if next_cursor is not None:
                return len(rows), f"{name}: unexpected cursor on spot (not followed); universe INCOMPLETE"
            return len(rows), None
        if name == "tickers_linear":
            rows, _, failure = _check_page(parsed, "linear")
            if failure is not None:
                return None, f"{name}: {failure}"
            return len(rows), None
        return None, f"unknown payload {name}"

    def cached_fetch(url: str) -> bytes:
        value = cache[url]
        if isinstance(value, OSError):
            raise value
        return value

    extra_summary = {
        "panel_symbols": list(panel_symbols),
        "payload_families": 3,
        "request_count": len(order),
        "linear_pages_fetched": pages,
        "max_linear_pages": max_linear_pages,
        "linear_pagination_complete": pagination_complete,
        "panel_membership": membership,
        "panel_absent": absent,
        "payload_fetched_at": payload_fetched_at,
        "wall_seconds": round(monotonic() - started, 3),
        "limits": {
            "max_response_bytes": MAX_RESPONSE_BYTES,
            "timeout_seconds": TIMEOUT_SECONDS,
            "wall_budget_seconds": wall_budget_seconds,
        },
        "coverage_note": (
            "point-in-time receipt of three payload families; panel membership is "
            "prospective evidence from this run only, never inferred history; the "
            "tickers payload is a coarse once-a-day reference, not a settled funding "
            "rate and not an executable quote"
        ),
    }
    return capture_raw_snapshot(
        output_root,
        source=SOURCE_NAME,
        payloads=order,
        validate=validate,
        fetch=cached_fetch,
        clock=clock,
        extra_summary=extra_summary,
    )
