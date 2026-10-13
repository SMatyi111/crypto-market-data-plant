"""Bybit linear universe snapshot - raw-only reference lane (STANDARDS 4.17).

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
  recorded as failures without being sent. Transport faults that are not ``OSError``
  (``http.client.IncompleteRead`` and friends) are recorded as fetch failures too, so
  the summary is always written and received bytes are never discarded.
- Panel membership is evidence from THIS run only. A symbol found in a payload is
  present; a symbol missing from a COMPLETE payload family is recorded absent
  (last-known membership); a symbol missing from an incomplete or failed family is
  recorded ``null`` = unknown, never absent. Absence or unknown is never a validation
  failure, so a delisted contract cannot break the daily job. The lane deletes nothing
  and enables no replacement; nothing reconstructs historical membership.

Each payload is parsed once, in the fetch loop, which records a per-payload verdict;
the shared skeleton (``raw_snapshot.py``) then owns the on-disk contract, re-reads the
bytes from a per-run cache keyed by URL (no second request) and asks the validator for
that recorded verdict. The skeleton's per-payload ``fetched_at`` is therefore the capture
time; the actual request times are in ``payload_fetched_at``.
"""

from __future__ import annotations

import http.client
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
MAX_CURSOR_CHARS = 1024  # untrusted cursor is percent-encoded into the next URL; bound it
MAX_FIELD_CHARS = 64  # summary copies of status/deliveryTime/fundingInterval are bounded
MAX_RESPONSE_BYTES = 1024 * 1024
TIMEOUT_SECONDS = 10.0
WALL_BUDGET_SECONDS = 120.0
LINEAR_URL = f"{BASE_URL}/v5/market/instruments-info?category=linear&limit={PAGE_LIMIT}"
SPOT_URL = f"{BASE_URL}/v5/market/instruments-info?category=spot"
TICKERS_URL = f"{BASE_URL}/v5/market/tickers?category=linear"

Verdict = tuple[int | None, str | None]


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


def _parse_bytes(raw: bytes, category: str) -> tuple[list[dict[str, Any]], str | None, str | None]:
    try:
        parsed = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return [], None, "payload is not valid JSON"
    return _check_page(parsed, category)


def _bounded(value: Any) -> Any:
    """Copy a scalar summary field from an untrusted row: bounded ASCII text, int, or None."""
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, str) and len(value) <= MAX_FIELD_CHARS and value.isascii():
        return value
    return None


def _cursor_url(cursor: str) -> str:
    return f"{LINEAR_URL}&cursor={quote(cursor, safe='')}"


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
    """Capture one dated universe snapshot into its own run dir (raw-only, 4.17)."""
    require_safe_tokens(panel_symbols, "panel_symbols")
    if max_linear_pages < 1:
        raise ValueError("max_linear_pages must be at least 1")
    started = monotonic()
    cache: dict[str, bytes | OSError] = {}
    order: list[tuple[str, str]] = []
    payload_fetched_at: dict[str, str] = {}
    verdicts: dict[str, Verdict] = {}

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
        except http.client.HTTPException as exc:  # IncompleteRead etc. are not OSError
            cache[url] = OSError(f"{type(exc).__name__}: {exc}")
            return None
        cache[url] = raw
        return raw

    # Linear instruments: bounded cursor pagination, every request counted, one parse each.
    linear_rows: list[dict[str, Any]] = []
    seen_symbols: set[str] = set()
    seen_cursors: set[str] = set()
    cursor: str | None = None
    pages = 0
    pagination_complete = False
    instruments_failed = False
    for page in range(1, max_linear_pages + 1):
        pages = page
        name = f"instruments_linear_p{page}"
        raw = get(name, LINEAR_URL if cursor is None else _cursor_url(cursor))
        if raw is None:
            instruments_failed = True
            break
        rows, next_cursor, failure = _parse_bytes(raw, "linear")
        if failure is not None:
            verdicts[name] = (None, f"{name}: {failure}")
            instruments_failed = True
            break
        duplicates = sum(1 for row in rows if row["symbol"] in seen_symbols)
        seen_symbols.update(row["symbol"] for row in rows)
        if duplicates:
            verdicts[name] = (len(rows), f"{name}: {duplicates} symbol(s) repeated across pages")
            instruments_failed = True
            break
        linear_rows.extend(rows)
        if next_cursor is None:
            verdicts[name] = (len(rows), None)
            pagination_complete = True
            break
        if next_cursor in seen_cursors:
            verdicts[name] = (len(rows), f"{name}: repeated cursor")
            instruments_failed = True
            break
        if len(next_cursor) > MAX_CURSOR_CHARS or not next_cursor.isascii():
            # The cursor is untrusted response data echoed into our next URL.
            verdicts[name] = (len(rows), f"{name}: cursor rejected (length or non-ASCII)")
            instruments_failed = True
            break
        seen_cursors.add(next_cursor)
        if page == max_linear_pages:
            verdicts[name] = (len(rows), (
                f"{name}: pagination incomplete after {page} page(s) "
                f"(bounded maximum {max_linear_pages}); universe INCOMPLETE"
            ))
            break
        verdicts[name] = (len(rows), None)
        cursor = next_cursor

    raw = get("instruments_spot", SPOT_URL)
    if raw is not None:
        rows, next_cursor, failure = _parse_bytes(raw, "spot")
        if failure is not None:
            verdicts["instruments_spot"] = (None, f"instruments_spot: {failure}")
        elif next_cursor is not None:
            verdicts["instruments_spot"] = (len(rows), "instruments_spot: unexpected cursor on spot (not followed); universe INCOMPLETE")
        else:
            verdicts["instruments_spot"] = (len(rows), None)

    ticker_symbols: set[str] = set()
    tickers_ok = False
    raw = get("tickers_linear", TICKERS_URL)
    if raw is not None:
        rows, _, failure = _parse_bytes(raw, "linear")
        if failure is not None:
            verdicts["tickers_linear"] = (None, f"tickers_linear: {failure}")
        else:
            verdicts["tickers_linear"] = (len(rows), None)
            ticker_symbols = {row["symbol"] for row in rows}
            tickers_ok = True

    # Membership: present on positive evidence; absent only against a COMPLETE family;
    # otherwise unknown (null). Never fabricated, never a failure.
    instruments_complete = pagination_complete and not instruments_failed
    by_symbol = {row["symbol"]: row for row in linear_rows}
    membership: dict[str, dict[str, Any]] = {}
    for symbol in panel_symbols:
        row = by_symbol.get(symbol)
        in_instruments: bool | None = True if row is not None else (False if instruments_complete else None)
        in_tickers: bool | None = True if symbol in ticker_symbols else (False if tickers_ok else None)
        membership[symbol] = {
            "present_in_instruments": in_instruments,
            "status": _bounded(row.get("status")) if row is not None else None,
            "delivery_time": _bounded(row.get("deliveryTime")) if row is not None else None,
            "funding_interval": _bounded(row.get("fundingInterval")) if row is not None else None,
            "present_in_tickers": in_tickers,
        }
    panel_absent = [s for s in panel_symbols if membership[s]["present_in_instruments"] is False]
    panel_unknown = [s for s in panel_symbols if membership[s]["present_in_instruments"] is None]

    def validate(name: str, parsed: Any) -> Verdict:
        return verdicts.get(name, (None, f"{name}: no verdict recorded"))

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
        "panel_membership_coverage": "complete" if instruments_complete else "incomplete",
        "panel_membership": membership,
        "panel_absent": panel_absent,
        "panel_unknown": panel_unknown,
        "payload_fetched_at": payload_fetched_at,
        "wall_seconds": round(monotonic() - started, 3),
        "limits": {
            "max_response_bytes": MAX_RESPONSE_BYTES,
            "timeout_seconds": TIMEOUT_SECONDS,
            "wall_budget_seconds": wall_budget_seconds,
        },
        "coverage_note": (
            "point-in-time receipt of three payload families; panel membership is "
            "evidence from this run only (absent only against a complete family, "
            "null when unknown), never inferred history; the tickers payload is a "
            "coarse once-a-day reference, not a settled funding rate and not an "
            "executable quote"
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
