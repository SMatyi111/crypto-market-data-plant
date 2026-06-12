from __future__ import annotations

import asyncio
import json
import os
from datetime import UTC, datetime
from pathlib import Path
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
    initial_from_id: int | None = None,
    fetch: FetchFn = _get_json,
):
    """Build a gapless aggregate-trades poll. Binance's `a` (aggregate trade id) is a dense
    per-symbol counter, so paging by `fromId = last_a + 1` yields a CONTIGUOUS id stream
    with no gaps — curated as a provable `sequence` feed (replay_trades_run, gap-proof),
    same class as the spot trades lane.

    Continuity is dense WITHIN a poll and, crucially, ACROSS segment rotations: each
    collector segment is its own subprocess, so without a seed the pager would reset to
    "now" every rotation and silently miss (or, on a liquid market, re-fetch and duplicate)
    every trade in the rotation window. The caller therefore passes `initial_from_id` — the
    last durably-written `a` + 1 from the previous segment (see `aggtrades_resume_from_id`)
    — so the stream stays gapless and overlap-free across rotations. Only the first-ever
    poll for a lane (no prior cursor) leaves `initial_from_id` unset, anchoring to the most
    recent page.

    REST aggTrade rows carry no `s`/`e`, so we inject them to match the shape
    `BinanceTradeNormalizer` reads (`s`=symbol, `e`="aggTrade", `a`/`p`/`q`/`T`/`m`).
    Returns (rows, more_pending) where more_pending=True means the last page was full
    (still catching up) so the collector should re-poll without sleeping.
    """
    sym = symbol.upper()
    state: dict[str, int | None] = {"from_id": initial_from_id}

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


# --- aggTrades cross-segment continuity --------------------------------------
# Each collector segment is its own subprocess, so the aggTrades pager's `from_id`
# would reset every ~30-min rotation and re-anchor to the most-recent page. On an
# illiquid market that silently DROPS every trade in the rotation window; on a
# liquid market the most-recent page reaches back past the prior segment's tail, so
# the overlap is RE-FETCHED into a fresh run and (promotion has no cross-run dedup)
# emitted as duplicate `a` ids. Because `a` is a dense per-symbol counter we instead
# persist the highest DURABLY-WRITTEN `a` and seed the next segment from there:
# strictly forward, no gap and no overlap — the one continuity guarantee a dense-id
# REST feed can make that a WS reconnect cannot. The cursor advances only after a
# segment's clean events are on disk (at-least-once), so a crash risks a small
# bounded re-fetch (dup), never a gap.

CURSOR_DIR_NAME = "_cursors"


def aggtrades_cursor_path(output_root: Path | str, source_name: str) -> Path:
    """Per-lane cursor file, kept in a `_cursors/` sibling dir OUTSIDE the run-dir tree
    so promotion/quarantine/replay/manifest run-dir scans (all `is_dir()`-filtered and
    timestamp-named) never mistake it for a run."""
    return Path(output_root) / CURSOR_DIR_NAME / f"{source_name}.json"


def read_aggtrades_cursor(path: Path | str) -> dict | None:
    """Return the persisted cursor, or None if absent/unreadable. Never raises: a torn or
    corrupt cursor is treated as 'no cursor' (re-anchor to live) rather than bricking the
    lane on every restart."""
    try:
        text = Path(path).read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        # UnicodeDecodeError: a torn write / disk garbage can leave non-UTF-8 bytes;
        # without this the "never raises" contract broke and the lane crash-looped.
        return None
    try:
        data = json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def write_aggtrades_cursor(
    path: Path | str, *, symbol: str, last_agg_id: int, now: datetime | None = None
) -> None:
    """Atomically persist the highest durably-written aggregate-trade id for this lane."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "symbol": symbol.upper(),
        "last_agg_id": int(last_agg_id),
        "updated_at": (now or datetime.now(tz=UTC)).isoformat(),
    }
    tmp = target.with_name(f"{target.name}.{os.getpid()}.tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True))
        handle.flush()
        # fsync BEFORE the atomic rename: without it a power cut can promote a
        # zero-length/garbage tmp into place, and a corrupt cursor re-anchors the
        # lane to live silently — an unlogged gap on an otherwise gap-proof lane.
        os.fsync(handle.fileno())
    tmp.replace(target)


def aggtrades_resume_from_id(
    cursor: dict | None,
    *,
    symbol: str,
    now: datetime,
    max_resume_gap_seconds: float,
) -> tuple[int | None, str | None]:
    """Decide the next segment's starting `fromId` from the persisted cursor.

    Returns (initial_from_id, reset_finding). `initial_from_id=None` means "anchor to the
    most-recent page" (first-ever poll, or a deliberate reset). `reset_finding` is a
    non-None tag when a usable cursor was intentionally discarded, so the segment can
    surface a logged gap.
    """
    if not cursor:
        return None, None
    if str(cursor.get("symbol", "")).upper() != symbol.upper():
        return None, "cursor_reset_symbol_mismatch"
    last = cursor.get("last_agg_id")
    if not isinstance(last, int) or isinstance(last, bool):
        return None, "cursor_reset_invalid"
    updated_at = _parse_iso(cursor.get("updated_at"))
    if updated_at is None:
        return None, "cursor_reset_invalid"
    if (now - updated_at).total_seconds() > max_resume_gap_seconds:
        # Extended downtime: paging forward from a very old id would backfill an unbounded
        # number of trades (and blow the fapi weight budget), so accept a single logged
        # gap and re-anchor to live instead.
        return None, "cursor_reset_stale_gap"
    return last + 1, None


def max_agg_id_in_events(events_path: Path | str) -> int | None:
    """Highest normalized `sequence` (= aggregate-trade `a`) durably written to a run's
    clean events. None when the run wrote no sequenced trades. Streams the file so memory
    stays flat for a full-segment run."""
    highest: int | None = None
    try:
        handle = Path(events_path).open("r", encoding="utf-8")
    except OSError:
        return None
    with handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            try:
                row = json.loads(stripped)
            except (json.JSONDecodeError, ValueError):
                continue
            seq = row.get("sequence") if isinstance(row, dict) else None
            if isinstance(seq, int) and not isinstance(seq, bool):
                if highest is None or seq > highest:
                    highest = seq
    return highest


def max_agg_id_in_recent_runs(
    output_root: Path | str,
    source_name: str,
    *,
    now: datetime,
    max_age_seconds: float,
    scan_limit: int = 3,
) -> int | None:
    """Highest durably-written aggregate-trade id across the lane's recent run dirs.

    The cursor advances only when a segment completes normally, so a segment that
    dies mid-run (hard kill, transient HTTP error — the poll has no internal retry)
    leaves clean events on disk beyond the cursor. Resuming from the stale cursor
    re-fetches that whole range into the next run, and the hourly catch-up scorer +
    run-keyed promotion (no cross-run row dedup) then curate every one of those rows
    TWICE. Scanning the newest few run dirs' clean events recovers the true durable
    high-water so the resume floor can be raised past the already-written range.

    Only run dirs younger than `max_age_seconds` count — the same bound
    `aggtrades_resume_from_id` applies to a stale cursor, so an extended outage
    still re-anchors to live (one logged gap) instead of paging an unbounded
    backfill through the fapi weight budget."""
    lane_root = Path(output_root) / source_name
    try:
        run_dirs = sorted(
            (path for path in lane_root.iterdir() if path.is_dir()),
            key=lambda path: path.name,
            reverse=True,
        )
    except OSError:
        return None
    scanned = 0
    for run_dir in run_dirs:
        if scanned >= scan_limit:
            break
        started_at = _parse_run_dir_timestamp(run_dir.name)
        if started_at is None:
            continue
        if (now - started_at).total_seconds() > max_age_seconds:
            break  # dirs are sorted newest-first; everything past here is older
        scanned += 1
        candidate = max_agg_id_in_events(run_dir / "clean" / "events.jsonl")
        if candidate is not None:
            # `a` ids ascend with time, so the newest run that wrote any sequenced
            # trade already holds the lane's durable maximum — no need to scan older
            # (larger) files.
            return candidate
    return None


def _parse_run_dir_timestamp(name: str) -> datetime | None:
    """Run dirs are named `YYYYMMDD_HHMMSS[...]` (see prepare_run_paths). UTC."""
    try:
        return datetime.strptime(name[:15], "%Y%m%d_%H%M%S").replace(tzinfo=UTC)
    except ValueError:
        return None


def _parse_iso(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)
