from __future__ import annotations

import asyncio
import base64
import gzip
import hashlib
import json
import logging
import os
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

# Text-capture P1 lanes (ROADMAP item 15, owner-approved 2026-07-13). Two REST-polled
# lane families feeding the standard raw -> quarantine -> promote chain:
#
#   text-rss    - five crypto news RSS/Atom feeds, conditional GET (ETag /
#                 If-Modified-Since), 1-5 minute polling. Validated by the 72h P0
#                 probe (artifacts/text_probe/, local-only): 10,740 polls, 421 item
#                 rows, zero duplicate new ids, zero missing/future source
#                 timestamps, two transient network errors.
#   text-reddit - fixed subreddit list, OAuth *client-credentials* (app-only) read
#                 of /new posts + comments. No account password anywhere; the app
#                 id/secret live in an EXTERNAL json file outside the repo.
#
# Capture contract (STANDARDS "text" section): raw text only - no capture-time NLP
# or filtering; per-row envelope (source, source_id, source_ts, ingestion_ts, poll
# metadata, untouched raw payload); dedup key (source, source_id, content_hash)
# with edits retained as new rows. `source_ts` is the PLATFORM-CLAIMED timestamp
# and is preserved verbatim but never trusted for gating: the probe caught a ~16h
# stale Cointelegraph pubDate outlier, so `ingestion_ts` (plant clock) is the
# authoritative time axis for partitioning, ordering, and any future as-of join.
#
# The content hash is computed over the SEMANTIC fields only (RSS: title, link,
# summary; Reddit: title+selftext / comment body), NOT the raw XML/JSON blob. The
# probe's 37 observed edits split 25 semantic (title changed) vs 12 raw-only feed
# churn (attribute noise, whitespace, tracking params); hashing the semantic fields
# means raw-only churn emits NO new row while every semantic edit is retained as an
# `edit` row. A content revert (A -> B -> A) legitimately re-emits an earlier
# (source, source_id, content_hash) pair - the scorer reports it, non-gating.

# The five probe-validated P1 feeds. The set is deliberately fixed (source
# selection is the only "filtering" the contract allows); changing it is a config
# change on the lane, not a code default edit.
DEFAULT_RSS_FEEDS: dict[str, str] = {
    "coindesk": "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "cointelegraph": "https://cointelegraph.com/rss",
    "theblock": "https://www.theblock.co/rss.xml",
    "decrypt": "https://decrypt.co/feed",
    "bitcoinmagazine": "https://bitcoinmagazine.com/feed",
}

# The owner-approved fixed subreddit list (P1 scope).
DEFAULT_REDDIT_SUBREDDITS: tuple[str, ...] = (
    "CryptoCurrency",
    "Bitcoin",
    "ethereum",
    "CryptoMarkets",
    "BitcoinMarkets",
)

# Descriptive User-Agents: both platforms ask that automated read-only clients
# identify themselves. Reddit additionally wants the platform:app:version shape and
# will rate-limit generic UAs harder. The reddit UA can be overridden from the
# credentials file (`user_agent` key) so the owner can append their /u/ handle
# without a code change.
RSS_USER_AGENT = "crypto-market-data-plant/0.1 (read-only research RSS collector)"
DEFAULT_REDDIT_USER_AGENT = (
    "windows:crypto-market-data-plant:0.1 (read-only research collector)"
)

_HTTP_TIMEOUT_SECONDS = 20.0
REDDIT_TOKEN_URL = "https://www.reddit.com/api/v1/access_token"
REDDIT_OAUTH_BASE = "https://oauth.reddit.com"
# Refresh the app token this many seconds before its reported expiry so a poll
# never races the deadline mid-listing.
_TOKEN_REFRESH_MARGIN_SECONDS = 60.0

_ATOM_NS = "{http://www.w3.org/2005/Atom}"
# Separator for hash inputs and seen-map keys: a control char that cannot appear in
# titles/ids, so joined fields can't collide ("a"+"bc" vs "ab"+"c").
_SEP = "\x1f"


# ---------------------------------------------------------------------------
# Envelope helpers
# ---------------------------------------------------------------------------


def semantic_content_hash(*fields: str | None) -> str:
    """SHA-256 over the semantic content fields (edit detection key). Raw-payload
    churn that leaves these fields untouched produces the same hash -> no row."""
    return hashlib.sha256(
        _SEP.join(part or "" for part in fields).encode("utf-8", "replace")
    ).hexdigest()


def parse_source_ts(raw: str | None) -> datetime | None:
    """Best-effort parse of a platform-claimed timestamp (RSS RFC-822 or Atom
    ISO-8601) to an aware UTC datetime. None on absent/unparseable input - the
    claim is preserved verbatim in `source_ts_raw` either way and never gates
    capture (ingestion_ts is the authoritative clock)."""
    if not raw:
        return None
    try:  # RSS 2.0 RFC-822 dates
        from email.utils import parsedate_to_datetime

        parsed = parsedate_to_datetime(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    except (TypeError, ValueError):
        pass
    try:  # Atom ISO-8601 dates
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    except ValueError:
        return None


def _seen_key(product: str, source_id: str) -> str:
    return f"{product}{_SEP}{source_id}"


# ---------------------------------------------------------------------------
# Cross-segment seen-state cursor
# ---------------------------------------------------------------------------
# Each collector segment is its own subprocess (same as every other lane), so an
# in-memory dedup map would reset every ~30-min rotation and re-emit the feeds'
# whole visible window (~25 items/feed, 100/listing on reddit) as "new" rows every
# segment. Like the aggTrades cursor, the (source_id -> content_hash) map is
# persisted OUTSIDE the run-dir tree and reloaded at segment start. The write
# happens only after the segment's clean events are durably closed (at-least-once):
# a crash before the write re-emits a bounded window as duplicate rows - which the
# (source, source_id, content_hash) dedup key absorbs at read time - never a gap.

TEXT_SEEN_DIR_NAME = "_cursors"
DEFAULT_SEEN_CAP = 5000


def text_seen_path(output_root: Path | str, source_name: str) -> Path:
    """Per-lane seen-map file, in the `_cursors/` sibling dir outside the run-dir
    tree so run-dir scans (promotion/quarantine/offload) never mistake it for a run."""
    return Path(output_root) / TEXT_SEEN_DIR_NAME / f"{source_name}_seen.json"


def read_text_seen(path: Path | str) -> dict[str, str]:
    """Load the persisted seen map (insertion order = recency). Never raises: a
    torn/corrupt file reads as empty (bounded duplicate re-emit, not a crash loop)."""
    try:
        text = Path(path).read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return {}
    try:
        data = json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return {}
    entries = data.get("entries") if isinstance(data, dict) else None
    if not isinstance(entries, list):
        return {}
    seen: dict[str, str] = {}
    for entry in entries:
        if (
            isinstance(entry, list)
            and len(entry) == 2
            and isinstance(entry[0], str)
            and isinstance(entry[1], str)
        ):
            seen[entry[0]] = entry[1]
    return seen


def write_text_seen(
    path: Path | str,
    seen: dict[str, str],
    *,
    cap: int = DEFAULT_SEEN_CAP,
    now: datetime | None = None,
) -> None:
    """Atomically persist the seen map, keeping only the `cap` most recent entries.
    Feeds only expose a bounded recent window (RSS ~25 items/feed, reddit 100 per
    listing), so entries older than the cap can never be re-served as "new" by the
    platform - dropping them keeps the cursor O(bounded) forever."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    entries = list(seen.items())[-max(1, int(cap)):]
    payload = {
        "version": 1,
        "updated_at": (now or datetime.now(tz=UTC)).isoformat(),
        "entries": entries,
    }
    tmp = target.with_name(f"{target.name}.{os.getpid()}.tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True))
        handle.flush()
        # fsync before the atomic rename (same as the aggTrades cursor): a power cut
        # must not promote a zero-length tmp into place - a corrupt cursor silently
        # re-emits the whole window as duplicates on every restart.
        os.fsync(handle.fileno())
    tmp.replace(target)


def _remember(seen: dict[str, str], key: str, digest: str) -> None:
    # Re-insert on update so dict insertion order tracks recency for the cap.
    seen.pop(key, None)
    seen[key] = digest


# ---------------------------------------------------------------------------
# RSS lane
# ---------------------------------------------------------------------------

# Injectable conditional-GET fetch: (url, etag, last_modified) ->
# (http_status, body_bytes, response_etag, response_last_modified).
FetchRssFn = Callable[[str, str | None, str | None], tuple[int, bytes, str | None, str | None]]


def _fetch_rss(
    url: str, etag: str | None, last_modified: str | None
) -> tuple[int, bytes, str | None, str | None]:
    """Default conditional GET. 304 returns an empty body and echoes the validators
    back so the caller's conditional state survives. Gzip bodies are decoded here
    (Cointelegraph serves gzip regardless of Accept-Encoding - probe-verified)."""
    request = Request(url, headers={"User-Agent": RSS_USER_AGENT})
    if etag:
        request.add_header("If-None-Match", etag)
    if last_modified:
        request.add_header("If-Modified-Since", last_modified)
    try:
        with urlopen(request, timeout=_HTTP_TIMEOUT_SECONDS) as response:
            body = response.read()
            if response.headers.get("Content-Encoding") == "gzip":
                body = gzip.decompress(body)
            return (
                response.status,
                body,
                response.headers.get("ETag") or etag,
                response.headers.get("Last-Modified") or last_modified,
            )
    except HTTPError as error:
        if error.code == 304:
            return 304, b"", etag, last_modified
        raise


def parse_rss_items(body: bytes) -> list[dict[str, Any]]:
    """Minimal RSS 2.0 / Atom item extraction (probe-validated against all five P1
    feeds). Each item keeps its raw XML fragment untouched alongside the semantic
    fields used for the content hash."""
    root = ET.fromstring(body)
    items: list[dict[str, Any]] = []
    for node in root.iter("item"):  # RSS 2.0
        items.append(
            {
                "source_id": (node.findtext("guid") or node.findtext("link") or "").strip(),
                "title": (node.findtext("title") or "").strip(),
                "link": (node.findtext("link") or "").strip(),
                "summary": (node.findtext("description") or "").strip(),
                "source_ts_raw": (node.findtext("pubDate") or "").strip() or None,
                "raw_item": ET.tostring(node, encoding="unicode"),
            }
        )
    if not items:  # Atom fallback
        for node in root.iter(f"{_ATOM_NS}entry"):
            link_el = node.find(f"{_ATOM_NS}link")
            link = link_el.get("href", "") if link_el is not None else ""
            items.append(
                {
                    "source_id": (node.findtext(f"{_ATOM_NS}id") or link or "").strip(),
                    "title": (node.findtext(f"{_ATOM_NS}title") or "").strip(),
                    "link": link,
                    "summary": (node.findtext(f"{_ATOM_NS}summary") or "").strip(),
                    "source_ts_raw": (
                        node.findtext(f"{_ATOM_NS}published")
                        or node.findtext(f"{_ATOM_NS}updated")
                        or ""
                    ).strip()
                    or None,
                    "raw_item": ET.tostring(node, encoding="unicode"),
                }
            )
    return items


@dataclass(slots=True)
class TextPollState:
    """Mutable state shared between a text poll fn and its collect-segment wrapper:
    the dedup map (persisted across segments), per-feed conditional-GET validators,
    and observability counters the segment summary reports."""

    seen: dict[str, str] = field(default_factory=dict)
    conditional: dict[str, dict[str, str | None]] = field(default_factory=dict)
    poll_seq: int = 0
    error_count: int = 0
    new_count: int = 0
    edit_count: int = 0


def make_rss_poll(
    feeds: dict[str, str],
    state: TextPollState,
    *,
    fetch: FetchRssFn = _fetch_rss,
    time_fn: Callable[[], float] = time.perf_counter,
):
    """Build the RSS poll fn for `RestPollingCollector`. One poll cycle sweeps every
    feed with a conditional GET and yields one payload per NEW or semantically
    EDITED item. A single feed failing (network blip, malformed XML) is contained:
    it logs, bumps `state.error_count`, and the sweep continues - the 72h probe saw
    exactly two such transient errors and no feed-level flakiness beyond that."""

    async def poll() -> tuple[list[dict], bool]:
        state.poll_seq += 1
        payloads: list[dict] = []
        for feed_name, url in feeds.items():
            cond = state.conditional.setdefault(
                url, {"etag": None, "last_modified": None}
            )
            started = time_fn()
            try:
                status, body, etag, last_modified = await asyncio.to_thread(
                    fetch, url, cond["etag"], cond["last_modified"]
                )
            except (HTTPError, URLError, OSError, TimeoutError) as exc:
                state.error_count += 1
                logger.warning("text-rss poll failed for %s: %s", feed_name, exc)
                continue
            latency_ms = round((time_fn() - started) * 1000.0, 1)
            cond["etag"] = etag
            cond["last_modified"] = last_modified
            if status == 304 or not body:
                continue
            try:
                items = parse_rss_items(body)
            except ET.ParseError as exc:
                state.error_count += 1
                logger.warning("text-rss parse failed for %s: %s", feed_name, exc)
                continue
            poll_meta = {
                "poll_seq": state.poll_seq,
                "http_status": status,
                "latency_ms": latency_ms,
                "feed_url": url,
            }
            for item in items:
                source_id = item["source_id"]
                if not source_id:
                    # No stable id -> can't participate in the dedup contract. Emit
                    # anyway (raw must keep everything); the normalizer tags it
                    # missing_source_id and the gate quarantines it for forensics.
                    payloads.append(
                        _rss_payload(feed_name, item, row_type="new", poll=poll_meta)
                    )
                    continue
                digest = semantic_content_hash(
                    item["title"], item["link"], item["summary"]
                )
                key = _seen_key(feed_name, source_id)
                prior = state.seen.get(key)
                if prior == digest:
                    continue
                row_type = "new" if prior is None else "edit"
                if row_type == "new":
                    state.new_count += 1
                else:
                    state.edit_count += 1
                _remember(state.seen, key, digest)
                payloads.append(
                    _rss_payload(
                        feed_name, item, row_type=row_type, poll=poll_meta, digest=digest
                    )
                )
        return payloads, False

    return poll


def _rss_payload(
    feed_name: str,
    item: dict[str, Any],
    *,
    row_type: str,
    poll: dict[str, Any],
    digest: str | None = None,
) -> dict[str, Any]:
    return {
        "kind": "rss_item",
        "row_type": row_type,
        "feed": feed_name,
        "source_id": item["source_id"] or None,
        "title": item["title"],
        "link": item["link"],
        "summary": item["summary"],
        "source_ts_raw": item["source_ts_raw"],
        "content_hash": digest,
        "raw_item": item["raw_item"],
        "poll": dict(poll),
    }


# ---------------------------------------------------------------------------
# Reddit lane
# ---------------------------------------------------------------------------


def default_reddit_credentials_path() -> Path:
    """The owner-provisioned OAuth app credentials live OUTSIDE the repo (ops root),
    never in git: `<ops_root>/reddit_app.json` with keys `client_id`,
    `client_secret`, and optional `user_agent`. No account password is involved
    anywhere - this is app-only (client-credentials) read access."""
    from ..config import default_ops_root

    return default_ops_root() / "reddit_app.json"


def read_reddit_credentials(path: Path | str) -> dict[str, str]:
    """Load and validate the external credentials file. Raises with an actionable
    message when absent/incomplete: a misconfigured lane must fail loudly at
    segment start (error heartbeat), not silently poll unauthenticated."""
    target = Path(path)
    try:
        data = json.loads(target.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise RuntimeError(
            f"reddit credentials file not found: {target} - the text-reddit lane "
            "needs an owner-created OAuth app (client_id + client_secret json, "
            "no account password) at that path"
        ) from exc
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"reddit credentials file unreadable: {target}: {exc}") from exc
    if not isinstance(data, dict) or not data.get("client_id") or not data.get("client_secret"):
        raise RuntimeError(
            f"reddit credentials file must contain client_id and client_secret: {target}"
        )
    return {
        "client_id": str(data["client_id"]),
        "client_secret": str(data["client_secret"]),
        "user_agent": str(data.get("user_agent") or DEFAULT_REDDIT_USER_AGENT),
    }


# Injectable seams (tests stub these; no live network in the suite).
FetchTokenFn = Callable[[str, str], dict[str, Any]]  # (basic_auth_b64, user_agent) -> token json
FetchListingFn = Callable[[str, dict[str, str]], dict[str, Any]]  # (url, headers) -> listing json


def _fetch_reddit_token(basic_auth_b64: str, user_agent: str) -> dict[str, Any]:
    request = Request(
        REDDIT_TOKEN_URL,
        data=urlencode({"grant_type": "client_credentials"}).encode("ascii"),
        headers={
            "Authorization": f"Basic {basic_auth_b64}",
            "User-Agent": user_agent,
        },
        method="POST",
    )
    with urlopen(request, timeout=_HTTP_TIMEOUT_SECONDS) as response:
        return json.loads(response.read().decode("utf-8"))


def _fetch_reddit_listing(url: str, headers: dict[str, str]) -> dict[str, Any]:
    request = Request(url, headers=headers)
    with urlopen(request, timeout=_HTTP_TIMEOUT_SECONDS) as response:
        return json.loads(response.read().decode("utf-8"))


class RedditAppAuth:
    """Client-credentials (app-only) token holder. Fetches lazily, refreshes ahead
    of expiry, and never touches any account password. The Basic header is built
    once from the external credentials file."""

    def __init__(
        self,
        credentials: dict[str, str],
        *,
        fetch_token: FetchTokenFn = _fetch_reddit_token,
        time_fn: Callable[[], float] = time.monotonic,
    ) -> None:
        self.user_agent = credentials["user_agent"]
        self._basic = base64.b64encode(
            f"{credentials['client_id']}:{credentials['client_secret']}".encode("utf-8")
        ).decode("ascii")
        self._fetch_token = fetch_token
        self._time_fn = time_fn
        self._token: str | None = None
        self._expires_at = 0.0

    def invalidate(self) -> None:
        self._token = None

    def token(self) -> str:
        if self._token is None or self._time_fn() >= self._expires_at:
            payload = self._fetch_token(self._basic, self.user_agent)
            token = payload.get("access_token") if isinstance(payload, dict) else None
            if not token:
                raise RuntimeError(f"reddit token response missing access_token: {payload!r}")
            self._token = str(token)
            expires_in = payload.get("expires_in", 3600)
            try:
                expires_in = float(expires_in)
            except (TypeError, ValueError):
                expires_in = 3600.0
            self._expires_at = self._time_fn() + max(
                60.0, expires_in - _TOKEN_REFRESH_MARGIN_SECONDS
            )
        return self._token

    def headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token()}",
            "User-Agent": self.user_agent,
        }


def make_reddit_poll(
    subreddits: list[str] | tuple[str, ...],
    state: TextPollState,
    auth: RedditAppAuth,
    *,
    listing_limit: int = 100,
    request_pause_seconds: float = 1.0,
    fetch_listing: FetchListingFn = _fetch_reddit_listing,
    sleep=asyncio.sleep,
    time_fn: Callable[[], float] = time.perf_counter,
):
    """Build the reddit poll fn: per subreddit, read-only GETs of /new (posts) and
    /comments, paced `request_pause_seconds` apart. At the default 60s poll interval
    and 5 subs this is ~10 requests/min against the ~100 QPM app budget - an order
    of magnitude of headroom by construction. A 401 mid-sweep invalidates the token
    and retries that listing once (app tokens expire ~hourly); other per-listing
    errors are contained exactly like the RSS lane's."""
    subs = [str(sub) for sub in subreddits]

    async def _get_listing(url: str) -> dict[str, Any]:
        headers = auth.headers()
        try:
            return await asyncio.to_thread(fetch_listing, url, headers)
        except HTTPError as error:
            if error.code != 401:
                raise
            # Expired/revoked token: refresh once and retry this listing.
            auth.invalidate()
            headers = auth.headers()
            return await asyncio.to_thread(fetch_listing, url, headers)

    async def poll() -> tuple[list[dict], bool]:
        state.poll_seq += 1
        payloads: list[dict] = []
        first_request = True
        for sub in subs:
            for listing in ("new", "comments"):
                if not first_request and request_pause_seconds > 0:
                    await sleep(request_pause_seconds)
                first_request = False
                url = (
                    f"{REDDIT_OAUTH_BASE}/r/{sub}/{listing}.json"
                    f"?limit={int(listing_limit)}&raw_json=1"
                )
                started = time_fn()
                try:
                    body = await _get_listing(url)
                except (HTTPError, URLError, OSError, TimeoutError, RuntimeError) as exc:
                    state.error_count += 1
                    logger.warning(
                        "text-reddit poll failed for r/%s %s: %s", sub, listing, exc
                    )
                    continue
                latency_ms = round((time_fn() - started) * 1000.0, 1)
                poll_meta = {
                    "poll_seq": state.poll_seq,
                    "http_status": 200,
                    "latency_ms": latency_ms,
                    "listing": listing,
                }
                for child in _listing_children(body):
                    payload = _reddit_payload(sub, listing, child, poll_meta, state)
                    if payload is not None:
                        payloads.append(payload)
        return payloads, False

    return poll


def _listing_children(body: Any) -> list[dict[str, Any]]:
    data = body.get("data") if isinstance(body, dict) else None
    children = data.get("children") if isinstance(data, dict) else None
    if not isinstance(children, list):
        return []
    out: list[dict[str, Any]] = []
    for child in children:
        child_data = child.get("data") if isinstance(child, dict) else None
        if isinstance(child_data, dict):
            out.append(child_data)
    return out


def _reddit_payload(
    sub: str,
    listing: str,
    child: dict[str, Any],
    poll_meta: dict[str, Any],
    state: TextPollState,
) -> dict[str, Any] | None:
    source_id = str(child.get("name") or "") or None  # fullname: t3_xxx / t1_xxx
    if listing == "new":
        digest = semantic_content_hash(
            str(child.get("title") or ""), str(child.get("selftext") or "")
        )
    else:
        digest = semantic_content_hash(str(child.get("body") or ""))
    if source_id is not None:
        key = _seen_key(sub, source_id)
        prior = state.seen.get(key)
        if prior == digest:
            return None
        row_type = "new" if prior is None else "edit"
        if row_type == "new":
            state.new_count += 1
        else:
            state.edit_count += 1
        _remember(state.seen, key, digest)
    else:
        # Emit id-less rows for forensics; the quality gate quarantines them.
        row_type = "new"
    created_utc = child.get("created_utc")
    return {
        "kind": "reddit_item",
        "row_type": row_type,
        "subreddit": sub,
        "listing": listing,
        "source_id": source_id,
        "created_utc": created_utc if isinstance(created_utc, (int, float)) else None,
        "content_hash": digest if source_id is not None else None,
        # Untouched raw payload as a compact JSON string: reddit children are deep,
        # variably-shaped structs - a string column keeps the parquet schema stable
        # while losing nothing (json.loads round-trips it).
        "raw_item": json.dumps(child, sort_keys=True, ensure_ascii=False),
        "poll": dict(poll_meta),
    }
