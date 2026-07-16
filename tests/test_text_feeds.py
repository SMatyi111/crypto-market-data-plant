"""Text-capture lane unit tests: RSS/Atom parsing, semantic-hash dedup + edit
semantics, conditional GET, the cross-segment seen cursor, and the reddit
client-credentials poller. All network behavior is mocked - the suite must stay
hermetic (no live feeds, no credentials)."""

from __future__ import annotations

import asyncio
import base64
import json
from datetime import UTC, datetime
from email.utils import format_datetime
from pathlib import Path
from urllib.error import HTTPError, URLError

import pytest

from crypto_collector.collectors.text_feeds import (
    DEFAULT_REDDIT_SUBREDDITS,
    DEFAULT_RSS_FEEDS,
    RedditAppAuth,
    TextPollState,
    durable_seen_snapshot,
    make_reddit_poll,
    make_rss_poll,
    parse_rss_items,
    parse_source_ts,
    read_reddit_credentials,
    read_text_seen,
    semantic_content_hash,
    text_seen_path,
    write_text_seen,
)
from crypto_collector.models import RawMessage, utc_now
from crypto_collector.text_normalizers import TextItemNormalizer, TextQualityGate


def _rss_body(items: list[dict], *, extra_attr: str = "") -> bytes:
    """Build a minimal RSS 2.0 document. `extra_attr` injects raw-XML churn inside
    the <item> element without touching the semantic fields."""
    chunks = []
    for item in items:
        guid = f"<guid>{item['guid']}</guid>" if item.get("guid") else ""
        pub = f"<pubDate>{item['pub']}</pubDate>" if item.get("pub") else ""
        chunks.append(
            f"<item{extra_attr}>{guid}<title>{item.get('title', '')}</title>"
            f"<link>{item.get('link', '')}</link>"
            f"<description>{item.get('desc', '')}</description>{pub}</item>"
        )
    return (
        "<?xml version='1.0'?><rss version='2.0'><channel>"
        + "".join(chunks)
        + "</channel></rss>"
    ).encode("utf-8")


_ATOM_BODY = b"""<?xml version='1.0'?>
<feed xmlns='http://www.w3.org/2005/Atom'>
  <entry>
    <id>urn:atom-1</id>
    <title>Atom title</title>
    <link href='https://example.com/atom-1'/>
    <summary>Atom summary</summary>
    <published>2026-07-15T10:00:00Z</published>
  </entry>
</feed>
"""


def test_parse_rss_items_rss2_and_atom() -> None:
    items = parse_rss_items(
        _rss_body(
            [{"guid": "g1", "title": "T1", "link": "https://x/1", "desc": "D1", "pub": "Tue, 14 Jul 2026 08:00:00 GMT"}]
        )
    )
    assert len(items) == 1
    assert items[0]["source_id"] == "g1"
    assert items[0]["title"] == "T1"
    assert items[0]["source_ts_raw"] == "Tue, 14 Jul 2026 08:00:00 GMT"
    assert "<item>" in items[0]["raw_item"]

    atom = parse_rss_items(_ATOM_BODY)
    assert len(atom) == 1
    assert atom[0]["source_id"] == "urn:atom-1"
    assert atom[0]["link"] == "https://example.com/atom-1"
    assert atom[0]["source_ts_raw"] == "2026-07-15T10:00:00Z"


def test_parse_source_ts_rfc822_iso_and_garbage() -> None:
    rfc = parse_source_ts("Tue, 14 Jul 2026 08:00:00 GMT")
    assert rfc == datetime(2026, 7, 14, 8, 0, 0, tzinfo=UTC)
    iso = parse_source_ts("2026-07-15T10:00:00Z")
    assert iso == datetime(2026, 7, 15, 10, 0, 0, tzinfo=UTC)
    assert parse_source_ts("not a timestamp") is None
    assert parse_source_ts(None) is None
    assert parse_source_ts("") is None


def test_semantic_hash_separator_prevents_field_collisions() -> None:
    assert semantic_content_hash("a", "bc") != semantic_content_hash("ab", "c")
    assert semantic_content_hash("t", "l", None) == semantic_content_hash("t", "l", "")


class _FakeRssFetch:
    """Scripted conditional-GET fetch: pops one (status, body, etag, lm) per feed
    call and records the conditional headers it was invoked with."""

    def __init__(self) -> None:
        self.responses: list[tuple[int, bytes, str | None, str | None]] = []
        self.calls: list[tuple[str, str | None, str | None]] = []

    def __call__(self, url: str, etag: str | None, last_modified: str | None):
        self.calls.append((url, etag, last_modified))
        status, body, new_etag, new_lm = self.responses.pop(0)
        if status == 304:
            return 304, b"", etag, last_modified
        return status, body, new_etag, new_lm


def _run_poll(poll):
    return asyncio.run(poll())


def test_rss_poll_dedup_edit_and_raw_only_churn() -> None:
    feeds = {"feedx": "https://feedx/rss"}
    state = TextPollState()
    fetch = _FakeRssFetch()
    poll = make_rss_poll(feeds, state, fetch=fetch)

    base = [{"guid": "g1", "title": "T1", "link": "https://x/1", "desc": "D1", "pub": "Tue, 14 Jul 2026 08:00:00 GMT"}]
    # 1) first sight -> one "new" payload with the full envelope.
    fetch.responses.append((200, _rss_body(base), "e1", "lm1"))
    payloads, more = _run_poll(poll)
    assert more is False
    assert len(payloads) == 1
    row = payloads[0]
    assert row["kind"] == "rss_item"
    assert row["row_type"] == "new"
    assert row["feed"] == "feedx"
    assert row["source_id"] == "g1"
    assert row["content_hash"] == semantic_content_hash("T1", "https://x/1", "D1")
    assert row["poll"]["http_status"] == 200
    assert row["poll"]["poll_seq"] == 1
    assert "<item>" in row["raw_item"]

    # 2) identical body again -> no payload (seen).
    fetch.responses.append((200, _rss_body(base), "e1", "lm1"))
    payloads, _ = _run_poll(poll)
    assert payloads == []

    # 3) raw-only churn (same semantic fields, different raw XML) -> still no row.
    fetch.responses.append((200, _rss_body(base, extra_attr=" tracking='zz'"), "e1", "lm1"))
    payloads, _ = _run_poll(poll)
    assert payloads == []

    # 4) semantic edit (title change) -> one "edit" row, same source_id, new hash.
    edited = [dict(base[0], title="T1 updated")]
    fetch.responses.append((200, _rss_body(edited), "e2", "lm2"))
    payloads, _ = _run_poll(poll)
    assert len(payloads) == 1
    assert payloads[0]["row_type"] == "edit"
    assert payloads[0]["source_id"] == "g1"
    assert payloads[0]["content_hash"] != row["content_hash"]

    assert state.new_count == 1
    assert state.edit_count == 1
    assert state.error_count == 0


def test_rss_poll_sends_conditional_headers_and_handles_304() -> None:
    feeds = {"feedx": "https://feedx/rss"}
    state = TextPollState()
    fetch = _FakeRssFetch()
    poll = make_rss_poll(feeds, state, fetch=fetch)

    body = _rss_body([{"guid": "g1", "title": "T", "link": "l", "desc": "d", "pub": None}])
    fetch.responses.append((200, body, "etag-1", "lm-1"))
    _run_poll(poll)
    # First call carries no validators.
    assert fetch.calls[0] == ("https://feedx/rss", None, None)

    fetch.responses.append((304, b"", None, None))
    payloads, _ = _run_poll(poll)
    assert payloads == []
    # Second call sent the stored validators; 304 preserved them for the third.
    assert fetch.calls[1] == ("https://feedx/rss", "etag-1", "lm-1")
    fetch.responses.append((304, b"", None, None))
    _run_poll(poll)
    assert fetch.calls[2] == ("https://feedx/rss", "etag-1", "lm-1")


def test_rss_poll_contains_per_feed_errors() -> None:
    feeds = {"bad": "https://bad/rss", "good": "https://good/rss"}
    state = TextPollState()
    calls = {"n": 0}

    def fetch(url, etag, last_modified):
        calls["n"] += 1
        if "bad" in url:
            raise URLError("boom")
        return 200, _rss_body([{"guid": "g", "title": "T", "link": "l", "desc": "d", "pub": None}]), None, None

    poll = make_rss_poll(feeds, state, fetch=fetch)
    payloads, _ = _run_poll(poll)
    assert len(payloads) == 1  # the good feed still flowed
    assert state.error_count == 1
    assert calls["n"] == 2


def test_rss_poll_emits_idless_items_for_quarantine() -> None:
    feeds = {"feedx": "https://feedx/rss"}
    state = TextPollState()
    fetch = _FakeRssFetch()
    body = _rss_body([{"guid": "", "title": "no id", "link": "", "desc": "", "pub": None}])
    fetch.responses.append((200, body, None, None))
    poll = make_rss_poll(feeds, state, fetch=fetch)
    payloads, _ = _run_poll(poll)
    assert len(payloads) == 1
    assert payloads[0]["source_id"] is None
    assert payloads[0]["content_hash"] is None
    # And the normalizer + gate route it to quarantine.
    event = TextItemNormalizer(source="rss").normalize(
        RawMessage(source="rss", received_at=utc_now(), payload=payloads[0])
    )
    verdict = TextQualityGate().validate(event)
    assert not verdict.accepted
    assert "missing_source_id" in verdict.reasons
    assert "missing_content_hash" in verdict.reasons


def test_default_feed_and_subreddit_sets_are_the_approved_p1_scope() -> None:
    assert set(DEFAULT_RSS_FEEDS) == {
        "coindesk",
        "cointelegraph",
        "theblock",
        "decrypt",
        "bitcoinmagazine",
    }
    assert list(DEFAULT_REDDIT_SUBREDDITS) == [
        "CryptoCurrency",
        "Bitcoin",
        "ethereum",
        "CryptoMarkets",
        "BitcoinMarkets",
    ]


def test_rss_parse_failure_preserves_conditional_validators() -> None:
    """A 200 body that fails to parse must NOT commit its validators: they belong
    to a revision whose items were never captured, and a stored ETag would 304 that
    revision away until the feed's next change."""
    feeds = {"feedx": "https://feedx/rss"}
    state = TextPollState()
    fetch = _FakeRssFetch()
    poll = make_rss_poll(feeds, state, fetch=fetch)

    fetch.responses.append((200, b"<rss><channel><item>torn", "etag-bad", "lm-bad"))
    payloads, _ = _run_poll(poll)
    assert payloads == []
    assert state.error_count == 1
    # Next poll still carries NO validators -> the revision is re-fetched.
    good = _rss_body([{"guid": "g1", "title": "T", "link": "l", "desc": "d", "pub": None}])
    fetch.responses.append((200, good, "etag-good", "lm-good"))
    payloads, _ = _run_poll(poll)
    assert fetch.calls[1] == ("https://feedx/rss", None, None)
    assert len(payloads) == 1


def test_rss_zero_item_200_body_counts_as_error_every_poll() -> None:
    """A well-formed body with zero recognizable items (e.g. an RSS 1.0/RDF feed)
    must stay loudly visible: error counted per poll, validators not committed."""
    feeds = {"feedx": "https://feedx/rss"}
    state = TextPollState()
    fetch = _FakeRssFetch()
    poll = make_rss_poll(feeds, state, fetch=fetch)
    empty = b"<?xml version='1.0'?><rss version='2.0'><channel></channel></rss>"
    fetch.responses.append((200, empty, "e1", "lm1"))
    fetch.responses.append((200, empty, "e1", "lm1"))
    _run_poll(poll)
    _run_poll(poll)
    assert state.error_count == 2
    # Validators never committed -> both calls were unconditional re-fetches.
    assert fetch.calls == [
        ("https://feedx/rss", None, None),
        ("https://feedx/rss", None, None),
    ]


def test_rss_unchanged_sighting_refreshes_recency() -> None:
    """A still-visible unchanged item must be recency-refreshed (seen + refreshed)
    so seen-cap eviction can only hit items no longer served by the platform."""
    feeds = {"feedx": "https://feedx/rss"}
    state = TextPollState()
    digest = semantic_content_hash("T", "l", "d")
    key = "feedx\x1fg-old"
    state.seen[key] = digest
    state.seen["feedx\x1fg-newer"] = "otherhash"  # inserted after -> more recent
    fetch = _FakeRssFetch()
    body = _rss_body([{"guid": "g-old", "title": "T", "link": "l", "desc": "d", "pub": None}])
    fetch.responses.append((200, body, None, None))
    poll = make_rss_poll(feeds, state, fetch=fetch)
    payloads, _ = _run_poll(poll)
    assert payloads == []  # unchanged -> no row
    assert list(state.seen) == ["feedx\x1fg-newer", key]  # moved to the recent end
    assert state.refreshed == {key: digest}


def test_durable_seen_snapshot_only_persists_written_evidence(tmp_path: Path) -> None:
    """Poll-side seen state can run ahead of the files when a segment ends
    mid-batch; the persisted cursor must contain only durable evidence, or the
    promised bounded duplicate becomes a permanent gap."""
    initial = {"feedx\x1fa": "h-a"}
    refreshed = {"feedx\x1fa": "h-a"}  # sighted unchanged this segment
    events = tmp_path / "events.jsonl"
    events.write_text(
        json.dumps({"product": "feedx", "source_id": "b", "content_hash": "h-b"}) + "\n"
        + "not json\n"  # tolerant of a torn line
        + json.dumps({"product": "feedx", "source_id": "c"}) + "\n",  # no hash -> skipped
        encoding="utf-8",
    )
    # "d" was remembered in-memory by the poll but never written: must NOT persist.
    snapshot = durable_seen_snapshot(initial, refreshed, events)
    assert snapshot == {"feedx\x1fa": "h-a", "feedx\x1fb": "h-b"}
    assert list(snapshot) == ["feedx\x1fa", "feedx\x1fb"]
    # Missing events file: snapshot is just initial + refreshed.
    assert durable_seen_snapshot(initial, refreshed, tmp_path / "absent.jsonl") == initial


# ---------------------------------------------------------------------------
# Seen-state cursor
# ---------------------------------------------------------------------------


def test_seen_cursor_roundtrip_cap_and_corruption_tolerance(tmp_path: Path) -> None:
    path = text_seen_path(tmp_path, "text_rss")
    assert "_cursors" in str(path)
    seen = {f"feed\x1fid{i}": f"hash{i}" for i in range(5)}
    write_text_seen(path, seen, cap=3)
    loaded = read_text_seen(path)
    # Only the 3 most recent (insertion-ordered) entries survive the cap.
    assert list(loaded) == ["feed\x1fid2", "feed\x1fid3", "feed\x1fid4"]
    assert loaded["feed\x1fid4"] == "hash4"

    # Corruption reads as empty, never raises.
    path.write_text("{torn garbage", encoding="utf-8")
    assert read_text_seen(path) == {}
    assert read_text_seen(tmp_path / "absent.json") == {}


# ---------------------------------------------------------------------------
# Reddit
# ---------------------------------------------------------------------------


def test_read_reddit_credentials_missing_and_invalid(tmp_path: Path) -> None:
    missing = tmp_path / "reddit_app.json"
    with pytest.raises(RuntimeError, match="reddit credentials file not found"):
        read_reddit_credentials(missing)
    missing.write_text(json.dumps({"client_id": "abc"}), encoding="utf-8")
    with pytest.raises(RuntimeError, match="client_id and client_secret"):
        read_reddit_credentials(missing)


def test_read_reddit_credentials_ok_with_default_user_agent(tmp_path: Path) -> None:
    path = tmp_path / "reddit_app.json"
    path.write_text(
        json.dumps({"client_id": "cid", "client_secret": "sec"}), encoding="utf-8"
    )
    creds = read_reddit_credentials(path)
    assert creds["client_id"] == "cid"
    assert creds["client_secret"] == "sec"
    # Descriptive default UA; overridable from the file, never a password field.
    assert "crypto-market-data-plant" in creds["user_agent"]
    path.write_text(
        json.dumps({"client_id": "cid", "client_secret": "sec", "user_agent": "custom-ua"}),
        encoding="utf-8",
    )
    assert read_reddit_credentials(path)["user_agent"] == "custom-ua"


def test_reddit_auth_uses_basic_client_credentials_and_caches() -> None:
    token_calls: list[tuple[str, str]] = []

    def fetch_token(basic, user_agent):
        token_calls.append((basic, user_agent))
        return {"access_token": f"tok{len(token_calls)}", "expires_in": 3600}

    clock = {"now": 0.0}
    auth = RedditAppAuth(
        {"client_id": "cid", "client_secret": "sec", "user_agent": "ua-test"},
        fetch_token=fetch_token,
        time_fn=lambda: clock["now"],
    )
    headers = auth.headers()
    assert headers == {"Authorization": "Bearer tok1", "User-Agent": "ua-test"}
    expected_basic = base64.b64encode(b"cid:sec").decode("ascii")
    assert token_calls == [(expected_basic, "ua-test")]
    # Cached within expiry ...
    auth.headers()
    assert len(token_calls) == 1
    # ... refreshed past it.
    clock["now"] = 3600.0
    assert auth.headers()["Authorization"] == "Bearer tok2"
    assert len(token_calls) == 2


def _reddit_listing(children: list[dict]) -> dict:
    return {"data": {"children": [{"kind": "t", "data": child} for child in children]}}


def test_reddit_poll_posts_comments_dedup_and_pacing() -> None:
    state = TextPollState()
    auth = RedditAppAuth(
        {"client_id": "c", "client_secret": "s", "user_agent": "ua-test"},
        fetch_token=lambda basic, ua: {"access_token": "tok", "expires_in": 3600},
    )
    urls: list[str] = []
    headers_seen: list[dict] = []
    sleeps: list[float] = []
    post = {"name": "t3_p1", "title": "Post title", "selftext": "body", "created_utc": 1784160000}
    comment = {"name": "t1_c1", "body": "a comment", "created_utc": 1784160100}

    def fetch_listing(url, headers):
        urls.append(url)
        headers_seen.append(headers)
        # Realistic shape: fullnames are globally unique, /comments only carries
        # t1_* items, and a post lives in exactly one subreddit.
        if "/r/CryptoCurrency/new.json" in url:
            return _reddit_listing([post])
        if "/r/CryptoCurrency/comments.json" in url:
            return _reddit_listing([comment])
        return _reddit_listing([])

    async def fake_sleep(seconds):
        sleeps.append(seconds)

    poll = make_reddit_poll(
        ["CryptoCurrency", "Bitcoin"],
        state,
        auth,
        listing_limit=50,
        request_pause_seconds=1.5,
        fetch_listing=fetch_listing,
        sleep=fake_sleep,
    )
    payloads, more = _run_poll(poll)
    assert more is False
    # 2 subs x 2 listings, but the same fullnames dedup after the first sub.
    assert [u.split("oauth.reddit.com")[1] for u in urls] == [
        "/r/CryptoCurrency/new.json?limit=50&raw_json=1",
        "/r/CryptoCurrency/comments.json?limit=50&raw_json=1",
        "/r/Bitcoin/new.json?limit=50&raw_json=1",
        "/r/Bitcoin/comments.json?limit=50&raw_json=1",
    ]
    assert all(h["Authorization"] == "Bearer tok" and h["User-Agent"] == "ua-test" for h in headers_seen)
    # Paced between consecutive requests (3 pauses for 4 requests), never before the first.
    assert sleeps == [1.5, 1.5, 1.5]
    assert len(payloads) == 2
    post_row = payloads[0]
    assert post_row["kind"] == "reddit_item"
    assert post_row["source_id"] == "t3_p1"
    assert post_row["subreddit"] == "CryptoCurrency"
    assert post_row["listing"] == "new"
    assert post_row["created_utc"] == 1784160000
    assert json.loads(post_row["raw_item"]) == post  # untouched payload round-trips
    assert payloads[1]["source_id"] == "t1_c1"

    # Second sweep, same content -> nothing new.
    payloads, _ = _run_poll(poll)
    assert payloads == []

    # Edit: selftext change re-emits the post as an edit row.
    post["selftext"] = "body v2"
    payloads, _ = _run_poll(poll)
    assert [p["row_type"] for p in payloads] == ["edit"]
    assert payloads[0]["source_id"] == "t3_p1"
    assert state.new_count == 2
    assert state.edit_count == 1


def test_reddit_poll_refreshes_token_on_401() -> None:
    state = TextPollState()
    token_calls = {"n": 0}

    def fetch_token(basic, ua):
        token_calls["n"] += 1
        return {"access_token": f"tok{token_calls['n']}", "expires_in": 3600}

    auth = RedditAppAuth(
        {"client_id": "c", "client_secret": "s", "user_agent": "ua"},
        fetch_token=fetch_token,
    )
    attempts = {"n": 0}

    def fetch_listing(url, headers):
        attempts["n"] += 1
        if headers["Authorization"] == "Bearer tok1":
            raise HTTPError(url, 401, "unauthorized", None, None)
        if "/new.json" in url:
            return _reddit_listing(
                [{"name": "t3_x", "title": "t", "selftext": "", "created_utc": 1.0}]
            )
        return _reddit_listing([])

    async def fake_sleep(seconds):
        return None

    poll = make_reddit_poll(
        ["CryptoCurrency"], state, auth, fetch_listing=fetch_listing, sleep=fake_sleep
    )
    payloads, _ = _run_poll(poll)
    assert token_calls["n"] == 2  # initial token + refresh after the 401
    assert state.error_count == 0
    # Both listings succeeded post-refresh; the post appears once.
    assert [p["source_id"] for p in payloads] == ["t3_x"]


def test_reddit_normalizer_maps_created_utc_to_source_ts() -> None:
    payload = {
        "kind": "reddit_item",
        "row_type": "new",
        "subreddit": "Bitcoin",
        "listing": "comments",
        "source_id": "t1_z",
        "created_utc": 1784160000,
        "content_hash": "h",
        "raw_item": "{}",
        "poll": {"poll_seq": 1},
    }
    event = TextItemNormalizer(source="reddit").normalize(
        RawMessage(source="reddit", received_at=utc_now(), payload=payload)
    )
    assert event.source == "reddit"
    assert event.product == "Bitcoin"
    assert event.channel == "text"
    assert event.source_ts == datetime.fromtimestamp(1784160000, tz=UTC)
    assert event.metadata["listing"] == "comments"
    assert TextQualityGate().validate(event).accepted


def test_rss_normalizer_envelope_and_unparseable_source_ts() -> None:
    received = utc_now()
    pub = format_datetime(datetime(2026, 7, 14, 8, 0, 0, tzinfo=UTC))
    payload = {
        "kind": "rss_item",
        "row_type": "new",
        "feed": "cointelegraph",
        "source_id": "g1",
        "title": "T",
        "link": "L",
        "summary": "S",
        "source_ts_raw": pub,
        "content_hash": "h",
        "raw_item": "<item/>",
        "poll": {"poll_seq": 3, "http_status": 200},
    }
    event = TextItemNormalizer(source="rss").normalize(
        RawMessage(source="rss", received_at=received, payload=payload)
    )
    row = event.to_dict()
    assert row["source"] == "rss"
    assert row["product"] == "cointelegraph"
    assert row["event_type"] == "new"
    assert row["source_id"] == "g1"
    assert row["content_hash"] == "h"
    assert row["raw_item"] == "<item/>"
    assert row["ingestion_ts"] == received.isoformat()
    # received_at mirrors ingestion_ts so event_date partitions on the plant clock.
    assert row["received_at"] == row["ingestion_ts"]
    assert row["source_ts"] == datetime(2026, 7, 14, 8, 0, 0, tzinfo=UTC).isoformat()
    assert row["metadata"]["poll"]["poll_seq"] == 3

    # Unparseable claim: preserved raw, flagged in metadata, still CLEAN.
    bad = dict(payload, source_ts_raw="yesterday-ish")
    event = TextItemNormalizer(source="rss").normalize(
        RawMessage(source="rss", received_at=received, payload=bad)
    )
    assert event.source_ts is None
    assert event.metadata["source_ts_raw"] == "yesterday-ish"
    assert event.metadata["source_ts_unparseable"] is True
    assert TextQualityGate().validate(event).accepted
