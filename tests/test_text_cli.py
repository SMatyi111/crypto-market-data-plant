"""Text-lane CLI wiring tests: every config field survives dispatch into the
segment (the repo's lambda arg-drop regression bar), the ops job types dispatch,
the quiet-lane segment rotation, and an end-to-end segment -> promote pass over
mocked network."""

from __future__ import annotations

import asyncio
import json
import time
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

import crypto_collector.cli as cli
from crypto_collector.cli import _job_args
from crypto_collector.collectors.rest_poll import RestPollingCollector
from crypto_collector.collectors.text_feeds import make_rss_poll
from crypto_collector.models import utc_now
from crypto_collector.ops import COLLECTOR_JOB_TYPES, JobSpec
from crypto_collector.promotion import promote_replayable_runs


def test_text_worker_job_types_are_pool_dispatched() -> None:
    # Pool (subprocess) dispatch, never the scheduler thread: a hung HTTP call in
    # an in-scheduler job blocked the whole plant for 15h on 2026-06-11.
    assert "text-rss-worker" in COLLECTOR_JOB_TYPES
    assert "text-reddit-worker" in COLLECTOR_JOB_TYPES
    # The catch-up scorer is a maintenance job, NOT a pooled lane.
    assert "backfill-text-replay" not in COLLECTOR_JOB_TYPES


def test_job_args_text_rss_worker_threads_lane_flags(tmp_path: Path) -> None:
    job = SimpleNamespace(
        job_type="text-rss-worker",
        args={
            "feeds": {"feedx": "https://feedx/rss", "feedy": "https://feedy/rss"},
            "poll_interval_seconds": 45.0,
            "stale_source_lag_seconds": 7200.0,
            "seen_cap": 1234,
            "segment_count": 777,
            "max_segments": 2,
            "cooldown_seconds": 0.5,
            "output_root": str(tmp_path / "raw_text"),
            "ops_root": str(tmp_path / "ops"),
            "worker_name": "text-rss-worker-x",
            "jsonl_fsync": False,
            "normalized_parquet": True,
            "source_suffix": "alt",
            "rotate_at_midnight": True,
        },
    )
    ns = _job_args(job)
    assert ns.feeds == {"feedx": "https://feedx/rss", "feedy": "https://feedy/rss"}
    assert ns.poll_interval_seconds == 45.0
    assert ns.stale_source_lag_seconds == 7200.0
    assert ns.seen_cap == 1234
    assert ns.segment_count == 777
    assert ns.max_segments == 2
    assert ns.output_root == Path(tmp_path / "raw_text")
    assert ns.worker_name == "text-rss-worker-x"
    assert ns.jsonl_fsync is False
    assert ns.normalized_parquet is True
    assert ns.source_suffix == "alt"
    assert ns.rotate_at_midnight is True
    # Defaults when the config omits the text knobs.
    bare = _job_args(SimpleNamespace(job_type="text-rss-worker", args={}))
    assert bare.feeds is None  # None -> the fixed P1 default set at segment build
    assert bare.poll_interval_seconds == 120.0
    assert bare.normalized_parquet is False  # text default: no hot-path normalized


def test_job_args_text_reddit_worker_threads_lane_flags(tmp_path: Path) -> None:
    job = SimpleNamespace(
        job_type="text-reddit-worker",
        args={
            "subreddits": ["CryptoCurrency", "Bitcoin"],
            "credentials_path": str(tmp_path / "reddit_app.json"),
            "request_pause_seconds": 2.5,
            "listing_limit": 42,
            "poll_interval_seconds": 90.0,
            "stale_source_lag_seconds": 1800.0,
            "seen_cap": 999,
            "segment_count": 555,
            "output_root": str(tmp_path / "raw_text"),
            "ops_root": str(tmp_path / "ops"),
            "worker_name": "text-reddit-worker-x",
        },
    )
    ns = _job_args(job)
    assert ns.subreddits == ["CryptoCurrency", "Bitcoin"]
    assert ns.credentials_path == str(tmp_path / "reddit_app.json")
    assert ns.request_pause_seconds == 2.5
    assert ns.listing_limit == 42
    assert ns.poll_interval_seconds == 90.0
    assert ns.stale_source_lag_seconds == 1800.0
    assert ns.seen_cap == 999
    assert ns.normalized_parquet is False
    bare = _job_args(SimpleNamespace(job_type="text-reddit-worker", args={}))
    assert bare.subreddits is None  # None -> the owner-approved fixed P1 list
    assert bare.credentials_path is None  # None -> <ops_root>/reddit_app.json
    assert bare.poll_interval_seconds == 60.0


def test_job_args_backfill_text_replay(tmp_path: Path) -> None:
    ns = _job_args(
        SimpleNamespace(
            job_type="backfill-text-replay",
            args={
                "source_root": str(tmp_path / "raw_text" / "text_rss"),
                "limit": 111,
                "max_age_hours": 12.0,
                "overwrite": True,
                "min_age_hours": 2.5,
                "stale_source_lag_seconds": 60.0,
            },
        )
    )
    assert ns.source_root == tmp_path / "raw_text" / "text_rss"
    assert ns.limit == 111
    assert ns.max_age_hours == 12.0
    assert ns.overwrite is True
    assert ns.min_age_hours == 2.5
    assert ns.stale_source_lag_seconds == 60.0
    # Default floor protects the live 1800s segments (2x margin).
    bare = _job_args(SimpleNamespace(job_type="backfill-text-replay", args={}))
    assert bare.min_age_hours == 1.0


def test_workers_thread_text_config_through_build_segment_args(tmp_path, monkeypatch) -> None:
    """Regression (the lambda arg-drop trap that shipped twice): every text lane
    config field must reach the segment namespace, including the centrally-threaded
    durability/normalized toggles."""
    captured: dict[str, dict[str, object]] = {}

    def make_fake(key):
        async def fake(segment_args):
            captured[key] = {
                name: getattr(segment_args, name, "MISSING")
                for name in (
                    "feeds",
                    "subreddits",
                    "credentials_path",
                    "request_pause_seconds",
                    "listing_limit",
                    "poll_interval_seconds",
                    "stale_source_lag_seconds",
                    "seen_cap",
                    "source_suffix",
                    "jsonl_fsync",
                    "normalized_parquet",
                )
            }
            return {"run_path": str(tmp_path / key), "clean_events": 0, "replayable": True}

        return fake

    monkeypatch.setattr(cli, "collect_text_rss_segment", make_fake("rss"))
    monkeypatch.setattr(cli, "collect_text_reddit_segment", make_fake("reddit"))

    rss_args = _job_args(
        SimpleNamespace(
            job_type="text-rss-worker",
            args={
                "feeds": {"feedx": "https://feedx/rss"},
                "poll_interval_seconds": 33.0,
                "stale_source_lag_seconds": 111.0,
                "seen_cap": 77,
                "source_suffix": "alt",
                "max_segments": 1,
                "cooldown_seconds": 0.0,
                "heartbeat_interval_seconds": 0.1,
                "worker_name": "text-rss-threading-test",
                "output_root": str(tmp_path),
                "ops_root": str(tmp_path),
            },
        )
    )
    cli.run_text_rss_worker(rss_args)
    assert captured["rss"]["feeds"] == {"feedx": "https://feedx/rss"}
    assert captured["rss"]["poll_interval_seconds"] == 33.0
    assert captured["rss"]["stale_source_lag_seconds"] == 111.0
    assert captured["rss"]["seen_cap"] == 77
    assert captured["rss"]["source_suffix"] == "alt"
    assert captured["rss"]["jsonl_fsync"] is True
    assert captured["rss"]["normalized_parquet"] is False

    reddit_args = _job_args(
        SimpleNamespace(
            job_type="text-reddit-worker",
            args={
                "subreddits": ["CryptoMarkets"],
                "credentials_path": str(tmp_path / "creds.json"),
                "request_pause_seconds": 3.0,
                "listing_limit": 25,
                "poll_interval_seconds": 44.0,
                "max_segments": 1,
                "cooldown_seconds": 0.0,
                "heartbeat_interval_seconds": 0.1,
                "worker_name": "text-reddit-threading-test",
                "output_root": str(tmp_path),
                "ops_root": str(tmp_path),
            },
        )
    )
    cli.run_text_reddit_worker(reddit_args)
    assert captured["reddit"]["subreddits"] == ["CryptoMarkets"]
    assert captured["reddit"]["credentials_path"] == str(tmp_path / "creds.json")
    assert captured["reddit"]["request_pause_seconds"] == 3.0
    assert captured["reddit"]["listing_limit"] == 25
    assert captured["reddit"]["poll_interval_seconds"] == 44.0
    assert captured["reddit"]["normalized_parquet"] is False


def test_execute_ops_job_inprocess_dispatches_text_jobs(monkeypatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(cli, "run_text_rss_worker", lambda args: calls.append("rss"))
    monkeypatch.setattr(cli, "run_text_reddit_worker", lambda args: calls.append("reddit"))
    monkeypatch.setattr(cli, "run_backfill_text_replay", lambda args: calls.append("backfill"))
    for job_type in ("text-rss-worker", "text-reddit-worker", "backfill-text-replay"):
        result = cli._execute_ops_job_inprocess(
            JobSpec(name=f"j-{job_type}", job_type=job_type, interval_seconds=5, args={})
        )
        assert "completed" in str(result)
    assert calls == ["rss", "reddit", "backfill"]


def test_rest_polling_collector_deadline_rotates_a_quiet_stream() -> None:
    """A poll loop that yields nothing must still end at the segment deadline -
    without the collector-side deadline the pipeline's per-frame check never runs
    and a quiet text lane would never rotate."""

    async def quiet_poll():
        return [], False

    collector = RestPollingCollector(
        source="rss",
        poll=quiet_poll,
        poll_interval_seconds=0.01,
        deadline_utc=utc_now() + timedelta(seconds=0.15),
    )

    async def consume():
        frames = []
        async for raw in collector.stream():
            frames.append(raw)
        return frames

    started = time.monotonic()
    assert asyncio.run(consume()) == []
    assert time.monotonic() - started < 5.0


def test_collect_text_reddit_segment_fails_loudly_without_credentials(tmp_path: Path) -> None:
    args = SimpleNamespace(
        subreddits=["Bitcoin"],
        credentials_path=tmp_path / "absent.json",
        output_root=tmp_path / "raw_text",
        source_suffix="",
        count=1,
        deadline_utc=None,
    )
    with pytest.raises(RuntimeError, match="reddit credentials file not found"):
        asyncio.run(cli.collect_text_reddit_segment(args))
    # Fails BEFORE creating a run dir: no empty orphan is minted.
    assert not (tmp_path / "raw_text" / "text_reddit").exists()


def _rss_body_one_item() -> bytes:
    return (
        "<?xml version='1.0'?><rss version='2.0'><channel><item>"
        "<guid>g1</guid><title>Headline</title><link>https://x/1</link>"
        "<description>Body</description>"
        "<pubDate>Tue, 14 Jul 2026 08:00:00 GMT</pubDate>"
        "</item></channel></rss>"
    ).encode("utf-8")


def _inject_rss_fetch(monkeypatch) -> None:
    def fetch(url, etag, last_modified):
        return 200, _rss_body_one_item(), "e1", "lm1"

    def fake_make_rss_poll(feeds, state, **kwargs):
        return make_rss_poll(feeds, state, fetch=fetch)

    monkeypatch.setattr(cli, "make_rss_poll", fake_make_rss_poll)


def _segment_args(tmp_path: Path, **overrides) -> SimpleNamespace:
    ns = SimpleNamespace(
        feeds={"feedx": "https://feedx/rss"},
        poll_interval_seconds=0.02,
        stale_source_lag_seconds=3600.0,
        seen_cap=100,
        count=1,
        output_root=tmp_path / "raw_text",
        source_suffix="",
        deadline_utc=None,
        jsonl_fsync=True,
        normalized_parquet=False,
    )
    for key, value in overrides.items():
        setattr(ns, key, value)
    return ns


def test_text_rss_segment_end_to_end_then_promotes(tmp_path, monkeypatch) -> None:
    _inject_rss_fetch(monkeypatch)

    # Segment 1: one item -> one clean envelope row, replayable, cursor persisted.
    summary = asyncio.run(cli.collect_text_rss_segment(_segment_args(tmp_path)))
    assert summary["clean_events"] == 1
    assert summary["quarantined_events"] == 0
    assert summary["new_items"] == 1
    assert summary["replayable"] is True
    run_dir = Path(str(summary["run_path"]))
    assert run_dir.parent.name == "text_rss"

    raw_lines = (run_dir / "raw" / "messages.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(raw_lines) == 1
    clean_rows = [
        json.loads(line)
        for line in (run_dir / "clean" / "events.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    row = clean_rows[0]
    assert row["source"] == "rss"
    assert row["product"] == "feedx"
    assert row["source_id"] == "g1"
    assert row["event_type"] == "new"
    assert row["content_hash"]
    assert row["ingestion_ts"] == row["received_at"]
    assert row["source_ts"].startswith("2026-07-14T08:00:00")
    assert "<item>" in row["raw_item"]
    replay = json.loads((run_dir / "metrics" / "replay_summary.json").read_text(encoding="utf-8"))
    assert replay["replayable"] is True
    assert replay["gap_detection"] == "none_native"
    assert (tmp_path / "raw_text" / "_cursors" / "text_rss_seen.json").exists()

    # Segment 2, same feed content: the PERSISTED cursor dedups everything, the
    # quiet segment rotates on its deadline and scores no_events/unreplayable.
    time.sleep(1.1)  # run dirs are second-resolution timestamps
    quiet_args = _segment_args(
        tmp_path, count=50, deadline_utc=utc_now() + timedelta(seconds=0.2)
    )
    summary2 = asyncio.run(cli.collect_text_rss_segment(quiet_args))
    assert summary2["clean_events"] == 0
    assert summary2["replayable"] is False
    assert summary2["replay_findings"] == ["no_events"]

    # Promotion: standard promote-replayable job semantics - the replayable run
    # promotes into curated/research/text with the v2 partition layout keyed on the
    # INGESTION date; the quiet run is skipped.
    target_root = tmp_path / "curated" / "text"
    report = promote_replayable_runs(
        source_root=tmp_path / "raw_text" / "text_rss",
        target_root=target_root,
        limit=10,
        max_age_hours=24 * 365 * 10,
    )
    assert report.promoted_run_count == 1
    assert report.promoted_row_count == 1
    today = utc_now().date().isoformat()
    partition_dir = (
        target_root
        / "schema_version=v2"
        / "source=rss"
        / "instrument=feedx"
        / f"event_date={today}"
    )
    part_files = list(partition_dir.glob("part-*.parquet"))
    assert part_files, f"expected curated parquet under {partition_dir}"

    pq = pytest.importorskip("pyarrow.parquet")
    table = pq.read_table(part_files[0])
    curated = table.to_pylist()[0]
    assert curated["source_id"] == "g1"
    assert curated["content_hash"] == row["content_hash"]
    assert curated["raw_item"] == row["raw_item"]
    assert curated["promotion_tag"] == "replayable"

    # Re-running promotion is idempotent (promotion index).
    report = promote_replayable_runs(
        source_root=tmp_path / "raw_text" / "text_rss",
        target_root=target_root,
        limit=10,
        max_age_hours=24 * 365 * 10,
    )
    assert report.promoted_run_count == 0
