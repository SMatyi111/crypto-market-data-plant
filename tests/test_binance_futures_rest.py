from __future__ import annotations

import asyncio
import json
import time
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import crypto_collector.cli as cli
from crypto_collector.collectors.binance_futures_rest import (
    aggtrades_cursor_path,
    aggtrades_resume_from_id,
    make_aggtrades_poll,
    make_depth_poll,
    make_funding_poll,
    max_agg_id_in_events,
    read_aggtrades_cursor,
    write_aggtrades_cursor,
)
from crypto_collector.collectors.rest_poll import RestPollingCollector
from crypto_collector.market_normalizers import (
    BinanceDepthNormalizer,
    BinanceFuturesFundingNormalizer,
)
from crypto_collector.models import RawMessage, utc_now
from crypto_collector.replay import replay_funding_run


def _run(coro):
    return asyncio.run(coro)


async def _drain(collector, limit=None):
    out = []
    async for m in collector.stream(limit=limit):
        out.append(m)
    return out


# --- generic collector -------------------------------------------------------


def test_rest_polling_collector_emits_and_honors_limit() -> None:
    calls = {"n": 0}

    async def poll():
        calls["n"] += 1
        return [{"a": calls["n"]}], False

    c = RestPollingCollector(source="x", poll=poll, poll_interval_seconds=0)
    msgs = _run(_drain(c, limit=3))
    assert len(msgs) == 3
    assert all(isinstance(m, RawMessage) and m.source == "x" for m in msgs)
    assert [m.payload["a"] for m in msgs] == [1, 2, 3]


def test_rest_polling_collector_catch_up_skips_sleep(monkeypatch) -> None:
    # more_pending=True must re-poll immediately (no sleep) so the gapless pager catches up.
    sleeps = []

    async def fake_sleep(s):
        sleeps.append(s)

    monkeypatch.setattr("crypto_collector.collectors.rest_poll.asyncio.sleep", fake_sleep)
    seq = iter([([{"a": 1}], True), ([{"a": 2}], False)])

    async def poll():
        return next(seq)

    c = RestPollingCollector(source="x", poll=poll, poll_interval_seconds=5)
    msgs = _run(_drain(c, limit=2))
    assert [m.payload["a"] for m in msgs] == [1, 2]
    # first poll had more_pending=True -> no sleep; limit hit on 2nd poll's item before any sleep.
    assert sleeps == []


# --- pollers -----------------------------------------------------------------


def test_aggtrades_poll_is_gapless_and_injects_symbol_and_event() -> None:
    pages = [
        [{"a": 100, "p": "1", "q": "1", "T": 1, "m": True},
         {"a": 101, "p": "1", "q": "1", "T": 2, "m": False}],  # full page (limit=2) -> keep paging
        [{"a": 102, "p": "1", "q": "1", "T": 3, "m": True}],    # partial -> stop
    ]
    seq = iter(pages)

    def fetch(path, params):
        assert path == "/fapi/v1/aggTrades"
        assert params["symbol"] == "BTCUSDT"
        return next(seq, [])

    poll = make_aggtrades_poll("btcusdt", page_limit=2, max_pages_per_poll=5, fetch=fetch)
    rows, more = _run(poll())
    assert [r["a"] for r in rows] == [100, 101, 102]  # contiguous dense ids -> gapless
    assert all(r["s"] == "BTCUSDT" and r["e"] == "aggTrade" for r in rows)
    assert more is False
    # next poll advances fromId past the end -> empty
    rows2, more2 = _run(poll())
    assert rows2 == [] and more2 is False


def test_aggtrades_poll_signals_more_pending_when_page_full() -> None:
    def fetch(path, params):
        # always return a full page of one item (limit=1) -> still catching up
        base = params.get("fromId", 1)
        return [{"a": base, "p": "1", "q": "1", "T": 1, "m": True}]

    poll = make_aggtrades_poll("btcusdt", page_limit=1, max_pages_per_poll=1, fetch=fetch)
    rows, more = _run(poll())
    assert len(rows) == 1 and more is True


def test_aggtrades_poll_seeds_from_initial_from_id() -> None:
    # A seeded pager resumes from the given id on its FIRST request, instead of anchoring
    # to the most-recent page (which is what reset every rotation and caused gaps/dups).
    seen_from_ids = []

    def fetch(path, params):
        seen_from_ids.append(params.get("fromId"))
        return []  # empty -> stop immediately

    poll = make_aggtrades_poll("btcusdt", page_limit=2, initial_from_id=500, fetch=fetch)
    _run(poll())
    assert seen_from_ids == [500]


# --- aggTrades cross-segment cursor ------------------------------------------


def test_aggtrades_resume_from_id_fresh_cursor_seeds_next_id() -> None:
    now = datetime(2026, 6, 9, 12, 0, tzinfo=UTC)
    cursor = {"symbol": "BTCUSDT", "last_agg_id": 12,
              "updated_at": (now - timedelta(seconds=30)).isoformat()}
    from_id, finding = aggtrades_resume_from_id(
        cursor, symbol="BTCUSDT", now=now, max_resume_gap_seconds=3600
    )
    assert from_id == 13 and finding is None


def test_aggtrades_resume_from_id_no_cursor_anchors_to_live() -> None:
    now = datetime(2026, 6, 9, 12, 0, tzinfo=UTC)
    assert aggtrades_resume_from_id(
        None, symbol="BTCUSDT", now=now, max_resume_gap_seconds=3600
    ) == (None, None)


def test_aggtrades_resume_from_id_stale_cursor_reanchors_with_logged_gap() -> None:
    now = datetime(2026, 6, 9, 12, 0, tzinfo=UTC)
    cursor = {"symbol": "BTCUSDT", "last_agg_id": 12,
              "updated_at": (now - timedelta(hours=9)).isoformat()}
    from_id, finding = aggtrades_resume_from_id(
        cursor, symbol="BTCUSDT", now=now, max_resume_gap_seconds=21_600
    )
    assert from_id is None and finding == "cursor_reset_stale_gap"


def test_aggtrades_resume_from_id_symbol_mismatch_reanchors() -> None:
    now = datetime(2026, 6, 9, 12, 0, tzinfo=UTC)
    cursor = {"symbol": "ETHUSDT", "last_agg_id": 5, "updated_at": now.isoformat()}
    assert aggtrades_resume_from_id(
        cursor, symbol="BTCUSDT", now=now, max_resume_gap_seconds=3600
    ) == (None, "cursor_reset_symbol_mismatch")


def test_aggtrades_cursor_roundtrip_and_corrupt_is_treated_as_absent(tmp_path) -> None:
    path = aggtrades_cursor_path(tmp_path, "binance_perp_trades")
    write_aggtrades_cursor(path, symbol="btcusdt", last_agg_id=99)
    cursor = read_aggtrades_cursor(path)
    assert cursor["symbol"] == "BTCUSDT" and cursor["last_agg_id"] == 99
    # A torn/corrupt cursor must read as None (re-anchor), never raise.
    path.write_text("{not valid json", encoding="utf-8")
    assert read_aggtrades_cursor(path) is None


def test_max_agg_id_in_events_returns_highest_sequence(tmp_path) -> None:
    events = tmp_path / "events.jsonl"
    events.write_text(
        "\n".join(json.dumps({"sequence": s}) for s in (10, 11, 12)) + "\n",
        encoding="utf-8",
    )
    assert max_agg_id_in_events(events) == 12
    assert max_agg_id_in_events(tmp_path / "missing.jsonl") is None


def test_depth_poll_remaps_snapshot_to_binance_depth_shape() -> None:
    def fetch(path, params):
        assert path == "/fapi/v1/depth"
        return {"lastUpdateId": 555, "E": 1000, "T": 1001, "bids": [["10", "1"]], "asks": [["11", "2"]]}

    poll = make_depth_poll("btcusdt", limit=5, fetch=fetch)
    rows, more = _run(poll())
    assert more is False and len(rows) == 1
    p = rows[0]
    assert p["s"] == "BTCUSDT" and p["e"] == "snapshot" and p["u"] == 555 and p["E"] == 1000
    assert p["b"] == [["10", "1"]] and p["a"] == [["11", "2"]]


def test_funding_poll_passes_premium_index_through() -> None:
    def fetch(path, params):
        assert path == "/fapi/v1/premiumIndex"
        return {"symbol": "BTCUSDT", "markPrice": "61000", "lastFundingRate": "0.0001", "time": 1234}

    poll = make_funding_poll("btcusdt", fetch=fetch)
    rows, more = _run(poll())
    assert more is False and rows[0]["markPrice"] == "61000"


# --- normalizers -------------------------------------------------------------


def test_binance_depth_normalizer_perp_tags_futures_on_rest_snapshot() -> None:
    raw = RawMessage(
        source="binance-futures",
        received_at=utc_now(),
        payload={"s": "BTCUSDT", "e": "snapshot", "E": 1000, "u": 555,
                 "b": [["10", "1"]], "a": [["11", "2"]]},
    )
    ev = BinanceDepthNormalizer(instrument_type="perp").normalize(raw)
    assert ev.event_type == "snapshot"
    assert ev.instrument.instrument_id == "perp:binance-futures:BTCUSDT"
    assert ev.bids == [[10.0, 1.0]] and ev.asks == [[11.0, 2.0]]


def test_binance_futures_funding_normalizer() -> None:
    raw = RawMessage(
        source="binance-futures",
        received_at=utc_now(),
        payload={"symbol": "BTCUSDT", "markPrice": "61000.5", "indexPrice": "61010",
                 "lastFundingRate": "0.00000568", "interestRate": "0.0001",
                 "estimatedSettlePrice": "61500", "nextFundingTime": 1781000000000,
                 "time": 1781018000000},
    )
    ev = BinanceFuturesFundingNormalizer().normalize(raw)
    assert ev.channel == "funding" and ev.event_type == "funding"
    assert ev.price == 61000.5
    assert ev.metadata["instrument_id"] == "perp:binance-futures:BTCUSDT"
    assert ev.metadata["canonical_symbol"] == "BTC/USDT-PERP"
    assert ev.metadata["mark_price"] == 61000.5
    assert ev.metadata["funding_rate"] == 0.00000568


# --- funding replay ----------------------------------------------------------


def _write_clean_run(tmp_path, rows):
    run = tmp_path / "run"
    (run / "clean").mkdir(parents=True)
    with (run / "clean" / "events.jsonl").open("w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r) + "\n")
    return run


def test_replay_funding_run_replayable_and_flags_missing_mark(tmp_path) -> None:
    now = utc_now()
    good = [
        BinanceFuturesFundingNormalizer().normalize(
            RawMessage(source="binance-futures", received_at=now,
                       payload={"symbol": "BTCUSDT", "markPrice": "61000",
                                "time": int(now.timestamp() * 1000)})
        ).to_dict()
    ]
    summary = replay_funding_run(_write_clean_run(tmp_path, good), write_summary=True)
    assert summary.replayable is True
    assert summary.gap_detection == "none_native"
    assert summary.event_count == 1

    bad_run = tmp_path / "bad"
    (bad_run / "clean").mkdir(parents=True)
    row = good[0].copy()
    row["price"] = None  # no mark price
    (bad_run / "clean" / "events.jsonl").write_text(json.dumps(row) + "\n", encoding="utf-8")
    bad = replay_funding_run(bad_run, write_summary=True)
    assert bad.replayable is False and "invalid_mark_price" in bad.findings


# --- end-to-end segment (stubbed pollers, no network, no parquet) ------------


def _segment_args(tmp_path, stream):
    return SimpleNamespace(
        symbol="BTCUSDT", stream=stream, poll_interval_seconds=0.0,
        page_limit=1000, depth=1000, count=3,
        output_root=tmp_path, source_suffix="",
        max_delay_ms=60_000, max_future_skew_ms=5_000, max_clock_skew_ms=60_000.0,
        jsonl_fsync=True, normalized_parquet=False, deadline_utc=None,
    )


def test_collect_segment_trades_gapless_perp(tmp_path, monkeypatch) -> None:
    now_ms = int(time.time() * 1000)
    batch = [{"a": 10 + i, "p": "61000.0", "q": "0.01", "T": now_ms + i, "m": i % 2 == 0,
              "s": "BTCUSDT", "e": "aggTrade"} for i in range(3)]

    def fake_factory(symbol, **kw):
        async def poll():
            return list(batch), False
        return poll

    monkeypatch.setattr(cli, "make_aggtrades_poll", fake_factory)
    summary = _run(cli.collect_binance_futures_rest_segment(_segment_args(tmp_path, "trades")))
    assert summary["clean_events"] == 3
    assert summary["replayable"] is True
    run = tmp_path / "binance_perp_trades"
    rows = [json.loads(line) for line in (next(run.iterdir()) / "clean" / "events.jsonl").read_text().splitlines()]
    assert rows[0]["metadata"]["instrument_id"] == "perp:binance-futures:BTCUSDT"
    assert [r["sequence"] for r in rows] == [10, 11, 12]  # dense -> gap-proof


def test_collect_segment_trades_resumes_across_rotations_no_gap_no_dup(tmp_path, monkeypatch) -> None:
    # Two consecutive segments (= two subprocesses in prod). Segment 1 captures a=10,11,12
    # and persists a cursor; segment 2 MUST resume from a=13 via that cursor instead of
    # re-anchoring to "now". Proves rotations are gapless (no missed ids) and overlap-free
    # (no duplicate ids) — the bug this fix targets.
    now_ms = int(time.time() * 1000)
    seeded_from_ids = []

    def factory_for(page):
        def fake_factory(symbol, **kw):
            seeded_from_ids.append(kw.get("initial_from_id"))

            async def poll():
                return list(page), False

            return poll

        return fake_factory

    # Force distinct, ordered run dirs (prepare_run_paths is 1s-resolution; both segments
    # run within the same wall-clock second in a test).
    real_prepare = cli.prepare_run_paths
    ts_clock = {"n": 0}

    def prepare_with_distinct_ts(output_root, source, started_at=None):
        ts_clock["n"] += 1
        return real_prepare(
            output_root, source, started_at=datetime(2026, 6, 9, 12, 0, ts_clock["n"], tzinfo=UTC)
        )

    monkeypatch.setattr(cli, "prepare_run_paths", prepare_with_distinct_ts)

    seg1 = [{"a": 10 + i, "p": "61000.0", "q": "0.01", "T": now_ms + i, "m": i % 2 == 0,
             "s": "BTCUSDT", "e": "aggTrade"} for i in range(3)]   # a=10,11,12
    monkeypatch.setattr(cli, "make_aggtrades_poll", factory_for(seg1))
    s1 = _run(cli.collect_binance_futures_rest_segment(_segment_args(tmp_path, "trades")))
    assert s1["clean_events"] == 3

    cursor = read_aggtrades_cursor(aggtrades_cursor_path(tmp_path, "binance_perp_trades"))
    assert cursor["last_agg_id"] == 12  # highest durably-written id persisted

    seg2 = [{"a": 13 + i, "p": "61000.0", "q": "0.01", "T": now_ms + 100 + i, "m": i % 2 == 0,
             "s": "BTCUSDT", "e": "aggTrade"} for i in range(2)]   # a=13,14
    monkeypatch.setattr(cli, "make_aggtrades_poll", factory_for(seg2))
    s2 = _run(cli.collect_binance_futures_rest_segment(_segment_args(tmp_path, "trades")))
    assert s2["clean_events"] == 2

    # Segment 1 anchored to live (None); segment 2 resumed strictly from 12+1.
    assert seeded_from_ids == [None, 13]

    # Union of both runs' clean events is contiguous 10..14 — no gap, no repeat.
    run_root = tmp_path / "binance_perp_trades"
    seqs = []
    for run_dir in sorted(d for d in run_root.iterdir() if d.is_dir()):
        ev = run_dir / "clean" / "events.jsonl"
        seqs += [json.loads(line)["sequence"] for line in ev.read_text().splitlines() if line.strip()]
    assert seqs == [10, 11, 12, 13, 14]


def test_collect_segment_depth_snapshot_perp(tmp_path, monkeypatch) -> None:
    now_ms = int(time.time() * 1000)

    def fake_factory(symbol, **kw):
        async def poll():
            return [{"s": "BTCUSDT", "e": "snapshot", "E": now_ms, "u": 1,
                     "b": [["61000", "1.0"]], "a": [["61001", "2.0"]]}], False
        return poll

    monkeypatch.setattr(cli, "make_depth_poll", fake_factory)
    summary = _run(cli.collect_binance_futures_rest_segment(_segment_args(tmp_path, "depth")))
    assert summary["clean_events"] == 3 and summary["replayable"] is True
    run = tmp_path / "binance_perp_depth"
    rows = [json.loads(line) for line in (next(run.iterdir()) / "clean" / "events.jsonl").read_text().splitlines()]
    assert rows[0]["instrument"]["instrument_id"] == "perp:binance-futures:BTCUSDT"


def test_collect_segment_funding_perp(tmp_path, monkeypatch) -> None:
    now_ms = int(time.time() * 1000)

    def fake_factory(symbol, **kw):
        async def poll():
            return [{"symbol": "BTCUSDT", "markPrice": "61000.5", "indexPrice": "61010",
                     "lastFundingRate": "0.00001", "time": now_ms}], False
        return poll

    monkeypatch.setattr(cli, "make_funding_poll", fake_factory)
    summary = _run(cli.collect_binance_futures_rest_segment(_segment_args(tmp_path, "funding")))
    assert summary["clean_events"] == 3 and summary["replayable"] is True
    run = tmp_path / "binance_perp_funding"
    rows = [json.loads(line) for line in (next(run.iterdir()) / "clean" / "events.jsonl").read_text().splitlines()]
    assert rows[0]["channel"] == "funding"
    assert rows[0]["metadata"]["instrument_id"] == "perp:binance-futures:BTCUSDT"


def test_read_cursor_tolerates_non_utf8_garbage(tmp_path):
    """A torn write / disk garbage can leave non-UTF-8 bytes in the cursor file.
    read_aggtrades_cursor documents a never-raises contract — a UnicodeDecodeError
    here crash-looped the lane on every segment start."""
    from crypto_collector.collectors.binance_futures_rest import read_aggtrades_cursor

    path = tmp_path / "binance_perp_trades.json"
    path.write_bytes(b"\xff\xfe\x00garbage\xff")
    assert read_aggtrades_cursor(path) is None


def test_job_args_threads_max_resume_gap_and_resume_friendly_stale_windows():
    """Regression for the per-job-type enumeration trap: a config-set
    max_resume_gap_seconds must reach the worker, and the default staleness windows
    must cover the resume gap — a 60s gate quarantined every cursor-resumed backfill
    after an outage (then advanced the cursor past it: silent loss on a gap-proof lane)."""
    from crypto_collector.cli import _job_args
    from crypto_collector.ops import JobSpec

    defaults = _job_args(
        JobSpec(name="x", job_type="binance-futures-rest-worker", interval_seconds=5, args={})
    )
    assert defaults.max_resume_gap_seconds == 21_600.0
    # Windows must be >= the resume gap, or resumed backfill is quarantined/blocked.
    assert defaults.max_delay_ms >= defaults.max_resume_gap_seconds * 1000
    assert defaults.max_clock_skew_ms >= defaults.max_resume_gap_seconds * 1000

    overridden = _job_args(
        JobSpec(
            name="x",
            job_type="binance-futures-rest-worker",
            interval_seconds=5,
            args={"max_resume_gap_seconds": 120.0},
        )
    )
    assert overridden.max_resume_gap_seconds == 120.0


def _write_run_with_sequences(root, lane: str, name: str, sequences: list[int]) -> None:
    run_dir = root / lane / name
    (run_dir / "clean").mkdir(parents=True)
    (run_dir / "clean" / "events.jsonl").write_text(
        "".join(json.dumps({"sequence": seq}) + "\n" for seq in sequences),
        encoding="utf-8",
    )


def test_max_agg_id_in_recent_runs_finds_durable_high_water(tmp_path) -> None:
    """Regression (crash-resume duplicates): the cursor only advances on a clean
    segment end, so after a hard kill the durable high-water on disk runs AHEAD of
    the cursor. The scan must surface it so the resume floor skips the
    already-written range instead of re-fetching it into curated twice."""
    from crypto_collector.collectors.binance_futures_rest import max_agg_id_in_recent_runs

    now = datetime(2026, 6, 12, 12, 0, 0, tzinfo=UTC)
    # An older completed run and a newer crashed run (no cursor advance for it).
    _write_run_with_sequences(tmp_path, "binance_perp_trades", "20260612_110000", [100, 150])
    _write_run_with_sequences(tmp_path, "binance_perp_trades", "20260612_113000", [151, 220])

    high = max_agg_id_in_recent_runs(
        tmp_path, "binance_perp_trades", now=now, max_age_seconds=21_600.0
    )
    assert high == 220

    # No lane dir at all -> None (first-ever segment).
    assert (
        max_agg_id_in_recent_runs(tmp_path, "nope", now=now, max_age_seconds=21_600.0) is None
    )


def test_max_agg_id_in_recent_runs_ignores_runs_older_than_the_resume_gap(tmp_path) -> None:
    """An extended outage must still re-anchor to live (bounded fapi weight), exactly
    like the stale-cursor rule: run dirs older than max_age_seconds don't count."""
    from crypto_collector.collectors.binance_futures_rest import max_agg_id_in_recent_runs

    now = datetime(2026, 6, 12, 12, 0, 0, tzinfo=UTC)
    _write_run_with_sequences(tmp_path, "binance_perp_trades", "20260610_110000", [100, 150])

    assert (
        max_agg_id_in_recent_runs(
            tmp_path, "binance_perp_trades", now=now, max_age_seconds=21_600.0
        )
        is None
    )
    # A wider window picks the same run up again.
    assert (
        max_agg_id_in_recent_runs(
            tmp_path, "binance_perp_trades", now=now, max_age_seconds=7 * 86_400.0
        )
        == 150
    )
