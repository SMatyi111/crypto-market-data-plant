from __future__ import annotations

import asyncio
import json
import time
from types import SimpleNamespace

import crypto_collector.cli as cli
from crypto_collector.collectors.binance_futures_rest import (
    make_aggtrades_poll,
    make_depth_poll,
    make_funding_poll,
)
from crypto_collector.collectors.rest_poll import RestPollingCollector
from crypto_collector.market_normalizers import (
    BinanceDepthNormalizer,
    BinanceFuturesFundingNormalizer,
    BinanceTradeNormalizer,
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
    rows = [json.loads(l) for l in (next(run.iterdir()) / "clean" / "events.jsonl").read_text().splitlines()]
    assert rows[0]["metadata"]["instrument_id"] == "perp:binance-futures:BTCUSDT"
    assert [r["sequence"] for r in rows] == [10, 11, 12]  # dense -> gap-proof


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
    rows = [json.loads(l) for l in (next(run.iterdir()) / "clean" / "events.jsonl").read_text().splitlines()]
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
    rows = [json.loads(l) for l in (next(run.iterdir()) / "clean" / "events.jsonl").read_text().splitlines()]
    assert rows[0]["channel"] == "funding"
    assert rows[0]["metadata"]["instrument_id"] == "perp:binance-futures:BTCUSDT"
