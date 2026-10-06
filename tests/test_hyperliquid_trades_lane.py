"""Hyperliquid public WS trades lane (STANDARDS 4.15): normalizer, subscribe
snapshot tagging, live-gate behavior and ops dispatch.

What this guards:

1. `users` is the reason the lane exists (wallet-attributed prints after the S3
   fill archive stopped). A print whose `users` pair is missing or malformed must
   fail the live gate, so every clean row is attributable.
2. Hyperliquid answers each subscribe with its recent prints in an UNFLAGGED
   frame. Untagged, those prints duplicate the previous segment's tail in curated
   (promotion has no cross-run dedup). The collector tags the first data frame of
   every connection and the gate quarantines it.
3. The public tape must not share a curated partition with the frozen-cohort
   wallet-fill lane (`instrument=BTC`), so `product` is the venue symbol BTCUSDC.
4. Per-lane config fields must survive the ops-runner path (the enumeration trap).
"""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import sys
from pathlib import Path
from types import SimpleNamespace

from crypto_collector.collectors.generic_ws import GenericWebsocketCollector
from crypto_collector.config import CollectorConfig
from crypto_collector.market_normalizers import (
    HYPERLIQUID_SUBSCRIBE_SNAPSHOT_KEY,
    HyperliquidTradeNormalizer,
)
from crypto_collector.models import RawMessage
from crypto_collector.ops import COLLECTOR_JOB_TYPES, JobSpec
from crypto_collector.quality import QualityGate

BUYER = "0x2cffce91b4e0c81df18726ff66b31b2b1545e1ad"
SELLER = "0x6BB971430554E3AF58FBD469BCE46AB2359A2D23"
RECEIVED = dt.datetime(2026, 10, 6, 13, 0, 1, tzinfo=dt.timezone.utc)
TRADE_MS = 1791291600500  # 2026-10-06T13:00:00.500Z


def _print(**overrides):
    item = {
        "coin": "BTC",
        "side": "A",
        "px": "86093.0",
        "sz": "0.00347",
        "time": TRADE_MS,
        "hash": "0xa114c41c6b550ddba28e0445f7dbac0201f9000206582cad44dd6f6f2a58e7c6",
        "tid": 1021093340028777,
        "users": [BUYER, SELLER],
    }
    item.update(overrides)
    return item


def _frame(*items, snapshot=False):
    payload = {"channel": "trades", "data": list(items) or [_print()]}
    if snapshot:
        payload[HYPERLIQUID_SUBSCRIBE_SNAPSHOT_KEY] = True
    return RawMessage(source="hyperliquid", received_at=RECEIVED, payload=payload)


def _gate():
    return QualityGate(max_delay_ms=900_000, max_future_skew_ms=5_000, session_id="t")


# --- normalizer -------------------------------------------------------------


def test_print_fields_and_wallet_pair():
    ev = HyperliquidTradeNormalizer().normalize_many(_frame())[0]
    assert ev.source == "hyperliquid"
    assert ev.channel == "trades" and ev.event_type == "trade"
    assert ev.raw_type == "ws_trades"  # wallet-flow fills say userFillsByTime
    assert ev.price == 86093.0 and ev.size == 0.00347
    assert ev.exchange_time == dt.datetime(2026, 10, 6, 13, 0, 0, 500000, tzinfo=dt.timezone.utc)
    assert ev.trade_id == "1021093340028777"
    # tid is unique but not dense -> no sequence proof (none_native).
    assert ev.sequence is None
    md = ev.metadata
    assert md["users"] == [BUYER, SELLER.lower()]
    assert md["buyer"] == BUYER and md["seller"] == SELLER.lower()
    assert md["instrument_id"] == "perp:hyperliquid:BTCUSDC"
    assert md["canonical_symbol"] == "BTC/USDC-PERP"
    assert md["hyperliquid_coin"] == "BTC"
    assert md["transaction_hash"].startswith("0xa114")
    assert "subscribe_replay" not in md and "parse_errors" not in md


def test_product_is_venue_symbol_not_bare_coin():
    """instrument=BTC is the frozen-cohort fill lane's partition; the full public
    tape must land in its own partition (BTCUSDC), never mixed with a subset."""
    for coin in ("BTC", "ETH", "SOL"):
        ev = HyperliquidTradeNormalizer().normalize_many(_frame(_print(coin=coin)))[0]
        assert ev.product == f"{coin}USDC"


def test_side_is_taker_side_and_buyer_is_maker_derived():
    sell, buy = HyperliquidTradeNormalizer().normalize_many(
        _frame(_print(side="A"), _print(side="B", tid=2))
    )
    assert sell.side == "sell" and sell.metadata["buyer_is_maker"] is True
    assert buy.side == "buy" and buy.metadata["buyer_is_maker"] is False
    # users order is [buyer, seller] regardless of side.
    assert buy.metadata["buyer"] == BUYER


def test_batched_frame_fans_out_and_non_list_is_ignored():
    raw = _frame(_print(), _print(tid=7), _print(tid=8))
    assert len(HyperliquidTradeNormalizer().normalize_many(raw)) == 3
    raw.payload["data"] = None
    assert HyperliquidTradeNormalizer().normalize_many(raw) == []


def test_missing_or_malformed_users_fails_the_live_gate():
    normalizer = HyperliquidTradeNormalizer()
    gate = _gate()
    for users in (None, [], [BUYER], [BUYER, "not-an-address"], [BUYER, SELLER, BUYER], "x"):
        item = _print()
        if users is None:
            item.pop("users")
        else:
            item["users"] = users
        ev = normalizer.normalize_many(_frame(item))[0]
        assert "invalid_users" in ev.metadata["parse_errors"]
        assert "users" not in ev.metadata
        result = gate.validate(ev)
        assert not result.accepted and "invalid_users" in result.reasons


def test_bad_side_and_missing_time_are_parse_errors():
    ev = HyperliquidTradeNormalizer().normalize_many(_frame(_print(side="X")))[0]
    assert ev.side is None and "invalid_side" in ev.metadata["parse_errors"]
    item = _print()
    item.pop("time")
    ev = HyperliquidTradeNormalizer().normalize_many(_frame(item))[0]
    assert ev.exchange_time is None and "invalid_event_time" in ev.metadata["parse_errors"]


def test_clean_print_passes_gate_and_snapshot_print_is_quarantined():
    normalizer = HyperliquidTradeNormalizer()
    gate = _gate()
    live = normalizer.normalize_many(_frame())[0]
    assert gate.validate(live).accepted
    replay = normalizer.normalize_many(_frame(snapshot=True))[0]
    assert replay.metadata["subscribe_replay"] is True
    result = gate.validate(replay)
    assert not result.accepted and result.reasons == ["subscribe_replay"]


# --- collector: subscription protocol + snapshot tagging --------------------


class _ScriptedHyperliquidSocket:
    """recv() serves the subscribe handshake; the async iterator serves data."""

    def __init__(self, data_frames):
        self._data = [json.dumps(f) if not isinstance(f, str) else f for f in data_frames]
        self.sent: list[str] = []
        self._acked = False

    async def send(self, message):
        self.sent.append(message)

    async def recv(self):
        if not self._acked:
            self._acked = True
            sub = json.loads(self.sent[0])["subscription"]
            return json.dumps(
                {"channel": "subscriptionResponse", "data": {"method": "subscribe", "subscription": sub}}
            )
        await asyncio.sleep(3600)
        raise RuntimeError("unreachable")

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._data:
            raise StopAsyncIteration
        return self._data.pop(0)


class _Conn:
    def __init__(self, ws):
        self._ws = ws

    async def __aenter__(self):
        return self._ws

    async def __aexit__(self, *exc):
        return None


def _collector():
    return GenericWebsocketCollector(
        CollectorConfig(
            source="hyperliquid",
            output_root=Path("data"),
            product="BTC",
            channel="trades",
            websocket_url="wss://api.hyperliquid.xyz/ws",
            subscription_style="hyperliquid",
        )
    )


def _trades(tid):
    return {"channel": "trades", "data": [_print(tid=tid)]}


def test_subscription_message_ack_and_error_shapes():
    c = _collector()
    assert c._subscription_message() == {
        "method": "subscribe",
        "subscription": {"type": "trades", "coin": "BTC"},
    }
    assert c._is_subscription_ack({"channel": "subscriptionResponse", "data": {}})
    assert not c._is_subscription_ack(_trades(1))
    assert c._is_subscription_error({"channel": "error", "data": "Invalid subscription"})
    assert c._should_emit(_trades(1))
    assert not c._should_emit({"channel": "pong"})
    assert not c._should_emit({"channel": "subscriptionResponse", "data": {}})
    assert not c._should_emit({"channel": "trades", "data": "oops"})


def test_first_frame_of_every_connection_is_tagged(monkeypatch):
    """Segment start AND a mid-segment reconnect each re-deliver recent prints."""
    sockets = [
        _ScriptedHyperliquidSocket([{"channel": "pong"}, _trades(1), _trades(2)]),
        _ScriptedHyperliquidSocket([_trades(3), _trades(4)]),
    ]
    monkeypatch.setitem(
        sys.modules, "websockets", SimpleNamespace(connect=lambda url, **kw: _Conn(sockets.pop(0)))
    )

    async def drive():
        return [raw async for raw in _collector().stream(limit=4)]

    emitted = asyncio.run(drive())
    tags = [raw.payload.get(HYPERLIQUID_SUBSCRIBE_SNAPSHOT_KEY) for raw in emitted]
    assert [raw.payload["data"][0]["tid"] for raw in emitted] == [1, 2, 3, 4]
    assert tags == [True, None, True, None]


def test_other_venues_never_get_the_snapshot_tag(monkeypatch):
    class _OkxSocket(_ScriptedHyperliquidSocket):
        async def recv(self):
            if not self._acked:
                self._acked = True
                return json.dumps({"event": "subscribe", "arg": {}})
            await asyncio.sleep(3600)

    ws = _OkxSocket([{"arg": {"channel": "trades"}, "data": [{}]}])
    monkeypatch.setitem(sys.modules, "websockets", SimpleNamespace(connect=lambda url, **kw: _Conn(ws)))
    collector = GenericWebsocketCollector(
        CollectorConfig(
            source="okx",
            output_root=Path("data"),
            product="BTC-USDT",
            channel="trades",
            websocket_url="wss://example.test",
            subscription_style="okx",
        )
    )

    async def drive():
        return [raw async for raw in collector.stream(limit=1)]

    (raw,) = asyncio.run(drive())
    assert HYPERLIQUID_SUBSCRIBE_SNAPSHOT_KEY not in raw.payload


# --- ops dispatch -------------------------------------------------------------


def test_job_type_registered_as_collector_lane():
    assert "hyperliquid-trades-worker" in COLLECTOR_JOB_TYPES


def test_job_args_survive_the_ops_runner_path():
    from crypto_collector.cli import _job_args

    job = JobSpec(
        name="hyperliquid-btc-trades",
        job_type="hyperliquid-trades-worker",
        interval_seconds=5,
        args={
            "symbol": "ETH",
            "source_suffix": "eth",
            "idle_timeout_seconds": 120,
            "worker_name": "hyperliquid-trades-worker-eth",
            "normalized_parquet": False,
        },
        enabled=True,
    )
    ns = _job_args(job)
    assert ns.symbol == "ETH" and ns.channel == "trades"
    assert ns.source_suffix == "eth"
    assert ns.idle_timeout_seconds == 120
    assert ns.worker_name == "hyperliquid-trades-worker-eth"
    assert ns.jsonl_fsync is True
    # 15-min stale window like every other trades lane, not argparse's 60 s.
    assert ns.max_delay_ms == 900_000


def test_cli_parser_defaults():
    from crypto_collector.cli import build_parser

    ns = build_parser().parse_args(["hyperliquid-trades-worker"])
    assert ns.symbol == "BTC" and ns.channel == "trades" and ns.source_suffix == ""


def test_segment_writes_lane_dir_and_stream_verdict(monkeypatch, tmp_path):
    """End to end through _collect_trades_segment with a scripted socket: the run
    lands in hyperliquid_perp_trades_<suffix>/, the snapshot print is quarantined,
    live prints are clean with users, and the verdict is none_native."""
    from crypto_collector import cli

    t0 = TRADE_MS
    snapshot = {"channel": "trades", "data": [_print(tid=1, time=t0)]}
    live = [{"channel": "trades", "data": [_print(tid=10 + i, time=t0 + 1000 * (i + 1))]} for i in range(3)]
    ws = _ScriptedHyperliquidSocket([snapshot, *live])
    monkeypatch.setitem(sys.modules, "websockets", SimpleNamespace(connect=lambda url, **kw: _Conn(ws)))
    # Make receipt time follow the scripted exchange times so the stale gate passes.
    monkeypatch.setattr(
        "crypto_collector.collectors.generic_ws.utc_now",
        lambda: dt.datetime.fromtimestamp((t0 + 5000) / 1000, tz=dt.timezone.utc),
    )
    args = SimpleNamespace(
        symbol="btc",
        channel="trades",
        count=4,
        output_root=tmp_path,
        max_delay_ms=900_000,
        max_future_skew_ms=5_000,
        max_clock_skew_ms=60_000.0,
        source_suffix="btc",
        deadline_utc=None,
        normalized_parquet=False,
    )
    summary = asyncio.run(cli.collect_hyperliquid_trades_segment(args))
    run = Path(summary["run_path"])
    assert run.parent.name == "hyperliquid_perp_trades_btc"
    assert summary["clean_events"] == 3 and summary["quarantined_events"] == 1
    assert json.loads(ws.sent[0]) == {"method": "subscribe", "subscription": {"type": "trades", "coin": "BTC"}}
    clean = [json.loads(line) for line in (run / "clean" / "events.jsonl").read_text().splitlines()]
    assert all(row["metadata"]["users"] for row in clean)
    quarantined = json.loads((run / "quarantine" / "events.jsonl").read_text().splitlines()[0])
    assert "subscribe_replay" in json.dumps(quarantined)
    verdict = json.loads((run / "metrics" / "replay_summary.json").read_text())
    assert verdict["replayable"] is True and verdict["gap_detection"] == "none_native"
