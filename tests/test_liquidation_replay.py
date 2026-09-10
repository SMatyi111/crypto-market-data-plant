"""STANDARDS 4.10 (v12): the liquidations-channel replay verdict.

The first OKX all-swap day-runs (2026-09-08/09) scored `replayable: false` under
the trades-stream verdict for reasons that are the venue's, not the capture's:
global exchange-time order across 340 interleaved products, and OKX's own delayed,
batched delivery of `liquidation-orders` details (rows up to ~15 min late, per-
product backward steps). `replay_liquidations_run` gates on what the capture can
vouch for (shape, finite prices/sizes, receipt-time order) and RECORDS the venue
behaviour as non-gating findings. These tests pin that split and the wiring.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from crypto_collector import cli
from crypto_collector.cli import _job_args, build_parser, run_backfill_trades_replay
from crypto_collector.ops import JobSpec
from crypto_collector.replay import TradesReplaySummary, replay_liquidations_run


def _row(
    *,
    product: str = "BTC-USDT-SWAP",
    exchange_time: str,
    received_at: str,
    price: float = 60_000.0,
    size: float = 0.1,
    channel: str = "liquidations",
) -> dict:
    return {
        "source": "okx",
        "product": product,
        "channel": channel,
        "event_type": "liquidation",
        "exchange_time": exchange_time,
        "received_at": received_at,
        "side": "sell",
        "price": price,
        "size": size,
        "trade_id": None,
        "sequence": None,
        "raw_type": "liquidation-orders",
        "metadata": {"instrument_id": f"perp:okx:{product}", "pos_side": "long"},
    }


def _write_run(run_path: Path, rows: list[dict]) -> None:
    clean = run_path / "clean"
    clean.mkdir(parents=True)
    (clean / "events.jsonl").write_text(
        "".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8"
    )


def _t(seconds: float) -> str:
    """ISO timestamp `seconds` after 2026-09-08T00:00:00Z."""
    whole = int(seconds)
    frac = int(round((seconds - whole) * 1_000_000))
    hh, rem = divmod(whole, 3600)
    mm, ss = divmod(rem, 60)
    return f"2026-09-08T{hh:02d}:{mm:02d}:{ss:02d}.{frac:06d}+00:00"


# --- the verdict split ------------------------------------------------------------


def test_okx_shaped_run_is_replayable_with_venue_lag_recorded(tmp_path: Path) -> None:
    """Interleaved products, one detail 10 min late, a per-product backward step:
    all venue behaviour - recorded, not gating. Receipt order is clean."""
    run = tmp_path / "okx_perp_liquidations" / "20260908_000000"
    rows = [
        _row(product="BTC-USDT-SWAP", exchange_time=_t(10.0), received_at=_t(11.0)),
        _row(product="SOPH-USDT-SWAP", exchange_time=_t(5.0), received_at=_t(12.0)),  # global step back
        _row(product="BTC-USDT-SWAP", exchange_time=_t(12.0), received_at=_t(13.0)),
        # OKX batch: two details of one product pushed together, older one second
        _row(product="SOPH-USDT-SWAP", exchange_time=_t(20.0), received_at=_t(14.0)),
        _row(product="SOPH-USDT-SWAP", exchange_time=_t(18.0), received_at=_t(14.0)),  # per-product step back
        # a detail delivered 10 minutes late
        _row(product="CP-USDT-SWAP", exchange_time=_t(0.0), received_at=_t(600.5)),
    ]
    _write_run(run, rows)

    summary = replay_liquidations_run(run, max_clock_skew_ms=60_000.0)

    assert summary.replayable is True, summary
    assert summary.findings == []
    assert summary.informational_findings == ["delayed_delivery", "per_product_reorder"]
    assert summary.mode == "liquidation_stream_none_native"
    assert summary.gap_detection == "none_native"
    assert summary.replay_type == "trades"  # same quarantine/promote contract
    assert summary.event_count == 6
    assert summary.product_count == 3
    assert summary.product is None  # all-venue lane: no single product
    assert summary.instrument_id is None
    assert summary.non_monotonic_count == 1  # per-product backward steps only
    assert summary.delayed_delivery_count == 1
    assert summary.max_delivery_delay_ms == 600_500.0
    assert summary.max_clock_skew_ms == 600_500.0
    assert summary.excessive_clock_skew_count == 0  # the skew gate does not apply
    assert summary.non_monotonic_received_at_count == 0
    on_disk = json.loads((run / "metrics" / "replay_summary.json").read_text(encoding="utf-8"))
    assert on_disk["replayable"] is True
    assert on_disk["informational_findings"] == ["delayed_delivery", "per_product_reorder"]
    assert on_disk["delayed_delivery_count"] == 1


def test_lane_threshold_moves_the_delay_finding_not_the_verdict(tmp_path: Path) -> None:
    run = tmp_path / "okx_perp_liquidations" / "20260908_000000"
    _write_run(
        run,
        [
            _row(exchange_time=_t(0.0), received_at=_t(1.0)),
            _row(exchange_time=_t(1.0), received_at=_t(500.0)),  # 499 s late
        ],
    )
    strict = replay_liquidations_run(run, max_clock_skew_ms=60_000.0, write_summary=False)
    venue = replay_liquidations_run(run, max_clock_skew_ms=900_000.0, write_summary=False)
    assert strict.replayable is True and venue.replayable is True
    assert strict.delayed_delivery_count == 1 and strict.informational_findings == ["delayed_delivery"]
    assert venue.delayed_delivery_count == 0 and venue.informational_findings == []


def test_single_symbol_lane_keeps_product_and_instrument(tmp_path: Path) -> None:
    run = tmp_path / "bybit_perp_liquidations_btcusdt" / "20260908_000000"
    rows = [_row(product="BTCUSDT", exchange_time=_t(i), received_at=_t(i + 0.2)) for i in range(3)]
    for r in rows:
        r["source"] = "bybit"
        r["metadata"]["instrument_id"] = "perp:bybit:BTCUSDT"
    _write_run(run, rows)
    summary = replay_liquidations_run(run, write_summary=False)
    assert summary.replayable is True
    assert summary.product == "BTCUSDT"
    assert summary.instrument_id == "perp:bybit:BTCUSDT"
    assert summary.product_count == 1
    assert summary.informational_findings == []


def test_receipt_time_going_backwards_blocks(tmp_path: Path) -> None:
    """received_at is the plant's own clock; a backward step is a capture defect."""
    run = tmp_path / "okx_perp_liquidations" / "20260908_000000"
    _write_run(
        run,
        [
            _row(exchange_time=_t(0.0), received_at=_t(5.0)),
            _row(exchange_time=_t(1.0), received_at=_t(4.0)),
            _row(exchange_time=_t(2.0), received_at=_t(6.0)),
        ],
    )
    summary = replay_liquidations_run(run, write_summary=False)
    assert summary.replayable is False
    assert summary.findings == ["non_monotonic_received_at"]
    assert summary.non_monotonic_received_at_count == 1


def test_missing_received_at_blocks(tmp_path: Path) -> None:
    run = tmp_path / "okx_perp_liquidations" / "20260908_000000"
    row = _row(exchange_time=_t(0.0), received_at=_t(1.0))
    row["received_at"] = None
    _write_run(run, [row])
    summary = replay_liquidations_run(run, write_summary=False)
    assert summary.replayable is False
    assert summary.findings == ["missing_received_at"]


def test_trade_row_filed_as_liquidation_blocks(tmp_path: Path) -> None:
    """The Bybit allLiquidation/publicTrade shape collision must fail closed."""
    run = tmp_path / "bybit_perp_liquidations_btcusdt" / "20260908_000000"
    _write_run(
        run,
        [
            _row(exchange_time=_t(0.0), received_at=_t(1.0)),
            _row(exchange_time=_t(1.0), received_at=_t(2.0), channel="trades"),
        ],
    )
    summary = replay_liquidations_run(run, write_summary=False)
    assert summary.replayable is False
    assert summary.findings == ["wrong_channel"]
    assert summary.wrong_channel_count == 1


def test_invalid_price_or_size_blocks_and_empty_run_is_not_replayable(tmp_path: Path) -> None:
    run = tmp_path / "okx_perp_liquidations" / "20260908_000000"
    _write_run(
        run,
        [
            _row(exchange_time=_t(0.0), received_at=_t(1.0), price=0.0),
            _row(exchange_time=_t(1.0), received_at=_t(2.0), size=-1.0),
        ],
    )
    summary = replay_liquidations_run(run, write_summary=False)
    assert summary.replayable is False
    assert summary.findings == ["invalid_prices", "invalid_sizes"]

    empty = tmp_path / "okx_perp_liquidations" / "20260908_000001"
    _write_run(empty, [])
    summary = replay_liquidations_run(empty, write_summary=False)
    assert summary.replayable is False
    assert summary.findings == ["no_events"]


def test_summary_contract_keeps_pre_v12_fields_neutral() -> None:
    """Consumers reading the pre-v12 TradesReplaySummary keys must see nothing new
    in `findings`, and the new fields must default so older summaries still load."""
    summary = TradesReplaySummary(
        replay_type="trades",
        mode="trade_stream_none_native",
        run_path="x",
        events_path="x",
        source=None,
        product=None,
        instrument_id=None,
        event_count=0,
        first_trade_id=None,
        last_trade_id=None,
        first_event_time=None,
        last_event_time=None,
        non_monotonic_count=0,
        trade_id_gap_count=0,
        trade_id_gap_total_missing=0,
        invalid_price_count=0,
        invalid_size_count=0,
        excessive_clock_skew_count=0,
        max_clock_skew_ms=None,
        duplicate_trade_id_count=0,
        replayable=False,
        findings=["no_events"],
    )
    d = summary.to_dict()
    assert d["informational_findings"] == []
    assert d["delayed_delivery_count"] == 0
    assert d["product_count"] == 0


# --- wiring: collectors, CLI, runner dispatch ---------------------------------------


def test_liquidation_collectors_use_the_liquidation_scorer() -> None:
    for fn in (
        cli.collect_okx_liquidations_segment,
        cli.collect_bybit_liquidations_segment,
        cli.collect_binance_liquidations_segment,
    ):
        names = fn.__code__.co_names
        assert "replay_liquidations_run" in names, fn.__name__
        assert "replay_trades_stream_run" not in names, fn.__name__
    # and the ordinary trade tapes did NOT move
    assert "replay_trades_stream_run" in cli.collect_bybit_trades_segment.__code__.co_names


def test_cli_flag_and_job_args_expose_liquidations_scorer() -> None:
    parser = build_parser()
    args = parser.parse_args(["backfill-trades-replay", "--liquidations"])
    assert args.liquidations is True
    assert parser.parse_args(["backfill-trades-replay"]).liquidations is False

    spec = JobSpec(name="x", job_type="backfill-trades-replay", interval_seconds=3600, args={})
    assert _job_args(spec).liquidations is False
    spec = JobSpec(
        name="x", job_type="backfill-trades-replay", interval_seconds=3600, args={"liquidations": True}
    )
    assert _job_args(spec).liquidations is True


def test_backfill_trades_replay_liquidations_flag_selects_scorer(tmp_path: Path, capsys) -> None:
    """--liquidations overrides --stream/--funding: an OKX-shaped run that the
    trades-stream verdict rejects is re-issued replayable with the v12 mode."""
    source_root = tmp_path / "raw" / "market" / "okx_perp_liquidations"
    run = source_root / "20200101_000000"  # far in the past: clears the 1 h floor
    _write_run(
        run,
        [
            _row(product="BTC-USDT-SWAP", exchange_time=_t(10.0), received_at=_t(11.0)),
            _row(product="SOPH-USDT-SWAP", exchange_time=_t(5.0), received_at=_t(12.0)),
        ],
    )
    args = SimpleNamespace(
        source_root=source_root,
        limit=10,
        max_age_hours=1_000_000.0,
        min_age_hours=1.0,
        overwrite=True,
        stream=True,
        wallet_flow=False,
        funding=False,
        liquidations=True,
        max_clock_skew_ms=900_000.0,
        format="json",
    )
    run_backfill_trades_replay(args)
    report = json.loads(capsys.readouterr().out)
    assert report["created_count"] == 1
    on_disk = json.loads((run / "metrics" / "replay_summary.json").read_text(encoding="utf-8"))
    assert on_disk["mode"] == "liquidation_stream_none_native"
    assert on_disk["replayable"] is True
    assert on_disk["findings"] == []
