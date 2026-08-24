"""Bybit liquidations lane: labelling and config-dispatch regressions.

Two failure modes this guards, both of which have real precedent in this repo:

1. The v5 `allLiquidation` frame is shaped exactly like `publicTrade`, so routing
   it through the trade normalizer "works" and silently files forced closes as
   ordinary prints. That corrupts the trade tape rather than failing loudly, so
   the channel/event_type labels are asserted explicitly.
2. Per-venue `build_segment_args` lambdas have twice dropped fields (`market`,
   `jsonl_fsync`) on the way to the segment collector. `market` matters more here
   than for trades: liquidations only exist on derivatives, so a dropped default
   would silently collect spot and find nothing forever.
"""
import datetime as dt

from crypto_collector.market_normalizers import BybitLiquidationNormalizer
from crypto_collector.models import RawMessage
from crypto_collector.ops import COLLECTOR_JOB_TYPES, JobSpec


def _frame(side="Sell"):
    return RawMessage(
        source="bybit",
        received_at=dt.datetime(2026, 8, 24, tzinfo=dt.timezone.utc),
        payload={
            "topic": "allLiquidation.BTCUSDT",
            "type": "snapshot",
            "ts": 1787561094982,
            "data": [{"S": side, "T": 1787561094796, "p": "77357.50",
                      "s": "BTCUSDT", "v": "2.315"}],
        },
    )


def test_liquidation_is_not_labelled_as_a_trade():
    ev = BybitLiquidationNormalizer().normalize_many(_frame())[0]
    assert ev.channel == "liquidations"
    assert ev.event_type == "liquidation"
    # A liquidation has no taker/maker relationship; emitting one would let a
    # consumer treat these as prints.
    assert "buyer_is_maker" not in ev.metadata


def test_liquidation_fields_and_raw_side_preserved():
    ev = BybitLiquidationNormalizer().normalize_many(_frame("Buy"))[0]
    assert ev.price == 77357.5 and ev.size == 2.315
    assert ev.product == "BTCUSDT"
    assert ev.metadata["canonical_symbol"] == "BTC/USDT-PERP"
    assert ev.metadata["instrument_id"] == "perp:bybit:BTCUSDT"
    # Side semantics differ from publicTrade; the venue token must survive so a
    # future reader can re-derive without re-collecting.
    assert ev.metadata["bybit_liquidation_side_raw"] == "Buy"
    # No liquidation id and no dense counter -> non-sequence feed.
    assert ev.trade_id is None and ev.sequence is None


def test_batched_frame_fans_out():
    raw = _frame()
    raw.payload["data"].append({"S": "Buy", "T": 1787561094800, "p": "77300.0",
                                "s": "BTCUSDT", "v": "0.5"})
    assert len(BybitLiquidationNormalizer().normalize_many(raw)) == 2


def test_non_list_data_is_ignored():
    raw = _frame()
    raw.payload["data"] = None
    assert BybitLiquidationNormalizer().normalize_many(raw) == []


def test_job_type_registered_as_collector_lane():
    assert "bybit-liquidations-worker" in COLLECTOR_JOB_TYPES


def test_market_defaults_to_linear_through_dispatch():
    """`market` must survive the arg builder, and must default to linear:
    liquidations do not exist on spot, so a silent 'spot' default collects
    nothing forever and looks like a quiet market."""
    from crypto_collector.cli import _job_args

    job = JobSpec(name="bybit-btc-liquidations",
                  job_type="bybit-liquidations-worker",
                  interval_seconds=5, args={"symbol": "BTCUSDT"}, enabled=True)
    ns = _job_args(job)
    assert ns.market == "linear"
    assert ns.channel == "allLiquidation"
    assert ns.symbol == "BTCUSDT"
