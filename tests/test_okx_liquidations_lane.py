"""OKX liquidations lane: nesting, instType scoping and symbol resolution.

OKX differs from every other lane here in ways each of which has already bitten
once during development:

* the channel is scoped by instrument TYPE, so one subscription streams all swaps
  and `product` varies row to row;
* the payload is doubly nested (`data[].details[]`);
* ids carry a market suffix (`BTC-USDT-SWAP`), and the GENERIC separator stripper
  silently yields `BTCUSDTSWAP`, which resolves to nothing and partitions every
  row as instrument=unknown.
"""
import datetime as dt

from crypto_collector.config import CollectorConfig
from crypto_collector.market_normalizers import OkxLiquidationNormalizer
from crypto_collector.models import RawMessage
from crypto_collector.ops import COLLECTOR_JOB_TYPES, JobSpec


def _frame(details=None, inst="BTC-USDT-SWAP"):
    return RawMessage(
        source="okx",
        received_at=dt.datetime(2026, 8, 24, tzinfo=dt.timezone.utc),
        payload={"arg": {"channel": "liquidation-orders", "instType": "SWAP"},
                 "data": [{"instId": inst, "instFamily": "BTC-USDT",
                           "instType": "SWAP",
                           "details": details if details is not None else
                           [{"bkPx": "77000.1", "posSide": "long", "side": "sell",
                             "sz": "12", "bkLoss": "0", "ts": "1787561822317"}]}]},
    )


def test_symbol_resolves_despite_swap_suffix():
    ev = OkxLiquidationNormalizer().normalize_many(_frame())[0]
    assert ev.metadata["canonical_symbol"] == "BTC/USDT-PERP"
    assert ev.metadata["instrument_id"] == "perp:okx:BTCUSDT"
    assert ev.product == "BTC-USDT-SWAP"


def test_labelled_as_liquidation_not_trade():
    ev = OkxLiquidationNormalizer().normalize_many(_frame())[0]
    assert ev.channel == "liquidations" and ev.event_type == "liquidation"
    assert ev.trade_id is None and ev.sequence is None


def test_nested_details_fan_out_and_pos_side_kept():
    ev = OkxLiquidationNormalizer().normalize_many(_frame([
        {"bkPx": "1", "posSide": "long", "side": "sell", "sz": "2", "ts": "1787561822317"},
        {"bkPx": "3", "posSide": "short", "side": "buy", "sz": "4", "ts": "1787561822317"},
    ]))
    assert len(ev) == 2
    # posSide is the venue's own statement of which side was liquidated; `side` is
    # the closing order. Keeping both is what makes a semantics change detectable.
    assert [e.metadata["okx_pos_side"] for e in ev] == ["long", "short"]
    assert [e.side for e in ev] == ["sell", "buy"]


def test_malformed_shapes_do_not_raise():
    raw = _frame()
    raw.payload["data"] = [{"instId": "X", "details": None}, "nonsense"]
    assert OkxLiquidationNormalizer().normalize_many(raw) == []
    raw.payload["data"] = None
    assert OkxLiquidationNormalizer().normalize_many(raw) == []


def test_subscription_key_defaults_to_instid_and_is_overridable():
    """The instType scoping must be expressible in config; defaulting wrong would
    subscribe to an instrument named 'SWAP' and silently receive nothing."""
    assert CollectorConfig(source="okx", output_root=".").okx_subscription_key == "instId"
    cfg = CollectorConfig(source="okx", output_root=".", okx_subscription_key="instType")
    assert cfg.okx_subscription_key == "instType"


def test_job_type_registered_and_defaults_to_swap():
    from crypto_collector.cli import _job_args

    assert "okx-liquidations-worker" in COLLECTOR_JOB_TYPES
    ns = _job_args(JobSpec(name="okx-liquidations", job_type="okx-liquidations-worker",
                           interval_seconds=5, args={}, enabled=True))
    assert ns.symbol == "SWAP"
    assert ns.channel == "liquidation-orders"
    # Bursty feed: a quiet stretch must not hold a segment open indefinitely.
    assert ns.rotate_at_midnight is True
