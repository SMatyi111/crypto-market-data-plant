"""Binance USD-M liquidations lane: field-choice and shape regressions.

Binance offers several near-synonyms per liquidation and they are NOT
equivalent; the lane commits to specific ones and these tests pin that choice so
it cannot drift silently:

* price: `ap` (average FILL price) over `p` (the order's limit price)
* size:  `z` (accumulated filled) over `q` (order quantity)

For a fully filled liquidation the pairs agree, so a wrong choice is invisible
until a partial fill - which is exactly when a cascade is being measured.
"""
import datetime as dt

from crypto_collector.market_normalizers import BinanceLiquidationNormalizer
from crypto_collector.models import RawMessage
from crypto_collector.ops import COLLECTOR_JOB_TYPES, JobSpec


def _frame(**over):
    order = {"s": "BTCUSDT", "S": "SELL", "o": "LIMIT", "f": "IOC", "q": "0.014",
             "p": "9910", "ap": "9905.5", "X": "FILLED", "l": "0.014",
             "z": "0.014", "T": 1568014460893}
    order.update(over)
    return RawMessage(source="binance",
                      received_at=dt.datetime(2026, 8, 24, tzinfo=dt.timezone.utc),
                      payload={"e": "forceOrder", "E": 1568014460893, "o": order})


def test_labels_and_instrument():
    ev = BinanceLiquidationNormalizer().normalize_many(_frame())[0]
    assert ev.channel == "liquidations" and ev.event_type == "liquidation"
    assert ev.metadata["canonical_symbol"] == "BTC/USDT-PERP"
    assert ev.trade_id is None and ev.sequence is None


def test_prefers_average_fill_price_and_filled_qty():
    """The case that distinguishes the choices: a PARTIAL fill at a price away
    from the limit. Picking `p`/`q` here would overstate both."""
    ev = BinanceLiquidationNormalizer().normalize_many(
        _frame(q="1.0", z="0.25", p="9910", ap="9800.0", X="PARTIALLY_FILLED"))[0]
    assert ev.price == 9800.0      # not 9910
    assert ev.size == 0.25         # not 1.0
    # the alternates survive so the choice is reversible without a re-collect
    assert ev.metadata["binance_order_price"] == "9910"
    assert ev.metadata["binance_order_qty"] == "1.0"
    assert ev.metadata["binance_order_status"] == "PARTIALLY_FILLED"


def test_falls_back_when_average_price_absent():
    ev = BinanceLiquidationNormalizer().normalize_many(_frame(ap=None, z=None))[0]
    assert ev.price == 9910.0 and ev.size == 0.014


def test_side_is_the_closing_order_side():
    # SELL => a long was force-closed. Mirrors OKX (closing order), NOT Bybit
    # (documented as position side) - the venues genuinely differ.
    assert BinanceLiquidationNormalizer().normalize_many(_frame(S="SELL"))[0].side == "sell"
    assert BinanceLiquidationNormalizer().normalize_many(_frame(S="BUY"))[0].side == "buy"


def test_missing_order_object_is_ignored():
    raw = _frame()
    raw.payload["o"] = None
    assert BinanceLiquidationNormalizer().normalize_many(raw) == []


def test_job_type_registered_and_all_market_defaults():
    from crypto_collector.cli import _job_args

    assert "binance-liquidations-worker" in COLLECTOR_JOB_TYPES
    ns = _job_args(JobSpec(name="binance-liquidations",
                           job_type="binance-liquidations-worker",
                           interval_seconds=5, args={}, enabled=True))
    assert ns.channel == "!forceOrder@arr"
    assert ns.rotate_at_midnight is True
