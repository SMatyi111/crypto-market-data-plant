"""Open-interest stream on the Binance futures REST worker.

OI is a venue-computed quantity, not a price and not an event stream. The two
things worth pinning: it must never ride in `price` (anything aggregating
prices would ingest 6-figure contract counts), and the poll builder must hit
the documented endpoint with only the symbol.
"""
import asyncio
import datetime as dt

from crypto_collector.collectors.binance_futures_rest import make_open_interest_poll
from crypto_collector.market_normalizers import BinanceOpenInterestNormalizer
from crypto_collector.models import RawMessage


def test_normalizer_puts_oi_in_size_not_price():
    raw = RawMessage(source="binance-futures",
                     received_at=dt.datetime(2026, 8, 25, tzinfo=dt.timezone.utc),
                     payload={"openInterest": "106529.858", "symbol": "BTCUSDT",
                              "time": 1787997783462})
    ev = BinanceOpenInterestNormalizer().normalize(raw)
    assert ev.channel == "open_interest" and ev.event_type == "open_interest"
    assert ev.price is None
    assert ev.size == 106529.858
    assert ev.metadata["open_interest"] == 106529.858
    assert ev.metadata["canonical_symbol"] == "BTC/USDT-PERP"
    assert ev.side is None and ev.trade_id is None and ev.sequence is None


def test_poll_hits_open_interest_endpoint():
    calls = []

    def fake_fetch(path, params):
        calls.append((path, params))
        return {"openInterest": "1.0", "symbol": "BTCUSDT", "time": 1}

    rows, done = asyncio.run(make_open_interest_poll("btcusdt", fetch=fake_fetch)())
    assert calls == [("/fapi/v1/openInterest", {"symbol": "BTCUSDT"})]
    assert rows == [{"openInterest": "1.0", "symbol": "BTCUSDT", "time": 1}]
    assert done is False


def test_stream_registered():
    from crypto_collector.cli import _BINANCE_FUTURES_REST_STREAMS
    assert "open_interest" in _BINANCE_FUTURES_REST_STREAMS
