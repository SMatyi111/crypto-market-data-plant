from __future__ import annotations

from crypto_collector.asset_registry import (
    resolve_asset,
    resolve_perp_instrument,
    resolve_spot_instrument,
    resolve_venue_instrument,
)


def test_resolve_asset_accepts_alias() -> None:
    asset = resolve_asset("xbt")
    assert asset is not None
    assert asset.asset_id == "crypto:BTC"
    assert asset.canonical_symbol == "BTC"


def test_resolve_spot_instrument_builds_ids() -> None:
    instrument = resolve_spot_instrument("BTCUSDT", venue="binance")
    assert instrument is not None
    assert instrument.instrument_id == "spot:binance:BTCUSDT"
    assert instrument.canonical_symbol == "BTC/USDT"
    assert instrument.base_asset.asset_id == "crypto:BTC"
    assert instrument.quote_asset is not None
    assert instrument.quote_asset.asset_id == "stablecoin:USDT"


def test_resolve_spot_instrument_accepts_slash_symbol() -> None:
    instrument = resolve_spot_instrument("BTC/USDT", venue="binance")
    assert instrument is not None
    assert instrument.instrument_id == "spot:binance:BTCUSDT"
    assert instrument.canonical_symbol == "BTC/USDT"


def test_resolve_spot_instrument_rejects_unknown_pair() -> None:
    assert resolve_spot_instrument("UNKNOWNUSDT", venue="binance") is None


def test_resolve_perp_instrument_handles_deribit_perpetual() -> None:
    instrument = resolve_perp_instrument("BTC-PERPETUAL", venue="deribit")
    assert instrument is not None
    assert instrument.instrument_id == "perp:deribit:BTC-PERPETUAL"
    assert instrument.canonical_symbol == "BTC/USD-PERP"
    assert instrument.base_asset.asset_id == "crypto:BTC"


def test_resolve_venue_instrument_handles_binance_futures() -> None:
    instrument = resolve_venue_instrument(
        "BTCUSDT",
        venue="binance-futures",
        instrument_type="perp",
    )
    assert instrument is not None
    assert instrument.instrument_id == "perp:binance-futures:BTCUSDT"
    assert instrument.canonical_symbol == "BTC/USDT-PERP"
    assert instrument.quote_asset is not None
    assert instrument.quote_asset.asset_id == "stablecoin:USDT"


def test_resolve_asset_handles_extended_alias_table() -> None:
    assert resolve_asset("SOL").asset_id == "crypto:SOL"
    assert resolve_asset("DOGE").asset_id == "crypto:DOGE"
    assert resolve_asset("RIPPLE").asset_id == "crypto:XRP"
