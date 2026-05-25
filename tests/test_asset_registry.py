from __future__ import annotations

from crypto_collector.asset_registry import (
    resolve_asset,
    resolve_spot_instrument,
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
