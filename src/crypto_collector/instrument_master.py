from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class AssetAliasRecord:
    asset_id: str
    canonical_symbol: str
    alias: str
    name: str
    alias_type: str = "symbol"
    venue: str | None = None
    status: str = "active"
    valid_from: str | None = None
    valid_to: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class InstrumentAliasRecord:
    instrument_id: str
    venue: str
    venue_symbol: str
    canonical_symbol: str
    instrument_type: str
    base_asset_id: str
    quote_asset_id: str | None = None
    settlement_asset_id: str | None = None
    expiry: str | None = None
    strike: float | None = None
    option_type: str | None = None
    status: str = "active"
    valid_from: str | None = None
    valid_to: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


ASSET_ALIAS_RECORDS: tuple[AssetAliasRecord, ...] = (
    AssetAliasRecord("crypto:BTC", "BTC", "BTC", "Bitcoin"),
    AssetAliasRecord("crypto:BTC", "BTC", "XBT", "Bitcoin", venue="kraken"),
    AssetAliasRecord("crypto:BTC", "BTC", "BITCOIN", "Bitcoin", alias_type="name"),
    AssetAliasRecord("crypto:ETH", "ETH", "ETH", "Ethereum"),
    AssetAliasRecord("crypto:ETH", "ETH", "ETHER", "Ethereum", alias_type="name"),
    AssetAliasRecord("crypto:ETH", "ETH", "ETHEREUM", "Ethereum", alias_type="name"),
    AssetAliasRecord("crypto:SOL", "SOL", "SOL", "Solana"),
    AssetAliasRecord("crypto:SOL", "SOL", "SOLANA", "Solana", alias_type="name"),
    AssetAliasRecord("crypto:XRP", "XRP", "XRP", "XRP"),
    AssetAliasRecord("crypto:XRP", "XRP", "RIPPLE", "XRP", alias_type="name"),
    AssetAliasRecord("crypto:DOGE", "DOGE", "DOGE", "Dogecoin"),
    AssetAliasRecord("crypto:DOGE", "DOGE", "DOGECOIN", "Dogecoin", alias_type="name"),
    AssetAliasRecord("crypto:BNB", "BNB", "BNB", "BNB"),
    AssetAliasRecord("crypto:BNB", "BNB", "BINANCE COIN", "BNB", alias_type="name"),
    AssetAliasRecord("crypto:ADA", "ADA", "ADA", "Cardano"),
    AssetAliasRecord("crypto:ADA", "ADA", "CARDANO", "Cardano", alias_type="name"),
    AssetAliasRecord("stablecoin:USDT", "USDT", "USDT", "Tether USD"),
    AssetAliasRecord("stablecoin:USDT", "USDT", "TETHER", "Tether USD", alias_type="name"),
    AssetAliasRecord("stablecoin:USDC", "USDC", "USDC", "USD Coin"),
    AssetAliasRecord("stablecoin:USDC", "USDC", "USD COIN", "USD Coin", alias_type="name"),
    AssetAliasRecord("fiat:USD", "USD", "USD", "US Dollar"),
    AssetAliasRecord("fiat:USD", "USD", "US DOLLAR", "US Dollar", alias_type="name"),
)

INSTRUMENT_ALIAS_RECORDS: tuple[InstrumentAliasRecord, ...] = (
    InstrumentAliasRecord(
        instrument_id="perp:binance-futures:BTCUSDT",
        venue="binance-futures",
        venue_symbol="BTCUSDT",
        canonical_symbol="BTC/USDT-PERP",
        instrument_type="perp",
        base_asset_id="crypto:BTC",
        quote_asset_id="stablecoin:USDT",
        settlement_asset_id="stablecoin:USDT",
    ),
    InstrumentAliasRecord(
        instrument_id="perp:binance-futures:ETHUSDT",
        venue="binance-futures",
        venue_symbol="ETHUSDT",
        canonical_symbol="ETH/USDT-PERP",
        instrument_type="perp",
        base_asset_id="crypto:ETH",
        quote_asset_id="stablecoin:USDT",
        settlement_asset_id="stablecoin:USDT",
    ),
    InstrumentAliasRecord(
        instrument_id="perp:deribit:BTC-PERPETUAL",
        venue="deribit",
        venue_symbol="BTC-PERPETUAL",
        canonical_symbol="BTC/USD-PERP",
        instrument_type="perp",
        base_asset_id="crypto:BTC",
        quote_asset_id="fiat:USD",
        settlement_asset_id="crypto:BTC",
    ),
    InstrumentAliasRecord(
        instrument_id="perp:deribit:ETH-PERPETUAL",
        venue="deribit",
        venue_symbol="ETH-PERPETUAL",
        canonical_symbol="ETH/USD-PERP",
        instrument_type="perp",
        base_asset_id="crypto:ETH",
        quote_asset_id="fiat:USD",
        settlement_asset_id="crypto:ETH",
    ),
)


def list_asset_aliases(*, asset_id: str | None = None) -> list[AssetAliasRecord]:
    rows = list(ASSET_ALIAS_RECORDS)
    if asset_id is not None:
        rows = [row for row in rows if row.asset_id == asset_id]
    return rows


def list_instrument_aliases(
    *,
    venue: str | None = None,
    instrument_type: str | None = None,
) -> list[InstrumentAliasRecord]:
    rows = list(INSTRUMENT_ALIAS_RECORDS)
    if venue is not None:
        rows = [row for row in rows if row.venue == venue]
    if instrument_type is not None:
        rows = [row for row in rows if row.instrument_type == instrument_type]
    return rows


def instrument_master_summary() -> dict[str, Any]:
    asset_ids = sorted({row.asset_id for row in ASSET_ALIAS_RECORDS})
    venues = sorted({row.venue for row in INSTRUMENT_ALIAS_RECORDS})
    return {
        "asset_count": len(asset_ids),
        "asset_alias_count": len(ASSET_ALIAS_RECORDS),
        "instrument_alias_count": len(INSTRUMENT_ALIAS_RECORDS),
        "venues": venues,
    }
