from __future__ import annotations

from .context_models import AssetRef, InstrumentRef


KNOWN_ASSETS = {
    "BTC": AssetRef(symbol="BTC", asset_id="crypto:BTC", canonical_symbol="BTC", name="Bitcoin"),
    "ETH": AssetRef(symbol="ETH", asset_id="crypto:ETH", canonical_symbol="ETH", name="Ethereum"),
    "USDT": AssetRef(symbol="USDT", asset_id="stablecoin:USDT", canonical_symbol="USDT", name="Tether USD"),
    "USDC": AssetRef(symbol="USDC", asset_id="stablecoin:USDC", canonical_symbol="USDC", name="USD Coin"),
    "USD": AssetRef(symbol="USD", asset_id="fiat:USD", canonical_symbol="USD", name="US Dollar"),
}
KNOWN_QUOTES = ("USDT", "USDC", "USD", "BTC", "ETH")


def resolve_asset(symbol_or_alias: str) -> AssetRef | None:
    key = symbol_or_alias.strip().upper()
    if not key:
        return None
    aliases = {
        "XBT": "BTC",
        "BITCOIN": "BTC",
        "ETHEREUM": "ETH",
        "TETHER": "USDT",
    }
    return KNOWN_ASSETS.get(aliases.get(key, key))


def resolve_spot_instrument(symbol: str, *, venue: str) -> InstrumentRef | None:
    value = symbol.strip().upper().replace("/", "")
    normalized_venue = venue.strip().lower()
    if not value:
        return None
    for quote in KNOWN_QUOTES:
        if value.endswith(quote) and len(value) > len(quote):
            base_asset = resolve_asset(value[: -len(quote)])
            quote_asset = resolve_asset(quote)
            if base_asset is None or quote_asset is None:
                continue
            return InstrumentRef(
                instrument_id=f"spot:{normalized_venue}:{value}",
                venue=normalized_venue,
                venue_symbol=value,
                canonical_symbol=f"{base_asset.canonical_symbol}/{quote_asset.canonical_symbol}",
                instrument_type="spot",
                base_asset=base_asset,
                quote_asset=quote_asset,
            )
    return None
