from __future__ import annotations

from .context_models import AssetRef, InstrumentRef
from .instrument_master import ASSET_ALIAS_RECORDS, list_instrument_aliases, list_asset_aliases


KNOWN_QUOTES = ("USDT", "USDC", "USD", "BTC", "ETH")
ALIAS_INDEX = {
    record.alias.upper(): record
    for record in ASSET_ALIAS_RECORDS
}


def resolve_asset(symbol_or_alias: str) -> AssetRef | None:
    key = symbol_or_alias.strip().upper()
    if not key:
        return None
    record = ALIAS_INDEX.get(key)
    if record is None:
        return None
    return AssetRef(
        symbol=record.canonical_symbol,
        asset_id=record.asset_id,
        canonical_symbol=record.canonical_symbol,
        name=record.name,
    )


def resolve_spot_instrument(symbol: str, *, venue: str) -> InstrumentRef | None:
    value = symbol.strip().upper().replace("/", "")
    normalized_venue = _normalize_venue(venue)
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


def resolve_perp_instrument(symbol: str, *, venue: str) -> InstrumentRef | None:
    normalized_venue = _normalize_venue(venue)
    value = symbol.strip().upper()
    if not value:
        return None

    explicit = _explicit_instrument_match(
        value,
        venue=normalized_venue,
        instrument_type="perp",
    )
    if explicit is not None:
        return explicit

    if normalized_venue == "deribit" and value.endswith("-PERPETUAL"):
        base_symbol = value.split("-", maxsplit=1)[0]
        base_asset = resolve_asset(base_symbol)
        quote_asset = resolve_asset("USD")
        if base_asset is None or quote_asset is None:
            return None
        return InstrumentRef(
            instrument_id=f"perp:{normalized_venue}:{value}",
            venue=normalized_venue,
            venue_symbol=value,
            canonical_symbol=f"{base_asset.symbol}/{quote_asset.symbol}-PERP",
            instrument_type="perp",
            base_asset=base_asset,
            quote_asset=quote_asset,
        )

    stripped = value
    if stripped.endswith("_PERP"):
        stripped = stripped[: -len("_PERP")]
    if stripped.endswith("PERP"):
        stripped = stripped[: -len("PERP")]
    stripped = stripped.replace("-", "").replace("_", "")
    spot_like = resolve_spot_instrument(stripped, venue=normalized_venue)
    if spot_like is None:
        return None
    return InstrumentRef(
        instrument_id=f"perp:{normalized_venue}:{value}",
        venue=normalized_venue,
        venue_symbol=value,
        canonical_symbol=f"{spot_like.base_asset.symbol}/{spot_like.quote_asset.symbol}-PERP",
        instrument_type="perp",
        base_asset=spot_like.base_asset,
        quote_asset=spot_like.quote_asset,
    )


def resolve_venue_instrument(
    symbol: str,
    *,
    venue: str,
    instrument_type: str | None = None,
) -> InstrumentRef | None:
    normalized_venue = _normalize_venue(venue)
    if instrument_type == "spot":
        return resolve_spot_instrument(symbol, venue=normalized_venue)
    if instrument_type == "perp":
        return resolve_perp_instrument(symbol, venue=normalized_venue)

    explicit = _explicit_instrument_match(symbol.strip().upper(), venue=normalized_venue)
    if explicit is not None:
        return explicit

    return resolve_spot_instrument(symbol, venue=normalized_venue) or resolve_perp_instrument(
        symbol,
        venue=normalized_venue,
    )


def _explicit_instrument_match(
    symbol: str,
    *,
    venue: str,
    instrument_type: str | None = None,
) -> InstrumentRef | None:
    for record in list_instrument_aliases(venue=venue, instrument_type=instrument_type):
        if record.venue_symbol.upper() != symbol.upper():
            continue
        base_asset = _asset_from_id(record.base_asset_id)
        quote_asset = _asset_from_id(record.quote_asset_id) if record.quote_asset_id else None
        if base_asset is None:
            return None
        return InstrumentRef(
            instrument_id=record.instrument_id,
            venue=record.venue,
            venue_symbol=record.venue_symbol,
            canonical_symbol=record.canonical_symbol,
            instrument_type=record.instrument_type,
            base_asset=base_asset,
            quote_asset=quote_asset,
        )
    return None


def _asset_from_id(asset_id: str | None) -> AssetRef | None:
    if asset_id is None:
        return None
    for record in list_asset_aliases(asset_id=asset_id):
        if record.alias == record.canonical_symbol:
            return resolve_asset(record.alias)
    return None


def _normalize_venue(value: str) -> str:
    mapping = {
        "binance_futures": "binance-futures",
        "binancefutures": "binance-futures",
    }
    return mapping.get(value.strip().lower(), value.strip().lower())
