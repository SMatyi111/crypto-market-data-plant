from __future__ import annotations

from crypto_collector.instrument_master import (
    instrument_master_summary,
    list_asset_aliases,
    list_instrument_aliases,
)


def test_instrument_master_summary_reports_counts() -> None:
    summary = instrument_master_summary()
    assert summary["asset_count"] >= 6
    assert summary["asset_alias_count"] >= summary["asset_count"]
    assert summary["instrument_alias_count"] >= 4


def test_list_asset_aliases_can_filter() -> None:
    rows = list_asset_aliases(asset_id="crypto:BTC")
    aliases = {row.alias for row in rows}
    assert "BTC" in aliases
    assert "XBT" in aliases


def test_list_instrument_aliases_filters_venue() -> None:
    rows = list_instrument_aliases(venue="deribit", instrument_type="perp")
    assert rows
    assert all(row.venue == "deribit" for row in rows)
    assert all(row.instrument_type == "perp" for row in rows)
