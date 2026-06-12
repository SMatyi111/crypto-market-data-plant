from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class AssetRef:
    symbol: str
    asset_id: str | None = None
    canonical_symbol: str | None = None
    name: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class InstrumentRef:
    instrument_id: str
    venue: str
    venue_symbol: str
    canonical_symbol: str
    instrument_type: str
    base_asset: AssetRef
    quote_asset: AssetRef | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class NormalizedDepthUpdate:
    source: str
    product: str
    channel: str
    event_type: str
    event_time: datetime | None
    received_at: datetime
    first_update_id: int | None
    final_update_id: int | None
    instrument: InstrumentRef | None = None
    bids: list[list[float]] = field(default_factory=list)
    asks: list[list[float]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        row["event_time"] = self.event_time.isoformat() if self.event_time is not None else None
        row["received_at"] = self.received_at.isoformat()
        return row


# NormalizedTrade used to live here: dead code (no producer or consumer anywhere),
# with a trade_id typed `int | None` that contradicted the live venues' string ids.
# Removed before anyone built on it; trades use NormalizedL3Event (models.py).
