from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any


def utc_now() -> datetime:
    return datetime.now(tz=UTC)


@dataclass(slots=True)
class RawMessage:
    source: str
    received_at: datetime
    payload: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        row["received_at"] = self.received_at.isoformat()
        return row


@dataclass(slots=True)
class NormalizedL3Event:
    source: str
    product: str
    channel: str
    event_type: str
    exchange_time: datetime | None
    received_at: datetime
    side: str | None = None
    price: float | None = None
    size: float | None = None
    order_id: str | None = None
    trade_id: str | None = None
    sequence: int | None = None
    raw_type: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        row["received_at"] = self.received_at.isoformat()
        row["exchange_time"] = (
            self.exchange_time.isoformat() if self.exchange_time is not None else None
        )
        return row


@dataclass(slots=True)
class NormalizedTextItem:
    """One captured text item (STANDARDS "text" section): raw text only, no
    capture-time NLP. `ingestion_ts` (plant clock) is the AUTHORITATIVE time axis;
    `source_ts` is the platform's claim, preserved but never trusted for gating or
    partitioning (the RSS probe caught a ~16h stale publish timestamp). The row's
    dedup key is (source, source_id, content_hash); an edit re-emits the same
    source_id with a new content_hash (`event_type="edit"`)."""

    source: str  # lane family: "rss" | "reddit"
    product: str  # feed key or subreddit, e.g. "cointelegraph", "CryptoCurrency"
    channel: str  # always "text"
    event_type: str  # "new" | "edit"
    source_id: str | None
    source_ts: datetime | None  # platform-claimed publish/create time (UTC)
    ingestion_ts: datetime  # plant clock, authoritative
    content_hash: str | None  # SHA-256 over the item's semantic content fields
    raw_item: str | None = None  # untouched raw payload (XML fragment / JSON string)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        row["source_ts"] = self.source_ts.isoformat() if self.source_ts is not None else None
        row["ingestion_ts"] = self.ingestion_ts.isoformat()
        # `received_at` mirrors ingestion_ts under the plant-wide column name so the
        # parquet event_date partition derives from the AUTHORITATIVE ingestion
        # clock (storage._event_date_for_row reads event_time/exchange_time/
        # received_at) - never from the claimed source_ts, and never from the
        # promotion pass's wall clock.
        row["received_at"] = row["ingestion_ts"]
        return row


@dataclass(slots=True)
class ValidationResult:
    accepted: bool
    reasons: list[str]

