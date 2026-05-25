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
class ValidationResult:
    accepted: bool
    reasons: list[str]

