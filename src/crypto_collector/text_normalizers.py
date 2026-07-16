from __future__ import annotations

import threading
from collections import Counter
from datetime import UTC, datetime
from typing import Any

from .collectors.text_feeds import parse_source_ts
from .models import NormalizedTextItem, RawMessage, ValidationResult

# Normalizer + live quality gate for the text-capture lanes (STANDARDS "text"
# section). Deliberately NOT the market QualityGate: text rows have no price/size/
# sequence, and - critically - no timestamp gate. The platform-claimed `source_ts`
# is a claim, not a measurement (the 72h RSS probe caught a ~16h stale Cointelegraph
# pubDate), so staleness must never quarantine an otherwise-good item; `ingestion_ts`
# (plant clock) is the authoritative axis and is stamped by the collector itself.
# What the gate DOES enforce is envelope integrity: an item without a stable
# source_id or content_hash cannot participate in the (source, source_id,
# content_hash) dedup contract and is quarantined for forensics.


class TextItemNormalizer:
    """Map a text-poll payload (rss_item / reddit_item, see collectors.text_feeds)
    onto the `NormalizedTextItem` envelope. The raw payload string is carried
    UNTOUCHED in `raw_item`; only envelope fields are derived."""

    def __init__(self, source: str) -> None:
        self.source = source  # lane family: "rss" | "reddit"

    def normalize(self, raw: RawMessage) -> NormalizedTextItem:
        payload = raw.payload
        parse_errors: list[str] = []

        product = str(payload.get("feed") or payload.get("subreddit") or "unknown")
        source_id = payload.get("source_id") or None
        if not source_id:
            parse_errors.append("missing_source_id")
        content_hash = payload.get("content_hash") or None
        if not content_hash:
            parse_errors.append("missing_content_hash")
        raw_item = payload.get("raw_item")
        if raw_item is None:
            parse_errors.append("missing_raw_item")

        row_type = payload.get("row_type")
        event_type = row_type if row_type in ("new", "edit") else "unknown"

        metadata: dict[str, Any] = {"poll": payload.get("poll") or {}}
        source_ts: datetime | None = None
        created_utc = payload.get("created_utc")
        source_ts_raw = payload.get("source_ts_raw")
        if isinstance(created_utc, (int, float)) and not isinstance(created_utc, bool):
            source_ts = datetime.fromtimestamp(float(created_utc), tz=UTC)
        elif source_ts_raw:
            metadata["source_ts_raw"] = source_ts_raw
            source_ts = parse_source_ts(str(source_ts_raw))
            if source_ts is None:
                # A malformed platform timestamp is a diagnostic, NOT a defect in
                # the text itself - the item stays clean (ingestion_ts is
                # authoritative) and the scorer counts it non-gating.
                metadata["source_ts_unparseable"] = True
        listing = payload.get("listing")
        if listing:
            metadata["listing"] = str(listing)
        if parse_errors:
            metadata["parse_errors"] = parse_errors

        return NormalizedTextItem(
            source=self.source,
            product=product,
            channel="text",
            event_type=event_type,
            source_id=str(source_id) if source_id else None,
            source_ts=source_ts,
            ingestion_ts=raw.received_at,
            content_hash=str(content_hash) if content_hash else None,
            raw_item=str(raw_item) if raw_item is not None else None,
            metadata=metadata,
        )


class TextQualityGate:
    """Envelope-integrity gate for text items. Quarantine reasons: any
    `parse_errors` from the normalizer (missing_source_id / missing_content_hash /
    missing_raw_item) and `unknown_event_type`. There is intentionally NO
    freshness/staleness reason - see the module docstring."""

    def __init__(self) -> None:
        self.reject_counts: Counter[str] = Counter()
        self._reject_lock = threading.Lock()

    def validate(self, event: NormalizedTextItem) -> ValidationResult:
        reasons: list[str] = []
        parse_errors = event.metadata.get("parse_errors", [])
        if parse_errors:
            reasons.extend(str(error) for error in parse_errors)
        if event.event_type == "unknown":
            reasons.append("unknown_event_type")
        if reasons:
            with self._reject_lock:
                for reason in reasons:
                    self.reject_counts[reason] += 1
        return ValidationResult(accepted=not reasons, reasons=reasons)

    def metrics(self) -> dict[str, int]:
        with self._reject_lock:
            return dict(self.reject_counts)
