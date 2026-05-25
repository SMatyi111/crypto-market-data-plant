from __future__ import annotations

import threading
from collections import Counter
from datetime import UTC, datetime

from .models import NormalizedL3Event, ValidationResult


class QualityGate:
    def __init__(
        self,
        *,
        max_delay_ms: int = 5_000,
        max_future_skew_ms: int = 60_000,
        require_monotonic_sequence: bool = True,
    ) -> None:
        self.max_delay_ms = max_delay_ms
        self.max_future_skew_ms = max_future_skew_ms
        self.require_monotonic_sequence = require_monotonic_sequence
        self._last_sequence_by_stream: dict[tuple[str, str, str], int] = {}
        self._sequence_lock = threading.Lock()
        self.reject_counts: Counter[str] = Counter()
        self._reject_lock = threading.Lock()

    def validate(self, event: NormalizedL3Event) -> ValidationResult:
        reasons: list[str] = []

        parse_errors = event.metadata.get("parse_errors", [])
        if parse_errors:
            reasons.extend(str(error) for error in parse_errors)

        if event.side is not None and event.side not in {"buy", "sell"}:
            reasons.append("invalid_side")

        if event.price is not None and event.price <= 0:
            reasons.append("non_positive_price")

        if event.size is not None and event.size < 0:
            reasons.append("negative_size")

        if event.exchange_time is not None:
            delay_ms = (event.received_at - event.exchange_time.astimezone(UTC)).total_seconds() * 1000
            if delay_ms > self.max_delay_ms or delay_ms < -self.max_future_skew_ms:
                reasons.append("stale_or_clock_skew")

        if self.require_monotonic_sequence and event.sequence is not None:
            stream_key = (event.source, event.product, event.channel)
            with self._sequence_lock:
                last_sequence = self._last_sequence_by_stream.get(stream_key)
                # Some venues legitimately re-send the same sequence as an idempotency marker.
                # Treat only strictly-decreasing sequences as a violation.
                if last_sequence is not None and event.sequence < last_sequence:
                    reasons.append("non_monotonic_sequence")
                elif last_sequence is None or event.sequence > last_sequence:
                    self._last_sequence_by_stream[stream_key] = event.sequence

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


def utc_now() -> datetime:
    return datetime.now(tz=UTC)


class MetadataQualityGate:
    def __init__(self) -> None:
        self.reject_counts: Counter[str] = Counter()
        self._reject_lock = threading.Lock()

    def validate(self, record: object) -> ValidationResult:
        metadata = getattr(record, "metadata", {})
        parse_errors = metadata.get("parse_errors", []) if isinstance(metadata, dict) else []
        reasons = [str(error) for error in parse_errors]

        first_update_id = getattr(record, "first_update_id", None)
        final_update_id = getattr(record, "final_update_id", None)
        if (
            first_update_id is not None
            and final_update_id is not None
            and final_update_id < first_update_id
        ):
            reasons.append("invalid_update_range")

        if reasons:
            with self._reject_lock:
                for reason in reasons:
                    self.reject_counts[reason] += 1

        return ValidationResult(accepted=not reasons, reasons=reasons)

    def metrics(self) -> dict[str, int]:
        with self._reject_lock:
            return dict(self.reject_counts)
