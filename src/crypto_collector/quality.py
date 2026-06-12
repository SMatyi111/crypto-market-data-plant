from __future__ import annotations

import math
import threading
from collections import Counter
from datetime import UTC, datetime

from .models import NormalizedL3Event, ValidationResult

# Every trades-lane normalizer emits channel="trades" (event_type varies by
# venue: "trade", Coinbase "match"/"last_match"). The promotion bar
# (`replay_trades_run`) fails a whole run when any clean print has a missing or
# non-positive price/size, so the live gate must quarantine those per-event —
# otherwise one odd print silently costs the full segment at scoring time.
# Funding (channel="funding") and depth lanes use other gates/shapes.
_TRADES_CHANNEL = "trades"


class QualityGate:
    def __init__(
        self,
        *,
        max_delay_ms: int = 60_000,
        max_future_skew_ms: int = 5_000,
        require_monotonic_sequence: bool = True,
        session_id: str | None = None,
    ) -> None:
        self.max_delay_ms = max_delay_ms
        self.max_future_skew_ms = max_future_skew_ms
        self.require_monotonic_sequence = require_monotonic_sequence
        # session_id keeps the per-stream sequence cursor scoped to a single collection
        # run. Otherwise an exchange-side sequence reset or a collector restart in the
        # same process would look like a backwards jump and falsely flag every event.
        self.session_id = session_id
        self._last_sequence_by_stream: dict[tuple[str, str, str, str | None], int] = {}
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

        if event.channel == _TRADES_CHANNEL:
            # Strict print bar (subsumes the generic price/size checks in the else
            # branch — one bad print must yield ONE reason, not two, or the reject
            # histogram double-counts every trades reject).
            if event.price is None or not (math.isfinite(event.price) and event.price > 0):
                reasons.append("invalid_trade_price")
            if event.size is None or not (math.isfinite(event.size) and event.size > 0):
                reasons.append("invalid_trade_size")
        else:
            if event.price is not None and event.price <= 0:
                reasons.append("non_positive_price")
            if event.size is not None and event.size < 0:
                reasons.append("negative_size")

        if event.exchange_time is not None:
            delay_ms = (event.received_at - event.exchange_time.astimezone(UTC)).total_seconds() * 1000
            if delay_ms > self.max_delay_ms or delay_ms < -self.max_future_skew_ms:
                reasons.append("stale_or_clock_skew")

        # Trades a venue replays at subscribe time (Kraken's trade-channel snapshot
        # frame, Coinbase's last_match) re-deliver recent history. Promotion dedups
        # by run only, so an already-captured print re-entering clean lands a
        # duplicate in curated — but a replayed print ABOVE this run's sequence
        # high-water is genuinely new data (it covers a mid-run reconnect window),
        # and quarantining it would punch a provable id gap into clean, failing the
        # WHOLE run at scoring. So a tagged print passes only when the run's cursor
        # proves it new; everything else (at/below the cursor, or no cursor yet —
        # segment-start replays whose originals live in the previous run) is
        # quarantined. Raw and quarantine keep every replayed print either way.
        subscribe_replay = event.metadata.get("subscribe_replay") is True

        if self.require_monotonic_sequence and event.sequence is not None:
            stream_key = (event.source, event.product, event.channel, self.session_id)
            with self._sequence_lock:
                last_sequence = self._last_sequence_by_stream.get(stream_key)
                if subscribe_replay and not (
                    last_sequence is not None and event.sequence > last_sequence
                ):
                    reasons.append("subscribe_replay")
                # Some venues legitimately re-send the same sequence as an idempotency marker.
                # Treat only strictly-decreasing sequences as a violation.
                elif last_sequence is not None and event.sequence < last_sequence:
                    reasons.append("non_monotonic_sequence")
                elif last_sequence is None or event.sequence > last_sequence:
                    self._last_sequence_by_stream[stream_key] = event.sequence
        elif subscribe_replay:
            # No sequence to prove novelty -> treat as the replay it claims to be.
            reasons.append("subscribe_replay")

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
