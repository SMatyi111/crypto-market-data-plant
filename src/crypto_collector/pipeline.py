from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from .collectors.base import BaseCollector
from .models import RawMessage, utc_now
from .normalizer import GenericL3Normalizer
from .quality import QualityGate
from .storage import JsonlSink, ParquetDatasetSink, RotatingJsonlSink, RunPaths


@dataclass(slots=True)
class RunSummary:
    raw_messages: int = 0
    clean_events: int = 0
    quarantined_events: int = 0
    deadline_reached: bool = False

    def to_dict(self) -> dict[str, int | bool]:
        return {
            "raw_messages": self.raw_messages,
            "clean_events": self.clean_events,
            "quarantined_events": self.quarantined_events,
            "deadline_reached": self.deadline_reached,
        }


class CollectorPipeline:
    def __init__(
        self,
        *,
        collector: BaseCollector,
        normalizer: GenericL3Normalizer,
        quality_gate: QualityGate,
        run_paths: RunPaths,
        normalized_root: Path | None = None,
        raw_rotate_bytes: int = 512 * 1024 * 1024,
        metrics_flush_every: int = 1000,
    ) -> None:
        self.metrics_flush_every = max(0, int(metrics_flush_every))
        self.collector = collector
        self.normalizer = normalizer
        self.quality_gate = quality_gate
        # Raw traffic is the fastest-growing file; rotate it so a long-running
        # collector doesn't produce a single multi-GB messages.jsonl.
        self.raw_sink = RotatingJsonlSink(run_paths.raw, "messages.jsonl", max_bytes=raw_rotate_bytes)
        self.clean_sink = JsonlSink(run_paths.clean, "events.jsonl")
        self.quarantine_sink = JsonlSink(run_paths.quarantine, "events.jsonl")
        self.metrics_sink = JsonlSink(run_paths.metrics, "summary.jsonl")
        self.parquet_sink = ParquetDatasetSink(normalized_root) if normalized_root else None

    async def run(
        self,
        limit: int | None = None,
        *,
        deadline_utc: datetime | None = None,
    ) -> RunSummary:
        """Run the pipeline until the collector stream ends, `limit` is reached, or
        the wall clock crosses `deadline_utc` (for day-bounded rotation). The deadline
        check happens after each frame so partial work is flushed in the existing
        finally block.

        `limit` bounds **frames** (raw WS messages), not normalized events. For
        single-event venues (Binance, Coinbase) one frame is one event so the two are
        the same; for batched venues (Bybit `publicTrade`, Kraken `trade`) one frame
        fans out to several events via `normalize_many`, so a frame-bounded segment can
        contain more clean events than `limit`."""
        summary = RunSummary()
        try:
            async for raw in self.collector.stream(limit=limit):
                summary.raw_messages += 1
                self.raw_sink.write(raw.to_dict())

                for normalized in _normalize_events(self.normalizer, raw):
                    verdict = self.quality_gate.validate(normalized)
                    if verdict.accepted:
                        summary.clean_events += 1
                        normalized_row = normalized.to_dict()
                        self.clean_sink.write(normalized_row)
                        if self.parquet_sink is not None:
                            self.parquet_sink.write(normalized_row)
                    else:
                        summary.quarantined_events += 1
                        quarantined_row = normalized.to_dict()
                        quarantined_row["reasons"] = verdict.reasons
                        self.quarantine_sink.write(quarantined_row)

                if deadline_utc is not None and utc_now() >= deadline_utc:
                    summary.deadline_reached = True
                    break

                if (
                    self.metrics_flush_every
                    and summary.raw_messages % self.metrics_flush_every == 0
                ):
                    # Stream partial metrics so external monitors see in-flight state
                    # instead of having to wait for shutdown to learn the gate is
                    # quarantining 30% of events.
                    self.metrics_sink.write(
                        {
                            **summary.to_dict(),
                            "reject_counts": self.quality_gate.metrics(),
                            "partial": True,
                        }
                    )
        finally:
            # Always flush, even on cancellation / exception, so buffered Parquet rows
            # and the summary metrics are persisted instead of lost on shutdown.
            self.metrics_sink.write(
                {
                    **summary.to_dict(),
                    "reject_counts": self.quality_gate.metrics(),
                    "partial": False,
                }
            )
            if self.parquet_sink is not None:
                self.parquet_sink.flush()
        return summary


def _normalize_events(normalizer: object, raw: RawMessage) -> list:
    """Normalize one raw frame into one-or-more normalized events.

    Most venues map a WS frame to a single event and expose only `normalize`. Batched
    venues (Bybit `publicTrade`, Kraken `trade`/`book`) deliver an array of events per
    frame and expose `normalize_many` instead. Preferring `normalize_many` when present
    keeps the fan-out logic with the venue-specific normalizer and leaves the existing
    single-event normalizers (and this pipeline's per-event accounting) unchanged."""
    normalize_many = getattr(normalizer, "normalize_many", None)
    if callable(normalize_many):
        return list(normalize_many(raw))
    return [normalizer.normalize(raw)]

