from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .collectors.base import BaseCollector
from .models import RawMessage
from .normalizer import GenericL3Normalizer
from .quality import QualityGate
from .storage import JsonlSink, ParquetDatasetSink, RunPaths


@dataclass(slots=True)
class RunSummary:
    raw_messages: int = 0
    clean_events: int = 0
    quarantined_events: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "raw_messages": self.raw_messages,
            "clean_events": self.clean_events,
            "quarantined_events": self.quarantined_events,
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
    ) -> None:
        self.collector = collector
        self.normalizer = normalizer
        self.quality_gate = quality_gate
        self.raw_sink = JsonlSink(run_paths.raw, "messages.jsonl")
        self.clean_sink = JsonlSink(run_paths.clean, "events.jsonl")
        self.quarantine_sink = JsonlSink(run_paths.quarantine, "events.jsonl")
        self.metrics_sink = JsonlSink(run_paths.metrics, "summary.jsonl")
        self.parquet_sink = ParquetDatasetSink(normalized_root) if normalized_root else None

    async def run(self, limit: int | None = None) -> RunSummary:
        summary = RunSummary()
        async for raw in self.collector.stream(limit=limit):
            summary.raw_messages += 1
            self.raw_sink.write(raw.to_dict())

            normalized = self.normalizer.normalize(raw)
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

        self.metrics_sink.write(
            {
                **summary.to_dict(),
                "reject_counts": self.quality_gate.metrics(),
            }
        )
        if self.parquet_sink is not None:
            self.parquet_sink.flush()
        return summary

