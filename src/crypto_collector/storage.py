from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4


_MISSING = object()


@dataclass(slots=True)
class RunPaths:
    base: Path
    raw: Path
    clean: Path
    quarantine: Path
    metrics: Path


def prepare_run_paths(output_root: Path, source: str, started_at: datetime | None = None) -> RunPaths:
    ts = (started_at or datetime.now(tz=UTC)).strftime("%Y%m%d_%H%M%S")
    base = output_root / source / ts
    raw = base / "raw"
    clean = base / "clean"
    quarantine = base / "quarantine"
    metrics = base / "metrics"
    for path in (raw, clean, quarantine, metrics):
        path.mkdir(parents=True, exist_ok=True)
    return RunPaths(base=base, raw=raw, clean=clean, quarantine=quarantine, metrics=metrics)


class JsonlSink:
    def __init__(self, root: Path, filename: str) -> None:
        self.path = root / filename

    def write(self, row: dict[str, Any]) -> None:
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, sort_keys=True))
            handle.write("\n")


class ParquetDatasetSink:
    def __init__(
        self,
        root: Path,
        *,
        schema_version: str = "v1",
        batch_size: int = 1000,
    ) -> None:
        self.root = root
        self.schema_version = schema_version
        self.batch_size = batch_size
        self._rows: list[dict[str, Any]] = []
        self.root.mkdir(parents=True, exist_ok=True)

    def write(self, row: dict[str, Any]) -> None:
        self._rows.append(_with_partitions(row, schema_version=self.schema_version))
        if len(self._rows) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        if not self._rows:
            return
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError as exc:
            raise RuntimeError("Install the 'pyarrow' package to write normalized Parquet datasets.") from exc

        table = pa.Table.from_pylist(self._rows)
        pq.write_to_dataset(
            table,
            root_path=str(self.root),
            partition_cols=["schema_version", "source", "event_date"],
            basename_template=f"part-{uuid4().hex}-{{i}}.parquet",
        )
        self._rows.clear()


def _with_partitions(row: dict[str, Any], *, schema_version: str) -> dict[str, Any]:
    partitioned: dict[str, Any] = {}
    for key, value in row.items():
        normalized = _normalize_for_parquet(value)
        if normalized is _MISSING:
            continue
        partitioned[key] = normalized
    partitioned["schema_version"] = schema_version
    partitioned["event_date"] = _event_date_for_row(partitioned)
    partitioned.setdefault("source", "unknown")
    return partitioned


def _event_date_for_row(row: dict[str, Any]) -> str:
    for key in ("event_time", "exchange_time", "received_at"):
        value = row.get(key)
        if not value:
            continue
        if isinstance(value, datetime):
            return value.astimezone(UTC).date().isoformat()
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC).date().isoformat()
            except ValueError:
                continue
    return datetime.now(tz=UTC).date().isoformat()


def _normalize_for_parquet(value: Any) -> Any:
    if value is None:
        return _MISSING
    if isinstance(value, dict):
        normalized_dict: dict[str, Any] = {}
        for key, child in value.items():
            normalized_child = _normalize_for_parquet(child)
            if normalized_child is _MISSING:
                continue
            normalized_dict[key] = normalized_child
        return normalized_dict or _MISSING
    if isinstance(value, list):
        normalized_list = [
            normalized_item
            for item in value
            if (normalized_item := _normalize_for_parquet(item)) is not _MISSING
        ]
        return normalized_list
    return value

