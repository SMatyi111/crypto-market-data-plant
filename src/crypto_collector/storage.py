from __future__ import annotations

import json
import os
import time
from collections.abc import Callable
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
    """Append-only JSONL writer with three durability postures, chosen by `fsync` and
    the `fsync_interval_*` knobs:

    * Per-event fsync (`fsync=True`, intervals at their 1-event / 0-ms defaults): every
      line is reopened-written-flushed-fsynced-closed. Maximally durable, no open handle
      to leak, readable mid-run — but the per-line fsync is disk-latency-bound, which is
      what caps a hot lane's sustainable events/sec.
    * Batched fsync (`fsync=True`, a >1 event interval and/or a >0 ms interval): one
      handle stays open and EVERY line is flushed (so the OS always holds whole lines and
      a hard kill can't leave a torn tail), while the disk-blocking fsync is amortized to
      once per `fsync_interval_events` events OR `fsync_interval_ms` milliseconds,
      whichever comes first. Raises the throughput ceiling; a clean close still fsyncs
      (no loss on shutdown) and a hard kill loses at most one un-fsynced batch.
    * Buffered, no fsync (`fsync=False`): one handle, flushed every `flush_every` rows,
      never fsynced. Fastest, least durable.
    """

    def __init__(
        self,
        root: Path,
        filename: str,
        *,
        fsync: bool = True,
        flush_every: int = 100,
        fsync_interval_events: int = 1,
        fsync_interval_ms: float = 0.0,
        time_fn: Callable[[], float] = time.monotonic,
    ) -> None:
        self.path = root / filename
        self._fsync = fsync
        self._flush_every = max(1, int(flush_every))
        self._fsync_interval_events = max(1, int(fsync_interval_events))
        self._fsync_interval_ms = max(0.0, float(fsync_interval_ms))
        self._time_fn = time_fn
        # Per-event fsync only when fsync is on AND batching is effectively disabled, so a
        # bare JsonlSink(...) keeps the exact reopen-per-line behavior the durability
        # test and the metrics sink rely on.
        self._per_event_fsync = (
            fsync and self._fsync_interval_events <= 1 and self._fsync_interval_ms <= 0.0
        )
        self._pending_writes = 0
        self._fsync_pending = 0
        self._last_fsync = 0.0
        self._handle = None

    def write(self, row: dict[str, Any]) -> None:
        line = json.dumps(row, sort_keys=True) + "\n"
        if self._per_event_fsync:
            with self.path.open("a", encoding="utf-8") as handle:
                handle.write(line)
                handle.flush()
                # fsync prevents a torn last line if the process dies before the OS flushes.
                os.fsync(handle.fileno())
            return

        if self._handle is None:
            self._handle = self.path.open("a", encoding="utf-8")
            self._last_fsync = self._time_fn()
        handle = self._handle
        handle.write(line)
        if self._fsync:
            # Flush every line so the OS holds only whole lines (no torn tail on a hard
            # kill); fsync — the disk-latency-bound call and the real throughput ceiling —
            # is batched per fsync_interval_events / fsync_interval_ms.
            handle.flush()
            self._fsync_pending += 1
            if self._should_fsync():
                os.fsync(handle.fileno())
                self._fsync_pending = 0
                self._last_fsync = self._time_fn()
        else:
            self._pending_writes += 1
            if self._pending_writes >= self._flush_every:
                handle.flush()
                self._pending_writes = 0

    def _should_fsync(self) -> bool:
        if self._fsync_pending >= self._fsync_interval_events:
            return True
        if (
            self._fsync_interval_ms > 0.0
            and (self._time_fn() - self._last_fsync) * 1000.0 >= self._fsync_interval_ms
        ):
            return True
        return False

    def close(self) -> None:
        if self._handle is not None:
            self._handle.flush()
            if self._fsync:
                # A clean shutdown must lose nothing, so force the final batch to disk.
                os.fsync(self._handle.fileno())
            self._handle.close()
            self._handle = None
            self._pending_writes = 0
            self._fsync_pending = 0


class RotatingJsonlSink:
    """JSONL sink that rolls the active file when it exceeds max_bytes.

    Long-running collectors otherwise produce single multi-GB files that are
    expensive to grep, replay, or copy. The on-disk layout is:
        messages.jsonl       (active)
        messages.1.jsonl     (rolled)
        messages.2.jsonl     (rolled)
    """

    def __init__(
        self,
        root: Path,
        filename: str,
        *,
        max_bytes: int = 512 * 1024 * 1024,
        fsync: bool = True,
        flush_every: int = 100,
        fsync_interval_events: int = 1,
        fsync_interval_ms: float = 0.0,
        time_fn: Callable[[], float] = time.monotonic,
    ) -> None:
        self.root = root
        self.filename = filename
        self.max_bytes = max(1, int(max_bytes))
        self._fsync = fsync
        self._flush_every = max(1, int(flush_every))
        self._fsync_interval_events = max(1, int(fsync_interval_events))
        self._fsync_interval_ms = max(0.0, float(fsync_interval_ms))
        self._time_fn = time_fn
        # See JsonlSink: per-event fsync (reopen per line) only when batching is off; a
        # batched/buffered handle stays open and is flushed per line so rotation and a
        # hard kill never leave a torn record.
        self._per_event_fsync = (
            fsync and self._fsync_interval_events <= 1 and self._fsync_interval_ms <= 0.0
        )
        self._pending_writes = 0
        self._fsync_pending = 0
        self._last_fsync = 0.0
        self._active_path = root / filename
        self._handle = None
        self._part_index = self._discover_next_part_index()
        self._current_bytes = (
            self._active_path.stat().st_size if self._active_path.exists() else 0
        )

    @property
    def path(self) -> Path:
        return self._active_path

    def write(self, row: dict[str, Any]) -> None:
        encoded = (json.dumps(row, sort_keys=True) + "\n").encode("utf-8")
        if self._current_bytes > 0 and self._current_bytes + len(encoded) > self.max_bytes:
            self._rotate()
        if self._per_event_fsync:
            with self._active_path.open("ab") as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
        else:
            if self._handle is None:
                self._handle = self._active_path.open("ab")
                self._last_fsync = self._time_fn()
            handle = self._handle
            handle.write(encoded)
            if self._fsync:
                handle.flush()
                self._fsync_pending += 1
                if self._should_fsync():
                    os.fsync(handle.fileno())
                    self._fsync_pending = 0
                    self._last_fsync = self._time_fn()
            else:
                self._pending_writes += 1
                if self._pending_writes >= self._flush_every:
                    handle.flush()
                    self._pending_writes = 0
        self._current_bytes += len(encoded)

    def _should_fsync(self) -> bool:
        if self._fsync_pending >= self._fsync_interval_events:
            return True
        if (
            self._fsync_interval_ms > 0.0
            and (self._time_fn() - self._last_fsync) * 1000.0 >= self._fsync_interval_ms
        ):
            return True
        return False

    def _rotate(self) -> None:
        self.close()
        stem, dot, ext = self.filename.rpartition(".")
        if dot:
            new_name = f"{stem}.{self._part_index}.{ext}"
        else:
            new_name = f"{self.filename}.{self._part_index}"
        rotated_path = self.root / new_name
        os.replace(self._active_path, rotated_path)
        self._part_index += 1
        self._current_bytes = 0

    def close(self) -> None:
        if self._handle is not None:
            self._handle.flush()
            if self._fsync:
                # A clean shutdown (and the close that precedes a rotation) must lose
                # nothing, so force the final batch to disk.
                os.fsync(self._handle.fileno())
            self._handle.close()
            self._handle = None
            self._pending_writes = 0
            self._fsync_pending = 0

    def _discover_next_part_index(self) -> int:
        stem, dot, ext = self.filename.rpartition(".")
        if not dot:
            prefix, suffix = self.filename + ".", ""
        else:
            prefix, suffix = stem + ".", "." + ext
        highest = 0
        if self.root.exists():
            for entry in self.root.iterdir():
                name = entry.name
                if not (name.startswith(prefix) and name.endswith(suffix)):
                    continue
                middle = name[len(prefix) : len(name) - len(suffix)] if suffix else name[len(prefix) :]
                if middle.isdigit():
                    highest = max(highest, int(middle))
        return highest + 1


class ParquetDatasetSink:
    def __init__(
        self,
        root: Path,
        *,
        schema_version: str = "v2",
        batch_size: int = 100,
    ) -> None:
        # batch_size caps how many normalized rows live in memory before a flush.
        # On a hard kill / power cut, anything buffered here is lost — raw JSONL
        # is still durable (per-row fsync) so you can rebuild from there, but the
        # normalized layer briefly disagrees with raw. 100 keeps the lost-on-kill
        # window to ~100 events of disagreement at the cost of more, smaller
        # part-files in the Parquet dataset.
        #
        # Partition layout is keyed off schema_version: v1 is the legacy
        # schema_version/source/event_date tree (no instrument level); v2+ adds an
        # `instrument=` partition (the sanitized canonical symbol) so data is
        # pullable by (venue, instrument, event_date). Existing v1 data on disk is
        # untouched — a v1-tagged sink keeps writing the 3-level layout.
        self.root = root
        self.schema_version = schema_version
        self.batch_size = batch_size
        self._partition_by_instrument = schema_version != "v1"
        # v2 puts `instrument` ABOVE `event_date` so one instrument's full history
        # lives under a single dir (the north-star pull-by-instrument layout); v1 is
        # the legacy source/event_date tree.
        if self._partition_by_instrument:
            self._partition_cols = ["schema_version", "source", "instrument", "event_date"]
        else:
            self._partition_cols = ["schema_version", "source", "event_date"]
        self._rows: list[dict[str, Any]] = []
        self.root.mkdir(parents=True, exist_ok=True)

    def write(self, row: dict[str, Any]) -> None:
        self._rows.append(
            _with_partitions(
                row,
                schema_version=self.schema_version,
                partition_by_instrument=self._partition_by_instrument,
            )
        )
        if len(self._rows) >= self.batch_size:
            self.flush()

    def discard(self) -> None:
        """Drop all buffered (un-flushed) rows without writing them.

        Used by promotion's per-run failure path: a failed run is retried from
        scratch on the next pass (it has no index entry), so flushing its partial
        rows would duplicate them in curated parquet on the retry — and a poisoned
        buffer would cascade the failure into every later run sharing the sink.
        """
        self._rows.clear()

    def flush(self) -> None:
        if not self._rows:
            return
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError as exc:
            raise RuntimeError("Install the 'pyarrow' package to write normalized Parquet datasets.") from exc

        # Build each column with pa.array (which scans ALL values for type inference)
        # rather than pa.Table.from_pylist, which infers a column's type from its
        # LEADING value. Under from_pylist a column whose first row is None is typed
        # `null` and silently dropped on write — so features that are None until the
        # first trade of the day (e.g. trade_aggressor_imbalance_60s, trade_vwap)
        # vanished entirely on days whose opening buckets had no trades, while
        # always-numeric columns survived. Union keys in first-seen order so
        # heterogeneous rows still contribute every column.
        column_names: list[str] = []
        seen: set[str] = set()
        for row in self._rows:
            for key in row:
                if key not in seen:
                    seen.add(key)
                    column_names.append(key)
        arrays = [pa.array([row.get(name) for row in self._rows]) for name in column_names]
        table = pa.Table.from_arrays(arrays, names=column_names)
        written_files: list[str] = []
        pq.write_to_dataset(
            table,
            root_path=str(self.root),
            partition_cols=self._partition_cols,
            basename_template=f"part-{uuid4().hex}-{{i}}.parquet",
            file_visitor=lambda visited: written_files.append(visited.path),
        )
        # fsync every part-file before returning: promotion appends a per-line-fsynced
        # index entry right after flush() on the promise that "an index hit implies the
        # curated copy is on disk". pyarrow only writes to the OS page cache, so without
        # this a power cut could persist the index while the parquet bytes are lost.
        # (Windows has no usable directory fsync; file-level durability is the
        # achievable guarantee here.)
        for file_path in written_files:
            with open(file_path, "ab") as handle:
                os.fsync(handle.fileno())
        self._rows.clear()


def _with_partitions(
    row: dict[str, Any],
    *,
    schema_version: str,
    partition_by_instrument: bool = True,
) -> dict[str, Any]:
    partitioned: dict[str, Any] = {}
    for key, value in row.items():
        normalized = _normalize_for_parquet(value)
        if normalized is _MISSING:
            continue
        partitioned[key] = normalized
    partitioned["schema_version"] = schema_version
    partitioned["event_date"] = _event_date_for_row(partitioned)
    partitioned.setdefault("source", "unknown")
    if partition_by_instrument:
        # The `instrument` partition value is the sanitized canonical symbol. To free
        # the `instrument` column name for the (string) partition without losing the
        # resolved InstrumentRef detail, the nested struct is preserved under
        # `instrument_ref` (v2 schema). v1 leaves the nested `instrument` field as-is.
        instrument_struct = partitioned.pop("instrument", _MISSING)
        if instrument_struct is not _MISSING:
            partitioned["instrument_ref"] = instrument_struct
        partitioned["instrument"] = _instrument_partition(row)
    return partitioned


def _instrument_partition(row: dict[str, Any]) -> str:
    """Derive the `instrument=` partition value: the resolved canonical symbol, else
    the venue product, else 'unknown' — sanitized for a filesystem/Hive path."""
    instrument = row.get("instrument")
    canonical = instrument.get("canonical_symbol") if isinstance(instrument, dict) else None
    value = canonical or row.get("product") or "unknown"
    return _sanitize_partition_value(str(value))


def _sanitize_partition_value(value: str) -> str:
    # Canonical symbols use '/' (e.g. "BTC/USDT"), which would split the partition
    # directory, so map it to '-' ("BTC-USDT", as in the curated layout) and replace
    # any other non-[alnum_-.] char with '_'. Hive partition values must be one path
    # segment.
    collapsed = value.replace("/", "-")
    safe = "".join(ch if (ch.isalnum() or ch in "-_.") else "_" for ch in collapsed)
    return safe or "unknown"


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

