"""Repair curated runs whose promotion captured only part of the raw segment.

Background (2026-09-07 ops audit, finding 1): for ~3 months the hourly score
jobs scored the 1800 s segment the collector was still writing, the 300 s
promoter indexed the partial rows, and the run-keyed promotion index never
revisited the run. About a third of promoted runs on every trades/depth lane
carry fewer curated rows than their raw `clean/events.jsonl`. Raw is intact on
the hot tier or the cold tier, so the rows are recoverable.

This module finds those runs and, with `apply=True`, replaces their curated rows:

1. Candidates come from the promotion index (latest row per `run_path`, the
   same dedup rule `research_manifest` uses) whose `promoted_rows` fall below
   `min_ratio` x the raw clean row count. Raw is resolved hot-first, then under
   `cold_root/<lane>/<run>` (the byte-verified offload copy).
2. The run's curated part-files are located by reading the `source_run_path`
   column of every part-file in the event_date partitions around the run date.
   The promoter flushes once per run, so part-files are run-pure; any file that
   also holds rows of another run is never touched (the run is skipped).
3. Safety cross-check: the rows found in those files must equal the index's
   `promoted_rows`. A mismatch means the locator missed or double-counted
   something, and the run is skipped rather than guessed at.
4. Apply order is delete-then-write: if the process dies in between, the run is
   simply absent from curated while its index row still claims the old (short)
   count, so a re-run finds it short again and repairs it. Write-then-delete
   would instead risk silent duplicates.
5. The index is append-only and shared with the live promoter, so the repair
   APPENDS a superseding row for the run (`repromoted: true`, new
   `promoted_rows`, `previous_promoted_rows`). Readers keep the latest
   `promoted_at` per run. A run whose current replay summary is no longer
   replayable has its partial rows removed and gets a `promoted_rows: 0` row.

Dry-run is the default; nothing is written or deleted without `apply=True`.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .promotion import _parse_run_started_at, _read_json_file, _read_jsonl
from .storage import JsonlSink, ParquetDatasetSink

DEFAULT_MIN_RATIO = 0.98
DEFAULT_MIN_AGE_HOURS = 1.0


@dataclass(slots=True)
class RepromoteRun:
    run_path: str
    lane: str
    action: str
    promoted_rows: int
    raw_rows: int | None = None
    raw_dir: str | None = None
    replayable: bool | None = None
    curated_files: list[str] = field(default_factory=list)
    curated_rows: int = 0
    new_rows: int = 0
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class RepromoteReport:
    mode: str
    checked_at: str
    target_root: str
    index_path: str
    lanes: list[str]
    index_runs: int
    candidate_count: int
    repromoted_count: int
    removed_count: int
    skipped_count: int
    failed_count: int
    rows_removed: int
    rows_written: int
    findings: list[str]
    runs: list[RepromoteRun]

    @property
    def status(self) -> str:
        if self.failed_count:
            return "error"
        if any(run.action.startswith("skipped_") for run in self.runs):
            return "warn"
        return "ok"

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["status"] = self.status
        payload["runs"] = [run.to_dict() for run in self.runs]
        return payload


def latest_index_rows(index_path: Path) -> dict[str, dict[str, Any]]:
    """Latest index row per `run_path` (max `promoted_at`), skipping torn lines.
    Mirrors `research_manifest.read_promotion_index_with_stats`."""
    latest: dict[str, dict[str, Any]] = {}
    if not index_path.exists():
        return latest
    for line in index_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except ValueError:
            continue
        if not isinstance(row, dict):
            continue
        run_path = str(row.get("run_path") or "")
        if not run_path:
            continue
        previous = latest.get(run_path)
        if previous is None or str(row.get("promoted_at") or "") >= str(previous.get("promoted_at") or ""):
            latest[run_path] = row
    return latest


def resolve_raw_dir(run_path: str, cold_root: Path | None) -> Path | None:
    hot = Path(run_path)
    if hot.is_dir():
        return hot
    if cold_root is not None:
        cold = cold_root / hot.parent.name / hot.name
        if cold.is_dir():
            return cold
    return None


def count_clean_rows(run_dir: Path) -> int | None:
    events = run_dir / "clean" / "events.jsonl"
    if not events.exists():
        return None
    count = 0
    with events.open("rb") as handle:
        for line in handle:
            if line.strip():
                count += 1
    return count


def _partition_value(path: Path, key: str) -> str | None:
    prefix = f"{key}="
    for part in path.parts:
        if part.startswith(prefix):
            return part[len(prefix):]
    return None


def build_run_file_map(
    target_root: Path,
    *,
    event_dates: set[str] | None = None,
) -> tuple[dict[str, list[Path]], dict[Path, int], set[Path]]:
    """Map `source_run_path` -> curated part-files, plus per-file row counts and
    the set of files holding rows from more than one run (never to be deleted).

    Reads one column per part-file, restricted to the given `event_date`
    partitions when provided (a 30-minute run's rows sit in the run date and, at
    most, its neighbours)."""
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - pyarrow is a hard dependency of curated
        raise RuntimeError("Install the 'pyarrow' package to read curated Parquet datasets.") from exc

    run_files: dict[str, list[Path]] = {}
    file_rows: dict[Path, int] = {}
    shared: set[Path] = set()
    if not target_root.exists():
        return run_files, file_rows, shared
    for part in target_root.rglob("*.parquet"):
        if event_dates is not None and _partition_value(part, "event_date") not in event_dates:
            continue
        try:
            table = pq.read_table(part, columns=["source_run_path"])
        except (OSError, ValueError, KeyError):
            # A file without the column (or unreadable) is not ours to touch.
            continue
        file_rows[part] = table.num_rows
        runs_in_file = {str(v) for v in table.column(0).to_pylist() if v}
        if len(runs_in_file) > 1:
            shared.add(part)
        for run_path in runs_in_file:
            run_files.setdefault(run_path, []).append(part)
    return run_files, file_rows, shared


def _neighbour_dates(started_at: datetime) -> set[str]:
    day = started_at.astimezone(UTC).date()
    return {(day + timedelta(days=offset)).isoformat() for offset in (-1, 0, 1)}


def repromote_short_runs(
    *,
    target_root: Path,
    index_path: Path | None = None,
    lanes: list[str] | None = None,
    cold_root: Path | None = None,
    min_ratio: float = DEFAULT_MIN_RATIO,
    min_age_hours: float = DEFAULT_MIN_AGE_HOURS,
    limit: int = 1000,
    apply: bool = False,
    parquet_batch_size: int = 50_000,
    now: datetime | None = None,
) -> RepromoteReport:
    checked_at = now or datetime.now(tz=UTC)
    index_path = index_path or (target_root / "_promotion_index.jsonl")
    lane_filter = set(lanes or [])
    latest = latest_index_rows(index_path)
    min_age_cutoff = checked_at - timedelta(hours=max(0.0, float(min_age_hours)))

    runs: list[RepromoteRun] = []
    candidates: list[RepromoteRun] = []
    for run_path in sorted(latest):
        row = latest[run_path]
        lane = Path(run_path).parent.name
        if lane_filter and lane not in lane_filter:
            continue
        promoted_rows = int(row.get("promoted_rows") or 0)
        started_at = _parse_run_started_at(Path(run_path))
        if started_at is not None and started_at > min_age_cutoff:
            continue  # may still be written; the live floor handles it
        raw_dir = resolve_raw_dir(run_path, cold_root)
        if raw_dir is None:
            runs.append(RepromoteRun(run_path, lane, "skipped_raw_missing", promoted_rows))
            continue
        raw_rows = count_clean_rows(raw_dir)
        if not raw_rows:
            continue  # nothing to compare against
        if promoted_rows >= raw_rows * min_ratio:
            continue  # complete enough
        candidates.append(
            RepromoteRun(run_path, lane, "candidate", promoted_rows, raw_rows=raw_rows, raw_dir=str(raw_dir))
        )
    candidates = candidates[: max(0, int(limit))]

    rows_removed = 0
    rows_written = 0
    repromoted_count = 0
    removed_count = 0
    failed_count = 0

    if candidates:
        dates: set[str] = set()
        for cand in candidates:
            started_at = _parse_run_started_at(Path(cand.run_path))
            if started_at is None:
                dates = set()  # unknown date -> scan everything
                break
            dates |= _neighbour_dates(started_at)
        run_files, file_rows, shared_files = build_run_file_map(
            target_root, event_dates=dates or None
        )
        sink = ParquetDatasetSink(target_root, batch_size=parquet_batch_size, fsync_parts=True)
        index_sink = JsonlSink(target_root, index_path.name) if index_path.parent == target_root else JsonlSink(
            index_path.parent, index_path.name
        )

        for cand in candidates:
            files = sorted(run_files.get(cand.run_path, []))
            cand.curated_files = [str(f) for f in files]
            cand.curated_rows = sum(file_rows.get(f, 0) for f in files)
            raw_dir = Path(cand.raw_dir or cand.run_path)
            summary = _read_json_file(raw_dir / "metrics" / "replay_summary.json") or {}
            cand.replayable = bool(summary.get("replayable")) if summary else None

            if any(f in shared_files for f in files):
                cand.action = "skipped_shared_part_file"
                runs.append(cand)
                continue
            if cand.curated_rows != cand.promoted_rows:
                cand.action = "skipped_row_mismatch"
                runs.append(cand)
                continue
            if cand.replayable is None:
                cand.action = "skipped_missing_replay_summary"
                runs.append(cand)
                continue

            planned = "repromote" if cand.replayable else "remove_not_replayable"
            if not apply:
                cand.action = f"would_{planned}"
                runs.append(cand)
                continue

            try:
                for part in files:
                    os.remove(part)
                rows_removed += cand.curated_rows
                new_rows = 0
                if cand.replayable:
                    hot_run = Path(cand.run_path)
                    hot_summary = hot_run / "metrics" / "replay_summary.json"
                    for row in _read_jsonl(raw_dir / "clean" / "events.jsonl"):
                        curated_row = dict(row)
                        curated_row["source_run_path"] = cand.run_path
                        curated_row["replay_summary_path"] = str(hot_summary)
                        curated_row["promotion_checked_at"] = checked_at.isoformat()
                        curated_row["promotion_tag"] = "replayable"
                        sink.write(curated_row)
                        new_rows += 1
                    sink.flush()
                index_sink.write(
                    {
                        "run_path": cand.run_path,
                        "promoted_at": checked_at.isoformat(),
                        "replay_summary_path": str(Path(cand.run_path) / "metrics" / "replay_summary.json"),
                        "promoted_rows": new_rows,
                        "repromoted": bool(cand.replayable),
                        "removed_not_replayable": not cand.replayable,
                        "previous_promoted_rows": cand.promoted_rows,
                        "removed_rows": cand.curated_rows,
                        "raw_dir": str(raw_dir),
                    }
                )
                cand.new_rows = new_rows
                rows_written += new_rows
                cand.action = planned
                if cand.replayable:
                    repromoted_count += 1
                else:
                    removed_count += 1
            except Exception as exc:  # noqa: BLE001 - keep going; the report carries the error
                sink.discard()
                cand.action = "failed"
                cand.error = str(exc)
                failed_count += 1
            runs.append(cand)

    skipped_count = sum(1 for run in runs if run.action.startswith("skipped_"))
    findings: list[str] = []
    if failed_count:
        findings.append("repromote_failures")
    if skipped_count:
        findings.append("skipped_runs")
    return RepromoteReport(
        mode="apply" if apply else "dry_run",
        checked_at=checked_at.isoformat(),
        target_root=str(target_root),
        index_path=str(index_path),
        lanes=sorted(lane_filter),
        index_runs=len(latest),
        candidate_count=len(candidates),
        repromoted_count=repromoted_count,
        removed_count=removed_count,
        skipped_count=skipped_count,
        failed_count=failed_count,
        rows_removed=rows_removed,
        rows_written=rows_written,
        findings=findings,
        runs=runs,
    )
