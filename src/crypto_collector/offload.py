"""Cold-tier offload for raw run directories.

Moves *accounted-for* raw run dirs (promoted into curated parquet, or quarantined
with a diagnostics bundle) older than a retention window from the hot archive
(NVMe) to a cold archive on another disk, freeing the hot disk while keeping raw
forever. The hot raw tree stays bounded at ~retention-window days.

Safety model — a run dir is only ever deleted from the hot tier when BOTH hold:

* it appears in its lane's `_promotion_index.jsonl` (the promoter flushes parquet
  rows durably BEFORE writing the index entry, so an index hit implies the curated
  copy is on disk) OR in the lane's `_quarantine_index.jsonl` (known-bad, has a
  diagnostics bundle); and
* a byte-identical copy (per-file relative paths + sizes) exists in the cold tier.

Old run dirs in NEITHER index are never touched; they are surfaced as
`stuck_unaccounted_runs` findings so a silently-stalled scorer/promoter shows up
in the offload report instead of aging data into deletion (the trap a purely
age-based cleanup has).
"""

from __future__ import annotations

import json
import os
import shutil
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from math import isfinite
from pathlib import Path
from typing import Any

from .storage import JsonlSink

# Cap how many stuck (old-but-unaccounted) run paths are listed per lane in the
# report. The COUNT is always exact; this only bounds the example paths so a
# stalled lane with thousands of unscored runs doesn't balloon the job log.
_STUCK_EXAMPLE_CAP = 5

# Staging directory inside each cold lane dir. A copy lands here first and is
# renamed into place only after it verifies, so a crash mid-copy can never leave
# a half-written run dir that looks final.
_PARTIAL_DIRNAME = ".offload_partial"

OFFLOAD_INDEX_FILENAME = "_offload_index.jsonl"

# Latest offload report, persisted to the ops root after every execution so the
# health surface can read offload state. The report object itself used to be
# discarded after a one-line job log entry, which hid a 14k-run stuck cohort
# behind "all jobs success" for a week (2026-07-04 audit).
OFFLOAD_REPORT_FILENAME = "offload_report_latest.json"


@dataclass(slots=True)
class OffloadLaneSpec:
    """One raw lane eligible for offload.

    Index paths are explicit (not derived from the lane name) on purpose: lane-name
    convention drift has already produced one real bug in the offline scorer's venue
    derivation, and the promote jobs in the same ops config carry these exact paths
    anyway, so the config is the single source of truth.

    gate="indexed" (default) offloads only promoted/quarantined runs and flags the
    rest as stuck — for lanes whose curation runs as separate score/promote jobs.
    gate="age_only" offloads every sufficiently old run — for lanes with no per-run
    promotion index because curation happens at write time (kalshi normalizes inline)
    or the lane is dead (delisted venue) and nothing will ever promote it. Offload
    verifies the cold copy before deleting either way, so age_only relocates data,
    it cannot lose it.

    min_age_days overrides the job-level age for this lane only (e.g. the Kalshi
    quote lane rotates to cold after 3 days because its curation is inline and
    nothing downstream needs the raw hot). A per-lane override keeps a single job
    owning the whole raw_root — a second job over the same root would warn
    `unconfigured_lane` for every dir it doesn't own, forever.
    """

    source: str
    promotion_index: Path | None = None
    quarantine_index: Path | None = None
    gate: str = "indexed"
    min_age_days: float | None = None

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> OffloadLaneSpec:
        source = str(raw.get("source") or "").strip()
        gate = str(raw.get("gate") or "indexed")
        if gate not in ("indexed", "age_only"):
            raise ValueError(f"offload lane gate must be 'indexed' or 'age_only': {raw!r}")
        promotion_index = raw.get("promotion_index")
        if not source or (gate == "indexed" and not promotion_index):
            raise ValueError(
                f"offload lane needs 'source' (and 'promotion_index' unless gate=age_only): {raw!r}"
            )
        raw_age = raw.get("min_age_days")
        min_age_days: float | None = None
        if raw_age is not None:
            if (
                isinstance(raw_age, bool)
                or not isinstance(raw_age, (int, float))
                or not isfinite(raw_age)
                or raw_age <= 0
            ):
                raise ValueError(
                    f"offload lane min_age_days must be a positive number: {raw!r}"
                )
            min_age_days = float(raw_age)
        quarantine_index = raw.get("quarantine_index")
        return cls(
            source=source,
            promotion_index=Path(promotion_index) if promotion_index else None,
            quarantine_index=Path(quarantine_index) if quarantine_index else None,
            gate=gate,
            min_age_days=min_age_days,
        )


@dataclass(slots=True)
class OffloadRunStatus:
    run_path: str
    lane: str
    action: str
    cold_path: str | None = None
    bytes: int = 0
    file_count: int = 0
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class OffloadLaneStatus:
    source: str
    min_age_days: float | None = None  # effective age for this lane (override or job default)
    scanned_count: int = 0
    eligible_count: int = 0
    moved_count: int = 0
    stuck_unaccounted_count: int = 0
    stuck_examples: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class OffloadReport:
    status: str
    mode: str
    checked_at: str
    raw_root: str
    cold_root: str
    min_age_days: float
    scanned_run_count: int
    eligible_count: int
    moved_count: int
    moved_bytes: int
    failed_count: int
    stuck_unaccounted_count: int
    findings: list[str]
    lanes: list[OffloadLaneStatus]
    runs: list[OffloadRunStatus]

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "mode": self.mode,
            "checked_at": self.checked_at,
            "raw_root": self.raw_root,
            "cold_root": self.cold_root,
            "min_age_days": self.min_age_days,
            "scanned_run_count": self.scanned_run_count,
            "eligible_count": self.eligible_count,
            "moved_count": self.moved_count,
            "moved_bytes": self.moved_bytes,
            "failed_count": self.failed_count,
            "stuck_unaccounted_count": self.stuck_unaccounted_count,
            "findings": self.findings,
            "lanes": [lane.to_dict() for lane in self.lanes],
            "runs": [run.to_dict() for run in self.runs],
        }


def write_offload_report_latest(report: OffloadReport, ops_root: Path) -> Path:
    """Atomically persist ``report`` as ``<ops_root>/offload_report_latest.json``.

    Temp-file + rename (the ops-root convention, mirroring the runner's heartbeat
    writer): the health check may read this file at any moment, so a torn or
    half-written JSON must be impossible. The rename is retried briefly because
    AV/backup tooling on Windows can hold the target open transiently.
    """
    path = ops_root / OFFLOAD_REPORT_FILENAME
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    temp_path.write_text(
        json.dumps(report.to_dict(), indent=2, sort_keys=True), encoding="utf-8"
    )
    for attempt in range(5):
        try:
            temp_path.replace(path)
            break
        except PermissionError:
            if attempt == 4:
                raise
            time.sleep(0.02 * (attempt + 1))
    return path


def offload_accounted_runs(
    *,
    raw_root: Path,
    cold_root: Path,
    lanes: list[OffloadLaneSpec],
    min_age_days: float = 14.0,
    limit: int = 200,
    apply: bool = False,
) -> OffloadReport:
    checked_at = datetime.now(tz=UTC)

    findings: list[str] = []
    lane_statuses: list[OffloadLaneStatus] = []
    runs: list[OffloadRunStatus] = []
    scanned_run_count = 0
    eligible_count = 0
    moved_count = 0
    moved_bytes = 0
    failed_count = 0
    stuck_total = 0
    budget = max(0, int(limit))

    # A lane that exists on disk but is absent from the config would silently hoard
    # the hot disk forever (the same shape as the collector-concurrency starvation
    # trap: config grows, companion setting doesn't). Surface it loudly.
    configured = {lane.source for lane in lanes}
    if raw_root.exists():
        for child in sorted(raw_root.iterdir()):
            if not child.is_dir() or child.name.startswith("_"):
                continue
            if child.name not in configured:
                findings.append(f"unconfigured_lane:{child.name}")

    index_cache: dict[str, set[str]] = {}
    index_sink: JsonlSink | None = None

    for lane in lanes:
        lane_age_days = lane.min_age_days if lane.min_age_days is not None else min_age_days
        lane_cutoff = checked_at - timedelta(days=lane_age_days)
        lane_status = OffloadLaneStatus(source=lane.source, min_age_days=lane_age_days)
        lane_statuses.append(lane_status)
        lane_raw = raw_root / lane.source
        if not lane_raw.exists():
            findings.append(f"missing_lane_dir:{lane.source}")
            continue

        accounted: set[str] | None = None
        if lane.gate == "indexed":
            # No promotion index on an indexed lane means nothing can be proven
            # promoted => every old run reports stuck rather than anything moving.
            accounted = set()
            if lane.promotion_index is not None:
                accounted |= _load_index_run_paths(lane.promotion_index, index_cache)
            if lane.quarantine_index is not None:
                accounted |= _load_index_run_paths(lane.quarantine_index, index_cache)

        # Oldest first so a backlog drains deterministically from the far end.
        for run_dir in sorted(path for path in lane_raw.iterdir() if path.is_dir()):
            started_at = _parse_run_started_at(run_dir)
            if started_at is None or started_at >= lane_cutoff:
                continue
            scanned_run_count += 1
            lane_status.scanned_count += 1
            run_key = str(run_dir)

            if accounted is not None and run_key not in accounted:
                stuck_total += 1
                lane_status.stuck_unaccounted_count += 1
                if len(lane_status.stuck_examples) < _STUCK_EXAMPLE_CAP:
                    lane_status.stuck_examples.append(run_key)
                continue

            if budget <= 0:
                continue
            budget -= 1
            eligible_count += 1
            lane_status.eligible_count += 1

            if not apply:
                size, count = _measure_dir(run_dir)
                runs.append(
                    OffloadRunStatus(
                        run_path=run_key,
                        lane=lane.source,
                        action="would_move",
                        cold_path=str(cold_root / lane.source / run_dir.name),
                        bytes=size,
                        file_count=count,
                    )
                )
                continue

            if index_sink is None:
                cold_root.mkdir(parents=True, exist_ok=True)
                index_sink = JsonlSink(cold_root, OFFLOAD_INDEX_FILENAME)
            status = _move_run_dir(
                run_dir=run_dir,
                lane=lane.source,
                cold_lane_root=cold_root / lane.source,
                index_sink=index_sink,
                checked_at=checked_at,
            )
            runs.append(status)
            if status.error is not None:
                failed_count += 1
            else:
                moved_count += 1
                moved_bytes += status.bytes
                lane_status.moved_count += 1

    if stuck_total:
        findings.append(f"stuck_unaccounted_runs:{stuck_total}")
    if eligible_count == 0:
        findings.append("no_offload_candidates")

    status = "ok"
    if any(finding.startswith(("stuck_unaccounted_runs", "unconfigured_lane", "missing_lane_dir")) for finding in findings):
        status = "warn"
    if failed_count:
        status = "error"

    return OffloadReport(
        status=status,
        mode="apply" if apply else "dry-run",
        checked_at=checked_at.isoformat(),
        raw_root=str(raw_root),
        cold_root=str(cold_root),
        min_age_days=min_age_days,
        scanned_run_count=scanned_run_count,
        eligible_count=eligible_count,
        moved_count=moved_count,
        moved_bytes=moved_bytes,
        failed_count=failed_count,
        stuck_unaccounted_count=stuck_total,
        findings=findings,
        lanes=lane_statuses,
        runs=runs,
    )


def _move_run_dir(
    *,
    run_dir: Path,
    lane: str,
    cold_lane_root: Path,
    index_sink: JsonlSink,
    checked_at: datetime,
) -> OffloadRunStatus:
    run_key = str(run_dir)
    final_target = cold_lane_root / run_dir.name
    try:
        source_manifest = _file_manifest(run_dir)

        if final_target.exists():
            # Resume: a previous cycle copied (and possibly indexed) this run but died
            # before deleting the source. Re-verify against the source before deleting —
            # a name collision with different content must never cost the hot copy.
            cold_manifest = _file_manifest(final_target)
            if cold_manifest != source_manifest:
                # A crash mid-rmtree leaves a partially-deleted source: every
                # remaining source file still matches the verified cold copy, the
                # source is just a strict subset. That's a resumable partial delete
                # — finish it. Anything else (extra or size-changed source files)
                # is a genuine collision and the hot copy is kept. Without the
                # subset check, one interrupted delete wedged the run in a
                # permanent cold_target_mismatch that errored every later pass.
                # (vacuously true for an empty leftover dir skeleton — rmtree got
                # them all; index-before-delete means its row was already written)
                is_partial_delete = all(
                    cold_manifest.get(name) == size
                    for name, size in source_manifest.items()
                )
                if not is_partial_delete:
                    return OffloadRunStatus(
                        run_path=run_key,
                        lane=lane,
                        action="cold_target_mismatch",
                        cold_path=str(final_target),
                        error="cold target exists with different content; source kept",
                    )
            # Index BEFORE deleting, mirroring the fresh-move ordering. A previous
            # cycle that died between its rename and its index write left the run
            # cold-only with NO index row — breaking the "raw remains locatable
            # after offload" contract (STANDARDS §7). A duplicate row in the
            # append-only index (prior crash was after the write) is harmless;
            # a missing one is not.
            size = sum(cold_manifest.values())
            index_sink.write(
                {
                    "run_path": run_key,
                    "cold_path": str(final_target),
                    "lane": lane,
                    "moved_at": checked_at.isoformat(),
                    "bytes": size,
                    "file_count": len(cold_manifest),
                    "resumed": True,
                }
            )
            shutil.rmtree(run_dir)
            return OffloadRunStatus(
                run_path=run_key,
                lane=lane,
                action="resumed_delete",
                cold_path=str(final_target),
                bytes=size,
                file_count=len(cold_manifest),
            )

        partial_target = cold_lane_root / _PARTIAL_DIRNAME / run_dir.name
        if partial_target.exists():
            shutil.rmtree(partial_target)  # stale half-copy from a crashed cycle
        partial_target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(run_dir, partial_target)

        if _file_manifest(partial_target) != source_manifest:
            shutil.rmtree(partial_target)
            return OffloadRunStatus(
                run_path=run_key,
                lane=lane,
                action="copy_verify_failed",
                cold_path=str(partial_target),
                error="copied tree did not match source; source kept",
            )

        partial_target.rename(final_target)
        size = sum(source_manifest.values())
        # Defense-in-depth re-verify BEFORE the index write and the destructive
        # step: a writer appending to the source AFTER the copy completed (a zombie
        # worker writing into an aged segment) would otherwise lose the appended
        # tail silently — and indexing first would record a move that didn't
        # complete, wedging the run in cold_target_mismatch on every later pass.
        # On abort the just-renamed cold copy is removed too (nothing references
        # it: no index row, source kept), so the next pass re-copies the grown
        # source cleanly. One stat pass per moved run.
        if _file_manifest(run_dir) != source_manifest:
            shutil.rmtree(final_target, ignore_errors=True)
            return OffloadRunStatus(
                run_path=run_key,
                lane=lane,
                action="source_changed_during_move",
                cold_path=str(final_target),
                error="source changed between copy and delete; source kept",
            )
        # Index BEFORE deleting the source: an index entry must imply the cold copy is
        # in place, and a crash between index and delete resumes safely above.
        index_sink.write(
            {
                "run_path": run_key,
                "cold_path": str(final_target),
                "lane": lane,
                "moved_at": checked_at.isoformat(),
                "bytes": size,
                "file_count": len(source_manifest),
            }
        )
        shutil.rmtree(run_dir)
        return OffloadRunStatus(
            run_path=run_key,
            lane=lane,
            action="moved",
            cold_path=str(final_target),
            bytes=size,
            file_count=len(source_manifest),
        )
    except Exception as exc:  # noqa: BLE001
        return OffloadRunStatus(
            run_path=run_key,
            lane=lane,
            action="failed",
            cold_path=str(final_target),
            error=str(exc),
        )


def _file_manifest(root: Path) -> dict[str, int]:
    """Relative path -> size for every file under root. Size+path equality is the
    verification bar: cheap enough to run on every move, and a truncated or missing
    file (the realistic copy-failure modes) always changes it."""
    manifest: dict[str, int] = {}
    for path in root.rglob("*"):
        if path.is_file():
            manifest[path.relative_to(root).as_posix()] = path.stat().st_size
    return manifest


def _measure_dir(root: Path) -> tuple[int, int]:
    manifest = _file_manifest(root)
    return sum(manifest.values()), len(manifest)


def _load_index_run_paths(path: Path, cache: dict[str, set[str]]) -> set[str]:
    key = str(path)
    if key in cache:
        return cache[key]
    run_paths: set[str] = set()
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            run_path = row.get("run_path")
            if run_path:
                run_paths.add(str(run_path))
    cache[key] = run_paths
    return run_paths


def _parse_run_started_at(path: Path) -> datetime | None:
    try:
        return datetime.strptime(path.name, "%Y%m%d_%H%M%S").replace(tzinfo=UTC)
    except ValueError:
        return None
