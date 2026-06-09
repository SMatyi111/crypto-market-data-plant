from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .storage import JsonlSink, ParquetDatasetSink


@dataclass(slots=True)
class PromotionRunStatus:
    run_path: str
    action: str
    promoted_rows: int
    replayable: bool | None
    findings: list[str]
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class PromotionReport:
    status: str
    checked_at: str
    source_root: str
    target_root: str
    scanned_run_count: int
    promoted_run_count: int
    promoted_row_count: int
    skipped_count: int
    failed_count: int
    findings: list[str]
    runs: list[PromotionRunStatus]

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "checked_at": self.checked_at,
            "source_root": self.source_root,
            "target_root": self.target_root,
            "scanned_run_count": self.scanned_run_count,
            "promoted_run_count": self.promoted_run_count,
            "promoted_row_count": self.promoted_row_count,
            "skipped_count": self.skipped_count,
            "failed_count": self.failed_count,
            "findings": self.findings,
            "runs": [run.to_dict() for run in self.runs],
        }


def promote_replayable_runs(
    source_root: Path,
    target_root: Path,
    *,
    limit: int = 50,
    max_age_hours: float = 24.0,
    quarantine_index_path: Path | None = None,
    parquet_batch_size: int = 50_000,
) -> PromotionReport:
    checked_at = datetime.now(tz=UTC)
    cutoff = checked_at - timedelta(hours=max_age_hours)
    target_root.mkdir(parents=True, exist_ok=True)
    index_path = target_root / "_promotion_index.jsonl"
    promoted_runs = _read_promoted_runs(index_path)
    quarantined_runs = _read_quarantined_runs(quarantine_index_path)
    index_sink = JsonlSink(target_root, "_promotion_index.jsonl")
    # Promotion writes whole runs and flushes per-run (see the explicit flush before
    # each index write below), so the buffer only needs to hold one run's rows. The
    # sink's default batch_size (100) would auto-flush mid-run — for a 5k-row run that
    # is ~50 tiny part-files PER run, which both fragments the dataset and slows
    # write_to_dataset as the partition dir fills (a backfill of thousands of runs can
    # stall on it). Sizing the buffer past a run's row count means exactly one flush
    # (≈one part-file) per run, with the same per-run durability guarantee.
    parquet_sink = ParquetDatasetSink(target_root, batch_size=parquet_batch_size)

    runs: list[PromotionRunStatus] = []
    promoted_run_count = 0
    promoted_row_count = 0
    skipped_count = 0
    failed_count = 0

    for run_dir in _recent_run_dirs(source_root, limit=limit):
        started_at = _parse_run_started_at(run_dir)
        if started_at is not None and started_at < cutoff:
            continue
        run_key = str(run_dir)
        replay_summary_path = run_dir / "metrics" / "replay_summary.json"
        replay_summary = _read_json_file(replay_summary_path)
        if replay_summary is None:
            skipped_count += 1
            runs.append(
                PromotionRunStatus(
                    run_path=run_key,
                    action="skipped_missing_replay",
                    promoted_rows=0,
                    replayable=None,
                    findings=["missing_replay_summary"],
                )
            )
            continue

        replayable = bool(replay_summary.get("replayable"))
        findings = [str(item) for item in replay_summary.get("findings", [])]
        if run_key in quarantined_runs:
            skipped_count += 1
            runs.append(
                PromotionRunStatus(
                    run_path=run_key,
                    action="skipped_quarantined",
                    promoted_rows=0,
                    replayable=replayable,
                    findings=["quarantined_run", *findings],
                )
            )
            continue

        if not replayable:
            skipped_count += 1
            runs.append(
                PromotionRunStatus(
                    run_path=run_key,
                    action="skipped_unreplayable",
                    promoted_rows=0,
                    replayable=False,
                    findings=findings,
                )
            )
            continue

        if run_key in promoted_runs:
            skipped_count += 1
            runs.append(
                PromotionRunStatus(
                    run_path=run_key,
                    action="skipped_promoted",
                    promoted_rows=0,
                    replayable=True,
                    findings=findings,
                )
            )
            continue

        events_path = run_dir / "clean" / "events.jsonl"
        if not events_path.exists():
            skipped_count += 1
            runs.append(
                PromotionRunStatus(
                    run_path=run_key,
                    action="skipped_missing_events",
                    promoted_rows=0,
                    replayable=True,
                    findings=findings,
                )
            )
            continue

        try:
            rows = _read_jsonl(events_path)
            promoted_rows = 0
            for row in rows:
                curated_row = dict(row)
                curated_row["source_run_path"] = run_key
                curated_row["replay_summary_path"] = str(replay_summary_path)
                curated_row["promotion_checked_at"] = checked_at.isoformat()
                curated_row["promotion_tag"] = "replayable"
                parquet_sink.write(curated_row)
                promoted_rows += 1
            # Flush per-run BEFORE appending to the index. An index entry must imply
            # the Parquet rows for that run are durably on disk: otherwise a flush
            # failure after the index write would leave the index claiming a run was
            # promoted while the rows were silently dropped on retry.
            parquet_sink.flush()
            index_sink.write(
                {
                    "run_path": run_key,
                    "promoted_at": checked_at.isoformat(),
                    "replay_summary_path": str(replay_summary_path),
                    "promoted_rows": promoted_rows,
                }
            )
            promoted_runs.add(run_key)
            promoted_run_count += 1
            promoted_row_count += promoted_rows
            runs.append(
                PromotionRunStatus(
                    run_path=run_key,
                    action="promoted",
                    promoted_rows=promoted_rows,
                    replayable=True,
                    findings=findings,
                )
            )
        except Exception as exc:  # noqa: BLE001
            # Best-effort flush of any partial rows already written for this run so we
            # don't lose them silently on subsequent failures.
            try:
                parquet_sink.flush()
            except Exception:  # noqa: BLE001
                pass
            failed_count += 1
            runs.append(
                PromotionRunStatus(
                    run_path=run_key,
                    action="failed",
                    promoted_rows=0,
                    replayable=True,
                    findings=findings,
                    error=str(exc),
                )
            )

    parquet_sink.flush()
    report_findings: list[str] = []
    status = "ok"
    if failed_count:
        report_findings.append("promotion_failures")
        status = "error"
    elif promoted_run_count == 0:
        report_findings.append("no_promotion_changes")
        status = "warn"

    return PromotionReport(
        status=status,
        checked_at=checked_at.isoformat(),
        source_root=str(source_root),
        target_root=str(target_root),
        scanned_run_count=len(runs),
        promoted_run_count=promoted_run_count,
        promoted_row_count=promoted_row_count,
        skipped_count=skipped_count,
        failed_count=failed_count,
        findings=report_findings,
        runs=runs,
    )


def _read_promoted_runs(path: Path) -> set[str]:
    promoted: set[str] = set()
    if not path.exists():
        return promoted
    for row in _read_jsonl(path):
        run_path = row.get("run_path")
        if run_path:
            promoted.add(str(run_path))
    return promoted


def _read_quarantined_runs(path: Path | None) -> set[str]:
    quarantined: set[str] = set()
    if path is None or not path.exists():
        return quarantined
    for row in _read_jsonl(path):
        run_path = row.get("run_path")
        if run_path:
            quarantined.add(str(run_path))
    return quarantined


def _recent_run_dirs(source_root: Path, *, limit: int) -> list[Path]:
    if not source_root.exists():
        return []
    run_dirs = [path for path in source_root.iterdir() if path.is_dir()]
    return sorted(run_dirs, key=lambda path: path.name, reverse=True)[:limit]


def _parse_run_started_at(path: Path) -> datetime | None:
    try:
        return datetime.strptime(path.name, "%Y%m%d_%H%M%S").replace(tzinfo=UTC)
    except ValueError:
        return None


def _read_json_file(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows
