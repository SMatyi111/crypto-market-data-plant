from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .storage import JsonlSink, write_text_atomic


@dataclass(slots=True)
class QuarantineRunStatus:
    run_path: str
    action: str
    quarantine_dir: str | None
    findings: list[str]
    replayable: bool | None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class QuarantineReport:
    status: str
    checked_at: str
    source_root: str
    quarantine_root: str
    scanned_run_count: int
    quarantined_count: int
    skipped_count: int
    failed_count: int
    findings: list[str]
    runs: list[QuarantineRunStatus]

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "checked_at": self.checked_at,
            "source_root": self.source_root,
            "quarantine_root": self.quarantine_root,
            "scanned_run_count": self.scanned_run_count,
            "quarantined_count": self.quarantined_count,
            "skipped_count": self.skipped_count,
            "failed_count": self.failed_count,
            "findings": self.findings,
            "runs": [run.to_dict() for run in self.runs],
        }


def quarantine_bad_runs(
    source_root: Path,
    quarantine_root: Path,
    *,
    limit: int = 50,
    max_age_hours: float = 24.0,
) -> QuarantineReport:
    checked_at = datetime.now(tz=UTC)
    cutoff = checked_at - timedelta(hours=max_age_hours)
    quarantine_root.mkdir(parents=True, exist_ok=True)
    index_path = quarantine_root / "_quarantine_index.jsonl"
    index_sink = JsonlSink(quarantine_root, "_quarantine_index.jsonl")
    known = _read_known_quarantines(index_path)

    runs: list[QuarantineRunStatus] = []
    quarantined_count = 0
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
                QuarantineRunStatus(
                    run_path=run_key,
                    action="skipped_missing_replay",
                    quarantine_dir=None,
                    findings=["missing_replay_summary"],
                    replayable=None,
                )
            )
            continue

        replayable = bool(replay_summary.get("replayable"))
        findings = [str(item) for item in replay_summary.get("findings", [])]
        if replayable:
            skipped_count += 1
            runs.append(
                QuarantineRunStatus(
                    run_path=run_key,
                    action="skipped_replayable",
                    quarantine_dir=None,
                    findings=findings,
                    replayable=True,
                )
            )
            continue
        if run_key in known:
            skipped_count += 1
            runs.append(
                QuarantineRunStatus(
                    run_path=run_key,
                    action="skipped_quarantined",
                    quarantine_dir=known[run_key],
                    findings=findings,
                    replayable=False,
                )
            )
            continue

        quarantine_dir = quarantine_root / run_dir.name
        try:
            diagnostics = _build_diagnostics_bundle(run_dir, replay_summary)
            quarantine_dir.mkdir(parents=True, exist_ok=True)
            (quarantine_dir / "diagnostics.json").write_text(
                json.dumps(diagnostics, indent=2, sort_keys=True),
                encoding="utf-8",
            )
            index_sink.write(
                {
                    "run_path": run_key,
                    "quarantined_at": checked_at.isoformat(),
                    "quarantine_dir": str(quarantine_dir),
                    "findings": findings,
                }
            )
            known[run_key] = str(quarantine_dir)
            quarantined_count += 1
            runs.append(
                QuarantineRunStatus(
                    run_path=run_key,
                    action="quarantined",
                    quarantine_dir=str(quarantine_dir),
                    findings=findings,
                    replayable=False,
                )
            )
        except Exception as exc:  # noqa: BLE001
            failed_count += 1
            runs.append(
                QuarantineRunStatus(
                    run_path=run_key,
                    action="failed",
                    quarantine_dir=str(quarantine_dir),
                    findings=findings,
                    replayable=False,
                    error=str(exc),
                )
            )

    report_findings: list[str] = []
    status = "ok"
    if failed_count:
        report_findings.append("quarantine_failures")
        status = "error"
    elif quarantined_count == 0:
        report_findings.append("no_quarantine_changes")
        status = "warn"

    return QuarantineReport(
        status=status,
        checked_at=checked_at.isoformat(),
        source_root=str(source_root),
        quarantine_root=str(quarantine_root),
        scanned_run_count=len(runs),
        quarantined_count=quarantined_count,
        skipped_count=skipped_count,
        failed_count=failed_count,
        findings=report_findings,
        runs=runs,
    )


def quarantine_aged_unaccounted_run(
    run_dir: Path,
    *,
    quarantine_index_path: Path,
    checked_at: datetime,
) -> QuarantineRunStatus:
    """Account an aged run that missed both promotion and quarantine.

    This is the preserve-first terminal backstop for interrupted/crash-loop run
    directories. It writes a bounded diagnostics bundle and a quarantine index
    row, but deliberately leaves the raw directory in place. The existing
    verify-copy-delete cold offloader can then relocate the raw tree safely.

    The caller owns the age and index-membership checks. Keeping those checks in
    the offloader lets one scan use the exact same configured promotion and
    quarantine indexes that define ``stuck_unaccounted``.
    """

    run_key = str(run_dir)
    quarantine_root = quarantine_index_path.parent
    quarantine_dir = quarantine_root / run_dir.name
    replay_summary_path = run_dir / "metrics" / "replay_summary.json"
    replay_summary = _read_json_file(replay_summary_path)
    replayable = None if replay_summary is None else bool(replay_summary.get("replayable"))
    findings = ["aged_unaccounted"]
    if replay_summary is None:
        findings.append("missing_replay_summary")
    else:
        findings.extend(str(item) for item in replay_summary.get("findings", []))
        if replayable:
            findings.append("replayable_but_unpromoted")

    try:
        diagnostics = _build_diagnostics_bundle(run_dir, replay_summary)
        diagnostics["classification"] = {
            "classified_at": checked_at.isoformat(),
            "findings": findings,
            "reason": "run remained outside promotion and quarantine indexes beyond the processing window",
        }
        quarantine_dir.mkdir(parents=True, exist_ok=True)
        write_text_atomic(
            quarantine_dir / "diagnostics.json",
            json.dumps(diagnostics, indent=2, sort_keys=True),
            fsync=True,
        )
        JsonlSink(quarantine_root, quarantine_index_path.name).write(
            {
                "run_path": run_key,
                "quarantined_at": checked_at.isoformat(),
                "quarantine_dir": str(quarantine_dir),
                "findings": findings,
                "classification": "aged_unaccounted",
            }
        )
        return QuarantineRunStatus(
            run_path=run_key,
            action="quarantined_aged_unaccounted",
            quarantine_dir=str(quarantine_dir),
            findings=findings,
            replayable=replayable,
        )
    except Exception as exc:  # noqa: BLE001
        return QuarantineRunStatus(
            run_path=run_key,
            action="failed_aged_unaccounted",
            quarantine_dir=str(quarantine_dir),
            findings=findings,
            replayable=replayable,
            error=str(exc),
        )


def _build_diagnostics_bundle(
    run_dir: Path, replay_summary: dict[str, Any] | None
) -> dict[str, Any]:
    events_path = run_dir / "clean" / "events.jsonl"
    raw_path = run_dir / "raw" / "messages.jsonl"
    summary_path = run_dir / "metrics" / "summary.jsonl"
    return {
        "run_path": str(run_dir),
        "replay_summary": replay_summary,
        # Interrupted runs can contain multi-GB raw files. Diagnostics are evidence,
        # not a second archive: stream only a bounded prefix instead of loading the
        # whole file before slicing it.
        "metrics_summary": _read_jsonl_sample(summary_path, limit=20),
        "clean_sample": _read_jsonl_sample(events_path, limit=5),
        "raw_sample": _read_jsonl_sample(raw_path, limit=5),
    }


def _read_known_quarantines(path: Path) -> dict[str, str]:
    known: dict[str, str] = {}
    if not path.exists():
        return known
    for row in _read_jsonl(path):
        run_path = row.get("run_path")
        quarantine_dir = row.get("quarantine_dir")
        if run_path and quarantine_dir:
            known[str(run_path)] = str(quarantine_dir)
    return known


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


def _read_jsonl_sample(path: Path, *, limit: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists() or limit <= 0:
        return rows
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                rows.append({"_diagnostic_error": "invalid_json"})
            if len(rows) >= limit:
                break
    return rows
