from __future__ import annotations

import json
import os
import socket
import sys
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
import shutil
import threading
import time
from typing import Any, Callable

from .config import default_normalized_root
from .storage import JsonlSink


@dataclass(slots=True)
class JobSpec:
    name: str
    job_type: str
    interval_seconds: int
    args: dict[str, Any] = field(default_factory=dict)
    enabled: bool = True

    @classmethod
    def from_dict(cls, row: dict[str, Any]) -> "JobSpec":
        return cls(
            name=str(row["name"]),
            job_type=str(row["job_type"]),
            interval_seconds=int(row["interval_seconds"]),
            args=dict(row.get("args", {})),
            enabled=bool(row.get("enabled", True)),
        )


@dataclass(slots=True)
class JobRunResult:
    job_name: str
    job_type: str
    started_at: datetime
    finished_at: datetime
    status: str
    message: str | None = None
    retry_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        row["started_at"] = self.started_at.isoformat()
        row["finished_at"] = self.finished_at.isoformat()
        return row


@dataclass(slots=True)
class JobExecutionResult:
    message: str | None = None
    retry_count: int = 0


def load_ops_config(path: Path) -> list[JobSpec]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    jobs = [JobSpec.from_dict(row) for row in payload.get("jobs", [])]
    return [job for job in jobs if job.enabled]


@dataclass(slots=True)
class HealthReport:
    status: str
    checked_at: datetime
    heartbeat_age_seconds: float | None
    disk_free_gb: float | None
    disk_free_pct: float | None
    findings: list[str]
    jobs: list[dict[str, Any]]
    standalone_workers: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "checked_at": self.checked_at.isoformat(),
            "heartbeat_age_seconds": self.heartbeat_age_seconds,
            "disk_free_gb": self.disk_free_gb,
            "disk_free_pct": self.disk_free_pct,
            "findings": self.findings,
            "jobs": self.jobs,
            "standalone_workers": self.standalone_workers,
        }


@dataclass(slots=True)
class CleanupCandidate:
    path: str
    reason: str
    bytes: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class CleanupReport:
    mode: str
    checked_at: datetime
    candidate_count: int
    total_bytes: int
    removed_count: int
    removed_bytes: int
    candidates: list[CleanupCandidate]

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "checked_at": self.checked_at.isoformat(),
            "candidate_count": self.candidate_count,
            "total_bytes": self.total_bytes,
            "removed_count": self.removed_count,
            "removed_bytes": self.removed_bytes,
            "candidates": [candidate.to_dict() for candidate in self.candidates],
        }


@dataclass(slots=True)
class StaleWorkerPruneCandidate:
    worker_name: str
    heartbeat_path: str
    age_seconds: float | None
    status: str
    reason: str
    related_paths: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class StaleWorkerPruneReport:
    mode: str
    checked_at: datetime
    ops_root: str
    archive_root: str
    candidate_count: int
    moved_count: int
    findings: list[str]
    candidates: list[StaleWorkerPruneCandidate]

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "checked_at": self.checked_at.isoformat(),
            "ops_root": self.ops_root,
            "archive_root": self.archive_root,
            "candidate_count": self.candidate_count,
            "moved_count": self.moved_count,
            "findings": self.findings,
            "candidates": [candidate.to_dict() for candidate in self.candidates],
        }


class OpsRunnerLock:
    def __init__(self, ops_root: Path, *, runner_name: str) -> None:
        self.ops_root = ops_root
        self.runner_name = runner_name
        self.lock_path = self.ops_root / "ops-runner.lock"

    def acquire(self) -> None:
        self.ops_root.mkdir(parents=True, exist_ok=True)
        while True:
            try:
                fd = os.open(
                    self.lock_path,
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                )
            except FileExistsError:
                owner = _read_json_file(self.lock_path)
                owner_pid = _read_pid(owner)
                if owner_pid is not None and _pid_exists(owner_pid):
                    raise RuntimeError(
                        f"ops runner already active for {self.ops_root} "
                        f"(pid={owner_pid}, runner={owner.get('runner_name', 'unknown')})"
                    )
                self.lock_path.unlink(missing_ok=True)
                continue
            payload = {
                "runner_name": self.runner_name,
                "pid": os.getpid(),
                "hostname": socket.gethostname(),
                "created_at": datetime.now(tz=UTC).isoformat(),
            }
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2, sort_keys=True)
            return

    def release(self) -> None:
        self.lock_path.unlink(missing_ok=True)

    def __enter__(self) -> "OpsRunnerLock":
        self.acquire()
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> None:
        self.release()


class StandaloneWorkerLock:
    def __init__(self, ops_root: Path, *, worker_name: str) -> None:
        self.ops_root = ops_root
        self.worker_name = worker_name
        self.lock_root = self.ops_root / "standalone_workers"
        self.lock_path = self.lock_root / f"{self.worker_name}.lock"

    def acquire(self) -> None:
        self.lock_root.mkdir(parents=True, exist_ok=True)
        while True:
            try:
                fd = os.open(
                    self.lock_path,
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                )
            except FileExistsError:
                owner = _read_json_file(self.lock_path)
                owner_pid = _read_pid(owner)
                if owner_pid is not None and _pid_exists(owner_pid):
                    raise RuntimeError(
                        f"standalone worker already active for {self.worker_name} "
                        f"(pid={owner_pid})"
                    )
                self.lock_path.unlink(missing_ok=True)
                continue
            payload = {
                "worker_name": self.worker_name,
                "pid": os.getpid(),
                "hostname": socket.gethostname(),
                "created_at": datetime.now(tz=UTC).isoformat(),
            }
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2, sort_keys=True)
            return

    def release(self) -> None:
        self.lock_path.unlink(missing_ok=True)

    def __enter__(self) -> "StandaloneWorkerLock":
        self.acquire()
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> None:
        self.release()


class StandaloneWorkerRuntime:
    def __init__(
        self,
        ops_root: Path,
        *,
        worker_name: str,
        worker_type: str,
        venue: str,
        symbol: str,
        heartbeat_interval_seconds: float = 30.0,
    ) -> None:
        self.ops_root = ops_root
        self.worker_name = worker_name
        self.worker_type = worker_type
        self.venue = venue
        self.symbol = symbol
        self.heartbeat_interval_seconds = heartbeat_interval_seconds
        self.workers_root = self.ops_root / "standalone_workers"
        self.workers_root.mkdir(parents=True, exist_ok=True)
        self.heartbeat_path = self.workers_root / f"{self.worker_name}.json"
        self.events_sink = JsonlSink(self.ops_root, "worker_events.jsonl")
        self._heartbeat_lock = threading.Lock()

    def record_event(self, event_type: str, *, details: dict[str, Any] | None = None) -> None:
        payload = {
            "event_time": datetime.now(tz=UTC).isoformat(),
            "event_type": event_type,
            "scope": "standalone_worker",
            "worker_name": self.worker_name,
            "worker_type": self.worker_type,
            "venue": self.venue,
            "symbol": self.symbol,
            "pid": os.getpid(),
            "hostname": socket.gethostname(),
        }
        if details:
            payload["details"] = details
        self.events_sink.write(payload)

    def write_heartbeat(
        self,
        *,
        status: str,
        message: str | None = None,
        last_segment_index: int | None = None,
        current_segment_index: int | None = None,
        current_segment_started_at: datetime | None = None,
        current_run_path: str | None = None,
        last_run_path: str | None = None,
    ) -> None:
        with self._heartbeat_lock:
            payload = {
                "worker_name": self.worker_name,
                "worker_type": self.worker_type,
                "venue": self.venue,
                "symbol": self.symbol,
                "pid": os.getpid(),
                "hostname": socket.gethostname(),
                "status": status,
                "last_seen": datetime.now(tz=UTC).isoformat(),
                "message": message,
                "last_segment_index": last_segment_index,
                "current_segment": (
                    {
                        "index": current_segment_index,
                        "started_at": current_segment_started_at.isoformat()
                        if current_segment_started_at is not None
                        else None,
                        "run_path": current_run_path,
                    }
                    if current_segment_index is not None
                    else None
                ),
                "last_run_path": last_run_path,
            }
            _write_json_atomic(self.heartbeat_path, payload)

    def start_segment_heartbeat(
        self,
        *,
        segment_index: int,
        started_at: datetime,
        run_path: str | None = None,
        last_segment_index: int | None = None,
        last_run_path: str | None = None,
    ) -> tuple[threading.Event, threading.Thread]:
        stop_event = threading.Event()
        interval_seconds = max(0.1, float(self.heartbeat_interval_seconds))

        def refresh() -> None:
            self.write_heartbeat(
                status="running",
                last_segment_index=last_segment_index,
                current_segment_index=segment_index,
                current_segment_started_at=started_at,
                current_run_path=run_path,
                last_run_path=last_run_path,
            )
            while not stop_event.wait(interval_seconds):
                self.write_heartbeat(
                    status="running",
                    last_segment_index=last_segment_index,
                    current_segment_index=segment_index,
                    current_segment_started_at=started_at,
                    current_run_path=run_path,
                    last_run_path=last_run_path,
                )

        thread = threading.Thread(
            target=refresh,
            name=f"worker-heartbeat-{self.worker_name}",
            daemon=True,
        )
        thread.start()
        return stop_event, thread


class OpsRunner:
    def __init__(
        self,
        ops_root: Path,
        *,
        runner_name: str = "collector-ops",
        poll_seconds: int = 5,
        heartbeat_interval_seconds: float = 30.0,
    ) -> None:
        self.ops_root = ops_root
        self.runner_name = runner_name
        self.poll_seconds = poll_seconds
        self.heartbeat_interval_seconds = heartbeat_interval_seconds
        self.ops_root.mkdir(parents=True, exist_ok=True)
        self.runs_sink = JsonlSink(self.ops_root, "job_runs.jsonl")
        self.heartbeat_history = JsonlSink(self.ops_root, "heartbeat_history.jsonl")
        self.heartbeat_path = self.ops_root / "heartbeat.json"
        self._last_heartbeat_status: str | None = None
        self._last_heartbeat_write: datetime | None = None
        self._job_counters: dict[str, dict[str, Any]] = {}
        self._heartbeat_lock = threading.Lock()

    def run(
        self,
        jobs: list[JobSpec],
        *,
        execute_job: Callable[[JobSpec], JobExecutionResult | str | None],
        max_runs: int | None = None,
        stop_on_error: bool = False,
    ) -> int:
        run_count = 0
        next_run_at = {job.name: datetime.now(tz=UTC) for job in jobs}
        self._write_heartbeat(status="starting", run_count=run_count, jobs=jobs, next_run_at=next_run_at)

        while max_runs is None or run_count < max_runs:
            now = datetime.now(tz=UTC)
            due_jobs = [job for job in jobs if now >= next_run_at[job.name]]
            if not due_jobs:
                self._write_heartbeat(
                    status="idle",
                    run_count=run_count,
                    jobs=jobs,
                    next_run_at=next_run_at,
                )
                time.sleep(self.poll_seconds if self.poll_seconds > 0 else 0.1)
                continue

            for job in due_jobs:
                started_at = datetime.now(tz=UTC)
                status = "success"
                message: str | None = None
                retry_count = 0
                heartbeat_stop = threading.Event()
                heartbeat_thread = self._start_running_heartbeat(
                    stop_event=heartbeat_stop,
                    run_count=run_count,
                    jobs=jobs,
                    next_run_at=next_run_at,
                    job=job,
                    started_at=started_at,
                )
                try:
                    execution = _normalize_job_execution_result(execute_job(job))
                    message = execution.message
                    retry_count = execution.retry_count
                except Exception as exc:  # noqa: BLE001
                    status = "error"
                    message = str(exc)
                    heartbeat_stop.set()
                    heartbeat_thread.join(timeout=self.heartbeat_interval_seconds + 1.0)
                    if stop_on_error:
                        finished_at = datetime.now(tz=UTC)
                        result = JobRunResult(
                            job_name=job.name,
                            job_type=job.job_type,
                            started_at=started_at,
                            finished_at=finished_at,
                            status=status,
                            message=message,
                            retry_count=retry_count,
                        )
                        self._update_job_counters(result)
                        self.runs_sink.write(result.to_dict())
                        self._write_heartbeat(
                            status="error",
                            run_count=run_count,
                            jobs=jobs,
                            next_run_at=next_run_at,
                            last_result=result,
                        )
                        raise
                else:
                    heartbeat_stop.set()
                    heartbeat_thread.join(timeout=self.heartbeat_interval_seconds + 1.0)

                finished_at = datetime.now(tz=UTC)
                result = JobRunResult(
                    job_name=job.name,
                    job_type=job.job_type,
                    started_at=started_at,
                    finished_at=finished_at,
                    status=status,
                    message=message,
                    retry_count=retry_count,
                )
                self._update_job_counters(result)
                self.runs_sink.write(result.to_dict())
                run_count += 1
                next_run_at[job.name] = finished_at + timedelta(seconds=job.interval_seconds)
                self._write_heartbeat(
                    status="running",
                    run_count=run_count,
                    jobs=jobs,
                    next_run_at=next_run_at,
                    last_result=result,
                )
                if max_runs is not None and run_count >= max_runs:
                    break

        self._write_heartbeat(status="stopped", run_count=run_count, jobs=jobs, next_run_at=next_run_at)
        return run_count

    def _start_running_heartbeat(
        self,
        *,
        stop_event: threading.Event,
        run_count: int,
        jobs: list[JobSpec],
        next_run_at: dict[str, datetime],
        job: JobSpec,
        started_at: datetime,
    ) -> threading.Thread:
        interval_seconds = max(0.1, float(self.heartbeat_interval_seconds))

        def refresh() -> None:
            self._write_heartbeat(
                status="running",
                run_count=run_count,
                jobs=jobs,
                next_run_at=next_run_at,
                current_job=job,
                current_job_started_at=started_at,
            )
            while not stop_event.wait(interval_seconds):
                self._write_heartbeat(
                    status="running",
                    run_count=run_count,
                    jobs=jobs,
                    next_run_at=next_run_at,
                    current_job=job,
                    current_job_started_at=started_at,
                )

        thread = threading.Thread(target=refresh, name=f"ops-heartbeat-{job.name}", daemon=True)
        thread.start()
        return thread

    def _write_heartbeat(
        self,
        *,
        status: str,
        run_count: int,
        jobs: list[JobSpec],
        next_run_at: dict[str, datetime],
        last_result: JobRunResult | None = None,
        current_job: JobSpec | None = None,
        current_job_started_at: datetime | None = None,
    ) -> None:
        with self._heartbeat_lock:
            payload = {
                "runner_name": self.runner_name,
                "status": status,
                "last_seen": datetime.now(tz=UTC).isoformat(),
                "run_count": run_count,
                "job_count": len(jobs),
                "next_run_at": {
                    name: value.isoformat()
                    for name, value in next_run_at.items()
                },
                "job_counters": {
                    name: dict(self._job_counters.get(name, {}))
                    for name in sorted(job.name for job in jobs)
                },
                "last_result": last_result.to_dict() if last_result is not None else None,
                "current_job": (
                    {
                        "name": current_job.name,
                        "job_type": current_job.job_type,
                        "started_at": current_job_started_at.isoformat() if current_job_started_at is not None else None,
                    }
                    if current_job is not None
                    else None
                ),
            }
            _write_json_atomic(self.heartbeat_path, payload)
            now = datetime.now(tz=UTC)
            should_append_history = (
                self._last_heartbeat_status != status
                or self._last_heartbeat_write is None
                or (now - self._last_heartbeat_write).total_seconds() >= 30
            )
            if should_append_history:
                self.heartbeat_history.write(payload)
                self._last_heartbeat_status = status
                self._last_heartbeat_write = now

    def _update_job_counters(self, result: JobRunResult) -> None:
        counters = self._job_counters.setdefault(
            result.job_name,
            {
                "success_count": 0,
                "error_count": 0,
                "retry_count": 0,
                "last_status": None,
                "last_finished_at": None,
            },
        )
        if result.status == "success":
            counters["success_count"] += 1
        else:
            counters["error_count"] += 1
        counters["retry_count"] += result.retry_count
        counters["last_status"] = result.status
        counters["last_finished_at"] = result.finished_at.isoformat()


def build_health_report(
    *,
    ops_root: Path,
    jobs: list[JobSpec] | None = None,
    stale_after_seconds: int = 180,
    job_stale_multiplier: float = 2.5,
    recent_failure_window_seconds: int = 900,
    min_disk_free_gb: float = 100.0,
    quarantine_ratio_threshold: float = 0.20,
) -> HealthReport:
    checked_at = datetime.now(tz=UTC)
    heartbeat_path = ops_root / "heartbeat.json"
    job_runs_path = ops_root / "job_runs.jsonl"

    findings: list[str] = []
    heartbeat = _read_json_file(heartbeat_path)
    heartbeat_age_seconds: float | None = None
    if heartbeat is None:
        findings.append("missing_heartbeat")
    else:
        last_seen = _parse_dt(heartbeat.get("last_seen"))
        if last_seen is None:
            findings.append("invalid_heartbeat_timestamp")
        else:
            heartbeat_age_seconds = (checked_at - last_seen).total_seconds()
            if heartbeat_age_seconds > stale_after_seconds:
                findings.append("stale_heartbeat")
        if heartbeat.get("status") == "error":
            findings.append("runner_error_state")
    job_counters = heartbeat.get("job_counters", {}) if isinstance(heartbeat, dict) else {}
    runner_status = str(heartbeat.get("status") or "") if isinstance(heartbeat, dict) else ""
    current_job = heartbeat.get("current_job") if isinstance(heartbeat, dict) else None
    current_job_name = str(current_job.get("name") or "") if isinstance(current_job, dict) else ""
    current_job_started_at = _parse_dt(current_job.get("started_at")) if isinstance(current_job, dict) else None
    heartbeat_is_fresh = heartbeat_age_seconds is not None and heartbeat_age_seconds <= stale_after_seconds

    run_rows = _read_jsonl(job_runs_path)
    recent_failures = []
    for row in run_rows[-200:]:
        if row.get("status") == "success":
            continue
        finished_at = _parse_dt(row.get("finished_at"))
        if finished_at is None:
            recent_failures.append(row)
            continue
        age_seconds = (checked_at - finished_at).total_seconds()
        if age_seconds <= recent_failure_window_seconds:
            recent_failures.append(row)
    if recent_failures:
        findings.append("recent_job_failures")
    recent_binance_failures = []
    for row in run_rows[-500:]:
        if row.get("job_type") not in {"binance-depth", "binance-depth-worker"} or row.get("status") == "success":
            continue
        finished_at = _parse_dt(row.get("finished_at"))
        if finished_at is None:
            recent_binance_failures.append(row)
            continue
        if (checked_at - finished_at).total_seconds() <= 3600:
            recent_binance_failures.append(row)
    if len(recent_binance_failures) >= 2:
        findings.append("repeated_binance_failures")

    job_rows: list[dict[str, Any]] = []
    if jobs:
        latest_by_job: dict[str, dict[str, Any]] = {}
        for row in run_rows:
            latest_by_job[row.get("job_name", "")] = row

        for job in jobs:
            latest = latest_by_job.get(job.name)
            counters = job_counters.get(job.name, {}) if isinstance(job_counters, dict) else {}
            finished_at = _parse_dt(latest.get("finished_at")) if latest else None
            age_seconds = (checked_at - finished_at).total_seconds() if finished_at else None
            stale_threshold = job.interval_seconds * job_stale_multiplier
            in_progress = (
                runner_status == "running"
                and heartbeat_is_fresh
                and current_job_name == job.name
                and current_job_started_at is not None
            )
            current_job_age_seconds = (
                (checked_at - current_job_started_at).total_seconds()
                if in_progress and current_job_started_at is not None
                else None
            )
            long_running = (
                in_progress
                and current_job_age_seconds is not None
                and current_job_age_seconds > stale_threshold
            )
            is_stale = False if in_progress else age_seconds is None or age_seconds > stale_threshold
            partition_dataset, partition_source = _job_partition_target(job)
            last_partition_write_at: str | None = None
            partition_age_seconds: float | None = None
            partition_stale: bool | None = None
            if is_stale:
                findings.append(f"stale_job:{job.name}")
            if long_running:
                findings.append(f"long_running_job:{job.name}")
            if latest and latest.get("status") != "success" and not in_progress:
                findings.append(f"job_error:{job.name}")
            if partition_dataset is not None and partition_source is not None:
                latest_partition_write = _latest_partition_write(
                    dataset=partition_dataset,
                    source=partition_source,
                )
                if latest_partition_write is None:
                    partition_stale = True
                    findings.append(f"missing_partition:{job.name}")
                else:
                    partition_age_seconds = (checked_at - latest_partition_write).total_seconds()
                    last_partition_write_at = latest_partition_write.isoformat()
                    partition_stale = partition_age_seconds > stale_threshold
                    if partition_stale:
                        findings.append(f"stale_partition:{job.name}")
            job_rows.append(
                {
                    "name": job.name,
                    "job_type": job.job_type,
                    "interval_seconds": job.interval_seconds,
                    "last_finished_at": finished_at.isoformat() if finished_at else None,
                    "age_seconds": age_seconds,
                    "in_progress": in_progress,
                    "current_job_started_at": current_job_started_at.isoformat() if in_progress and current_job_started_at is not None else None,
                    "current_job_age_seconds": current_job_age_seconds,
                    "long_running": long_running,
                    "long_running_threshold_seconds": stale_threshold,
                    "status": latest.get("status") if latest else "missing",
                    "stale": is_stale,
                    "normalized_dataset": partition_dataset,
                    "normalized_source": partition_source,
                    "last_partition_write_at": last_partition_write_at,
                    "partition_age_seconds": partition_age_seconds,
                    "partition_stale": partition_stale,
                    "success_count": counters.get("success_count", 0),
                    "error_count": counters.get("error_count", 0),
                    "retry_count": counters.get("retry_count", 0),
                }
            )

    managed_worker_names = _managed_worker_names(jobs) if jobs is not None else None
    standalone_workers, standalone_findings = _standalone_worker_rows(
        ops_root=ops_root,
        checked_at=checked_at,
        stale_after_seconds=stale_after_seconds,
        managed_worker_names=managed_worker_names,
        quarantine_ratio_threshold=quarantine_ratio_threshold,
    )
    findings.extend(standalone_findings)

    disk_free_gb: float | None = None
    disk_free_pct: float | None = None
    anchor = ops_root.drive or ops_root.anchor or str(ops_root)
    try:
        usage = shutil.disk_usage(anchor)
        disk_free_gb = usage.free / (1024 ** 3)
        disk_free_pct = usage.free / usage.total * 100 if usage.total else None
        if disk_free_gb < min_disk_free_gb:
            findings.append("low_disk_free_space")
    except FileNotFoundError:
        findings.append("ops_root_missing")

    status_findings = [item for item in findings if not item.startswith("unmanaged_")]
    status = "ok"
    if status_findings:
        status = "warn"
    if any(
        item in findings
        for item in ["missing_heartbeat", "stale_heartbeat", "runner_error_state", "recent_job_failures", "low_disk_free_space"]
    ):
        status = "error"
    if any(item.startswith("long_running_job:") for item in findings):
        status = "error"
    if any(
        item.startswith(prefix)
        for item in findings
        for prefix in ("stale_worker:", "worker_error:", "missing_worker_pid:")
    ):
        status = "error"

    return HealthReport(
        status=status,
        checked_at=checked_at,
        heartbeat_age_seconds=heartbeat_age_seconds,
        disk_free_gb=disk_free_gb,
        disk_free_pct=disk_free_pct,
        findings=sorted(set(findings)),
        jobs=job_rows,
        standalone_workers=standalone_workers,
    )


def run_cleanup(
    *,
    archive_root: Path,
    raw_days: int = 14,
    raw_policies: dict[str, int] | None = None,
    apply: bool = False,
) -> CleanupReport:
    checked_at = datetime.now(tz=UTC)
    candidates = _find_cleanup_candidates(
        archive_root=archive_root,
        raw_days=raw_days,
        raw_policies=raw_policies,
    )
    total_bytes = sum(candidate.bytes for candidate in candidates)
    removed_count = 0
    removed_bytes = 0

    if apply:
        for candidate in candidates:
            target = Path(candidate.path)
            if target.is_dir():
                shutil.rmtree(target)
            elif target.exists():
                target.unlink()
            removed_count += 1
            removed_bytes += candidate.bytes

    return CleanupReport(
        mode="apply" if apply else "dry-run",
        checked_at=checked_at,
        candidate_count=len(candidates),
        total_bytes=total_bytes,
        removed_count=removed_count,
        removed_bytes=removed_bytes,
        candidates=candidates,
    )


def prune_stale_worker_artifacts(
    *,
    ops_root: Path,
    stale_after_days: float = 2.0,
    apply: bool = False,
    managed_worker_names: set[str] | None = None,
) -> StaleWorkerPruneReport:
    checked_at = datetime.now(tz=UTC)
    workers_root = ops_root / "standalone_workers"
    archive_root = ops_root / "archived_standalone_workers" / checked_at.strftime("%Y%m%d_%H%M%S")
    protected = set(managed_worker_names or set()) | {"binance-depth-worker", "binance-trades-worker"}
    stale_after_seconds = stale_after_days * 24 * 60 * 60
    candidates: list[StaleWorkerPruneCandidate] = []

    if workers_root.exists():
        for heartbeat_path in sorted(workers_root.glob("*.json")):
            payload = _read_json_file(heartbeat_path)
            if not isinstance(payload, dict):
                continue
            name = str(payload.get("worker_name") or heartbeat_path.stem)
            if name in protected:
                continue
            last_seen = _parse_dt(payload.get("last_seen"))
            age_seconds = (checked_at - last_seen).total_seconds() if last_seen is not None else None
            stale = age_seconds is None or age_seconds > stale_after_seconds
            if not stale:
                continue
            related_paths = _worker_artifact_paths(workers_root, name)
            candidates.append(
                StaleWorkerPruneCandidate(
                    worker_name=name,
                    heartbeat_path=str(heartbeat_path),
                    age_seconds=age_seconds,
                    status=str(payload.get("status") or "unknown"),
                    reason="stale_unmanaged_worker",
                    related_paths=[str(path) for path in related_paths],
                )
            )

    moved_count = 0
    findings: list[str] = []
    if apply and candidates:
        archive_root.mkdir(parents=True, exist_ok=True)
        for candidate in candidates:
            for raw_path in candidate.related_paths:
                source = Path(raw_path)
                if not source.exists():
                    continue
                relative = source.relative_to(workers_root)
                target = archive_root / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                if target.exists():
                    target = target.with_name(f"{target.stem}.{int(time.time() * 1000)}{target.suffix}")
                shutil.move(str(source), str(target))
                moved_count += 1
    elif candidates:
        findings.append("dry_run_candidates")

    return StaleWorkerPruneReport(
        mode="apply" if apply else "dry-run",
        checked_at=checked_at,
        ops_root=str(ops_root),
        archive_root=str(archive_root),
        candidate_count=len(candidates),
        moved_count=moved_count,
        findings=findings,
        candidates=candidates,
    )


def _read_json_file(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    for attempt in range(5):
        try:
            temp_path.replace(path)
            return
        except PermissionError:
            if attempt == 4:
                raise
            time.sleep(0.02 * (attempt + 1))


def _read_pid(payload: dict[str, Any] | None) -> int | None:
    if not isinstance(payload, dict):
        return None
    try:
        pid = int(payload.get("pid"))
    except (TypeError, ValueError):
        return None
    return pid if pid > 0 else None


def _pid_exists(pid: int) -> bool:
    if sys.platform == "win32":
        return _pid_exists_windows(pid)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return True
    return True


def _pid_exists_windows(pid: int) -> bool:
    import ctypes

    error_access_denied = 5
    process_query_limited_information = 0x1000
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    handle = kernel32.OpenProcess(process_query_limited_information, False, pid)
    if handle == 0:
        if ctypes.get_last_error() == error_access_denied:
            return True
        return False
    kernel32.CloseHandle(handle)
    return True


def _normalize_job_execution_result(value: JobExecutionResult | str | None) -> JobExecutionResult:
    if isinstance(value, JobExecutionResult):
        return value
    if isinstance(value, str) or value is None:
        return JobExecutionResult(message=value, retry_count=0)
    raise TypeError(f"unsupported job execution result: {type(value)!r}")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def _standalone_worker_rows(
    *,
    ops_root: Path,
    checked_at: datetime,
    stale_after_seconds: int,
    managed_worker_names: set[str] | None = None,
    quarantine_ratio_threshold: float = 0.20,
) -> tuple[list[dict[str, Any]], list[str]]:
    findings: list[str] = []
    rows: list[dict[str, Any]] = []
    workers_root = ops_root / "standalone_workers"
    if not workers_root.exists():
        return rows, findings

    for heartbeat_path in sorted(workers_root.glob("*.json")):
        payload = _read_json_file(heartbeat_path)
        if not isinstance(payload, dict):
            continue
        name = str(payload.get("worker_name") or heartbeat_path.stem)
        managed = managed_worker_names is None or name in managed_worker_names
        status = str(payload.get("status") or "unknown")
        last_seen = _parse_dt(payload.get("last_seen"))
        age_seconds = max(0.0, (checked_at - last_seen).total_seconds()) if last_seen is not None else None
        pid = _read_pid(payload)
        active = status not in {"stopped", "completed"}
        stale = active and (age_seconds is None or age_seconds > stale_after_seconds)
        pid_missing = active and pid is not None and not _pid_exists(pid)
        current_segment = payload.get("current_segment") if isinstance(payload.get("current_segment"), dict) else None
        current_started_at = _parse_dt(current_segment.get("started_at")) if current_segment else None
        current_segment_age_seconds = (
            max(0.0, (checked_at - current_started_at).total_seconds())
            if current_started_at is not None
            else None
        )
        row_findings: list[str] = []
        if stale:
            row_findings.append("stale_worker")
            findings.append(f"{'stale_worker' if managed else 'unmanaged_stale_worker'}:{name}")
        if pid_missing:
            row_findings.append("missing_worker_pid")
            findings.append(f"{'missing_worker_pid' if managed else 'unmanaged_missing_worker_pid'}:{name}")
        if status == "error":
            row_findings.append("worker_error")
            findings.append(f"{'worker_error' if managed else 'unmanaged_worker_error'}:{name}")

        # Pull the in-flight metrics from the latest summary.jsonl row for the active
        # (or most recent) run. Without this, an operator can't see mid-run that the
        # quality gate is quarantining 30% of events until the run ends.
        current_run_path = current_segment.get("run_path") if current_segment else None
        last_run_path = payload.get("last_run_path")
        partial_metrics: dict[str, Any] | None = None
        quarantine_ratio: float | None = None
        partial_run_path = current_run_path or last_run_path
        if partial_run_path:
            partial_metrics = _read_latest_summary_row(Path(partial_run_path))
            if partial_metrics is not None:
                raw_messages = partial_metrics.get("raw_messages")
                clean_events = partial_metrics.get("clean_events")
                quarantined_events = partial_metrics.get("quarantined_events")
                if (
                    isinstance(raw_messages, (int, float))
                    and raw_messages > 0
                    and isinstance(quarantined_events, (int, float))
                ):
                    quarantine_ratio = float(quarantined_events) / float(raw_messages)
                elif (
                    isinstance(clean_events, (int, float))
                    and isinstance(quarantined_events, (int, float))
                    and (clean_events + quarantined_events) > 0
                ):
                    quarantine_ratio = (
                        float(quarantined_events)
                        / float(clean_events + quarantined_events)
                    )
                if (
                    quarantine_ratio is not None
                    and quarantine_ratio > quarantine_ratio_threshold
                    and active
                ):
                    # Only flag for active runs — historical high-quarantine runs are
                    # an artifact of past stream loss, not an in-flight problem.
                    row_findings.append("high_quarantine_ratio")
                    findings.append(
                        f"{'high_quarantine_ratio' if managed else 'unmanaged_high_quarantine_ratio'}:{name}"
                    )

        rows.append(
            {
                "name": name,
                "managed": managed,
                "worker_type": payload.get("worker_type"),
                "venue": payload.get("venue"),
                "symbol": payload.get("symbol"),
                "status": status,
                "pid": pid,
                "last_seen": last_seen.isoformat() if last_seen is not None else None,
                "age_seconds": age_seconds,
                "stale": stale,
                "pid_missing": pid_missing,
                "blocking": managed and bool(row_findings),
                "findings": row_findings,
                "message": payload.get("message"),
                "last_segment_index": payload.get("last_segment_index"),
                "last_run_path": last_run_path,
                "current_segment_index": current_segment.get("index") if current_segment else None,
                "current_segment_started_at": (
                    current_started_at.isoformat() if current_started_at is not None else None
                ),
                "current_segment_age_seconds": current_segment_age_seconds,
                "current_run_path": current_run_path,
                "partial_metrics": partial_metrics,
                "quarantine_ratio": quarantine_ratio,
            }
        )
    return rows, findings


def _read_latest_summary_row(run_path: Path) -> dict[str, Any] | None:
    """Return the last JSON row in <run_path>/metrics/summary.jsonl, or None.

    The pipeline emits a row every `metrics_flush_every` events with
    `partial: True`, plus a final `partial: False` row on shutdown. Reading the
    last line gives us the freshest in-flight snapshot of raw/clean/quarantined
    counts + reject_counts.
    """
    summary_path = run_path / "metrics" / "summary.jsonl"
    if not summary_path.exists():
        return None
    try:
        raw = summary_path.read_text(encoding="utf-8")
    except OSError:
        return None
    last_row: dict[str, Any] | None = None
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            last_row = parsed
    return last_row


def _managed_worker_names(jobs: list[JobSpec] | None) -> set[str]:
    names: set[str] = set()
    for job in jobs or []:
        # Any *-worker job is a managed standalone worker (binance/coinbase/kraken/bybit,
        # trades or depth). Each run_*_worker defaults worker_name to its job_type, so an
        # unset worker_name maps to the job_type — matching what the worker writes to its
        # heartbeat. Enumerating venues here was the old approach and silently dropped the
        # non-Binance lanes, flagging healthy workers as unmanaged.
        if job.job_type.endswith("-worker"):
            names.add(str(job.args.get("worker_name") or job.job_type))
    return names


def _worker_artifact_paths(workers_root: Path, worker_name: str) -> list[Path]:
    paths: list[Path] = []
    heartbeat = workers_root / f"{worker_name}.json"
    lock = workers_root / f"{worker_name}.lock"
    if heartbeat.exists():
        paths.append(heartbeat)
    if lock.exists():
        paths.append(lock)
    logs_root = workers_root / "logs"
    if logs_root.exists():
        paths.extend(sorted(path for path in logs_root.glob(f"{worker_name}.*") if path.is_file()))
    return paths


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC)
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
        except ValueError:
            return None
    return None


def _find_cleanup_candidates(
    *,
    archive_root: Path,
    raw_days: int,
    raw_policies: dict[str, int] | None = None,
) -> list[CleanupCandidate]:
    candidates: list[CleanupCandidate] = []
    raw_root = archive_root / "raw"
    checked_at = datetime.now(tz=UTC)

    if raw_root.exists():
        for dataset_dir in raw_root.iterdir():
            if not dataset_dir.is_dir():
                continue
            for source_dir in dataset_dir.iterdir():
                if not source_dir.is_dir():
                    continue
                retention_days = _raw_retention_days(
                    dataset=dataset_dir.name,
                    source=source_dir.name,
                    raw_days=raw_days,
                    raw_policies=raw_policies,
                )
                cutoff = checked_at - timedelta(days=retention_days)
                for run_dir in source_dir.iterdir():
                    if not run_dir.is_dir():
                        continue
                    run_started = _parse_run_dir_name(run_dir.name) or _from_mtime(run_dir)
                    if run_started is None or run_started > cutoff:
                        continue
                    candidates.append(
                        CleanupCandidate(
                            path=str(run_dir),
                            reason="old_raw_run_directory",
                            bytes=_directory_size(run_dir),
                        )
                    )

    normalized_root = archive_root / "normalized"
    if normalized_root.exists():
        for parquet_file in normalized_root.rglob("*.parquet"):
            if parquet_file.is_file() and parquet_file.stat().st_size == 0:
                candidates.append(
                    CleanupCandidate(
                        path=str(parquet_file),
                        reason="zero_byte_parquet",
                        bytes=0,
                    )
                )

    return sorted(candidates, key=lambda item: (item.reason, item.path))


def _parse_run_dir_name(name: str) -> datetime | None:
    try:
        return datetime.strptime(name, "%Y%m%d_%H%M%S").replace(tzinfo=UTC)
    except ValueError:
        return None


def _from_mtime(path: Path) -> datetime | None:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
    except FileNotFoundError:
        return None


def _directory_size(path: Path) -> int:
    total = 0
    for child in path.rglob("*"):
        if child.is_file():
            try:
                total += child.stat().st_size
            except FileNotFoundError:
                continue
    return total


def _raw_retention_days(
    *,
    dataset: str,
    source: str,
    raw_days: int,
    raw_policies: dict[str, int] | None = None,
) -> int:
    if not raw_policies:
        return raw_days
    return raw_policies.get(f"{dataset}/{source}", raw_days)


def _job_partition_target(job: JobSpec) -> tuple[str | None, str | None]:
    if job.job_type == "mock":
        return "market", "mock"
    if job.job_type == "binance-depth-worker":
        return "market", "binance"
    if job.job_type == "binance-trades-worker":
        return "trades", "binance"
    return None, None


def _latest_partition_write(*, dataset: str, source: str) -> datetime | None:
    dataset_root = default_normalized_root(dataset)
    latest: datetime | None = None
    if not dataset_root.exists():
        return None
    for schema_dir in dataset_root.glob("schema_version=*"):
        source_dir = schema_dir / f"source={source}"
        if not source_dir.exists():
            continue
        for parquet_file in source_dir.rglob("*.parquet"):
            modified_at = _from_mtime(parquet_file)
            if modified_at is None:
                continue
            if latest is None or modified_at > latest:
                latest = modified_at
    return latest
