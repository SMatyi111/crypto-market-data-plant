from __future__ import annotations

import json
import os
import socket
import sys
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
import shutil
import threading
import time
from typing import Any, Callable

from .config import default_normalized_root
from .storage import JsonlSink

# Job types treated as long-running *collector* jobs by the ops runner. These may run
# concurrently with one another (up to --collector-concurrency); every other job type
# is a maintenance job and stays serialized in the scheduler loop. Keep this in sync
# with the worker dispatch table in cli.py::_execute_ops_job.
COLLECTOR_JOB_TYPES: frozenset[str] = frozenset(
    {
        "binance-depth-worker",
        "binance-trades-worker",
        "coinbase-trades-worker",
        "coinbase-depth-worker",
        "kraken-trades-worker",
        "kraken-depth-worker",
        "bybit-trades-worker",
        "bybit-depth-worker",
        "mexc-trades-worker",
        "mexc-depth-worker",
    }
)


def is_collector_job(job: "JobSpec") -> bool:
    return job.job_type in COLLECTOR_JOB_TYPES


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


# Collector lanes that poll a REST/HTTP API on an interval instead of holding a
# websocket. Unlike the WS lanes (the *-worker job types tracked in
# standalone_workers), these never appear in the standalone-worker table, so the
# health report surfaces their freshness separately under poll_lanes. Extend this
# set as new poll-based collectors are added.
POLL_LANE_JOB_TYPES = frozenset({"kalshi-collect-crypto-quotes", "kalshi-discover-crypto"})


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
    binance_trades: dict[str, Any] | None = None
    poll_lanes: list[dict[str, Any]] = field(default_factory=list)

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
            "binance_trades": self.binance_trades,
            "poll_lanes": self.poll_lanes,
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
                if owner_pid is not None and _pid_exists(owner_pid) and self._heartbeat_is_fresh():
                    raise RuntimeError(
                        f"ops runner already active for {self.ops_root} "
                        f"(pid={owner_pid}, runner={owner.get('runner_name', 'unknown')})"
                    )
                # Stale lock: the recorded pid is gone, OR it was recycled to an unrelated
                # process (Windows OpenProcess -> access-denied makes _pid_exists report
                # "alive") while the heartbeat has gone stale. A live runner writes
                # heartbeat.json every ~30s, so a stale/absent heartbeat means the previous
                # runner is dead — reclaim the lock instead of refusing to start. Without
                # this, a crash or Stop+Start strands collection on a phantom lock until a
                # manual unlink.
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

    def _heartbeat_is_fresh(self, *, max_age_seconds: float = 180.0) -> bool:
        """A live ops-runner writes heartbeat.json every ~30s. Used to tell a genuinely
        active runner apart from a stale lock whose pid was recycled to another process.
        Missing / unparseable / older-than-max_age heartbeat -> not fresh (runner is dead)."""
        heartbeat = _read_json_file(self.ops_root / "heartbeat.json")
        if not isinstance(heartbeat, dict):
            return False
        last_seen = _parse_dt(heartbeat.get("last_seen"))
        if last_seen is None:
            return False
        return (datetime.now(tz=UTC) - last_seen).total_seconds() <= max_age_seconds

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
        collector_concurrency: int = 1,
    ) -> None:
        self.ops_root = ops_root
        self.runner_name = runner_name
        self.poll_seconds = poll_seconds
        self.heartbeat_interval_seconds = heartbeat_interval_seconds
        self.collector_concurrency = max(1, int(collector_concurrency))
        self.ops_root.mkdir(parents=True, exist_ok=True)
        self.runs_sink = JsonlSink(self.ops_root, "job_runs.jsonl")
        self.heartbeat_history = JsonlSink(self.ops_root, "heartbeat_history.jsonl")
        self.heartbeat_path = self.ops_root / "heartbeat.json"
        self._last_heartbeat_status: str | None = None
        self._last_heartbeat_write: datetime | None = None
        self._job_counters: dict[str, dict[str, Any]] = {}
        self._heartbeat_lock = threading.Lock()
        # Live scheduling state, guarded by _run_lock so the heartbeat refresher thread
        # and the pool worker threads can read/update it without tearing.
        self._run_lock = threading.Lock()
        self._sched_jobs: list[JobSpec] = []
        self._sched_run_count = 0
        self._sched_next_run_at: dict[str, datetime] = {}
        self._sched_active: dict[str, dict[str, Any]] = {}
        self._sched_last_result: JobRunResult | None = None

    def run(
        self,
        jobs: list[JobSpec],
        *,
        execute_job: Callable[[JobSpec], JobExecutionResult | str | None],
        max_runs: int | None = None,
        stop_on_error: bool = False,
    ) -> int:
        concurrency = self.collector_concurrency
        with self._run_lock:
            self._sched_jobs = list(jobs)
            self._sched_run_count = 0
            self._sched_next_run_at = {job.name: datetime.now(tz=UTC) for job in jobs}
            self._sched_active = {}
            self._sched_last_result = None
        self._write_heartbeat_snapshot(status="starting")

        heartbeat_interval = max(0.1, float(self.heartbeat_interval_seconds))
        heartbeat_stop = threading.Event()
        heartbeat_thread = threading.Thread(
            target=self._heartbeat_refresher,
            args=(heartbeat_stop, heartbeat_interval),
            name=f"ops-heartbeat-{self.runner_name}",
            daemon=True,
        )
        heartbeat_thread.start()

        pool = ThreadPoolExecutor(max_workers=concurrency, thread_name_prefix="ops-collector")
        futures: dict[Future, JobSpec] = {}
        pending_error: BaseException | None = None
        try:
            while True:
                with self._run_lock:
                    run_count = self._sched_run_count
                if max_runs is not None and run_count >= max_runs:
                    break

                # 1. Reap finished collector jobs (each records its own result).
                stop_now = False
                for future in [f for f in list(futures) if f.done()]:
                    job = futures.pop(future)
                    result = future.result()
                    self._finalize_run(job, result)
                    self._write_heartbeat_snapshot()
                    if result.status == "error" and stop_on_error:
                        pending_error = RuntimeError(
                            f"collector job {job.name} failed: {result.message}"
                        )
                        stop_now = True
                        break
                if stop_now:
                    break

                with self._run_lock:
                    run_count = self._sched_run_count
                    active_names = set(self._sched_active)
                    next_run_at = dict(self._sched_next_run_at)
                    active_collectors = sum(
                        1
                        for entry in self._sched_active.values()
                        if entry["job_type"] in COLLECTOR_JOB_TYPES
                    )
                if max_runs is not None and run_count >= max_runs:
                    break

                now = datetime.now(tz=UTC)
                due = [
                    job
                    for job in jobs
                    if now >= next_run_at[job.name] and job.name not in active_names
                ]
                collector_due = [job for job in due if job.job_type in COLLECTOR_JOB_TYPES]
                maintenance_due = [job for job in due if job.job_type not in COLLECTOR_JOB_TYPES]

                # `started` counts runs already finished plus runs in flight, so a bounded
                # (max_runs) run never launches more work than it will eventually report.
                started = run_count + len(futures)

                # 2. Dispatch collector jobs up to the concurrency budget; never launch a
                #    second instance of a job that is already active.
                dispatched = False
                for job in collector_due:
                    if active_collectors >= concurrency:
                        break
                    if max_runs is not None and started >= max_runs:
                        break
                    job_started_at = datetime.now(tz=UTC)
                    with self._run_lock:
                        self._sched_active[job.name] = {
                            "name": job.name,
                            "job_type": job.job_type,
                            "started_at": job_started_at,
                        }
                    future = pool.submit(self._run_collector_job, job, execute_job, job_started_at)
                    futures[future] = job
                    active_collectors += 1
                    started += 1
                    dispatched = True
                if dispatched:
                    self._write_heartbeat_snapshot()

                # 3. Run at most one maintenance job per tick. Synchronous execution in the
                #    scheduler thread keeps maintenance strictly serialized with itself
                #    while collectors keep running in the pool.
                ran_maintenance = False
                if maintenance_due and not (max_runs is not None and started >= max_runs):
                    self._run_maintenance_job(
                        maintenance_due[0], execute_job, stop_on_error=stop_on_error
                    )
                    ran_maintenance = True

                if dispatched or ran_maintenance:
                    # Did real work this tick; loop immediately to keep latency low.
                    continue
                if futures:
                    # Collectors are in flight but nothing new is due — poll for completion
                    # without pegging a core.
                    time.sleep(self.poll_seconds if self.poll_seconds > 0 else 0.05)
                    continue
                # Fully idle.
                self._write_heartbeat_snapshot(status="idle")
                time.sleep(self.poll_seconds if self.poll_seconds > 0 else 0.1)
        finally:
            heartbeat_stop.set()
            heartbeat_thread.join(timeout=heartbeat_interval + 1.0)
            # Drain any still-running collectors so their results are not lost.
            pool.shutdown(wait=True)
            for future in [f for f in list(futures) if f.done()]:
                job = futures.pop(future)
                try:
                    result = future.result()
                except Exception as exc:  # noqa: BLE001 - defensive; worker catches its own
                    moment = datetime.now(tz=UTC)
                    result = JobRunResult(
                        job_name=job.name,
                        job_type=job.job_type,
                        started_at=moment,
                        finished_at=moment,
                        status="error",
                        message=str(exc),
                    )
                self._finalize_run(job, result)

        if pending_error is not None:
            self._write_heartbeat_snapshot(status="error")
            raise pending_error

        self._write_heartbeat_snapshot(status="stopped")
        with self._run_lock:
            return self._sched_run_count

    def _run_collector_job(
        self,
        job: JobSpec,
        execute_job: Callable[[JobSpec], JobExecutionResult | str | None],
        started_at: datetime,
    ) -> JobRunResult:
        """Run one collector job on a pool thread. Never raises — a failure is captured
        into an error JobRunResult so the scheduler always reaps a result."""
        status = "success"
        message: str | None = None
        retry_count = 0
        try:
            execution = _normalize_job_execution_result(execute_job(job))
            message = execution.message
            retry_count = execution.retry_count
        except Exception as exc:  # noqa: BLE001
            status = "error"
            message = str(exc)
        finished_at = datetime.now(tz=UTC)
        return JobRunResult(
            job_name=job.name,
            job_type=job.job_type,
            started_at=started_at,
            finished_at=finished_at,
            status=status,
            message=message,
            retry_count=retry_count,
        )

    def _run_maintenance_job(
        self,
        job: JobSpec,
        execute_job: Callable[[JobSpec], JobExecutionResult | str | None],
        *,
        stop_on_error: bool,
    ) -> None:
        started_at = datetime.now(tz=UTC)
        with self._run_lock:
            self._sched_active[job.name] = {
                "name": job.name,
                "job_type": job.job_type,
                "started_at": started_at,
            }
        self._write_heartbeat_snapshot()
        status = "success"
        message: str | None = None
        retry_count = 0
        try:
            execution = _normalize_job_execution_result(execute_job(job))
            message = execution.message
            retry_count = execution.retry_count
        except Exception as exc:  # noqa: BLE001
            status = "error"
            message = str(exc)
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
        self._finalize_run(job, result)
        if status == "error" and stop_on_error:
            self._write_heartbeat_snapshot(status="error")
            raise RuntimeError(f"maintenance job {job.name} failed: {message}")
        self._write_heartbeat_snapshot()

    def _finalize_run(self, job: JobSpec, result: JobRunResult) -> None:
        """Record a completed run: drop it from the active set, update counters, advance
        its next-run time, and append to job_runs.jsonl. Safe to call for either a
        collector (from the scheduler thread after the future completes) or a maintenance
        job (inline)."""
        with self._run_lock:
            self._sched_active.pop(job.name, None)
            self._update_job_counters(result)
            self._sched_run_count += 1
            self._sched_next_run_at[job.name] = result.finished_at + timedelta(
                seconds=job.interval_seconds
            )
            self._sched_last_result = result
        self.runs_sink.write(result.to_dict())

    def _heartbeat_refresher(self, stop_event: threading.Event, interval_seconds: float) -> None:
        while not stop_event.wait(interval_seconds):
            self._write_heartbeat_snapshot()

    def _write_heartbeat_snapshot(self, *, status: str | None = None) -> None:
        with self._run_lock:
            resolved = (
                status if status is not None else ("running" if self._sched_active else "idle")
            )
            payload = self._build_heartbeat_payload(resolved)
        self._emit_heartbeat(payload)

    def _build_heartbeat_payload(self, status: str) -> dict[str, Any]:
        """Build the heartbeat payload from live scheduling state. Caller must hold
        self._run_lock."""
        active_sorted = sorted(
            self._sched_active.values(), key=lambda entry: entry["started_at"]
        )
        current_jobs = [
            {
                "name": entry["name"],
                "job_type": entry["job_type"],
                "started_at": entry["started_at"].isoformat(),
            }
            for entry in active_sorted
        ]
        return {
            "runner_name": self.runner_name,
            "status": status,
            "last_seen": datetime.now(tz=UTC).isoformat(),
            "run_count": self._sched_run_count,
            "job_count": len(self._sched_jobs),
            "next_run_at": {
                name: value.isoformat() for name, value in self._sched_next_run_at.items()
            },
            "job_counters": {
                name: dict(self._job_counters.get(name, {}))
                for name in sorted(job.name for job in self._sched_jobs)
            },
            "last_result": self._sched_last_result.to_dict()
            if self._sched_last_result is not None
            else None,
            # current_jobs is the full active set; current_job is kept for backward
            # compatibility and points at the oldest active job (None when idle).
            "current_jobs": current_jobs,
            "current_job": current_jobs[0] if current_jobs else None,
        }

    def _emit_heartbeat(self, payload: dict[str, Any]) -> None:
        with self._heartbeat_lock:
            _write_json_atomic(self.heartbeat_path, payload)
            now = datetime.now(tz=UTC)
            status = payload["status"]
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
    next_run_at_map = heartbeat.get("next_run_at", {}) if isinstance(heartbeat, dict) else {}
    runner_status = str(heartbeat.get("status") or "") if isinstance(heartbeat, dict) else ""
    # Active jobs: prefer the current_jobs list written by the parallel runner; fall
    # back to the legacy single current_job for older heartbeats. A job that appears
    # here is treated as in progress, which suppresses stale-job warnings for it.
    current_jobs_raw = heartbeat.get("current_jobs") if isinstance(heartbeat, dict) else None
    current_job = heartbeat.get("current_job") if isinstance(heartbeat, dict) else None
    active_started_by_name: dict[str, datetime | None] = {}
    if isinstance(current_jobs_raw, list):
        for item in current_jobs_raw:
            if isinstance(item, dict) and item.get("name"):
                active_started_by_name[str(item["name"])] = _parse_dt(item.get("started_at"))
    elif isinstance(current_job, dict) and current_job.get("name"):
        active_started_by_name[str(current_job["name"])] = _parse_dt(current_job.get("started_at"))
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
    poll_lanes: list[dict[str, Any]] = []
    if jobs:
        latest_by_job: dict[str, dict[str, Any]] = {}
        for row in run_rows:
            latest_by_job[row.get("job_name", "")] = row

        for job in jobs:
            latest = latest_by_job.get(job.name)
            counters = job_counters.get(job.name, {}) if isinstance(job_counters, dict) else {}
            finished_at = _parse_dt(latest.get("finished_at")) if latest else None
            age_seconds = (checked_at - finished_at).total_seconds() if finished_at else None
            # Continuous-capture collectors rotate a finalized segment every
            # max_segment_seconds, which is far longer than their tiny re-dispatch
            # interval (e.g. 1800s segments, 5s interval). Interval-based staleness
            # would then flag every healthy lane, so the expected completion cadence is
            # the segment length when set. Jobs without it (poll/maintenance lanes, the
            # legacy hourly config) keep pure interval-based thresholds.
            segment_seconds = 0.0
            try:
                segment_seconds = float(job.args.get("max_segment_seconds") or 0.0)
            except (TypeError, ValueError):
                segment_seconds = 0.0
            cadence_seconds = max(float(job.interval_seconds), segment_seconds)
            stale_threshold = cadence_seconds * job_stale_multiplier
            # A running segment is "long" only past its own rotation deadline (+ slack
            # for finalize); for non-segmented jobs this stays the interval-based bound.
            long_running_threshold = (
                segment_seconds * 1.5 if segment_seconds > 0 else stale_threshold
            )
            job_started_at = active_started_by_name.get(job.name)
            in_progress = (
                runner_status == "running"
                and heartbeat_is_fresh
                and job.name in active_started_by_name
                and job_started_at is not None
            )
            current_job_age_seconds = (
                (checked_at - job_started_at).total_seconds()
                if in_progress and job_started_at is not None
                else None
            )
            long_running = (
                in_progress
                and current_job_age_seconds is not None
                and current_job_age_seconds > long_running_threshold
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
                    "current_job_started_at": job_started_at.isoformat() if in_progress and job_started_at is not None else None,
                    "current_job_age_seconds": current_job_age_seconds,
                    "long_running": long_running,
                    "long_running_threshold_seconds": long_running_threshold,
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

            # Poll-based collector lanes (e.g. Kalshi) run as interval jobs, not WS
            # workers, so they never show up in standalone_workers. Surface their
            # freshness explicitly so they aren't a monitoring blind spot.
            if job.job_type in POLL_LANE_JOB_TYPES:
                poll_lanes.append(
                    {
                        "name": job.name,
                        "job_type": job.job_type,
                        "interval_seconds": job.interval_seconds,
                        "last_finished_at": finished_at.isoformat() if finished_at else None,
                        "age_seconds": age_seconds,
                        "stale": is_stale,
                        "in_progress": in_progress,
                        "status": latest.get("status") if latest else "missing",
                        "next_run_at": next_run_at_map.get(job.name) if isinstance(next_run_at_map, dict) else None,
                        "success_count": counters.get("success_count", 0),
                        "error_count": counters.get("error_count", 0),
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
    binance_trades, binance_trade_findings = _binance_trades_quality(
        ops_root=ops_root,
        checked_at=checked_at,
    )
    findings.extend(binance_trade_findings)

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
        binance_trades=binance_trades,
        poll_lanes=poll_lanes,
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

                # The data-arrival watchdog fired on this run — the feed acked then went
                # silent-but-connected, so the collector ended the segment instead of
                # hanging. Surface it as a (non-blocking, warn-level) finding for active
                # workers so an operator notices a silent venue; a healthy next segment
                # writes idle_timeout_count=0 and clears it. Not row_findings: the lane
                # self-heals (worker opens a fresh segment), so it isn't "blocking".
                idle_timeout_count = partial_metrics.get("idle_timeout_count")
                if (
                    isinstance(idle_timeout_count, (int, float))
                    and idle_timeout_count > 0
                    and active
                ):
                    findings.append(
                        f"{'idle_timeout' if managed else 'unmanaged_idle_timeout'}:{name}"
                    )

        # Fallback for actively-running workers: mid-segment a worker exposes no run path
        # (current_run_path and last_run_path are both None) and writes no summary.jsonl
        # until the segment finalizes, so the read above yields quarantine_ratio=None — a
        # monitoring blind spot (you can't tell a clean running lane from a bad one, and
        # low-volume lanes whose segment exceeds their interval may *never* report). Derive
        # the lane's raw source dir from ops_root + worker_type and read the latest run's
        # clean/quarantine event counts directly so health reports a real in-progress ratio.
        if quarantine_ratio is None:
            worker_type = str(payload.get("worker_type") or "")
            if worker_type.endswith("-worker"):
                source = worker_type[: -len("-worker")].replace("-", "_")
                raw_root = ops_root.parent / "raw" / "market" / source
                run_dirs = (
                    sorted((p for p in raw_root.glob("[0-9]*_*") if p.is_dir()), reverse=True)
                    if raw_root.exists()
                    else []
                )
                if run_dirs:
                    latest_run = run_dirs[0]
                    counts: dict[str, int] = {}
                    for rel in ("clean/events.jsonl", "quarantine/events.jsonl"):
                        f = latest_run / rel
                        counts[rel] = (
                            sum(1 for line in f.open(encoding="utf-8") if line.strip())
                            if f.exists()
                            else 0
                        )
                    clean_n = counts["clean/events.jsonl"]
                    quar_n = counts["quarantine/events.jsonl"]
                    if clean_n + quar_n > 0:
                        quarantine_ratio = float(quar_n) / float(clean_n + quar_n)
                        partial_metrics = {
                            "clean_events": clean_n,
                            "quarantined_events": quar_n,
                            "run_path": str(latest_run),
                            "source": "in_progress_run_dir",
                        }
                        if quarantine_ratio > quarantine_ratio_threshold and active:
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


def _binance_trades_quality(
    *,
    ops_root: Path,
    checked_at: datetime,
    recent_run_limit: int = 10,
    no_replayable_after_seconds: float = 30 * 60,
    low_clean_ratio_threshold: float = 0.25,
    low_clean_ratio_consecutive_runs: int = 3,
) -> tuple[dict[str, Any], list[str]]:
    source_root = ops_root.parent / "raw" / "market" / "binance_trades"
    promotion_index_path = (
        ops_root.parent
        / "curated"
        / "research"
        / "trades_replayable"
        / "_promotion_index.jsonl"
    )
    promoted_rows_by_run = _promoted_rows_by_run(promotion_index_path)
    run_rows: list[dict[str, Any]] = []
    findings: list[str] = []
    latest_replayable_started_at: datetime | None = None
    latest_replayable_run: str | None = None

    if source_root.exists():
        for run_dir in sorted(
            (path for path in source_root.iterdir() if path.is_dir()),
            key=lambda path: path.name,
            reverse=True,
        )[:recent_run_limit]:
            metrics = _read_latest_summary_row(run_dir)
            replay = _read_json_file(run_dir / "metrics" / "replay_summary.json")
            started_at = _parse_run_dir_started_at(run_dir.name)
            replayable = replay.get("replayable") if isinstance(replay, dict) else None
            raw_messages = _number_or_none(metrics.get("raw_messages")) if metrics else None
            clean_events = _number_or_none(metrics.get("clean_events")) if metrics else None
            quarantined_events = (
                _number_or_none(metrics.get("quarantined_events")) if metrics else None
            )
            clean_ratio: float | None = None
            if raw_messages and raw_messages > 0 and clean_events is not None:
                clean_ratio = float(clean_events) / float(raw_messages)
            elif (
                clean_events is not None
                and quarantined_events is not None
                and (clean_events + quarantined_events) > 0
            ):
                clean_ratio = float(clean_events) / float(clean_events + quarantined_events)

            if replayable is True and started_at is not None:
                if latest_replayable_started_at is None or started_at > latest_replayable_started_at:
                    latest_replayable_started_at = started_at
                    latest_replayable_run = run_dir.name

            run_rows.append(
                {
                    "run": run_dir.name,
                    "started_at": started_at.isoformat() if started_at is not None else None,
                    "raw_messages": raw_messages,
                    "clean_events": clean_events,
                    "quarantined_events": quarantined_events,
                    "clean_ratio": clean_ratio,
                    "replayable": replayable,
                    "findings": (
                        [str(item) for item in replay.get("findings", [])]
                        if isinstance(replay, dict)
                        else None
                    ),
                    "trade_id_gap_count": (
                        _number_or_none(replay.get("trade_id_gap_count"))
                        if isinstance(replay, dict)
                        else None
                    ),
                    "trade_id_gap_total_missing": (
                        _number_or_none(replay.get("trade_id_gap_total_missing"))
                        if isinstance(replay, dict)
                        else None
                    ),
                    "promoted_rows": promoted_rows_by_run.get(str(run_dir), 0),
                }
            )

    if run_rows and run_rows[0]["replayable"] is False:
        findings.append("binance_trades_latest_unreplayable")

    latest_replayable_age_seconds: float | None = None
    if run_rows and latest_replayable_started_at is None:
        findings.append("binance_trades_no_replayable_30m")
    elif latest_replayable_started_at is not None:
        latest_replayable_age_seconds = max(
            0.0, (checked_at - latest_replayable_started_at).total_seconds()
        )
        if latest_replayable_age_seconds > no_replayable_after_seconds:
            findings.append("binance_trades_no_replayable_30m")

    ratio_candidates = [
        row
        for row in run_rows
        if row.get("clean_ratio") is not None and row.get("replayable") is not None
    ][:low_clean_ratio_consecutive_runs]
    if (
        len(ratio_candidates) == low_clean_ratio_consecutive_runs
        and all(
            float(row["clean_ratio"]) < low_clean_ratio_threshold
            for row in ratio_candidates
        )
    ):
        findings.append("binance_trades_low_clean_ratio")

    return (
        {
            "source_root": str(source_root),
            "recent_run_limit": recent_run_limit,
            "checked_run_count": len(run_rows),
            "replayable_run_count": sum(1 for row in run_rows if row["replayable"] is True),
            "latest_run": run_rows[0]["run"] if run_rows else None,
            "latest_run_replayable": run_rows[0]["replayable"] if run_rows else None,
            "latest_replayable_run": latest_replayable_run,
            "latest_replayable_age_seconds": latest_replayable_age_seconds,
            "total_clean_events": int(sum(row.get("clean_events") or 0 for row in run_rows)),
            "total_quarantined_events": int(
                sum(row.get("quarantined_events") or 0 for row in run_rows)
            ),
            "total_promoted_rows": int(sum(row.get("promoted_rows") or 0 for row in run_rows)),
            "low_clean_ratio_threshold": low_clean_ratio_threshold,
            "runs": run_rows,
        },
        findings,
    )


def _promoted_rows_by_run(index_path: Path) -> dict[str, int]:
    rows: dict[str, int] = {}
    if not index_path.exists():
        return rows
    try:
        raw = index_path.read_text(encoding="utf-8")
    except OSError:
        return rows
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        run_path = payload.get("run_path")
        promoted_rows = _number_or_none(payload.get("promoted_rows"))
        if (
            isinstance(run_path, str)
            and "binance_trades" in Path(run_path).parts
            and promoted_rows
        ):
            rows[run_path] = rows.get(run_path, 0) + int(promoted_rows)
    return rows


def _parse_run_dir_started_at(name: str) -> datetime | None:
    try:
        return datetime.strptime(name, "%Y%m%d_%H%M%S").replace(tzinfo=UTC)
    except ValueError:
        return None


def _number_or_none(value: Any) -> int | float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return value
    return None


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
