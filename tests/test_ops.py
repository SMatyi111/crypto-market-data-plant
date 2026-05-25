from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
import threading
import time

import pytest

from crypto_collector.cli import (
    build_parser,
    run_binance_depth_worker,
    run_ops_runner,
)
from crypto_collector.models import utc_now
from crypto_collector.ops import (
    JobExecutionResult,
    JobSpec,
    OpsRunner,
    StandaloneWorkerRuntime,
    build_health_report,
    load_ops_config,
    prune_stale_worker_artifacts,
)


def test_load_ops_config_filters_disabled_jobs(tmp_path: Path) -> None:
    config_path = tmp_path / "ops.json"
    config_path.write_text(
        json.dumps(
            {
                "jobs": [
                    {
                        "name": "mock-a",
                        "job_type": "mock",
                        "interval_seconds": 60,
                        "args": {"count": 1},
                    },
                    {
                        "name": "mock-b",
                        "job_type": "mock",
                        "interval_seconds": 60,
                        "enabled": False,
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    jobs = load_ops_config(config_path)
    assert [job.name for job in jobs] == ["mock-a"]


def test_ops_runner_writes_heartbeat_and_run_logs(tmp_path: Path) -> None:
    runner = OpsRunner(tmp_path / "ops", poll_seconds=0)
    jobs = [
        JobSpec(
            name="mock-a",
            job_type="mock",
            interval_seconds=1,
            args={"count": 1},
        )
    ]

    executed = runner.run(
        jobs,
        execute_job=lambda job: f"ran {job.name}",
        max_runs=2,
    )

    assert executed == 2
    heartbeat = json.loads((tmp_path / "ops" / "heartbeat.json").read_text(encoding="utf-8"))
    assert heartbeat["status"] == "stopped"
    assert heartbeat["run_count"] == 2

    run_lines = (tmp_path / "ops" / "job_runs.jsonl").read_text(encoding="utf-8").strip().splitlines()
    assert len(run_lines) == 2
    assert all(json.loads(line)["status"] == "success" for line in run_lines)


def test_ops_runner_writes_job_counters_with_retries(tmp_path: Path) -> None:
    runner = OpsRunner(tmp_path / "ops", poll_seconds=0)
    jobs = [
        JobSpec(
            name="binance-btc-depth",
            job_type="binance-depth",
            interval_seconds=1,
        )
    ]

    executed = runner.run(
        jobs,
        execute_job=lambda job: JobExecutionResult(message="ok", retry_count=2),
        max_runs=1,
    )

    assert executed == 1
    heartbeat = json.loads((tmp_path / "ops" / "heartbeat.json").read_text(encoding="utf-8"))
    counters = heartbeat["job_counters"]["binance-btc-depth"]
    assert counters["success_count"] == 1
    assert counters["error_count"] == 0
    assert counters["retry_count"] == 2


def test_run_ops_runner_rejects_empty_enabled_job_set(tmp_path: Path) -> None:
    config_path = tmp_path / "ops-empty.json"
    config_path.write_text(
        json.dumps(
            {
                "jobs": [
                    {
                        "name": "mock-a",
                        "job_type": "mock",
                        "interval_seconds": 60,
                        "enabled": False,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="no enabled jobs"):
        run_ops_runner(
            build_parser().parse_args(
                ["ops-runner", "--config", str(config_path), "--ops-root", str(tmp_path / "ops")]
            )
        )


def test_run_ops_runner_rejects_duplicate_lock(tmp_path: Path) -> None:
    config_path = tmp_path / "ops.json"
    config_path.write_text(
        json.dumps(
            {
                "jobs": [
                    {
                        "name": "mock-a",
                        "job_type": "mock",
                        "interval_seconds": 60,
                        "args": {"count": 1},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    ops_root = tmp_path / "ops"
    ops_root.mkdir()
    (ops_root / "ops-runner.lock").write_text(
        json.dumps({"pid": os.getpid(), "runner_name": "collector-ops"}),
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="ops runner already active"):
        run_ops_runner(
            build_parser().parse_args(
                ["ops-runner", "--config", str(config_path), "--ops-root", str(ops_root)]
            )
        )


def test_run_ops_runner_clears_stale_lock(tmp_path: Path) -> None:
    config_path = tmp_path / "ops.json"
    config_path.write_text(
        json.dumps(
            {
                "jobs": [
                    {
                        "name": "mock-a",
                        "job_type": "mock",
                        "interval_seconds": 60,
                        "args": {"count": 1},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    ops_root = tmp_path / "ops"
    ops_root.mkdir()
    (ops_root / "ops-runner.lock").write_text(
        json.dumps({"pid": 999999, "runner_name": "collector-ops"}),
        encoding="utf-8",
    )

    run_ops_runner(
        build_parser().parse_args(
            [
                "ops-runner",
                "--config",
                str(config_path),
                "--ops-root",
                str(ops_root),
                "--max-runs",
                "1",
            ]
        )
    )

    assert not (ops_root / "ops-runner.lock").exists()


def test_ops_config_accepts_promote_replayable_job(tmp_path: Path) -> None:
    config_path = tmp_path / "ops-promote.json"
    config_path.write_text(
        json.dumps(
            {
                "jobs": [
                    {
                        "name": "promote-market",
                        "job_type": "promote-replayable",
                        "interval_seconds": 600,
                        "args": {
                            "source_root": r"D:\market_archive\raw\market\binance_depth",
                            "target_root": r"D:\market_archive\curated\research\market_replayable",
                            "limit": 10,
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    jobs = load_ops_config(config_path)
    assert len(jobs) == 1
    assert jobs[0].job_type == "promote-replayable"


def test_ops_config_accepts_quarantine_job(tmp_path: Path) -> None:
    config_path = tmp_path / "ops-quarantine.json"
    config_path.write_text(
        json.dumps(
            {
                "jobs": [
                    {
                        "name": "quarantine-market",
                        "job_type": "quarantine-runs",
                        "interval_seconds": 600,
                        "args": {
                            "source_root": r"D:\market_archive\raw\market\binance_depth",
                            "quarantine_root": r"D:\market_archive\quarantine\market\binance_depth",
                            "limit": 10,
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    jobs = load_ops_config(config_path)
    assert len(jobs) == 1
    assert jobs[0].job_type == "quarantine-runs"


def test_ops_config_accepts_binance_trades_worker_job(tmp_path: Path) -> None:
    config_path = tmp_path / "ops-binance-trades.json"
    config_path.write_text(
        json.dumps(
            {
                "jobs": [
                    {
                        "name": "binance-btc-trades",
                        "job_type": "binance-trades-worker",
                        "interval_seconds": 300,
                        "args": {
                            "symbol": "btcusdt",
                            "channel": "trade",
                            "segment_count": 5000,
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    jobs = load_ops_config(config_path)
    assert len(jobs) == 1
    assert jobs[0].job_type == "binance-trades-worker"


def test_ops_runner_refreshes_heartbeat_during_long_running_job(tmp_path: Path) -> None:
    runner = OpsRunner(tmp_path / "ops", poll_seconds=0, heartbeat_interval_seconds=0.1)
    jobs = [
        JobSpec(
            name="binance-btc-depth",
            job_type="binance-depth-worker",
            interval_seconds=3600,
        )
    ]

    def execute_job(_job: JobSpec) -> str:
        time.sleep(0.35)
        return "segment complete"

    thread = threading.Thread(
        target=runner.run,
        kwargs={"jobs": jobs, "execute_job": execute_job, "max_runs": 1},
        daemon=True,
    )
    thread.start()

    heartbeat_path = tmp_path / "ops" / "heartbeat.json"
    deadline = time.time() + 5.0
    while not heartbeat_path.exists():
        if time.time() >= deadline:
            raise AssertionError("heartbeat.json was not created")
        time.sleep(0.02)

    first = json.loads(heartbeat_path.read_text(encoding="utf-8"))
    time.sleep(0.18)
    second = json.loads(heartbeat_path.read_text(encoding="utf-8"))

    thread.join(timeout=5.0)
    assert not thread.is_alive()
    assert first["status"] == "running"
    assert second["status"] == "running"
    assert first["current_job"]["name"] == "binance-btc-depth"
    assert second["current_job"]["job_type"] == "binance-depth-worker"
    assert second["last_seen"] != first["last_seen"]

    final = json.loads(heartbeat_path.read_text(encoding="utf-8"))
    assert final["status"] == "stopped"


def test_binance_depth_worker_writes_lifecycle_logs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_collect(_args: object) -> dict[str, object]:
        return {
            "raw_messages": 5,
            "clean_events": 5,
            "quarantined_events": 0,
            "run_path": str(tmp_path / "raw" / "market" / "binance_depth" / "20260426_000000"),
            "connect_attempts": 1,
            "replayable": True,
            "replay_findings": [],
        }

    monkeypatch.setattr("crypto_collector.cli.collect_binance_depth_segment", fake_collect)
    args = build_parser().parse_args(
        [
            "binance-depth-worker",
            "--max-segments",
            "1",
            "--output-root",
            str(tmp_path / "raw" / "market"),
            "--ops-root",
            str(tmp_path / "ops"),
        ]
    )

    run_binance_depth_worker(args)

    heartbeat = json.loads(
        (tmp_path / "ops" / "standalone_workers" / "binance-depth-worker.json").read_text(encoding="utf-8")
    )
    assert heartbeat["status"] == "stopped"
    assert heartbeat["last_segment_index"] == 1
    assert heartbeat["last_run_path"].endswith("20260426_000000")

    rows = [
        json.loads(line)
        for line in (tmp_path / "ops" / "worker_events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert [row["event_type"] for row in rows] == [
        "worker_started",
        "segment_complete",
        "worker_stopped",
    ]


def test_public_cli_excludes_private_research_commands() -> None:
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["account-worker"])
    with pytest.raises(SystemExit):
        parser.parse_args(["private-research-runner"])
    with pytest.raises(SystemExit):
        parser.parse_args(["model-train"])


def test_standalone_segment_heartbeat_preserves_last_run_path(tmp_path: Path) -> None:
    runtime = StandaloneWorkerRuntime(
        tmp_path / "ops",
        worker_name="binance-trades-worker",
        worker_type="binance-trades-worker",
        venue="binance",
        symbol="BTCUSDT",
        heartbeat_interval_seconds=30.0,
    )

    stop_event, heartbeat_thread = runtime.start_segment_heartbeat(
        segment_index=2,
        started_at=utc_now(),
        last_segment_index=1,
        last_run_path="D:\\market_archive\\raw\\market\\binance_trades\\previous",
    )
    stop_event.set()
    heartbeat_thread.join(timeout=2.0)

    heartbeat = json.loads(
        (tmp_path / "ops" / "standalone_workers" / "binance-trades-worker.json").read_text(encoding="utf-8")
    )
    assert heartbeat["status"] == "running"
    assert heartbeat["last_segment_index"] == 1
    assert heartbeat["last_run_path"].endswith("previous")


def test_health_with_config_does_not_error_on_unmanaged_stale_workers(tmp_path: Path) -> None:
    ops_root = tmp_path / "ops"
    workers_root = ops_root / "standalone_workers"
    workers_root.mkdir(parents=True)
    now = datetime.now(tz=UTC)
    (ops_root / "heartbeat.json").write_text(
        json.dumps(
            {
                "status": "running",
                "last_seen": now.isoformat(),
                "job_counters": {},
            }
        ),
        encoding="utf-8",
    )
    (ops_root / "job_runs.jsonl").write_text(
        json.dumps(
            {
                "job_name": "binance-btc-depth",
                "job_type": "binance-depth-worker",
                "status": "success",
                "started_at": (now - timedelta(seconds=10)).isoformat(),
                "finished_at": now.isoformat(),
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (workers_root / "binance-depth-worker.json").write_text(
        json.dumps(
            {
                "worker_name": "binance-depth-worker",
                "worker_type": "binance-depth-worker",
                "status": "stopped",
                "pid": os.getpid(),
                "last_seen": now.isoformat(),
            }
        ),
        encoding="utf-8",
    )
    (workers_root / "binance-depth-worker-btcfdusd.json").write_text(
        json.dumps(
            {
                "worker_name": "binance-depth-worker-btcfdusd",
                "worker_type": "binance-depth-worker",
                "status": "running",
                "pid": 999999,
                "last_seen": (now - timedelta(days=7)).isoformat(),
            }
        ),
        encoding="utf-8",
    )

    jobs = [
        JobSpec(
            name="binance-btc-depth",
            job_type="binance-depth-worker",
            interval_seconds=3600,
            args={"worker_name": "binance-depth-worker"},
        )
    ]
    report = build_health_report(
        ops_root=ops_root,
        jobs=jobs,
        stale_after_seconds=120,
        job_stale_multiplier=3.0,
    )

    assert report.status == "ok"
    assert "unmanaged_stale_worker:binance-depth-worker-btcfdusd" in report.findings
    assert not any(item == "stale_worker:binance-depth-worker-btcfdusd" for item in report.findings)
    worker = next(row for row in report.standalone_workers if row["name"] == "binance-depth-worker-btcfdusd")
    assert worker["managed"] is False
    assert worker["blocking"] is False


def test_health_without_config_keeps_legacy_all_worker_blocking_behavior(tmp_path: Path) -> None:
    ops_root = tmp_path / "ops"
    workers_root = ops_root / "standalone_workers"
    workers_root.mkdir(parents=True)
    now = datetime.now(tz=UTC)
    (ops_root / "heartbeat.json").write_text(
        json.dumps({"status": "running", "last_seen": now.isoformat(), "job_counters": {}}),
        encoding="utf-8",
    )
    (workers_root / "old-worker.json").write_text(
        json.dumps(
            {
                "worker_name": "old-worker",
                "worker_type": "old",
                "status": "running",
                "pid": 999999,
                "last_seen": (now - timedelta(days=7)).isoformat(),
            }
        ),
        encoding="utf-8",
    )

    report = build_health_report(ops_root=ops_root, jobs=None, stale_after_seconds=120)

    assert report.status == "error"
    assert "stale_worker:old-worker" in report.findings


def test_prune_stale_worker_artifacts_dry_run_and_apply(tmp_path: Path) -> None:
    ops_root = tmp_path / "ops"
    workers_root = ops_root / "standalone_workers"
    logs_root = workers_root / "logs"
    logs_root.mkdir(parents=True)
    old_seen = (datetime.now(tz=UTC) - timedelta(days=7)).isoformat()
    (workers_root / "old-fdusd.json").write_text(
        json.dumps({"worker_name": "old-fdusd", "status": "running", "last_seen": old_seen, "pid": 999999}),
        encoding="utf-8",
    )
    (workers_root / "old-fdusd.lock").write_text("stale", encoding="utf-8")
    (logs_root / "old-fdusd.out.log").write_text("out", encoding="utf-8")
    (workers_root / "binance-depth-worker.json").write_text(
        json.dumps({"worker_name": "binance-depth-worker", "status": "running", "last_seen": old_seen, "pid": 999999}),
        encoding="utf-8",
    )

    dry_run = prune_stale_worker_artifacts(
        ops_root=ops_root,
        stale_after_days=2,
        apply=False,
        managed_worker_names={"binance-depth-worker"},
    )
    assert dry_run.mode == "dry-run"
    assert dry_run.candidate_count == 1
    assert dry_run.candidates[0].worker_name == "old-fdusd"
    assert (workers_root / "old-fdusd.json").exists()

    applied = prune_stale_worker_artifacts(
        ops_root=ops_root,
        stale_after_days=2,
        apply=True,
        managed_worker_names={"binance-depth-worker"},
    )
    assert applied.mode == "apply"
    assert applied.candidate_count == 1
    assert applied.moved_count == 3
    assert not (workers_root / "old-fdusd.json").exists()
    assert (workers_root / "binance-depth-worker.json").exists()
    assert list((ops_root / "archived_standalone_workers").rglob("old-fdusd.json"))
