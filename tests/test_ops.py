from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
import threading
import time

import pytest

from crypto_collector.cli import (
    _execute_ops_job,
    _execute_ops_job_inprocess,
    _job_args,
    _ops_root_from_jobs,
    _segment_deadline_utc,
    build_parser,
    default_archive_root,
    run_binance_depth_worker,
    run_health,
    run_ops_runner,
    run_single_job,
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


_REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("config_name", ["ops.live.local.json", "ops.live.example.json"])
def test_live_config_job_types_are_all_dispatchable(config_name: str) -> None:
    """Every job_type shipped in a live ops config must have an arg builder in _job_args
    (which raises ValueError on unknown types). This guards the exact regression this
    catch-up work fixes: a scorer job_type living in the config that the runner can't
    actually dispatch (backfill-trades-replay / backfill-stream-depth used to be
    CLI-only). _job_args and _execute_ops_job_inprocess enumerate the same job_types, so
    a passing build here means the runner can dispatch it."""
    config_path = _REPO_ROOT / config_name
    if not config_path.exists():
        pytest.skip(f"{config_name} not present")
    jobs = load_ops_config(config_path)
    for job in jobs:
        # Raises ValueError("Unsupported job_type: ...") if the type has no builder.
        assert _job_args(job) is not None, job.name
    # The scoring catch-up lanes that self-heal cut-off segments must be wired in.
    by_type: dict[str, list[str]] = {}
    for job in jobs:
        by_type.setdefault(job.job_type, []).append(job.name)
    assert "backfill-replay" in by_type  # binance_depth scorer
    assert len(by_type.get("backfill-trades-replay", [])) == 5  # 5 trades lanes
    assert "backfill-stream-depth" in by_type  # 4 non-binance depth lanes (one job)


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


def test_startup_task_installer_disables_execution_time_limit() -> None:
    script = Path("scripts/install_startup_task.ps1").read_text(encoding="utf-8")

    assert "-ExecutionTimeLimit (New-TimeSpan -Seconds 0)" in script


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
    # A genuinely-active runner = alive pid AND a fresh heartbeat. Both are required to
    # reject a duplicate (the heartbeat distinguishes a live runner from a recycled pid).
    (ops_root / "heartbeat.json").write_text(
        json.dumps({"status": "running", "last_seen": datetime.now(tz=UTC).isoformat()}),
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="ops runner already active"):
        run_ops_runner(
            build_parser().parse_args(
                ["ops-runner", "--config", str(config_path), "--ops-root", str(ops_root)]
            )
        )


def test_run_ops_runner_reclaims_lock_when_pid_alive_but_heartbeat_stale(tmp_path: Path) -> None:
    """Regression: a recycled pid (Windows OpenProcess -> access-denied -> _pid_exists
    reports 'alive') must NOT strand the runner on a phantom lock. If the heartbeat is
    stale, the previous runner is dead and the lock is reclaimed. Uses os.getpid() (a real,
    live pid) plus a stale heartbeat to simulate the recycled-pid case."""
    config_path = tmp_path / "ops.json"
    config_path.write_text(
        json.dumps({"jobs": [{"name": "mock-a", "job_type": "mock", "interval_seconds": 60, "args": {"count": 1}}]}),
        encoding="utf-8",
    )
    ops_root = tmp_path / "ops"
    ops_root.mkdir()
    (ops_root / "ops-runner.lock").write_text(
        json.dumps({"pid": os.getpid(), "runner_name": "collector-ops"}),
        encoding="utf-8",
    )
    (ops_root / "heartbeat.json").write_text(
        json.dumps({"status": "running", "last_seen": (datetime.now(tz=UTC) - timedelta(seconds=600)).isoformat()}),
        encoding="utf-8",
    )

    # Should NOT raise — stale heartbeat means the prior runner is dead; the lock is reclaimed.
    run_ops_runner(
        build_parser().parse_args(
            ["ops-runner", "--config", str(config_path), "--ops-root", str(ops_root), "--max-runs", "1"]
        )
    )
    assert not (ops_root / "ops-runner.lock").exists()


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


def test_health_with_config_does_not_error_on_unmanaged_stale_workers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Point the normalized-partition check at a sandboxed root with a fresh write so
    # the report does not depend on whether the live D:\market_archive has recent data.
    normalized_root = tmp_path / "normalized"
    monkeypatch.setenv("MARKET_DATA_NORMALIZED_ROOT", str(normalized_root))
    partition = normalized_root / "market" / "schema_version=1" / "source=binance"
    partition.mkdir(parents=True)
    (partition / "part-0.parquet").write_bytes(b"")

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


def test_health_reports_poll_lane_freshness_for_kalshi(tmp_path: Path) -> None:
    """Poll-based collectors (Kalshi) run as interval jobs, not WS workers, so they
    never appear in standalone_workers. They must surface under poll_lanes with
    freshness so they aren't a monitoring blind spot."""
    ops_root = tmp_path / "ops"
    (ops_root / "standalone_workers").mkdir(parents=True)
    now = datetime.now(tz=UTC)
    (ops_root / "heartbeat.json").write_text(
        json.dumps(
            {
                "status": "running",
                "last_seen": now.isoformat(),
                "job_counters": {"kalshi-crypto-quotes": {"success_count": 67, "error_count": 0}},
                "next_run_at": {"kalshi-crypto-quotes": (now + timedelta(seconds=30)).isoformat()},
            }
        ),
        encoding="utf-8",
    )
    (ops_root / "job_runs.jsonl").write_text(
        json.dumps(
            {
                "job_name": "kalshi-crypto-quotes",
                "job_type": "kalshi-collect-crypto-quotes",
                "status": "success",
                "started_at": (now - timedelta(seconds=40)).isoformat(),
                "finished_at": (now - timedelta(seconds=20)).isoformat(),
            }
        )
        + "\n",
        encoding="utf-8",
    )
    jobs = [
        JobSpec(name="kalshi-crypto-quotes", job_type="kalshi-collect-crypto-quotes", interval_seconds=60)
    ]
    report = build_health_report(
        ops_root=ops_root, jobs=jobs, stale_after_seconds=120, job_stale_multiplier=2.5
    )

    lane = next((row for row in report.poll_lanes if row["name"] == "kalshi-crypto-quotes"), None)
    assert lane is not None, report.poll_lanes
    assert lane["job_type"] == "kalshi-collect-crypto-quotes"
    assert lane["interval_seconds"] == 60
    assert lane["stale"] is False
    assert lane["status"] == "success"
    assert lane["age_seconds"] is not None and lane["age_seconds"] < 150
    assert lane["next_run_at"] is not None
    assert lane["success_count"] == 67
    assert "stale_job:kalshi-crypto-quotes" not in report.findings


def test_segment_deadline_utc_time_bounded() -> None:
    """Continuous capture relies on TIME-based segment rotation: a positive
    max_segment_seconds bounds each segment regardless of message volume, while a
    zero/None bound (and no midnight rotation) means no deadline. rotate_at_midnight
    takes precedence so day-bounded files keep working."""
    start = datetime(2026, 6, 8, 12, 0, 0, tzinfo=UTC)
    # Fixed-cadence time bound -> deadline at start + window.
    assert _segment_deadline_utc(
        start, rotate_at_midnight=False, max_segment_seconds=1800
    ) == start + timedelta(seconds=1800)
    # Disabled -> no deadline (segment ends only on segment_count).
    assert _segment_deadline_utc(start, rotate_at_midnight=False, max_segment_seconds=0.0) is None
    assert _segment_deadline_utc(start, rotate_at_midnight=False, max_segment_seconds=0) is None
    # Midnight rotation wins even if a time bound is also set.
    assert _segment_deadline_utc(
        start, rotate_at_midnight=True, max_segment_seconds=1800
    ) == datetime(2026, 6, 9, 0, 0, 0, tzinfo=UTC)


def test_execute_ops_job_inprocess_injects_max_segment_seconds(monkeypatch) -> None:
    """The runner must thread max_segment_seconds from the job config onto the worker
    args for every collector lane, so time-based rotation actually reaches the worker."""
    import crypto_collector.cli as cli_mod

    captured = {}
    monkeypatch.setattr(cli_mod, "run_kraken_trades_worker", lambda args: captured.update(args=args))
    job = JobSpec(
        name="kraken-btc-trades",
        job_type="kraken-trades-worker",
        interval_seconds=5,
        args={"symbol": "BTC/USD", "max_segment_seconds": 1800, "ops_root": r"G:\x\ops"},
    )
    _execute_ops_job_inprocess(job)
    assert captured["args"].max_segment_seconds == 1800


def test_ops_root_from_jobs_prefers_config_root() -> None:
    """The health CLI derives its ops root from the discovered config's job args so a
    bare `health` follows the live collection root rather than the env/default fallback.
    Jobs without an ops_root arg (maintenance jobs) are ignored; the most common root
    across collector jobs wins."""
    jobs = [
        JobSpec(name="cb", job_type="coinbase-trades-worker", interval_seconds=60, args={"ops_root": r"G:\live\ops"}),
        JobSpec(name="bn", job_type="binance-depth-worker", interval_seconds=60, args={"ops_root": r"G:\live\ops"}),
        JobSpec(name="promote", job_type="promote-market", interval_seconds=60),
    ]
    assert _ops_root_from_jobs(jobs) == Path(r"G:\live\ops")
    assert _ops_root_from_jobs([]) is None
    assert _ops_root_from_jobs(None) is None


def test_health_without_ops_root_follows_config_root(tmp_path, monkeypatch, capsys) -> None:
    """Regression: after the D:->G: migration, a bare `health` (no --ops-root) read the
    stale env/default root via default_ops_root() and falsely reported errors. With a
    config discovered, it must inspect the ops root that config writes to instead."""
    import crypto_collector.cli as cli_mod

    live_ops = tmp_path / "live" / "ops"
    (live_ops / "standalone_workers").mkdir(parents=True)
    now = datetime.now(tz=UTC)
    (live_ops / "heartbeat.json").write_text(
        json.dumps(
            {
                "status": "running",
                "last_seen": now.isoformat(),
                "job_counters": {"kalshi-crypto-quotes": {"success_count": 5, "error_count": 0}},
                "next_run_at": {"kalshi-crypto-quotes": (now + timedelta(seconds=30)).isoformat()},
            }
        ),
        encoding="utf-8",
    )
    (live_ops / "job_runs.jsonl").write_text(
        json.dumps(
            {
                "job_name": "kalshi-crypto-quotes",
                "job_type": "kalshi-collect-crypto-quotes",
                "status": "success",
                "started_at": (now - timedelta(seconds=40)).isoformat(),
                "finished_at": (now - timedelta(seconds=20)).isoformat(),
            }
        )
        + "\n",
        encoding="utf-8",
    )
    config_path = tmp_path / "ops.json"
    config_path.write_text(
        json.dumps(
            {
                "jobs": [
                    {
                        "name": "kalshi-crypto-quotes",
                        "job_type": "kalshi-collect-crypto-quotes",
                        "interval_seconds": 60,
                        "args": {"ops_root": str(live_ops)},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    # A bogus default proves the CLI does NOT fall back to it when a config is present.
    bogus_root = tmp_path / "stale" / "ops"
    monkeypatch.setattr(cli_mod, "default_ops_root", lambda: bogus_root)

    captured: dict[str, Path] = {}
    real_build = cli_mod.build_health_report

    def spy(*spy_args, **spy_kwargs):
        captured["ops_root"] = spy_kwargs.get("ops_root")
        return real_build(*spy_args, **spy_kwargs)

    monkeypatch.setattr(cli_mod, "build_health_report", spy)

    args = build_parser().parse_args(["health", "--config", str(config_path)])
    run_health(args)

    assert captured["ops_root"] == live_ops
    assert captured["ops_root"] != bogus_root
    # End-to-end: the poll lane from the live heartbeat is rendered.
    out = capsys.readouterr().out
    assert "kalshi-crypto-quotes" in out


def test_health_flags_stale_poll_lane(tmp_path: Path) -> None:
    """A Kalshi lane that hasn't finished within interval*multiplier is marked stale
    in poll_lanes and flagged, so a stalled poll collector is caught."""
    ops_root = tmp_path / "ops"
    (ops_root / "standalone_workers").mkdir(parents=True)
    now = datetime.now(tz=UTC)
    (ops_root / "heartbeat.json").write_text(
        json.dumps({"status": "running", "last_seen": now.isoformat(), "job_counters": {}}),
        encoding="utf-8",
    )
    (ops_root / "job_runs.jsonl").write_text(
        json.dumps(
            {
                "job_name": "kalshi-crypto-quotes",
                "job_type": "kalshi-collect-crypto-quotes",
                "status": "success",
                "started_at": (now - timedelta(seconds=2000)).isoformat(),
                "finished_at": (now - timedelta(seconds=1800)).isoformat(),
            }
        )
        + "\n",
        encoding="utf-8",
    )
    jobs = [
        JobSpec(name="kalshi-crypto-quotes", job_type="kalshi-collect-crypto-quotes", interval_seconds=60)
    ]
    report = build_health_report(
        ops_root=ops_root, jobs=jobs, stale_after_seconds=120, job_stale_multiplier=2.5
    )

    lane = next(row for row in report.poll_lanes if row["name"] == "kalshi-crypto-quotes")
    assert lane["stale"] is True
    assert "stale_job:kalshi-crypto-quotes" in report.findings


def test_execute_ops_job_runs_collector_in_subprocess(monkeypatch: pytest.MonkeyPatch) -> None:
    """Collector jobs must run in a separate OS process (GIL isolation), passing the job
    as JSON to the `run-job` child entrypoint."""
    import crypto_collector.cli as cli_mod

    captured: dict = {}

    class _Proc:
        returncode = 0
        stdout = ""
        stderr = ""

    monkeypatch.setattr(cli_mod.subprocess, "run", lambda argv, **kw: (captured.update(argv=argv), _Proc())[1])
    job = JobSpec(name="coinbase-btc-trades", job_type="coinbase-trades-worker", interval_seconds=3600, args={"symbol": "BTC-USD"})

    res = _execute_ops_job(job)
    assert "process-isolated" in str(res)
    argv = captured["argv"]
    assert argv[0].endswith("python") or argv[0].endswith("python.exe") or "python" in argv[0]
    assert argv[1:4] == ["-m", "crypto_collector.cli", "run-job"]
    payload = json.loads(argv[argv.index("--job-json") + 1])
    assert payload["job_type"] == "coinbase-trades-worker"
    assert payload["args"]["symbol"] == "BTC-USD"


def test_execute_ops_job_subprocess_failure_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """A non-zero collector subprocess exit must raise so the runner records an error."""
    import crypto_collector.cli as cli_mod

    class _Proc:
        returncode = 2
        stdout = ""
        stderr = "kaboom"

    monkeypatch.setattr(cli_mod.subprocess, "run", lambda *a, **k: _Proc())
    job = JobSpec(name="x", job_type="bybit-trades-worker", interval_seconds=3600, args={})
    with pytest.raises(RuntimeError, match="exited 2"):
        _execute_ops_job(job)


def test_execute_ops_job_runs_maintenance_in_process(monkeypatch: pytest.MonkeyPatch) -> None:
    """Maintenance jobs stay in-process — no subprocess spawned."""
    import crypto_collector.cli as cli_mod

    def _no_subprocess(*a, **k):
        raise AssertionError("maintenance jobs must not spawn a subprocess")

    monkeypatch.setattr(cli_mod.subprocess, "run", _no_subprocess)
    seen: dict = {}

    def _fake_inproc(job):
        seen["job"] = job
        return "ok"

    monkeypatch.setattr(cli_mod, "_execute_ops_job_inprocess", _fake_inproc)
    job = JobSpec(name="quarantine-market", job_type="quarantine-runs", interval_seconds=300, args={})

    assert _execute_ops_job(job) == "ok"
    assert seen["job"].job_type == "quarantine-runs"


def test_run_single_job_reconstructs_and_dispatches_in_process(monkeypatch: pytest.MonkeyPatch) -> None:
    """The run-job child reconstructs the JobSpec from JSON and runs the in-process path."""
    import crypto_collector.cli as cli_mod

    seen: dict = {}
    monkeypatch.setattr(cli_mod, "_execute_ops_job_inprocess", lambda job: seen.setdefault("job", job))
    payload = json.dumps({"name": "c", "job_type": "coinbase-trades-worker", "interval_seconds": 3600, "args": {"symbol": "BTC-USD"}})
    args = build_parser().parse_args(["run-job", "--job-json", payload])

    run_single_job(args)
    assert seen["job"].job_type == "coinbase-trades-worker"
    assert seen["job"].args["symbol"] == "BTC-USD"


def test_execute_ops_job_inprocess_dispatches_backfill_trades_replay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The trades scorer must be dispatchable as an ops job so cut-off trade segments
    that never got an inline replay summary get scored by a live catch-up job (the old
    code only knew the depth scorers, so promote-replayable never saw these runs)."""
    import crypto_collector.cli as cli_mod

    captured: dict = {}
    monkeypatch.setattr(cli_mod, "run_backfill_trades_replay", lambda args: captured.update(args=args))
    job = JobSpec(
        name="score-bybit-trades",
        job_type="backfill-trades-replay",
        interval_seconds=3600,
        args={"source_root": r"G:\x\raw\market\bybit_trades", "stream": True, "limit": 1000},
    )

    assert _execute_ops_job_inprocess(job) == "backfill trades replay completed"
    assert captured["args"].source_root == Path(r"G:\x\raw\market\bybit_trades")
    assert captured["args"].stream is True
    assert captured["args"].limit == 1000


def test_execute_ops_job_inprocess_dispatches_backfill_stream_depth(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The non-binance depth scorer must be dispatchable as an ops job, defaulting to
    score-only so the live catch-up job writes replay summaries WITHOUT promoting —
    promotion stays single-sourced in the quarantine-aware promote-replayable jobs."""
    import crypto_collector.cli as cli_mod

    captured: dict = {}
    monkeypatch.setattr(cli_mod, "run_backfill_stream_depth", lambda args: captured.update(args=args))
    job = JobSpec(
        name="score-stream-depth",
        job_type="backfill-stream-depth",
        interval_seconds=3600,
        args={
            "raw_root": r"G:\x\raw\market",
            "source": ["coinbase_depth", "bybit_depth", "kraken_depth", "mexc_depth"],
            "limit": 1000,
        },
    )

    assert _execute_ops_job_inprocess(job) == "backfill stream depth completed"
    assert captured["args"].raw_root == Path(r"G:\x\raw\market")
    assert captured["args"].source == ["coinbase_depth", "bybit_depth", "kraken_depth", "mexc_depth"]
    # Ops job defaults to score-only: never promotes from the scorer.
    assert captured["args"].score_only is True
    assert captured["args"].apply is False


def test_job_args_backfill_trades_replay_defaults_and_stream() -> None:
    """_job_args wires the trades scorer: defaults to binance_trades + dense scorer,
    honors source_root / stream / limit / max_age_hours overrides per lane."""
    defaults = _job_args(
        JobSpec(name="x", job_type="backfill-trades-replay", interval_seconds=3600, args={})
    )
    assert defaults.source_root == default_archive_root() / "raw" / "market" / "binance_trades"
    assert defaults.stream is False
    assert defaults.overwrite is False
    assert defaults.limit == 50
    assert defaults.max_age_hours == 24.0

    overridden = _job_args(
        JobSpec(
            name="x",
            job_type="backfill-trades-replay",
            interval_seconds=3600,
            args={
                "source_root": r"G:\x\raw\market\mexc_trades",
                "stream": True,
                "limit": 1000,
                "max_age_hours": 168,
            },
        )
    )
    assert overridden.source_root == Path(r"G:\x\raw\market\mexc_trades")
    assert overridden.stream is True
    assert overridden.limit == 1000
    assert overridden.max_age_hours == 168


def test_job_args_backfill_stream_depth_defaults_to_score_only() -> None:
    """_job_args defaults the stream-depth scorer to score_only=True (write summaries,
    no promote) so the live catch-up job can't double-promote against the
    promote-replayable jobs. Config can still flip score_only / apply / source."""
    defaults = _job_args(
        JobSpec(name="x", job_type="backfill-stream-depth", interval_seconds=3600, args={})
    )
    assert isinstance(defaults.raw_root, Path)
    assert defaults.source == ["coinbase_depth", "bybit_depth", "kraken_depth"]
    assert defaults.score_only is True
    assert defaults.apply is False
    assert defaults.limit == 200
    assert defaults.max_age_hours == 720.0

    overridden = _job_args(
        JobSpec(
            name="x",
            job_type="backfill-stream-depth",
            interval_seconds=3600,
            args={
                "raw_root": r"G:\x\raw\market",
                "source": ["coinbase_depth", "bybit_depth", "kraken_depth", "mexc_depth"],
                "score_only": False,
                "apply": True,
                "limit": 1000,
            },
        )
    )
    assert overridden.raw_root == Path(r"G:\x\raw\market")
    assert overridden.source == ["coinbase_depth", "bybit_depth", "kraken_depth", "mexc_depth"]
    assert overridden.score_only is False
    assert overridden.apply is True
    assert overridden.limit == 1000


@pytest.mark.parametrize(
    "job_type",
    ["coinbase-trades-worker", "kraken-trades-worker", "bybit-trades-worker", "mexc-trades-worker"],
)
def test_job_args_trades_default_to_buffered_jsonl(job_type: str) -> None:
    """High-volume trade lanes must default to buffered JSONL (jsonl_fsync=False).
    Per-event fsync throttled the consumer below the feed rate, growing the backlog
    past the 60s freshness gate so valid trades were quarantined as stale."""
    args = _job_args(JobSpec(name="x", job_type=job_type, interval_seconds=3600, args={}))
    assert args.jsonl_fsync is False
    # Config can still force fsync back on per lane.
    args_on = _job_args(
        JobSpec(name="x", job_type=job_type, interval_seconds=3600, args={"jsonl_fsync": True})
    )
    assert args_on.jsonl_fsync is True


@pytest.mark.parametrize(
    "job_type",
    [
        "binance-trades-worker",
        "coinbase-trades-worker",
        "kraken-trades-worker",
        "bybit-trades-worker",
        "mexc-trades-worker",
    ],
)
def test_job_args_trades_widen_stale_window(job_type: str) -> None:
    """Trades keep a wide stale gate (15 min) so late-but-valid trades aren't quarantined
    under disk-I/O backlog, while the clock-skew (future) bound stays tight at 5s. Config
    can still override per lane."""
    args = _job_args(JobSpec(name="x", job_type=job_type, interval_seconds=300, args={}))
    assert args.max_delay_ms == 900_000
    assert args.max_future_skew_ms == 5_000
    overridden = _job_args(JobSpec(name="x", job_type=job_type, interval_seconds=300, args={"max_delay_ms": 60_000}))
    assert overridden.max_delay_ms == 60_000


def test_health_running_worker_reports_in_progress_quarantine_ratio(tmp_path: Path) -> None:
    """A running worker exposes no run path mid-segment, so quarantine_ratio used to be
    None (blind spot). Health should fall back to the latest run dir's clean/quarantine
    event counts (derived from ops_root + worker_type) and report a real ratio."""
    ops_root = tmp_path / "ops"
    (ops_root / "standalone_workers").mkdir(parents=True)
    now = datetime.now(tz=UTC)
    (ops_root / "standalone_workers" / "coinbase-trades-worker.json").write_text(
        json.dumps(
            {
                "worker_name": "coinbase-trades-worker",
                "worker_type": "coinbase-trades-worker",
                "status": "running",
                "pid": os.getpid(),
                "last_seen": now.isoformat(),
                "last_run_path": None,
            }
        ),
        encoding="utf-8",
    )
    run = tmp_path / "raw" / "market" / "coinbase_trades" / "20260608_120000"
    (run / "clean").mkdir(parents=True)
    (run / "quarantine").mkdir(parents=True)
    (run / "clean" / "events.jsonl").write_text("".join('{"e":1}\n' for _ in range(98)), encoding="utf-8")
    (run / "quarantine" / "events.jsonl").write_text("".join('{"e":1}\n' for _ in range(2)), encoding="utf-8")

    report = build_health_report(ops_root=ops_root, jobs=None, stale_after_seconds=300)
    w = next(r for r in report.standalone_workers if r["name"] == "coinbase-trades-worker")
    assert w["quarantine_ratio"] == 0.02  # 2 / (98+2)
    assert (w["partial_metrics"] or {}).get("source") == "in_progress_run_dir"


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


def _write_summary_jsonl(run_path: Path, rows: list[dict]) -> None:
    (run_path / "metrics").mkdir(parents=True)
    (run_path / "metrics" / "summary.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in rows),
        encoding="utf-8",
    )


def _write_trade_replay_summary(
    run_path: Path,
    *,
    replayable: bool = True,
    findings: list[str] | None = None,
    gap_missing: int = 0,
) -> None:
    (run_path / "metrics").mkdir(parents=True, exist_ok=True)
    (run_path / "metrics" / "replay_summary.json").write_text(
        json.dumps(
            {
                "replay_type": "trades",
                "source": "binance",
                "replayable": replayable,
                "findings": findings or [],
                "trade_id_gap_count": 1 if gap_missing else 0,
                "trade_id_gap_total_missing": gap_missing,
            }
        ),
        encoding="utf-8",
    )


def _write_binance_trade_run(
    archive_root: Path,
    run_name: str,
    *,
    raw: int = 300,
    clean: int = 300,
    quarantined: int = 0,
    replayable: bool = True,
    findings: list[str] | None = None,
    promoted_rows: int = 0,
) -> Path:
    run_path = archive_root / "raw" / "market" / "binance_trades" / run_name
    run_path.mkdir(parents=True)
    _write_summary_jsonl(
        run_path,
        [
            {
                "raw_messages": raw,
                "clean_events": clean,
                "quarantined_events": quarantined,
                "partial": False,
            }
        ],
    )
    _write_trade_replay_summary(
        run_path,
        replayable=replayable,
        findings=findings,
        gap_missing=10 if findings and "trade_id_gaps" in findings else 0,
    )
    if promoted_rows:
        target_root = archive_root / "curated" / "research" / "trades_replayable"
        target_root.mkdir(parents=True, exist_ok=True)
        with (target_root / "_promotion_index.jsonl").open("a", encoding="utf-8") as handle:
            handle.write(
                json.dumps({"run_path": str(run_path), "promoted_rows": promoted_rows})
                + "\n"
            )
    return run_path


def _run_name_at(dt: datetime) -> str:
    return dt.strftime("%Y%m%d_%H%M%S")


def test_health_reports_healthy_recent_binance_trades_quality(tmp_path: Path) -> None:
    archive_root = tmp_path
    ops_root = archive_root / "ops"
    ops_root.mkdir()
    now = datetime.now(tz=UTC)
    (ops_root / "heartbeat.json").write_text(
        json.dumps({"status": "running", "last_seen": now.isoformat(), "job_counters": {}}),
        encoding="utf-8",
    )
    _write_binance_trade_run(
        archive_root,
        _run_name_at(now - timedelta(minutes=1)),
        clean=300,
        quarantined=0,
        promoted_rows=300,
    )

    report = build_health_report(ops_root=ops_root, jobs=None, stale_after_seconds=300)

    assert "binance_trades_no_replayable_30m" not in report.findings
    assert "binance_trades_latest_unreplayable" not in report.findings
    assert report.binance_trades is not None
    assert report.binance_trades["checked_run_count"] == 1
    assert report.binance_trades["replayable_run_count"] == 1
    assert report.binance_trades["total_promoted_rows"] == 300


def test_health_flags_latest_unreplayable_binance_trade_run(tmp_path: Path) -> None:
    archive_root = tmp_path
    ops_root = archive_root / "ops"
    ops_root.mkdir()
    now = datetime.now(tz=UTC)
    (ops_root / "heartbeat.json").write_text(
        json.dumps({"status": "running", "last_seen": now.isoformat(), "job_counters": {}}),
        encoding="utf-8",
    )
    _write_binance_trade_run(archive_root, _run_name_at(now - timedelta(minutes=2)))
    _write_binance_trade_run(
        archive_root,
        _run_name_at(now - timedelta(minutes=1)),
        clean=200,
        quarantined=100,
        replayable=False,
        findings=["trade_id_gaps"],
    )

    report = build_health_report(ops_root=ops_root, jobs=None, stale_after_seconds=300)

    assert "binance_trades_latest_unreplayable" in report.findings
    assert "binance_trades_no_replayable_30m" not in report.findings
    assert report.status == "warn"
    assert report.binance_trades["latest_run_replayable"] is False


def test_health_flags_no_recent_replayable_binance_trades(tmp_path: Path) -> None:
    archive_root = tmp_path
    ops_root = archive_root / "ops"
    ops_root.mkdir()
    now = datetime.now(tz=UTC)
    (ops_root / "heartbeat.json").write_text(
        json.dumps({"status": "running", "last_seen": now.isoformat(), "job_counters": {}}),
        encoding="utf-8",
    )
    _write_binance_trade_run(archive_root, _run_name_at(now - timedelta(hours=1)))

    report = build_health_report(ops_root=ops_root, jobs=None, stale_after_seconds=300)

    assert "binance_trades_no_replayable_30m" in report.findings
    assert report.status == "warn"


def test_health_flags_three_low_clean_ratio_binance_trade_runs(tmp_path: Path) -> None:
    archive_root = tmp_path
    ops_root = archive_root / "ops"
    ops_root.mkdir()
    now = datetime.now(tz=UTC)
    (ops_root / "heartbeat.json").write_text(
        json.dumps({"status": "running", "last_seen": now.isoformat(), "job_counters": {}}),
        encoding="utf-8",
    )
    for minutes_ago in (3, 2, 1):
        _write_binance_trade_run(
            archive_root,
            _run_name_at(now - timedelta(minutes=minutes_ago)),
            clean=20,
            quarantined=280,
        )

    report = build_health_report(ops_root=ops_root, jobs=None, stale_after_seconds=300)

    assert "binance_trades_low_clean_ratio" in report.findings
    assert report.status == "warn"


def test_health_surfaces_partial_metrics_from_active_run(tmp_path: Path) -> None:
    """Per FOLLOW_UPS #4: an operator should see the in-flight reject ratio without
    waiting for the run to finish. build_health_report should read the latest
    summary.jsonl row of the active worker's current_run_path."""
    ops_root = tmp_path / "ops"
    workers_root = ops_root / "standalone_workers"
    workers_root.mkdir(parents=True)
    now = datetime.now(tz=UTC)
    (ops_root / "heartbeat.json").write_text(
        json.dumps({"status": "running", "last_seen": now.isoformat(), "job_counters": {}}),
        encoding="utf-8",
    )

    run_path = tmp_path / "raw" / "market" / "binance_depth" / "20260527_120000"
    run_path.mkdir(parents=True)
    _write_summary_jsonl(
        run_path,
        [
            {"raw_messages": 100, "clean_events": 90, "quarantined_events": 10, "partial": True},
            # Latest row — in-flight metrics
            {
                "raw_messages": 500,
                "clean_events": 350,
                "quarantined_events": 150,  # 30% — above default threshold
                "reject_counts": {"clock_skew": 150},
                "partial": True,
            },
        ],
    )
    (workers_root / "binance-depth-worker.json").write_text(
        json.dumps(
            {
                "worker_name": "binance-depth-worker",
                "worker_type": "binance-depth-worker",
                "status": "running",
                "pid": os.getpid(),
                "last_seen": now.isoformat(),
                "current_segment": {
                    "index": 1,
                    "started_at": now.isoformat(),
                    "run_path": str(run_path),
                },
            }
        ),
        encoding="utf-8",
    )

    report = build_health_report(
        ops_root=ops_root,
        jobs=None,
        stale_after_seconds=300,
        quarantine_ratio_threshold=0.20,
    )

    worker = next(
        row for row in report.standalone_workers if row["name"] == "binance-depth-worker"
    )
    # The in-flight partial metrics are now visible
    assert worker["partial_metrics"] is not None
    assert worker["partial_metrics"]["raw_messages"] == 500
    assert worker["partial_metrics"]["quarantined_events"] == 150
    # Ratio is computed and reported
    assert worker["quarantine_ratio"] is not None
    assert abs(worker["quarantine_ratio"] - 0.30) < 1e-9
    # High-quarantine finding is added since 0.30 > 0.20 threshold
    assert "high_quarantine_ratio" in worker["findings"]
    assert any(
        item.startswith("high_quarantine_ratio:") or item.startswith("unmanaged_high_quarantine_ratio:")
        for item in report.findings
    )


def test_health_does_not_flag_high_quarantine_for_stopped_workers(tmp_path: Path) -> None:
    """A historical run with high reject rate is not an in-flight problem.
    Stopped workers shouldn't add high_quarantine_ratio findings."""
    ops_root = tmp_path / "ops"
    workers_root = ops_root / "standalone_workers"
    workers_root.mkdir(parents=True)
    now = datetime.now(tz=UTC)
    (ops_root / "heartbeat.json").write_text(
        json.dumps({"status": "running", "last_seen": now.isoformat(), "job_counters": {}}),
        encoding="utf-8",
    )

    run_path = tmp_path / "raw" / "market" / "binance_depth" / "20260527_110000"
    run_path.mkdir(parents=True)
    _write_summary_jsonl(
        run_path,
        [
            {
                "raw_messages": 100,
                "clean_events": 40,
                "quarantined_events": 60,  # 60% — would trip threshold if active
                "partial": False,
            }
        ],
    )
    (workers_root / "binance-depth-worker.json").write_text(
        json.dumps(
            {
                "worker_name": "binance-depth-worker",
                "worker_type": "binance-depth-worker",
                "status": "stopped",  # not active
                "pid": os.getpid(),
                "last_seen": now.isoformat(),
                "last_run_path": str(run_path),
            }
        ),
        encoding="utf-8",
    )

    report = build_health_report(
        ops_root=ops_root,
        jobs=None,
        stale_after_seconds=300,
        quarantine_ratio_threshold=0.20,
    )

    worker = next(
        row for row in report.standalone_workers if row["name"] == "binance-depth-worker"
    )
    # The historical metrics are still surfaced (useful for context)
    assert worker["quarantine_ratio"] is not None
    assert worker["quarantine_ratio"] > 0.50
    # But no finding fires — the worker isn't active
    assert "high_quarantine_ratio" not in worker["findings"]
    assert not any(
        "high_quarantine_ratio:binance-depth-worker" in item for item in report.findings
    )


def test_health_surfaces_idle_timeout_finding_for_active_worker(tmp_path: Path) -> None:
    """Per FOLLOW_UPS #5: when the data-arrival watchdog fired on an active run
    (idle_timeout_count > 0 in the latest summary.jsonl row), the health report must
    surface an `idle_timeout:<worker>` finding so an operator notices a venue that went
    silent-but-connected. It is non-blocking — the lane self-heals via a fresh segment."""
    ops_root = tmp_path / "ops"
    workers_root = ops_root / "standalone_workers"
    workers_root.mkdir(parents=True)
    now = datetime.now(tz=UTC)
    (ops_root / "heartbeat.json").write_text(
        json.dumps({"status": "running", "last_seen": now.isoformat(), "job_counters": {}}),
        encoding="utf-8",
    )

    run_path = tmp_path / "raw" / "market" / "coinbase_depth" / "20260601_120000"
    run_path.mkdir(parents=True)
    _write_summary_jsonl(
        run_path,
        [
            {
                "raw_messages": 5,
                "clean_events": 5,
                "quarantined_events": 0,
                "idle_timeout_count": 2,  # watchdog fired twice on this run
                "partial": False,
            }
        ],
    )
    (workers_root / "coinbase-depth-worker.json").write_text(
        json.dumps(
            {
                "worker_name": "coinbase-depth-worker",
                "worker_type": "coinbase-depth-worker",
                "status": "running",
                "pid": os.getpid(),
                "last_seen": now.isoformat(),
                "current_segment": {
                    "index": 1,
                    "started_at": now.isoformat(),
                    "run_path": str(run_path),
                },
            }
        ),
        encoding="utf-8",
    )

    report = build_health_report(ops_root=ops_root, jobs=None, stale_after_seconds=300)

    worker = next(
        row for row in report.standalone_workers if row["name"] == "coinbase-depth-worker"
    )
    assert worker["partial_metrics"]["idle_timeout_count"] == 2
    assert any(
        item.startswith("idle_timeout:") or item.startswith("unmanaged_idle_timeout:")
        for item in report.findings
    )
    # Idle timeout self-heals (the worker opens a fresh segment), so it is NOT a blocking
    # finding the way a missing PID / high quarantine ratio is.
    assert "idle_timeout" not in worker["findings"]
    assert worker["blocking"] is False


def test_health_does_not_flag_idle_timeout_for_stopped_worker(tmp_path: Path) -> None:
    """A historical idle timeout on a stopped worker is not an in-flight problem, so no
    finding fires (mirrors the high_quarantine_ratio active-only rule)."""
    ops_root = tmp_path / "ops"
    workers_root = ops_root / "standalone_workers"
    workers_root.mkdir(parents=True)
    now = datetime.now(tz=UTC)
    (ops_root / "heartbeat.json").write_text(
        json.dumps({"status": "running", "last_seen": now.isoformat(), "job_counters": {}}),
        encoding="utf-8",
    )

    run_path = tmp_path / "raw" / "market" / "coinbase_depth" / "20260601_110000"
    run_path.mkdir(parents=True)
    _write_summary_jsonl(
        run_path,
        [{"raw_messages": 1, "clean_events": 1, "quarantined_events": 0, "idle_timeout_count": 3, "partial": False}],
    )
    (workers_root / "coinbase-depth-worker.json").write_text(
        json.dumps(
            {
                "worker_name": "coinbase-depth-worker",
                "worker_type": "coinbase-depth-worker",
                "status": "stopped",  # not active
                "pid": os.getpid(),
                "last_seen": now.isoformat(),
                "last_run_path": str(run_path),
            }
        ),
        encoding="utf-8",
    )

    report = build_health_report(ops_root=ops_root, jobs=None, stale_after_seconds=300)

    worker = next(
        row for row in report.standalone_workers if row["name"] == "coinbase-depth-worker"
    )
    # The count is still surfaced for context, but no finding fires.
    assert worker["partial_metrics"]["idle_timeout_count"] == 3
    assert not any("idle_timeout:coinbase-depth-worker" in item for item in report.findings)


def test_health_partial_metrics_handles_missing_summary_jsonl(tmp_path: Path) -> None:
    """If the worker just started and hasn't written summary.jsonl yet, the report
    should not crash — partial_metrics is just None."""
    ops_root = tmp_path / "ops"
    workers_root = ops_root / "standalone_workers"
    workers_root.mkdir(parents=True)
    now = datetime.now(tz=UTC)
    (ops_root / "heartbeat.json").write_text(
        json.dumps({"status": "running", "last_seen": now.isoformat(), "job_counters": {}}),
        encoding="utf-8",
    )

    run_path = tmp_path / "raw" / "market" / "binance_depth" / "20260527_130000"
    run_path.mkdir(parents=True)
    # NO metrics/summary.jsonl

    (workers_root / "binance-depth-worker.json").write_text(
        json.dumps(
            {
                "worker_name": "binance-depth-worker",
                "worker_type": "binance-depth-worker",
                "status": "running",
                "pid": os.getpid(),
                "last_seen": now.isoformat(),
                "current_segment": {
                    "index": 1,
                    "started_at": now.isoformat(),
                    "run_path": str(run_path),
                },
            }
        ),
        encoding="utf-8",
    )

    report = build_health_report(
        ops_root=ops_root,
        jobs=None,
        stale_after_seconds=300,
    )

    worker = next(
        row for row in report.standalone_workers if row["name"] == "binance-depth-worker"
    )
    assert worker["partial_metrics"] is None
    assert worker["quarantine_ratio"] is None
    # No high_quarantine finding
    assert "high_quarantine_ratio" not in worker["findings"]


def test_job_args_threads_lane_and_rotation_flags_for_depth() -> None:
    # Regression: the ops-runner is the production execution path, and the Phase 2
    # lane/rotation flags must survive the JobSpec -> SimpleNamespace translation.
    # Without this, the ETH lane in ops.live.example.json would collide with the BTC
    # lane in binance_depth/ because source_suffix never reaches the segment builder.
    job = JobSpec(
        name="binance-eth-depth",
        job_type="binance-depth-worker",
        interval_seconds=3600,
        args={
            "symbol": "ethusdt",
            "source_suffix": "ethusdt",
            "rotate_at_midnight": True,
            "max_backoff_seconds": 90.0,
            "worker_name": "binance-depth-worker-ethusdt",
        },
    )

    args = _job_args(job)

    assert args.source_suffix == "ethusdt"
    assert args.rotate_at_midnight is True
    assert args.max_backoff_seconds == 90.0
    assert args.worker_name == "binance-depth-worker-ethusdt"


def test_job_args_threads_lane_and_rotation_flags_for_trades() -> None:
    job = JobSpec(
        name="binance-eth-trades",
        job_type="binance-trades-worker",
        interval_seconds=3600,
        args={
            "symbol": "ethusdt",
            "source_suffix": "ethusdt",
            "rotate_at_midnight": True,
            "max_clock_skew_ms": 30_000.0,
            "jsonl_fsync": False,
            "normalized_parquet": False,
            "worker_name": "binance-trades-worker-ethusdt",
        },
    )

    args = _job_args(job)

    assert args.source_suffix == "ethusdt"
    assert args.rotate_at_midnight is True
    assert args.max_clock_skew_ms == 30_000.0
    assert args.jsonl_fsync is False
    assert args.normalized_parquet is False
    assert args.worker_name == "binance-trades-worker-ethusdt"


def test_job_args_coinbase_trades_worker_defaults() -> None:
    # The coinbase-trades-worker job_type must build a usable namespace with
    # Coinbase-shaped defaults (dashed product, matches channel) so the ops-runner
    # can drive it the same way it drives the Binance workers.
    args = _job_args(
        JobSpec(
            name="coinbase-btc-trades",
            job_type="coinbase-trades-worker",
            interval_seconds=3600,
            args={},
        )
    )

    assert args.symbol == "BTC-USD"
    assert args.channel == "matches"
    assert args.worker_name == "coinbase-trades-worker"
    assert args.source_suffix == ""
    assert args.rotate_at_midnight is False
    assert args.max_clock_skew_ms == 60_000.0


def test_job_args_coinbase_depth_worker_defaults() -> None:
    # The coinbase-depth-worker job_type must build a usable namespace with
    # Coinbase-shaped depth defaults (dashed product, level2_50 public channel) so
    # the ops-runner drives the none_native depth lane like the other workers.
    args = _job_args(
        JobSpec(
            name="coinbase-btc-depth",
            job_type="coinbase-depth-worker",
            interval_seconds=3600,
            args={},
        )
    )

    assert args.symbol == "BTC-USD"
    assert args.channel == "level2_50"
    assert args.worker_name == "coinbase-depth-worker"
    assert args.source_suffix == ""
    assert args.rotate_at_midnight is False


def test_job_args_coinbase_depth_worker_threads_lane_flags() -> None:
    args = _job_args(
        JobSpec(
            name="coinbase-eth-depth",
            job_type="coinbase-depth-worker",
            interval_seconds=3600,
            args={
                "symbol": "ETH-USD",
                "source_suffix": "ethusd",
                "rotate_at_midnight": True,
                "worker_name": "coinbase-depth-worker-ethusd",
            },
        )
    )

    assert args.symbol == "ETH-USD"
    assert args.source_suffix == "ethusd"
    assert args.rotate_at_midnight is True
    assert args.worker_name == "coinbase-depth-worker-ethusd"


def test_job_args_kraken_trades_worker_defaults() -> None:
    # The kraken-trades-worker job_type must build a usable namespace with Kraken v2
    # defaults (slash pair, trade channel) so the ops-runner drives the dense
    # sequence-bearing Kraken lane like the other workers.
    args = _job_args(
        JobSpec(
            name="kraken-btc-trades",
            job_type="kraken-trades-worker",
            interval_seconds=3600,
            args={},
        )
    )

    assert args.symbol == "BTC/USD"
    assert args.channel == "trade"
    assert args.worker_name == "kraken-trades-worker"
    assert args.source_suffix == ""
    assert args.rotate_at_midnight is False
    assert args.max_clock_skew_ms == 60_000.0


def test_job_args_kraken_trades_worker_threads_lane_flags() -> None:
    args = _job_args(
        JobSpec(
            name="kraken-eth-trades",
            job_type="kraken-trades-worker",
            interval_seconds=3600,
            args={
                "symbol": "ETH/USD",
                "source_suffix": "ethusd",
                "rotate_at_midnight": True,
                "worker_name": "kraken-trades-worker-ethusd",
            },
        )
    )

    assert args.symbol == "ETH/USD"
    assert args.source_suffix == "ethusd"
    assert args.rotate_at_midnight is True
    assert args.worker_name == "kraken-trades-worker-ethusd"


def test_job_args_bybit_trades_worker_defaults() -> None:
    # The bybit-trades-worker job_type must build a usable namespace with Bybit v5 spot
    # defaults (no-separator symbol, publicTrade channel) so the ops-runner drives the
    # none_native Bybit lane like the other workers.
    args = _job_args(
        JobSpec(
            name="bybit-btc-trades",
            job_type="bybit-trades-worker",
            interval_seconds=3600,
            args={},
        )
    )

    assert args.symbol == "BTCUSDT"
    assert args.channel == "publicTrade"
    assert args.worker_name == "bybit-trades-worker"
    assert args.source_suffix == ""
    assert args.rotate_at_midnight is False
    assert args.max_clock_skew_ms == 60_000.0


def test_job_args_bybit_trades_worker_threads_lane_flags() -> None:
    args = _job_args(
        JobSpec(
            name="bybit-eth-trades",
            job_type="bybit-trades-worker",
            interval_seconds=3600,
            args={
                "symbol": "ETHUSDT",
                "source_suffix": "ethusdt",
                "rotate_at_midnight": True,
                "worker_name": "bybit-trades-worker-ethusdt",
            },
        )
    )

    assert args.symbol == "ETHUSDT"
    assert args.source_suffix == "ethusdt"
    assert args.rotate_at_midnight is True
    assert args.worker_name == "bybit-trades-worker-ethusdt"


def test_job_args_bybit_depth_worker_defaults() -> None:
    # The bybit-depth-worker job_type must build a usable namespace with Bybit v5 spot
    # orderbook defaults (no-separator symbol, orderbook.50 topic prefix) so the
    # ops-runner drives the none_native Bybit depth lane like the other workers.
    args = _job_args(
        JobSpec(
            name="bybit-btc-depth",
            job_type="bybit-depth-worker",
            interval_seconds=3600,
            args={},
        )
    )

    assert args.symbol == "BTCUSDT"
    assert args.channel == "orderbook.50"
    assert args.worker_name == "bybit-depth-worker"
    assert args.source_suffix == ""
    assert args.rotate_at_midnight is False


def test_job_args_bybit_depth_worker_threads_lane_flags() -> None:
    args = _job_args(
        JobSpec(
            name="bybit-eth-depth",
            job_type="bybit-depth-worker",
            interval_seconds=3600,
            args={
                "symbol": "ETHUSDT",
                "channel": "orderbook.200",
                "source_suffix": "ethusdt",
                "rotate_at_midnight": True,
                "worker_name": "bybit-depth-worker-ethusdt",
            },
        )
    )

    assert args.symbol == "ETHUSDT"
    assert args.channel == "orderbook.200"
    assert args.source_suffix == "ethusdt"
    assert args.rotate_at_midnight is True
    assert args.worker_name == "bybit-depth-worker-ethusdt"


def test_job_args_kraken_depth_worker_defaults() -> None:
    # The kraken-depth-worker job_type must build a usable namespace with Kraken v2 book
    # defaults (slash pair, book channel) so the ops-runner drives the none_native
    # Kraken depth lane like the other workers.
    args = _job_args(
        JobSpec(
            name="kraken-btc-depth",
            job_type="kraken-depth-worker",
            interval_seconds=3600,
            args={},
        )
    )

    assert args.symbol == "BTC/USD"
    assert args.channel == "book"
    assert args.worker_name == "kraken-depth-worker"
    assert args.source_suffix == ""
    assert args.rotate_at_midnight is False


def test_job_args_kraken_depth_worker_threads_lane_flags() -> None:
    args = _job_args(
        JobSpec(
            name="kraken-eth-depth",
            job_type="kraken-depth-worker",
            interval_seconds=3600,
            args={
                "symbol": "ETH/USD",
                "source_suffix": "ethusd",
                "rotate_at_midnight": True,
                "worker_name": "kraken-depth-worker-ethusd",
            },
        )
    )

    assert args.symbol == "ETH/USD"
    assert args.source_suffix == "ethusd"
    assert args.rotate_at_midnight is True
    assert args.worker_name == "kraken-depth-worker-ethusd"


def test_job_args_lane_flags_default_to_legacy_behavior() -> None:
    # Omitting the flags must preserve the legacy single-symbol layout: empty suffix,
    # rotation off. This is what keeps the live BTC collector unaffected.
    depth = _job_args(
        JobSpec(name="d", job_type="binance-depth-worker", interval_seconds=3600, args={})
    )
    trades = _job_args(
        JobSpec(name="t", job_type="binance-trades-worker", interval_seconds=3600, args={})
    )

    assert depth.source_suffix == ""
    assert depth.rotate_at_midnight is False
    assert trades.source_suffix == ""
    assert trades.rotate_at_midnight is False


# ---------------------------------------------------------------------------
# Parallel collection runner (collector_concurrency)
# ---------------------------------------------------------------------------


def _collector_job(name: str, job_type: str, *, interval_seconds: int = 3600) -> JobSpec:
    return JobSpec(name=name, job_type=job_type, interval_seconds=interval_seconds)


def test_collectors_run_concurrently_with_concurrency_above_one(tmp_path: Path) -> None:
    """With collector_concurrency=3, three due collector jobs must overlap. A Barrier
    that requires all three to arrive proves true simultaneity — if they were serialized
    the barrier would time out and the runs would be recorded as errors."""
    runner = OpsRunner(tmp_path / "ops", poll_seconds=0, collector_concurrency=3)
    barrier = threading.Barrier(3, timeout=5.0)

    def execute_job(job: JobSpec) -> str:
        barrier.wait()  # raises BrokenBarrierError unless all three are running at once
        return f"ran {job.name}"

    jobs = [
        _collector_job("coinbase-btc-trades", "coinbase-trades-worker"),
        _collector_job("kraken-btc-trades", "kraken-trades-worker"),
        _collector_job("bybit-btc-trades", "bybit-trades-worker"),
    ]

    executed = runner.run(jobs, execute_job=execute_job, max_runs=3)

    assert executed == 3
    run_lines = (tmp_path / "ops" / "job_runs.jsonl").read_text(encoding="utf-8").strip().splitlines()
    assert len(run_lines) == 3
    # All three reached the barrier together -> all succeeded -> concurrency confirmed.
    assert all(json.loads(line)["status"] == "success" for line in run_lines)


def test_same_collector_job_not_launched_twice_while_running(tmp_path: Path) -> None:
    """A single collector job that is always due (interval 0) must never have two
    instances in flight at once, even when concurrency capacity is available."""
    runner = OpsRunner(tmp_path / "ops", poll_seconds=0, collector_concurrency=4)
    lock = threading.Lock()
    active = 0
    max_active = 0

    def execute_job(_job: JobSpec) -> str:
        nonlocal active, max_active
        with lock:
            active += 1
            max_active = max(max_active, active)
        time.sleep(0.15)
        with lock:
            active -= 1
        return "ok"

    jobs = [_collector_job("coinbase-btc-trades", "coinbase-trades-worker", interval_seconds=0)]

    executed = runner.run(jobs, execute_job=execute_job, max_runs=3)

    assert executed == 3
    assert max_active == 1


def test_maintenance_jobs_stay_serialized_when_multiple_due(tmp_path: Path) -> None:
    """Two maintenance jobs that are both due must run one after another, never at the
    same time, regardless of collector concurrency capacity."""
    runner = OpsRunner(tmp_path / "ops", poll_seconds=0, collector_concurrency=4)
    lock = threading.Lock()
    active = 0
    max_active = 0

    def execute_job(_job: JobSpec) -> str:
        nonlocal active, max_active
        with lock:
            active += 1
            max_active = max(max_active, active)
        time.sleep(0.1)
        with lock:
            active -= 1
        return "ok"

    jobs = [
        JobSpec(name="quarantine", job_type="quarantine-nonreplayable", interval_seconds=3600),
        JobSpec(name="promote", job_type="promote-replayable", interval_seconds=3600),
    ]

    executed = runner.run(jobs, execute_job=execute_job, max_runs=2)

    assert executed == 2
    assert max_active == 1


def test_maintenance_runs_while_a_collector_is_active(tmp_path: Path) -> None:
    """A maintenance job may run concurrently with an in-flight collector. The collector
    blocks until the test releases it; the maintenance job records whether the collector
    was already running when it executed."""
    runner = OpsRunner(tmp_path / "ops", poll_seconds=0, collector_concurrency=2)
    collector_running = threading.Event()
    release = threading.Event()
    observed: dict[str, bool] = {}

    def execute_job(job: JobSpec) -> str:
        if job.job_type == "coinbase-trades-worker":
            collector_running.set()
            release.wait(timeout=5.0)
            return "collector done"
        observed["collector_active"] = collector_running.wait(timeout=5.0)
        return "maintenance done"

    jobs = [
        _collector_job("coinbase-btc-trades", "coinbase-trades-worker"),
        JobSpec(name="manifest", job_type="build-manifest", interval_seconds=3600),
    ]

    thread = threading.Thread(
        target=runner.run,
        kwargs={"jobs": jobs, "execute_job": execute_job, "max_runs": 2},
        daemon=True,
    )
    thread.start()

    deadline = time.time() + 5.0
    while "collector_active" not in observed and time.time() < deadline:
        time.sleep(0.01)

    assert observed.get("collector_active") is True
    release.set()
    thread.join(timeout=5.0)
    assert not thread.is_alive()

    run_lines = (tmp_path / "ops" / "job_runs.jsonl").read_text(encoding="utf-8").strip().splitlines()
    assert len(run_lines) == 2
    assert all(json.loads(line)["status"] == "success" for line in run_lines)


def test_heartbeat_reports_current_jobs_and_preserves_current_job(tmp_path: Path) -> None:
    """The heartbeat must expose current_jobs (the full active set) and keep current_job
    pointing at the oldest active job for backward compatibility."""
    runner = OpsRunner(
        tmp_path / "ops", poll_seconds=0, heartbeat_interval_seconds=0.05, collector_concurrency=2
    )
    release = threading.Event()

    def execute_job(_job: JobSpec) -> str:
        release.wait(timeout=5.0)
        return "ok"

    jobs = [
        _collector_job("coinbase-btc-trades", "coinbase-trades-worker"),
        _collector_job("kraken-btc-trades", "kraken-trades-worker"),
    ]

    thread = threading.Thread(
        target=runner.run,
        kwargs={"jobs": jobs, "execute_job": execute_job, "max_runs": 2},
        daemon=True,
    )
    thread.start()

    heartbeat_path = tmp_path / "ops" / "heartbeat.json"
    deadline = time.time() + 5.0
    current_jobs: list = []
    heartbeat: dict = {}
    while time.time() < deadline:
        if heartbeat_path.exists():
            heartbeat = json.loads(heartbeat_path.read_text(encoding="utf-8"))
            current_jobs = heartbeat.get("current_jobs") or []
            if len(current_jobs) == 2:
                break
        time.sleep(0.02)

    try:
        assert isinstance(current_jobs, list)
        assert len(current_jobs) == 2
        names = {entry["name"] for entry in current_jobs}
        assert names == {"coinbase-btc-trades", "kraken-btc-trades"}
        for entry in current_jobs:
            assert set(entry) >= {"name", "job_type", "started_at"}
        # current_job is the oldest active job and matches the first current_jobs entry.
        assert heartbeat["current_job"] is not None
        assert heartbeat["current_job"]["name"] == current_jobs[0]["name"]
    finally:
        release.set()
        thread.join(timeout=5.0)

    assert not thread.is_alive()


def test_health_does_not_flag_running_collector_as_stale(tmp_path: Path) -> None:
    """A collector listed in current_jobs is in progress, so health must not mark it as a
    stale job even when it has never produced a completed run yet."""
    ops_root = tmp_path / "ops"
    ops_root.mkdir(parents=True)
    now = datetime.now(tz=UTC)
    (ops_root / "heartbeat.json").write_text(
        json.dumps(
            {
                "runner_name": "market-data-plant",
                "status": "running",
                "last_seen": now.isoformat(),
                "job_counters": {},
                "current_jobs": [
                    {
                        "name": "coinbase-btc-trades",
                        "job_type": "coinbase-trades-worker",
                        "started_at": now.isoformat(),
                    }
                ],
                "current_job": {
                    "name": "coinbase-btc-trades",
                    "job_type": "coinbase-trades-worker",
                    "started_at": now.isoformat(),
                },
            }
        ),
        encoding="utf-8",
    )

    jobs = [_collector_job("coinbase-btc-trades", "coinbase-trades-worker", interval_seconds=60)]
    report = build_health_report(ops_root=ops_root, jobs=jobs, stale_after_seconds=300)

    job_row = next(row for row in report.jobs if row["name"] == "coinbase-btc-trades")
    assert job_row["in_progress"] is True
    assert job_row["stale"] is False
    assert "stale_job:coinbase-btc-trades" not in report.findings


def test_health_does_not_flag_long_continuous_segment_as_long_running(tmp_path: Path) -> None:
    """Continuous-capture lanes rotate a finalized segment every max_segment_seconds with a
    tiny re-dispatch interval. A 20-min segment must read healthy, not long_running — the
    threshold follows the segment cadence (1.5x), not the interval. Without this, deploying
    continuous capture would flag every collector permanently."""
    ops_root = tmp_path / "ops"
    ops_root.mkdir(parents=True)
    now = datetime.now(tz=UTC)
    started = now - timedelta(seconds=1200)  # 20 min into a 30-min segment
    heartbeat = {
        "runner_name": "market-data-plant",
        "status": "running",
        "last_seen": now.isoformat(),
        "job_counters": {},
        "current_jobs": [
            {"name": "coinbase-btc-trades", "job_type": "coinbase-trades-worker", "started_at": started.isoformat()}
        ],
        "current_job": {"name": "coinbase-btc-trades", "job_type": "coinbase-trades-worker", "started_at": started.isoformat()},
    }
    (ops_root / "heartbeat.json").write_text(json.dumps(heartbeat), encoding="utf-8")

    continuous = JobSpec(
        name="coinbase-btc-trades",
        job_type="coinbase-trades-worker",
        interval_seconds=5,
        args={"max_segment_seconds": 1800},
    )
    report = build_health_report(
        ops_root=ops_root, jobs=[continuous], stale_after_seconds=300, job_stale_multiplier=3.0
    )
    row = next(r for r in report.jobs if r["name"] == "coinbase-btc-trades")
    assert row["in_progress"] is True
    assert row["long_running"] is False
    assert row["long_running_threshold_seconds"] == 2700.0  # 1800 * 1.5, not 5 * 3
    assert "long_running_job:coinbase-btc-trades" not in report.findings

    # Regression guard: with no segment cadence (legacy short-run config) the SAME 20-min
    # run IS long_running, so the relaxation is scoped to segmented continuous lanes only.
    legacy = JobSpec(name="coinbase-btc-trades", job_type="coinbase-trades-worker", interval_seconds=5)
    legacy_report = build_health_report(
        ops_root=ops_root, jobs=[legacy], stale_after_seconds=300, job_stale_multiplier=3.0
    )
    legacy_row = next(r for r in legacy_report.jobs if r["name"] == "coinbase-btc-trades")
    assert legacy_row["long_running"] is True
    assert "long_running_job:coinbase-btc-trades" in legacy_report.findings


def test_ops_runner_collector_concurrency_defaults_to_one(tmp_path: Path) -> None:
    parser = build_parser()
    config = str(tmp_path / "ops.json")

    default_args = parser.parse_args(["ops-runner", "--config", config])
    assert default_args.collector_concurrency == 1

    explicit_args = parser.parse_args(
        ["ops-runner", "--config", config, "--collector-concurrency", "4"]
    )
    assert explicit_args.collector_concurrency == 4
