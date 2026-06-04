"""CLI + ops wiring tests for the MEXC lanes.

Proves the two new job types are accepted (parser, ops config, _job_args, the
collector-concurrency set, the worker lifecycle) WITHOUT changing how any existing
job type is parsed or dispatched.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from crypto_collector.cli import _job_args, build_parser, run_mexc_depth_worker
from crypto_collector.collectors.mexc import MEXC_DEALS_CHANNEL, MEXC_LIMIT_DEPTH_CHANNEL
from crypto_collector.ops import COLLECTOR_JOB_TYPES, JobSpec, load_ops_config


def test_cli_parser_accepts_mexc_workers() -> None:
    parser = build_parser()
    trades = parser.parse_args(["mexc-trades-worker", "--max-segments", "1"])
    depth = parser.parse_args(["mexc-depth-worker", "--depth", "10", "--max-segments", "1"])
    assert trades.command == "mexc-trades-worker"
    assert trades.symbol == "BTCUSDT"
    assert trades.channel == MEXC_DEALS_CHANNEL
    assert trades.interval == "100ms"
    assert depth.command == "mexc-depth-worker"
    assert depth.channel == MEXC_LIMIT_DEPTH_CHANNEL
    assert depth.depth == 10


def test_mexc_workers_are_collector_job_types() -> None:
    assert "mexc-trades-worker" in COLLECTOR_JOB_TYPES
    assert "mexc-depth-worker" in COLLECTOR_JOB_TYPES


def test_job_args_mexc_trades_worker_defaults() -> None:
    args = _job_args(
        JobSpec(name="mexc-btc-trades", job_type="mexc-trades-worker", interval_seconds=3600, args={})
    )
    assert args.symbol == "BTCUSDT"
    assert args.channel == MEXC_DEALS_CHANNEL
    assert args.interval == "100ms"
    assert args.worker_name == "mexc-trades-worker"
    assert args.source_suffix == ""
    assert args.rotate_at_midnight is False
    assert args.max_clock_skew_ms == 60_000.0


def test_job_args_mexc_depth_worker_defaults() -> None:
    args = _job_args(
        JobSpec(name="mexc-btc-depth", job_type="mexc-depth-worker", interval_seconds=3600, args={})
    )
    assert args.symbol == "BTCUSDT"
    assert args.channel == MEXC_LIMIT_DEPTH_CHANNEL
    assert args.depth == 20
    assert args.worker_name == "mexc-depth-worker"
    assert args.source_suffix == ""
    assert args.rotate_at_midnight is False


def test_job_args_mexc_workers_thread_lane_flags() -> None:
    trades = _job_args(
        JobSpec(
            name="mexc-eth-trades",
            job_type="mexc-trades-worker",
            interval_seconds=3600,
            args={
                "symbol": "ETHUSDT",
                "interval": "10ms",
                "source_suffix": "ethusdt",
                "rotate_at_midnight": True,
                "worker_name": "mexc-trades-worker-ethusdt",
            },
        )
    )
    assert trades.symbol == "ETHUSDT"
    assert trades.interval == "10ms"
    assert trades.source_suffix == "ethusdt"
    assert trades.rotate_at_midnight is True
    assert trades.worker_name == "mexc-trades-worker-ethusdt"

    depth = _job_args(
        JobSpec(
            name="mexc-eth-depth",
            job_type="mexc-depth-worker",
            interval_seconds=3600,
            args={
                "symbol": "ETHUSDT",
                "depth": 5,
                "source_suffix": "ethusdt",
                "rotate_at_midnight": True,
                "worker_name": "mexc-depth-worker-ethusdt",
            },
        )
    )
    assert depth.symbol == "ETHUSDT"
    assert depth.depth == 5
    assert depth.source_suffix == "ethusdt"
    assert depth.rotate_at_midnight is True
    assert depth.worker_name == "mexc-depth-worker-ethusdt"


def test_ops_config_accepts_mexc_jobs_alongside_existing(tmp_path: Path) -> None:
    """A config mixing an existing Binance job with the two MEXC jobs loads cleanly -
    adding MEXC must not break parsing of the other lanes."""
    config_path = tmp_path / "ops-mexc.json"
    config_path.write_text(
        json.dumps(
            {
                "jobs": [
                    {
                        "name": "binance-btc-depth",
                        "job_type": "binance-depth-worker",
                        "interval_seconds": 3600,
                        "args": {"symbol": "btcusdt"},
                    },
                    {
                        "name": "mexc-btc-trades",
                        "job_type": "mexc-trades-worker",
                        "interval_seconds": 3600,
                        "args": {"symbol": "BTCUSDT"},
                    },
                    {
                        "name": "mexc-btc-depth",
                        "job_type": "mexc-depth-worker",
                        "interval_seconds": 3600,
                        "args": {"symbol": "BTCUSDT", "depth": 20},
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    jobs = load_ops_config(config_path)
    assert [job.job_type for job in jobs] == [
        "binance-depth-worker",
        "mexc-trades-worker",
        "mexc-depth-worker",
    ]
    # Every job builds a usable namespace (no Unsupported job_type for the new ones).
    for job in jobs:
        _job_args(job)


def test_mexc_depth_worker_writes_lifecycle_logs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The worker runner drives a MEXC depth segment the same way it drives every
    other lane: started -> segment_complete -> stopped, with a heartbeat. The segment
    itself is faked so the test needs no network/protobuf socket."""

    async def fake_collect(_args: object) -> dict[str, object]:
        return {
            "raw_messages": 4,
            "clean_events": 4,
            "quarantined_events": 0,
            "run_path": str(tmp_path / "raw" / "market" / "mexc_depth" / "20260601_000000"),
            "replayable": True,
            "replay_findings": [],
        }

    monkeypatch.setattr("crypto_collector.cli.collect_mexc_depth_segment", fake_collect)
    args = build_parser().parse_args(
        [
            "mexc-depth-worker",
            "--max-segments",
            "1",
            "--output-root",
            str(tmp_path / "raw" / "market"),
            "--ops-root",
            str(tmp_path / "ops"),
        ]
    )

    run_mexc_depth_worker(args)

    heartbeat = json.loads(
        (tmp_path / "ops" / "standalone_workers" / "mexc-depth-worker.json").read_text(encoding="utf-8")
    )
    assert heartbeat["status"] == "stopped"
    assert heartbeat["venue"] == "mexc"
    assert heartbeat["last_segment_index"] == 1

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
