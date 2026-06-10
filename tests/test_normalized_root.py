"""Normalized-parquet root threading tests.

The contract under test: the per-lane `normalized_root` from the ops config reaches
the worker pipelines (no silent fallback to the env/default root), because the
2026-06-08 disk migration proved the fallback can point at a retired drive while
every config-carried path moved.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from crypto_collector import config
from crypto_collector.cli import _job_args, _resolve_normalized_root

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_resolve_normalized_root_explicit_beats_default(monkeypatch) -> None:
    monkeypatch.setenv("MARKET_DATA_NORMALIZED_ROOT", r"Z:\env_root")
    explicit = _resolve_normalized_root(
        SimpleNamespace(normalized_root=r"G:\market_archive\normalized\trades"), "trades"
    )
    assert explicit == Path(r"G:\market_archive\normalized\trades")

    fallback = _resolve_normalized_root(SimpleNamespace(normalized_root=None), "trades")
    assert fallback == Path(r"Z:\env_root") / "trades"

    absent = _resolve_normalized_root(SimpleNamespace(), "market")
    assert absent == Path(r"Z:\env_root") / "market"


def test_default_archive_root_matches_live_config_root() -> None:
    # The fallback constant must track the disk the ops configs actually use —
    # after the D:->G: cut it kept pointing at D: and normalized parquet silently
    # landed on the retired drive. Derive the live root from the example config
    # instead of hardcoding a drive letter twice.
    payload = json.loads((REPO_ROOT / "ops.live.example.json").read_text(encoding="utf-8"))
    ops_roots = {
        job["args"]["ops_root"]
        for job in payload["jobs"]
        if isinstance(job.get("args"), dict) and "ops_root" in job["args"]
    }
    assert ops_roots, "example config carries no ops_root to compare against"
    config_archive_roots = {str(Path(root).parent) for root in ops_roots}
    assert config_archive_roots == {str(config.DEFAULT_ARCHIVE_ROOT)}


def test_job_args_pass_normalized_root_for_every_worker_type() -> None:
    # Regression shape: ops args silently dropped between config and worker have
    # bitten three times now (market, jsonl_fsync, normalized_root).
    worker_types = [
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
        "okx-trades-worker",
        "okx-depth-worker",
        "binance-futures-rest-worker",
    ]
    for job_type in worker_types:
        configured = _job_args(
            SimpleNamespace(
                job_type=job_type,
                args={"normalized_root": r"G:\market_archive\normalized\trades"},
            )
        )
        assert configured.normalized_root == Path(r"G:\market_archive\normalized\trades"), job_type
        unset = _job_args(SimpleNamespace(job_type=job_type, args={}))
        assert unset.normalized_root is None, job_type


def test_workers_thread_normalized_root_through_build_segment_args(tmp_path, monkeypatch) -> None:
    """The per-worker build_segment_args lambdas don't enumerate normalized_root —
    it's threaded centrally in _run_segmented_worker (like jsonl_fsync and
    idle_timeout_seconds) so no lambda can silently drop it before the segment
    builds its parquet sink."""
    import crypto_collector.cli as cli

    captured: dict[str, object] = {}

    def make_fake(key):
        async def fake(segment_args):
            captured[key] = getattr(segment_args, "normalized_root", "MISSING")
            return {"run_path": str(tmp_path / key), "clean_events": 0, "replayable": True}

        return fake

    monkeypatch.setattr(cli, "collect_binance_trades_segment", make_fake("binance_trades"))
    monkeypatch.setattr(cli, "collect_bybit_trades_segment", make_fake("bybit_trades"))

    def drive(job_type, runner, extra_args):
        args = _job_args(
            SimpleNamespace(
                job_type=job_type,
                args={
                    "symbol": "BTCUSDT",
                    "max_segments": 1,
                    "cooldown_seconds": 0.0,
                    "heartbeat_interval_seconds": 0.1,
                    "worker_name": f"{job_type}-normroot",
                    "output_root": str(tmp_path),
                    "ops_root": str(tmp_path),
                    **extra_args,
                },
            )
        )
        runner(args)

    drive(
        "binance-trades-worker",
        cli.run_binance_trades_worker,
        {"normalized_root": str(tmp_path / "normalized" / "trades")},
    )
    drive("bybit-trades-worker", cli.run_bybit_trades_worker, {})

    assert captured["binance_trades"] == tmp_path / "normalized" / "trades"
    # No config value -> None on the segment; the segment body then resolves the
    # env/default fallback itself (unchanged legacy behavior).
    assert captured["bybit_trades"] is None


def test_example_config_worker_lanes_carry_normalized_root() -> None:
    """Config invariant: every collector-worker lane spells normalized_root out,
    with the dataset matching the lane kind. A future lane added without it would
    quietly fall back to the env/default root again."""
    payload = json.loads((REPO_ROOT / "ops.live.example.json").read_text(encoding="utf-8"))
    rest_stream_dataset = {"trades": "trades", "depth": "market", "funding": "funding"}
    checked = 0
    for job in payload["jobs"]:
        job_type = job["job_type"]
        if job_type == "binance-futures-rest-worker":
            expected = rest_stream_dataset[job["args"].get("stream", "trades")]
        elif job_type.endswith("-trades-worker"):
            expected = "trades"
        elif job_type.endswith("-depth-worker"):
            expected = "market"
        else:
            continue
        checked += 1
        normalized_root = job["args"].get("normalized_root")
        assert normalized_root, f"{job['name']} missing normalized_root"
        assert Path(normalized_root).name == expected, job["name"]
        assert Path(normalized_root).parent.name == "normalized", job["name"]
    assert checked >= 20  # all live lanes + disabled template lanes
