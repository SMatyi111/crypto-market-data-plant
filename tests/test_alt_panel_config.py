"""Alt research panel config (ops.live.example.json): disabled lanes, per-instrument
isolation, exactly one promoter per lane, offload and cleanup rows, and the derived
concurrency arithmetic for the staged phase-2 subsets. Hermetic: reads the example config
and the runner scripts only."""

from __future__ import annotations

import json
import re
from pathlib import Path

from crypto_collector.cli import _build_source_name, _job_args
from crypto_collector.ops import COLLECTOR_JOB_TYPES, JobSpec

REPO_ROOT = Path(__file__).resolve().parents[1]
PANEL = ("XLMUSDT", "GRAMUSDT", "TRXUSDT", "MEGAUSDT")
UNIVERSE_JOB = "bybit-linear-universe-snapshot"


def _config() -> dict:
    return json.loads((REPO_ROOT / "ops.live.example.json").read_text(encoding="utf-8"))


def _jobs_by_name(config: dict) -> dict[str, dict]:
    return {job["name"]: job for job in config["jobs"]}


def _cap_default() -> int:
    body = (REPO_ROOT / "scripts" / "run_ops_runner.ps1").read_text(encoding="ascii")
    return int(re.search(r"\[int\]\$CollectorConcurrency\s*=\s*(\d+)", body).group(1))


def _enabled_collector_lanes(config: dict) -> list[dict]:
    return [job for job in config["jobs"] if job.get("enabled", True) and job["job_type"] in COLLECTOR_JOB_TYPES]


def _panel_lane_names() -> list[str]:
    return [f"bybit-{s.lower().removesuffix('usdt')}-perp-{kind}" for s in PANEL for kind in ("trades", "depth")]


def test_panel_lanes_exist_disabled_with_linear_market_and_distinct_dirs() -> None:
    jobs = _jobs_by_name(_config())
    dirs: set[str] = set()
    workers: set[str] = set()
    for symbol in PANEL:
        suffix = symbol.lower()
        for kind, job_type, channel, base in (
            ("trades", "bybit-trades-worker", "publicTrade", "bybit_perp_trades"),
            ("depth", "bybit-depth-worker", "orderbook.50", "bybit_perp_depth"),
        ):
            name = f"bybit-{suffix.removesuffix('usdt')}-perp-{kind}"
            job = jobs[name]
            assert job["job_type"] == job_type
            assert job["enabled"] is False, f"{name} must ship disabled"
            args = job["args"]
            assert args["symbol"] == symbol
            assert args["market"] == "linear"
            assert args["channel"] == channel
            assert args["source_suffix"] == suffix
            assert args["worker_name"] not in workers
            workers.add(args["worker_name"])
            source_dir = _build_source_name(base, args["source_suffix"])
            assert source_dir == f"{base}_{suffix}"
            assert source_dir not in dirs
            dirs.add(source_dir)
            # Threaded through the central job-arg builder, not a per-venue lambda.
            ns = _job_args(JobSpec(name=name, job_type=job_type, interval_seconds=5, args=args, enabled=False))
            assert (ns.symbol, ns.market, ns.source_suffix, ns.channel) == (symbol, "linear", suffix, channel)
            assert ns.rotate_at_midnight is True
            assert ns.idle_timeout_seconds == 120
    assert len(dirs) == 8


def test_each_panel_lane_has_exactly_one_disabled_promoter_quarantine_and_score() -> None:
    config = _config()
    jobs = config["jobs"]
    promoters_by_source: dict[str, list[str]] = {}
    for job in jobs:
        if job["job_type"] == "promote-replayable":
            promoters_by_source.setdefault(job["args"]["source_root"], []).append(job["name"])
    for symbol in PANEL:
        suffix = symbol.lower()
        for base, target in (("bybit_perp_trades", "trades_replayable"), ("bybit_perp_depth", "market_replayable")):
            source_root = f"G:\\market_archive\\raw\\market\\{base}_{suffix}"
            names = promoters_by_source.get(source_root, [])
            assert len(names) == 1, f"{source_root} must have exactly one promoter, has {names}"
            promoter = next(j for j in jobs if j["name"] == names[0])
            assert promoter["enabled"] is False
            assert promoter["args"]["target_root"].endswith(target)
            assert promoter["args"]["quarantine_index"] == (
                f"G:\\market_archive\\quarantine\\market\\{base}_{suffix}\\_quarantine_index.jsonl"
            )
            quarantine = [j for j in jobs if j["job_type"] == "quarantine-runs" and j["args"]["source_root"] == source_root]
            assert len(quarantine) == 1 and quarantine[0]["enabled"] is False
    # Scoring: trades per lane (disabled), depth through one disabled stream-depth job listing the four dirs.
    for symbol in PANEL:
        suffix = symbol.lower()
        score = [j for j in jobs if j["job_type"] == "backfill-trades-replay"
                 and j["args"]["source_root"].endswith(f"bybit_perp_trades_{suffix}")]
        assert len(score) == 1 and score[0]["enabled"] is False
    depth_score = next(j for j in jobs if j["name"] == "score-bybit-alt-panel-depth")
    assert depth_score["enabled"] is False
    assert depth_score["job_type"] == "backfill-stream-depth"
    assert sorted(depth_score["args"]["source"]) == sorted(f"bybit_perp_depth_{s.lower()}" for s in PANEL)
    # No existing enabled job gained the new sources (nothing in the example turns on by accident).
    enabled_depth_scores = [j for j in jobs if j["job_type"] == "backfill-stream-depth" and j.get("enabled", True)]
    for job in enabled_depth_scores:
        assert not any(s.endswith(tuple(f"_{x.lower()}" for x in PANEL)) for s in job["args"]["source"])


def test_universe_snapshot_job_disabled_and_pooled() -> None:
    jobs = _jobs_by_name(_config())
    job = jobs["bybit-linear-universe-snapshot"]
    assert job["job_type"] == UNIVERSE_JOB
    assert job["enabled"] is False
    assert job["interval_seconds"] == 86400
    assert job["args"]["panel_symbols"] == list(PANEL)
    assert UNIVERSE_JOB in COLLECTOR_JOB_TYPES  # holds a pool slot while it runs
    for script in ("run_ops_runner.ps1", "redeploy_runner.ps1"):
        assert f'"{UNIVERSE_JOB}"' in (REPO_ROOT / "scripts" / script).read_text(encoding="ascii")


def test_offload_and_cleanup_rows_cover_the_new_sources() -> None:
    config = _config()
    jobs = _jobs_by_name(config)
    lanes = {lane["source"]: lane for lane in jobs["archive-offload-cold"]["args"]["lanes"]}
    for symbol in PANEL:
        suffix = symbol.lower()
        for base in ("bybit_perp_trades", "bybit_perp_depth"):
            lane = lanes[f"{base}_{suffix}"]
            assert lane["promotion_index"].endswith("_promotion_index.jsonl")
            assert lane["quarantine_index"].endswith(f"{base}_{suffix}\\_quarantine_index.jsonl")
    assert lanes["bybit_linear_universe"]["gate"] == "age_only"
    policy = jobs["cleanup-dry-run"]["args"]["raw_policy"]
    for symbol in PANEL:
        suffix = symbol.lower()
        assert f"market/bybit_perp_trades_{suffix}=14" in policy
        assert f"market/bybit_perp_depth_{suffix}=14" in policy
    assert "market/bybit_linear_universe=3650" in policy


def test_concurrency_is_derived_not_raised_in_phase_one() -> None:
    """Phase 1 ships everything disabled, so the pool default is untouched. The staged
    phase-2 subsets must raise the cap to (enabled lanes + subset + reserve) in BOTH
    scripts before activation; this pins the arithmetic the rollout ticket quotes."""
    config = _config()
    enabled = len(_enabled_collector_lanes(config))
    cap = _cap_default()
    assert cap == 49, "phase 1 must not raise the pool cap; activation does, with the subset arithmetic"
    assert enabled <= cap
    reserve = 2
    stage_one = 1 + 2 + 1  # universe snapshot + one symbol's trades and depth  (3 lanes)
    stage_two = 1 + 8  # universe snapshot + four symbols' trades and depth (9 lanes)
    ticker_worker = 1  # 60 s coarse reference poll, NOT implemented in phase 1 -> 10 collectors if added
    assert stage_one - 1 == 3 and stage_two == 9 and stage_two + ticker_worker == 10
    new_lane_names = set(_panel_lane_names()) | {"bybit-linear-universe-snapshot"}
    jobs = _jobs_by_name(config)
    assert all(jobs[name]["enabled"] is False for name in new_lane_names)
    # Required cap for a subset is a function of the live enabled count at activation time.
    def required_cap(live_enabled: int, subset: int) -> int:
        return live_enabled + subset + reserve
    assert required_cap(47, 3) == 52 and required_cap(47, 9) == 58 and required_cap(47, 10) == 59
