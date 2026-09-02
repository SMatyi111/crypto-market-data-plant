"""Repo-level guards for failure modes that have actually bitten production.

These are not unit tests of plant logic; they pin operational invariants of the
repository itself so CI turns known footguns into red X's:

- .ps1 files must be pure ASCII (PowerShell 5.1 misdecodes UTF-8 punctuation into
  string-terminating curly quotes -- caused the 2026-06-10 redeploy outage).
- Enabled collector lanes must fit inside the runner scripts' default
  -CollectorConcurrency pool, or the lanes sorting last in the config are silently
  never dispatched (starved lanes shipped twice: 12<17 and 17<21).
- Each raw lane may appear in at most one archive-offload job: two jobs owning
  the same lane is a double-move race on aged run dirs.
- Enabled collector lanes must not share an effective worker name
  (`args.worker_name`, else the job type): the standalone worker lock is keyed
  by that name, so only one of them ever runs (2026-08-25: the 3 open-interest
  lanes + funding, and the 3 bybit liquidation lanes, crash-looped for 8 days
  through a redeploy).
- No control characters in config string values: a JSON path with a single
  backslash before `raw` parses as a carriage return and the lane fails every
  mkdir with WinError 123.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from crypto_collector.ops import (
    COLLECTOR_JOB_TYPES,
    JobSpec,
    find_control_characters,
    shared_worker_names,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS = sorted((REPO_ROOT / "scripts").glob("*.ps1"))
_CAP_PATTERN = re.compile(r"\[int\]\$CollectorConcurrency\s*=\s*(\d+)")

# Config-level invariants run against the template in CI and additionally against
# the live config on the collection box.
OPS_CONFIGS = [
    "ops.live.example.json",
    pytest.param(
        "ops.live.local.json",
        marks=pytest.mark.skipif(
            not (REPO_ROOT / "ops.live.local.json").exists(),
            reason="local ops config only exists on the live box",
        ),
    ),
]


def _concurrency_default(script: Path) -> int:
    match = _CAP_PATTERN.search(script.read_text(encoding="utf-8"))
    assert match, f"{script.name}: no [int]$CollectorConcurrency = <n> default found"
    return int(match.group(1))


def _enabled_collector_lanes(config_path: Path) -> int:
    config = json.loads(config_path.read_text(encoding="utf-8"))
    return sum(
        1
        for job in config["jobs"]
        if job.get("enabled", True) and job["job_type"] in COLLECTOR_JOB_TYPES
    )


@pytest.mark.parametrize("script", SCRIPTS, ids=lambda p: p.name)
def test_ps1_scripts_are_ascii(script: Path) -> None:
    data = script.read_bytes()
    bad = [
        (line_no, line)
        for line_no, line in enumerate(data.splitlines(), start=1)
        if any(byte > 0x7F for byte in line)
    ]
    assert not bad, (
        f"{script.name} contains non-ASCII bytes (PowerShell 5.1 will misdecode them); "
        f"first offending lines: {[(n, raw.decode('utf-8', 'replace')) for n, raw in bad[:3]]}"
    )


def test_collector_job_types_match_ps1_preflight_patterns() -> None:
    # The .ps1 preflights count pooled lanes with `job_type -like "*-worker"` plus an
    # explicit list of the pooled kalshi job types. Both directions must hold:
    # (a) every pool-dispatched job type matches the preflight, and (b) nothing the
    # preflight matches is a maintenance type — a kalshi-* wildcard used to also
    # match kalshi-summarize-crypto-quotes (scheduler-side), so adding that valid
    # job to the config would have tripped the lane count and refused a boot.
    # Derived, not hardcoded: a new pooled non-worker type automatically widens
    # this set and fails the script-pin assertions below until both scripts learn it.
    kalshi_pool_types = {t for t in COLLECTOR_JOB_TYPES if not t.endswith("-worker")}

    def ps1_preflight_matches(job_type: str) -> bool:
        return job_type.endswith("-worker") or job_type in kalshi_pool_types

    assert all(ps1_preflight_matches(job_type) for job_type in COLLECTOR_JOB_TYPES)
    # Converse: the maintenance kalshi job must NOT count as a pooled lane.
    assert not ps1_preflight_matches("kalshi-summarize-crypto-quotes")
    # Pin the explicit list in BOTH runner scripts so it can't drift from the code.
    for script in ("run_ops_runner.ps1", "redeploy_runner.ps1"):
        body = (REPO_ROOT / "scripts" / script).read_text(encoding="ascii")
        for job_type in sorted(kalshi_pool_types):
            assert f'"{job_type}"' in body, f"{script} preflight is missing {job_type}"
        assert 'kalshi-*' not in body, f"{script} still uses the over-broad kalshi-* wildcard"


def test_runner_scripts_concurrency_defaults_match() -> None:
    run_script = REPO_ROOT / "scripts" / "run_ops_runner.ps1"
    redeploy_script = REPO_ROOT / "scripts" / "redeploy_runner.ps1"
    assert _concurrency_default(run_script) == _concurrency_default(redeploy_script), (
        "run_ops_runner.ps1 and redeploy_runner.ps1 have drifted CollectorConcurrency "
        "defaults; a redeploy would silently throttle coverage until the next reboot"
    )


@pytest.mark.parametrize("config_name", OPS_CONFIGS)
def test_collector_concurrency_covers_enabled_lanes(config_name: str) -> None:
    cap = _concurrency_default(REPO_ROOT / "scripts" / "run_ops_runner.ps1")
    lanes = _enabled_collector_lanes(REPO_ROOT / config_name)
    assert lanes <= cap, (
        f"{config_name} enables {lanes} collector lanes but the runner default pool is "
        f"{cap}; the lanes sorting last would be silently starved. Raise "
        f"CollectorConcurrency in run_ops_runner.ps1 AND redeploy_runner.ps1."
    )


@pytest.mark.parametrize("config_name", OPS_CONFIGS)
def test_each_lane_has_at_most_one_offload_job(config_name: str) -> None:
    """A lane owned by two archive-offload jobs is double-handled: in-runner
    maintenance jobs are serialized, so the second job re-scans dirs the first
    just moved and reports missing/verify-fail noise every hour — and a manual
    `archive-offload` CLI run alongside the runner can race the same run dir
    for real. Per-lane retention belongs on the lane spec (`min_age_days`
    override), never on a second job: each job also warns `unconfigured_lane`
    for every raw dir it doesn't own, so overlapping jobs bury that signal.
    Pinned 2026-06-13 when the Kalshi 3-day rotation almost shipped as a
    second job."""
    config = json.loads((REPO_ROOT / config_name).read_text(encoding="utf-8"))
    owners: dict[str, list[str]] = {}
    for job in config["jobs"]:
        if job.get("job_type") != "archive-offload" or not job.get("enabled", True):
            continue
        name = job.get("name", "<unnamed>")
        lanes = job.get("args", {}).get("lanes")
        assert isinstance(lanes, list) and lanes, (
            f"{config_name}: archive-offload job {name!r} declares no lanes"
        )
        for lane in lanes:
            owners.setdefault(lane["source"], []).append(name)
    duplicates = {src: names for src, names in owners.items() if len(names) > 1}
    assert not duplicates, (
        f"{config_name}: lanes offloaded by more than one archive-offload job "
        f"(double-handling): {duplicates}"
    )


def _job_specs(config_name: str) -> list[JobSpec]:
    config = json.loads((REPO_ROOT / config_name).read_text(encoding="utf-8"))
    return [JobSpec.from_dict(row) for row in config["jobs"]]


@pytest.mark.parametrize("config_name", OPS_CONFIGS)
def test_enabled_collector_lanes_have_unique_worker_names(config_name: str) -> None:
    """`StandaloneWorkerLock` is keyed by the EFFECTIVE worker name
    (`args.worker_name`, else the job type), not by job name. Lanes copy-pasted
    from a sibling (open-interest from funding, bybit liquidations per symbol)
    kept the sibling's worker_name, so the scheduler dispatched all of them but
    only the first ever acquired the lock; the rest failed every 5 s with
    "standalone worker already active" -- 39k of 48k job results per day -- and
    it survived the 2026-09-01 redeploy because every job NAME was unique. Same
    rule the runner applies at load (`load_ops_config`), pinned here for the
    template, which the runner never loads."""
    shared = shared_worker_names(_job_specs(config_name))
    assert not shared, (
        f"{config_name}: enabled collector lanes share a worker name (one lock per "
        f"name, so only one lane per name can run): {shared}"
    )


@pytest.mark.parametrize("config_name", OPS_CONFIGS)
def test_config_strings_have_no_control_characters(config_name: str) -> None:
    """Hand-edited JSON: a path with ONE backslash before `raw` is valid JSON
    containing a carriage return. The leaderboard lane shipped that way in the
    live config and failed every daily run with WinError 123 on mkdir. Same
    rule the runner applies at load, here over disabled jobs as well so a lane
    cannot be enabled into the failure later."""
    offenders = find_control_characters(_job_specs(config_name))
    assert not offenders, f"{config_name}: control characters in job args: {offenders}"
