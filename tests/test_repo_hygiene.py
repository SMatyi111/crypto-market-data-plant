"""Repo-level guards for failure modes that have actually bitten production.

These are not unit tests of plant logic; they pin operational invariants of the
repository itself so CI turns known footguns into red X's:

- .ps1 files must be pure ASCII (PowerShell 5.1 misdecodes UTF-8 punctuation into
  string-terminating curly quotes -- caused the 2026-06-10 redeploy outage).
- Enabled collector lanes must fit inside the runner scripts' default
  -CollectorConcurrency pool, or the lanes sorting last in the config are silently
  never dispatched (starved lanes shipped twice: 12<17 and 17<21).
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from crypto_collector.ops import COLLECTOR_JOB_TYPES

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS = sorted((REPO_ROOT / "scripts").glob("*.ps1"))
_CAP_PATTERN = re.compile(r"\[int\]\$CollectorConcurrency\s*=\s*(\d+)")


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


@pytest.mark.parametrize(
    "config_name",
    [
        "ops.live.example.json",
        pytest.param(
            "ops.live.local.json",
            marks=pytest.mark.skipif(
                not (REPO_ROOT / "ops.live.local.json").exists(),
                reason="local ops config only exists on the live box",
            ),
        ),
    ],
)
def test_collector_concurrency_covers_enabled_lanes(config_name: str) -> None:
    cap = _concurrency_default(REPO_ROOT / "scripts" / "run_ops_runner.ps1")
    lanes = _enabled_collector_lanes(REPO_ROOT / config_name)
    assert lanes <= cap, (
        f"{config_name} enables {lanes} collector lanes but the runner default pool is "
        f"{cap}; the lanes sorting last would be silently starved. Raise "
        f"CollectorConcurrency in run_ops_runner.ps1 AND redeploy_runner.ps1."
    )
