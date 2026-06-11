"""Durability test: spawn the mock pipeline in a subprocess, SIGKILL it mid-write,
and verify every line in the produced JSONL files still parses cleanly. No torn
tails, no partial JSON objects.

This guards the no-torn-tail invariant of the JSONL sinks. The pipeline now BATCHES
fsync (fsync only every N events / ~200 ms) to lift the per-event-fsync throughput
ceiling, but it still flushes after EVERY line — so the OS only ever holds whole
lines and a hard kill (which drops the process's in-memory buffer, not the OS page
cache) can't leave a truncated final record. If a future refactor stops flushing
per line, the killed subprocess would expose a torn tail and this test fails.

We use subprocess.Popen + kill() (which is TerminateProcess on Windows,
equivalent to SIGKILL — the process gets no chance to flush its own buffers).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest


def _python_executable() -> str:
    """Use the same interpreter pytest is running under so we hit the same
    install (venv, dependency versions)."""
    return sys.executable


def _start_mock_subprocess(*, output_root: Path, count: int, delay_ms: float) -> subprocess.Popen:
    cmd = [
        _python_executable(),
        "-m",
        "crypto_collector.cli",
        "mock",
        "--count",
        str(count),
        "--output-root",
        str(output_root),
        "--delay-ms",
        str(delay_ms),
    ]
    # Inherit env so the venv site-packages are visible; explicitly pass through
    # MARKET_DATA_* so the subprocess doesn't fall back to D:\market_archive.
    env = os.environ.copy()
    env["MARKET_DATA_OUTPUT_ROOT"] = str(output_root)
    return subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )


def _assert_jsonl_clean(path: Path) -> int:
    """Every line in `path` must parse as JSON. Returns the line count.

    A SIGKILL-induced torn tail would manifest as:
    - a final line without trailing newline (truncated mid-write), OR
    - a final line with truncated content that doesn't parse.

    Flushing after every line prevents both: each completed line is handed to the
    OS (which survives the kill) before the next write begins, even though the
    disk-blocking fsync is batched.
    """
    raw = path.read_bytes()
    # If the file ends with a partial line (no trailing newline), that's a torn tail.
    # An empty file is fine — no writes ever happened.
    if raw and not raw.endswith(b"\n"):
        pytest.fail(
            f"durability violation: {path} ends with a partial line "
            f"(no trailing newline); fsync would have prevented this"
        )
    lines = [line for line in raw.split(b"\n") if line.strip()]
    for index, line in enumerate(lines, start=1):
        try:
            json.loads(line)
        except json.JSONDecodeError as exc:
            pytest.fail(
                f"durability violation: {path} line {index} did not parse: {exc}\n"
                f"line bytes: {line[:200]!r}..."
            )
    return len(lines)


def test_jsonl_writes_survive_sigkill_mid_stream(tmp_path: Path) -> None:
    """Run the mock for ~5 seconds of writes, kill it after 1s, then assert every
    line in messages.jsonl / events.jsonl is valid JSON."""
    output_root = tmp_path / "archive"
    output_root.mkdir()

    # 500 events @ 10ms = 5s total; we kill after ~1s, so we expect ~80-120 lines.
    proc = _start_mock_subprocess(output_root=output_root, count=500, delay_ms=10.0)
    try:
        # We can't glob-stat reliably across platforms; just sleep then kill.
        time.sleep(1.0)
        # Make sure the subprocess actually has written something before we kill it.
        # If it hasn't, give it more time — subprocess startup on Windows can be
        # slow, especially under CI.
        run_dirs = list((output_root / "mock").glob("*")) if (output_root / "mock").exists() else []
        if run_dirs:
            messages_path = run_dirs[0] / "raw" / "messages.jsonl"
            if not messages_path.exists() or messages_path.stat().st_size == 0:
                time.sleep(1.0)
        proc.kill()
        proc.wait(timeout=10.0)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5.0)

    # Find the run directory the subprocess created.
    run_root = output_root / "mock"
    assert run_root.exists(), f"subprocess never created the run root: {run_root}"
    run_dirs = list(run_root.glob("*"))
    assert run_dirs, f"subprocess never created a run dir under: {run_root}"
    run_dir = run_dirs[0]

    # The subprocess was killed mid-stream — verify the durable outputs are clean.
    messages_path = run_dir / "raw" / "messages.jsonl"
    events_path = run_dir / "clean" / "events.jsonl"

    if messages_path.exists():
        message_count = _assert_jsonl_clean(messages_path)
        # We expect SOME messages were written before kill.
        assert message_count > 0, (
            f"durability test inconclusive: 0 lines written to {messages_path}; "
            f"the kill landed too early to exercise the fsync path"
        )
    if events_path.exists():
        _assert_jsonl_clean(events_path)
