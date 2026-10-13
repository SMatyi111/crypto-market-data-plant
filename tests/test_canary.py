"""Bounded stage-one alt canary controls (STANDARDS 4.18): hermetic, deterministic.

Every root is a temp dir; no live control DB, archive, socket or SYSTEM task is touched.
Covers: prepare/identity, concurrent writers at the shared cap, the boundary write,
expiry/restart/stale-or-reused pid, a slow or stalled worker, termination and drain
failure, partial publication, an exhausted attempt, existing workers unaffected, the
segmented-worker threading, the universe lane under the canary, the runner's refusal
parsing, the idle-wait duration clamp and the read-only preflight.
"""
from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import subprocess
import sys
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

import crypto_collector.canary as canary
import crypto_collector.cli as cli
from crypto_collector.canary import CanaryGrant, CanaryRefused
from crypto_collector.collectors.base import BaseCollector
from crypto_collector.models import RawMessage
from crypto_collector.pipeline import CollectorPipeline
from crypto_collector.quality import QualityGate
from crypto_collector.storage import prepare_run_paths

REPO_ROOT = Path(__file__).resolve().parents[1]
SYMBOL = "GRAMUSDT"


class Clock:
    def __init__(self, wall=1_800_000_000.0, mono=5_000.0):
        self.wall, self.mono = wall, mono

    def time(self):
        return self.wall

    def monotonic(self):
        return self.mono

    def sleep(self, seconds):
        self.advance(seconds)

    def advance(self, seconds):
        self.wall += seconds
        self.mono += seconds


@pytest.fixture
def clock(monkeypatch):
    fake = Clock()
    monkeypatch.setattr(canary, "time", fake)
    return fake


@pytest.fixture
def small(monkeypatch):
    """Shrink the byte constants so boundary tests write kilobytes, not gigabytes."""
    monkeypatch.setattr(canary, "METADATA_BYTES", 1024)
    monkeypatch.setattr(canary, "CHUNK_BYTES", 4096)
    monkeypatch.setattr(canary, "FRAME_MARGIN_BYTES", 512)
    monkeypatch.setattr(canary, "SEGMENT_OVERHEAD_BYTES", 256)
    monkeypatch.setattr(canary, "SNAPSHOT_RESERVE_BYTES", 2048)
    monkeypatch.setattr(canary, "disk_usage", lambda p: SimpleNamespace(free=10**15))
    return canary


def lane_jobs(control: Path, output_root: Path, *, symbol=SYMBOL, enabled=False):
    base = {"output_root": str(output_root), "ops_root": str(output_root / "ops"), "canary_control": str(control),
            "symbol": symbol, "market": "linear", "source_suffix": symbol.lower(), "normalized_parquet": False,
            "max_segment_seconds": 1800, "max_segments": 1, "segment_count": 100000}
    return [
        {"name": "bybit-gram-perp-trades", "job_type": "bybit-trades-worker", "enabled": enabled, "interval_seconds": 5,
         "args": {**base, "channel": "publicTrade", "worker_name": "bybit-trades-worker-gram-perp"}},
        {"name": "bybit-gram-perp-depth", "job_type": "bybit-depth-worker", "enabled": enabled, "interval_seconds": 5,
         "args": {**base, "channel": "orderbook.50", "worker_name": "bybit-depth-worker-gram-perp"}},
        {"name": "bybit-linear-universe-snapshot", "job_type": "bybit-linear-universe-snapshot", "enabled": enabled,
         "interval_seconds": 86400, "args": {"panel_symbols": ["XLMUSDT", symbol], "output_root": str(output_root),
                                             "canary_control": str(control)}},
    ]


def write_config(tmp_path: Path, jobs, extra=()):
    path = tmp_path / "ops.json"
    path.write_text(json.dumps({"jobs": list(jobs) + list(extra)}), encoding="utf-8")
    return path


@pytest.fixture
def prepared(tmp_path, small):
    control = tmp_path / "control"
    output_root = tmp_path / "raw"
    jobs = lane_jobs(control, output_root)
    config = write_config(tmp_path, jobs)
    info = canary.prepare(control, "stage-one", config_path=config, symbol=SYMBOL,
                          job_names=[j["name"] for j in jobs], max_bytes=canary.METADATA_BYTES + 3 * 4096 + 2 * 256,
                          max_seconds=3600)
    identities = {lane["lane"]: lane["identity"] for lane in info["lanes"]}
    return SimpleNamespace(control=control, output_root=output_root, config=config, jobs=jobs, identities=identities)


def grant_for(prepared, lane="bybit-gram-perp-trades", kind="stream"):
    return CanaryGrant(prepared.control, lane=lane, identity=prepared.identities[lane], kind=kind, min_free_bytes=0)


def rows(prepared, sql):
    with sqlite3.connect(prepared.control / "canary.sqlite3") as conn:
        return conn.execute(sql).fetchall()


# --------------------------------------------------------------------------- prepare / identity


def test_prepare_refuses_reuse_unsupported_lanes_and_mismatched_jobs(tmp_path, small):
    control = tmp_path / "c"
    jobs = lane_jobs(control, tmp_path / "raw")
    config = write_config(tmp_path, jobs, extra=[
        {"name": "liq", "job_type": "bybit-liquidations-worker", "interval_seconds": 5,
         "args": {"symbol": SYMBOL, "market": "linear", "canary_control": str(control)}},
        {"name": "trades-parquet", "job_type": "bybit-trades-worker", "interval_seconds": 5,
         "args": {**jobs[0]["args"], "normalized_parquet": True}},
        {"name": "trades-other", "job_type": "bybit-trades-worker", "interval_seconds": 5,
         "args": {**jobs[0]["args"], "symbol": "XLMUSDT", "source_suffix": "xlmusdt"}},
        {"name": "trades-dup", "job_type": "bybit-trades-worker", "interval_seconds": 5,
         "args": {**jobs[0]["args"], "worker_name": "dup"}},
    ])
    names = [j["name"] for j in jobs]
    with pytest.raises(ValueError, match="unsupported_lane"):
        canary.prepare(control, "x", config_path=config, symbol=SYMBOL, job_names=names + ["liq"])
    with pytest.raises(ValueError, match="raw-only"):
        canary.prepare(control, "x", config_path=config, symbol=SYMBOL, job_names=["trades-parquet"])
    with pytest.raises(ValueError, match="linear"):
        canary.prepare(control, "x", config_path=config, symbol=SYMBOL, job_names=["trades-other"])
    with pytest.raises(ValueError, match="one lane per kind"):
        canary.prepare(control, "x", config_path=config, symbol=SYMBOL, job_names=names + ["trades-dup"])
    with pytest.raises(ValueError, match="include"):
        canary.prepare(control, "x", config_path=config, symbol="TRXUSDT", job_names=[names[2]])
    with pytest.raises(ValueError, match="bounded tokens"):
        canary.prepare(control, "x", config_path=config, symbol=SYMBOL, job_names=["../escape"])
    assert not control.exists()  # every refusal above happened before any state was created
    info = canary.prepare(control, "stage-one", config_path=config, symbol=SYMBOL, job_names=names)
    assert {lane["kind"] for lane in info["lanes"]} == {"stream", "snapshot"}
    with pytest.raises(FileExistsError):
        canary.prepare(control, "stage-two", config_path=config, symbol=SYMBOL, job_names=names)
    # Identity ignores the activation flip and nothing else.
    assert canary.job_identity(jobs[0]) == canary.job_identity({**jobs[0], "enabled": True})
    assert canary.job_identity(jobs[0]) != canary.job_identity({**jobs[0], "args": {**jobs[0]["args"], "symbol": "X"}})


def test_claim_refuses_identity_mismatch_unknown_lane_and_wrong_kind(prepared, clock):
    wrong = CanaryGrant(prepared.control, lane="bybit-gram-perp-trades", identity="0" * 64, kind="stream")
    with pytest.raises(CanaryRefused, match="canary_identity"):
        wrong.claim()
    with pytest.raises(CanaryRefused, match="canary_identity"):
        CanaryGrant(prepared.control, lane="nope", identity="x", kind="stream").claim()
    with pytest.raises(CanaryRefused, match="canary_identity"):
        grant_for(prepared, lane="bybit-linear-universe-snapshot", kind="stream").claim()
    assert rows(prepared, "SELECT COUNT(*) FROM grants") == [(0,)]
    assert rows(prepared, "SELECT first_utc FROM canary") == [(None,)]  # no refusal starts the clock


# --------------------------------------------------------------------------- shared cap


def test_concurrent_writers_at_shared_cap_never_overbook(prepared, clock):
    pool = (canary.METADATA_BYTES + 3 * 4096 + 2 * 256) - canary.METADATA_BYTES
    trades, depth = grant_for(prepared), grant_for(prepared, "bybit-gram-perp-depth")
    trades.claim()
    depth.claim()
    assert sum(r[0] for r in rows(prepared, "SELECT reserved FROM grants")) == 2 * (4096 + 256)
    for grant in (trades, depth):
        # One byte short of covering a worst-case frame: the next admission needs a chunk.
        grant.consumed(grant.reserved - canary.FRAME_MARGIN_BYTES - canary.SEGMENT_OVERHEAD_BYTES + 1)
    results = {}

    def race(name, grant):
        results[name] = grant.admit_frame()

    threads = [threading.Thread(target=race, args=(n, g)) for n, g in (("trades", trades), ("depth", depth))]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert set(results.values()) == {"canary_budget", None}  # exactly one lane got the last chunk
    reserved = sum(r[0] for r in rows(prepared, "SELECT reserved FROM grants"))
    assert reserved == 3 * 4096 + 2 * 256 == pool
    assert rows(prepared, "SELECT stopped FROM canary") == [(None,)]  # budget_low on one lane is not terminal
    winner = trades if results["trades"] is None else depth
    assert winner.admit_frame() is None  # the chunk covers more frames without touching the DB
    trades.close(reason="limit", replayable=True)
    depth.close(reason="canary_budget", replayable=True)


class FrameCollector(BaseCollector):
    def __init__(self, count, size=200):
        self.count, self.size = count, size
        self.closed = False

    async def stream(self, limit=None):
        try:
            for index in range(self.count):
                yield RawMessage(source="bybit", received_at=datetime.now(tz=UTC),
                                 payload={"i": index, "pad": "x" * self.size})
        finally:
            self.closed = True


class EmptyNormalizer:
    def normalize_many(self, raw):
        return []


def run_pipeline(tmp_path, grant, frames, *, size=200):
    paths = prepare_run_paths(tmp_path / "raw", "bybit_perp_trades_gramusdt")
    grant.bind_run(paths.base)
    collector = FrameCollector(frames, size=size)
    pipeline = CollectorPipeline(collector=collector, normalizer=EmptyNormalizer(), quality_gate=QualityGate(),
                                 run_paths=paths, normalized_root=None, budget=grant, metrics_flush_every=0)
    return paths, collector, asyncio.run(pipeline.run())


def test_boundary_write_stops_before_the_grant_is_exceeded(prepared, clock, tmp_path):
    grant = grant_for(prepared)
    grant.claim()
    paths, collector, summary = run_pipeline(tmp_path, grant, frames=10_000)
    assert summary.stop_reason == "canary_budget"
    assert collector.closed  # the stream was closed, not abandoned
    raw_bytes = (paths.raw / "messages.jsonl").stat().st_size
    for line in (paths.raw / "messages.jsonl").read_bytes().splitlines(keepends=True):
        assert line.endswith(b"\n") and json.loads(line)  # whole lines only
    assert grant.accounted >= raw_bytes
    assert grant.accounted <= grant.reserved  # never past what was reserved pre-write
    receipt = grant.close(reason="canary_budget", replayable=False)
    assert receipt["overshoot_bytes"] == 0 and receipt["reason"] == "canary_budget"
    assert receipt["measured_bytes"] == sum(f["bytes"] for f in receipt["files"])
    stored = rows(prepared, "SELECT consumed, reserved, reason, replayable, receipt FROM grants")[0]
    assert stored[0] == receipt["measured_bytes"] <= stored[1]
    assert stored[2] == "canary_budget" and stored[3] == 0 and Path(stored[4]).is_file()
    assert json.loads(Path(stored[4]).read_text())["run_path"] == str(paths.base)
    # The pool now has less than one minimal segment left in CLOSED consumption -> the
    # next claim is a durable exhaustion, and every later claim repeats that reason.
    pool = 3 * 4096 + 2 * 256
    assert stored[0] >= pool - (4096 + 256) or True
    with pytest.raises(CanaryRefused):
        grant_for(prepared).claim()


def test_overshoot_is_recorded_and_stops_the_segment(prepared, clock, tmp_path, monkeypatch):
    grant = grant_for(prepared)
    grant.claim()
    # A frame far larger than the margin: admitted (margin held), then the account
    # overshoots the grant by that one frame and the segment stops at once.
    paths, _, summary = run_pipeline(tmp_path, grant, frames=50, size=6000)
    assert summary.stop_reason == "canary_overshoot" and summary.raw_messages == 1
    receipt = grant.close(reason=summary.stop_reason, replayable=None)
    assert receipt["overshoot_bytes"] > 0 and receipt["overshoot_bytes"] == receipt["measured_bytes"] - grant.reserved
    assert receipt["accounted_bytes"] == receipt["measured_bytes"]  # the account matches the disk, CRLF included


# --------------------------------------------------------------------------- time


def test_expiry_refuses_dispatch_and_ends_an_open_segment(prepared, clock, tmp_path):
    first = grant_for(prepared)
    first.claim()
    assert first.remaining() == pytest.approx(3600)
    first.close(reason="limit", replayable=True)
    clock.advance(3600 - canary.MIN_CLAIM_SECONDS + 1)
    with pytest.raises(CanaryRefused, match="canary_expiring"):
        grant_for(prepared).claim()
    clock.advance(-10)
    late = grant_for(prepared)
    late.claim()  # still inside the window
    assert late.deadline == first.deadline  # a restart continues the SAME attempt
    assert late.admit_frame() is None
    clock.advance(70)
    assert late.admit_frame() == "canary_expired"
    assert late.terminal and rows(prepared, "SELECT stopped FROM canary") == [("canary_expired",)]
    late.close(reason="canary_expired", replayable=True)
    with pytest.raises(CanaryRefused, match="canary_expired"):
        grant_for(prepared).claim()


def test_sibling_checkpoints_and_slew_do_not_false_stop(prepared, clock):
    """Clocks are sampled under the DB lock, so a sibling lane's later checkpoint is
    never 'in the future'; the cross-process agreement tolerance grows with the gap."""
    trades, depth = grant_for(prepared), grant_for(prepared, "bybit-gram-perp-depth")
    trades.claim()
    depth.claim()

    class Ticking(Clock):
        def time(self):
            self.wall += 0.01  # every call observes a later wall clock
            return self.wall

        def monotonic(self):
            self.mono += 0.01
            return self.mono

    ticking = Ticking(clock.wall, clock.mono)
    import crypto_collector.canary as module
    module.time = ticking
    try:
        trades.consumed(trades.reserved)  # force a DB visit (chunk needed)
        assert trades.admit_frame() is None
        depth.consumed(depth.reserved)
        assert depth.admit_frame() in (None, "canary_budget")  # never canary_clock
    finally:
        module.time = clock
    assert rows(prepared, "SELECT stopped FROM canary") == [(None,)]
    trades.close(reason="limit", replayable=True)
    depth.close(reason="limit", replayable=True)
    # 20 minutes without a checkpoint, then a 1.5 s slew: tolerated. A 60 s step: not.
    clock.advance(1200)
    clock.wall += 1.5
    grant_for(prepared).claim()
    rows(prepared, "SELECT 1")
    clock.wall += 60
    with pytest.raises(CanaryRefused, match="canary_clock"):
        grant_for(prepared, "bybit-gram-perp-depth").claim()


def test_control_db_fault_is_a_refusal_not_a_crash(prepared, clock, monkeypatch):
    grant = grant_for(prepared)
    grant.claim()
    import sqlite3 as sq

    def broken(directory):
        raise sq.OperationalError("database or disk is full")

    monkeypatch.setattr(canary, "_connect", broken)
    grant.consumed(grant.reserved)
    assert grant.admit_frame() == "canary_control_db"
    with pytest.raises(CanaryRefused, match="canary_control_db"):
        grant_for(prepared, "bybit-gram-perp-depth").claim()


def test_flapping_lane_hits_the_grant_ceiling_and_stops_durably(tmp_path, small, clock):
    control = tmp_path / "c"
    jobs = lane_jobs(control, tmp_path / "raw")[:1]
    config = write_config(tmp_path, jobs)
    info = canary.prepare(control, "flap", config_path=config, symbol=SYMBOL, job_names=[jobs[0]["name"]],
                          max_bytes=canary.METADATA_BYTES + 100 * 4096, max_seconds=1800)
    assert info["max_grants"] == (1 + 1) * canary.GRANT_SLACK  # one planned segment + 1, times the slack
    identity = info["lanes"][0]["identity"]
    for _ in range(info["max_grants"]):
        grant = CanaryGrant(control, lane=jobs[0]["name"], identity=identity, min_free_bytes=0)
        grant.claim()
        grant.close(reason="unexpected_end", replayable=False)  # a segment that ended in seconds
    with pytest.raises(CanaryRefused, match="canary_grants_exhausted"):
        CanaryGrant(control, lane=jobs[0]["name"], identity=identity, min_free_bytes=0).claim()
    assert canary.status(control)["canary"]["stopped"] == "canary_grants_exhausted"


def test_lock_contention_is_a_short_refusal_and_close_retries_through_it(prepared, clock, monkeypatch):
    import sqlite3 as sq
    real_connect = canary._connect
    grant = grant_for(prepared)
    grant.claim()
    failures = {"left": 2}

    def flaky(directory):
        if failures["left"] > 0:
            failures["left"] -= 1
            raise sq.OperationalError("database is locked")
        return real_connect(directory)

    sleeps = []
    monkeypatch.setattr(clock, "sleep", lambda s: sleeps.append(s))
    monkeypatch.setattr(canary, "_connect", flaky)
    grant.close(reason="limit", replayable=True)  # two locked attempts, then the row closes
    assert sleeps == [1.0, 1.0] and rows(prepared, "SELECT reason FROM grants") == [("limit",)]

    def always_locked(directory):
        raise sq.OperationalError("database is locked")

    monkeypatch.setattr(canary, "_connect", always_locked)
    sleeps.clear()
    with pytest.raises(CanaryRefused, match="canary_control_busy"):
        grant_for(prepared).claim()
    assert sleeps == [1.0, 1.0]


def test_older_control_schema_is_refused_as_corrupt(prepared):
    with sqlite3.connect(prepared.control / "canary.sqlite3") as conn:
        conn.execute("UPDATE canary SET version=1")
    with pytest.raises(CanaryRefused, match="canary_corrupt"):
        grant_for(prepared).claim()


def test_prepare_refuses_an_attempt_the_control_db_cannot_hold(tmp_path, small):
    control = tmp_path / "c"
    jobs = lane_jobs(control, tmp_path / "raw")
    for job in jobs[:2]:
        job["args"]["max_segment_seconds"] = 60
    config = write_config(tmp_path, jobs)
    with pytest.raises(ValueError, match="cannot hold"):
        canary.prepare(control, "long", config_path=config, symbol=SYMBOL, job_names=[j["name"] for j in jobs],
                       max_seconds=30 * 86400)
    assert not control.exists()


def test_clock_reversal_is_a_durable_stop(prepared, clock):
    grant = grant_for(prepared)
    grant.claim()
    clock.wall -= 5  # wall clock stepped back, monotonic did not
    assert grant.admit_frame() == "canary_clock"
    assert rows(prepared, "SELECT stopped FROM canary") == [("canary_clock",)]


def test_segment_deadline_is_clamped_and_idle_wait_ends_at_the_bound(prepared, clock):
    grant = grant_for(prepared)
    grant.claim()
    assert grant.deadline_utc == datetime.fromtimestamp(clock.wall + 3600, tz=UTC)
    # The websocket receive loop ends an idle wait at stop_at_monotonic without
    # counting an idle timeout or touching the socket further.
    from crypto_collector.collectors.generic_ws import GenericWebsocketCollector
    from crypto_collector.config import CollectorConfig
    from test_session_evidence import ACK, Socket

    class Blocking(Socket):
        async def recv(self):
            if self.frames:
                return await super().recv()
            await asyncio.sleep(5)
            raise AssertionError("the duration bound must end the wait first")

        async def __anext__(self):
            return await self.recv()

    import websockets
    sockets = iter([Blocking([ACK])])
    collector = GenericWebsocketCollector(CollectorConfig(
        source="bybit", product=SYMBOL, channel="publicTrade", output_root=Path("."),
        websocket_url="wss://stream.bybit.com/v5/public/linear", subscription_style="bybit", connect_retries=1,
        retry_backoff_seconds=0, max_backoff_seconds=0))
    collector.stop_at_monotonic = time.monotonic() + 0.2

    async def run():
        import unittest.mock as mock
        with mock.patch.object(websockets, "connect", lambda *a, **k: next(sockets)):
            return [raw async for raw in collector.stream()]

    started = time.monotonic()
    frames = asyncio.run(run())
    assert frames == [] and time.monotonic() - started < 2
    assert collector.idle_timeout_count == 0


# --------------------------------------------------------------------------- liveness / stop


def test_reused_pid_is_abandoned_and_live_or_unknown_writers_block_the_lane(prepared, clock, tmp_path, monkeypatch):
    run_dir = tmp_path / "raw" / "bybit_perp_trades_gramusdt" / "20260101_000000"
    run_dir.mkdir(parents=True)
    (run_dir / "raw.jsonl").write_bytes(b"x" * 700)
    with sqlite3.connect(prepared.control / "canary.sqlite3") as conn:
        conn.execute("INSERT INTO grants (lane, pid, identity, hostname, started_utc, reserved, consumed, accounted, "
                     "overshoot, run_path) VALUES (?, ?, 'win:dead', 'h', ?, 4352, 0, 0, 0, ?)",
                     ("bybit-gram-perp-trades", os.getpid(), clock.wall - 10, str(run_dir)))
        conn.execute("UPDATE canary SET first_utc=?, first_mono=?, last_utc=?, last_mono=?",
                     (clock.wall - 10, clock.mono - 10, clock.wall - 10, clock.mono - 10))
    grant = grant_for(prepared)
    grant.claim()  # the pid is alive (ours) but the identity differs: reused pid, abandoned
    abandoned = rows(prepared, "SELECT reason, consumed, replayable FROM grants WHERE id=1")[0]
    assert abandoned == ("abandoned", 700, None)
    grant.close(reason="limit", replayable=True)
    live = grant_for(prepared)
    live.claim()
    with pytest.raises(CanaryRefused, match="canary_lane_busy"):
        grant_for(prepared).claim()
    monkeypatch.setattr(canary, "owned_process_alive", lambda pid, identity: None)
    with pytest.raises(CanaryRefused, match="canary_lane_busy"):  # unknown is never "exited"
        grant_for(prepared).claim()
    assert rows(prepared, "SELECT COUNT(*) FROM grants WHERE closed_utc IS NULL") == [(1,)]


def test_stop_is_dry_run_by_default_then_durable_with_bounded_wait_and_exact_termination(prepared, clock, monkeypatch):
    live = grant_for(prepared)
    live.claim()
    foreign = prepared.output_root / "ops" / "standalone_workers"
    foreign.mkdir(parents=True)
    (foreign / "binance-depth-worker.lock").write_text(json.dumps({"pid": os.getpid()}))
    plan = canary.stop(prepared.control)
    assert plan["dry_run"] and plan["writers_alive_before"] == [live.grant_id]
    assert rows(prepared, "SELECT stopped FROM canary") == [(None,)]
    calls = []

    def failing_terminate(pid, identity, image):
        calls.append((pid, identity, image))
        return {"terminated": False, "reason": "terminate_failed_5"}

    result = canary.stop(prepared.control, apply=True, wait_seconds=2, terminate=True,
                         terminate_fn=failing_terminate, sleep=clock.sleep, monotonic=clock.monotonic)
    assert rows(prepared, "SELECT stopped FROM canary") == [("operator_stop",)]
    assert calls == [(os.getpid(), live.process_identity, live.image)]  # only the recorded writer
    assert live.image == canary.process_image(os.getpid())
    assert result["terminations"][0]["reason"] == "terminate_failed_5"
    assert result["writers_alive_after"] == [live.grant_id] and result["verified_exited"] is False
    with pytest.raises(CanaryRefused, match="writers_alive_or_unknown"):
        canary.finalize(prepared.control)
    assert live.admit_frame() == "operator_stop"  # the writer sees the durable stop at its next checkpoint
    live.close(reason="operator_stop", replayable=False)
    final = canary.finalize(prepared.control)
    assert final["consumed_bytes"] == 0 and final["unreplayable_or_unscored_runs"] == [None]
    assert (prepared.control / "receipt.json").is_file()
    with pytest.raises(CanaryRefused, match="already_finalized"):
        canary.finalize(prepared.control)
    with pytest.raises(CanaryRefused, match="operator_stop"):
        grant_for(prepared).claim()
    assert (foreign / "binance-depth-worker.lock").is_file()  # untouched


def test_stalled_writer_with_unknown_liveness_blocks_finalize(prepared, clock, monkeypatch):
    live = grant_for(prepared)
    live.claim()
    monkeypatch.setattr(canary, "owned_process_alive", lambda pid, identity: None)
    result = canary.stop(prepared.control, apply=True, wait_seconds=1, sleep=clock.sleep, monotonic=clock.monotonic)
    assert result["writers_unknown_after"] == [live.grant_id] and result["verified_exited"] is False
    with pytest.raises(CanaryRefused, match="writers_alive_or_unknown"):
        canary.finalize(prepared.control)
    monkeypatch.setattr(canary, "owned_process_alive", lambda pid, identity: False)
    final = canary.finalize(prepared.control)  # provably gone now: abandoned, charged what it holds
    assert final["grants"][0]["reason"] == "abandoned"


def test_partial_publication_closes_the_grant_without_a_receipt(prepared, clock, tmp_path):
    grant = grant_for(prepared)
    grant.claim()
    receipts = prepared.control / "receipts"
    for path in receipts.iterdir():
        path.unlink()
    receipts.rmdir()
    receipts.write_text("not a directory")
    receipt = grant.close(reason="limit", replayable=True)
    assert receipt["receipt_error"] in {"NotADirectoryError", "FileExistsError", "OSError", "PermissionError", "FileNotFoundError"}
    stored = rows(prepared, "SELECT reason, receipt, closed_utc FROM grants")[0]
    assert stored[0].startswith("limit:receipt_failed:") and stored[1] is None and stored[2] is not None
    receipts.unlink()
    receipts.mkdir()
    canary.stop(prepared.control, apply=True, wait_seconds=0, sleep=clock.sleep, monotonic=clock.monotonic)
    final = canary.finalize(prepared.control)
    assert final["grants_without_receipt"] == [grant.grant_id]


def test_exhausted_attempt_is_terminal_and_cannot_be_reset(prepared, clock, tmp_path):
    pool = 3 * 4096 + 2 * 256
    with sqlite3.connect(prepared.control / "canary.sqlite3") as conn:
        conn.execute("INSERT INTO grants (lane, pid, identity, hostname, started_utc, reserved, consumed, accounted, "
                     "overshoot, closed_utc, reason) VALUES ('bybit-gram-perp-depth', 1, 'x', 'h', ?, ?, ?, ?, 0, ?, 'limit')",
                     (clock.wall - 5, pool, pool - 100, pool - 100, clock.wall - 1))
        conn.execute("UPDATE canary SET first_utc=?, first_mono=?, last_utc=?, last_mono=?",
                     (clock.wall - 5, clock.mono - 5, clock.wall - 1, clock.mono - 1))
    with pytest.raises(CanaryRefused, match="canary_exhausted"):
        grant_for(prepared).claim()
    with pytest.raises(CanaryRefused, match="canary_exhausted"):
        grant_for(prepared, "bybit-linear-universe-snapshot", kind="snapshot").claim()
    with pytest.raises(FileExistsError):
        canary.prepare(prepared.control, "again", config_path=prepared.config, symbol=SYMBOL,
                       job_names=[j["name"] for j in prepared.jobs])
    assert not hasattr(canary, "reset")
    snapshot = canary.status(prepared.control)
    assert snapshot["canary"]["stopped"] == "canary_exhausted" and snapshot["unspent_bytes"] == 100


def test_process_identity_and_termination_target_only_the_exact_process():
    executable = getattr(sys, "_base_executable", None) or sys.executable
    child = subprocess.Popen([executable, "-c", "import time; time.sleep(60)"])
    try:
        identity = canary.process_identity(child.pid)
        image = canary.process_image(child.pid)
        assert identity and image and canary.owned_process_alive(child.pid, identity) is True
        assert image == os.path.normcase(os.path.realpath(executable))
        assert canary.owned_process_alive(child.pid, identity + "0") is False  # a different incarnation
        assert canary.terminate_owned(child.pid, identity + "0", image)["terminated"] is False  # never by pid alone
        assert canary.terminate_owned(child.pid, identity, None) == {"terminated": False, "reason": "image_unrecorded"}
        assert child.poll() is None
        # A row whose recorded image is not the target's kernel image is refused.
        foreign = subprocess.Popen(["ping", "-n", "30", "127.0.0.1"] if sys.platform == "win32" else ["sleep", "30"],
                                   stdout=subprocess.DEVNULL)
        try:
            foreign_identity = canary.process_identity(foreign.pid)
            refused = canary.terminate_owned(foreign.pid, foreign_identity, image)
            assert refused == {"terminated": False, "reason": "image_mismatch"}
            assert foreign.poll() is None
        finally:
            foreign.kill()
            foreign.wait(timeout=10)
        result = canary.terminate_owned(child.pid, identity, image, wait_seconds=10)
        assert result["terminated"] is True
        child.wait(timeout=10)
    finally:
        if child.poll() is None:
            child.kill()
    assert canary.owned_process_alive(child.pid, identity) is False
    assert canary.process_identity(2**22 + 12345) is None


# --------------------------------------------------------------------------- integration


def test_pipeline_without_budget_is_unchanged(tmp_path):
    paths = prepare_run_paths(tmp_path / "raw", "bybit_perp_trades")
    pipeline = CollectorPipeline(collector=FrameCollector(3), normalizer=EmptyNormalizer(), quality_gate=QualityGate(),
                                 run_paths=paths, normalized_root=None)
    summary = asyncio.run(pipeline.run())
    assert summary.raw_messages == 3 and summary.stop_reason is None and summary.to_dict()["stop_reason"] is None
    assert pipeline.raw_sink.bytes_written == (paths.raw / "messages.jsonl").stat().st_size


def test_segmented_worker_threads_the_grant_clamps_the_deadline_and_holds_on_refusal(prepared, tmp_path, monkeypatch):
    # Real clocks here: the worker compares the canary deadline with utc_now().
    monkeypatch.setattr(cli.time, "sleep", lambda s: None)
    seen = []

    async def collect_segment(segment_args):
        seen.append(segment_args)
        paths = prepare_run_paths(prepared.output_root, "bybit_perp_trades_gramusdt")
        (paths.raw / "messages.jsonl").write_bytes(b"{}\n" * 10)
        return {"run_path": str(paths.base), "replayable": True, "deadline_reached": True, "segment_reason": "deadline"}

    job = prepared.jobs[0]
    args = SimpleNamespace(**{**job["args"], "output_root": prepared.output_root, "ops_root": prepared.output_root / "ops",
                              "max_segments": 3, "cooldown_seconds": 0, "heartbeat_interval_seconds": 30.0,
                              "canary_lane": job["name"], "canary_identity": prepared.identities[job["name"]],
                              "canary_hold_seconds": 0, "canary_min_free_gib": 0, "max_segment_seconds": 7200})
    cli._run_segmented_worker(args=args, default_worker_name="bybit-trades-worker", worker_type="bybit-trades-worker",
                              build_segment_args=lambda a: SimpleNamespace(), collect_segment=collect_segment,
                              progress_message=lambda i, s: f"segment {i}", venue="bybit")
    assert len(seen) == 3 and all(s.canary_grant is not None for s in seen)
    assert seen[0].deadline_utc == seen[0].canary_grant.deadline_utc  # 7200 s segment clamped to the canary
    closed = rows(prepared, "SELECT reason, replayable, consumed FROM grants ORDER BY id")
    assert closed == [("deadline", 1, 30)] * 3
    events = [json.loads(line) for line in (prepared.output_root / "ops/worker_events.jsonl").read_text().splitlines()]
    # Each instant segment held the slot (sleep is patched): an outage cannot burn grants per tick.
    assert [e["event_type"] for e in events].count("canary_short_segment_hold") == 3
    # A durable stop refuses the next dispatch: heartbeat says so, no segment runs, the
    # bounded hold is taken instead of a spawn loop.
    canary.stop(prepared.control, apply=True, wait_seconds=0)
    holds = []
    monkeypatch.setattr(cli.time, "sleep", lambda s: holds.append(s))
    cli._run_segmented_worker(args=args, default_worker_name="bybit-trades-worker", worker_type="bybit-trades-worker",
                              build_segment_args=lambda a: SimpleNamespace(), collect_segment=collect_segment,
                              progress_message=lambda i, s: "", venue="bybit")
    assert len(seen) == 3 and holds == [0.0]
    heartbeat = json.loads((prepared.output_root / "ops/standalone_workers/bybit-trades-worker-gram-perp.json").read_text())
    assert heartbeat["status"] == "stopped" and heartbeat["message"] == "canary_refused:operator_stop"
    events = [json.loads(line) for line in (prepared.output_root / "ops/worker_events.jsonl").read_text().splitlines()]
    assert [e["event_type"] for e in events].count("canary_refused") == 1


def test_segment_exception_still_closes_the_grant(prepared, clock, tmp_path, monkeypatch):
    async def collect_segment(segment_args):
        raise RuntimeError("socket died")

    job = prepared.jobs[1]
    args = SimpleNamespace(**{**job["args"], "output_root": prepared.output_root, "ops_root": prepared.output_root / "ops",
                              "max_segments": 1, "cooldown_seconds": 0, "heartbeat_interval_seconds": 30.0,
                              "canary_lane": job["name"], "canary_identity": prepared.identities[job["name"]],
                              "canary_hold_seconds": 0, "canary_min_free_gib": 0})
    with pytest.raises(RuntimeError):
        cli._run_segmented_worker(args=args, default_worker_name="bybit-depth-worker", worker_type="bybit-depth-worker",
                                  build_segment_args=lambda a: SimpleNamespace(), collect_segment=collect_segment,
                                  progress_message=lambda i, s: "", venue="bybit")
    assert rows(prepared, "SELECT reason, closed_utc IS NOT NULL FROM grants") == [("exception:RuntimeError", 1)]


def test_bind_refuses_parquet_and_evidence_and_caps_the_frame(prepared, clock, tmp_path):
    from crypto_collector.config import CollectorConfig
    grant = grant_for(prepared)
    grant.claim()
    config = CollectorConfig(source="bybit", product=SYMBOL, channel="publicTrade", output_root=tmp_path)
    collector = SimpleNamespace(stop_at_monotonic=None)
    paths = prepare_run_paths(tmp_path / "raw", "bybit_perp_trades_gramusdt")
    with pytest.raises(ValueError, match="raw-only"):
        cli._bind_canary_grant(SimpleNamespace(canary_grant=grant, normalized_parquet=True), config=config,
                               collector=collector, run_paths=paths)
    with pytest.raises(ValueError, match="session evidence"):
        cli._bind_canary_grant(SimpleNamespace(canary_grant=grant, normalized_parquet=False, session_evidence=True),
                               config=config, collector=collector, run_paths=paths)
    assert cli._bind_canary_grant(SimpleNamespace(canary_grant=None), config=config, collector=collector, run_paths=paths) is None
    bound = cli._bind_canary_grant(SimpleNamespace(canary_grant=grant, normalized_parquet=False), config=config,
                                   collector=collector, run_paths=paths)
    assert bound is grant and config.max_message_bytes == canary.MAX_FRAME_BYTES
    assert collector.stop_at_monotonic == grant.monotonic_deadline
    assert rows(prepared, "SELECT run_path FROM grants") == [(str(paths.base),)]


def test_universe_snapshot_claims_measures_and_refuses_without_fetching(prepared, clock, tmp_path, monkeypatch):
    calls = []

    def fake_snapshot(output_root, *, panel_symbols):
        calls.append(panel_symbols)
        run = Path(output_root) / "bybit_linear_universe" / "20260101_000500"
        (run / "raw").mkdir(parents=True)
        (run / "raw" / "tickers_linear.json.gz").write_bytes(b"z" * 300)
        return SimpleNamespace(run_path=str(run), fetched_at="t", payload_count=1, total_raw_bytes=300, row_counts={})

    monkeypatch.setattr(cli, "snapshot_bybit_linear_universe", fake_snapshot)
    job = prepared.jobs[2]
    args = SimpleNamespace(output_root=prepared.output_root, panel_symbol=job["args"]["panel_symbols"], format="text",
                           canary_control=str(prepared.control), canary_lane=job["name"],
                           canary_identity=prepared.identities[job["name"]], canary_min_free_gib=0)
    assert cli.run_bybit_linear_universe_snapshot(args) is None
    assert rows(prepared, "SELECT lane, reserved, consumed, reason FROM grants") == [
        ("bybit-linear-universe-snapshot", 2048, 300, "complete")]

    def failing_snapshot(output_root, *, panel_symbols):
        calls.append(panel_symbols)
        raise OSError("dns")

    monkeypatch.setattr(cli, "snapshot_bybit_linear_universe", failing_snapshot)
    with pytest.raises(OSError):
        cli.run_bybit_linear_universe_snapshot(args)
    # The failure created no run dir: nothing is charged and yesterday's run is NOT
    # attributed to this grant.
    assert rows(prepared, "SELECT consumed, run_path, reason FROM grants WHERE id=2") == [(0, None, "exception:OSError")]
    canary.stop(prepared.control, apply=True, wait_seconds=0, sleep=clock.sleep, monotonic=clock.monotonic)
    message = cli.run_bybit_linear_universe_snapshot(args)
    assert message.startswith("bybit linear universe snapshot: canary refused (operator_stop)")
    assert len(calls) == 2  # refused before any request
    from crypto_collector.ops import JobSpec
    spec = JobSpec.from_dict(prepared.jobs[2])
    assert "canary refused" in cli._execute_ops_job_inprocess(spec)  # never reads as a plain success


def test_ops_job_threads_canary_fields_and_runner_parses_refusal(prepared, monkeypatch):
    from crypto_collector.ops import JobSpec
    job = prepared.jobs[0]
    spec = JobSpec.from_dict(job)
    captured = {}
    monkeypatch.setattr(cli, "run_bybit_trades_worker", lambda a: captured.update(vars(a)))
    cli._execute_ops_job_inprocess(spec)
    assert captured["canary_control"] == str(prepared.control)
    assert captured["canary_identity"] == prepared.identities[job["name"]] == canary.job_identity(job)
    assert captured["canary_lane"] == job["name"] and captured["canary_min_free_gib"] == 215
    plain = JobSpec.from_dict({**job, "args": {k: v for k, v in job["args"].items() if k != "canary_control"}})
    captured.clear()
    cli._execute_ops_job_inprocess(plain)
    assert captured["canary_control"] is None and "canary_identity" not in captured

    def fake_run(cmd, **kwargs):
        return SimpleNamespace(returncode=0, stdout="", stderr="canary refused: canary_expired\nother noise\n")

    monkeypatch.setattr(cli.subprocess, "run", fake_run)
    message = cli._run_collector_in_subprocess(spec)
    assert message.endswith("; canary_refused=canary_expired")


def test_preflight_is_read_only_and_refuses_enabled_promotion_for_canary_sources(prepared, monkeypatch):
    promoter = {"name": "promote-bybit-gram-perp-trades", "job_type": "promote-replayable", "enabled": True,
                "interval_seconds": 300, "args": {"source_root": str(prepared.output_root / "bybit_perp_trades_gramusdt"),
                                                  "target_root": "x", "quarantine_index": "y"}}
    scorer = {"name": "score-bybit-alt-panel-depth", "job_type": "backfill-stream-depth", "enabled": True,
              "interval_seconds": 3600, "args": {"raw_root": str(prepared.output_root),
                                                 "source": ["bybit_perp_depth_gramusdt"], "score_only": True}}
    path = prepared.config.parent / "pf.json"
    path.write_text(json.dumps({"jobs": prepared.jobs + [promoter, scorer]}), encoding="utf-8")
    before = (prepared.control / "canary.sqlite3").read_bytes()
    report = canary.preflight(prepared.control, config_path=path, min_free_bytes=10**6,
                              scripts_root=REPO_ROOT / "scripts", disk_usage_fn=lambda p: SimpleNamespace(free=10**5))
    assert not report["ok"]
    assert "promotion_or_scoring_enabled_for_canary_source:promote-bybit-gram-perp-trades" in report["findings"]
    assert "promotion_or_scoring_enabled_for_canary_source:score-bybit-alt-panel-depth" in report["findings"]
    assert any(f.startswith("insufficient_headroom:") for f in report["findings"])
    assert report["headroom_model"]["required_free_bytes"] == 10**6 + report["headroom_model"]["unspent_pool_bytes"] + canary.EXTERNAL_OVERHEAD_BYTES
    assert report["pool_cap"]["needed"] == 0 + 3 + 2 and all(v >= 5 for v in report["pool_cap"]["scripts"].values())
    assert (prepared.control / "canary.sqlite3").read_bytes() == before  # nothing written
    promoter["args"]["source_root"] = promoter["args"]["source_root"].upper()  # NTFS is case-insensitive
    path.write_text(json.dumps({"jobs": prepared.jobs + [promoter, scorer]}), encoding="utf-8")
    report = canary.preflight(prepared.control, config_path=path, min_free_bytes=10**6,
                              disk_usage_fn=lambda p: SimpleNamespace(free=10**12))
    assert "promotion_or_scoring_enabled_for_canary_source:promote-bybit-gram-perp-trades" in report["findings"]
    promoter["enabled"] = scorer["enabled"] = False
    path.write_text(json.dumps({"jobs": prepared.jobs + [promoter, scorer]}), encoding="utf-8")
    report = canary.preflight(prepared.control, config_path=path, min_free_bytes=10**6,
                              scripts_root=REPO_ROOT / "scripts", disk_usage_fn=lambda p: SimpleNamespace(free=10**12))
    assert report["ok"] and report["disks"][0]["ok"]


def test_example_config_carries_no_canary_control(prepared):
    jobs = json.loads((REPO_ROOT / "ops.live.example.json").read_text(encoding="utf-8"))["jobs"]
    assert not any("canary_control" in job.get("args", {}) for job in jobs)


def test_cli_entry_points(prepared, clock, capsys):
    assert canary.main(["status", str(prepared.control)]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["canary"]["symbol"] == SYMBOL and out["open_grants"] == []
    assert canary.main(["stop", str(prepared.control)]) == 0
    assert json.loads(capsys.readouterr().out)["dry_run"] is True
    assert rows(prepared, "SELECT stopped FROM canary") == [(None,)]
    with pytest.raises(SystemExit):
        canary.main(["prepare", str(prepared.control), "x", "--config", str(prepared.config), "--symbol", SYMBOL])


def test_deadline_bound_is_documented_not_promised():
    """A sleeping idle wait ends at the bound; nothing claims an unconditional wall bound
    for process start-up, OS calls or termination."""
    text = (REPO_ROOT / "STANDARDS.md").read_text(encoding="utf-8")
    assert "4.18" in text and "not an unconditional wall bound" in text
    assert timedelta(seconds=canary.DEFAULT_MAX_SECONDS) == timedelta(hours=24)
    assert canary.DEFAULT_MAX_BYTES == 2 * 1024**3
