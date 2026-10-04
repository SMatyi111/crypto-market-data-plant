from __future__ import annotations

import asyncio
import json
import sqlite3
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from types import SimpleNamespace

import pytest

from crypto_collector import evidence_lease as lease
from crypto_collector import session_evidence as journal
from crypto_collector.evidence_budget import GIB, MAX_BYTES
from crypto_collector.models import utc_now
from test_session_evidence import ACK, book, setup

REAL_POPEN = subprocess.Popen


@pytest.fixture
def directory(tmp_path):
    path = tmp_path / "lease"
    lease.prepare(path, "offline-test")
    return path


@pytest.fixture(autouse=True)
def no_external_io(monkeypatch):
    monkeypatch.setattr(journal, "disk_usage", lambda p: SimpleNamespace(free=200 * GIB))
    import crypto_collector.bybit_references as refs
    monkeypatch.setattr(refs.subprocess, "Popen", lambda *a, **k: pytest.fail("No real HTTP helper"))


def claim(directory, run="run-1"):
    obj = lease.EvidenceLease(directory, "offline-test", run)
    obj.claim()
    return obj


def rows(directory):
    with sqlite3.connect(directory / "lease.sqlite3") as conn:
        return conn.execute("SELECT slot,run_id,reserved FROM claims ORDER BY slot").fetchall()


def test_two_nonrefundable_claims_survive_restart_and_absent_run_files(directory):
    first = claim(directory)
    deadline = first.deadline
    first.close()  # No artifacts created: reservation is still spent.
    second = claim(directory, "run-2")
    assert second.deadline == deadline
    second.close()
    with pytest.raises(lease.LeaseRefused, match="spent"):
        claim(directory, "run-3")
    assert rows(directory) == [(1, "run-1", lease.RESERVATION_BYTES), (2, "run-2", lease.RESERVATION_BYTES)]
    assert sum(r[2] for r in rows(directory)) == lease.TOTAL_BYTES == 1040 * 1024**2
    with pytest.raises(FileExistsError):
        lease.prepare(directory, "different-id")


def test_duplicate_run_cannot_get_another_slot(directory):
    first = claim(directory)
    first.close()
    with pytest.raises(lease.LeaseRefused, match="already_spent"):
        claim(directory)
    assert len(rows(directory)) == 1


def test_competing_writer_is_refused_but_its_committed_slot_is_spent(directory):
    first = claim(directory)
    try:
        with pytest.raises(OSError):
            claim(directory, "run-2")
        assert len(rows(directory)) == 2
    finally:
        first.close()
    with pytest.raises(lease.LeaseRefused, match="spent"):
        claim(directory, "run-3")


def test_concurrent_claims_cannot_overbook(directory):
    barrier = threading.Barrier(6)
    def attempt(i):
        barrier.wait()
        try:
            obj = claim(directory, f"run-{i}")
            obj.close()
            return True
        except (OSError, sqlite3.Error, lease.LeaseRefused):
            return False
    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(attempt, range(6)))
    assert 1 <= sum(results) <= 2
    assert 1 <= len(rows(directory)) <= 2


def test_process_crash_releases_writer_without_refunding_claim(directory, monkeypatch):
    # Explicit exception to the no-child fixture: only this offline Python snippet.
    monkeypatch.setattr(subprocess, "Popen", REAL_POPEN)
    source = ("import os,sys; from pathlib import Path; "
              "from crypto_collector.evidence_lease import EvidenceLease; "
              "x=EvidenceLease(Path(sys.argv[1]),'offline-test','crashed-run'); "
              "x.claim(); os._exit(7)")
    result = subprocess.run([sys.executable, "-c", source, str(directory)],
                            timeout=10, capture_output=True)
    assert result.returncode == 7, result.stderr
    assert len(rows(directory)) == 1
    survivor = claim(directory, "run-2")
    assert survivor.slot == 2
    survivor.close()
    assert sum(p.stat().st_size for p in directory.iterdir()) < lease.CONTROL_BYTES


@pytest.mark.parametrize("damage", ["missing", "corrupt", "wrong_id", "wrong_reserved", "missing_lock"])
def test_missing_corrupt_or_wrong_state_never_reinitialized(directory, damage):
    db = directory / "lease.sqlite3"
    if damage == "missing":
        db.rename(directory / "saved.sqlite3")
    elif damage == "corrupt":
        db.write_bytes(b"corrupt")
    elif damage == "missing_lock":
        (directory / "writer.lock").rename(directory / "saved.lock")
    else:
        with sqlite3.connect(db) as conn:
            conn.execute("UPDATE trial SET " + ("id='wrong'" if damage == "wrong_id" else "reserved=1"))
    with pytest.raises((sqlite3.Error, OSError, lease.LeaseRefused)):
        claim(directory)
    if damage == "missing":
        assert not db.exists()


@pytest.mark.parametrize("jump", [-2, 3600])
def test_restart_clock_reversal_or_expiry_permanently_stops_lease(directory, monkeypatch, jump):
    clock = [10_000.0, 100.0]
    monkeypatch.setattr(lease.time, "time", lambda: clock[0])
    monkeypatch.setattr(lease.time, "monotonic", lambda: clock[1])
    first = claim(directory)
    first.close()
    clock[0] += jump
    clock[1] += max(jump, 1)
    with pytest.raises(lease.LeaseRefused):
        claim(directory, "run-2")
    clock[:] = [10_001.0, 101.0]
    with pytest.raises(lease.LeaseRefused, match="stopped"):
        claim(directory, "run-2")
    assert len(rows(directory)) == 1


def test_active_clock_reversal_is_durable(directory, monkeypatch):
    clock = [10_000.0, 100.0]
    monkeypatch.setattr(lease.time, "time", lambda: clock[0])
    monkeypatch.setattr(lease.time, "monotonic", lambda: clock[1])
    obj = claim(directory)
    try:
        clock[:] = [9999.0, 101.0]
        with pytest.raises(lease.LeaseRefused, match="clock"):
            obj.check()
    finally:
        obj.close()
    clock[:] = [10_002.0, 102.0]
    with pytest.raises(lease.LeaseRefused, match="stopped"):
        claim(directory, "run-2")


def test_leased_pipeline_can_publish_and_verify(directory, tmp_path, monkeypatch):
    paths, pipeline, evidence, _ = setup(tmp_path / "data", monkeypatch, [[ACK, book()]],
        max_bytes=MAX_BYTES, lease_directory=directory, trial_id="offline-test")
    assert evidence.ready.wait(2)
    assert evidence.error is None
    assert asyncio.run(pipeline.run(limit=1)).raw_messages == 1
    manifest = journal.verify_session_evidence(paths.base)
    assert manifest["evidence_lease"]["slot"] == 1
    assert manifest["evidence_lease"]["total_bytes"] == lease.TOTAL_BYTES
    p = paths.base / "session_evidence/manifest.json"
    manifest["evidence_lease"]["slot"] = 3
    p.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="lease"):
        journal.verify_session_evidence(paths.base)


@pytest.mark.parametrize("fault", ["headroom", "missing_state", "metadata", "expiry"])
def test_optional_failures_preserve_market_rows(directory, tmp_path, monkeypatch, fault):
    if fault == "headroom":
        monkeypatch.setattr(journal, "disk_usage", lambda p: SimpleNamespace(free=100 * GIB + lease.TOTAL_BYTES - 1))
    if fault == "missing_state":
        (directory / "lease.sqlite3").rename(directory / "saved.sqlite3")
    if fault == "metadata":
        monkeypatch.setattr(lease, "MANIFEST_BYTES", 1)
    paths, pipeline, evidence, _ = setup(tmp_path / "data", monkeypatch, [[ACK, book()]],
        max_bytes=MAX_BYTES, lease_directory=directory, trial_id="offline-test")
    assert evidence.ready.wait(2)
    if fault == "expiry":
        evidence.lease.monotonic_deadline = 0
    assert asyncio.run(pipeline.run(limit=1)).raw_messages == 1
    assert evidence.error
    assert not (paths.base / "session_evidence/manifest.json").exists()
    if fault in ("headroom", "missing_state"):
        assert not (paths.base / "session_evidence").exists()
    if fault != "missing_state":
        assert len(rows(directory)) == 1


def test_cli_missing_lease_continues_with_original_market_collector(directory, tmp_path, monkeypatch):
    import crypto_collector.cli as cli
    (directory / "lease.sqlite3").rename(directory / "saved.sqlite3")
    captured = []
    class StopBeforeMarket(Exception):
        pass
    def fake_pipeline(**kwargs):
        captured.append(kwargs["collector"])
        raise StopBeforeMarket()
    monkeypatch.setattr(cli, "CollectorPipeline", fake_pipeline)
    args = SimpleNamespace(market="linear", session_evidence=True, reference_evidence=True,
        symbol="BTCUSDT", channel="orderbook.50", output_root=tmp_path / "data",
        session_evidence_max_mib=512, session_evidence_lease=directory,
        session_evidence_trial_id="offline-test", deadline_utc=utc_now() + timedelta(seconds=1800))
    with pytest.raises(StopBeforeMarket):
        asyncio.run(cli.collect_bybit_depth_segment(args))
    assert captured[0].session_evidence is None
    assert captured[0].reference_evidence is None


def test_central_dispatch_preserves_lease(directory, tmp_path, monkeypatch):
    import crypto_collector.cli as cli
    from crypto_collector.ops import JobSpec
    captured = []
    async def segment(args):
        captured.append((args.session_evidence_lease, args.session_evidence_trial_id))
        return {"run_path": str(tmp_path / "run"), "clean_events": 0, "replayable": True}
    monkeypatch.setattr(cli, "collect_bybit_depth_segment", segment)
    job = JobSpec(name="test", job_type="bybit-depth-worker", interval_seconds=60,
        args={"session_evidence": True, "session_evidence_lease": str(directory),
              "session_evidence_trial_id": "offline-test", "market": "linear", "max_segments": 1,
              "output_root": str(tmp_path / "data"), "ops_root": str(tmp_path / "ops")})
    cli._execute_ops_job_inprocess(job)
    assert captured == [(str(directory), "offline-test")]


def test_expired_lease_stops_reference_requests(directory, tmp_path):
    from crypto_collector.bybit_references import BybitReferences
    root = tmp_path / "reference-run"
    root.mkdir()
    evidence = journal.SessionEvidence(root, max_bytes=MAX_BYTES, reference_mode=True,
        lease_directory=directory, trial_id="offline-test")
    assert evidence.ready.wait(2)
    calls = []
    refs = BybitReferences(evidence, fetch=lambda *a: calls.append(a))
    evidence.lease.monotonic_deadline = 0
    refs._fetch("rules_before")
    evidence.finish(reason="test", sinks_closed=False)
    assert not calls
    assert evidence.error


def test_disabled_path_never_opens_lease(tmp_path, monkeypatch):
    monkeypatch.setattr(lease, "_connect", lambda *a: pytest.fail("Disabled lease opened state"))
    paths, pipeline, _, _ = setup(tmp_path, monkeypatch, [[ACK, book()]], enabled=False)
    assert asyncio.run(pipeline.run(limit=1)).raw_messages == 1
    assert not (paths.base / "session_evidence").exists()
