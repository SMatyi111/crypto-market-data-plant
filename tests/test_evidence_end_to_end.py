"""Synthetic recorded Bybit session end to end: lease claim, multiplexed books and
tickers, public reference records, terminal publication, independent rehash of raw
and journal, reference verification and source admission; then the negatives.

Software correctness only. No venue request, live capture or economic result:
`economic_admission` stays False and nothing here authorizes a trial.
"""
from __future__ import annotations

import asyncio
import itertools
import json
import sqlite3
import time
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from crypto_collector import bybit_references as refs
from crypto_collector import evidence_lease as lease
from crypto_collector import session_evidence as journal
from crypto_collector.collectors import generic_ws
from crypto_collector.evidence_budget import GIB, MAX_BYTES
from crypto_collector.session_evidence import file_info
from test_bybit_references import instrument, response, ticker
from test_session_evidence import ACK, book, setup


@pytest.fixture
def directory(tmp_path):
    path = tmp_path / "lease"
    lease.prepare(path, "offline-test")
    return path


@pytest.fixture(autouse=True)
def no_external_io(monkeypatch):
    monkeypatch.setattr(journal, "disk_usage", lambda p: SimpleNamespace(free=200 * GIB))
    monkeypatch.setattr(refs.subprocess, "Popen", lambda *a, **k: pytest.fail("No real HTTP helper"))


@pytest.fixture
def shared_clock(monkeypatch):
    """One monotonic/UTC clock for the collector, journal and reference threads:
    every read advances one millisecond, so receipts are strictly ordered without
    depending on the platform timer resolution. The lease keeps the real clock."""
    origin = datetime.now(UTC)
    mono_origin = time.monotonic_ns()
    ticks = itertools.count(1)
    def utc_now():
        return origin + timedelta(milliseconds=next(ticks))
    def monotonic_ns():
        return mono_origin + next(ticks) * 1_000_000
    for module in (generic_ws, journal, refs):
        monkeypatch.setattr(module, "utc_now", utc_now)
    monkeypatch.setattr(time, "monotonic_ns", monotonic_ns)
    return origin


def recorded_session(tmp_path, monkeypatch, directory, frames=None, run="data", hours=0):
    if hours:
        # Distinct run id for a second segment in the same wall-clock second.
        import test_session_evidence
        from crypto_collector.storage import prepare_run_paths
        monkeypatch.setattr(test_session_evidence, "prepare_run_paths", lambda root, source:
                            prepare_run_paths(root, source, datetime.now(UTC) + timedelta(hours=hours)))
    future = int(time.time() * 1000) + 3_600_000
    frames = frames if frames is not None else [
        ticker(future), book(), ticker(0, "delta", markPrice="60002"),
        book("delta", 2), book("delta", 3)]
    paths, pipeline, evidence, sockets = setup(tmp_path / run, monkeypatch, [frames],
        reference_mode=True, max_bytes=MAX_BYTES, lease_directory=directory, trial_id="offline-test")
    assert evidence.ready.wait(2) and evidence.error is None
    sockets[0].frames.insert(0, {**ACK, "req_id": evidence.process_session})
    calls = []
    def fetch(stage, *args):
        calls.append(stage)
        return response(instrument() if stage != "funding_history"
                        else {"retCode": 0, "result": {"category": "linear", "list": []}})
    controller = refs.BybitReferences(evidence, fetch=fetch)
    pipeline.collector.reference_evidence = controller
    return paths, pipeline, evidence, controller, calls


def test_recorded_session_is_published_rehashed_and_source_admitted(tmp_path, monkeypatch, directory, shared_clock):
    paths, pipeline, evidence, controller, calls = recorded_session(tmp_path, monkeypatch, directory)
    assert asyncio.run(pipeline.run(limit=3)).raw_messages == 3
    refs.drain_reference_finalizers()
    assert not controller.thread.is_alive() and evidence.error is None
    assert calls == ["rules_before", "rules_after", "funding_history"]

    manifest = json.loads((paths.base / "session_evidence/manifest.json").read_text())
    assert manifest["session_admitted"] and manifest["capture_complete"] and not manifest["issues"]
    assert manifest["version"] == 2 and manifest["raw_count"] == 3
    assert manifest["raw_accounting"] == {"mode": "streamed", "files_closed": True, "durable_close": True}
    assert manifest["evidence_lease"]["slot"] == 1
    assert set(manifest["terminal_timing_ms"]) >= {"drain", "journal_fsync", "lease_checkpoint", "raw_accounting"}
    # Independent rehash of every published file equals the streamed account.
    assert manifest["journal"] == file_info(paths.base / "session_evidence/events.jsonl", paths.base)
    assert manifest["raw_files"] == [file_info(p, paths.base) for p in journal.raw_files(paths.base)]
    assert not (paths.base / "session_evidence/manifest.tmp").exists()

    verified = journal.verify_session_evidence(paths.base)
    assert verified["session_admitted"] and not verified["economic_admission"]
    result = refs.verify_bybit_references(paths.base)
    assert result["reference_integrity"] and result["no_settlement_supported"]
    assert result["economic_admission"] is False
    rows = [json.loads(line) for line in (paths.base / "session_evidence/events.jsonl").read_text().splitlines()]
    assert [r["kind"] for r in rows][-5:] == ["connection_end", "http_reference", "http_reference",
                                               "http_reference", "terminal"]
    assert len([r for r in rows if r["kind"] == "raw_written"]) == 3
    with sqlite3.connect(directory / "lease.sqlite3") as conn:
        assert conn.execute("SELECT stopped FROM trial").fetchone()[0] is None
        assert conn.execute("SELECT count(*) FROM claims").fetchone()[0] == 1


@pytest.mark.parametrize("tamper", ["raw_byte", "raw_byte_rehashed_manifest", "raw_missing",
                                    "journal_append", "reference_dropped", "manifest_tmp_only"])
def test_tampered_or_incomplete_session_is_refused(tmp_path, monkeypatch, directory, shared_clock, tamper):
    paths, pipeline, evidence, controller, _ = recorded_session(tmp_path, monkeypatch, directory)
    assert asyncio.run(pipeline.run(limit=3)).raw_messages == 3
    refs.drain_reference_finalizers()
    refs.verify_bybit_references(paths.base)
    parts = journal.raw_files(paths.base)
    assert len(parts) > 1  # the test sink rotates, so parts and the active file are covered
    raw = parts[0]
    manifest_path = paths.base / "session_evidence/manifest.json"
    journal_path = paths.base / "session_evidence/events.jsonl"
    manifest = json.loads(manifest_path.read_text())
    if tamper in {"raw_byte", "raw_byte_rehashed_manifest"}:
        lines = raw.read_text(encoding="utf-8").splitlines(keepends=True)
        assert '"10"' in lines[0]
        lines[0] = lines[0].replace('"10"', '"12"', 1)
        raw.write_text("".join(lines), encoding="utf-8")
        if tamper == "raw_byte_rehashed_manifest":
            manifest["raw_files"][0] = file_info(raw, paths.base)
            manifest_path.write_text(json.dumps(manifest))
    elif tamper == "raw_missing":
        raw.unlink()
    elif tamper == "journal_append":
        with journal_path.open("ab") as stream:
            stream.write(b'{"seq":999}\n')
    elif tamper == "reference_dropped":
        rows = [json.loads(line) for line in journal_path.read_text().splitlines()]
        rows = [r for r in rows if not (r["kind"] == "http_reference" and r["stage"] == "funding_history")]
        for seq, row in enumerate(rows, 1):
            row["seq"] = seq
        journal_path.write_text("".join(json.dumps(r) + "\n" for r in rows))
        manifest["journal"] = file_info(journal_path, paths.base)
        manifest_path.write_text(json.dumps(manifest))
    else:
        manifest_path.rename(paths.base / "session_evidence/manifest.tmp")
    expected = {"raw_byte": "Raw file set/hash/count mismatch",
                "raw_byte_rehashed_manifest": "Raw envelope binding mismatch",
                "raw_missing": "Raw file set/hash/count mismatch",
                "journal_append": "Journal hash/count mismatch",
                "reference_dropped": "Missing/duplicate reference attempts"}
    if tamper == "manifest_tmp_only":
        with pytest.raises(FileNotFoundError):
            refs.verify_bybit_references(paths.base)
    else:
        with pytest.raises(ValueError, match=expected[tamper]):
            refs.verify_bybit_references(paths.base)


def test_interrupted_session_publishes_unadmitted_and_spends_its_slot(tmp_path, monkeypatch, directory, shared_clock):
    frames = [ticker(int(time.time() * 1000) + 3_600_000), book(), asyncio.CancelledError()]
    paths, pipeline, evidence, controller, calls = recorded_session(tmp_path, monkeypatch, directory, frames)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(pipeline.run(limit=3))
    refs.drain_reference_finalizers()
    assert calls == ["rules_before"]  # no post-close references after an interrupted capture
    manifest = json.loads((paths.base / "session_evidence/manifest.json").read_text())
    assert manifest["reason"] == "CancelledError" and not manifest["capture_complete"]
    assert not manifest["session_admitted"] and manifest["raw_count"] == 1
    assert manifest["raw_accounting"]["mode"] == "streamed"
    with pytest.raises(ValueError, match="not admitted"):
        journal.verify_session_evidence(paths.base)
    with sqlite3.connect(directory / "lease.sqlite3") as conn:
        assert conn.execute("SELECT count(*) FROM claims").fetchone()[0] == 1
    # The slot is spent even though nothing was admitted; the same run cannot claim again.
    with pytest.raises(lease.LeaseRefused, match="already_spent"):
        lease.EvidenceLease(directory, "offline-test", paths.base.name).claim()


def test_second_segment_after_a_failed_terminal_still_gets_its_own_slot(tmp_path, monkeypatch, directory, shared_clock):
    """Ordinary capture and the next segment's optional evidence continue after a
    terminal stall: the stalled writer is released, the slot is spent, the next run
    claims slot two and publishes normally."""
    paths, pipeline, evidence, controller, _ = recorded_session(tmp_path, monkeypatch, directory, run="first")
    monkeypatch.setattr(journal, "TERMINAL_WAIT_SECONDS", 0.3)
    from test_session_evidence import stall_at
    release, entered = stall_at(evidence, "manifest_write")
    try:
        assert asyncio.run(pipeline.run(limit=3)).raw_messages == 3
        refs.drain_reference_finalizers()
        assert evidence.error == "writer_close_timeout"
    finally:
        release.set()
        evidence._thread.join(5)
    assert not (paths.base / "session_evidence/manifest.json").exists()
    assert sum(p.read_text().count("\n") for p in journal.raw_files(paths.base)) == 3
    monkeypatch.setattr(journal, "TERMINAL_WAIT_SECONDS", 2)
    paths2, pipeline2, evidence2, controller2, _ = recorded_session(tmp_path, monkeypatch, directory,
                                                                    run="second", hours=1)
    assert paths2.base.name != paths.base.name
    assert asyncio.run(pipeline2.run(limit=3)).raw_messages == 3
    refs.drain_reference_finalizers()
    assert evidence2.error is None
    manifest = refs.verify_bybit_references(paths2.base)
    assert manifest["economic_admission"] is False
    assert json.loads((paths2.base / "session_evidence/manifest.json").read_text())["evidence_lease"]["slot"] == 2
    with pytest.raises(lease.LeaseRefused, match="lease_spent"):
        lease.EvidenceLease(directory, "offline-test", "third").claim()
