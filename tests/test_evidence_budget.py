from __future__ import annotations

import asyncio
import json
import threading
import time
from types import SimpleNamespace

import pytest

from crypto_collector import session_evidence as journal
from crypto_collector.evidence_budget import GIB, MIB, configured_budget, required_free_bytes
from test_session_evidence import ACK, book, setup


@pytest.fixture(autouse=True)
def synthetic_disk(monkeypatch):
    monkeypatch.setattr(journal, "disk_usage", lambda p: SimpleNamespace(free=200 * GIB))


@pytest.mark.parametrize("cap,floor", [(0, 100), (513, 100), (True, 100), (64.5, 100),
                                      ("512", 100), (64, 99), (64, 4097), (64, True), (64, None)])
def test_invalid_config_rejected_before_capture(tmp_path, cap, floor):
    from crypto_collector.cli import collect_bybit_depth_segment
    args = SimpleNamespace(market="linear", session_evidence=True, symbol="BTCUSDT",
                           channel="orderbook.50", output_root=tmp_path,
                           session_evidence_max_mib=cap, session_evidence_min_free_gib=floor)
    with pytest.raises(ValueError):
        asyncio.run(collect_bybit_depth_segment(args))
    assert not list(tmp_path.iterdir())


def test_defaults_and_complete_remaining_budget():
    assert configured_budget() == (64 * MIB, 100 * GIB)
    assert configured_budget(512, 150) == (512 * MIB, 150 * GIB)
    assert required_free_bytes(512 * MIB, 12 * MIB, 100 * GIB) == 100 * GIB + 500 * MIB


@pytest.mark.parametrize("cap", [64, 512])
def test_enabled_cli_builds_exact_budget(tmp_path, monkeypatch, cap):
    import crypto_collector.cli as cli
    captured = []
    class StopBeforePipeline(Exception):
        pass
    def fake_evidence(root, **kwargs):
        captured.append(kwargs)
        raise StopBeforePipeline()
    monkeypatch.setattr(journal, "SessionEvidence", fake_evidence)
    args = SimpleNamespace(market="linear", session_evidence=True, symbol="BTCUSDT",
                           channel="orderbook.50", output_root=tmp_path,
                           session_evidence_max_mib=cap, session_evidence_min_free_gib=150)
    with pytest.raises(StopBeforePipeline):
        asyncio.run(cli.collect_bybit_depth_segment(args))
    assert captured == [{"reference_mode": False, "max_bytes": cap * MIB, "min_free_bytes": 150 * GIB}]


@pytest.mark.parametrize("config,expected", [({}, (64, 100)),
                                            ({"session_evidence_max_mib": 512,
                                              "session_evidence_min_free_gib": 150}, (512, 150))])
def test_central_dispatch_preserves_budget(tmp_path, monkeypatch, config, expected):
    import crypto_collector.cli as cli
    from crypto_collector.ops import JobSpec
    captured = []
    async def segment(args):
        captured.append((args.session_evidence_max_mib, args.session_evidence_min_free_gib))
        return {"run_path": str(tmp_path / "run"), "clean_events": 0, "replayable": True}
    monkeypatch.setattr(cli, "collect_bybit_depth_segment", segment)
    job = JobSpec(name="test", job_type="bybit-depth-worker", interval_seconds=60,
                  args={**config, "session_evidence": True, "market": "linear", "max_segments": 1,
                        "output_root": str(tmp_path / "data"), "ops_root": str(tmp_path / "ops")})
    cli._execute_ops_job_inprocess(job)
    assert captured == [expected]


@pytest.mark.parametrize("free_delta,admitted", [(0, True), (-1, False)])
def test_floor_plus_remaining_cap_boundary(tmp_path, monkeypatch, free_delta, admitted):
    threshold = 100 * GIB + 512 * MIB
    monkeypatch.setattr(journal, "disk_usage", lambda p: SimpleNamespace(free=threshold + free_delta))
    paths, pipeline, evidence, _ = setup(tmp_path, monkeypatch, [[ACK, book()]], max_bytes=512 * MIB)
    assert asyncio.run(pipeline.run(limit=1)).raw_messages == 1
    if admitted:
        manifest = journal.verify_session_evidence(paths.base)
        assert manifest["resource_budget"]["max_bytes"] == 512 * MIB
        assert manifest["resource_budget"]["disk_check_count"] >= 2
        assert manifest["resource_budget"]["initial_free_bytes"] == threshold
    else:
        assert evidence.error == "insufficient_disk_headroom"
        assert not (paths.base / "session_evidence/manifest.json").exists()


def test_failed_probe_refuses_evidence_not_books(tmp_path, monkeypatch):
    def fail(path):
        raise OSError("private error detail")
    monkeypatch.setattr(journal, "disk_usage", fail)
    paths, pipeline, evidence, _ = setup(tmp_path, monkeypatch, [[ACK, book()]])
    assert asyncio.run(pipeline.run(limit=1)).raw_messages == 1
    assert evidence.error == "disk_headroom_unavailable"
    assert not (paths.base / "session_evidence/manifest.json").exists()


def test_space_loss_rechecked_before_more_journal_writes(tmp_path, monkeypatch):
    calls = []
    def usage(path):
        calls.append(path)
        return SimpleNamespace(free=200 * GIB if len(calls) == 1 else 99 * GIB)
    monkeypatch.setattr(journal, "disk_usage", usage)
    monkeypatch.setattr(journal, "DISK_CHECK_BYTES", 100)
    paths, pipeline, evidence, _ = setup(tmp_path, monkeypatch, [[ACK, book(), book("delta", 2)]])
    assert asyncio.run(pipeline.run(limit=2)).raw_messages == 2
    assert len(calls) >= 2
    assert evidence.error == "insufficient_disk_headroom"
    assert not (paths.base / "session_evidence/manifest.json").exists()


def test_stalled_probe_keeps_books_flowing_and_writer_bounded(tmp_path, monkeypatch):
    entered, release = threading.Event(), threading.Event()
    def usage(path):
        entered.set()
        release.wait(5)
        return SimpleNamespace(free=200 * GIB)
    monkeypatch.setattr(journal, "disk_usage", usage)
    paths, pipeline, evidence, _ = setup(tmp_path, monkeypatch, [[ACK, book()]])
    try:
        assert entered.wait(1)
        started = time.monotonic()
        assert asyncio.run(pipeline.run(limit=1)).raw_messages == 1
        assert time.monotonic() - started < 4
        assert evidence.error == "writer_close_timeout"
        other_root = tmp_path / "other"
        other_root.mkdir()
        other = journal.SessionEvidence(other_root)
        assert other.error == "previous_writer_pending"
    finally:
        release.set()
        evidence._thread.join(3)
    assert not (paths.base / "session_evidence/manifest.json").exists()


def test_512_budget_crosses_old_cap_without_large_allocation(tmp_path):
    # Exercise actual byte-accounting branch at the boundary, not 512 MiB of disk I/O.
    evidence = journal.SessionEvidence(tmp_path, max_bytes=512 * MIB)
    try:
        evidence._total = 64 * MIB
        assert evidence.event("synthetic_capacity_boundary") > 0
        assert evidence.error is None
        evidence._total = 512 * MIB
        assert evidence.event("synthetic_capacity_boundary") == 0
        assert evidence.error == "byte_cap"
    finally:
        evidence.finish(reason="test", sinks_closed=False)


def test_manifest_budget_checked_and_legacy_manifest_readable(tmp_path, monkeypatch):
    paths, pipeline, _, _ = setup(tmp_path, monkeypatch, [[ACK, book()]])
    asyncio.run(pipeline.run(limit=1))
    p = paths.base / "session_evidence/manifest.json"
    manifest = json.loads(p.read_text())
    manifest["resource_budget"]["max_bytes"] = 1
    p.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="budget"):
        journal.verify_session_evidence(paths.base)
    manifest.pop("resource_budget")
    p.write_text(json.dumps(manifest))
    assert journal.verify_session_evidence(paths.base)["session_admitted"]


def test_disabled_pipeline_never_probes_space(tmp_path, monkeypatch):
    def forbidden(path):
        pytest.fail("Disabled evidence must not probe disk")
    monkeypatch.setattr(journal, "disk_usage", forbidden)
    paths, pipeline, evidence, _ = setup(tmp_path, monkeypatch, [[ACK, book()]], enabled=False)
    assert asyncio.run(pipeline.run(limit=1)).raw_messages == 1
    assert evidence is None
    assert not (paths.base / "session_evidence").exists()
