from __future__ import annotations

import asyncio
import json
import threading
import time
from datetime import timedelta
from types import SimpleNamespace

import pytest

from crypto_collector.collectors.generic_ws import GenericWebsocketCollector
from crypto_collector.config import CollectorConfig
from crypto_collector.models import utc_now
from crypto_collector.pipeline import CollectorPipeline
from crypto_collector.quality import QualityGate
from crypto_collector.session_evidence import ENDPOINT, TOPIC, SessionEvidence, verify_session_evidence
from crypto_collector.storage import prepare_run_paths


ACK = {"op": "subscribe", "success": True, "conn_id": "fake-local-socket"}


def book(kind="snapshot", update=1):
    return {"topic": TOPIC, "type": kind, "ts": 1000,
            "data": {"s": "BTCUSDT", "u": update, "seq": update,
                     "b": [["10", "2"]], "a": [["11", "3"]]}}


class Socket:
    def __init__(self, frames):
        self.frames = list(frames)
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        self.closed = True

    async def send(self, data):
        pass

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self.frames:
            raise StopAsyncIteration
        return await self.recv()

    async def recv(self):
        if not self.frames:
            raise TimeoutError("ack timed out")
        frame = self.frames.pop(0)
        if isinstance(frame, BaseException):
            raise frame
        return frame if isinstance(frame, str) else json.dumps(frame)


class EmptyNormalizer:
    def normalize_many(self, raw):
        return []


def setup(tmp_path, monkeypatch, sessions, *, enabled=True, **journal_kwargs):
    import websockets
    sockets = [Socket(frames) for frames in sessions]
    pending = iter(sockets)
    monkeypatch.setattr(websockets, "connect", lambda *a, **kw: next(pending))
    paths = prepare_run_paths(tmp_path, "bybit_perp_depth")
    collector = GenericWebsocketCollector(CollectorConfig(
        source="bybit", product="BTCUSDT", channel="orderbook.50", output_root=tmp_path,
        websocket_url=ENDPOINT, subscription_style="bybit", connect_retries=1,
        retry_backoff_seconds=0, max_backoff_seconds=0))
    if enabled:
        collector.session_evidence = SessionEvidence(paths.base, **journal_kwargs)
    pipeline = CollectorPipeline(collector=collector, normalizer=EmptyNormalizer(),
                                 quality_gate=QualityGate(), run_paths=paths,
                                 normalized_root=None, raw_rotate_bytes=400)
    return paths, pipeline, collector.session_evidence, sockets


def records(paths):
    return [json.loads(s) for s in
            (paths.base / "session_evidence/events.jsonl").read_text().splitlines()]


def test_ack_prebuffer_and_rotated_raw_bindings(tmp_path, monkeypatch):
    paths, pipeline, _, sockets = setup(tmp_path, monkeypatch,
        [[book(), ACK, book("delta", 2), book("snapshot", 3)]])
    result = asyncio.run(pipeline.run(limit=3))
    manifest = verify_session_evidence(paths.base)
    assert manifest["raw_count"] == result.raw_messages == 3
    assert len(manifest["raw_files"]) > 1
    assert not manifest["economic_admission"]
    assert sockets[0].closed
    rows = records(paths)
    assert next(r["payload"] for r in rows if r["kind"] == "ack") == ACK
    written = [r for r in rows if r["kind"] == "raw_written"]
    assert len({r["receipt"]["receive_seq"] for r in written}) == 3
    assert written[0]["receipt"]["receive_seq"] < next(r["seq"] for r in rows if r["kind"] == "ack")
    # Independently rebuild terminal book from legacy raw; snapshots reset state.
    bids, asks = {}, {}
    for info in manifest["raw_files"]:
        for line in (paths.base / info["path"]).read_text().splitlines():
            raw = json.loads(line)
            assert set(raw) == {"source", "received_at", "payload"}
            payload = raw["payload"]
            if payload["type"] == "snapshot":
                bids.clear()
                asks.clear()
            for target, key in [(bids, "b"), (asks, "a")]:
                target.update(payload["data"][key])
    assert bids == {"10": "2"} and asks == {"11": "3"}


def test_disabled_has_no_evidence_files(tmp_path, monkeypatch):
    paths, pipeline, evidence, _ = setup(tmp_path, monkeypatch, [[ACK, book()]], enabled=False)
    asyncio.run(pipeline.run(limit=1))
    assert evidence is None
    assert not (paths.base / "session_evidence").exists()


def test_reconnect_delta_ineligible_until_snapshot(tmp_path, monkeypatch):
    paths, pipeline, _, _ = setup(tmp_path, monkeypatch,
        [[ACK, book()], [ACK, book("delta", 2), book("snapshot", 3), book("delta", 4)]])
    asyncio.run(pipeline.run(limit=4))
    bindings = [r for r in records(paths) if r["kind"] == "raw_written"]
    assert [r["eligible"] for r in bindings] == [True, False, True, True]
    with pytest.raises(ValueError, match="not admitted"):
        verify_session_evidence(paths.base)  # first version conservatively refuses whole run


@pytest.mark.parametrize("frames,error", [
    ([{"op": "subscribe", "success": False}], RuntimeError),
    ([book()], TimeoutError),
    ([ACK, book(), asyncio.CancelledError()], asyncio.CancelledError),
])
def test_failed_shutdown_never_admitted(tmp_path, monkeypatch, frames, error):
    paths, pipeline, _, sockets = setup(tmp_path, monkeypatch, [frames])
    with pytest.raises(error):
        asyncio.run(pipeline.run(limit=2))
    assert sockets[0].closed
    with pytest.raises(ValueError, match="not admitted"):
        verify_session_evidence(paths.base)


def test_malformed_pre_ack_recorded(tmp_path, monkeypatch):
    paths, pipeline, _, _ = setup(tmp_path, monkeypatch, [["not-json", ACK, book()]])
    asyncio.run(pipeline.run(limit=1))
    assert any(r["kind"] == "decode_error" for r in records(paths))
    with pytest.raises(ValueError):
        verify_session_evidence(paths.base)


@pytest.mark.parametrize("caps,reason", [({"max_bytes": 200}, "byte_cap"),
                                        ({"queue_bytes": 1}, "queue_overflow")])
def test_caps_preserve_market_writes(tmp_path, monkeypatch, caps, reason):
    paths, pipeline, evidence, _ = setup(tmp_path, monkeypatch, [[ACK, book(), book("delta", 2)]], **caps)
    result = asyncio.run(pipeline.run(limit=2))
    assert result.raw_messages == 2
    assert evidence.error == reason
    assert not (paths.base / "session_evidence/manifest.json").exists()


def test_disk_failure_preserves_market_writes(tmp_path, monkeypatch):
    def publish_failure(self):
        raise OSError("injected_disk_error")
    monkeypatch.setattr(SessionEvidence, "_publish", publish_failure)
    paths, pipeline, _, _ = setup(tmp_path, monkeypatch, [[ACK, book()]])
    assert asyncio.run(pipeline.run(limit=1)).raw_messages == 1
    assert not (paths.base / "session_evidence/manifest.json").exists()


@pytest.mark.parametrize("target", ["journal", "raw"])
def test_mutated_bytes_refused(tmp_path, monkeypatch, target):
    paths, pipeline, _, _ = setup(tmp_path, monkeypatch, [[ACK, book()]])
    asyncio.run(pipeline.run(limit=1))
    manifest = verify_session_evidence(paths.base)
    info = manifest["journal"] if target == "journal" else manifest["raw_files"][0]
    path = paths.base / info["path"]
    with path.open("ab") as stream:
        stream.write(b"{}\n")
    with pytest.raises(ValueError, match="mismatch"):
        verify_session_evidence(paths.base)


def test_raw_close_error_cannot_publish_success(tmp_path, monkeypatch):
    paths, pipeline, _, _ = setup(tmp_path, monkeypatch, [[ACK, book()]])
    original = pipeline.raw_sink.close
    def fail():
        original()
        raise OSError("injected fsync failure")
    monkeypatch.setattr(pipeline.raw_sink, "close", fail)
    with pytest.raises(OSError):
        asyncio.run(pipeline.run(limit=1))
    with pytest.raises(ValueError):
        verify_session_evidence(paths.base)


def test_clock_jump_refused(tmp_path, monkeypatch):
    paths, pipeline, evidence, _ = setup(tmp_path, monkeypatch, [[ACK, book()]])
    evidence.event("test_clock", clock=(utc_now() - timedelta(seconds=10), 1))
    asyncio.run(pipeline.run(limit=1))
    with pytest.raises(ValueError):
        verify_session_evidence(paths.base)


def test_scope_rejected_before_capture(tmp_path):
    from crypto_collector.cli import collect_bybit_depth_segment
    args = SimpleNamespace(market="spot", session_evidence=True, symbol="BTCUSDT",
                           channel="orderbook.50", output_root=tmp_path)
    with pytest.raises(ValueError, match="scoped"):
        asyncio.run(collect_bybit_depth_segment(args))
    assert not list(tmp_path.iterdir())


def test_job_dispatch_and_segment_carry_opt_in(tmp_path, monkeypatch):
    import crypto_collector.cli as cli
    from crypto_collector.ops import JobSpec
    captured = []
    async def segment(args):
        captured.append(args.session_evidence)
        return {"run_path": str(tmp_path / "run"), "clean_events": 0, "replayable": True}
    monkeypatch.setattr(cli, "collect_bybit_depth_segment", segment)
    job = JobSpec(name="test", job_type="bybit-depth-worker", interval_seconds=60,
                  args={"session_evidence": True, "market": "linear", "max_segments": 1,
                        "output_root": str(tmp_path / "data"), "ops_root": str(tmp_path / "ops")})
    cli._execute_ops_job_inprocess(job)
    assert captured == [True]


def test_nonfinite_extra_field_disables_evidence_not_market(tmp_path, monkeypatch):
    frame = book()
    frame["extra"] = float("nan")
    paths, pipeline, evidence, _ = setup(tmp_path, monkeypatch, [[ACK, frame, book("delta", 2)]])
    assert asyncio.run(pipeline.run(limit=2)).raw_messages == 2
    assert evidence.error == "ValueError"
    assert not (paths.base / "session_evidence/manifest.json").exists()


@pytest.mark.parametrize("mutation", ["empty", "ack", "snapshot", "subscribe", "clock"])
def test_self_consistent_hashes_do_not_replace_structural_checks(tmp_path, monkeypatch, mutation):
    from crypto_collector.session_evidence import file_info
    paths, pipeline, _, _ = setup(tmp_path, monkeypatch, [[ACK, book()]])
    asyncio.run(pipeline.run(limit=1))
    manifest_path = paths.base / "session_evidence/manifest.json"
    manifest = json.loads(manifest_path.read_text())
    journal_path = paths.base / manifest["journal"]["path"]
    rows = records(paths)
    if mutation == "empty":
        manifest["raw_count"] = 0
    elif mutation == "ack":
        next(r for r in rows if r["kind"] == "ack")["receive_seq"] = 999
    elif mutation == "snapshot":
        next(r for r in rows if r["kind"] == "snapshot")["receive_seq"] = 999
    elif mutation == "subscribe":
        next(r for r in rows if r["kind"] == "subscribe_sent")["payload"]["args"] = ["orderbook.50.ETHUSDT"]
    else:
        rows[-1]["monotonic_ns"] += 10**10
    journal_path.write_text("".join(json.dumps(r) + "\n" for r in rows))
    manifest["journal"] = file_info(journal_path, paths.base)
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError):
        verify_session_evidence(paths.base)


def test_slow_finalization_does_not_hold_market_or_accumulate_writers(tmp_path, monkeypatch):
    import crypto_collector.session_evidence as module
    release, entered = threading.Event(), threading.Event()
    original = module.file_info
    def slow(path, root):
        entered.set()
        release.wait(5)
        return original(path, root)
    monkeypatch.setattr(module, "file_info", slow)
    paths, pipeline, evidence, _ = setup(tmp_path, monkeypatch, [[ACK, book()]])
    started = time.monotonic()
    try:
        assert asyncio.run(pipeline.run(limit=1)).raw_messages == 1
        assert entered.is_set()
        assert time.monotonic() - started < 4
        assert evidence.error == "writer_close_timeout"
        (tmp_path / "another").mkdir()
        another = SessionEvidence(tmp_path / "another")
        assert another.error == "previous_writer_pending"
        assert not (paths.base / "session_evidence/manifest.json").exists()
    finally:
        release.set()
        evidence._thread.join(5)
    assert not (paths.base / "session_evidence/manifest.json").exists()


def test_sidecar_included_in_offload_file_manifest(tmp_path, monkeypatch):
    from crypto_collector.offload import _file_manifest
    paths, pipeline, _, _ = setup(tmp_path, monkeypatch, [[ACK, book()]])
    asyncio.run(pipeline.run(limit=1))
    entries = _file_manifest(paths.base)
    assert entries["session_evidence/manifest.json"] > 0
    assert entries["session_evidence/events.jsonl"] > 0


def test_pre_ack_buffer_truncation_is_not_complete_evidence(tmp_path, monkeypatch):
    paths, pipeline, _, _ = setup(tmp_path, monkeypatch, [[book(), book("delta", 2), ACK]])
    assert asyncio.run(pipeline.run(limit=1)).raw_messages == 1
    manifest = json.loads((paths.base / "session_evidence/manifest.json").read_text())
    assert "unpersisted_book_frames" in manifest["issues"]
    with pytest.raises(ValueError):
        verify_session_evidence(paths.base)


def test_rehashed_manifest_cannot_admit_a_missing_raw_snapshot(tmp_path, monkeypatch):
    from crypto_collector.session_evidence import file_info
    paths, pipeline, _, _ = setup(tmp_path, monkeypatch, [[ACK, book(), book("delta", 2)]])
    pipeline.raw_sink.max_bytes = 1024**2
    asyncio.run(pipeline.run(limit=2))
    manifest_path = paths.base / "session_evidence/manifest.json"
    manifest = verify_session_evidence(paths.base)
    raw_path = paths.base / manifest["raw_files"][0]["path"]
    raw_path.write_text(raw_path.read_text().splitlines()[1] + "\n")
    rows = [r for r in records(paths) if not (r["kind"] == "raw_written" and r["ordinal"] == 1)]
    remap = {r["seq"]: i for i, r in enumerate(rows, 1)}
    for row in rows:
        row["seq"] = remap[row["seq"]]
        if "receive_seq" in row:
            row["receive_seq"] = remap[row["receive_seq"]]
        if row["kind"] == "raw_written":
            row["ordinal"] = 1
            row["receipt"]["receive_seq"] = remap[row["receipt"]["receive_seq"]]
    journal = paths.base / manifest["journal"]["path"]
    journal.write_text("".join(json.dumps(r) + "\n" for r in rows))
    manifest.update(raw_count=1, journal=file_info(journal, paths.base),
                    raw_files=[file_info(raw_path, paths.base)])
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="Unpersisted book frames or snapshot anchor"):
        verify_session_evidence(paths.base)
