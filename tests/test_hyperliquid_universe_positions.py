from __future__ import annotations

import hashlib
import json
import shutil
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

import crypto_collector.cli as cli
from crypto_collector.cli import _job_args
from crypto_collector.collectors.hyperliquid_universe_positions import (
    LEDGER_NAME,
    SOURCE_NAME,
    ingest_universe_positions,
    sweep_run_id,
)
from crypto_collector.ops import COLLECTOR_JOB_TYPES, POLL_LANE_JOB_TYPES


NOW = datetime(2026, 9, 19, 18, 30, 0, tzinfo=UTC)


def _write_sweep(source_root: Path, name: str, payload: bytes) -> Path:
    day = source_root / f"date={name[6:14]}"
    day.mkdir(parents=True, exist_ok=True)
    path = day / name
    path.write_bytes(payload)
    return path


def _manifest_line(name: str, payload: bytes | None, rows: int = 3) -> str:
    record = {
        "file": f"date={name[6:14]}\\{name}",
        "rows": rows,
        "started": "2026-09-19T17:02:51+00:00",
        "ended": "2026-09-19T17:57:57+00:00",
    }
    if payload is not None:
        record["sha256"] = hashlib.sha256(payload).hexdigest()
    return json.dumps(record)


def _ingest(tmp_path: Path, **kw):
    kw.setdefault("source_root", tmp_path / "snapshots")
    kw.setdefault("manifest_path", tmp_path / "snapshot_manifest.jsonl")
    kw.setdefault("clock", lambda: NOW)
    return ingest_universe_positions(tmp_path / "raw", **kw)


def test_sweep_run_id_parses_sweep_names_only() -> None:
    assert sweep_run_id("sweep=20260919T1702Z.parquet") == "20260919_170200"
    assert sweep_run_id("sweep=20260919T1702Z.parquet.tmp") is None
    assert sweep_run_id("marks.jsonl") is None


def test_ingest_archives_listed_sweep_verbatim_and_is_idempotent(tmp_path: Path) -> None:
    payload = b"PAR1 fake parquet bytes"
    name = "sweep=20260919T1702Z.parquet"
    _write_sweep(tmp_path / "snapshots", name, payload)
    (tmp_path / "snapshot_manifest.jsonl").write_text(_manifest_line(name, payload) + "\n", encoding="utf-8")

    result = _ingest(tmp_path)
    assert result.archived == ["20260919_170200"]
    assert result.stale is False
    run_dir = tmp_path / "raw" / SOURCE_NAME / "20260919_170200"
    assert (run_dir / "raw" / name).read_bytes() == payload
    assert not list((run_dir / "raw").glob("*.tmp"))
    summary = json.loads((run_dir / "metrics" / "summary.json").read_text(encoding="utf-8"))
    assert summary["sha256"] == hashlib.sha256(payload).hexdigest()
    assert summary["manifest_sha256_match"] is True
    assert summary["ingested_at"] == NOW.isoformat()
    assert summary["manifest_rows"] == 3
    ledger = (tmp_path / "raw" / SOURCE_NAME / LEDGER_NAME).read_text(encoding="utf-8").splitlines()
    assert len(ledger) == 1 and json.loads(ledger[0])["run_id"] == "20260919_170200"

    again = _ingest(tmp_path)
    assert again.archived == []
    assert again.skipped_existing == 1
    assert len((tmp_path / "raw" / SOURCE_NAME / LEDGER_NAME).read_text(encoding="utf-8").splitlines()) == 1


def test_offloaded_run_is_not_re_ingested(tmp_path: Path) -> None:
    """archive-offload removes the hot-tier run dir after a few days; the source
    never rotates, so 'archived' must be the ledger, not hot-tier presence."""
    payload = b"bytes"
    name = "sweep=20260919T1702Z.parquet"
    _write_sweep(tmp_path / "snapshots", name, payload)
    (tmp_path / "snapshot_manifest.jsonl").write_text(_manifest_line(name, payload) + "\n", encoding="utf-8")
    assert _ingest(tmp_path).archived == ["20260919_170200"]

    shutil.rmtree(tmp_path / "raw" / SOURCE_NAME / "20260919_170200")  # what offload does
    again = _ingest(tmp_path)
    assert again.archived == []
    assert again.skipped_existing == 1
    assert not (tmp_path / "raw" / SOURCE_NAME / "20260919_170200").exists()


def test_unlisted_or_hashless_sweep_is_left_pending_not_archived(tmp_path: Path) -> None:
    done = b"finished"
    _write_sweep(tmp_path / "snapshots", "sweep=20260919T1500Z.parquet", b"listed without sha256")
    _write_sweep(tmp_path / "snapshots", "sweep=20260919T1600Z.parquet", done)  # 2.5 h old: fresh
    _write_sweep(tmp_path / "snapshots", "sweep=20260919T1702Z.parquet", b"still being written")
    (tmp_path / "snapshot_manifest.jsonl").write_text(
        _manifest_line("sweep=20260919T1500Z.parquet", None) + "\n"
        + _manifest_line("sweep=20260919T1600Z.parquet", done) + "\n",
        encoding="utf-8",
    )

    result = _ingest(tmp_path)
    assert result.archived == ["20260919_160000"]
    assert result.pending_unlisted == [
        "sweep=20260919T1500Z.parquet",
        "sweep=20260919T1702Z.parquet",
    ]
    lane = tmp_path / "raw" / SOURCE_NAME
    assert not (lane / "20260919_150000").exists()
    assert not (lane / "20260919_170200").exists()


def test_sha_mismatch_fails_job_and_archives_nothing_for_that_sweep(tmp_path: Path) -> None:
    name = "sweep=20260919T1702Z.parquet"
    _write_sweep(tmp_path / "snapshots", name, b"bytes on disk")
    (tmp_path / "snapshot_manifest.jsonl").write_text(_manifest_line(name, b"different bytes") + "\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="do not hash"):
        _ingest(tmp_path)
    assert not (tmp_path / "raw" / SOURCE_NAME / "20260919_170200").exists()
    assert not (tmp_path / "raw" / SOURCE_NAME / LEDGER_NAME).exists()


def test_manifest_hash_changed_after_archiving_is_a_failure_not_a_rewrite(tmp_path: Path) -> None:
    payload = b"v1"
    name = "sweep=20260919T1702Z.parquet"
    _write_sweep(tmp_path / "snapshots", name, payload)
    manifest = tmp_path / "snapshot_manifest.jsonl"
    manifest.write_text(_manifest_line(name, payload) + "\n", encoding="utf-8")
    assert _ingest(tmp_path).archived == ["20260919_170200"]

    _write_sweep(tmp_path / "snapshots", name, b"v2")
    manifest.write_text(manifest.read_text(encoding="utf-8") + _manifest_line(name, b"v2") + "\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="changed since archiving"):
        _ingest(tmp_path)
    archived = tmp_path / "raw" / SOURCE_NAME / "20260919_170200" / "raw" / name
    assert archived.read_bytes() == payload  # the original archive is untouched


def test_stale_newest_sweep_fails_job_after_archiving(tmp_path: Path) -> None:
    payload = b"old"
    name = "sweep=20260919T1000Z.parquet"  # 8.5 h before NOW
    _write_sweep(tmp_path / "snapshots", name, payload)
    (tmp_path / "snapshot_manifest.jsonl").write_text(_manifest_line(name, payload) + "\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="hl-ladder-sweep alive"):
        _ingest(tmp_path, stale_after_seconds=3 * 3600)
    # The evidence was still archived before the job failed.
    assert (tmp_path / "raw" / SOURCE_NAME / "20260919_100000" / "raw" / name).exists()


def test_no_sweeps_at_all_is_a_failure(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="absent"):
        _ingest(tmp_path, source_root=tmp_path / "missing", manifest_path=tmp_path / "missing.jsonl")


def test_ops_job_registration_and_arg_threading(tmp_path: Path, monkeypatch) -> None:
    assert "hyperliquid-universe-positions-snapshot" in COLLECTOR_JOB_TYPES
    assert "hyperliquid-universe-positions-snapshot" in POLL_LANE_JOB_TYPES
    args = _job_args(
        SimpleNamespace(
            job_type="hyperliquid-universe-positions-snapshot",
            args={
                "output_root": str(tmp_path / "raw"),
                "source_root": str(tmp_path / "snap"),
                "manifest_path": str(tmp_path / "m.jsonl"),
                "stale_after_seconds": 7200,
                "format": "json",
            },
        )
    )
    assert args.output_root == tmp_path / "raw"
    assert args.source_root == tmp_path / "snap"
    assert args.manifest_path == tmp_path / "m.jsonl"
    assert args.stale_after_seconds == 7200.0
    assert args.format == "json"

    # A null manifest_path in config is passed through as None (the collector
    # then treats every sweep as unfinished, which is the safe reading).
    no_manifest = _job_args(
        SimpleNamespace(
            job_type="hyperliquid-universe-positions-snapshot",
            args={"output_root": str(tmp_path / "raw"), "manifest_path": None},
        )
    )
    assert no_manifest.manifest_path is None

    captured = {}

    def fake(output_root, *, source_root, manifest_path, stale_after_seconds):
        captured.update(
            output_root=output_root,
            source_root=source_root,
            manifest_path=manifest_path,
            stale_after_seconds=stale_after_seconds,
        )
        return SimpleNamespace(
            archived=["20260919_170200"],
            skipped_existing=0,
            pending_unlisted=[],
            newest_sweep_id="20260919_170200",
            newest_sweep_age_seconds=60.0,
            stale=False,
        )

    monkeypatch.setattr(cli, "ingest_universe_positions", fake)
    cli.run_hyperliquid_universe_positions_snapshot(args)
    assert captured["source_root"] == tmp_path / "snap"
    assert captured["stale_after_seconds"] == 7200.0
