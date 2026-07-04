"""Cold-tier offload tests.

The contract under test: a raw run dir leaves the hot tier ONLY when it is
accounted for (promoted or quarantined index entry) AND a verified byte-identical
copy exists in the cold tier; everything else is reported, never touched.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from crypto_collector.cli import (
    _execute_ops_job_inprocess,
    _job_args,
    build_parser,
    run_archive_offload,
)
from crypto_collector.config import default_ops_root
from crypto_collector.offload import (
    OFFLOAD_INDEX_FILENAME,
    OFFLOAD_REPORT_FILENAME,
    OffloadLaneSpec,
    offload_accounted_runs,
    write_offload_report_latest,
)
from crypto_collector.ops import COLLECTOR_JOB_TYPES, JobSpec

OLD_RUN = "20200101_000000"  # far older than any min_age_days used here
OLD_RUN_2 = "20200102_000000"
YOUNG_RUN = "20990101_000000"  # far in the future => never older than the cutoff


def _make_run(raw_root: Path, lane: str, name: str, *, payload: str = "x" * 64) -> Path:
    run_dir = raw_root / lane / name
    (run_dir / "clean").mkdir(parents=True)
    (run_dir / "metrics").mkdir(parents=True)
    (run_dir / "clean" / "events.jsonl").write_text(payload, encoding="utf-8")
    (run_dir / "metrics" / "replay_summary.json").write_text(
        json.dumps({"replayable": True, "findings": []}), encoding="utf-8"
    )
    return run_dir


def _write_index(path: Path, run_dirs: list[Path]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for run_dir in run_dirs:
            handle.write(json.dumps({"run_path": str(run_dir)}) + "\n")
    return path


def _lane_spec(tmp_path: Path, lane: str) -> OffloadLaneSpec:
    return OffloadLaneSpec(
        source=lane,
        promotion_index=tmp_path / "curated" / "_promotion_index.jsonl",
        quarantine_index=tmp_path / "quarantine" / lane / "_quarantine_index.jsonl",
    )


def test_dry_run_reports_candidates_and_moves_nothing(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    cold_root = tmp_path / "cold"
    run_dir = _make_run(raw_root, "binance_trades", OLD_RUN)
    spec = _lane_spec(tmp_path, "binance_trades")
    _write_index(spec.promotion_index, [run_dir])

    report = offload_accounted_runs(
        raw_root=raw_root, cold_root=cold_root, lanes=[spec], apply=False
    )

    assert report.status == "ok"
    assert report.mode == "dry-run"
    assert report.eligible_count == 1
    assert report.moved_count == 0
    assert report.runs[0].action == "would_move"
    assert report.runs[0].bytes > 0
    assert run_dir.exists()
    assert not cold_root.exists()


def test_apply_moves_promoted_run_verbatim_and_indexes_it(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    cold_root = tmp_path / "cold"
    run_dir = _make_run(raw_root, "binance_trades", OLD_RUN, payload="hello-events")
    spec = _lane_spec(tmp_path, "binance_trades")
    _write_index(spec.promotion_index, [run_dir])

    report = offload_accounted_runs(
        raw_root=raw_root, cold_root=cold_root, lanes=[spec], apply=True
    )

    assert report.status == "ok"
    assert report.moved_count == 1
    assert report.failed_count == 0
    assert not run_dir.exists()
    cold_run = cold_root / "binance_trades" / OLD_RUN
    assert (cold_run / "clean" / "events.jsonl").read_text(encoding="utf-8") == "hello-events"
    assert (cold_run / "metrics" / "replay_summary.json").exists()
    index_rows = [
        json.loads(line)
        for line in (cold_root / OFFLOAD_INDEX_FILENAME).read_text(encoding="utf-8").splitlines()
    ]
    assert index_rows[0]["run_path"] == str(run_dir)
    assert index_rows[0]["cold_path"] == str(cold_run)
    assert index_rows[0]["file_count"] == 2


def test_quarantined_run_is_eligible(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    cold_root = tmp_path / "cold"
    run_dir = _make_run(raw_root, "bybit_depth", OLD_RUN)
    spec = _lane_spec(tmp_path, "bybit_depth")
    _write_index(spec.quarantine_index, [run_dir])

    report = offload_accounted_runs(
        raw_root=raw_root, cold_root=cold_root, lanes=[spec], apply=True
    )

    assert report.moved_count == 1
    assert not run_dir.exists()
    assert (cold_root / "bybit_depth" / OLD_RUN).exists()


def test_old_unaccounted_run_is_kept_and_flagged(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    cold_root = tmp_path / "cold"
    run_dir = _make_run(raw_root, "okx_trades", OLD_RUN)  # in NO index
    spec = _lane_spec(tmp_path, "okx_trades")

    report = offload_accounted_runs(
        raw_root=raw_root, cold_root=cold_root, lanes=[spec], apply=True
    )

    assert report.status == "warn"
    assert report.moved_count == 0
    assert report.stuck_unaccounted_count == 1
    assert "stuck_unaccounted_runs:1" in report.findings
    assert report.lanes[0].stuck_examples == [str(run_dir)]
    assert run_dir.exists()


def test_young_run_is_ignored_even_when_promoted(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    cold_root = tmp_path / "cold"
    run_dir = _make_run(raw_root, "binance_trades", YOUNG_RUN)
    spec = _lane_spec(tmp_path, "binance_trades")
    _write_index(spec.promotion_index, [run_dir])

    report = offload_accounted_runs(
        raw_root=raw_root, cold_root=cold_root, lanes=[spec], apply=True
    )

    assert report.scanned_run_count == 0
    assert report.eligible_count == 0
    assert "no_offload_candidates" in report.findings
    assert run_dir.exists()


def test_resume_deletes_source_when_cold_copy_already_matches(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    cold_root = tmp_path / "cold"
    run_dir = _make_run(raw_root, "binance_trades", OLD_RUN, payload="same-bytes")
    _make_run(cold_root, "binance_trades", OLD_RUN, payload="same-bytes")
    spec = _lane_spec(tmp_path, "binance_trades")
    _write_index(spec.promotion_index, [run_dir])

    report = offload_accounted_runs(
        raw_root=raw_root, cold_root=cold_root, lanes=[spec], apply=True
    )

    assert report.status == "ok"
    assert report.runs[0].action == "resumed_delete"
    assert not run_dir.exists()
    assert (cold_root / "binance_trades" / OLD_RUN).exists()
    # Regression: the resume path used to delete the hot copy WITHOUT writing an
    # index row — a crash between the rename and the index write on the previous
    # cycle left the run cold-only and permanently unlocatable via the index.
    index_rows = [
        json.loads(line)
        for line in (cold_root / OFFLOAD_INDEX_FILENAME).read_text(encoding="utf-8").splitlines()
    ]
    assert index_rows[-1]["run_path"] == str(run_dir)
    assert index_rows[-1]["resumed"] is True


def test_cold_collision_with_different_content_keeps_source_and_fails(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    cold_root = tmp_path / "cold"
    run_dir = _make_run(raw_root, "binance_trades", OLD_RUN, payload="hot-version")
    _make_run(cold_root, "binance_trades", OLD_RUN, payload="different-cold-version!!")
    spec = _lane_spec(tmp_path, "binance_trades")
    _write_index(spec.promotion_index, [run_dir])

    report = offload_accounted_runs(
        raw_root=raw_root, cold_root=cold_root, lanes=[spec], apply=True
    )

    assert report.status == "error"
    assert report.failed_count == 1
    assert report.runs[0].action == "cold_target_mismatch"
    assert run_dir.exists()  # hot copy must survive
    assert (cold_root / "binance_trades" / OLD_RUN / "clean" / "events.jsonl").read_text(
        encoding="utf-8"
    ) == "different-cold-version!!"


def test_stale_partial_copy_is_replaced_not_trusted(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    cold_root = tmp_path / "cold"
    run_dir = _make_run(raw_root, "binance_trades", OLD_RUN, payload="full-payload")
    # Simulate a crash mid-copy from an earlier cycle: truncated file in the staging dir.
    stale_partial = cold_root / "binance_trades" / ".offload_partial" / OLD_RUN / "clean"
    stale_partial.mkdir(parents=True)
    (stale_partial / "events.jsonl").write_text("trunc", encoding="utf-8")
    spec = _lane_spec(tmp_path, "binance_trades")
    _write_index(spec.promotion_index, [run_dir])

    report = offload_accounted_runs(
        raw_root=raw_root, cold_root=cold_root, lanes=[spec], apply=True
    )

    assert report.status == "ok"
    assert report.moved_count == 1
    cold_run = cold_root / "binance_trades" / OLD_RUN
    assert (cold_run / "clean" / "events.jsonl").read_text(encoding="utf-8") == "full-payload"
    assert not (cold_root / "binance_trades" / ".offload_partial" / OLD_RUN).exists()


def test_unconfigured_lane_on_disk_is_flagged(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    cold_root = tmp_path / "cold"
    _make_run(raw_root, "binance_trades", OLD_RUN)
    _make_run(raw_root, "new_venue_trades", OLD_RUN)  # exists on disk, not configured
    (raw_root / "_cursors").mkdir()  # infra dir, must not be flagged
    spec = _lane_spec(tmp_path, "binance_trades")
    _write_index(spec.promotion_index, [raw_root / "binance_trades" / OLD_RUN])

    report = offload_accounted_runs(
        raw_root=raw_root, cold_root=cold_root, lanes=[spec], apply=False
    )

    assert "unconfigured_lane:new_venue_trades" in report.findings
    assert not any(f == "unconfigured_lane:_cursors" for f in report.findings)
    assert report.status == "warn"


def test_limit_bounds_moves_per_cycle(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    cold_root = tmp_path / "cold"
    run_a = _make_run(raw_root, "binance_trades", OLD_RUN)
    run_b = _make_run(raw_root, "binance_trades", OLD_RUN_2)
    spec = _lane_spec(tmp_path, "binance_trades")
    _write_index(spec.promotion_index, [run_a, run_b])

    report = offload_accounted_runs(
        raw_root=raw_root, cold_root=cold_root, lanes=[spec], apply=True, limit=1
    )

    assert report.moved_count == 1
    # Oldest drains first; the rest waits for the next cycle.
    assert not run_a.exists()
    assert run_b.exists()


def test_age_only_lane_moves_unindexed_old_runs(tmp_path: Path) -> None:
    # kalshi-style lane: curation happens at write time, no promotion index exists.
    raw_root = tmp_path / "raw"
    cold_root = tmp_path / "cold"
    old_run = _make_run(raw_root, "kalshi_crypto_quotes", OLD_RUN)
    young_run = _make_run(raw_root, "kalshi_crypto_quotes", YOUNG_RUN)
    spec = OffloadLaneSpec(source="kalshi_crypto_quotes", gate="age_only")

    report = offload_accounted_runs(
        raw_root=raw_root, cold_root=cold_root, lanes=[spec], apply=True
    )

    assert report.status == "ok"
    assert report.moved_count == 1
    assert report.stuck_unaccounted_count == 0
    assert not old_run.exists()
    assert young_run.exists()
    assert (cold_root / "kalshi_crypto_quotes" / OLD_RUN).exists()


def test_lane_spec_from_dict_validates_gate() -> None:
    with pytest.raises(ValueError, match="gate"):
        OffloadLaneSpec.from_dict({"source": "x", "promotion_index": "p", "gate": "yolo"})
    with pytest.raises(ValueError, match="promotion_index"):
        OffloadLaneSpec.from_dict({"source": "x"})  # indexed (default) needs an index
    spec = OffloadLaneSpec.from_dict({"source": "x", "gate": "age_only"})
    assert spec.promotion_index is None and spec.gate == "age_only"


def test_indexed_lane_without_index_paths_moves_nothing(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    cold_root = tmp_path / "cold"
    run_dir = _make_run(raw_root, "binance_trades", OLD_RUN)
    spec = OffloadLaneSpec(source="binance_trades")  # indexed, but no index paths

    report = offload_accounted_runs(
        raw_root=raw_root, cold_root=cold_root, lanes=[spec], apply=True
    )

    assert report.moved_count == 0
    assert report.stuck_unaccounted_count == 1
    assert run_dir.exists()


def test_cli_parser_accepts_archive_offload() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "archive-offload",
            "--cold-root", r"D:\market_archive_cold\raw\market",
            "--lanes-file", "lanes.json",
            "--min-age-days", "14",
            "--ops-root", r"G:\market_archive\ops",
            "--apply",
        ]
    )
    assert args.command == "archive-offload"
    assert args.apply is True
    assert args.min_age_days == 14.0
    assert args.limit == 200
    assert args.ops_root == Path(r"G:\market_archive\ops")


def test_archive_offload_is_not_a_collector_job_type() -> None:
    # Maintenance jobs run in the runner's scheduler thread, not the collector pool;
    # adding it to COLLECTOR_JOB_TYPES would burn a collector slot per cycle.
    assert "archive-offload" not in COLLECTOR_JOB_TYPES


def test_job_args_archive_offload_passes_every_config_arg() -> None:
    # Regression shape: ops args silently dropped between config and worker have
    # bitten twice (market field, jsonl_fsync). Assert each arg lands.
    lanes = [
        {
            "source": "binance_trades",
            "promotion_index": r"G:\x\_promotion_index.jsonl",
            "quarantine_index": r"G:\y\_quarantine_index.jsonl",
        }
    ]
    args = _job_args(
        JobSpec(
            name="archive-offload-cold",
            job_type="archive-offload",
            interval_seconds=3600,
            args={
                "raw_root": r"G:\market_archive\raw\market",
                "cold_root": r"D:\market_archive_cold\raw\market",
                "lanes": lanes,
                "min_age_days": 21.0,
                "limit": 50,
                "apply": True,
                "ops_root": r"G:\market_archive\ops",
            },
        )
    )
    assert args.raw_root == Path(r"G:\market_archive\raw\market")
    assert args.cold_root == Path(r"D:\market_archive_cold\raw\market")
    assert args.lanes == lanes
    assert args.min_age_days == 21.0
    assert args.limit == 50
    assert args.apply is True
    assert args.ops_root == Path(r"G:\market_archive\ops")


def test_job_args_archive_offload_defaults_ops_root() -> None:
    # An offload job config that predates report persistence carries no ops_root;
    # it must fall back to the same default the collector lanes use, not crash.
    args = _job_args(
        JobSpec(
            name="archive-offload-cold",
            job_type="archive-offload",
            interval_seconds=3600,
            args={"cold_root": r"D:\cold", "lanes": []},
        )
    )
    assert args.ops_root == default_ops_root()


def test_job_args_archive_offload_requires_cold_root_and_lanes() -> None:
    with pytest.raises(ValueError, match="cold_root"):
        _job_args(
            JobSpec(name="bad", job_type="archive-offload", interval_seconds=3600, args={})
        )


def test_run_archive_offload_raises_on_failed_moves(tmp_path: Path) -> None:
    # End-to-end through the CLI command wrapper: a cold-collision failure must
    # surface as a raised error (=> runner job failure), not a quiet log line.
    raw_root = tmp_path / "raw"
    cold_root = tmp_path / "cold"
    ops_root = tmp_path / "ops"
    run_dir = _make_run(raw_root, "binance_trades", OLD_RUN, payload="hot")
    _make_run(cold_root, "binance_trades", OLD_RUN, payload="cold-mismatch")
    spec_dict = {
        "source": "binance_trades",
        "promotion_index": str(tmp_path / "curated" / "_promotion_index.jsonl"),
    }
    _write_index(Path(spec_dict["promotion_index"]), [run_dir])
    lanes_file = tmp_path / "lanes.json"
    lanes_file.write_text(json.dumps([spec_dict]), encoding="utf-8")

    parser = build_parser()
    args = parser.parse_args(
        [
            "archive-offload",
            "--raw-root", str(raw_root),
            "--cold-root", str(cold_root),
            "--lanes-file", str(lanes_file),
            "--ops-root", str(ops_root),
            "--apply",
        ]
    )
    with pytest.raises(RuntimeError, match="failed moves"):
        run_archive_offload(args)
    assert run_dir.exists()
    # The report must be persisted BEFORE the raise — a failing offload is exactly
    # when the health surface needs to see the counts.
    persisted = json.loads((ops_root / OFFLOAD_REPORT_FILENAME).read_text(encoding="utf-8"))
    assert persisted["failed_count"] == 1
    assert persisted["status"] == "error"


def test_resume_finishes_partially_deleted_source(tmp_path: Path) -> None:
    """Regression: a crash mid-rmtree leaves the source a strict subset of the verified
    cold copy. That used to wedge the run in a permanent cold_target_mismatch erroring
    every later offload pass; it must be recognized as a resumable partial delete."""
    raw_root = tmp_path / "raw"
    cold_root = tmp_path / "cold"
    run_dir = _make_run(raw_root, "binance_trades", OLD_RUN, payload="same-bytes")
    _make_run(cold_root, "binance_trades", OLD_RUN, payload="same-bytes")
    # Simulate the interrupted delete: one of the two source files already gone.
    (run_dir / "metrics" / "replay_summary.json").unlink()
    spec = _lane_spec(tmp_path, "binance_trades")
    _write_index(spec.promotion_index, [run_dir])

    report = offload_accounted_runs(
        raw_root=raw_root, cold_root=cold_root, lanes=[spec], apply=True
    )

    assert report.status == "ok"
    assert report.runs[0].action == "resumed_delete"
    assert not run_dir.exists()
    # The cold copy keeps BOTH files.
    assert (cold_root / "binance_trades" / OLD_RUN / "metrics" / "replay_summary.json").exists()


def test_source_with_extra_content_vs_cold_copy_still_fails_safe(tmp_path: Path) -> None:
    """The partial-delete recovery must not weaken the collision guard: a source file
    LARGER than (or absent from) the cold copy is real divergence — hot copy kept."""
    raw_root = tmp_path / "raw"
    cold_root = tmp_path / "cold"
    run_dir = _make_run(raw_root, "binance_trades", OLD_RUN, payload="same-bytes-plus-extra-tail")
    _make_run(cold_root, "binance_trades", OLD_RUN, payload="same-bytes")
    spec = _lane_spec(tmp_path, "binance_trades")
    _write_index(spec.promotion_index, [run_dir])

    report = offload_accounted_runs(
        raw_root=raw_root, cold_root=cold_root, lanes=[spec], apply=True
    )

    assert report.status == "error"
    assert report.runs[0].action == "cold_target_mismatch"
    assert run_dir.exists()


def test_source_grown_between_copy_and_delete_is_kept(tmp_path: Path, monkeypatch) -> None:
    """Defense-in-depth: bytes appended to the source AFTER the verified copy (zombie
    writer) must not be silently lost by the rmtree — the source is kept and the run
    reports source_changed_during_move."""
    import crypto_collector.offload as offload_mod

    raw_root = tmp_path / "raw"
    cold_root = tmp_path / "cold"
    run_dir = _make_run(raw_root, "binance_trades", OLD_RUN, payload="original")
    spec = _lane_spec(tmp_path, "binance_trades")
    _write_index(spec.promotion_index, [run_dir])

    real_manifest = offload_mod._file_manifest
    calls = {"count": 0}

    def growing_manifest(root: Path):
        manifest = real_manifest(root)
        calls["count"] += 1
        # Grow the SOURCE just before its pre-delete re-verify (the last manifest
        # call against run_dir in the fresh-move path).
        if root == run_dir and calls["count"] >= 3:
            (run_dir / "clean" / "late-append.jsonl").write_text("tail", encoding="utf-8")
            return real_manifest(root)
        return manifest

    monkeypatch.setattr(offload_mod, "_file_manifest", growing_manifest)

    report = offload_accounted_runs(
        raw_root=raw_root, cold_root=cold_root, lanes=[spec], apply=True
    )

    assert report.runs[0].action == "source_changed_during_move"
    assert run_dir.exists()


def test_per_lane_min_age_overrides_job_default(tmp_path: Path) -> None:
    """A lane-level min_age_days beats the job default for that lane only: a
    7-day-old run is offload-eligible on the 3-day Kalshi lane while the same
    age stays hot on a default-14-day lane."""
    from datetime import UTC, datetime, timedelta

    raw_root = tmp_path / "raw"
    cold_root = tmp_path / "cold"
    mid_aged = (datetime.now(tz=UTC) - timedelta(days=7)).strftime("%Y%m%d_%H%M%S")
    _make_run(raw_root, "kalshi_crypto_quotes", mid_aged)
    _make_run(raw_root, "coinbase_trades_usdc", mid_aged)

    report = offload_accounted_runs(
        raw_root=raw_root,
        cold_root=cold_root,
        lanes=[
            OffloadLaneSpec(
                source="kalshi_crypto_quotes", gate="age_only", min_age_days=3.0
            ),
            OffloadLaneSpec(source="coinbase_trades_usdc", gate="age_only"),
        ],
        min_age_days=14.0,
        apply=False,
    )

    assert [run.lane for run in report.runs] == ["kalshi_crypto_quotes"]
    assert report.runs[0].action == "would_move"
    by_source = {lane.source: lane for lane in report.lanes}
    assert by_source["kalshi_crypto_quotes"].min_age_days == 3.0
    assert by_source["kalshi_crypto_quotes"].eligible_count == 1
    assert by_source["coinbase_trades_usdc"].min_age_days == 14.0
    assert by_source["coinbase_trades_usdc"].eligible_count == 0


def test_lane_spec_parses_min_age_override() -> None:
    spec = OffloadLaneSpec.from_dict(
        {"source": "kalshi_crypto_quotes", "gate": "age_only", "min_age_days": 3}
    )
    assert spec.min_age_days == 3.0
    assert OffloadLaneSpec.from_dict(
        {"source": "x", "gate": "age_only"}
    ).min_age_days is None


@pytest.mark.parametrize("bad_age", [0, -1, "soon", True, float("nan"), float("inf")])
def test_lane_spec_rejects_non_positive_min_age(bad_age) -> None:
    with pytest.raises(ValueError, match="min_age_days"):
        OffloadLaneSpec.from_dict(
            {"source": "x", "gate": "age_only", "min_age_days": bad_age}
        )


def test_write_offload_report_latest_atomic_and_overwrites(tmp_path: Path) -> None:
    """The persisted report follows the ops-root temp+rename convention: readers
    never see a torn file and no *.tmp residue is left behind, including when a
    previous report is being replaced."""
    raw_root = tmp_path / "raw"
    ops_root = tmp_path / "ops"
    _make_run(raw_root, "okx_trades", OLD_RUN)  # stuck (no index)
    spec = _lane_spec(tmp_path, "okx_trades")

    first = offload_accounted_runs(
        raw_root=raw_root, cold_root=tmp_path / "cold", lanes=[spec], apply=False
    )
    path = write_offload_report_latest(first, ops_root)
    assert path == ops_root / OFFLOAD_REPORT_FILENAME
    assert json.loads(path.read_text(encoding="utf-8"))["stuck_unaccounted_count"] == 1

    second = offload_accounted_runs(
        raw_root=raw_root, cold_root=tmp_path / "cold", lanes=[spec], apply=False
    )
    write_offload_report_latest(second, ops_root)
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["checked_at"] == second.checked_at
    assert list(ops_root.glob("*.tmp")) == []


def test_run_archive_offload_persists_report_on_cli_path(tmp_path: Path, capsys) -> None:
    # Manual `archive-offload` CLI invocations must persist the same latest-report
    # file the runner job path does, so health never reads a stale runner report
    # after a manual pass.
    raw_root = tmp_path / "raw"
    ops_root = tmp_path / "ops"
    run_dir = _make_run(raw_root, "binance_trades", OLD_RUN)
    spec_dict = {
        "source": "binance_trades",
        "promotion_index": str(tmp_path / "curated" / "_promotion_index.jsonl"),
    }
    _write_index(Path(spec_dict["promotion_index"]), [run_dir])
    lanes_file = tmp_path / "lanes.json"
    lanes_file.write_text(json.dumps([spec_dict]), encoding="utf-8")

    parser = build_parser()
    args = parser.parse_args(
        [
            "archive-offload",
            "--raw-root", str(raw_root),
            "--cold-root", str(tmp_path / "cold"),
            "--lanes-file", str(lanes_file),
            "--ops-root", str(ops_root),
        ]
    )
    report = run_archive_offload(args)
    capsys.readouterr()

    payload = json.loads(
        (ops_root / OFFLOAD_REPORT_FILENAME).read_text(encoding="utf-8")
    )
    assert payload["mode"] == "dry-run"
    assert payload["eligible_count"] == 1
    assert payload["stuck_unaccounted_count"] == 0
    assert payload["checked_at"] == report.checked_at
    assert list(ops_root.glob("*.tmp")) == []


def test_ops_job_archive_offload_persists_report_and_reports_counts(tmp_path: Path, capsys) -> None:
    # The runner job path: the job_runs.jsonl message must carry the headline
    # counts ("completed" alone hid a growing stuck cohort for a week), and the
    # report must land in the job's ops_root for health to read.
    raw_root = tmp_path / "raw"
    ops_root = tmp_path / "ops"
    _make_run(raw_root, "okx_trades", OLD_RUN)  # in NO index => stuck
    job = JobSpec(
        name="archive-offload-cold",
        job_type="archive-offload",
        interval_seconds=3600,
        args={
            "raw_root": str(raw_root),
            "cold_root": str(tmp_path / "cold"),
            "lanes": [
                {
                    "source": "okx_trades",
                    "promotion_index": str(tmp_path / "curated" / "_promotion_index.jsonl"),
                }
            ],
            "apply": True,
            "ops_root": str(ops_root),
        },
    )

    message = _execute_ops_job_inprocess(job)
    capsys.readouterr()

    assert message == (
        "archive offload completed; status=warn moved=0 failed=0 stuck_unaccounted=1"
    )
    payload = json.loads(
        (ops_root / OFFLOAD_REPORT_FILENAME).read_text(encoding="utf-8")
    )
    assert payload["stuck_unaccounted_count"] == 1
    assert "stuck_unaccounted_runs:1" in payload["findings"]
