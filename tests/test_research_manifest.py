from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from crypto_collector.research_manifest import build_manifest, generate_research_manifest


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def test_build_manifest_marks_completed_ready_and_current_building(tmp_path: Path) -> None:
    archive = tmp_path / "archive"
    curated = archive / "curated" / "research" / "market_replayable"
    promotion_index = curated / "_promotion_index.jsonl"
    _write_jsonl(
        promotion_index,
        [
            {
                "run_path": str(archive / "raw" / "market" / "binance_depth" / "20260418_000000"),
                "promoted_at": "2026-04-18T00:02:00+00:00",
                "promoted_rows": 50,
            },
            {
                "run_path": str(archive / "raw" / "market" / "binance_depth" / "20260419_000000"),
                "promoted_at": "2026-04-19T00:02:00+00:00",
                "promoted_rows": 50,
            },
        ],
    )
    for day in ["2026-04-18", "2026-04-19"]:
        parquet = curated / "schema_version=v1" / "source=binance" / f"event_date={day}" / "part-0.parquet"
        parquet.parent.mkdir(parents=True, exist_ok=True)
        parquet.write_bytes(b"ok")
        for dataset in ["market", "trades"]:
            source = "binance"
            path = archive / "normalized" / dataset / "schema_version=v1" / f"source={source}" / f"event_date={day}" / "part-0.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"ok")

    for run_name, replayable in [("20260418_000000", True), ("20260419_000000", True)]:
        _write_json(
            archive / "raw" / "market" / "binance_depth" / run_name / "metrics" / "replay_summary.json",
            {
                "event_count": 50,
                "replayable": replayable,
                "findings": [],
                "gap_count": 0,
                "snapshot_gap_count": 0,
                "crossed_book_count": 0,
            },
        )

    manifest = build_manifest(archive_root=archive, current_date=date(2026, 4, 19))

    by_day = {item["date"]: item for item in manifest["days"]}
    assert by_day["2026-04-18"]["readiness"] == "ready"
    assert by_day["2026-04-19"]["readiness"] == "building"
    assert manifest["summary"]["ready_day_count"] == 1
    assert manifest["summary"]["building_day_count"] == 1


def test_generate_research_manifest_writes_latest_and_snapshot(tmp_path: Path) -> None:
    archive = tmp_path / "archive"
    output = tmp_path / "manifests"

    manifest = generate_research_manifest(
        archive_root=archive,
        output_root=output,
        current_date=date(2026, 4, 19),
    )

    assert Path(manifest["output_paths"]["latest_json"]).exists()
    assert Path(manifest["output_paths"]["latest_markdown"]).exists()
    assert Path(manifest["output_paths"]["snapshot_json"]).exists()
    written = json.loads(Path(manifest["output_paths"]["latest_json"]).read_text(encoding="utf-8"))
    assert "output_paths" in written


def test_manifest_marks_promoted_day_with_bad_raw_runs_ready_with_quarantine(tmp_path: Path) -> None:
    archive = tmp_path / "archive"
    curated = archive / "curated" / "research" / "market_replayable"
    _write_jsonl(
        curated / "_promotion_index.jsonl",
        [
            {
                "run_path": str(archive / "raw" / "market" / "binance_depth" / "20260418_000000"),
                "promoted_at": "2026-04-18T00:02:00+00:00",
                "promoted_rows": 50,
            }
        ],
    )
    parquet = curated / "schema_version=v1" / "source=binance" / "event_date=2026-04-18" / "part-0.parquet"
    parquet.parent.mkdir(parents=True, exist_ok=True)
    parquet.write_bytes(b"ok")
    _write_json(
        archive / "raw" / "market" / "binance_depth" / "20260418_000000" / "metrics" / "replay_summary.json",
        {
            "event_count": 50,
            "replayable": True,
            "findings": [],
            "gap_count": 0,
            "snapshot_gap_count": 0,
            "crossed_book_count": 0,
        },
    )
    _write_json(
        archive / "raw" / "market" / "binance_depth" / "20260418_001000" / "metrics" / "replay_summary.json",
        {
            "event_count": 50,
            "replayable": False,
            "findings": ["snapshot_anchor_gap"],
            "gap_count": 0,
            "snapshot_gap_count": 1,
            "crossed_book_count": 0,
        },
    )

    manifest = build_manifest(archive_root=archive, current_date=date(2026, 4, 19))
    by_day = {item["date"]: item for item in manifest["days"]}

    assert by_day["2026-04-18"]["readiness"] == "ready_with_quarantine"
    assert "raw_depth_has_unreplayable_runs" in by_day["2026-04-18"]["notes"]
    assert manifest["summary"]["ready_with_quarantine_day_count"] == 1


def test_manifest_deduplicates_promotion_index_rows(tmp_path: Path) -> None:
    archive = tmp_path / "archive"
    curated = archive / "curated" / "research" / "market_replayable"
    run_path = archive / "raw" / "market" / "binance_depth" / "20260418_000000"
    _write_jsonl(
        curated / "_promotion_index.jsonl",
        [
            {
                "run_path": str(run_path),
                "promoted_at": "2026-04-18T00:02:00+00:00",
                "promoted_rows": 50,
            },
            {
                "run_path": str(run_path),
                "promoted_at": "2026-04-18T00:03:00+00:00",
                "promoted_rows": 50,
            },
        ],
    )
    _write_json(
        run_path / "metrics" / "replay_summary.json",
        {
            "event_count": 50,
            "replayable": True,
            "findings": [],
            "gap_count": 0,
            "snapshot_gap_count": 0,
            "crossed_book_count": 0,
        },
    )

    manifest = build_manifest(archive_root=archive, current_date=date(2026, 4, 19))
    by_day = {item["date"]: item for item in manifest["days"]}

    assert by_day["2026-04-18"]["curated_market_replayable"]["runs"] == 1
    assert by_day["2026-04-18"]["curated_market_replayable"]["rows"] == 50
    assert by_day["2026-04-18"]["curated_market_replayable"]["latest_promoted_at"] == "2026-04-18T00:03:00+00:00"
    assert manifest["summary"]["raw_promotion_index_entry_count"] == 2
    assert manifest["summary"]["deduped_promoted_run_count"] == 1
    assert manifest["summary"]["duplicate_promotion_entry_count"] == 1
    assert manifest["summary"]["duplicate_promoted_run_count"] == 1
