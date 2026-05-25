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
