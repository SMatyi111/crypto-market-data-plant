from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from crypto_collector.config import STANDARDS_VERSION
from crypto_collector.research_manifest import (
    build_manifest,
    generate_research_manifest,
    parse_lane,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def _write_replay_summary(
    archive: Path, lane: str, run_name: str, *, replayable: bool = True, **extra
) -> None:
    payload = {"event_count": 50, "replayable": replayable, "findings": []}
    payload.update(extra)
    _write_json(
        archive / "raw" / "market" / lane / run_name / "metrics" / "replay_summary.json",
        payload,
    )


def _promote(archive: Path, curated_dataset: str, lane: str, run_name: str, rows: int = 50) -> None:
    index = archive / "curated" / "research" / curated_dataset / "_promotion_index.jsonl"
    existing = index.read_text(encoding="utf-8") if index.exists() else ""
    index.parent.mkdir(parents=True, exist_ok=True)
    row = {
        "run_path": str(archive / "raw" / "market" / lane / run_name),
        "promoted_at": f"{run_name[:4]}-{run_name[4:6]}-{run_name[6:8]}T00:02:00+00:00",
        "promoted_rows": rows,
    }
    index.write_text(existing + json.dumps(row) + "\n", encoding="utf-8")


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


def test_parse_lane_splits_venue_dataset_instrument() -> None:
    assert parse_lane("binance_depth") == ("binance", "depth", None)
    assert parse_lane("coinbase_trades") == ("coinbase", "trades", None)
    assert parse_lane("binance_trades_ethusdt") == ("binance", "trades", "ethusdt")
    assert parse_lane("binance_depth_eth_usdt") == ("binance", "depth", "eth_usdt")
    # Unknown dataset token or malformed names are skipped, not guessed.
    assert parse_lane("binance_orderflow") is None
    assert parse_lane("binance") is None


def test_manifest_tags_standards_version() -> None:
    from datetime import date as _date

    manifest = build_manifest(archive_root=Path("/does/not/exist"), current_date=_date(2026, 4, 19))
    assert manifest["standards_version"] == STANDARDS_VERSION
    assert manifest["lanes"] == []
    assert manifest["lanes_summary"]["lane_count"] == 0


def test_manifest_lanes_are_venue_instrument_dataset_aware(tmp_path: Path) -> None:
    archive = tmp_path / "archive"
    # depth + trades for Binance BTC, Coinbase BTC trades, and a separate
    # Binance ETH trades lane — all promoted & replayable on the same day. The
    # summaries declare their gap class explicitly, as live replay summaries do;
    # an undeclared class reports "unknown", never a provable default.
    _write_replay_summary(archive, "binance_depth", "20260418_000000", gap_detection="sequence")
    _promote(archive, "market_replayable", "binance_depth", "20260418_000000")
    _write_replay_summary(archive, "binance_trades", "20260418_000000", gap_detection="sequence")
    _promote(archive, "trades_replayable", "binance_trades", "20260418_000000")
    _write_replay_summary(archive, "coinbase_trades", "20260418_000000", gap_detection="sequence")
    _promote(archive, "trades_replayable", "coinbase_trades", "20260418_000000")
    _write_replay_summary(
        archive,
        "binance_trades_ethusdt",
        "20260418_000000",
        promoted_rows=70,
        gap_detection="sequence",
    )
    _promote(archive, "trades_replayable", "binance_trades_ethusdt", "20260418_000000", rows=70)

    manifest = build_manifest(archive_root=archive, current_date=date(2026, 4, 19))
    lanes = {lane["lane"]: lane for lane in manifest["lanes"]}

    assert set(lanes) == {
        "binance_depth",
        "binance_trades",
        "coinbase_trades",
        "binance_trades_ethusdt",
    }
    # The ETH lane stays separated from the BTC lane even though both trades
    # lanes share one trades_replayable promotion index.
    eth = lanes["binance_trades_ethusdt"]
    assert eth["venue"] == "binance"
    assert eth["instrument"] == "ethusdt"
    assert eth["dataset"] == "trades"
    assert eth["curated_dataset"] == "trades_replayable"
    assert eth["total_curated_rows"] == 70
    assert eth["days"][0]["readiness"] == "ready"
    assert lanes["binance_trades"]["total_curated_rows"] == 50
    assert lanes["coinbase_trades"]["venue"] == "coinbase"
    # All sequence-bearing today.
    assert all(lane["gap_detection"] == "sequence" for lane in lanes.values())
    assert manifest["lanes_summary"]["lane_count"] == 4
    assert manifest["lanes_summary"]["ready_lane_days"] == 4
    assert sorted(manifest["lanes_summary"]["venues"]) == ["binance", "coinbase"]


def test_manifest_lane_marks_current_day_building_and_quarantine(tmp_path: Path) -> None:
    archive = tmp_path / "archive"
    # A ready day, a day with a bad raw run alongside a promoted run, and the
    # current building day.
    _write_replay_summary(archive, "binance_depth", "20260418_000000")
    _promote(archive, "market_replayable", "binance_depth", "20260418_000000")
    _write_replay_summary(archive, "binance_depth", "20260419_000000")
    _promote(archive, "market_replayable", "binance_depth", "20260419_000000")
    _write_replay_summary(
        archive, "binance_depth", "20260419_001000", replayable=False, findings=["snapshot_anchor_gap"]
    )
    _write_replay_summary(archive, "binance_depth", "20260420_000000")
    _promote(archive, "market_replayable", "binance_depth", "20260420_000000")

    manifest = build_manifest(archive_root=archive, current_date=date(2026, 4, 20))
    depth = next(lane for lane in manifest["lanes"] if lane["lane"] == "binance_depth")
    by_day = {day["date"]: day for day in depth["days"]}

    assert by_day["2026-04-18"]["readiness"] == "ready"
    assert by_day["2026-04-19"]["readiness"] == "ready_with_quarantine"
    assert "raw_has_unreplayable_runs" in by_day["2026-04-19"]["notes"]
    assert by_day["2026-04-20"]["readiness"] == "building"
    assert depth["readiness_counts"] == {
        "ready": 1,
        "ready_with_quarantine": 1,
        "building": 1,
        "missing": 0,
    }
    assert depth["latest_ready_date"] == "2026-04-19"


def test_manifest_lane_flags_none_native_gap_detection(tmp_path: Path) -> None:
    archive = tmp_path / "archive"
    _write_replay_summary(
        archive, "coinbase_depth", "20260418_000000", gap_detection="none_native"
    )
    _promote(archive, "market_replayable", "coinbase_depth", "20260418_000000")

    manifest = build_manifest(archive_root=archive, current_date=date(2026, 4, 19))
    coinbase_depth = next(lane for lane in manifest["lanes"] if lane["lane"] == "coinbase_depth")

    assert coinbase_depth["gap_detection"] == "none_native"
    assert manifest["lanes_summary"]["none_native_lane_count"] == 1


# The full live lane inventory: every one of these MUST be discoverable, or the
# manifest silently under-reports coverage to research consumers. The perp lanes
# (and funding with them) were invisible until parse_lane learned the `_perp`
# marker — 7 of ~21 live lanes missing from the canonical readiness view.
_LIVE_LANE_INVENTORY = {
    "binance_depth": ("binance", "depth", None),
    "binance_trades": ("binance", "trades", None),
    "binance_depth_btcusdc": ("binance", "depth", "btcusdc"),
    "binance_trades_btcusdc": ("binance", "trades", "btcusdc"),
    "coinbase_depth": ("coinbase", "depth", None),
    "coinbase_trades": ("coinbase", "trades", None),
    "kraken_depth": ("kraken", "depth", None),
    "kraken_trades": ("kraken", "trades", None),
    "bybit_depth": ("bybit", "depth", None),
    "bybit_trades": ("bybit", "trades", None),
    "bybit_perp_depth": ("bybit_perp", "depth", None),
    "bybit_perp_trades": ("bybit_perp", "trades", None),
    "mexc_depth": ("mexc", "depth", None),
    "mexc_trades": ("mexc", "trades", None),
    "okx_depth": ("okx", "depth", None),
    "okx_trades": ("okx", "trades", None),
    "okx_perp_depth": ("okx_perp", "depth", None),
    "okx_perp_trades": ("okx_perp", "trades", None),
    "binance_perp_depth": ("binance_perp", "depth", None),
    "binance_perp_trades": ("binance_perp", "trades", None),
    "binance_perp_funding": ("binance_perp", "funding", None),
}


def test_parse_lane_covers_the_full_live_lane_inventory() -> None:
    from crypto_collector.research_manifest import parse_lane

    for dirname, expected in _LIVE_LANE_INVENTORY.items():
        assert parse_lane(dirname) == expected, dirname
    # Non-lanes are still rejected.
    assert parse_lane("_cursors") is None
    assert parse_lane("binance") is None
    assert parse_lane("binance_perp") is None  # marker with no dataset
    assert parse_lane("kalshi_crypto_quotes") is None  # outside the contract


def test_manifest_includes_perp_and_funding_lanes(tmp_path: Path) -> None:
    archive = tmp_path / "archive"
    _write_replay_summary(archive, "bybit_perp_depth", "20260418_000000", gap_detection="sequence")
    _promote(archive, "market_replayable", "bybit_perp_depth", "20260418_000000")
    _write_replay_summary(
        archive, "binance_perp_funding", "20260418_000000", gap_detection="none_native"
    )
    _promote(archive, "funding", "binance_perp_funding", "20260418_000000")

    manifest = build_manifest(archive_root=archive, current_date=date(2026, 4, 19))
    lanes = {lane["lane"]: lane for lane in manifest["lanes"]}

    perp_depth = lanes["bybit_perp_depth"]
    assert perp_depth["venue"] == "bybit_perp"
    assert perp_depth["dataset"] == "depth"
    assert perp_depth["curated_dataset"] == "market_replayable"
    assert perp_depth["gap_detection"] == "sequence"
    assert perp_depth["total_curated_rows"] == 50
    assert perp_depth["days"][0]["readiness"] == "ready"

    funding = lanes["binance_perp_funding"]
    assert funding["venue"] == "binance_perp"
    assert funding["dataset"] == "funding"
    assert funding["curated_dataset"] == "funding"
    assert funding["total_curated_rows"] == 50
    assert funding["days"][0]["readiness"] == "ready"


def test_manifest_lane_gap_detection_latches_worst_class_and_defaults_unknown(
    tmp_path: Path,
) -> None:
    archive = tmp_path / "archive"
    # Kraken depth declares checksum — must surface as checksum, not "sequence"
    # (the old latch could only downgrade on a literal none_native).
    _write_replay_summary(archive, "kraken_depth", "20260418_000000", gap_detection="checksum")
    _promote(archive, "market_replayable", "kraken_depth", "20260418_000000")
    # A lane that produced checksum once and none_native once reports the worst.
    _write_replay_summary(archive, "kraken_depth", "20260419_000000", gap_detection="none_native")
    _promote(archive, "market_replayable", "kraken_depth", "20260419_000000")
    # A lane whose summaries never declared a class reports unknown, not provable.
    _write_replay_summary(archive, "mexc_depth", "20260418_000000")
    _promote(archive, "market_replayable", "mexc_depth", "20260418_000000")

    manifest = build_manifest(archive_root=archive, current_date=date(2026, 4, 20))
    lanes = {lane["lane"]: lane for lane in manifest["lanes"]}

    assert lanes["kraken_depth"]["gap_detection"] == "none_native"
    assert lanes["mexc_depth"]["gap_detection"] == "unknown"


def test_manifest_tolerates_torn_index_lines_and_garbage_run_names(tmp_path: Path) -> None:
    archive = tmp_path / "archive"
    _write_replay_summary(archive, "binance_depth", "20260418_000000", gap_detection="sequence")
    _promote(archive, "market_replayable", "binance_depth", "20260418_000000")
    index = archive / "curated" / "research" / "market_replayable" / "_promotion_index.jsonl"
    # A torn final line (concurrent append / crash) and a garbage 8-digit run name
    # used to crash the whole manifest build, freezing "latest" forever.
    index.write_text(
        index.read_text(encoding="utf-8") + '{"run_path": "' ,
        encoding="utf-8",
    )
    bogus_run = archive / "raw" / "market" / "binance_depth" / "20269999_000000"
    (bogus_run / "metrics").mkdir(parents=True)

    manifest = build_manifest(archive_root=archive, current_date=date(2026, 4, 19))

    assert manifest["summary"]["corrupt_index_line_count"] == 1
    depth = next(lane for lane in manifest["lanes"] if lane["lane"] == "binance_depth")
    assert depth["days"][0]["readiness"] == "ready"


def test_manifest_torn_replay_summary_counts_as_missing(tmp_path: Path) -> None:
    archive = tmp_path / "archive"
    _write_replay_summary(archive, "binance_depth", "20260418_000000", gap_detection="sequence")
    _promote(archive, "market_replayable", "binance_depth", "20260418_000000")
    torn = (
        archive / "raw" / "market" / "binance_depth" / "20260418_001000" / "metrics"
        / "replay_summary.json"
    )
    torn.parent.mkdir(parents=True)
    torn.write_text('{"replayable": tr', encoding="utf-8")

    manifest = build_manifest(archive_root=archive, current_date=date(2026, 4, 19))
    depth = next(lane for lane in manifest["lanes"] if lane["lane"] == "binance_depth")

    assert depth["days"][0]["raw_quality"]["missing_summaries"] == 1
    assert depth["days"][0]["readiness"] == "ready_with_quarantine"
