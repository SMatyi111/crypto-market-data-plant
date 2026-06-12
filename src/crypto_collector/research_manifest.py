from __future__ import annotations

from collections import Counter, defaultdict
from datetime import UTC, date, datetime
import json
from pathlib import Path
from typing import Any

from .config import STANDARDS_VERSION, default_archive_root
from .storage import write_text_atomic


DEFAULT_MANIFEST_ROOT = default_archive_root() / "curated" / "research" / "manifests"

# How each dataset maps onto the on-disk layout (STANDARDS.md §1/§2). The raw
# lane directory name is `<venue>[_perp]_<dataset>[_<instrument>]`; depth promotes
# into market_replayable, trades into trades_replayable, funding into funding.
# (Kalshi quote lanes are deliberately absent: kalshi sits outside the STANDARDS
# market-data contract and has its own summaries.)
DATASET_CONFIG: dict[str, dict[str, str]] = {
    "depth": {"curated_dataset": "market_replayable", "normalized_dataset": "market"},
    "trades": {"curated_dataset": "trades_replayable", "normalized_dataset": "trades"},
    "funding": {"curated_dataset": "funding", "normalized_dataset": "funding"},
}

# Worst-first precedence for a lane's gap-detection class: a lane that has EVER
# produced a non-provable day must not present itself as provable.
_GAP_DETECTION_PRECEDENCE = ("none_native", "checksum", "sequence")


def generate_research_manifest(
    *,
    archive_root: Path | None = None,
    output_root: Path = DEFAULT_MANIFEST_ROOT,
    current_date: date | None = None,
) -> dict[str, Any]:
    manifest = build_manifest(
        archive_root=archive_root or default_archive_root(),
        current_date=current_date,
    )
    output_root.mkdir(parents=True, exist_ok=True)
    built_at = datetime.now(tz=UTC)
    latest_json = output_root / "research_manifest_latest.json"
    latest_markdown = output_root / "research_manifest_latest.md"
    snapshot_json = output_root / f"research_manifest_{built_at.strftime('%Y%m%d_%H%M%S')}.json"
    manifest["output_paths"] = {
        "latest_json": str(latest_json),
        "latest_markdown": str(latest_markdown),
        "snapshot_json": str(snapshot_json),
    }
    payload = json.dumps(manifest, indent=2, sort_keys=True)
    # Atomic writes (tmp + replace): research consumers read latest_json at
    # arbitrary times while this runs as a scheduled job — a truncate-then-write
    # would hand them an empty/partial document, and a crash mid-write would leave
    # the contract entry point torn until the next manifest pass. (No fsync: the
    # manifest is rebuilt every pass, so a power-cut loss is self-healing.)
    write_text_atomic(latest_json, payload)
    write_text_atomic(snapshot_json, payload)
    write_text_atomic(latest_markdown, render_manifest_markdown(manifest))
    return manifest


def build_manifest(*, archive_root: Path, current_date: date | None = None) -> dict[str, Any]:
    built_at = datetime.now(tz=UTC)
    current = current_date or built_at.date()
    curated_root = archive_root / "curated" / "research" / "market_replayable"
    raw_depth_root = archive_root / "raw" / "market" / "binance_depth"
    normalized_roots = {
        "market": archive_root / "normalized" / "market",
        "trades": archive_root / "normalized" / "trades",
    }
    promoted, promotion_stats = read_promotion_index_with_stats(curated_root / "_promotion_index.jsonl")
    replay = read_replay_summaries(raw_depth_root)
    curated_files = collect_partition_files(curated_root)
    normalized = {
        name: collect_partition_files(root)
        for name, root in normalized_roots.items()
    }
    all_dates = sorted(
        {
            *promoted,
            *replay,
            *curated_files,
            *(day for dataset in normalized.values() for day in dataset),
        }
    )
    days: list[dict[str, Any]] = []
    for day_value in all_dates:
        promoted_day = promoted.get(day_value, empty_promoted_day())
        replay_day = replay.get(day_value, empty_replay_day())
        curated_day = curated_files.get(day_value, {"parquet_files": 0, "parquet_bytes": 0})
        normalized_day = {
            name: rows.get(day_value, {"parquet_files": 0, "parquet_bytes": 0})
            for name, rows in normalized.items()
        }
        status = compute_readiness(promoted_day, replay_day, day_value, current)
        notes: list[str] = []
        if replay_day["unreplayable_runs"]:
            notes.append("raw_depth_has_unreplayable_runs")
        if replay_day["missing_summaries"]:
            notes.append("missing_replay_summaries")
        if promoted_day["runs"] == 0:
            notes.append("no_promoted_market_runs")
        days.append(
            {
                "date": day_value,
                "readiness": status,
                "curated_market_replayable": {
                    **promoted_day,
                    **curated_day,
                },
                "normalized": normalized_day,
                "raw_depth_quality": replay_day,
                "notes": notes,
            }
        )
    summary = {
        "ready_day_count": sum(1 for item in days if item["readiness"] == "ready"),
        "ready_with_quarantine_day_count": sum(1 for item in days if item["readiness"] == "ready_with_quarantine"),
        "building_day_count": sum(1 for item in days if item["readiness"] == "building"),
        "missing_day_count": sum(1 for item in days if item["readiness"] == "missing"),
        "total_curated_market_rows": sum(item["curated_market_replayable"]["rows"] for item in days),
        "total_promoted_runs": sum(item["curated_market_replayable"]["runs"] for item in days),
        "deduped_promoted_run_count": promotion_stats["deduped_promoted_run_count"],
        "duplicate_promotion_entry_count": promotion_stats["duplicate_promotion_entry_count"],
        "duplicate_promoted_run_count": promotion_stats["duplicate_promoted_run_count"],
        "raw_promotion_index_entry_count": promotion_stats["raw_promotion_index_entry_count"],
        "corrupt_index_line_count": promotion_stats["corrupt_index_line_count"],
        "missing_replay_summary_count": sum(item["raw_depth_quality"]["missing_summaries"] for item in days),
        "total_raw_depth_unreplayable_runs": sum(item["raw_depth_quality"]["unreplayable_runs"] for item in days),
    }
    lanes = build_lane_views(archive_root=archive_root, current=current)
    return {
        "standards_version": STANDARDS_VERSION,
        "built_at": built_at.isoformat(),
        "archive_root": str(archive_root),
        "summary": summary,
        "days": days,
        "lanes_summary": summarize_lanes(lanes),
        "lanes": lanes,
    }


def compute_readiness(
    promoted_day: dict[str, Any],
    replay_day: dict[str, Any],
    day_value: str,
    current: date,
) -> str:
    """Shared readiness rule for both the legacy day timeline and per-lane views.

    `building` (the current UTC day) wins over everything else because the lane
    is still collecting; otherwise a day is `ready` when it has promoted rows and
    no bad/missing raw runs, `ready_with_quarantine` when promoted but some raw
    runs were unreplayable, and `missing` when nothing was promoted.
    """
    has_promoted = promoted_day["rows"] > 0 or promoted_day["runs"] > 0
    has_bad_raw = replay_day["unreplayable_runs"] > 0 or replay_day["missing_summaries"] > 0
    status = "missing"
    if has_promoted:
        status = "ready_with_quarantine" if has_bad_raw else "ready"
    # day_value can come from an on-disk partition name (event_date=...), so a
    # stray malformed partition must not crash the whole manifest build.
    try:
        is_current = date.fromisoformat(day_value) == current
    except ValueError:
        is_current = False
    if is_current:
        status = "building"
    return status


def build_lane_views(*, archive_root: Path, current: date) -> list[dict[str, Any]]:
    """Per-(venue, instrument, dataset) readiness — the canonical contract view.

    Lanes are discovered from the raw lane directories (`<venue>_<dataset>[_<inst>]`)
    so trades and non-Binance venues are first-class, not folded into a single
    Binance-depth timeline. Readiness is driven by the curated promotion index
    (`run_path` -> lane, accurate per instrument) and the lane's raw replay
    summaries. Curated/normalized Parquet is only venue-partitioned today
    (STANDARDS §2 partition note), so row/run counts here come from the promotion
    index rather than the Parquet partitions.
    """
    raw_market_root = archive_root / "raw" / "market"
    promo_by_lane: dict[str, dict[str, dict[str, Any]]] = {}
    for cfg in DATASET_CONFIG.values():
        index_path = (
            archive_root / "curated" / "research" / cfg["curated_dataset"] / "_promotion_index.jsonl"
        )
        promo_by_lane.update(read_promotion_index_by_lane(index_path))

    lanes: list[dict[str, Any]] = []
    for meta in discover_lanes(raw_market_root):
        replay = read_replay_summaries(raw_market_root / meta["lane"])
        promoted = promo_by_lane.get(meta["lane"], {})
        observed_gap_detection: set[str] = set()
        readiness_counts: Counter[str] = Counter()
        total_rows = 0
        total_runs = 0
        latest_ready_date: str | None = None
        day_views: list[dict[str, Any]] = []
        for day_value in sorted({*promoted, *replay}):
            promoted_day = promoted.get(day_value, empty_promoted_day())
            replay_day = replay.get(day_value, empty_replay_day())
            status = compute_readiness(promoted_day, replay_day, day_value, current)
            # Only days with an actually-read summary contribute evidence; the
            # empty-day default must not let a lane claim a provable class.
            day_gap = replay_day.get("gap_detection")
            if day_gap and day_gap != "unknown":
                observed_gap_detection.add(str(day_gap))
            notes: list[str] = []
            if replay_day["unreplayable_runs"]:
                notes.append("raw_has_unreplayable_runs")
            if replay_day["missing_summaries"]:
                notes.append("missing_replay_summaries")
            if promoted_day["runs"] == 0:
                notes.append("no_promoted_runs")
            readiness_counts[status] += 1
            total_rows += promoted_day["rows"]
            total_runs += promoted_day["runs"]
            if status in ("ready", "ready_with_quarantine"):
                latest_ready_date = max_optional(latest_ready_date, day_value)
            day_views.append(
                {
                    "date": day_value,
                    "readiness": status,
                    "curated": dict(promoted_day),
                    "raw_quality": dict(replay_day),
                    "notes": notes,
                }
            )
        # Worst class ever observed wins (none_native > checksum > sequence): the
        # old version could only downgrade FROM "sequence" on a literal
        # "none_native", so Kraken's checksum lane was published as "sequence" and
        # a lane with zero evidence defaulted to provable. No evidence -> "unknown".
        gap_detection = next(
            (value for value in _GAP_DETECTION_PRECEDENCE if value in observed_gap_detection),
            "unknown",
        )
        lanes.append(
            {
                "lane": meta["lane"],
                "venue": meta["venue"],
                "instrument": meta["instrument"],
                "dataset": meta["dataset"],
                "curated_dataset": DATASET_CONFIG[meta["dataset"]]["curated_dataset"],
                "gap_detection": gap_detection,
                "readiness_counts": {
                    "ready": readiness_counts["ready"],
                    "ready_with_quarantine": readiness_counts["ready_with_quarantine"],
                    "building": readiness_counts["building"],
                    "missing": readiness_counts["missing"],
                },
                "total_curated_rows": total_rows,
                "total_promoted_runs": total_runs,
                "latest_ready_date": latest_ready_date,
                "days": day_views,
            }
        )
    lanes.sort(key=lambda lane: (lane["venue"], lane["dataset"], lane["instrument"] or ""))
    return lanes


def summarize_lanes(lanes: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "lane_count": len(lanes),
        "venues": sorted({lane["venue"] for lane in lanes}),
        "datasets": sorted({lane["dataset"] for lane in lanes}),
        "ready_lane_days": sum(lane["readiness_counts"]["ready"] for lane in lanes),
        "ready_with_quarantine_lane_days": sum(
            lane["readiness_counts"]["ready_with_quarantine"] for lane in lanes
        ),
        "building_lane_days": sum(lane["readiness_counts"]["building"] for lane in lanes),
        "missing_lane_days": sum(lane["readiness_counts"]["missing"] for lane in lanes),
        "total_curated_rows": sum(lane["total_curated_rows"] for lane in lanes),
        "none_native_lane_count": sum(1 for lane in lanes if lane["gap_detection"] == "none_native"),
    }


def discover_lanes(raw_market_root: Path) -> list[dict[str, Any]]:
    lanes: list[dict[str, Any]] = []
    if not raw_market_root.exists():
        return lanes
    for path in sorted(p for p in raw_market_root.iterdir() if p.is_dir()):
        parsed = parse_lane(path.name)
        if parsed is None:
            continue
        venue, dataset, instrument = parsed
        lanes.append(
            {"lane": path.name, "venue": venue, "dataset": dataset, "instrument": instrument}
        )
    return lanes


def parse_lane(dirname: str) -> tuple[str, str, str | None] | None:
    """Split a raw lane directory name into (venue, dataset, instrument).

    Names follow `<venue>[_perp]_<dataset>[_<instrument>]` (STANDARDS §2.1), e.g.
    `binance_depth`, `coinbase_trades`, `binance_trades_ethusdt`,
    `bybit_perp_depth`, `binance_perp_funding`. A perp lane keeps the `_perp`
    marker in its venue (`bybit_perp`) so spot and perp lanes stay distinct rows
    in the lanes view — they are different instruments. Remaining tokens are the
    per-instrument lane suffix (None for legacy single-symbol lanes).

    The pre-perp version required token[1] to be the dataset, so every
    `<venue>_perp_<dataset>` lane parsed as dataset="perp" -> None: all 7 perp
    lanes (and funding with it) were silently absent from the manifest.
    """
    parts = dirname.split("_")
    if len(parts) < 2:
        return None
    venue, rest = parts[0], parts[1:]
    if rest[0] == "perp" and len(rest) >= 2:
        venue = f"{venue}_perp"
        rest = rest[1:]
    dataset = rest[0]
    if not venue or dataset not in DATASET_CONFIG:
        return None
    instrument = "_".join(rest[1:]) or None
    return venue, dataset, instrument


def read_promotion_index_by_lane(path: Path) -> dict[str, dict[str, dict[str, Any]]]:
    """Group a curated `_promotion_index.jsonl` by lane and day.

    Returns `{lane_dirname: {event_date: promoted_day}}`, deduped by `run_path`
    keeping the latest `promoted_at` (re-promotions overwrite). The lane is the
    parent directory of the promoted run path, so suffixed per-instrument lanes
    stay separated even though they share one curated promotion index.
    """
    by_lane: dict[str, dict[str, dict[str, Any]]] = defaultdict(
        lambda: defaultdict(empty_promoted_day)
    )
    if not path.exists():
        return {}
    rows, _corrupt = _parse_promotion_index_lines(path)
    latest_by_run: dict[str, dict[str, Any]] = {}
    for row in rows:
        run_path = str(row.get("run_path") or "")
        if not run_path:
            continue
        previous = latest_by_run.get(run_path)
        if previous is None or str(row.get("promoted_at") or "") >= str(previous.get("promoted_at") or ""):
            latest_by_run[run_path] = row
    for row in latest_by_run.values():
        run_path = Path(str(row.get("run_path") or ""))
        day_value = run_name_to_day(run_path.name)
        if day_value is None:
            continue
        lane = run_path.parent.name
        item = by_lane[lane][day_value]
        item["runs"] += 1
        item["rows"] += int(row.get("promoted_rows") or 0)
        item["latest_run"] = max_optional(item["latest_run"], run_path.name)
        item["latest_promoted_at"] = max_optional(item["latest_promoted_at"], str(row.get("promoted_at") or ""))
    return {lane: dict(days) for lane, days in by_lane.items()}


def read_promotion_index(path: Path) -> dict[str, dict[str, Any]]:
    promoted, _stats = read_promotion_index_with_stats(path)
    return promoted


def read_promotion_index_with_stats(path: Path) -> tuple[dict[str, dict[str, Any]], dict[str, int]]:
    by_day: dict[str, dict[str, Any]] = defaultdict(empty_promoted_day)
    if not path.exists():
        return {}, {
            "raw_promotion_index_entry_count": 0,
            "deduped_promoted_run_count": 0,
            "duplicate_promotion_entry_count": 0,
            "duplicate_promoted_run_count": 0,
            "corrupt_index_line_count": 0,
        }
    latest_by_run: dict[str, dict[str, Any]] = {}
    seen_counts: Counter[str] = Counter()
    rows, corrupt_line_count = _parse_promotion_index_lines(path)
    for row in rows:
        run_path = str(row.get("run_path") or "")
        if not run_path:
            continue
        seen_counts[run_path] += 1
        previous = latest_by_run.get(run_path)
        if previous is None or str(row.get("promoted_at") or "") >= str(previous.get("promoted_at") or ""):
            latest_by_run[run_path] = row

    for row in latest_by_run.values():
        run_path = str(row.get("run_path") or "")
        day_value = run_name_to_day(Path(run_path).name)
        if day_value is None:
            continue
        item = by_day[day_value]
        item["runs"] += 1
        item["rows"] += int(row.get("promoted_rows") or 0)
        item["latest_run"] = max_optional(item["latest_run"], Path(run_path).name)
        item["latest_promoted_at"] = max_optional(item["latest_promoted_at"], str(row.get("promoted_at") or ""))
    duplicate_run_count = sum(1 for count in seen_counts.values() if count > 1)
    duplicate_entry_count = sum(count - 1 for count in seen_counts.values() if count > 1)
    return dict(by_day), {
        "raw_promotion_index_entry_count": sum(seen_counts.values()),
        "deduped_promoted_run_count": len(latest_by_run),
        "duplicate_promotion_entry_count": duplicate_entry_count,
        "duplicate_promoted_run_count": duplicate_run_count,
        "corrupt_index_line_count": corrupt_line_count,
    }


def _parse_promotion_index_lines(path: Path) -> tuple[list[dict[str, Any]], int]:
    """Parse a promotion index, skipping torn/garbage lines (the file is appended
    concurrently by promote jobs; one bad line must not crash-loop the manifest
    job and freeze "latest" forever). Single tolerance policy for both the
    by-day and by-lane readers. Returns (rows, corrupt_line_count)."""
    rows: list[dict[str, Any]] = []
    corrupt_line_count = 0
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except ValueError:
            corrupt_line_count += 1
            continue
        if not isinstance(row, dict):
            corrupt_line_count += 1
            continue
        rows.append(row)
    return rows, corrupt_line_count


def read_replay_summaries(root: Path) -> dict[str, dict[str, Any]]:
    by_day: dict[str, dict[str, Any]] = defaultdict(empty_replay_day)
    if not root.exists():
        return {}
    for run_dir in sorted(path for path in root.iterdir() if path.is_dir()):
        day_value = run_name_to_day(run_dir.name)
        if day_value is None:
            continue
        item = by_day[day_value]
        item["summaries"] += 1
        summary_path = run_dir / "metrics" / "replay_summary.json"
        if not summary_path.exists():
            item["missing_summaries"] += 1
            continue
        try:
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            # A torn summary reads as missing (same posture as replay/health) rather
            # than crashing the manifest build forever.
            item["missing_summaries"] += 1
            continue
        if not isinstance(summary, dict):
            item["missing_summaries"] += 1
            continue
        replayable = bool(summary.get("replayable"))
        item["replayable_runs" if replayable else "unreplayable_runs"] += 1
        item["gap_runs"] += 1 if summary.get("gap_count", 0) else 0
        item["snapshot_gap_runs"] += 1 if summary.get("snapshot_gap_count", 0) else 0
        item["crossed_book_runs"] += 1 if summary.get("crossed_book_count", 0) else 0
        # Non-sequence feeds tag themselves so consumers don't assume gaplessness
        # (STANDARDS §4.3). Days with no read summary stay "unknown" — never
        # default to a provable class without evidence.
        if summary.get("gap_detection"):
            item["gap_detection"] = summary["gap_detection"]
        item["latest_run"] = max_optional(item["latest_run"], run_dir.name)
    return dict(by_day)


def collect_partition_files(root: Path) -> dict[str, dict[str, int]]:
    by_day: dict[str, dict[str, int]] = defaultdict(lambda: {"parquet_files": 0, "parquet_bytes": 0})
    if not root.exists():
        return {}
    for path in root.rglob("*.parquet"):
        day_value = partition_value(path, "event_date")
        if day_value is None:
            continue
        by_day[day_value]["parquet_files"] += 1
        by_day[day_value]["parquet_bytes"] += path.stat().st_size
    return dict(by_day)


def render_manifest_markdown(manifest: dict[str, Any]) -> str:
    lines = [
        "# Research Data Manifest",
        "",
        f"- Standards version: `{manifest.get('standards_version', 'unknown')}`",
        f"- Built at: `{manifest['built_at']}`",
        f"- Archive root: `{manifest['archive_root']}`",
        f"- Ready days: `{manifest['summary']['ready_day_count']}`",
        f"- Ready with quarantine days: `{manifest['summary']['ready_with_quarantine_day_count']}`",
        f"- Building days: `{manifest['summary']['building_day_count']}`",
        f"- Curated market rows: `{manifest['summary']['total_curated_market_rows']:,}`",
        "",
        "## Lanes (venue, instrument, dataset)",
        "",
    ]
    lanes = manifest.get("lanes", [])
    if lanes:
        lines.append(
            "| Lane | Venue | Instrument | Dataset | Gaps | Ready | Quarantine | Building | "
            "Missing | Curated rows | Latest ready |"
        )
        lines.append("| --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |")
        for lane in lanes:
            counts = lane["readiness_counts"]
            lines.append(
                f"| {lane['lane']} | {lane['venue']} | {lane['instrument'] or '-'} | "
                f"{lane['dataset']} | {lane['gap_detection']} | {counts['ready']} | "
                f"{counts['ready_with_quarantine']} | {counts['building']} | {counts['missing']} | "
                f"{lane['total_curated_rows']:,} | {lane['latest_ready_date'] or '-'} |"
            )
    else:
        lines.append("_No lanes discovered._")
    lines.extend(
        [
            "",
            "## Legacy day timeline (Binance depth)",
            "",
            "| Date | Status | Market rows | Promoted runs | Replayable/raw | Notes |",
            "| --- | --- | ---: | ---: | --- | --- |",
        ]
    )
    for day in manifest["days"]:
        curated = day["curated_market_replayable"]
        quality = day["raw_depth_quality"]
        replay_text = f"{quality['replayable_runs']}/{quality['summaries']}"
        lines.append(
            f"| {day['date']} | {day['readiness']} | {curated['rows']:,} | "
            f"{curated['runs']:,} | {replay_text} | {', '.join(day['notes'])} |"
        )
    return "\n".join(lines) + "\n"


def empty_promoted_day() -> dict[str, Any]:
    return {"runs": 0, "rows": 0, "latest_run": None, "latest_promoted_at": None}


def empty_replay_day() -> dict[str, Any]:
    return {
        "summaries": 0,
        "missing_summaries": 0,
        "replayable_runs": 0,
        "unreplayable_runs": 0,
        "gap_runs": 0,
        "snapshot_gap_runs": 0,
        "crossed_book_runs": 0,
        "gap_detection": "unknown",
        "latest_run": None,
    }


def run_name_to_day(name: str) -> str | None:
    if len(name) < 8 or not name[:8].isdigit():
        return None
    day = f"{name[:4]}-{name[4:6]}-{name[6:8]}"
    try:
        # 8 leading digits is not enough — a stray dir like 20269999_x would
        # later crash date.fromisoformat inside compute_readiness.
        date.fromisoformat(day)
    except ValueError:
        return None
    return day


def partition_value(path: Path, key: str) -> str | None:
    prefix = f"{key}="
    for part in path.parts:
        if part.startswith(prefix):
            return part[len(prefix) :]
    return None


def max_optional(left: str | None, right: str | None) -> str | None:
    if not left:
        return right
    if not right:
        return left
    return max(left, right)
