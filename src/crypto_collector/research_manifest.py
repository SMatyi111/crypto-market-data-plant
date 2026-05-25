from __future__ import annotations

from collections import Counter, defaultdict
from datetime import UTC, date, datetime
import json
from pathlib import Path
from typing import Any

from .config import default_archive_root


DEFAULT_MANIFEST_ROOT = default_archive_root() / "curated" / "research" / "manifests"


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
    latest_json.write_text(payload, encoding="utf-8")
    snapshot_json.write_text(payload, encoding="utf-8")
    latest_markdown.write_text(render_manifest_markdown(manifest), encoding="utf-8")
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
    promoted = read_promotion_index(curated_root / "_promotion_index.jsonl")
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
        status = "ready" if promoted_day["runs"] > 0 and replay_day["unreplayable_runs"] == 0 else "missing"
        if date.fromisoformat(day_value) == current:
            status = "building"
        notes: list[str] = []
        if replay_day["unreplayable_runs"]:
            notes.append("raw_depth_has_unreplayable_runs")
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
        "building_day_count": sum(1 for item in days if item["readiness"] == "building"),
        "missing_day_count": sum(1 for item in days if item["readiness"] == "missing"),
        "total_curated_market_rows": sum(item["curated_market_replayable"]["rows"] for item in days),
        "total_promoted_runs": sum(item["curated_market_replayable"]["runs"] for item in days),
        "total_raw_depth_unreplayable_runs": sum(item["raw_depth_quality"]["unreplayable_runs"] for item in days),
    }
    return {
        "built_at": built_at.isoformat(),
        "archive_root": str(archive_root),
        "summary": summary,
        "days": days,
    }


def read_promotion_index(path: Path) -> dict[str, dict[str, Any]]:
    by_day: dict[str, dict[str, Any]] = defaultdict(empty_promoted_day)
    if not path.exists():
        return {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        day_value = run_name_to_day(Path(str(row.get("run_path", ""))).name)
        if day_value is None:
            continue
        item = by_day[day_value]
        item["runs"] += 1
        item["rows"] += int(row.get("promoted_rows") or 0)
        item["latest_run"] = max_optional(item["latest_run"], Path(str(row.get("run_path", ""))).name)
        item["latest_promoted_at"] = max_optional(item["latest_promoted_at"], str(row.get("promoted_at") or ""))
    return dict(by_day)


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
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        replayable = bool(summary.get("replayable"))
        item["replayable_runs" if replayable else "unreplayable_runs"] += 1
        item["gap_runs"] += 1 if summary.get("gap_count", 0) else 0
        item["snapshot_gap_runs"] += 1 if summary.get("snapshot_gap_count", 0) else 0
        item["crossed_book_runs"] += 1 if summary.get("crossed_book_count", 0) else 0
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
        f"- Built at: `{manifest['built_at']}`",
        f"- Archive root: `{manifest['archive_root']}`",
        f"- Ready days: `{manifest['summary']['ready_day_count']}`",
        f"- Building days: `{manifest['summary']['building_day_count']}`",
        f"- Curated market rows: `{manifest['summary']['total_curated_market_rows']:,}`",
        "",
        "| Date | Status | Market rows | Promoted runs | Replayable/raw | Notes |",
        "| --- | --- | ---: | ---: | --- | --- |",
    ]
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
        "latest_run": None,
    }


def run_name_to_day(name: str) -> str | None:
    if len(name) < 8 or not name[:8].isdigit():
        return None
    return f"{name[:4]}-{name[4:6]}-{name[6:8]}"


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
