"""Hyperliquid universe positions - raw-only reference lane (STANDARDS 4.11).

Archives, verbatim and sha256-verified, every hourly positioning sweep the
`hl-liquidation-ladder-2026` study writes for the Hyperliquid wallet universe
(~22k wallets with an open BTC/ETH/SOL position: per wallet and coin the signed
size, entry, liquidation price, leverage, margin and account value, from
`clearinghouseState`). The sweep is the ONLY poller: it already spends 720 of
the venue's 1200 weight/min IP budget for ~55 min of every hour, so a second
poller from this host would starve a preregistered study for a duplicate of
its own readings. This lane therefore INGESTS rather than polls.

Completion signal is the sweep's own manifest (`snapshot_manifest.jsonl`,
one line per finished file with its sha256): the sweep writes the Parquet
non-atomically and appends the manifest line only after hashing the finished
file, so a sweep is archived only once it is listed there WITH a sha256 and
the bytes on disk hash to the listed value. A sweep not yet listed (or listed
without a hash) is left for the next run; a listed sweep whose bytes do not
match is reported and NOT archived (evidence of a torn or altered file, never
silently copied).

"Archived" is defined by the lane's own durable ledger
(`<lane>/_ingested.jsonl`, one line per archived sweep: run_id, sha256,
ingested_at), NOT by the presence of the run directory on the hot tier: the
lane is offloaded `age_only` to the cold tier, so hot-tier run directories
disappear after a few days while the source directory never rotates. Without
the ledger every offloaded sweep would be re-ingested (and re-offloaded)
every hour, forever. The ledger lives in the lane root with a leading
underscore, which archive-offload skips by contract.

Layout mirrors the other raw-only reference lanes (4.8/4.9):
`raw/market/hyperliquid_universe_positions/<run_id>/raw/<sweep file>` +
`metrics/summary.json`. run_id is derived from the sweep timestamp, so the
lane is idempotent. Nothing is ever deleted or rewritten.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from ..models import utc_now
from ..storage import write_text_atomic


SOURCE_NAME = "hyperliquid_universe_positions"
LEDGER_NAME = "_ingested.jsonl"
DEFAULT_SOURCE_ROOT = Path(r"G:\03-reference-data\hyperliquid_ladder\snapshots")
DEFAULT_MANIFEST_PATH = Path(
    r"G:\01-active\research\hl-liquidation-ladder-2026\log\snapshot_manifest.jsonl"
)
DEFAULT_STALE_AFTER_SECONDS = 3 * 3600  # sweeps are hourly; 3 misses is an incident

_SWEEP_RE = re.compile(r"^sweep=(\d{8})T(\d{4})Z\.parquet$")
_SHA_RE = re.compile(r"^[0-9a-f]{64}$")


def sweep_run_id(file_name: str) -> str | None:
    """`sweep=20260919T1702Z.parquet` -> `20260919_170200`; None if not a sweep file."""
    match = _SWEEP_RE.match(file_name)
    if not match:
        return None
    return f"{match.group(1)}_{match.group(2)}00"


def sweep_timestamp(run_id: str) -> datetime:
    return datetime.strptime(run_id, "%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc)


def sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_manifest(manifest_path: Path | None) -> dict[str, dict[str, Any]]:
    """Map sweep file name -> manifest record (last line wins). Missing -> {}."""
    records: dict[str, dict[str, Any]] = {}
    if manifest_path is None or not manifest_path.exists():
        return records
    with manifest_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except ValueError:
                continue
            if not isinstance(record, dict):
                continue
            file_value = str(record.get("file", ""))
            name = file_value.replace("\\", "/").rsplit("/", 1)[-1]
            if name:
                records[name] = record
    return records


def read_ledger(lane_root: Path) -> dict[str, str]:
    """Map run_id -> sha256 of every sweep this lane has archived (any tier)."""
    ledger: dict[str, str] = {}
    path = lane_root / LEDGER_NAME
    if not path.exists():
        return ledger
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except ValueError:
                continue
            run_id = record.get("run_id") if isinstance(record, dict) else None
            sha = record.get("sha256") if isinstance(record, dict) else None
            if isinstance(run_id, str) and isinstance(sha, str):
                ledger[run_id] = sha
    return ledger


def append_ledger(lane_root: Path, record: dict[str, Any]) -> None:
    lane_root.mkdir(parents=True, exist_ok=True)
    with (lane_root / LEDGER_NAME).open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def parquet_row_count(path: Path) -> int | None:
    try:
        import pyarrow.parquet as pq

        return int(pq.read_metadata(path).num_rows)
    except Exception:  # pragma: no cover - pyarrow absent or unreadable file
        return None


@dataclass(slots=True)
class UniversePositionsIngestResult:
    archived: list[str] = field(default_factory=list)
    skipped_existing: int = 0
    pending_unlisted: list[str] = field(default_factory=list)
    sha_mismatch: list[str] = field(default_factory=list)
    newest_sweep_id: str | None = None
    newest_sweep_age_seconds: float | None = None
    stale: bool = False


def ingest_universe_positions(
    output_root: Path | str,
    *,
    source_root: Path | str = DEFAULT_SOURCE_ROOT,
    manifest_path: Path | str | None = DEFAULT_MANIFEST_PATH,
    stale_after_seconds: float = DEFAULT_STALE_AFTER_SECONDS,
    clock: Callable[[], Any] = utc_now,
) -> UniversePositionsIngestResult:
    """Archive every finished, manifest-verified sweep not yet in the ledger.

    Raises RuntimeError AFTER archiving whatever was available when the newest
    finished sweep is older than `stale_after_seconds` (or no finished sweep
    exists at all), or when a manifest-listed sweep fails its sha256 check, so
    the runner's job-status counters surface the condition instead of a quiet
    success.
    """
    now = clock()
    source_root = Path(source_root)
    manifest = read_manifest(Path(manifest_path) if manifest_path else None)
    lane_root = Path(output_root) / SOURCE_NAME
    ledger = read_ledger(lane_root)
    result = UniversePositionsIngestResult()

    candidates: list[tuple[str, Path]] = []
    if source_root.exists():
        for day_dir in sorted(source_root.iterdir()):
            if not day_dir.is_dir():
                continue
            for file_path in sorted(day_dir.iterdir()):
                run_id = sweep_run_id(file_path.name)
                if run_id is not None and file_path.is_file():
                    candidates.append((run_id, file_path))
    candidates.sort()

    finished: list[str] = []
    for run_id, file_path in candidates:
        record = manifest.get(file_path.name)
        listed_sha = str(record.get("sha256", "")).lower() if record else ""
        if record is None or not _SHA_RE.match(listed_sha):
            # Not finished yet, or finished without a hash to verify against:
            # never archive unverified bytes; leave it for the next run.
            result.pending_unlisted.append(file_path.name)
            continue
        finished.append(run_id)

        ledger_sha = ledger.get(run_id)
        if ledger_sha is not None:
            if ledger_sha == listed_sha:
                result.skipped_existing += 1
                continue
            # The study never rewrites a sweep; a changed hash is evidence, not data.
            result.sha_mismatch.append(file_path.name)
            continue

        actual_sha = sha256_path(file_path)
        if actual_sha != listed_sha:
            result.sha_mismatch.append(file_path.name)
            continue

        run_dir = lane_root / run_id
        raw_dir = run_dir / "raw"
        metrics_dir = run_dir / "metrics"
        raw_dir.mkdir(parents=True, exist_ok=True)
        metrics_dir.mkdir(parents=True, exist_ok=True)
        target = raw_dir / file_path.name
        temporary = target.with_name(f"{target.name}.{os.getpid()}.tmp")
        shutil.copyfile(file_path, temporary)
        copied_sha = sha256_path(temporary)
        if copied_sha != actual_sha:
            temporary.unlink(missing_ok=True)
            raise RuntimeError(
                f"universe positions copy corrupted in flight: {file_path.name} "
                f"{actual_sha} != {copied_sha}"
            )
        temporary.replace(target)

        summary = {
            "source": SOURCE_NAME,
            "sweep_id": run_id,
            "sweep_started_at": record.get("started"),
            "sweep_ended_at": record.get("ended"),
            "source_path": str(file_path),
            "manifest_path": str(manifest_path) if manifest_path else None,
            "ingested_at": now.isoformat(),
            "raw_bytes": target.stat().st_size,
            "sha256": actual_sha,
            "manifest_sha256_match": True,
            "row_count": parquet_row_count(target),
            "manifest_rows": record.get("rows"),
        }
        write_text_atomic(
            metrics_dir / "summary.json", json.dumps(summary, indent=2, sort_keys=True)
        )
        append_ledger(
            lane_root,
            {"run_id": run_id, "sha256": actual_sha, "ingested_at": now.isoformat(),
             "raw_bytes": summary["raw_bytes"]},
        )
        ledger[run_id] = actual_sha
        result.archived.append(run_id)

    if finished:
        result.newest_sweep_id = finished[-1]
        age = now - sweep_timestamp(finished[-1])
        result.newest_sweep_age_seconds = age.total_seconds()
        result.stale = age > timedelta(seconds=stale_after_seconds)
    else:
        result.stale = True

    if result.sha_mismatch:
        raise RuntimeError(
            "universe positions: manifest-listed sweep(s) do not hash to the listed "
            f"sha256 (or changed since archiving), NOT archived: {result.sha_mismatch}"
        )
    if result.stale:
        raise RuntimeError(
            "universe positions: newest finished sweep is "
            f"{result.newest_sweep_id or 'absent'} "
            f"(age {result.newest_sweep_age_seconds}s > {stale_after_seconds}s); "
            "is hl-ladder-sweep alive?"
        )
    return result
