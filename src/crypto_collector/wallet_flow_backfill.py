"""Audit and backfill frozen-cohort wallet fills from the local Hyperliquid node archive.

Background (ROADMAP finding 2026-09-14): cohort wallet 9 of the
`hyperliquid-wallet-flow` lane sat on a repeating capped page from 2026-08-25 until
PR #82 fixed the paging. The public `userFillsByTime` endpoint documents a window of
a wallet's most recent 10,000 fills, so a stall that long risks permanent loss and the
poller cannot be relied on to close it. The owner-held mirror of the chain's
`node_fills_by_block` stream (the requester-pays S3 archive, kept current daily under
the reference-data tree) holds every fill, so whatever the poller does not reach is
recoverable from disk without another API or S3 call - and the same comparison is a
completeness audit of the lane against the chain.

What this writes, and how it stays honest:

1. A normal lane run directory under the lane's raw root (`raw/messages.jsonl`,
   `clean/events.jsonl`, `metrics/summary.jsonl`, `metrics/replay_summary.json`),
   named with the backfill moment (bumped by a second if that name is taken, so a
   backfill never appends into another run), so the EXISTING score / promote chain
   lands the rows. No second promoter, no direct curated write (CLAUDE.md "exactly
   one promoter per lane").
2. Rows are built by the lane's own normalizer from API-shaped fill dicts. The only
   differences from a polled row are `raw_type="node_fills_by_block"` (the
   provenance marker - an existing string column, so the curated struct schema is
   unchanged) and `received_at` = the backfill moment. The plant genuinely did not
   hold these rows before then; a consumer joining on availability sees them as
   late data, which is the truth. Event time is the venue fill time, untouched.
3. Dedup runs against every durable row the plant holds for the lane: hot raw runs
   (the same scan the collector performs at start-up), the cold-tier raw runs the
   archive offload has already moved (`cold_root`), and the curated parquet
   (`curated_root`, the research surface, which keeps rows of runs whose raw is on
   either tier). A hot-only scan is NOT a completeness audit: this lane's runs are
   offloaded after ~4 days.
4. Poller guard: a fill is written only if it is older than the live poller's
   re-fetch window for that wallet (hot high-water minus the overlap minus a margin).
   The poller loads its own dedup set once per segment and never learns about a run
   written under it, so anything inside its window could be landed twice (by it after
   our scan, or by us after its fetch). Newer missing fills are reported as
   `deferred_recent_count` and left to the poller or a later pass. A wallet with no
   hot rows has no known window and is not written (`poller_window_unknown`) unless
   the guard is disabled explicitly, which is only sane while the lane is stopped.
5. Skew guard: `replay_wallet_flow_run` fails a run whose `received_at - exchange_time`
   exceeds its 90-day gate; with `received_at` = now such rows would be quarantined and
   re-written on every pass. They are excluded up front (`skew_excluded_count`) and
   never written. Only the cohort's target coins, only fills at or after the cohort's
   prospective boundary, only the requested window.

Dry-run is the default and writes nothing; the default wallet set is the whole cohort,
so the dry-run is a node-archive completeness audit of the lane. `apply=True` adds
rows to a curated lane and is owner-gated (STANDARDS 4.7 backfill provenance).
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from .collectors.hyperliquid_wallet_flow import (
    DEFAULT_OVERLAP_SECONDS,
    HyperliquidWalletFillNormalizer,
    load_wallet_flow_cohort,
    scan_durable_wallet_fills,
    wallet_trade_key,
)
from .models import RawMessage
from .replay import replay_wallet_flow_run
from .storage import JsonlSink, prepare_run_paths

BACKFILL_RAW_TYPE = "node_fills_by_block"
DEFAULT_MAX_CLOCK_SKEW_MS = 7_776_000_000.0  # the lane's scorer default (90 days)
DEFAULT_POLLER_MARGIN_SECONDS = 60.0
NODE_FILL_COLUMNS = (
    "user",
    "coin",
    "px",
    "sz",
    "side",
    "time",
    "startPosition",
    "dir",
    "closedPnl",
    "hash",
    "oid",
    "crossed",
    "fee",
    "tid",
    "feeToken",
    "cloid",
    "builderFee",
    "liquidation",
)
# Node archive column -> `userFillsByTime` fill key (identical names on purpose; the
# archive was extracted with the API's field names).
_PASSTHROUGH_KEYS = (
    "coin",
    "px",
    "sz",
    "side",
    "startPosition",
    "dir",
    "closedPnl",
    "hash",
    "oid",
    "fee",
    "tid",
    "feeToken",
    "cloid",
    "builderFee",
)


@dataclass(slots=True)
class WalletBackfillReport:
    mode: str
    status: str
    checked_at: str
    wallet: str
    candidate_rank: int | None
    cohort_rank: int | None
    node_root: str
    source_root: str
    cold_root: str | None
    curated_root: str | None
    start_ms: int
    end_ms: int
    node_files_scanned: int = 0
    durable_hot_count: int = 0
    durable_cold_count: int = 0
    durable_curated_count: int = 0
    hot_highwater_ms: int | None = None
    poller_safe_end_ms: int | None = None
    node_fill_count: int = 0
    target_fill_count: int = 0
    already_durable_count: int = 0
    missing_count: int = 0
    missing_first_time: str | None = None
    missing_last_time: str | None = None
    missing_per_day: dict[str, int] = field(default_factory=dict)
    missing_per_coin: dict[str, int] = field(default_factory=dict)
    deferred_recent_count: int = 0
    skew_excluded_count: int = 0
    writable_count: int = 0
    run_path: str | None = None
    written_rows: int = 0
    replayable: bool | None = None
    replay_findings: list[str] = field(default_factory=list)
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def node_date_dirs(node_root: Path, *, start_ms: int, end_ms: int) -> list[Path]:
    """`date=YYYYMMDD` partitions of the archive that can hold fills in [start, end]."""
    start_day = datetime.fromtimestamp(start_ms / 1000, tz=UTC).strftime("%Y%m%d")
    end_day = datetime.fromtimestamp(end_ms / 1000, tz=UTC).strftime("%Y%m%d")
    if not node_root.exists():
        return []
    selected: list[Path] = []
    for path in node_root.iterdir():
        if not path.is_dir() or not path.name.startswith("date="):
            continue
        day = path.name[len("date="):]
        if len(day) == 8 and day.isdigit() and start_day <= day <= end_day:
            selected.append(path)
    return sorted(selected, key=lambda path: path.name)


def node_hour_files(node_root: Path, *, start_ms: int, end_ms: int) -> list[Path]:
    files: list[Path] = []
    for date_dir in node_date_dirs(node_root, start_ms=start_ms, end_ms=end_ms):
        files.extend(sorted(date_dir.glob("hour=*.parquet")))
    return files


def _node_row_to_fill(row: dict[str, Any]) -> dict[str, Any]:
    fill: dict[str, Any] = {}
    for key in _PASSTHROUGH_KEYS:
        value = row.get(key)
        if value is not None and value != "":
            fill[key] = value
    fill["time"] = int(row["time"])
    crossed = row.get("crossed")
    if isinstance(crossed, bool):
        fill["crossed"] = crossed
    liquidation = row.get("liquidation")
    if isinstance(liquidation, str) and liquidation.strip():
        try:
            parsed = json.loads(liquidation)
        except ValueError:
            parsed = None
        if isinstance(parsed, dict):
            fill["liquidation"] = parsed
    return fill


def iter_node_fills(
    files: Iterable[Path],
    *,
    wallets: Iterable[str],
    start_ms: int,
    end_ms: int,
) -> Iterator[tuple[str, dict[str, Any]]]:
    """Yield (wallet_lower, API-shaped fill) for the given wallets in one archive pass.

    Address match is case-insensitive; the window is inclusive on both ends. Each hour
    file is probed on `user`/`time` first and fully decoded only when it holds a hit.
    """
    wallet_set = sorted({wallet.lower() for wallet in wallets})
    if not wallet_set:
        return
    wallet_values = pa.array(wallet_set, type=pa.string())
    for file in files:
        schema_names = set(pq.read_schema(file).names)
        if "user" not in schema_names or "time" not in schema_names:
            continue
        probe = pq.read_table(file, columns=["user", "time"])
        if probe.num_rows == 0:
            continue
        mask = pc.is_in(pc.utf8_lower(probe["user"]), value_set=wallet_values)
        mask = pc.and_(mask, pc.greater_equal(probe["time"], start_ms))
        mask = pc.and_(mask, pc.less_equal(probe["time"], end_ms))
        if not pc.any(mask).as_py():
            continue
        columns = [name for name in NODE_FILL_COLUMNS if name in schema_names]
        subset = pq.read_table(file, columns=columns).filter(mask)
        for row in subset.to_pylist():
            yield str(row["user"]).lower(), _node_row_to_fill(row)


def curated_trade_keys(curated_root: Path, *, source: str = "hyperliquid") -> set[str]:
    """`trade_id` values already promoted for the lane (`<wallet>:<tid>` keys)."""
    if not curated_root.exists():
        return set()
    dataset = ds.dataset(curated_root, format="parquet", partitioning="hive")
    if "source" in dataset.schema.names:
        table = dataset.to_table(columns=["trade_id"], filter=ds.field("source") == source)
    else:
        table = dataset.to_table(columns=["trade_id"])
    return {key for key in table["trade_id"].to_pylist() if isinstance(key, str)}


def _count_prefix(keys: set[str], prefix: str) -> int:
    return sum(1 for key in keys if key.startswith(prefix))


def _iso_ms(value_ms: int) -> str:
    return datetime.fromtimestamp(value_ms / 1000, tz=UTC).isoformat()


def _free_run_started_at(source_root: Path, started_at: datetime) -> datetime:
    """First second at/after `started_at` whose run-dir name is unused. Names are
    second-granular and shared with the live poller's runs; appending into an
    existing run would merge two runs' rows and overwrite its replay summary."""
    candidate = started_at.replace(microsecond=0)
    for _ in range(3600):
        if not (source_root / candidate.strftime("%Y%m%d_%H%M%S")).exists():
            return candidate
        candidate += timedelta(seconds=1)
    raise RuntimeError("no free run-dir name within an hour of the requested start")


def audit_wallet_flow_against_node(
    *,
    node_root: Path | str,
    cohort_path: Path | str,
    source_root: Path | str,
    wallets: Iterable[str] | None = None,
    cold_root: Path | str | None = None,
    curated_root: Path | str | None = None,
    start_ms: int | None = None,
    end_ms: int | None = None,
    apply: bool = False,
    now: datetime | None = None,
    max_clock_skew_ms: float = DEFAULT_MAX_CLOCK_SKEW_MS,
    overlap_seconds: float = DEFAULT_OVERLAP_SECONDS,
    poller_margin_seconds: float = DEFAULT_POLLER_MARGIN_SECONDS,
    poller_guard: bool = True,
) -> list[WalletBackfillReport]:
    """One archive pass + one durable scan for every requested cohort wallet.

    Every requested wallet is validated against the cohort BEFORE anything is read or
    written, so a typo cannot abort the loop after an earlier wallet was applied.
    """
    node_root = Path(node_root)
    source_root = Path(source_root)
    cold_lane_root = Path(cold_root) / source_root.name if cold_root is not None else None
    curated_root_path = Path(curated_root) if curated_root is not None else None
    checked_at = (now or datetime.now(tz=UTC)).astimezone(UTC)
    checked_at_ms = int(checked_at.timestamp() * 1000)
    cohort = load_wallet_flow_cohort(cohort_path)
    by_address = {entry.address.lower(): entry for entry in cohort.wallets}

    requested = [w.lower() for w in wallets] if wallets is not None else list(by_address)
    unknown = [w for w in requested if w not in by_address]
    if unknown:
        raise ValueError(f"wallet(s) not in the frozen cohort: {', '.join(unknown)}")
    requested = list(dict.fromkeys(requested))  # keep order, drop repeats

    prospective_ms = int(cohort.prospective_start_at.timestamp() * 1000)
    window_start = max(prospective_ms, int(start_ms) if start_ms is not None else prospective_ms)
    window_end = int(end_ms) if end_ms is not None else checked_at_ms
    mode = "apply" if apply else "dry-run"

    reports: dict[str, WalletBackfillReport] = {}
    for wallet in requested:
        entry = by_address[wallet]
        reports[wallet] = WalletBackfillReport(
            mode=mode,
            status="ok",
            checked_at=checked_at.isoformat(),
            wallet=wallet,
            candidate_rank=entry.candidate_rank,
            cohort_rank=entry.cohort_rank,
            node_root=str(node_root),
            source_root=str(source_root),
            cold_root=str(cold_lane_root) if cold_lane_root is not None else None,
            curated_root=str(curated_root_path) if curated_root_path is not None else None,
            start_ms=window_start,
            end_ms=window_end,
        )
    if window_end < window_start:
        for report in reports.values():
            report.status = "empty_window"
        return [reports[w] for w in requested]

    # One durable scan per tier, shared by every wallet.
    hot_seen, hot_highwater = scan_durable_wallet_fills(
        source_root, prospective_start_at=cohort.prospective_start_at
    )
    cold_seen: set[str] = set()
    if cold_lane_root is not None and cold_lane_root.exists():
        cold_seen, _ = scan_durable_wallet_fills(
            cold_lane_root, prospective_start_at=cohort.prospective_start_at
        )
    curated_seen: set[str] = set()
    if curated_root_path is not None:
        curated_seen = curated_trade_keys(curated_root_path)
    seen = hot_seen | cold_seen | curated_seen

    overlap_ms = max(0, int(float(overlap_seconds) * 1000))
    margin_ms = max(0, int(float(poller_margin_seconds) * 1000))
    for wallet, report in reports.items():
        prefix = wallet + ":"
        report.durable_hot_count = _count_prefix(hot_seen, prefix)
        report.durable_cold_count = _count_prefix(cold_seen, prefix)
        report.durable_curated_count = _count_prefix(curated_seen, prefix)
        highwater = hot_highwater.get(wallet)
        report.hot_highwater_ms = highwater
        if highwater is not None:
            report.poller_safe_end_ms = highwater - overlap_ms - margin_ms
        elif not poller_guard:
            report.poller_safe_end_ms = window_end

    files = node_hour_files(node_root, start_ms=window_start, end_ms=window_end)
    missing: dict[str, dict[str, dict[str, Any]]] = {wallet: {} for wallet in requested}
    for wallet, fill in iter_node_fills(
        files, wallets=requested, start_ms=window_start, end_ms=window_end
    ):
        report = reports[wallet]
        report.node_fill_count += 1
        if str(fill.get("coin") or "").upper() not in cohort.target_coins:
            continue
        report.target_fill_count += 1
        key = wallet_trade_key(wallet, fill)
        if key in seen:
            report.already_durable_count += 1
            continue
        if key in missing[wallet]:
            # The archive should be duplicate-free; keep the first copy defensively.
            continue
        fill["_trade_key"] = key
        missing[wallet][key] = fill

    normalizer = HyperliquidWalletFillNormalizer()
    next_started_at = checked_at
    for wallet in requested:
        report = reports[wallet]
        report.node_files_scanned = len(files)
        ordered = sorted(
            missing[wallet].values(),
            key=lambda fill: (int(fill["time"]), str(fill.get("tid") or "")),
        )
        report.missing_count = len(ordered)
        if ordered:
            report.missing_first_time = _iso_ms(int(ordered[0]["time"]))
            report.missing_last_time = _iso_ms(int(ordered[-1]["time"]))
            for fill in ordered:
                day = _iso_ms(int(fill["time"]))[:10]
                report.missing_per_day[day] = report.missing_per_day.get(day, 0) + 1
                coin = str(fill["coin"]).upper()
                report.missing_per_coin[coin] = report.missing_per_coin.get(coin, 0) + 1

        writable: list[dict[str, Any]] = []
        for fill in ordered:
            time_ms = int(fill["time"])
            if checked_at_ms - time_ms > max_clock_skew_ms:
                report.skew_excluded_count += 1
            elif report.poller_safe_end_ms is None or time_ms > report.poller_safe_end_ms:
                report.deferred_recent_count += 1
            else:
                writable.append(fill)
        report.writable_count = len(writable)

        if not apply:
            continue
        if not ordered:
            report.status = "nothing_to_backfill"
            continue
        if report.poller_safe_end_ms is None:
            report.status = "poller_window_unknown"
            continue
        if not writable:
            report.status = "nothing_writable"
            continue

        entry = by_address[wallet]
        started_at = _free_run_started_at(source_root, next_started_at)
        next_started_at = started_at + timedelta(seconds=1)
        run_paths = prepare_run_paths(
            output_root=source_root.parent, source=source_root.name, started_at=started_at
        )
        report.run_path = str(run_paths.base)
        # Batched fsync: tens of thousands of rows in one shot; a clean close() still
        # fsyncs, and the run is scored only after both sinks are closed.
        raw_sink = JsonlSink(run_paths.raw, "messages.jsonl", fsync=True, fsync_interval_events=1000)
        clean_sink = JsonlSink(run_paths.clean, "events.jsonl", fsync=True, fsync_interval_events=1000)
        try:
            for fill in writable:
                payload = dict(fill)
                payload["_wallet"] = wallet
                payload["_candidate_rank"] = entry.candidate_rank
                payload["_cohort_rank"] = entry.cohort_rank
                payload["_prospective_start_at"] = cohort.prospective_start_at.isoformat()
                payload["_cohort_sha256"] = cohort.sha256
                payload["_capture_source"] = BACKFILL_RAW_TYPE
                raw = RawMessage(source="hyperliquid", received_at=checked_at, payload=payload)
                raw_sink.write(raw.to_dict())
                event = normalizer.normalize(raw)
                event.raw_type = BACKFILL_RAW_TYPE
                clean_sink.write(event.to_dict())
                report.written_rows += 1
        finally:
            raw_sink.close()
            clean_sink.close()

        summary = replay_wallet_flow_run(run_paths.base, max_clock_skew_ms=max_clock_skew_ms)
        report.replayable = bool(summary.replayable)
        report.replay_findings = list(summary.findings)
        JsonlSink(run_paths.metrics, "summary.jsonl").write(
            {
                "capture_source": BACKFILL_RAW_TYPE,
                "backfill": True,
                "wallet": wallet,
                "candidate_rank": entry.candidate_rank,
                "cohort_rank": entry.cohort_rank,
                "cohort_sha256": cohort.sha256,
                "prospective_start_at": cohort.prospective_start_at.isoformat(),
                "wallet_count": 1,
                "node_root": str(node_root),
                "window_start_ms": window_start,
                "window_end_ms": window_end,
                "poller_safe_end_ms": report.poller_safe_end_ms,
                "node_fill_count": report.node_fill_count,
                "target_fill_count": report.target_fill_count,
                "already_durable_count": report.already_durable_count,
                "missing_count": report.missing_count,
                "deferred_recent_count": report.deferred_recent_count,
                "skew_excluded_count": report.skew_excluded_count,
                "raw_messages": report.written_rows,
                "clean_events": report.written_rows,
                "quarantined_events": 0,
                "poll_count": 0,
                "poll_error_count": 0,
                "emitted_count": report.written_rows,
                "duplicate_count": report.already_durable_count,
                "replayable": report.replayable,
                "replay_findings": report.replay_findings,
                "replay_summary_path": summary.summary_path,
                "run_path": str(run_paths.base),
                "checked_at": checked_at.isoformat(),
            }
        )
        if not report.replayable:
            report.status = "written_not_replayable"
    return [reports[w] for w in requested]


def backfill_wallet_flow_from_node(*, wallet: str, **kwargs: Any) -> WalletBackfillReport:
    """Single-wallet convenience wrapper around `audit_wallet_flow_against_node`."""
    return audit_wallet_flow_against_node(wallets=[wallet], **kwargs)[0]
