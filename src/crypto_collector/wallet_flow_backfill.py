"""Backfill a frozen-cohort wallet's fills from the local Hyperliquid node archive.

Background (ROADMAP finding 2026-09-14): cohort wallet 9 of the
`hyperliquid-wallet-flow` lane sat on a repeating capped page from 2026-08-25 until
PR #82 fixed the paging. The public `userFillsByTime` endpoint exposes only a
wallet's most recent 10,000 fills, so the code fix cannot recover the older part of
that gap. The owner-held mirror of the chain's `node_fills_by_block` stream (the
requester-pays S3 archive, kept current daily under the reference-data tree) holds
every fill, so the gap is recoverable from disk without another API or S3 call.

What this writes, and how it stays honest:

1. A normal lane run directory under the lane's raw root (`raw/messages.jsonl`,
   `clean/events.jsonl`, `metrics/summary.jsonl`, `metrics/replay_summary.json`),
   named with the backfill moment, so the EXISTING score / promote chain lands the
   rows. No second promoter, no direct curated write (CLAUDE.md "exactly one
   promoter per lane").
2. Rows are built by the lane's own normalizer from API-shaped fill dicts. The only
   differences from a polled row are `raw_type="node_fills_by_block"` (the
   provenance marker - an existing string column, so the curated struct schema is
   unchanged) and `received_at` = the backfill moment. The plant genuinely did not
   hold these rows before then; a consumer joining on availability sees them as
   late data, which is the truth. Event time is the venue fill time, untouched.
3. Dedup runs against EVERY durable clean row of the lane (the same scan the
   collector performs at start-up), so re-running is idempotent and fills the fixed
   poller has already recovered are never duplicated.
4. Only the cohort's target coins, only fills at or after the cohort's prospective
   boundary, only the requested window.

Dry-run is the default and writes nothing. `apply=True` adds rows to a curated lane
and is owner-gated (STANDARDS 4.7 backfill provenance paragraph).
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow.compute as pc
import pyarrow.parquet as pq

from .collectors.hyperliquid_wallet_flow import (
    HyperliquidWalletFillNormalizer,
    WalletFlowCohort,
    load_wallet_flow_cohort,
    scan_durable_wallet_fills,
    wallet_trade_key,
)
from .models import RawMessage
from .replay import replay_wallet_flow_run
from .storage import JsonlSink, prepare_run_paths

BACKFILL_RAW_TYPE = "node_fills_by_block"
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
    start_ms: int
    end_ms: int
    node_files_scanned: int = 0
    node_fill_count: int = 0
    target_fill_count: int = 0
    already_durable_count: int = 0
    missing_count: int = 0
    missing_first_time: str | None = None
    missing_last_time: str | None = None
    missing_per_day: dict[str, int] = field(default_factory=dict)
    missing_per_coin: dict[str, int] = field(default_factory=dict)
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
    node_root: Path,
    *,
    wallet: str,
    start_ms: int,
    end_ms: int,
    coins: frozenset[str] | set[str] | None = None,
) -> Iterator[tuple[Path, dict[str, Any]]]:
    """Yield (file, API-shaped fill) for one wallet from the hive-partitioned archive.

    Address match is case-insensitive; the window is inclusive on both ends.
    """
    wallet_lower = wallet.lower()
    for date_dir in node_date_dirs(node_root, start_ms=start_ms, end_ms=end_ms):
        for file in sorted(date_dir.glob("hour=*.parquet")):
            schema_names = set(pq.read_schema(file).names)
            columns = [name for name in NODE_FILL_COLUMNS if name in schema_names]
            table = pq.read_table(file, columns=columns)
            if table.num_rows == 0:
                continue
            mask = pc.equal(pc.utf8_lower(table["user"]), wallet_lower)
            mask = pc.and_(mask, pc.greater_equal(table["time"], start_ms))
            mask = pc.and_(mask, pc.less_equal(table["time"], end_ms))
            subset = table.filter(mask)
            for row in subset.to_pylist():
                coin = str(row.get("coin") or "").upper()
                if coins is not None and coin not in coins:
                    continue
                yield file, _node_row_to_fill(row)


def _wallet_entry(cohort: WalletFlowCohort, wallet: str):
    wallet_lower = wallet.lower()
    for entry in cohort.wallets:
        if entry.address.lower() == wallet_lower:
            return entry
    raise ValueError(f"wallet {wallet} is not a member of the frozen cohort")


def backfill_wallet_flow_from_node(
    *,
    node_root: Path | str,
    cohort_path: Path | str,
    source_root: Path | str,
    wallet: str,
    start_ms: int | None = None,
    end_ms: int | None = None,
    apply: bool = False,
    now: datetime | None = None,
    max_clock_skew_ms: float = 7_776_000_000.0,
) -> WalletBackfillReport:
    node_root = Path(node_root)
    source_root = Path(source_root)
    checked_at = (now or datetime.now(tz=UTC)).astimezone(UTC)
    cohort = load_wallet_flow_cohort(cohort_path)
    entry = _wallet_entry(cohort, wallet)
    wallet = entry.address.lower()
    prospective_ms = int(cohort.prospective_start_at.timestamp() * 1000)
    window_start = max(prospective_ms, int(start_ms) if start_ms is not None else prospective_ms)
    window_end = int(end_ms) if end_ms is not None else int(checked_at.timestamp() * 1000)

    report = WalletBackfillReport(
        mode="apply" if apply else "dry-run",
        status="ok",
        checked_at=checked_at.isoformat(),
        wallet=wallet,
        candidate_rank=entry.candidate_rank,
        cohort_rank=entry.cohort_rank,
        node_root=str(node_root),
        source_root=str(source_root),
        start_ms=window_start,
        end_ms=window_end,
    )
    if window_end < window_start:
        report.status = "empty_window"
        return report

    seen, _highwater = scan_durable_wallet_fills(
        source_root, prospective_start_at=cohort.prospective_start_at
    )

    files_seen: set[Path] = set()
    missing: dict[str, dict[str, Any]] = {}
    for file, fill in iter_node_fills(
        node_root, wallet=wallet, start_ms=window_start, end_ms=window_end
    ):
        files_seen.add(file)
        report.node_fill_count += 1
        if str(fill.get("coin") or "").upper() not in cohort.target_coins:
            continue
        report.target_fill_count += 1
        key = wallet_trade_key(wallet, fill)
        if key in seen:
            report.already_durable_count += 1
            continue
        if key in missing:
            # The archive should be duplicate-free; keep the first copy defensively.
            continue
        fill["_trade_key"] = key
        missing[key] = fill
    report.node_files_scanned = len(files_seen)

    ordered = sorted(
        missing.values(), key=lambda fill: (int(fill["time"]), str(fill.get("tid") or ""))
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

    if not apply:
        return report
    if not ordered:
        report.status = "nothing_to_backfill"
        return report

    run_paths = prepare_run_paths(
        output_root=source_root.parent, source=source_root.name, started_at=checked_at
    )
    report.run_path = str(run_paths.base)
    normalizer = HyperliquidWalletFillNormalizer()
    # Batched fsync: tens of thousands of rows in one shot; a clean close() still
    # fsyncs, and the run is scored only after both sinks are closed.
    raw_sink = JsonlSink(run_paths.raw, "messages.jsonl", fsync=True, fsync_interval_events=1000)
    clean_sink = JsonlSink(run_paths.clean, "events.jsonl", fsync=True, fsync_interval_events=1000)
    try:
        for fill in ordered:
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
            "node_fill_count": report.node_fill_count,
            "target_fill_count": report.target_fill_count,
            "already_durable_count": report.already_durable_count,
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
    return report


def _iso_ms(value_ms: int) -> str:
    return datetime.fromtimestamp(value_ms / 1000, tz=UTC).isoformat()
