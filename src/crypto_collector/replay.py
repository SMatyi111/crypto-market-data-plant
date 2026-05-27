from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class ReplaySummary:
    replay_type: str
    mode: str
    run_path: str
    events_path: str
    source: str | None
    product: str | None
    instrument_id: str | None
    snapshot_path: str | None
    snapshot_last_update_id: int | None
    event_count: int
    applied_bid_updates: int
    applied_ask_updates: int
    gap_count: int
    snapshot_gap_count: int
    reordered_count: int
    invalid_range_count: int
    crossed_book_count: int
    first_event_time: str | None
    last_event_time: str | None
    first_update_id: int | None
    last_update_id: int | None
    bid_levels: int
    ask_levels: int
    best_bid: float | None
    best_ask: float | None
    spread: float | None
    replayable: bool
    findings: list[str]
    top_bids: list[list[float]]
    top_asks: list[list[float]]
    summary_path: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class BookSyncRunStatus:
    run_path: str
    started_at: str | None
    replay_summary_path: str | None
    snapshot_path: str | None
    replayable: bool | None
    findings: list[str]
    gap_count: int | None
    snapshot_gap_count: int | None
    reordered_count: int | None
    invalid_range_count: int | None
    crossed_book_count: int | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class BookSyncHealthReport:
    status: str
    checked_at: str
    source_root: str
    scanned_run_count: int
    replayable_run_count: int
    missing_replay_count: int
    snapshot_gap_run_count: int
    gap_run_count: int
    crossed_book_run_count: int
    findings: list[str]
    runs: list[BookSyncRunStatus]

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "checked_at": self.checked_at,
            "source_root": self.source_root,
            "scanned_run_count": self.scanned_run_count,
            "replayable_run_count": self.replayable_run_count,
            "missing_replay_count": self.missing_replay_count,
            "snapshot_gap_run_count": self.snapshot_gap_run_count,
            "gap_run_count": self.gap_run_count,
            "crossed_book_run_count": self.crossed_book_run_count,
            "findings": self.findings,
            "runs": [run.to_dict() for run in self.runs],
        }


@dataclass(slots=True)
class ReplayBackfillRun:
    run_path: str
    action: str
    replay_summary_path: str | None
    replayable: bool | None
    findings: list[str]
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ReplayBackfillReport:
    status: str
    checked_at: str
    source_root: str
    scanned_run_count: int
    created_count: int
    updated_count: int
    skipped_count: int
    failed_count: int
    findings: list[str]
    runs: list[ReplayBackfillRun]

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "checked_at": self.checked_at,
            "source_root": self.source_root,
            "scanned_run_count": self.scanned_run_count,
            "created_count": self.created_count,
            "updated_count": self.updated_count,
            "skipped_count": self.skipped_count,
            "failed_count": self.failed_count,
            "findings": self.findings,
            "runs": [run.to_dict() for run in self.runs],
        }


def replay_depth_run(
    run_path: Path,
    *,
    max_levels: int = 10,
    write_summary: bool = True,
) -> ReplaySummary:
    resolved_run_path, events_path, summary_path = _resolve_run_paths(run_path)
    snapshot_path = resolved_run_path / "snapshots" / "book_snapshot.json"
    snapshot_payload = _load_snapshot(snapshot_path)
    snapshot = snapshot_payload.get("snapshot") if isinstance(snapshot_payload, dict) else None
    snapshot_last_update_id = _optional_int(snapshot.get("lastUpdateId")) if isinstance(snapshot, dict) else None
    bids = _book_from_snapshot(snapshot.get("bids")) if isinstance(snapshot, dict) else {}
    asks = _book_from_snapshot(snapshot.get("asks")) if isinstance(snapshot, dict) else {}
    event_count = 0
    applied_bid_updates = 0
    applied_ask_updates = 0
    gap_count = 0
    snapshot_gap_count = 0
    reordered_count = 0
    invalid_range_count = 0
    crossed_book_count = 0
    first_update_id: int | None = None
    last_update_id: int | None = None
    previous_final_update_id: int | None = None
    first_event_time: str | None = None
    last_event_time: str | None = None
    source: str | None = None
    product: str | None = None
    instrument_id: str | None = None
    snapshot_anchor_applied = snapshot_last_update_id is None

    for row in _read_jsonl(events_path):
        event_count += 1
        source = source or _optional_str(row.get("source"))
        product = product or _optional_str(row.get("product"))
        instrument = row.get("instrument")
        if instrument_id is None and isinstance(instrument, dict):
            instrument_id = _optional_str(instrument.get("instrument_id"))

        event_time = _optional_str(row.get("event_time")) or _optional_str(row.get("received_at"))
        if first_event_time is None:
            first_event_time = event_time
        last_event_time = event_time

        first_id = _optional_int(row.get("first_update_id"))
        final_id = _optional_int(row.get("final_update_id"))
        if first_id is None or final_id is None or final_id < first_id:
            invalid_range_count += 1
        else:
            if snapshot_last_update_id is not None and not snapshot_anchor_applied:
                if final_id <= snapshot_last_update_id:
                    # Update predates the snapshot; ignore it without touching the cursor.
                    continue
                if first_id > snapshot_last_update_id + 1:
                    snapshot_gap_count += 1
                snapshot_anchor_applied = True
                if first_update_id is None:
                    first_update_id = first_id
                last_update_id = final_id
                # Anchor the post-snapshot cursor on this event's final_id so subsequent
                # events are compared against the actual stream, not against the stale
                # snapshot id (which would double-count a snapshot gap as a sequence gap).
                previous_final_update_id = final_id
            else:
                if first_update_id is None:
                    first_update_id = first_id
                last_update_id = final_id
                if previous_final_update_id is not None:
                    if final_id <= previous_final_update_id:
                        reordered_count += 1
                    elif first_id > previous_final_update_id + 1:
                        gap_count += 1
                if previous_final_update_id is None or final_id > previous_final_update_id:
                    previous_final_update_id = final_id

        bid_updates = _levels_from_row(row.get("bids"))
        ask_updates = _levels_from_row(row.get("asks"))
        applied_bid_updates += len(bid_updates)
        applied_ask_updates += len(ask_updates)
        _apply_levels(bids, bid_updates)
        _apply_levels(asks, ask_updates)

        best_bid = max(bids) if bids else None
        best_ask = min(asks) if asks else None
        if best_bid is not None and best_ask is not None and best_bid >= best_ask:
            crossed_book_count += 1

    best_bid = max(bids) if bids else None
    best_ask = min(asks) if asks else None
    spread = (best_ask - best_bid) if best_bid is not None and best_ask is not None else None

    findings: list[str] = []
    if gap_count:
        findings.append("gaps_detected")
    if snapshot_gap_count:
        findings.append("snapshot_anchor_gap")
    if reordered_count:
        findings.append("reordered_or_duplicate_updates")
    if invalid_range_count:
        findings.append("invalid_update_ranges")
    if crossed_book_count:
        findings.append("crossed_book_states")
    if event_count == 0:
        findings.append("no_events")

    summary = ReplaySummary(
        replay_type="depth",
        mode="delta_only",
        run_path=str(resolved_run_path),
        events_path=str(events_path),
        source=source,
        product=product,
        instrument_id=instrument_id,
        snapshot_path=str(snapshot_path) if snapshot_payload is not None else None,
        snapshot_last_update_id=snapshot_last_update_id,
        event_count=event_count,
        applied_bid_updates=applied_bid_updates,
        applied_ask_updates=applied_ask_updates,
        gap_count=gap_count,
        snapshot_gap_count=snapshot_gap_count,
        reordered_count=reordered_count,
        invalid_range_count=invalid_range_count,
        crossed_book_count=crossed_book_count,
        first_event_time=first_event_time,
        last_event_time=last_event_time,
        first_update_id=first_update_id,
        last_update_id=last_update_id,
        bid_levels=len(bids),
        ask_levels=len(asks),
        best_bid=best_bid,
        best_ask=best_ask,
        spread=spread,
        replayable=event_count > 0 and gap_count == 0 and snapshot_gap_count == 0 and reordered_count == 0 and invalid_range_count == 0,
        findings=findings,
        top_bids=_top_levels(bids, reverse=True, max_levels=max_levels),
        top_asks=_top_levels(asks, reverse=False, max_levels=max_levels),
        summary_path=str(summary_path) if summary_path is not None else None,
    )

    if write_summary and summary_path is not None:
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(
            json.dumps(summary.to_dict(), indent=2, sort_keys=True),
            encoding="utf-8",
        )

    return summary


def build_book_sync_health_report(
    source_root: Path,
    *,
    limit: int = 20,
    max_age_hours: float = 24.0,
) -> BookSyncHealthReport:
    checked_at = datetime.now(tz=UTC)
    runs: list[BookSyncRunStatus] = []
    findings: list[str] = []
    replayable_run_count = 0
    missing_replay_count = 0
    snapshot_gap_run_count = 0
    gap_run_count = 0
    crossed_book_run_count = 0
    cutoff = checked_at - timedelta(hours=max_age_hours)

    for run_dir in _recent_run_dirs(source_root, limit=limit):
        started_at = _parse_run_started_at(run_dir)
        if started_at is not None and started_at < cutoff:
            continue
        replay_summary_path = run_dir / "metrics" / "replay_summary.json"
        replay_payload = _load_snapshot(replay_summary_path)
        if replay_payload is None:
            missing_replay_count += 1
            runs.append(
                BookSyncRunStatus(
                    run_path=str(run_dir),
                    started_at=started_at.isoformat() if started_at is not None else None,
                    replay_summary_path=None,
                    snapshot_path=str(run_dir / "snapshots" / "book_snapshot.json") if (run_dir / "snapshots" / "book_snapshot.json").exists() else None,
                    replayable=None,
                    findings=["missing_replay_summary"],
                    gap_count=None,
                    snapshot_gap_count=None,
                    reordered_count=None,
                    invalid_range_count=None,
                    crossed_book_count=None,
                )
            )
            continue

        run_findings = [str(item) for item in replay_payload.get("findings", [])]
        gap_count = _optional_int(replay_payload.get("gap_count"))
        snapshot_gap_count = _optional_int(replay_payload.get("snapshot_gap_count"))
        reordered_count = _optional_int(replay_payload.get("reordered_count"))
        invalid_range_count = _optional_int(replay_payload.get("invalid_range_count"))
        crossed_book_count = _optional_int(replay_payload.get("crossed_book_count"))
        replayable = bool(replay_payload.get("replayable"))
        if replayable:
            replayable_run_count += 1
        if snapshot_gap_count:
            snapshot_gap_run_count += 1
        if gap_count:
            gap_run_count += 1
        if crossed_book_count:
            crossed_book_run_count += 1

        runs.append(
            BookSyncRunStatus(
                run_path=str(run_dir),
                started_at=started_at.isoformat() if started_at is not None else None,
                replay_summary_path=str(replay_summary_path),
                snapshot_path=_optional_str(replay_payload.get("snapshot_path")),
                replayable=replayable,
                findings=run_findings,
                gap_count=gap_count,
                snapshot_gap_count=snapshot_gap_count,
                reordered_count=reordered_count,
                invalid_range_count=invalid_range_count,
                crossed_book_count=crossed_book_count,
            )
        )

    if not runs:
        findings.append("no_recent_runs")
    if missing_replay_count:
        findings.append("missing_replay_summaries")
    if snapshot_gap_run_count:
        findings.append("snapshot_anchor_gaps_detected")
    if gap_run_count:
        findings.append("sequence_gaps_detected")
    if crossed_book_run_count:
        findings.append("crossed_book_states_detected")
    if any(run.replayable is False for run in runs if run.replayable is not None):
        findings.append("unreplayable_runs_detected")

    status = "ok"
    if findings:
        status = "warn"
    if any(item in findings for item in ["snapshot_anchor_gaps_detected", "sequence_gaps_detected", "crossed_book_states_detected", "unreplayable_runs_detected"]):
        status = "error"

    return BookSyncHealthReport(
        status=status,
        checked_at=checked_at.isoformat(),
        source_root=str(source_root),
        scanned_run_count=len(runs),
        replayable_run_count=replayable_run_count,
        missing_replay_count=missing_replay_count,
        snapshot_gap_run_count=snapshot_gap_run_count,
        gap_run_count=gap_run_count,
        crossed_book_run_count=crossed_book_run_count,
        findings=findings,
        runs=runs,
    )


def backfill_replay_summaries(
    source_root: Path,
    *,
    limit: int = 50,
    max_age_hours: float = 24.0,
    overwrite: bool = False,
) -> ReplayBackfillReport:
    checked_at = datetime.now(tz=UTC)
    cutoff = checked_at - timedelta(hours=max_age_hours)
    runs: list[ReplayBackfillRun] = []
    created_count = 0
    updated_count = 0
    skipped_count = 0
    failed_count = 0

    for run_dir in _recent_run_dirs(source_root, limit=limit):
        started_at = _parse_run_started_at(run_dir)
        if started_at is not None and started_at < cutoff:
            continue
        replay_summary_path = run_dir / "metrics" / "replay_summary.json"
        events_path = run_dir / "clean" / "events.jsonl"
        if not events_path.exists():
            skipped_count += 1
            runs.append(
                ReplayBackfillRun(
                    run_path=str(run_dir),
                    action="skipped_missing_events",
                    replay_summary_path=str(replay_summary_path) if replay_summary_path.exists() else None,
                    replayable=None,
                    findings=[],
                )
            )
            continue
        if replay_summary_path.exists() and not overwrite:
            skipped_count += 1
            existing = _load_snapshot(replay_summary_path) or {}
            runs.append(
                ReplayBackfillRun(
                    run_path=str(run_dir),
                    action="skipped_existing",
                    replay_summary_path=str(replay_summary_path),
                    replayable=bool(existing.get("replayable")) if "replayable" in existing else None,
                    findings=[str(item) for item in existing.get("findings", [])],
                )
            )
            continue
        existed_before = replay_summary_path.exists()
        try:
            summary = replay_depth_run(run_dir, write_summary=True)
        except Exception as exc:  # noqa: BLE001
            failed_count += 1
            runs.append(
                ReplayBackfillRun(
                    run_path=str(run_dir),
                    action="failed",
                    replay_summary_path=str(replay_summary_path) if replay_summary_path.exists() else None,
                    replayable=None,
                    findings=[],
                    error=str(exc),
                )
            )
            continue

        if existed_before and overwrite:
            updated_count += 1
            action = "updated"
        else:
            created_count += 1
            action = "created"
        runs.append(
            ReplayBackfillRun(
                run_path=str(run_dir),
                action=action,
                replay_summary_path=summary.summary_path,
                replayable=summary.replayable,
                findings=summary.findings,
            )
        )

    findings: list[str] = []
    status = "ok"
    if failed_count:
        findings.append("backfill_failures")
        status = "error"
    elif created_count == 0 and updated_count == 0:
        findings.append("no_backfill_changes")
        status = "warn"

    return ReplayBackfillReport(
        status=status,
        checked_at=checked_at.isoformat(),
        source_root=str(source_root),
        scanned_run_count=len(runs),
        created_count=created_count,
        updated_count=updated_count,
        skipped_count=skipped_count,
        failed_count=failed_count,
        findings=findings,
        runs=runs,
    )


@dataclass(slots=True)
class TradesReplaySummary:
    replay_type: str
    mode: str
    run_path: str
    events_path: str
    source: str | None
    product: str | None
    instrument_id: str | None
    event_count: int
    first_trade_id: int | None
    last_trade_id: int | None
    first_event_time: str | None
    last_event_time: str | None
    non_monotonic_count: int
    trade_id_gap_count: int
    trade_id_gap_total_missing: int
    invalid_price_count: int
    invalid_size_count: int
    excessive_clock_skew_count: int
    max_clock_skew_ms: float | None
    duplicate_trade_id_count: int
    replayable: bool
    findings: list[str]
    summary_path: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def replay_trades_run(
    run_path: Path,
    *,
    max_clock_skew_ms: float = 60_000.0,
    write_summary: bool = True,
) -> TradesReplaySummary:
    """Replay-validate a trades run. The contract mirrors `replay_depth_run`: the function
    writes `metrics/replay_summary.json` with `replayable: bool` + `findings: list[str]`,
    so the existing `quarantine_bad_runs` and `promote_replayable_runs` work unchanged.

    Quality bar (per `FOLLOW_UPS.md` #2):
    - trade_id monotonicity (non-decreasing — duplicates are flagged but tolerated)
    - no trade_id gaps (Binance trade_id is a dense global counter per symbol)
    - price and size both positive and finite
    - exchange_time within `max_clock_skew_ms` of received_at
    """
    resolved_run_path, events_path, summary_path = _resolve_run_paths(run_path)
    event_count = 0
    first_trade_id: int | None = None
    last_trade_id: int | None = None
    first_event_time: str | None = None
    last_event_time: str | None = None
    non_monotonic_count = 0
    trade_id_gap_count = 0
    trade_id_gap_total_missing = 0
    invalid_price_count = 0
    invalid_size_count = 0
    excessive_clock_skew_count = 0
    max_clock_skew_ms_seen: float | None = None
    duplicate_trade_id_count = 0
    source: str | None = None
    product: str | None = None
    instrument_id: str | None = None
    previous_trade_id: int | None = None

    for row in _read_jsonl(events_path):
        event_count += 1
        source = source or _optional_str(row.get("source"))
        product = product or _optional_str(row.get("product"))
        metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        if instrument_id is None:
            instrument_id = _optional_str(metadata.get("instrument_id"))

        exchange_time_str = _optional_str(row.get("exchange_time"))
        if first_event_time is None:
            first_event_time = exchange_time_str
        last_event_time = exchange_time_str

        # trade_id monotonicity + gap
        trade_id = _optional_int(row.get("sequence"))
        if trade_id is None:
            trade_id = _optional_int(row.get("trade_id"))
        if trade_id is not None:
            if first_trade_id is None:
                first_trade_id = trade_id
            last_trade_id = trade_id
            if previous_trade_id is not None:
                if trade_id < previous_trade_id:
                    non_monotonic_count += 1
                elif trade_id == previous_trade_id:
                    duplicate_trade_id_count += 1
                else:
                    delta = trade_id - previous_trade_id
                    if delta > 1:
                        trade_id_gap_count += 1
                        trade_id_gap_total_missing += delta - 1
            previous_trade_id = trade_id

        # price/size positivity
        price = _optional_float(row.get("price"))
        if price is None or not _is_finite_positive(price):
            invalid_price_count += 1
        size = _optional_float(row.get("size"))
        if size is None or not _is_finite_positive(size):
            invalid_size_count += 1

        # clock-skew
        received_at_str = _optional_str(row.get("received_at"))
        skew_ms = _abs_skew_ms(exchange_time_str, received_at_str)
        if skew_ms is not None:
            if max_clock_skew_ms_seen is None or skew_ms > max_clock_skew_ms_seen:
                max_clock_skew_ms_seen = skew_ms
            if skew_ms > max_clock_skew_ms:
                excessive_clock_skew_count += 1

    findings: list[str] = []
    if event_count == 0:
        findings.append("no_events")
    if non_monotonic_count:
        findings.append("non_monotonic_trade_ids")
    if trade_id_gap_count:
        findings.append("trade_id_gaps")
    if invalid_price_count:
        findings.append("invalid_prices")
    if invalid_size_count:
        findings.append("invalid_sizes")
    if excessive_clock_skew_count:
        findings.append("excessive_clock_skew")

    replayable = (
        event_count > 0
        and non_monotonic_count == 0
        and trade_id_gap_count == 0
        and invalid_price_count == 0
        and invalid_size_count == 0
        and excessive_clock_skew_count == 0
    )

    summary = TradesReplaySummary(
        replay_type="trades",
        mode="trade_stream",
        run_path=str(resolved_run_path),
        events_path=str(events_path),
        source=source,
        product=product,
        instrument_id=instrument_id,
        event_count=event_count,
        first_trade_id=first_trade_id,
        last_trade_id=last_trade_id,
        first_event_time=first_event_time,
        last_event_time=last_event_time,
        non_monotonic_count=non_monotonic_count,
        trade_id_gap_count=trade_id_gap_count,
        trade_id_gap_total_missing=trade_id_gap_total_missing,
        invalid_price_count=invalid_price_count,
        invalid_size_count=invalid_size_count,
        excessive_clock_skew_count=excessive_clock_skew_count,
        max_clock_skew_ms=max_clock_skew_ms_seen,
        duplicate_trade_id_count=duplicate_trade_id_count,
        replayable=replayable,
        findings=findings,
        summary_path=str(summary_path) if summary_path is not None else None,
    )

    if write_summary and summary_path is not None:
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(
            json.dumps(summary.to_dict(), indent=2, sort_keys=True),
            encoding="utf-8",
        )

    return summary


def _is_finite_positive(value: float) -> bool:
    try:
        if value <= 0:
            return False
        if value != value:  # NaN
            return False
        if value in (float("inf"), float("-inf")):
            return False
    except TypeError:
        return False
    return True


def _abs_skew_ms(exchange_time_iso: str | None, received_at_iso: str | None) -> float | None:
    if not exchange_time_iso or not received_at_iso:
        return None
    try:
        exchange_dt = datetime.fromisoformat(exchange_time_iso)
        received_dt = datetime.fromisoformat(received_at_iso)
    except (TypeError, ValueError):
        return None
    delta = received_dt - exchange_dt
    return abs(delta.total_seconds() * 1000.0)


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _resolve_run_paths(run_path: Path) -> tuple[Path, Path, Path | None]:
    if run_path.is_file():
        events_path = run_path
        if run_path.parent.name == "clean" and run_path.parent.parent.exists():
            resolved_run_path = run_path.parent.parent
            return resolved_run_path, events_path, resolved_run_path / "metrics" / "replay_summary.json"
        return run_path.parent, events_path, None

    events_path = run_path / "clean" / "events.jsonl"
    if not events_path.exists():
        raise FileNotFoundError(f"replay events not found: {events_path}")
    return run_path, events_path, run_path / "metrics" / "replay_summary.json"


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rows.append(json.loads(line))
    return rows


def _load_snapshot(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _recent_run_dirs(source_root: Path, *, limit: int) -> list[Path]:
    if not source_root.exists():
        return []
    run_dirs = [path for path in source_root.iterdir() if path.is_dir()]
    return sorted(run_dirs, key=lambda path: path.name, reverse=True)[:limit]


def _levels_from_row(value: Any) -> list[tuple[float, float]]:
    if not isinstance(value, list):
        return []
    levels: list[tuple[float, float]] = []
    for item in value:
        try:
            price = float(item[0])
            size = float(item[1])
        except (TypeError, ValueError, IndexError):
            continue
        levels.append((price, size))
    return levels


def _apply_levels(book: dict[float, float], levels: list[tuple[float, float]]) -> None:
    for price, size in levels:
        if size == 0:
            book.pop(price, None)
        else:
            book[price] = size


def _book_from_snapshot(value: Any) -> dict[float, float]:
    book: dict[float, float] = {}
    for price, size in _levels_from_row(value):
        if size > 0:
            book[price] = size
    return book


def _top_levels(book: dict[float, float], *, reverse: bool, max_levels: int) -> list[list[float]]:
    levels = sorted(book.items(), key=lambda item: item[0], reverse=reverse)
    return [[price, size] for price, size in levels[:max_levels]]


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _optional_str(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _parse_run_started_at(path: Path) -> datetime | None:
    try:
        return datetime.strptime(path.name, "%Y%m%d_%H%M%S").replace(tzinfo=UTC)
    except ValueError:
        return None
