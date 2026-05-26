from __future__ import annotations

import argparse
import asyncio
import json
import logging
import shutil
import sys
import threading
import time
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

from .collectors.generic_ws import (
    GenericWebsocketCollector,
    _backoff_delay,
    _is_retryable_connect_error,
)
from .collectors.mock_l3 import MockL3Collector
from .config import (
    DEFAULT_ARCHIVE_ROOT,
    CollectorConfig,
    default_archive_root,
    default_curated_root,
    default_normalized_root,
    default_ops_root,
    default_output_root,
)
from .market_normalizers import BinanceDepthNormalizer, BinanceTradeNormalizer
from .market_snapshots import fetch_binance_order_book_snapshot, write_snapshot_file
from .models import RawMessage, utc_now
from .normalizer import GenericL3Normalizer
from .ops import (
    JobExecutionResult,
    JobSpec,
    OpsRunner,
    OpsRunnerLock,
    StandaloneWorkerLock,
    StandaloneWorkerRuntime,
    build_health_report,
    load_ops_config,
    prune_stale_worker_artifacts,
    run_cleanup,
)
from .pipeline import CollectorPipeline
from .promotion import promote_replayable_runs
from .quality import MetadataQualityGate, QualityGate
from .quarantine import quarantine_bad_runs
from .replay import backfill_replay_summaries, build_book_sync_health_report, replay_depth_run
from .research_manifest import DEFAULT_MANIFEST_ROOT, generate_research_manifest
from .storage import JsonlSink, ParquetDatasetSink, prepare_run_paths

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="market-data-plant")
    subparsers = parser.add_subparsers(dest="command", required=True)

    mock_parser = subparsers.add_parser("mock", help="Run a bounded local mock ingest")
    mock_parser.add_argument("--count", type=int, default=25)
    mock_parser.add_argument("--output-root", type=Path, default=default_output_root())
    mock_parser.add_argument("--product", default="BTC-USD")

    depth_parser = subparsers.add_parser("binance-depth-worker", help="Run segmented Binance depth collection")
    depth_parser.add_argument("--symbol", default="btcusdt")
    depth_parser.add_argument("--speed", choices=["100ms", "1000ms"], default="100ms")
    depth_parser.add_argument("--segment-count", type=int, default=5000)
    depth_parser.add_argument("--max-segments", type=int)
    depth_parser.add_argument("--cooldown-seconds", type=float, default=1.0)
    depth_parser.add_argument("--output-root", type=Path, default=default_output_root())
    depth_parser.add_argument("--ops-root", type=Path, default=default_ops_root())
    depth_parser.add_argument("--worker-name", default="binance-depth-worker")
    depth_parser.add_argument("--heartbeat-interval-seconds", type=float, default=30.0)
    depth_parser.add_argument("--snapshot-limit", type=int, default=1000)
    depth_parser.add_argument("--connect-retries", type=int, default=3)
    depth_parser.add_argument("--retry-backoff-seconds", type=float, default=2.0)
    depth_parser.add_argument("--max-backoff-seconds", type=float, default=60.0)
    depth_parser.add_argument("--snapshot-base-url", default="https://api.binance.com/api/v3/depth")

    trades_parser = subparsers.add_parser("binance-trades-worker", help="Run segmented Binance trade collection")
    trades_parser.add_argument("--symbol", default="btcusdt")
    trades_parser.add_argument("--channel", choices=["trade", "aggTrade"], default="trade")
    trades_parser.add_argument("--segment-count", type=int, default=5000)
    trades_parser.add_argument("--max-segments", type=int)
    trades_parser.add_argument("--cooldown-seconds", type=float, default=1.0)
    trades_parser.add_argument("--output-root", type=Path, default=default_output_root())
    trades_parser.add_argument("--ops-root", type=Path, default=default_ops_root())
    trades_parser.add_argument("--worker-name", default="binance-trades-worker")
    trades_parser.add_argument("--heartbeat-interval-seconds", type=float, default=30.0)
    trades_parser.add_argument("--max-delay-ms", type=int, default=60_000)
    trades_parser.add_argument("--max-future-skew-ms", type=int, default=5_000)

    ops_parser = subparsers.add_parser("ops-runner", help="Run collection and curation jobs from a manifest")
    ops_parser.add_argument("--config", type=Path, required=True)
    ops_parser.add_argument("--ops-root", type=Path, default=default_ops_root())
    ops_parser.add_argument("--poll-seconds", type=int, default=5)
    ops_parser.add_argument("--max-runs", type=int)
    ops_parser.add_argument("--stop-on-error", action="store_true")

    health_parser = subparsers.add_parser("health", help="Inspect runner and archive health")
    health_parser.add_argument("--ops-root", type=Path, default=default_ops_root())
    health_parser.add_argument("--config", type=Path)
    health_parser.add_argument("--stale-after-seconds", type=float, default=120.0)
    health_parser.add_argument("--job-stale-multiplier", type=float, default=3.0)
    health_parser.add_argument("--recent-failure-window-seconds", type=float, default=3600.0)
    health_parser.add_argument("--min-disk-free-gb", type=float, default=50.0)
    health_parser.add_argument("--format", choices=["json", "text"], default="text")

    cleanup_parser = subparsers.add_parser("cleanup", help="Report or apply archive cleanup")
    cleanup_parser.add_argument("--archive-root", type=Path, default=default_archive_root())
    cleanup_parser.add_argument("--raw-days", type=int, default=14)
    cleanup_parser.add_argument("--raw-policy", action="append", default=[], metavar="DATASET/SOURCE=DAYS")
    cleanup_parser.add_argument("--apply", action="store_true")
    cleanup_parser.add_argument("--format", choices=["json", "text"], default="text")

    prune_parser = subparsers.add_parser("ops-prune-stale-workers", help="Archive stale unmanaged worker metadata")
    prune_parser.add_argument("--ops-root", type=Path, default=default_ops_root())
    prune_parser.add_argument("--config", type=Path)
    prune_parser.add_argument("--stale-after-days", type=float, default=2.0)
    prune_parser.add_argument("--apply", action="store_true")
    prune_parser.add_argument("--format", choices=["json", "text"], default="text")

    replay_parser = subparsers.add_parser("replay", help="Replay one archived Binance depth run")
    replay_parser.add_argument("--run-path", type=Path, required=True)
    replay_parser.add_argument("--max-levels", type=int, default=10)
    replay_parser.add_argument("--format", choices=["json", "text"], default="text")

    book_sync_parser = subparsers.add_parser("book-sync-health", help="Inspect recent depth replay summaries")
    book_sync_parser.add_argument("--source-root", type=Path, default=default_archive_root() / "raw" / "market" / "binance_depth")
    book_sync_parser.add_argument("--limit", type=int, default=20)
    book_sync_parser.add_argument("--max-age-hours", type=float, default=24.0)
    book_sync_parser.add_argument("--format", choices=["json", "text"], default="text")

    backfill_parser = subparsers.add_parser("backfill-replay", help="Backfill missing replay summaries")
    backfill_parser.add_argument("--source-root", type=Path, default=default_archive_root() / "raw" / "market" / "binance_depth")
    backfill_parser.add_argument("--limit", type=int, default=50)
    backfill_parser.add_argument("--max-age-hours", type=float, default=24.0)
    backfill_parser.add_argument("--overwrite", action="store_true")
    backfill_parser.add_argument("--format", choices=["json", "text"], default="text")

    quarantine_parser = subparsers.add_parser("quarantine-runs", help="Quarantine unreplayable depth runs")
    quarantine_parser.add_argument("--source-root", type=Path, default=default_archive_root() / "raw" / "market" / "binance_depth")
    quarantine_parser.add_argument("--quarantine-root", type=Path, default=default_archive_root() / "quarantine" / "market" / "binance_depth")
    quarantine_parser.add_argument("--limit", type=int, default=50)
    quarantine_parser.add_argument("--max-age-hours", type=float, default=24.0)
    quarantine_parser.add_argument("--format", choices=["json", "text"], default="text")

    promote_parser = subparsers.add_parser("promote-replayable", help="Promote replayable runs into curated Parquet")
    promote_parser.add_argument("--source-root", type=Path, default=default_archive_root() / "raw" / "market" / "binance_depth")
    promote_parser.add_argument("--target-root", type=Path, default=default_curated_root("market_replayable"))
    promote_parser.add_argument("--quarantine-index", type=Path, default=default_archive_root() / "quarantine" / "market" / "binance_depth" / "_quarantine_index.jsonl")
    promote_parser.add_argument("--limit", type=int, default=50)
    promote_parser.add_argument("--max-age-hours", type=float, default=24.0)
    promote_parser.add_argument("--format", choices=["json", "text"], default="text")

    manifest_parser = subparsers.add_parser("research-manifest", help="Build the curated data readiness manifest")
    manifest_parser.add_argument("--archive-root", type=Path, default=default_archive_root())
    manifest_parser.add_argument("--output-root", type=Path, default=DEFAULT_MANIFEST_ROOT)
    manifest_parser.add_argument("--current-date")
    manifest_parser.add_argument("--format", choices=["json", "text"], default="text")

    subparsers.add_parser("state", help="Show archive and package state")
    return parser


async def run_mock(args: argparse.Namespace) -> None:
    collector = MockL3Collector(source="mock", product=args.product)
    run_paths = prepare_run_paths(output_root=args.output_root, source="mock")
    pipeline = CollectorPipeline(
        collector=collector,
        normalizer=GenericL3Normalizer(),
        quality_gate=QualityGate(session_id=run_paths.base.name),
        run_paths=run_paths,
        normalized_root=default_normalized_root("market"),
    )
    summary = await pipeline.run(limit=args.count)
    print(f"mock run finished: {summary.to_dict()} -> {run_paths.base}")


async def collect_binance_depth_segment(args: argparse.Namespace) -> dict[str, object]:
    try:
        import websockets
    except ImportError as exc:
        raise RuntimeError("Install the 'websockets' package to use Binance depth ingest.") from exc

    channel = "depth@100ms" if args.speed == "100ms" else "depth"
    config = CollectorConfig(
        source="binance",
        output_root=args.output_root,
        product=args.symbol,
        channel=channel,
        websocket_url="wss://stream.binance.com:9443/ws",
        subscription_style="binance",
    )
    run_paths = prepare_run_paths(output_root=config.output_root, source="binance_depth")
    raw_sink = JsonlSink(run_paths.raw, "messages.jsonl")
    clean_sink = JsonlSink(run_paths.clean, "events.jsonl")
    quarantine_sink = JsonlSink(run_paths.quarantine, "events.jsonl")
    metrics_sink = JsonlSink(run_paths.metrics, "summary.jsonl")
    parquet_sink = ParquetDatasetSink(default_normalized_root("market"))
    normalizer = BinanceDepthNormalizer()
    quality_gate = MetadataQualityGate()

    max_attempts = max(1, int(args.connect_retries))
    retry_backoff_seconds = float(args.retry_backoff_seconds)
    max_backoff_seconds = float(getattr(args, "max_backoff_seconds", 60.0))
    resubscribe_buffer_seconds = float(getattr(args, "resubscribe_buffer_seconds", 1.0))

    connection, websocket, snapshot, pending_raws, connect_attempts = await _open_binance_depth_connection(
        websockets=websockets,
        websocket_url=str(config.websocket_url),
        product=str(config.product),
        channel=str(config.channel),
        snapshot_limit=args.snapshot_limit,
        snapshot_base_url=args.snapshot_base_url,
        connect_retries=max_attempts,
        retry_backoff_seconds=retry_backoff_seconds,
    )
    snapshot_last_update_id = int(snapshot.get("lastUpdateId", 0))
    write_snapshot_file(
        run_paths.base / "snapshots" / "book_snapshot.json",
        source="binance",
        product=str(config.product).upper(),
        snapshot=snapshot,
        received_at=utc_now(),
    )

    message_count = 0
    clean_count = 0
    quarantined_count = 0
    reconnect_count = 0
    alignment_break_count = 0
    alignment_broken = False
    reconnect_attempts = 0  # consecutive retryable failures since last successful frame
    # Tracks the latest event id we've consumed so reconnect-in-place can check
    # whether the next event bridges where we left off (not where the snapshot started).
    last_seen_final_update_id = snapshot_last_update_id

    def _process_batch(raws: list[RawMessage]) -> bool:
        """Process buffered raws. Returns True if count reached."""
        nonlocal message_count, clean_count, quarantined_count, last_seen_final_update_id
        for raw in raws:
            message_count, clean_count, quarantined_count = _process_binance_depth_raw(
                raw=raw,
                raw_sink=raw_sink,
                clean_sink=clean_sink,
                quarantine_sink=quarantine_sink,
                parquet_sink=parquet_sink,
                normalizer=normalizer,
                quality_gate=quality_gate,
                message_count=message_count,
                clean_count=clean_count,
                quarantined_count=quarantined_count,
            )
            window = _binance_update_window(raw.payload)
            if window is not None and window[1] > last_seen_final_update_id:
                last_seen_final_update_id = window[1]
            if message_count >= args.count:
                return True
        return False

    try:
        if _process_batch(pending_raws):
            return _finalize_depth_segment(
                run_paths=run_paths,
                snapshot=snapshot,
                connect_attempts=connect_attempts,
                reconnect_count=reconnect_count,
                alignment_break_count=alignment_break_count,
                message_count=message_count,
                clean_count=clean_count,
                quarantined_count=quarantined_count,
                quality_gate=quality_gate,
                parquet_sink=parquet_sink,
                metrics_sink=metrics_sink,
            )

        while message_count < args.count and not alignment_broken:
            try:
                async for message in websocket:
                    payload = json.loads(message)
                    if not _is_binance_depth_payload(payload):
                        continue
                    raw = RawMessage(source=config.source, received_at=utc_now(), payload=payload)
                    if _process_batch([raw]):
                        break
                    reconnect_attempts = 0  # any successful frame resets the retry budget
                # Stream returned (or break above). If count reached, exit outer loop.
                if message_count >= args.count:
                    break
                # Clean close — try to reconnect-in-place reusing the existing snapshot anchor.
                await connection.__aexit__(None, None, None)
                connection = None
                websocket = None
                logger.warning(
                    "binance depth websocket closed cleanly; reconnecting-in-place source=binance_depth run=%s",
                    run_paths.base.name,
                )
            except Exception as exc:  # noqa: BLE001
                try:
                    if connection is not None:
                        await connection.__aexit__(type(exc), exc, exc.__traceback__)
                except Exception:
                    pass
                connection = None
                websocket = None
                if not _is_retryable_connect_error(exc):
                    raise
                reconnect_attempts += 1
                if reconnect_attempts >= max_attempts:
                    raise
                delay = _backoff_delay(
                    attempt=reconnect_attempts,
                    base=retry_backoff_seconds,
                    cap=max_backoff_seconds,
                )
                logger.warning(
                    "binance depth websocket reconnect run=%s attempt=%d delay=%.2fs error=%s",
                    run_paths.base.name,
                    reconnect_attempts,
                    delay,
                    exc,
                )
                await asyncio.sleep(delay)

            # Reopen WS + resubscribe, NO new REST snapshot. We try alignment against the
            # existing snapshot anchor — if the post-disconnect window still aligns, we stay
            # in the same run. If alignment is broken, we end the segment cleanly so the
            # next segment opens a fresh run with a fresh snapshot (replay invariants
            # require one snapshot anchor per run dir).
            try:
                connection, websocket, buffered_raws = await _reopen_binance_depth_connection(
                    websockets=websockets,
                    websocket_url=str(config.websocket_url),
                    product=str(config.product),
                    channel=str(config.channel),
                    resubscribe_buffer_seconds=resubscribe_buffer_seconds,
                )
            except Exception as exc:  # noqa: BLE001
                if not _is_retryable_connect_error(exc):
                    raise
                reconnect_attempts += 1
                if reconnect_attempts >= max_attempts:
                    raise
                delay = _backoff_delay(
                    attempt=reconnect_attempts,
                    base=retry_backoff_seconds,
                    cap=max_backoff_seconds,
                )
                logger.warning(
                    "binance depth resubscribe retry run=%s attempt=%d delay=%.2fs error=%s",
                    run_paths.base.name,
                    reconnect_attempts,
                    delay,
                    exc,
                )
                await asyncio.sleep(delay)
                continue

            reconnect_count += 1
            reconnect_attempts = 0

            # Filter out events we already saw (u <= last seen). For the unprocessed-events
            # case (last_seen_final_update_id == snapshot_last_update_id) this is identical
            # to the snapshot-anchor alignment; after we've processed events it advances.
            aligned = _align_binance_buffered_events(buffered_raws, last_seen_final_update_id)
            if not _post_reconnect_alignment_holds(aligned, last_seen_final_update_id):
                alignment_break_count += 1
                alignment_broken = True
                logger.warning(
                    "binance depth post-reconnect alignment broken run=%s "
                    "last_seen_final_update_id=%d first_window=%s; ending segment for fresh snapshot",
                    run_paths.base.name,
                    last_seen_final_update_id,
                    _binance_update_window(aligned[0].payload) if aligned else None,
                )
                await connection.__aexit__(None, None, None)
                connection = None
                websocket = None
                break

            if _process_batch(aligned):
                break
    finally:
        if connection is not None:
            try:
                await connection.__aexit__(None, None, None)
            except Exception:  # noqa: BLE001
                pass

    return _finalize_depth_segment(
        run_paths=run_paths,
        snapshot=snapshot,
        connect_attempts=connect_attempts,
        reconnect_count=reconnect_count,
        alignment_break_count=alignment_break_count,
        message_count=message_count,
        clean_count=clean_count,
        quarantined_count=quarantined_count,
        quality_gate=quality_gate,
        parquet_sink=parquet_sink,
        metrics_sink=metrics_sink,
    )


def _finalize_depth_segment(
    *,
    run_paths,
    snapshot: dict[str, object],
    connect_attempts: int,
    reconnect_count: int,
    alignment_break_count: int,
    message_count: int,
    clean_count: int,
    quarantined_count: int,
    quality_gate: MetadataQualityGate,
    parquet_sink: ParquetDatasetSink,
    metrics_sink: JsonlSink,
) -> dict[str, object]:
    parquet_sink.flush()
    events_path = run_paths.base / "clean" / "events.jsonl"
    if events_path.exists():
        replay_summary = replay_depth_run(run_paths.base, write_summary=True)
        replayable = replay_summary.replayable
        findings = list(replay_summary.findings)
        summary_path = replay_summary.summary_path
    else:
        # Segment ended with zero clean events (e.g., immediate alignment break on
        # reconnect). No data to replay — flag the run as unreplayable so downstream
        # quarantine + promotion treat it as a no-op rather than crashing.
        replayable = False
        findings = ["no_clean_events"]
        summary_path = None
    metrics_sink.write(
        {
            "raw_messages": message_count,
            "clean_events": clean_count,
            "quarantined_events": quarantined_count,
            "reject_counts": quality_gate.metrics(),
            "snapshot_last_update_id": snapshot.get("lastUpdateId"),
            "snapshot_path": str(run_paths.base / "snapshots" / "book_snapshot.json"),
            "connect_attempts": connect_attempts,
            "reconnect_count": reconnect_count,
            "alignment_break_count": alignment_break_count,
            "replayable": replayable,
            "replay_findings": findings,
            "replay_summary_path": summary_path,
        }
    )
    return {
        "raw_messages": message_count,
        "clean_events": clean_count,
        "quarantined_events": quarantined_count,
        "run_path": str(run_paths.base),
        "connect_attempts": connect_attempts,
        "reconnect_count": reconnect_count,
        "alignment_break_count": alignment_break_count,
        "replayable": replayable,
        "replay_findings": findings,
    }


async def collect_binance_trades_segment(args: argparse.Namespace) -> dict[str, object]:
    config = CollectorConfig(
        source="binance",
        output_root=args.output_root,
        product=args.symbol,
        channel=args.channel,
        websocket_url="wss://stream.binance.com:9443/ws",
        subscription_style="binance",
        max_delay_ms=args.max_delay_ms,
        max_future_skew_ms=getattr(args, "max_future_skew_ms", 5_000),
    )
    collector = GenericWebsocketCollector(config=config)
    run_paths = prepare_run_paths(output_root=config.output_root, source="binance_trades")
    pipeline = CollectorPipeline(
        collector=collector,
        normalizer=BinanceTradeNormalizer(),
        quality_gate=QualityGate(
            max_delay_ms=config.max_delay_ms,
            max_future_skew_ms=config.max_future_skew_ms,
            session_id=run_paths.base.name,
        ),
        run_paths=run_paths,
        normalized_root=default_normalized_root("trades"),
    )
    summary = await pipeline.run(limit=args.count)
    return {
        "raw_messages": summary.raw_messages,
        "clean_events": summary.clean_events,
        "quarantined_events": summary.quarantined_events,
        "run_path": str(run_paths.base),
    }


def run_binance_depth_worker(args: argparse.Namespace) -> None:
    _run_segmented_worker(
        args=args,
        default_worker_name="binance-depth-worker",
        worker_type="binance-depth-worker",
        build_segment_args=lambda source_args: SimpleNamespace(
            symbol=source_args.symbol,
            speed=source_args.speed,
            count=source_args.segment_count,
            output_root=source_args.output_root,
            snapshot_limit=source_args.snapshot_limit,
            snapshot_base_url=source_args.snapshot_base_url,
            connect_retries=source_args.connect_retries,
            retry_backoff_seconds=source_args.retry_backoff_seconds,
            max_backoff_seconds=getattr(source_args, "max_backoff_seconds", 60.0),
        ),
        collect_segment=collect_binance_depth_segment,
        progress_message=lambda segment_index, summary: (
            "binance depth segment finished: "
            f"segment={segment_index} replayable={summary['replayable']} "
            f"connect_attempts={summary['connect_attempts']} "
            f"reconnects={summary.get('reconnect_count', 0)} "
            f"run_path={summary['run_path']}"
        ),
    )


def run_binance_trades_worker(args: argparse.Namespace) -> None:
    _run_segmented_worker(
        args=args,
        default_worker_name="binance-trades-worker",
        worker_type="binance-trades-worker",
        build_segment_args=lambda source_args: SimpleNamespace(
            symbol=source_args.symbol,
            channel=source_args.channel,
            count=source_args.segment_count,
            output_root=source_args.output_root,
            max_delay_ms=source_args.max_delay_ms,
            max_future_skew_ms=getattr(source_args, "max_future_skew_ms", 5_000),
        ),
        collect_segment=collect_binance_trades_segment,
        progress_message=lambda segment_index, summary: (
            "binance trades segment finished: "
            f"segment={segment_index} clean_events={summary['clean_events']} run_path={summary['run_path']}"
        ),
    )


def _run_segmented_worker(
    *,
    args: argparse.Namespace,
    default_worker_name: str,
    worker_type: str,
    build_segment_args,
    collect_segment,
    progress_message,
) -> None:
    worker_name = str(getattr(args, "worker_name", default_worker_name) or default_worker_name)
    runtime = StandaloneWorkerRuntime(
        args.ops_root,
        worker_name=worker_name,
        worker_type=worker_type,
        venue="binance",
        symbol=str(args.symbol).upper(),
        heartbeat_interval_seconds=float(getattr(args, "heartbeat_interval_seconds", 30.0)),
    )
    completed_segments = 0
    last_run_path: str | None = None
    with StandaloneWorkerLock(args.ops_root, worker_name=worker_name):
        runtime.record_event("worker_started", details={"max_segments": args.max_segments, "output_root": str(args.output_root)})
        runtime.write_heartbeat(status="idle", last_segment_index=0)
        try:
            while args.max_segments is None or completed_segments < args.max_segments:
                segment_index = completed_segments + 1
                segment_started_at = utc_now()
                stop_event, heartbeat_thread = runtime.start_segment_heartbeat(
                    segment_index=segment_index,
                    started_at=segment_started_at,
                    last_segment_index=completed_segments,
                    last_run_path=last_run_path,
                )
                try:
                    summary = asyncio.run(collect_segment(build_segment_args(args)))
                except Exception as exc:
                    runtime.record_event("segment_error", details={"segment_index": segment_index, "error": str(exc)})
                    runtime.write_heartbeat(
                        status="error",
                        message=str(exc),
                        last_segment_index=completed_segments,
                        current_segment_index=segment_index,
                        current_segment_started_at=segment_started_at,
                    )
                    raise
                finally:
                    stop_event.set()
                    heartbeat_thread.join(timeout=max(1.0, float(args.heartbeat_interval_seconds) + 1.0))
                completed_segments += 1
                last_run_path = str(summary.get("run_path")) if summary.get("run_path") is not None else None
                runtime.record_event("segment_complete", details={"segment_index": completed_segments, **summary})
                runtime.write_heartbeat(status="idle", last_segment_index=completed_segments, last_run_path=last_run_path)
                print(progress_message(completed_segments, summary))
                if args.max_segments is not None and completed_segments >= args.max_segments:
                    break
                if args.cooldown_seconds > 0:
                    time.sleep(args.cooldown_seconds)
        except KeyboardInterrupt:
            runtime.record_event("worker_stopped", details={"reason": "keyboard_interrupt", "completed_segments": completed_segments})
            runtime.write_heartbeat(status="stopped", last_segment_index=completed_segments, last_run_path=last_run_path)
            raise
        except Exception:
            runtime.record_event("worker_crashed", details={"completed_segments": completed_segments})
            raise
        else:
            runtime.record_event("worker_stopped", details={"reason": "completed", "completed_segments": completed_segments})
            runtime.write_heartbeat(status="stopped", last_segment_index=completed_segments, last_run_path=last_run_path)


def run_ops_runner(args: argparse.Namespace) -> None:
    jobs = load_ops_config(args.config)
    if not jobs:
        raise ValueError(f"no enabled jobs found in ops config: {args.config}")
    with OpsRunnerLock(args.ops_root, runner_name="market-data-plant"):
        runner = OpsRunner(args.ops_root, runner_name="market-data-plant", poll_seconds=args.poll_seconds)
        executed = runner.run(jobs, execute_job=_execute_ops_job, max_runs=args.max_runs, stop_on_error=args.stop_on_error)
    print(f"ops runner finished: {executed} job runs -> {args.ops_root}")


def _execute_ops_job(job: JobSpec) -> JobExecutionResult | str | None:
    args = _job_args(job)
    if job.job_type == "mock":
        asyncio.run(run_mock(args))
        return "mock completed"
    if job.job_type == "binance-depth-worker":
        run_binance_depth_worker(args)
        return "binance depth worker completed"
    if job.job_type == "binance-trades-worker":
        run_binance_trades_worker(args)
        return "binance trades worker completed"
    if job.job_type == "book-sync-health":
        run_book_sync_health(args)
        return "book sync health completed"
    if job.job_type == "backfill-replay":
        run_backfill_replay(args)
        return "backfill replay completed"
    if job.job_type == "quarantine-runs":
        run_quarantine_runs(args)
        return "quarantine completed"
    if job.job_type == "promote-replayable":
        run_promote_replayable(args)
        return "promotion completed"
    if job.job_type == "research-manifest":
        run_research_manifest(args)
        return "research manifest completed"
    if job.job_type == "cleanup":
        run_cleanup_command(args)
        return "cleanup completed"
    raise ValueError(f"Unsupported job_type: {job.job_type}")


def _job_args(job: JobSpec) -> SimpleNamespace:
    raw_args = dict(job.args)
    if "output_root" in raw_args:
        raw_args["output_root"] = Path(raw_args["output_root"])
    if job.job_type == "mock":
        return SimpleNamespace(count=raw_args.get("count", 25), output_root=raw_args.get("output_root", default_output_root()), product=raw_args.get("product", "BTC-USD"))
    if job.job_type == "binance-depth-worker":
        return SimpleNamespace(
            symbol=raw_args.get("symbol", "btcusdt"),
            speed=raw_args.get("speed", "100ms"),
            segment_count=raw_args.get("segment_count", 5000),
            max_segments=raw_args.get("max_segments"),
            cooldown_seconds=raw_args.get("cooldown_seconds", 1.0),
            output_root=raw_args.get("output_root", default_output_root()),
            ops_root=Path(raw_args.get("ops_root", default_ops_root())),
            worker_name=raw_args.get("worker_name", "binance-depth-worker"),
            heartbeat_interval_seconds=raw_args.get("heartbeat_interval_seconds", 30.0),
            snapshot_limit=raw_args.get("snapshot_limit", 1000),
            connect_retries=raw_args.get("connect_retries", 3),
            retry_backoff_seconds=raw_args.get("retry_backoff_seconds", 2.0),
            snapshot_base_url=raw_args.get("snapshot_base_url", "https://api.binance.com/api/v3/depth"),
        )
    if job.job_type == "binance-trades-worker":
        return SimpleNamespace(
            symbol=raw_args.get("symbol", "btcusdt"),
            channel=raw_args.get("channel", "trade"),
            segment_count=raw_args.get("segment_count", 5000),
            max_segments=raw_args.get("max_segments"),
            cooldown_seconds=raw_args.get("cooldown_seconds", 1.0),
            output_root=raw_args.get("output_root", default_output_root()),
            ops_root=Path(raw_args.get("ops_root", default_ops_root())),
            worker_name=raw_args.get("worker_name", "binance-trades-worker"),
            heartbeat_interval_seconds=raw_args.get("heartbeat_interval_seconds", 30.0),
            max_delay_ms=raw_args.get("max_delay_ms", 60_000),
            max_future_skew_ms=raw_args.get("max_future_skew_ms", 5_000),
        )
    if job.job_type in {"book-sync-health", "backfill-replay"}:
        return SimpleNamespace(
            source_root=Path(raw_args.get("source_root", default_archive_root() / "raw" / "market" / "binance_depth")),
            limit=raw_args.get("limit", 50),
            max_age_hours=raw_args.get("max_age_hours", 24.0),
            overwrite=raw_args.get("overwrite", False),
            format=raw_args.get("format", "text"),
        )
    if job.job_type == "quarantine-runs":
        return SimpleNamespace(
            source_root=Path(raw_args.get("source_root", default_archive_root() / "raw" / "market" / "binance_depth")),
            quarantine_root=Path(raw_args.get("quarantine_root", default_archive_root() / "quarantine" / "market" / "binance_depth")),
            limit=raw_args.get("limit", 50),
            max_age_hours=raw_args.get("max_age_hours", 24.0),
            format=raw_args.get("format", "text"),
        )
    if job.job_type == "promote-replayable":
        return SimpleNamespace(
            source_root=Path(raw_args.get("source_root", default_archive_root() / "raw" / "market" / "binance_depth")),
            target_root=Path(raw_args.get("target_root", default_curated_root("market_replayable"))),
            quarantine_index=Path(raw_args.get("quarantine_index", default_archive_root() / "quarantine" / "market" / "binance_depth" / "_quarantine_index.jsonl")),
            limit=raw_args.get("limit", 50),
            max_age_hours=raw_args.get("max_age_hours", 24.0),
            format=raw_args.get("format", "text"),
        )
    if job.job_type == "research-manifest":
        return SimpleNamespace(
            archive_root=Path(raw_args.get("archive_root", default_archive_root())),
            output_root=Path(raw_args.get("output_root", DEFAULT_MANIFEST_ROOT)),
            current_date=raw_args.get("current_date"),
            format=raw_args.get("format", "text"),
        )
    if job.job_type == "cleanup":
        return SimpleNamespace(
            archive_root=Path(raw_args.get("archive_root", default_archive_root())),
            raw_days=raw_args.get("raw_days", 14),
            raw_policy=raw_args.get("raw_policy", []),
            apply=raw_args.get("apply", False),
            format=raw_args.get("format", "text"),
        )
    raise ValueError(f"Unsupported job_type: {job.job_type}")


def run_health(args: argparse.Namespace) -> None:
    jobs = load_ops_config(args.config) if args.config and args.config.exists() else None
    report = build_health_report(
        ops_root=args.ops_root,
        jobs=jobs,
        stale_after_seconds=args.stale_after_seconds,
        job_stale_multiplier=args.job_stale_multiplier,
        recent_failure_window_seconds=args.recent_failure_window_seconds,
        min_disk_free_gb=args.min_disk_free_gb,
    )
    if args.format == "json":
        print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
        return
    print(f"status={report.status}")
    print(f"heartbeat_age_seconds={report.heartbeat_age_seconds}")
    print(f"disk_free_gb={report.disk_free_gb:.2f}" if report.disk_free_gb is not None else "disk_free_gb=None")
    print(f"findings={','.join(report.findings) if report.findings else 'none'}")


def run_cleanup_command(args: argparse.Namespace) -> None:
    report = run_cleanup(
        archive_root=args.archive_root,
        raw_days=args.raw_days,
        raw_policies=_parse_raw_policies(args.raw_policy),
        apply=args.apply,
    )
    if args.format == "json":
        print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
        return
    print(f"mode={report.mode}")
    print(f"candidate_count={report.candidate_count}")
    print(f"total_bytes={report.total_bytes}")
    print(f"removed_count={report.removed_count}")
    print(f"removed_bytes={report.removed_bytes}")


def run_ops_prune_stale_workers(args: argparse.Namespace) -> None:
    jobs = load_ops_config(args.config) if args.config and args.config.exists() else None
    managed_names: set[str] = set()
    for job in jobs or []:
        if job.job_type == "binance-depth-worker":
            managed_names.add(str(job.args.get("worker_name") or "binance-depth-worker"))
        elif job.job_type == "binance-trades-worker":
            managed_names.add(str(job.args.get("worker_name") or "binance-trades-worker"))
    report = prune_stale_worker_artifacts(
        ops_root=args.ops_root,
        stale_after_days=args.stale_after_days,
        apply=args.apply,
        managed_worker_names=managed_names,
    )
    if args.format == "json":
        print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
        return
    print(f"mode={report.mode}")
    print(f"candidate_count={report.candidate_count}")
    print(f"moved_count={report.moved_count}")
    print(f"archive_root={report.archive_root}")
    print(f"findings={','.join(report.findings) if report.findings else 'none'}")
    for candidate in report.candidates:
        print(f"candidate={candidate.worker_name} status={candidate.status} paths={len(candidate.related_paths)}")


def run_replay(args: argparse.Namespace) -> None:
    summary = replay_depth_run(run_path=args.run_path, max_levels=args.max_levels, write_summary=True)
    if args.format == "json":
        print(json.dumps(summary.to_dict(), indent=2, sort_keys=True))
        return
    print(f"status={'ok' if summary.replayable else 'warn'}")
    print(f"replayable={summary.replayable}")
    print(f"event_count={summary.event_count}")
    print(f"findings={','.join(summary.findings) if summary.findings else 'none'}")


def run_book_sync_health(args: argparse.Namespace) -> None:
    report = build_book_sync_health_report(source_root=args.source_root, limit=args.limit, max_age_hours=args.max_age_hours)
    if args.format == "json":
        print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
        return
    print(f"status={report.status}")
    print(f"scanned_run_count={report.scanned_run_count}")
    print(f"replayable_run_count={report.replayable_run_count}")
    print(f"findings={','.join(report.findings) if report.findings else 'none'}")


def run_backfill_replay(args: argparse.Namespace) -> None:
    report = backfill_replay_summaries(args.source_root, limit=args.limit, max_age_hours=args.max_age_hours, overwrite=args.overwrite)
    if args.format == "json":
        print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
        return
    print(f"status={report.status}")
    print(f"created_count={report.created_count}")
    print(f"updated_count={report.updated_count}")
    print(f"failed_count={report.failed_count}")
    print(f"findings={','.join(report.findings) if report.findings else 'none'}")


def run_quarantine_runs(args: argparse.Namespace) -> None:
    report = quarantine_bad_runs(args.source_root, quarantine_root=args.quarantine_root, limit=args.limit, max_age_hours=args.max_age_hours)
    if args.format == "json":
        print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
        return
    print(f"status={report.status}")
    print(f"quarantined_count={report.quarantined_count}")
    print(f"findings={','.join(report.findings) if report.findings else 'none'}")


def run_promote_replayable(args: argparse.Namespace) -> None:
    report = promote_replayable_runs(
        source_root=args.source_root,
        target_root=args.target_root,
        limit=args.limit,
        max_age_hours=args.max_age_hours,
        quarantine_index_path=args.quarantine_index,
    )
    if args.format == "json":
        print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
        return
    print(f"status={report.status}")
    print(f"promoted_run_count={report.promoted_run_count}")
    print(f"promoted_row_count={report.promoted_row_count}")
    print(f"findings={','.join(report.findings) if report.findings else 'none'}")


def run_research_manifest(args: argparse.Namespace) -> None:
    manifest = generate_research_manifest(
        archive_root=args.archive_root,
        output_root=args.output_root,
        current_date=date.fromisoformat(args.current_date) if args.current_date else None,
    )
    if args.format == "json":
        print(json.dumps(manifest, indent=2, sort_keys=True))
        return
    summary = manifest["summary"]
    outputs = manifest["output_paths"]
    print("status=ok")
    print(f"ready_day_count={summary['ready_day_count']}")
    print(f"building_day_count={summary['building_day_count']}")
    print(f"total_curated_market_rows={summary['total_curated_market_rows']}")
    print(f"latest_json={outputs['latest_json']}")
    print(f"latest_markdown={outputs['latest_markdown']}")


def run_state() -> None:
    archive_root = default_archive_root()
    output_root = default_output_root()
    normalized_market_root = default_normalized_root("market")
    normalized_trades_root = default_normalized_root("trades")
    curated_market_root = default_curated_root("market_replayable")
    ops_root = default_ops_root()
    print(f"archive_root={archive_root}")
    print(f"default_output_root={output_root}")
    print(f"normalized_market_root={normalized_market_root}")
    print(f"normalized_trades_root={normalized_trades_root}")
    print(f"curated_market_root={curated_market_root}")
    print(f"ops_root={ops_root}")
    print(f"archive_exists={archive_root.exists()}")
    if archive_root.exists():
        usage = shutil.disk_usage(archive_root.drive or archive_root.anchor)
        print(f"archive_disk_total_tb={usage.total / (1024 ** 4):.2f}")
        print(f"archive_disk_used_tb={usage.used / (1024 ** 4):.2f}")
        print(f"archive_disk_free_tb={usage.free / (1024 ** 4):.2f}")


async def _capture_binance_snapshot_and_buffer(
    websocket: object,
    *,
    product: str,
    snapshot_limit: int,
    snapshot_base_url: str,
) -> tuple[dict[str, object], list[RawMessage]]:
    snapshot_task = asyncio.create_task(
        asyncio.to_thread(fetch_binance_order_book_snapshot, symbol=product, limit=snapshot_limit, base_url=snapshot_base_url)
    )
    buffered: list[RawMessage] = []
    while not snapshot_task.done():
        try:
            message = await asyncio.wait_for(websocket.recv(), timeout=0.05)
        except asyncio.TimeoutError:
            continue
        payload = json.loads(message)
        if _is_binance_depth_payload(payload):
            buffered.append(RawMessage(source="binance", received_at=utc_now(), payload=payload))
    snapshot = await snapshot_task
    snapshot_last_update_id = int(snapshot.get("lastUpdateId", 0))
    while buffered:
        first_window = _binance_update_window(buffered[0].payload)
        if first_window is None:
            buffered.pop(0)
            continue
        if snapshot_last_update_id >= first_window[0]:
            break
        snapshot = await asyncio.to_thread(fetch_binance_order_book_snapshot, symbol=product, limit=snapshot_limit, base_url=snapshot_base_url)
        snapshot_last_update_id = int(snapshot.get("lastUpdateId", 0))
    return snapshot, _align_binance_buffered_events(buffered, snapshot_last_update_id)


async def _open_binance_depth_connection(
    *,
    websockets: object,
    websocket_url: str,
    product: str,
    channel: str,
    snapshot_limit: int,
    snapshot_base_url: str,
    connect_retries: int,
    retry_backoff_seconds: float,
) -> tuple[object, object, dict[str, object], list[RawMessage], int]:
    last_error: Exception | None = None
    for attempt in range(1, connect_retries + 1):
        connection = websockets.connect(websocket_url)
        websocket = None
        try:
            websocket = await connection.__aenter__()
            await websocket.send(json.dumps({"method": "SUBSCRIBE", "params": [f"{product.lower()}@{channel}"], "id": 1}))
            snapshot, pending_raws = await _capture_binance_snapshot_and_buffer(
                websocket,
                product=product,
                snapshot_limit=snapshot_limit,
                snapshot_base_url=snapshot_base_url,
            )
            return connection, websocket, snapshot, pending_raws, attempt
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if websocket is not None:
                await connection.__aexit__(type(exc), exc, exc.__traceback__)
            if attempt >= connect_retries or not _is_retryable_connect_error(exc):
                break
            await asyncio.sleep(max(0.0, retry_backoff_seconds) * attempt)
    raise RuntimeError(f"binance depth connect/open failed after {connect_retries} attempt(s): {last_error}") from last_error


async def _reopen_binance_depth_connection(
    *,
    websockets: object,
    websocket_url: str,
    product: str,
    channel: str,
    resubscribe_buffer_seconds: float = 1.0,
) -> tuple[object, object, list[RawMessage]]:
    """Open a new WS + resubscribe to depth, WITHOUT fetching a fresh REST snapshot.

    The caller re-uses its existing snapshot anchor and decides via
    `_post_reconnect_alignment_holds` whether the post-reconnect window still bridges that
    snapshot. A single attempt; the caller handles retry/backoff.
    """
    connection = websockets.connect(websocket_url)
    websocket = None
    try:
        websocket = await connection.__aenter__()
        await websocket.send(
            json.dumps({"method": "SUBSCRIBE", "params": [f"{product.lower()}@{channel}"], "id": 1})
        )
        buffered: list[RawMessage] = []
        loop = asyncio.get_event_loop()
        deadline = loop.time() + max(0.0, resubscribe_buffer_seconds)
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=remaining)
            except asyncio.TimeoutError:
                break
            try:
                payload = json.loads(message)
            except (TypeError, ValueError):
                continue
            if _is_binance_depth_payload(payload):
                buffered.append(RawMessage(source="binance", received_at=utc_now(), payload=payload))
        return connection, websocket, buffered
    except Exception as exc:
        if websocket is not None:
            try:
                await connection.__aexit__(type(exc), exc, exc.__traceback__)
            except Exception:
                pass
        raise


def _post_reconnect_alignment_holds(
    aligned: list[RawMessage], snapshot_last_update_id: int
) -> bool:
    """Return True if the first aligned event after reconnect bridges the existing snapshot
    (no gap), False if there's a sequence gap that would corrupt the run.

    Empty `aligned` returns True — no events seen in the resubscribe window means we
    couldn't observe a gap yet, so we keep streaming and let the normal flow handle the
    next event. If the next event is itself a gap, replay will catch it downstream.
    """
    if not aligned:
        return True
    first_window = _binance_update_window(aligned[0].payload)
    if first_window is None:
        return True
    return first_window[0] <= snapshot_last_update_id + 1


def _is_binance_depth_payload(payload: object) -> bool:
    if not isinstance(payload, dict):
        return False
    if payload.get("result") is None and "id" in payload:
        return False
    return payload.get("e") is not None or "data" in payload


def _binance_update_window(payload: dict[str, object]) -> tuple[int, int] | None:
    event = payload.get("data", payload)
    if not isinstance(event, dict):
        return None
    try:
        return int(event["U"]), int(event["u"])
    except (KeyError, TypeError, ValueError):
        return None


def _align_binance_buffered_events(buffered: list[RawMessage], snapshot_last_update_id: int) -> list[RawMessage]:
    aligned: list[RawMessage] = []
    for raw in buffered:
        window = _binance_update_window(raw.payload)
        if window is not None and window[1] > snapshot_last_update_id:
            aligned.append(raw)
    return aligned


def _process_binance_depth_raw(
    *,
    raw: RawMessage,
    raw_sink: JsonlSink,
    clean_sink: JsonlSink,
    quarantine_sink: JsonlSink,
    parquet_sink: ParquetDatasetSink,
    normalizer: BinanceDepthNormalizer,
    quality_gate: MetadataQualityGate,
    message_count: int,
    clean_count: int,
    quarantined_count: int,
) -> tuple[int, int, int]:
    message_count += 1
    raw_sink.write(raw.to_dict())
    normalized = normalizer.normalize(raw)
    verdict = quality_gate.validate(normalized)
    if verdict.accepted:
        clean_count += 1
        row = normalized.to_dict()
        clean_sink.write(row)
        parquet_sink.write(row)
    else:
        quarantined_count += 1
        row = normalized.to_dict()
        row["reasons"] = verdict.reasons
        quarantine_sink.write(row)
    return message_count, clean_count, quarantined_count


def _parse_raw_policies(entries: list[str]) -> dict[str, int]:
    policies: dict[str, int] = {}
    for entry in entries:
        key, separator, value = entry.partition("=")
        if separator != "=":
            raise ValueError(f"invalid raw policy '{entry}'; expected DATASET/SOURCE=DAYS")
        days = int(value)
        if days < 0:
            raise ValueError(f"invalid raw policy '{entry}'; DAYS must be non-negative")
        policies[key] = days
    return policies


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "mock":
        asyncio.run(run_mock(args))
    elif args.command == "binance-depth-worker":
        run_binance_depth_worker(args)
    elif args.command == "binance-trades-worker":
        run_binance_trades_worker(args)
    elif args.command == "ops-runner":
        run_ops_runner(args)
    elif args.command == "health":
        run_health(args)
    elif args.command == "cleanup":
        run_cleanup_command(args)
    elif args.command == "ops-prune-stale-workers":
        run_ops_prune_stale_workers(args)
    elif args.command == "replay":
        run_replay(args)
    elif args.command == "book-sync-health":
        run_book_sync_health(args)
    elif args.command == "backfill-replay":
        run_backfill_replay(args)
    elif args.command == "quarantine-runs":
        run_quarantine_runs(args)
    elif args.command == "promote-replayable":
        run_promote_replayable(args)
    elif args.command == "research-manifest":
        run_research_manifest(args)
    elif args.command == "state":
        run_state()
    else:
        parser.error(f"unsupported command: {args.command}")


if __name__ == "__main__":
    main()
