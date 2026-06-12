from __future__ import annotations

import argparse
import asyncio
import json
import logging
import shutil
import subprocess
import sys
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

from .collectors.generic_ws import (
    GenericWebsocketCollector,
    _backoff_delay,
    _is_retryable_connect_error,
)
from .collectors.kalshi import (
    DEFAULT_KALSHI_CATEGORY,
    DEFAULT_KALSHI_DURATION_SECONDS,
    DEFAULT_KALSHI_MARKETS_PER_SERIES,
    DEFAULT_KALSHI_POLL_INTERVAL_SECONDS,
    DEFAULT_KALSHI_STALE_AFTER_SECONDS,
    DEFAULT_KALSHI_TARGET_ASSETS,
    DEFAULT_KALSHI_TARGET_FREQUENCIES,
    collect_kalshi_crypto_quotes,
    default_kalshi_normalized_root,
    default_kalshi_output_root,
    discover_kalshi_crypto_markets,
    summarize_kalshi_quote_rows,
)
from .collectors.mexc import (
    MEXC_DEALS_CHANNEL,
    MEXC_LIMIT_DEPTH_CHANNEL,
    MEXC_PING_INTERVAL_SECONDS,
    MEXC_PING_MESSAGE,
    MEXC_WS_URL,
    build_deals_topic,
    build_limit_depth_topic,
    decode_mexc_frame,
)
from .collectors.mock_l3 import MockL3Collector
from .collectors.binance_futures_rest import (
    aggtrades_cursor_path,
    aggtrades_resume_from_id,
    make_aggtrades_poll,
    make_depth_poll,
    make_funding_poll,
    max_agg_id_in_events,
    max_agg_id_in_recent_runs,
    read_aggtrades_cursor,
    write_aggtrades_cursor,
)
from .collectors.rest_poll import RestPollingCollector
from .config import (
    CollectorConfig,
    default_archive_root,
    default_curated_root,
    default_normalized_root,
    default_ops_root,
    default_output_root,
)
from .market_normalizers import (
    BinanceDepthNormalizer,
    BinanceFuturesFundingNormalizer,
    BinanceTradeNormalizer,
    BybitDepthNormalizer,
    BybitTradeNormalizer,
    CoinbaseDepthNormalizer,
    CoinbaseTradeNormalizer,
    KrakenDepthNormalizer,
    KrakenTradeNormalizer,
    MexcDepthNormalizer,
    MexcTradeNormalizer,
    OkxDepthNormalizer,
    OkxTradeNormalizer,
)
from .market_snapshots import fetch_binance_order_book_snapshot, write_snapshot_file
from .models import RawMessage, utc_now
from .normalizer import GenericL3Normalizer
from .ops import (
    COLLECTOR_JOB_TYPES,
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
from .pipeline import (
    DEFAULT_FSYNC_INTERVAL_EVENTS,
    DEFAULT_FSYNC_INTERVAL_MS,
    CollectorPipeline,
)
from .offload import OffloadLaneSpec, offload_accounted_runs
from .promotion import promote_replayable_runs
from .quality import MetadataQualityGate, QualityGate
from .quarantine import quarantine_bad_runs
from .replay import (
    backfill_replay_summaries,
    build_book_sync_health_report,
    replay_depth_run,
    replay_depth_stream_run,
    replay_funding_run,
    replay_trades_run,
    replay_trades_stream_run,
)
from .research_manifest import DEFAULT_MANIFEST_ROOT, generate_research_manifest
from .storage import JsonlSink, ParquetDatasetSink, prepare_run_paths

logger = logging.getLogger(__name__)


def _add_fsync_batching_args(parser: argparse.ArgumentParser) -> None:
    """Expose the batched-fsync cadence on a worker subparser. With fsync enabled the
    raw/clean/quarantine JSONL is flushed every line (no torn tail on a hard kill) but
    fsynced only every N events OR every M ms, whichever comes first — so a high-tick
    lane isn't throttled below the feed rate by per-line fsync latency. Defaults are the
    pipeline's safe batching; lower them toward 1 / 0 for stricter durability."""
    parser.add_argument(
        "--fsync-interval-events",
        type=int,
        default=DEFAULT_FSYNC_INTERVAL_EVENTS,
        help="Flush+fsync the data JSONL at least this often (in events). Default "
        f"{DEFAULT_FSYNC_INTERVAL_EVENTS}; 1 = fsync every event.",
    )
    parser.add_argument(
        "--fsync-interval-ms",
        type=float,
        default=DEFAULT_FSYNC_INTERVAL_MS,
        help="Also flush+fsync the data JSONL at least this often (in milliseconds). "
        f"Default {DEFAULT_FSYNC_INTERVAL_MS:g}; 0 = disable the time bound.",
    )


def _fsync_intervals(args: argparse.Namespace) -> tuple[int, float]:
    """Resolve the (events, ms) batched-fsync cadence from a namespace, mapping a missing
    or None value (e.g. an ops job that didn't set the knob) to the safe default. Keeping
    the resolution in one place means every CollectorPipeline construction site batches
    identically without each call repeating the None handling."""
    events = getattr(args, "fsync_interval_events", None)
    ms = getattr(args, "fsync_interval_ms", None)
    return (
        DEFAULT_FSYNC_INTERVAL_EVENTS if events is None else int(events),
        DEFAULT_FSYNC_INTERVAL_MS if ms is None else float(ms),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="market-data-plant")
    subparsers = parser.add_subparsers(dest="command", required=True)

    mock_parser = subparsers.add_parser("mock", help="Run a bounded local mock ingest")
    mock_parser.add_argument("--count", type=int, default=25)
    mock_parser.add_argument("--output-root", type=Path, default=default_output_root())
    mock_parser.add_argument("--product", default="BTC-USD")
    mock_parser.add_argument(
        "--delay-ms",
        type=float,
        default=0.0,
        help="Per-event delay (ms). Used by durability tests to slow the stream "
        "enough that SIGKILL can land mid-write.",
    )

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
    depth_parser.add_argument(
        "--snapshot-anchor-timeout-seconds",
        type=float,
        default=10.0,
        help="Max seconds to keep buffering deltas while anchoring the REST snapshot "
        "before giving up. The diff stream emits every ~100ms, so the default is a wide "
        "safety net; lower only for tests.",
    )
    depth_parser.add_argument("--snapshot-base-url", default="https://api.binance.com/api/v3/depth")
    depth_parser.add_argument(
        "--source-suffix",
        default="",
        help="Optional per-instrument lane suffix. When non-empty, runs go to "
        "<output_root>/binance_depth_<suffix>/<timestamp>/ instead of "
        "binance_depth/. Leave empty to preserve the legacy single-symbol BTC layout.",
    )
    depth_parser.add_argument(
        "--rotate-at-midnight",
        action="store_true",
        help="Rotate the run directory at midnight UTC instead of at --segment-count "
        "messages. With this set, --segment-count becomes a soft cap (memory bound) "
        "rather than the primary rotation trigger. Day-bounded run dirs line up with "
        "the curated event_date partitioning downstream.",
    )

    trades_parser = subparsers.add_parser("binance-trades-worker", help="Run segmented Binance trade collection")
    trades_parser.add_argument("--symbol", default="btcusdt")
    trades_parser.add_argument("--channel", choices=["trade", "aggTrade"], default="trade")
    trades_parser.add_argument(
        "--market",
        choices=list(_BINANCE_TRADES_MARKETS),
        default="spot",
        help="Binance product type. 'spot' (default) keeps the legacy lane "
        "(binance_trades/, spot:binance:* instrument). 'futures' = USDT-M perpetual "
        "futures on fstream.binance.com: streams aggregate trades (channel forced to "
        "aggTrade), runs land in binance_perp_trades/ tagged perp:binance-futures:*.",
    )
    trades_parser.add_argument("--segment-count", type=int, default=5000)
    trades_parser.add_argument("--max-segments", type=int)
    trades_parser.add_argument("--cooldown-seconds", type=float, default=1.0)
    trades_parser.add_argument("--output-root", type=Path, default=default_output_root())
    trades_parser.add_argument("--ops-root", type=Path, default=default_ops_root())
    trades_parser.add_argument("--worker-name", default="binance-trades-worker")
    trades_parser.add_argument("--heartbeat-interval-seconds", type=float, default=30.0)
    trades_parser.add_argument("--max-delay-ms", type=int, default=60_000)
    trades_parser.add_argument("--max-future-skew-ms", type=int, default=5_000)
    trades_parser.add_argument("--max-clock-skew-ms", type=float, default=60_000.0)
    trades_parser.add_argument(
        "--source-suffix",
        default="",
        help="Optional per-instrument lane suffix. When non-empty, runs go to "
        "<output_root>/binance_trades_<suffix>/<timestamp>/ instead of "
        "binance_trades/. Leave empty to preserve the legacy single-symbol BTC layout.",
    )
    trades_parser.add_argument(
        "--rotate-at-midnight",
        action="store_true",
        help="Rotate the run directory at midnight UTC instead of at --segment-count "
        "messages. See the depth worker's help text for the same flag.",
    )
    trades_parser.add_argument(
        "--no-jsonl-fsync",
        action="store_false",
        dest="jsonl_fsync",
        default=True,
        help="Disable per-row fsync for raw/clean/quarantine JSONL writes. Use only "
        "for high-rate lanes where replay continuity is the promotion gate.",
    )
    trades_parser.add_argument(
        "--no-normalized-parquet",
        action="store_false",
        dest="normalized_parquet",
        default=True,
        help="Skip hot-path normalized Parquet writes. Raw/clean JSONL and replay "
        "summaries are still written; use for high-rate lanes that curate via replay.",
    )
    _add_fsync_batching_args(trades_parser)

    bfr_parser = subparsers.add_parser(
        "binance-futures-rest-worker",
        help="Collect Binance USDT-M futures (perp) via REST polling, for hosts where the "
        "fstream WebSocket is blocked but the fapi REST data API works.",
    )
    bfr_parser.add_argument("--symbol", default="BTCUSDT", help="Binance futures symbol, e.g. BTCUSDT.")
    bfr_parser.add_argument(
        "--stream",
        choices=list(_BINANCE_FUTURES_REST_STREAMS),
        default="trades",
        help="Which REST data to poll: 'trades' (gapless aggTrades via fromId paging -> "
        "binance_perp_trades/, perp:binance-futures:*, gap-proof sequence feed), 'depth' "
        "(per-poll full-book snapshots -> binance_perp_depth/, none_native), or 'funding' "
        "(premiumIndex mark/index/funding metric -> binance_perp_funding/, none_native).",
    )
    bfr_parser.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=1.0,
        help="Seconds between polls. Trades auto-catch-up (ignore the sleep while a page is "
        "full); depth/funding poll on this cadence.",
    )
    bfr_parser.add_argument("--page-limit", type=int, default=1000, help="aggTrades page size (trades).")
    bfr_parser.add_argument(
        "--max-resume-gap-seconds",
        type=float,
        default=21_600.0,
        help="Oldest cursor age (seconds) the trades stream will backfill from; older "
        "cursors re-anchor to live with a logged cursor_reset_stale_gap finding.",
    )
    bfr_parser.add_argument("--depth", type=int, default=1000, help="Order-book snapshot depth (depth stream).")
    bfr_parser.add_argument("--segment-count", type=int, default=5000)
    bfr_parser.add_argument("--max-segments", type=int)
    bfr_parser.add_argument("--cooldown-seconds", type=float, default=1.0)
    bfr_parser.add_argument("--output-root", type=Path, default=default_output_root())
    bfr_parser.add_argument("--ops-root", type=Path, default=default_ops_root())
    bfr_parser.add_argument("--worker-name", default="binance-futures-rest-worker")
    bfr_parser.add_argument("--heartbeat-interval-seconds", type=float, default=30.0)
    # Staleness windows default to the resume gap (6h), NOT the WS lanes' 60s: after
    # any outage >60s the cursor deliberately backfills old trades whose
    # exchange_time→received_at delay IS the outage length. A 60s gate quarantined
    # the entire backfilled window (and the replay skew gate then blocked what was
    # left), silently losing the outage window from curated on a lane whose whole
    # point is gaplessness — the dense `a` sequence, not freshness, is its proof.
    bfr_parser.add_argument("--max-delay-ms", type=int, default=21_600_000)
    bfr_parser.add_argument("--max-future-skew-ms", type=int, default=5_000)
    bfr_parser.add_argument("--max-clock-skew-ms", type=float, default=21_600_000.0)
    bfr_parser.add_argument(
        "--source-suffix",
        default="",
        help="Optional per-instrument lane suffix (e.g. ethusdt) appended to the lane dir.",
    )
    bfr_parser.add_argument("--rotate-at-midnight", action="store_true")
    bfr_parser.add_argument(
        "--no-jsonl-fsync", action="store_false", dest="jsonl_fsync", default=True
    )
    bfr_parser.add_argument(
        "--no-normalized-parquet", action="store_false", dest="normalized_parquet", default=True
    )
    _add_fsync_batching_args(bfr_parser)

    cb_trades_parser = subparsers.add_parser(
        "coinbase-trades-worker", help="Run segmented Coinbase trade collection"
    )
    cb_trades_parser.add_argument(
        "--symbol",
        default="BTC-USD",
        help="Coinbase product id, e.g. BTC-USD / ETH-USD (dash-separated).",
    )
    cb_trades_parser.add_argument(
        "--channel",
        default="matches",
        help="Coinbase channel to subscribe to. 'matches' is the public trade feed.",
    )
    cb_trades_parser.add_argument("--segment-count", type=int, default=5000)
    cb_trades_parser.add_argument("--max-segments", type=int)
    cb_trades_parser.add_argument("--cooldown-seconds", type=float, default=1.0)
    cb_trades_parser.add_argument("--output-root", type=Path, default=default_output_root())
    cb_trades_parser.add_argument("--ops-root", type=Path, default=default_ops_root())
    cb_trades_parser.add_argument("--worker-name", default="coinbase-trades-worker")
    cb_trades_parser.add_argument("--heartbeat-interval-seconds", type=float, default=30.0)
    cb_trades_parser.add_argument("--max-delay-ms", type=int, default=60_000)
    cb_trades_parser.add_argument("--max-future-skew-ms", type=int, default=5_000)
    cb_trades_parser.add_argument("--max-clock-skew-ms", type=float, default=60_000.0)
    cb_trades_parser.add_argument(
        "--source-suffix",
        default="",
        help="Optional per-instrument lane suffix. When non-empty, runs go to "
        "<output_root>/coinbase_trades_<suffix>/<timestamp>/ instead of "
        "coinbase_trades/.",
    )
    cb_trades_parser.add_argument(
        "--rotate-at-midnight",
        action="store_true",
        help="Rotate the run directory at midnight UTC instead of at --segment-count "
        "messages. See the depth worker's help text for the same flag.",
    )

    cb_depth_parser = subparsers.add_parser(
        "coinbase-depth-worker",
        help="Run segmented Coinbase level2 depth collection (non-sequence feed)",
    )
    cb_depth_parser.add_argument(
        "--symbol",
        default="BTC-USD",
        help="Coinbase product id, e.g. BTC-USD / ETH-USD (dash-separated).",
    )
    cb_depth_parser.add_argument(
        "--channel",
        default="level2_50",
        help="Coinbase depth channel. 'level2_50' is the unauthenticated public feed "
        "(emits the same in-stream `snapshot` + `l2update` frames the normalizer parses); "
        "the plain 'level2'/'level2_batch' channels now require Coinbase auth. The "
        "level2_50 snapshot is the full book (~1.4 MiB), so the collector raises the WS "
        "max frame size (CollectorConfig.max_message_bytes). This is a non-sequence "
        "('none_native') feed: replayable means structurally clean, not gap-proof "
        "(STANDARDS 4.3).",
    )
    cb_depth_parser.add_argument("--segment-count", type=int, default=5000)
    cb_depth_parser.add_argument("--max-segments", type=int)
    cb_depth_parser.add_argument("--cooldown-seconds", type=float, default=1.0)
    cb_depth_parser.add_argument("--output-root", type=Path, default=default_output_root())
    cb_depth_parser.add_argument("--ops-root", type=Path, default=default_ops_root())
    cb_depth_parser.add_argument("--worker-name", default="coinbase-depth-worker")
    cb_depth_parser.add_argument("--heartbeat-interval-seconds", type=float, default=30.0)
    cb_depth_parser.add_argument(
        "--source-suffix",
        default="",
        help="Optional per-instrument lane suffix. When non-empty, runs go to "
        "<output_root>/coinbase_depth_<suffix>/<timestamp>/ instead of "
        "coinbase_depth/.",
    )
    cb_depth_parser.add_argument(
        "--rotate-at-midnight",
        action="store_true",
        help="Rotate the run directory at midnight UTC instead of at --segment-count "
        "messages. See the depth worker's help text for the same flag.",
    )

    kraken_trades_parser = subparsers.add_parser(
        "kraken-trades-worker", help="Run segmented Kraken v2 trade collection"
    )
    kraken_trades_parser.add_argument(
        "--symbol",
        default="BTC/USD",
        help="Kraken v2 pair, e.g. BTC/USD / ETH/USD (slash-separated).",
    )
    kraken_trades_parser.add_argument(
        "--channel",
        default="trade",
        help="Kraken v2 channel. 'trade' is the public trade feed. Kraken's "
        "per-pair trade_id is a dense counter, so this is curated as a "
        "gap-proof sequence-bearing feed (STANDARDS 4.2).",
    )
    kraken_trades_parser.add_argument("--segment-count", type=int, default=5000)
    kraken_trades_parser.add_argument("--max-segments", type=int)
    kraken_trades_parser.add_argument("--cooldown-seconds", type=float, default=1.0)
    kraken_trades_parser.add_argument("--output-root", type=Path, default=default_output_root())
    kraken_trades_parser.add_argument("--ops-root", type=Path, default=default_ops_root())
    kraken_trades_parser.add_argument("--worker-name", default="kraken-trades-worker")
    kraken_trades_parser.add_argument("--heartbeat-interval-seconds", type=float, default=30.0)
    kraken_trades_parser.add_argument("--max-delay-ms", type=int, default=60_000)
    kraken_trades_parser.add_argument("--max-future-skew-ms", type=int, default=5_000)
    kraken_trades_parser.add_argument("--max-clock-skew-ms", type=float, default=60_000.0)
    kraken_trades_parser.add_argument(
        "--source-suffix",
        default="",
        help="Optional per-instrument lane suffix. When non-empty, runs go to "
        "<output_root>/kraken_trades_<suffix>/<timestamp>/ instead of "
        "kraken_trades/.",
    )
    kraken_trades_parser.add_argument(
        "--rotate-at-midnight",
        action="store_true",
        help="Rotate the run directory at midnight UTC instead of at --segment-count "
        "messages. See the depth worker's help text for the same flag.",
    )

    bybit_trades_parser = subparsers.add_parser(
        "bybit-trades-worker", help="Run segmented Bybit v5 spot trade collection (non-sequence feed)"
    )
    bybit_trades_parser.add_argument(
        "--symbol",
        default="BTCUSDT",
        help="Bybit v5 spot symbol, e.g. BTCUSDT / ETHUSDT (no separator).",
    )
    bybit_trades_parser.add_argument(
        "--channel",
        default="publicTrade",
        help="Bybit v5 channel. 'publicTrade' is the public trade feed. Bybit's "
        "trade id is a UUID (not a dense counter), so gaplessness is unprovable: "
        "this is curated as a non-sequence ('none_native') feed -- structurally "
        "clean only, NOT gap-proof (STANDARDS 4.3).",
    )
    bybit_trades_parser.add_argument(
        "--market",
        choices=list(_BYBIT_MARKETS),
        default="spot",
        help="Bybit v5 product type. 'spot' (default) keeps the legacy lane "
        "(bybit_trades/, spot:bybit:* instrument). 'linear' = USDT-perpetual futures: "
        "same publicTrade topic + curation, but the URL path is /v5/public/linear and "
        "runs land in bybit_perp_trades/ tagged perp:bybit:* so perp never mixes with spot.",
    )
    bybit_trades_parser.add_argument("--segment-count", type=int, default=5000)
    bybit_trades_parser.add_argument("--max-segments", type=int)
    bybit_trades_parser.add_argument("--cooldown-seconds", type=float, default=1.0)
    bybit_trades_parser.add_argument("--output-root", type=Path, default=default_output_root())
    bybit_trades_parser.add_argument("--ops-root", type=Path, default=default_ops_root())
    bybit_trades_parser.add_argument("--worker-name", default="bybit-trades-worker")
    bybit_trades_parser.add_argument("--heartbeat-interval-seconds", type=float, default=30.0)
    bybit_trades_parser.add_argument("--max-delay-ms", type=int, default=60_000)
    bybit_trades_parser.add_argument("--max-future-skew-ms", type=int, default=5_000)
    bybit_trades_parser.add_argument("--max-clock-skew-ms", type=float, default=60_000.0)
    bybit_trades_parser.add_argument(
        "--source-suffix",
        default="",
        help="Optional per-instrument lane suffix. When non-empty, runs go to "
        "<output_root>/bybit_trades_<suffix>/<timestamp>/ instead of "
        "bybit_trades/.",
    )
    bybit_trades_parser.add_argument(
        "--rotate-at-midnight",
        action="store_true",
        help="Rotate the run directory at midnight UTC instead of at --segment-count "
        "messages. See the depth worker's help text for the same flag.",
    )

    bybit_depth_parser = subparsers.add_parser(
        "bybit-depth-worker",
        help="Run segmented Bybit v5 spot orderbook depth collection (non-sequence feed)",
    )
    bybit_depth_parser.add_argument(
        "--symbol",
        default="BTCUSDT",
        help="Bybit v5 spot symbol, e.g. BTCUSDT / ETHUSDT (no separator).",
    )
    bybit_depth_parser.add_argument(
        "--channel",
        default="orderbook.50",
        help="Bybit v5 depth topic prefix '<orderbook>.<depth>', e.g. orderbook.50 "
        "(spot supports 1/50/200). The symbol is appended to form the full topic "
        "orderbook.<depth>.<symbol>. Bybit ships an in-stream snapshot + deltas whose "
        "data.u increments by exactly 1 per message, so this lane is curated as a "
        "provable 'sequence' feed (delta==1, gap-proof) — a dropped message is caught "
        "and blocks promotion (STANDARDS 4.1/4.3).",
    )
    bybit_depth_parser.add_argument(
        "--market",
        choices=list(_BYBIT_MARKETS),
        default="spot",
        help="Bybit v5 product type. 'spot' (default) keeps the legacy lane "
        "(bybit_depth/, spot:bybit:* instrument). 'linear' = USDT-perpetual futures: "
        "same orderbook topic + delta==1 sequence guarantee, but the URL path is "
        "/v5/public/linear and runs land in bybit_perp_depth/ tagged perp:bybit:*.",
    )
    bybit_depth_parser.add_argument("--segment-count", type=int, default=5000)
    bybit_depth_parser.add_argument("--max-segments", type=int)
    bybit_depth_parser.add_argument("--cooldown-seconds", type=float, default=1.0)
    bybit_depth_parser.add_argument("--output-root", type=Path, default=default_output_root())
    bybit_depth_parser.add_argument("--ops-root", type=Path, default=default_ops_root())
    bybit_depth_parser.add_argument("--worker-name", default="bybit-depth-worker")
    bybit_depth_parser.add_argument("--heartbeat-interval-seconds", type=float, default=30.0)
    bybit_depth_parser.add_argument(
        "--source-suffix",
        default="",
        help="Optional per-instrument lane suffix. When non-empty, runs go to "
        "<output_root>/bybit_depth_<suffix>/<timestamp>/ instead of bybit_depth/.",
    )
    bybit_depth_parser.add_argument(
        "--rotate-at-midnight",
        action="store_true",
        help="Rotate the run directory at midnight UTC instead of at --segment-count "
        "messages. See the depth worker's help text for the same flag.",
    )

    okx_trades_parser = subparsers.add_parser(
        "okx-trades-worker", help="Run segmented OKX v5 `trades` collection (non-sequence feed)"
    )
    okx_trades_parser.add_argument(
        "--symbol",
        default="BTC-USDT",
        help="OKX instId base, e.g. BTC-USDT. For --market linear the '-SWAP' suffix "
        "is added automatically, so pass the spot form here for both markets.",
    )
    okx_trades_parser.add_argument("--channel", default="trades", help="OKX v5 trades channel.")
    okx_trades_parser.add_argument(
        "--market",
        choices=list(_OKX_MARKETS),
        default="spot",
        help="OKX v5 market. 'spot' (default) collects BTC-USDT into okx_trades/ "
        "(spot:okx:* instrument). 'linear' = USDT-margined perpetual swap (BTC-USDT-SWAP): "
        "runs land in okx_perp_trades/ tagged perp:okx:* so perp never mixes with spot.",
    )
    okx_trades_parser.add_argument("--segment-count", type=int, default=5000)
    okx_trades_parser.add_argument("--max-segments", type=int)
    okx_trades_parser.add_argument("--cooldown-seconds", type=float, default=1.0)
    okx_trades_parser.add_argument("--output-root", type=Path, default=default_output_root())
    okx_trades_parser.add_argument("--ops-root", type=Path, default=default_ops_root())
    okx_trades_parser.add_argument("--worker-name", default="okx-trades-worker")
    okx_trades_parser.add_argument("--heartbeat-interval-seconds", type=float, default=30.0)
    okx_trades_parser.add_argument("--max-delay-ms", type=int, default=60_000)
    okx_trades_parser.add_argument("--max-future-skew-ms", type=int, default=5_000)
    okx_trades_parser.add_argument("--max-clock-skew-ms", type=float, default=60_000.0)
    okx_trades_parser.add_argument(
        "--source-suffix",
        default="",
        help="Optional per-instrument lane suffix. When non-empty, runs go to "
        "<output_root>/okx_trades_<suffix>/<timestamp>/ instead of okx_trades/.",
    )
    okx_trades_parser.add_argument(
        "--rotate-at-midnight",
        action="store_true",
        help="Rotate the run directory at midnight UTC instead of at --segment-count messages.",
    )

    okx_depth_parser = subparsers.add_parser(
        "okx-depth-worker",
        help="Run segmented OKX v5 `books` orderbook depth collection (seqId chain, provable)",
    )
    okx_depth_parser.add_argument(
        "--symbol",
        default="BTC-USDT",
        help="OKX instId base, e.g. BTC-USDT. For --market linear the '-SWAP' suffix "
        "is added automatically.",
    )
    okx_depth_parser.add_argument(
        "--channel",
        default="books",
        help="OKX v5 depth channel. 'books' (default) = 400-level in-stream snapshot + "
        "incremental updates carrying seqId/prevSeqId (provable chain) + a CRC32 checksum.",
    )
    okx_depth_parser.add_argument(
        "--market",
        choices=list(_OKX_MARKETS),
        default="spot",
        help="OKX v5 market. 'spot' (default) -> okx_depth/, spot:okx:*. 'linear' = "
        "USDT-margined perpetual swap (BTC-USDT-SWAP) -> okx_perp_depth/, perp:okx:*.",
    )
    okx_depth_parser.add_argument("--segment-count", type=int, default=5000)
    okx_depth_parser.add_argument("--max-segments", type=int)
    okx_depth_parser.add_argument("--cooldown-seconds", type=float, default=1.0)
    okx_depth_parser.add_argument("--output-root", type=Path, default=default_output_root())
    okx_depth_parser.add_argument("--ops-root", type=Path, default=default_ops_root())
    okx_depth_parser.add_argument("--worker-name", default="okx-depth-worker")
    okx_depth_parser.add_argument("--heartbeat-interval-seconds", type=float, default=30.0)
    okx_depth_parser.add_argument(
        "--source-suffix",
        default="",
        help="Optional per-instrument lane suffix. When non-empty, runs go to "
        "<output_root>/okx_depth_<suffix>/<timestamp>/ instead of okx_depth/.",
    )
    okx_depth_parser.add_argument(
        "--rotate-at-midnight",
        action="store_true",
        help="Rotate the run directory at midnight UTC instead of at --segment-count messages.",
    )

    kraken_depth_parser = subparsers.add_parser(
        "kraken-depth-worker",
        help="Run segmented Kraken v2 book depth collection (non-sequence feed)",
    )
    kraken_depth_parser.add_argument(
        "--symbol",
        default="BTC/USD",
        help="Kraken v2 pair, e.g. BTC/USD / ETH/USD (slash-separated).",
    )
    kraken_depth_parser.add_argument(
        "--channel",
        default="book",
        help="Kraken v2 channel. 'book' is the public order book feed (default depth "
        "10). Kraken ships an in-stream snapshot + updates plus a per-frame CRC32 "
        "checksum. For a pair whose native precision is known (BTC/USD), the collector "
        "validates that checksum at replay time, so the lane is curated 'checksum' "
        "(provable integrity — a dropped/corrupted update is caught and blocks "
        "promotion); other pairs fall back to none_native (STANDARDS 4.3).",
    )
    kraken_depth_parser.add_argument("--segment-count", type=int, default=5000)
    kraken_depth_parser.add_argument("--max-segments", type=int)
    kraken_depth_parser.add_argument("--cooldown-seconds", type=float, default=1.0)
    kraken_depth_parser.add_argument("--output-root", type=Path, default=default_output_root())
    kraken_depth_parser.add_argument("--ops-root", type=Path, default=default_ops_root())
    kraken_depth_parser.add_argument("--worker-name", default="kraken-depth-worker")
    kraken_depth_parser.add_argument("--heartbeat-interval-seconds", type=float, default=30.0)
    kraken_depth_parser.add_argument(
        "--source-suffix",
        default="",
        help="Optional per-instrument lane suffix. When non-empty, runs go to "
        "<output_root>/kraken_depth_<suffix>/<timestamp>/ instead of kraken_depth/.",
    )
    kraken_depth_parser.add_argument(
        "--rotate-at-midnight",
        action="store_true",
        help="Rotate the run directory at midnight UTC instead of at --segment-count "
        "messages. See the depth worker's help text for the same flag.",
    )

    mexc_trades_parser = subparsers.add_parser(
        "mexc-trades-worker",
        help="Run segmented MEXC spot aggregated-deals collection (protobuf, non-sequence feed)",
    )
    mexc_trades_parser.add_argument(
        "--symbol",
        default="BTCUSDT",
        help="MEXC spot symbol, e.g. BTCUSDT / ETHUSDT (no separator).",
    )
    mexc_trades_parser.add_argument(
        "--channel",
        default=MEXC_DEALS_CHANNEL,
        help="MEXC aggregated-deals protobuf channel prefix. The interval and symbol "
        "are appended to form the topic '<channel>@<interval>@<SYMBOL>' (e.g. "
        "spot@public.aggre.deals.v3.api.pb@100ms@BTCUSDT). MEXC retired its JSON "
        "websocket on 2025-08-04, so market-data frames are Protocol Buffers and are "
        "decoded via the vendored bindings (requires the 'protobuf' runtime). The "
        "deals stream carries no per-trade id, so this lane is curated as a "
        "non-sequence ('none_native') feed -- structurally clean only, NOT gap-proof "
        "(STANDARDS 4.3).",
    )
    mexc_trades_parser.add_argument(
        "--interval",
        default="100ms",
        choices=["10ms", "100ms"],
        help="Aggregation push interval for the deals stream.",
    )
    mexc_trades_parser.add_argument("--segment-count", type=int, default=5000)
    mexc_trades_parser.add_argument("--max-segments", type=int)
    mexc_trades_parser.add_argument("--cooldown-seconds", type=float, default=1.0)
    mexc_trades_parser.add_argument("--output-root", type=Path, default=default_output_root())
    mexc_trades_parser.add_argument("--ops-root", type=Path, default=default_ops_root())
    mexc_trades_parser.add_argument("--worker-name", default="mexc-trades-worker")
    mexc_trades_parser.add_argument("--heartbeat-interval-seconds", type=float, default=30.0)
    mexc_trades_parser.add_argument("--max-delay-ms", type=int, default=60_000)
    mexc_trades_parser.add_argument("--max-future-skew-ms", type=int, default=5_000)
    mexc_trades_parser.add_argument("--max-clock-skew-ms", type=float, default=60_000.0)
    mexc_trades_parser.add_argument(
        "--source-suffix",
        default="",
        help="Optional per-instrument lane suffix. When non-empty, runs go to "
        "<output_root>/mexc_trades_<suffix>/<timestamp>/ instead of mexc_trades/.",
    )
    mexc_trades_parser.add_argument(
        "--rotate-at-midnight",
        action="store_true",
        help="Rotate the run directory at midnight UTC instead of after --segment-count "
        "messages. See the depth worker's help text for the same flag.",
    )

    mexc_depth_parser = subparsers.add_parser(
        "mexc-depth-worker",
        help="Run segmented MEXC spot limit (partial-book) depth collection (protobuf, non-sequence feed)",
    )
    mexc_depth_parser.add_argument(
        "--symbol",
        default="BTCUSDT",
        help="MEXC spot symbol, e.g. BTCUSDT / ETHUSDT (no separator).",
    )
    mexc_depth_parser.add_argument(
        "--channel",
        default=MEXC_LIMIT_DEPTH_CHANNEL,
        help="MEXC limit (partial-book) depth protobuf channel prefix. The symbol and "
        "depth are appended to form the topic '<channel>@<SYMBOL>@<depth>' (e.g. "
        "spot@public.limit.depth.v3.api.pb@BTCUSDT@20). Each frame is a full top-N "
        "book, emitted as a 'snapshot' anchor; the per-frame 'version' is preserved in "
        "metadata as gap-detection metadata but NOT used to prove gaplessness (the "
        "frames are independent full books, not a delta chain), so this lane is curated "
        "as a non-sequence ('none_native') feed -- structurally clean only (STANDARDS "
        "4.3). Frames are Protocol Buffers (decoded via the vendored bindings; requires "
        "the 'protobuf' runtime).",
    )
    mexc_depth_parser.add_argument(
        "--depth",
        type=int,
        default=20,
        choices=[5, 10, 20],
        help="Number of book levels per side in the limit-depth stream.",
    )
    mexc_depth_parser.add_argument("--segment-count", type=int, default=5000)
    mexc_depth_parser.add_argument("--max-segments", type=int)
    mexc_depth_parser.add_argument("--cooldown-seconds", type=float, default=1.0)
    mexc_depth_parser.add_argument("--output-root", type=Path, default=default_output_root())
    mexc_depth_parser.add_argument("--ops-root", type=Path, default=default_ops_root())
    mexc_depth_parser.add_argument("--worker-name", default="mexc-depth-worker")
    mexc_depth_parser.add_argument("--heartbeat-interval-seconds", type=float, default=30.0)
    mexc_depth_parser.add_argument(
        "--source-suffix",
        default="",
        help="Optional per-instrument lane suffix. When non-empty, runs go to "
        "<output_root>/mexc_depth_<suffix>/<timestamp>/ instead of mexc_depth/.",
    )
    mexc_depth_parser.add_argument(
        "--rotate-at-midnight",
        action="store_true",
        help="Rotate the run directory at midnight UTC instead of after --segment-count "
        "messages. See the depth worker's help text for the same flag.",
    )

    kalshi_discover_parser = subparsers.add_parser(
        "kalshi-discover-crypto",
        help="Discover public Kalshi BTC/ETH crypto binary series and open markets",
    )
    kalshi_discover_parser.add_argument("--category", default=DEFAULT_KALSHI_CATEGORY)
    kalshi_discover_parser.add_argument("--target-assets", nargs="+", default=DEFAULT_KALSHI_TARGET_ASSETS)
    kalshi_discover_parser.add_argument(
        "--target-frequencies",
        nargs="+",
        default=DEFAULT_KALSHI_TARGET_FREQUENCIES,
        help="Series frequencies to keep, e.g. fifteen_min hourly.",
    )
    kalshi_discover_parser.add_argument("--markets-per-series", type=int, default=DEFAULT_KALSHI_MARKETS_PER_SERIES)
    kalshi_discover_parser.add_argument("--output-root", type=Path, default=default_curated_root("kalshi_crypto_binary_options"))
    kalshi_discover_parser.add_argument("--format", choices=["json", "text"], default="text")

    kalshi_collect_parser = subparsers.add_parser(
        "kalshi-collect-crypto-quotes",
        help="Collect public Kalshi BTC/ETH binary quote snapshots into raw and normalized storage",
    )
    kalshi_collect_parser.add_argument("--category", default=DEFAULT_KALSHI_CATEGORY)
    kalshi_collect_parser.add_argument("--target-assets", nargs="+", default=DEFAULT_KALSHI_TARGET_ASSETS)
    kalshi_collect_parser.add_argument("--target-frequencies", nargs="+", default=DEFAULT_KALSHI_TARGET_FREQUENCIES)
    kalshi_collect_parser.add_argument("--markets-per-series", type=int, default=DEFAULT_KALSHI_MARKETS_PER_SERIES)
    kalshi_collect_parser.add_argument("--duration-seconds", type=float, default=DEFAULT_KALSHI_DURATION_SECONDS)
    kalshi_collect_parser.add_argument("--sample-count", type=int)
    kalshi_collect_parser.add_argument("--poll-interval-seconds", type=float, default=DEFAULT_KALSHI_POLL_INTERVAL_SECONDS)
    kalshi_collect_parser.add_argument("--stale-after-seconds", type=float, default=DEFAULT_KALSHI_STALE_AFTER_SECONDS)
    kalshi_collect_parser.add_argument("--output-root", type=Path, default=default_kalshi_output_root())
    kalshi_collect_parser.add_argument("--normalized-root", type=Path, default=default_kalshi_normalized_root())
    kalshi_collect_parser.add_argument(
        "--no-jsonl-fsync",
        action="store_false",
        dest="jsonl_fsync",
        default=True,
        help="Disable per-row fsync for raw/clean/quarantine JSONL writes.",
    )
    kalshi_collect_parser.add_argument(
        "--no-normalized-parquet",
        action="store_false",
        dest="normalized_parquet",
        default=True,
        help="Skip normalized Parquet writes; raw and clean JSONL are still written.",
    )
    _add_fsync_batching_args(kalshi_collect_parser)
    kalshi_collect_parser.add_argument("--format", choices=["json", "text"], default="text")

    kalshi_summary_parser = subparsers.add_parser(
        "kalshi-summarize-crypto-quotes",
        help="Summarize a Kalshi quote run or events.jsonl with per-symbol quote transitions",
    )
    kalshi_summary_parser.add_argument("--input-path", type=Path, required=True)
    kalshi_summary_parser.add_argument("--format", choices=["json", "text"], default="text")

    ops_parser = subparsers.add_parser("ops-runner", help="Run collection and curation jobs from a manifest")
    ops_parser.add_argument("--config", type=Path, required=True)
    ops_parser.add_argument("--ops-root", type=Path, default=default_ops_root())
    ops_parser.add_argument("--poll-seconds", type=int, default=5)
    ops_parser.add_argument("--max-runs", type=int)
    ops_parser.add_argument("--stop-on-error", action="store_true")
    ops_parser.add_argument(
        "--collector-concurrency",
        type=int,
        default=1,
        help=(
            "Maximum collector jobs (the *-worker types) to run concurrently. "
            "Maintenance jobs stay serialized regardless. Defaults to 1 (serial)."
        ),
    )

    run_job_parser = subparsers.add_parser(
        "run-job",
        help="Run a single ops job in this process (child entrypoint for process-isolated collectors)",
    )
    run_job_parser.add_argument("--job-json", required=True, help="JSON: {name, job_type, interval_seconds, args}")

    health_parser = subparsers.add_parser("health", help="Inspect runner and archive health")
    health_parser.add_argument(
        "--ops-root",
        type=Path,
        default=None,
        help="Ops root to inspect. Default: derive from the discovered ops config so the "
        "report follows the live collection root, falling back to the env/default root.",
    )
    health_parser.add_argument("--config", type=Path)
    health_parser.add_argument("--stale-after-seconds", type=float, default=120.0)
    health_parser.add_argument("--job-stale-multiplier", type=float, default=3.0)
    health_parser.add_argument("--recent-failure-window-seconds", type=float, default=3600.0)
    health_parser.add_argument("--min-disk-free-gb", type=float, default=50.0)
    health_parser.add_argument(
        "--quarantine-ratio-threshold",
        type=float,
        default=0.20,
        help="Flag a worker as high_quarantine_ratio when "
        "quarantined_events / raw_messages exceeds this fraction in the latest "
        "summary.jsonl row of the active run.",
    )
    health_parser.add_argument("--format", choices=["json", "text"], default="text")

    cleanup_parser = subparsers.add_parser("cleanup", help="Report or apply archive cleanup")
    cleanup_parser.add_argument("--archive-root", type=Path, default=default_archive_root())
    cleanup_parser.add_argument("--raw-days", type=int, default=14)
    cleanup_parser.add_argument("--raw-policy", action="append", default=[], metavar="DATASET/SOURCE=DAYS")
    cleanup_parser.add_argument("--apply", action="store_true")
    cleanup_parser.add_argument("--format", choices=["json", "text"], default="text")

    offload_parser = subparsers.add_parser(
        "archive-offload",
        help="Move promoted/quarantined raw run dirs older than the retention window "
        "to a cold archive on another disk (verify-then-delete; dry-run by default)",
    )
    offload_parser.add_argument("--raw-root", type=Path, default=default_output_root())
    offload_parser.add_argument("--cold-root", type=Path, required=True)
    offload_parser.add_argument(
        "--lanes-file",
        type=Path,
        required=True,
        help="JSON file: list of {source, promotion_index, quarantine_index} lane specs "
        "(same shape as the archive-offload ops job's 'lanes' arg)",
    )
    offload_parser.add_argument("--min-age-days", type=float, default=14.0)
    offload_parser.add_argument("--limit", type=int, default=200)
    offload_parser.add_argument("--apply", action="store_true")
    offload_parser.add_argument("--format", choices=["json", "text"], default="text")

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

    backfill_parser = subparsers.add_parser("backfill-replay", help="Backfill missing depth replay summaries")
    backfill_parser.add_argument("--source-root", type=Path, default=default_archive_root() / "raw" / "market" / "binance_depth")
    backfill_parser.add_argument("--limit", type=int, default=50)
    backfill_parser.add_argument("--max-age-hours", type=float, default=24.0)
    backfill_parser.add_argument("--overwrite", action="store_true")
    backfill_parser.add_argument("--format", choices=["json", "text"], default="text")

    backfill_trades_parser = subparsers.add_parser(
        "backfill-trades-replay",
        help="Backfill missing replay summaries for a TRADES lane using the trades "
        "scorer (not the depth scorer). Defaults to binance_trades.",
    )
    backfill_trades_parser.add_argument(
        "--source-root", type=Path, default=default_archive_root() / "raw" / "market" / "binance_trades"
    )
    backfill_trades_parser.add_argument("--limit", type=int, default=50)
    backfill_trades_parser.add_argument("--max-age-hours", type=float, default=24.0)
    backfill_trades_parser.add_argument("--overwrite", action="store_true")
    backfill_trades_parser.add_argument(
        "--stream",
        action="store_true",
        help="Score with the none_native stream-trades replay (Bybit/MEXC, whose "
        "trade ids are UUIDs) instead of the dense sequence-bearing scorer "
        "(Binance/Coinbase/Kraken).",
    )
    backfill_trades_parser.add_argument("--format", choices=["json", "text"], default="text")

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

    backfill_parser = subparsers.add_parser(
        "backfill-stream-depth",
        help=(
            "Re-replay already-collected stream-snapshot depth runs "
            "(Coinbase/Bybit/Kraken) with the current multi-anchor logic, then "
            "optionally promote the replayable ones. Dry-run unless --apply."
        ),
    )
    backfill_parser.add_argument("--raw-root", type=Path, default=default_output_root())
    backfill_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Re-score runs that already have a replay_summary.json. Without this, "
        "already-scored runs are skipped (the catch-up job only needs to rescue "
        "cut-off segments, and re-replaying everything blocked the scheduler for "
        "~100 minutes per hourly pass once the backlog grew).",
    )
    backfill_parser.add_argument(
        "--source",
        nargs="+",
        default=["coinbase_depth", "bybit_depth", "kraken_depth"],
        help="Raw source directory names under --raw-root to backfill.",
    )
    backfill_parser.add_argument(
        "--target-root", type=Path, default=default_curated_root("market_replayable")
    )
    backfill_parser.add_argument("--limit", type=int, default=200)
    backfill_parser.add_argument("--max-age-hours", type=float, default=720.0)
    backfill_parser.add_argument(
        "--apply",
        action="store_true",
        help=(
            "Regenerate each run's metrics/replay_summary.json and promote the "
            "replayable runs. Without this flag the command is a read-only dry run."
        ),
    )
    backfill_parser.add_argument(
        "--score-only",
        action="store_true",
        help=(
            "Regenerate each run's metrics/replay_summary.json but DO NOT promote. "
            "Promotion is left to the quarantine-aware promote-replayable jobs so a "
            "single promoter owns the curated parquet (avoids duplicate rows). This "
            "is the mode the live ops scoring catch-up job uses. Wins over --apply."
        ),
    )
    backfill_parser.add_argument("--format", choices=["json", "text"], default="text")

    subparsers.add_parser("state", help="Show archive and package state")
    return parser


async def run_mock(args: argparse.Namespace) -> None:
    collector = MockL3Collector(
        source="mock",
        product=args.product,
        delay_ms=float(getattr(args, "delay_ms", 0.0)),
    )
    run_paths = prepare_run_paths(output_root=args.output_root, source="mock")
    # Synthetic rows must never land in the live normalized `market` dataset (the
    # one the real depth lanes write): a bare `mock` invocation on the plant box
    # used to do exactly that via the default-root fallback. Normalized parquet is
    # opt-in via an explicit normalized_root (ops job arg); default is none.
    configured_normalized_root = getattr(args, "normalized_root", None)
    pipeline = CollectorPipeline(
        collector=collector,
        normalizer=GenericL3Normalizer(),
        quality_gate=QualityGate(session_id=run_paths.base.name),
        run_paths=run_paths,
        normalized_root=Path(configured_normalized_root) if configured_normalized_root else None,
    )
    summary = await pipeline.run(limit=args.count)
    print(f"mock run finished: {summary.to_dict()} -> {run_paths.base}")


def _resolve_normalized_root(args: argparse.Namespace, dataset: str) -> Path:
    """Normalized-parquet root for a worker: explicit per-lane config beats the
    env/default fallback. The fallback default once pointed at the pre-migration
    disk and the workers were the only writers not fed from the ops config, so
    their normalized output silently landed on the retired drive — explicit
    threading makes the next disk migration a config-only change."""
    configured = getattr(args, "normalized_root", None)
    if configured:
        return Path(configured)
    return default_normalized_root(dataset)


def _binance_rest_snapshot_clean_row(
    normalizer: BinanceDepthNormalizer,
    *,
    source: str,
    product: str,
    snapshot: dict[str, object],
    snapshot_last_update_id: int,
    received_at: datetime,
) -> dict[str, object]:
    """Build an ``event_type="snapshot"`` clean-event row from binance's REST order-book
    snapshot. Binance's diff-depth WS sends no snapshot frame (unlike coinbase/bybit/
    kraken/mexc, whose in-stream snapshot becomes a clean event), so without this the REST
    seed lived only in the sidecar file and the curated market_replayable dataset had no
    binance snapshot row — i.e. it wasn't self-contained for replay. Built through the
    normal BinanceDepthNormalizer so the row schema matches the deltas exactly, with
    first/final_update_id pinned to the snapshot's lastUpdateId. That id placement means
    raw-run replay (replay_depth_run, which seeds from the sidecar) harmlessly skips this
    row (final_id <= snapshot_last_update_id), while stream/curated replay reseeds from it."""
    event = normalizer.normalize(
        RawMessage(
            source=source,
            received_at=received_at,
            payload={
                "e": "snapshot",
                # No exchange event time: the REST snapshot carries none, and
                # stamping local wall-clock here put a row whose "exchange time"
                # POSTDATES the buffered deltas written after it (event-time
                # inversion at the head of every segment; a stream-mode rescore
                # would flag non_monotonic_event_time). event_time=None matches the
                # other venues' in-stream snapshots; replay's monotonicity check
                # skips null times and event_date partitioning falls back to
                # received_at, so placement is unchanged.
                "E": None,
                "s": str(product).upper(),
                "U": snapshot_last_update_id,
                "u": snapshot_last_update_id,
                "b": snapshot.get("bids", []),
                "a": snapshot.get("asks", []),
            },
        )
    )
    return event.to_dict()


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
    source_name = _build_source_name("binance_depth", getattr(args, "source_suffix", ""))
    run_paths = prepare_run_paths(output_root=config.output_root, source=source_name)
    raw_sink = JsonlSink(run_paths.raw, "messages.jsonl")
    clean_sink = JsonlSink(run_paths.clean, "events.jsonl")
    quarantine_sink = JsonlSink(run_paths.quarantine, "events.jsonl")
    metrics_sink = JsonlSink(run_paths.metrics, "summary.jsonl")
    parquet_sink = ParquetDatasetSink(_resolve_normalized_root(args, "market"))
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
        snapshot_anchor_timeout_seconds=float(getattr(args, "snapshot_anchor_timeout_seconds", 10.0)),
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
    deadline_reached = False
    reconnect_attempts = 0  # consecutive retryable failures since last successful frame
    # Tracks the latest event id we've consumed so reconnect-in-place can check
    # whether the next event bridges where we left off (not where the snapshot started).
    last_seen_final_update_id = snapshot_last_update_id

    # Emit the REST snapshot as the FIRST clean event so the curated dataset carries a
    # binance snapshot row like the other venues (their in-stream snapshot already does).
    # Without it, curated market_replayable has only binance deltas and can't be replayed
    # without the raw sidecar. Written to clean + normalized parquet (what promotion reads).
    snapshot_clean_row = _binance_rest_snapshot_clean_row(
        normalizer,
        source=config.source,
        product=str(config.product),
        snapshot=snapshot,
        snapshot_last_update_id=snapshot_last_update_id,
        received_at=utc_now(),
    )
    clean_sink.write(snapshot_clean_row)
    parquet_sink.write(snapshot_clean_row)
    clean_count += 1

    deadline_utc: datetime | None = getattr(args, "deadline_utc", None)

    def _deadline_crossed() -> bool:
        return deadline_utc is not None and utc_now() >= deadline_utc

    def _process_batch(raws: list[RawMessage]) -> bool:
        """Process buffered raws. Returns True if the segment should stop (count
        reached OR day-bounded deadline crossed)."""
        nonlocal message_count, clean_count, quarantined_count, last_seen_final_update_id
        nonlocal deadline_reached
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
            if _deadline_crossed():
                deadline_reached = True
                return True
        return False

    try:
        if _process_batch(pending_raws):
            # Close the WS before finalizing — _process_batch returns True when count
            # is reached OR the day-bounded deadline crossed, and either way we want
            # the socket cleanup to happen before the parquet flush / replay summary.
            try:
                await connection.__aexit__(None, None, None)
            except Exception:  # noqa: BLE001
                pass
            connection = None
            websocket = None
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
                deadline_reached=deadline_reached,
            )

        while message_count < args.count and not alignment_broken and not deadline_reached:
            try:
                async for message in websocket:
                    payload = json.loads(message)
                    if not _is_binance_depth_payload(payload):
                        continue
                    raw = RawMessage(source=config.source, received_at=utc_now(), payload=payload)
                    if _process_batch([raw]):
                        break
                    reconnect_attempts = 0  # any successful frame resets the retry budget
                # Stream returned (or break above). If the segment is done — count
                # reached OR day/segment deadline crossed — exit the outer loop; the
                # finally block closes the socket. Falling through on deadline used
                # to run a pointless reconnect-in-place for an already-finished
                # segment, recording a spurious reconnect + alignment break on EVERY
                # rotation (constant 1-per-segment noise in book-sync-health) and, if
                # that reconnect failed, erroring a perfectly complete segment.
                if message_count >= args.count or deadline_reached:
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
        deadline_reached=deadline_reached,
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
    deadline_reached: bool = False,
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
            "deadline_reached": deadline_reached,
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
        "deadline_reached": deadline_reached,
        "replayable": replayable,
        "replay_findings": findings,
    }


# Binance trades run against spot or USDT-M futures (perp). Both speak the same WS
# subscription protocol and BinanceTradeNormalizer already reads the aggTrade `a` id, so
# futures is a URL + endpoint + instrument-tagging switch. Futures streams only aggregate
# trades (no raw @trade), so the futures lane forces channel=aggTrade, routes to
# binance_perp_trades/, and tags perp:binance-futures:* (the explicit instrument-master
# record) instead of spot:binance:*. aggTrade's `a` is a dense per-symbol counter, so the
# lane stays a provable sequence feed (replay_trades_run, gap-proof) just like spot.
_BINANCE_SPOT_TRADES_WS = "wss://stream.binance.com:9443/ws"
_BINANCE_FUTURES_WS = "wss://fstream.binance.com/ws"
_BINANCE_TRADES_MARKETS = ("spot", "futures")


def _binance_trades_market(args: argparse.Namespace) -> str:
    market = str(getattr(args, "market", "spot") or "spot").lower()
    if market not in _BINANCE_TRADES_MARKETS:
        raise SystemExit(
            f"--market must be one of {', '.join(_BINANCE_TRADES_MARKETS)} (got {market!r})"
        )
    return market


async def collect_binance_trades_segment(args: argparse.Namespace) -> dict[str, object]:
    if _binance_trades_market(args) == "futures":
        return await _collect_trades_segment(
            args,
            source="binance-futures",
            websocket_url=_BINANCE_FUTURES_WS,
            subscription_style="binance",
            normalizer=BinanceTradeNormalizer(instrument_type="perp"),
            source_base="binance_perp_trades",
            channel_override="aggTrade",
        )
    return await _collect_trades_segment(
        args,
        source="binance",
        websocket_url=_BINANCE_SPOT_TRADES_WS,
        subscription_style="binance",
        normalizer=BinanceTradeNormalizer(),
        source_base="binance_trades",
    )


async def collect_coinbase_trades_segment(args: argparse.Namespace) -> dict[str, object]:
    return await _collect_trades_segment(
        args,
        source="coinbase",
        websocket_url="wss://ws-feed.exchange.coinbase.com",
        subscription_style="coinbase",
        normalizer=CoinbaseTradeNormalizer(),
        source_base="coinbase_trades",
    )


async def collect_kraken_trades_segment(args: argparse.Namespace) -> dict[str, object]:
    # Kraken v2 `trade_id` is a dense per-pair counter, so the standard
    # sequence-bearing trades replay (gap-proof, STANDARDS §4.2) applies.
    return await _collect_trades_segment(
        args,
        source="kraken",
        websocket_url="wss://ws.kraken.com/v2",
        subscription_style="kraken_v2",
        normalizer=KrakenTradeNormalizer(),
        source_base="kraken_trades",
    )


# Bybit drops idle public connections after ~10 min and documents an application-
# level {"op":"ping"} heartbeat roughly every 20 s. Both Bybit lanes opt into the
# collector's generic keepalive with these; every other venue leaves it off and
# relies on the websockets library's protocol-level ping/pong (STANDARDS §4.3).
_BYBIT_PING_MESSAGE = {"op": "ping"}
_BYBIT_PING_INTERVAL_SECONDS = 20.0

# Bybit v5 public market types we collect. 'spot' is the original lane; 'linear' is
# USDT-perpetual futures. Both use the identical publicTrade/orderbook.50 topics, the
# same v5 keepalive, and the same generic WS collector + curation chain — only the URL
# path differs (…/v5/public/spot vs …/linear) and the resulting instrument is a perp
# rather than spot. Keeping the market explicit (not inferred from the symbol) avoids
# silently mixing spot and perp BTCUSDT, which share a venue_symbol.
_BYBIT_WS_BASE = "wss://stream.bybit.com/v5/public"
_BYBIT_MARKETS = ("spot", "linear")


def _bybit_market(args: argparse.Namespace) -> str:
    market = str(getattr(args, "market", "spot") or "spot").lower()
    if market not in _BYBIT_MARKETS:
        raise SystemExit(
            f"--market must be one of {', '.join(_BYBIT_MARKETS)} (got {market!r})"
        )
    return market


def _bybit_ws_url(market: str) -> str:
    return f"{_BYBIT_WS_BASE}/{market}"


def _bybit_instrument_type(market: str) -> str:
    # linear is a USDT-margined perpetual; spot stays spot.
    return "perp" if market == "linear" else "spot"


# OKX v5 public market data. Spot and USDT-margined perpetual swap share ONE public
# socket; the market is carried by the instId (BTC-USDT vs BTC-USDT-SWAP), not the URL.
# The v5 keepalive is the bare text string "ping" (server replies "pong"), unlike
# Bybit's JSON {"op":"ping"}; send it every <30 s or the idle socket is dropped.
_OKX_WS_URL = "wss://ws.okx.com:8443/ws/v5/public"
_OKX_PING_MESSAGE = "ping"
_OKX_PING_INTERVAL_SECONDS = 15.0
_OKX_MARKETS = ("spot", "linear")


def _okx_market(args: argparse.Namespace) -> str:
    market = str(getattr(args, "market", "spot") or "spot").lower()
    if market not in _OKX_MARKETS:
        raise SystemExit(
            f"--market must be one of {', '.join(_OKX_MARKETS)} (got {market!r})"
        )
    return market


def _okx_instrument_type(market: str) -> str:
    return "perp" if market == "linear" else "spot"


def _okx_instid(symbol: str, market: str) -> str:
    """The OKX instId to subscribe. linear appends `-SWAP` to the spot base so a lane
    can pass the same `BTC-USDT` symbol for both markets (BTC-USDT / BTC-USDT-SWAP)."""
    base = str(symbol).upper()
    if base.endswith("-SWAP"):
        base = base[: -len("-SWAP")]
    return f"{base}-SWAP" if market == "linear" else base


async def collect_okx_trades_segment(args: argparse.Namespace) -> dict[str, object]:
    # OKX `tradeId` is per-instrument but the public `trades` channel may conflate
    # fills, so it isn't trusted as a dense counter: curate as a non-sequence
    # ("none_native") feed — structurally clean only, NOT gap-proof (STANDARDS §4.3),
    # same class as Bybit/MEXC trades. spot vs linear only changes the instId suffix,
    # the lane directory (okx_trades vs okx_perp_trades) and the resolved instrument.
    market = _okx_market(args)
    instid = _okx_instid(getattr(args, "symbol", "BTC-USDT"), market)
    args.symbol = instid
    return await _collect_trades_segment(
        args,
        source="okx",
        websocket_url=_OKX_WS_URL,
        subscription_style="okx",
        normalizer=OkxTradeNormalizer(instrument_type=_okx_instrument_type(market)),
        source_base="okx_trades" if market == "spot" else "okx_perp_trades",
        replay_fn=replay_trades_stream_run,
        ping_message=_OKX_PING_MESSAGE,
        ping_interval_seconds=_OKX_PING_INTERVAL_SECONDS,
    )


async def collect_okx_depth_segment(args: argparse.Namespace) -> dict[str, object]:
    # OKX `books` delivers a 400-level in-stream snapshot + incremental updates carrying
    # seqId/prevSeqId, where prevSeqId(N) == seqId(N-1). Passing chain_sequence promotes
    # this lane from none_native to a provable `sequence` gap proof (chain equality), so
    # a dropped message breaks the link and blocks promotion (STANDARDS §4.1). The same
    # books frames carry the chain for spot and swap; only the instId suffix, lane dir
    # and instrument (perp) differ.
    market = _okx_market(args)
    instid = _okx_instid(getattr(args, "symbol", "BTC-USDT"), market)
    args.symbol = instid
    return await _collect_depth_stream_segment(
        args,
        source="okx",
        websocket_url=_OKX_WS_URL,
        subscription_style="okx",
        normalizer=OkxDepthNormalizer(instrument_type=_okx_instrument_type(market)),
        source_base="okx_depth" if market == "spot" else "okx_perp_depth",
        ping_message=_OKX_PING_MESSAGE,
        ping_interval_seconds=_OKX_PING_INTERVAL_SECONDS,
        **_stream_depth_replay_kwargs("okx", instid),
    )


async def collect_bybit_trades_segment(args: argparse.Namespace) -> dict[str, object]:
    # Bybit trade id is a UUID (not a dense counter), so gaplessness is unprovable:
    # curate as a non-sequence ("none_native") feed — structurally clean only, NOT
    # gap-proof (STANDARDS §4.3). spot vs linear only changes the URL path, the lane
    # directory (bybit_trades vs bybit_perp_trades) and the resolved instrument.
    market = _bybit_market(args)
    return await _collect_trades_segment(
        args,
        source="bybit",
        websocket_url=_bybit_ws_url(market),
        subscription_style="bybit",
        normalizer=BybitTradeNormalizer(instrument_type=_bybit_instrument_type(market)),
        source_base="bybit_trades" if market == "spot" else "bybit_perp_trades",
        replay_fn=replay_trades_stream_run,
        ping_message=_BYBIT_PING_MESSAGE,
        ping_interval_seconds=_BYBIT_PING_INTERVAL_SECONDS,
    )


async def collect_mexc_trades_segment(args: argparse.Namespace) -> dict[str, object]:
    # MEXC's public market data is protobuf-encoded (JSON endpoint retired
    # 2025-08-04), so the lane supplies decode_mexc_frame as the binary-frame decoder;
    # text ack/PONG frames stay JSON. The aggregated-deals stream carries no per-trade
    # id, so curate as a non-sequence ("none_native") feed — structurally clean only,
    # NOT gap-proof (STANDARDS 4.3), same class as Bybit spot trades.
    topic = build_deals_topic(
        channel=args.channel,
        symbol=str(args.symbol),
        interval=str(getattr(args, "interval", "100ms")),
    )
    return await _collect_trades_segment(
        args,
        source="mexc",
        websocket_url=MEXC_WS_URL,
        subscription_style="mexc",
        normalizer=MexcTradeNormalizer(),
        source_base="mexc_trades",
        replay_fn=replay_trades_stream_run,
        ping_message=MEXC_PING_MESSAGE,
        ping_interval_seconds=MEXC_PING_INTERVAL_SECONDS,
        message_decoder=decode_mexc_frame,
        channel_override=topic,
    )


async def _collect_trades_segment(
    args: argparse.Namespace,
    *,
    source: str,
    websocket_url: str,
    subscription_style: str,
    normalizer: object,
    source_base: str,
    replay_fn=replay_trades_run,
    ping_message: dict | str | None = None,
    ping_interval_seconds: float = 0.0,
    message_decoder=None,
    channel_override: str | None = None,
) -> dict[str, object]:
    """Venue-agnostic trades segment. The trades pipeline (generic WS collector +
    normalizer + quality gate + trades replay) is identical across venues; only the
    endpoint, subscription style, normalizer, source-directory base and replay function
    differ, so each venue is a thin wrapper that supplies those. Keeping one body means
    the curation contract (`metrics/replay_summary.json`) stays consistent
    venue-to-venue. `replay_fn` is `replay_trades_run` for dense sequence-bearing feeds
    (Binance/Coinbase/Kraken) and `replay_trades_stream_run` for none_native feeds
    (Bybit spot). `ping_message`/`ping_interval_seconds` opt a venue into the collector's
    app-level keepalive (Bybit only); default off leaves every other venue unchanged."""
    config = CollectorConfig(
        source=source,
        output_root=args.output_root,
        product=args.symbol,
        channel=channel_override or args.channel,
        websocket_url=websocket_url,
        subscription_style=subscription_style,
        max_delay_ms=args.max_delay_ms,
        max_future_skew_ms=getattr(args, "max_future_skew_ms", 5_000),
        ping_message=ping_message,
        ping_interval_seconds=ping_interval_seconds,
        idle_timeout_seconds=float(getattr(args, "idle_timeout_seconds", 0.0) or 0.0),
        message_decoder=message_decoder,
    )
    collector = GenericWebsocketCollector(config=config)
    source_name = _build_source_name(source_base, getattr(args, "source_suffix", ""))
    run_paths = prepare_run_paths(output_root=config.output_root, source=source_name)
    fsync_events, fsync_ms = _fsync_intervals(args)
    pipeline = CollectorPipeline(
        collector=collector,
        normalizer=normalizer,
        quality_gate=QualityGate(
            max_delay_ms=config.max_delay_ms,
            max_future_skew_ms=config.max_future_skew_ms,
            session_id=run_paths.base.name,
        ),
        run_paths=run_paths,
        normalized_root=(
            _resolve_normalized_root(args, "trades")
            if bool(getattr(args, "normalized_parquet", True))
            else None
        ),
        jsonl_fsync=bool(getattr(args, "jsonl_fsync", True)),
        fsync_interval_events=fsync_events,
        fsync_interval_ms=fsync_ms,
    )
    pipeline_summary = await pipeline.run(
        limit=args.count,
        deadline_utc=getattr(args, "deadline_utc", None),
    )

    # Write the trades replay summary so the existing quarantine + promote chain
    # (which keys off `metrics/replay_summary.json`) can curate trades runs the
    # same way it curates depth runs.
    events_path = run_paths.base / "clean" / "events.jsonl"
    if events_path.exists():
        replay_summary = replay_fn(
            run_paths.base,
            max_clock_skew_ms=float(getattr(args, "max_clock_skew_ms", 60_000.0)),
            write_summary=True,
        )
        replayable = replay_summary.replayable
        replay_findings = list(replay_summary.findings)
        replay_summary_path = replay_summary.summary_path
    else:
        replayable = False
        replay_findings = ["no_clean_events"]
        replay_summary_path = None

    return {
        "raw_messages": pipeline_summary.raw_messages,
        "clean_events": pipeline_summary.clean_events,
        "quarantined_events": pipeline_summary.quarantined_events,
        "run_path": str(run_paths.base),
        "replayable": replayable,
        "replay_findings": replay_findings,
        "replay_summary_path": replay_summary_path,
        "deadline_reached": bool(pipeline_summary.deadline_reached),
        "idle_timeout_count": collector.idle_timeout_count,
    }


async def collect_coinbase_depth_segment(args: argparse.Namespace) -> dict[str, object]:
    return await _collect_depth_stream_segment(
        args,
        source="coinbase",
        websocket_url="wss://ws-feed.exchange.coinbase.com",
        subscription_style="coinbase",
        normalizer=CoinbaseDepthNormalizer(),
        source_base="coinbase_depth",
    )


async def collect_bybit_depth_segment(args: argparse.Namespace) -> dict[str, object]:
    # Bybit orderbook delivers an in-stream snapshot + deltas AND a dense per-symbol
    # update id (data.u) that increments by exactly 1 per message (verified live
    # 2026-06-01). Passing sequence_metadata_key promotes this lane from none_native
    # to a provable `sequence` gap proof (delta==1), so a dropped message is caught
    # and blocks promotion (STANDARDS 4.1 / 4.3). Linear (USDT-perp) orderbook carries
    # the same in-stream snapshot + delta==1 update id, so the sequence guarantee holds
    # identically; only the URL path, lane dir and instrument (perp) differ.
    market = _bybit_market(args)
    return await _collect_depth_stream_segment(
        args,
        source="bybit",
        websocket_url=_bybit_ws_url(market),
        subscription_style="bybit",
        normalizer=BybitDepthNormalizer(instrument_type=_bybit_instrument_type(market)),
        source_base="bybit_depth" if market == "spot" else "bybit_perp_depth",
        ping_message=_BYBIT_PING_MESSAGE,
        ping_interval_seconds=_BYBIT_PING_INTERVAL_SECONDS,
        **_stream_depth_replay_kwargs("bybit", getattr(args, "symbol", None)),
    )


# Kraken v2 `book` CRC32 checksum is computed over the top-10 book at each pair's
# native (price, qty) decimal precision (pair_decimals, lot_decimals from the REST
# AssetPairs endpoint). Validating it proves the local book matches Kraken's, so a
# known-precision pair is curated as `gap_detection="checksum"` (provable integrity);
# a pair absent from this table falls back to none_native (no false validation).
# BTC/USD = (price 1dp, qty 8dp), verified against the live socket 2026-06-01.
_KRAKEN_BOOK_PRECISION: dict[str, tuple[int, int]] = {
    "BTC/USD": (1, 8),
}

# Kraken v2 `book` (no explicit depth) maintains the top-10 levels per side and the
# per-frame CRC32 is taken over exactly that top-10. Kraken silently drops the worst
# level past depth 10 without sending a delete, so replay must trim to 10 per side or
# the reconstructed top-10 (and its CRC) diverges once the book churns past the snapshot.
_KRAKEN_BOOK_DEPTH = 10


def _stream_depth_replay_kwargs(source: str, symbol: str | None = None) -> dict[str, object]:
    """Per-venue kwargs for `replay_depth_stream_run`. Single source of truth shared by
    the live depth collectors (`collect_*_depth_segment`) and the offline backfill tool
    (`backfill-stream-depth`) so the two cannot drift:

    - bybit  -> provable `sequence` via the dense `bybit_update_id`
    - okx    -> provable `sequence` via the prevSeqId/seqId chain (chain_sequence)
    - kraken -> provable `checksum` (when the pair precision is known) + depth-bounded book
    - others -> none_native (structural-only)
    """
    if source == "bybit":
        return {"sequence_metadata_key": "bybit_update_id"}
    if source == "okx":
        return {"chain_sequence": True}
    if source == "kraken":
        kwargs: dict[str, object] = {"book_depth": _KRAKEN_BOOK_DEPTH}
        precision = _KRAKEN_BOOK_PRECISION.get(symbol or "")
        if precision is not None:
            kwargs.update(
                checksum_metadata_key="kraken_checksum",
                checksum_price_precision=precision[0],
                checksum_qty_precision=precision[1],
            )
        return kwargs
    return {}


async def collect_kraken_depth_segment(args: argparse.Namespace) -> dict[str, object]:
    # Kraken v2 book delivers an in-stream snapshot + updates plus a per-frame CRC32
    # checksum. For a pair whose native precision we know, validate that checksum at
    # replay time (provable integrity, gap_detection="checksum"); otherwise fall back
    # to none_native — structurally clean only (STANDARDS 4.3).
    return await _collect_depth_stream_segment(
        args,
        source="kraken",
        websocket_url="wss://ws.kraken.com/v2",
        subscription_style="kraken_v2",
        normalizer=KrakenDepthNormalizer(),
        source_base="kraken_depth",
        **_stream_depth_replay_kwargs("kraken", getattr(args, "symbol", None)),
    )


async def collect_mexc_depth_segment(args: argparse.Namespace) -> dict[str, object]:
    # MEXC limit (partial-book) depth pushes a full top-N book every frame (decoded
    # from protobuf), emitted as a snapshot anchor; replay_depth_stream_run validates
    # each as a structurally-clean book. No sequence_metadata_key / checksum is passed,
    # so the lane stays none_native: the per-frame `version` is preserved in metadata
    # for forensics but isn't used to prove gaplessness (independent full books, not a
    # delta chain). Same class as Coinbase depth (STANDARDS 4.3).
    topic = build_limit_depth_topic(
        channel=args.channel,
        symbol=str(args.symbol),
        depth=int(getattr(args, "depth", 20)),
    )
    return await _collect_depth_stream_segment(
        args,
        source="mexc",
        websocket_url=MEXC_WS_URL,
        subscription_style="mexc",
        normalizer=MexcDepthNormalizer(),
        source_base="mexc_depth",
        ping_message=MEXC_PING_MESSAGE,
        ping_interval_seconds=MEXC_PING_INTERVAL_SECONDS,
        message_decoder=decode_mexc_frame,
        channel_override=topic,
    )


async def _collect_depth_stream_segment(
    args: argparse.Namespace,
    *,
    source: str,
    websocket_url: str,
    subscription_style: str,
    normalizer: object,
    source_base: str,
    ping_message: dict | str | None = None,
    ping_interval_seconds: float = 0.0,
    sequence_metadata_key: str | None = None,
    chain_sequence: bool = False,
    checksum_metadata_key: str | None = None,
    checksum_price_precision: int | None = None,
    checksum_qty_precision: int | None = None,
    book_depth: int | None = None,
    message_decoder=None,
    channel_override: str | None = None,
) -> dict[str, object]:
    """Venue-agnostic in-stream-snapshot depth segment.

    Unlike Binance depth (REST snapshot + U/u reconnect-in-place alignment), these
    feeds deliver the snapshot in-stream and carry no sequence, so there's nothing to
    align across a reconnect — a reconnect simply yields a second in-stream snapshot,
    which `replay_depth_stream_run` flags so the run is curated as one clean book or
    not at all. That makes the depth stream a perfect fit for the same generic
    pipeline the trades lanes use; only the normalizer, the depth-specific
    `MetadataQualityGate`, the normalized dataset root and the none-native replay
    differ from `_collect_trades_segment`. `ping_message`/`ping_interval_seconds` opt
    a venue into the collector's app-level keepalive (Bybit only); default off leaves
    every other venue unchanged."""
    config = CollectorConfig(
        source=source,
        output_root=args.output_root,
        product=args.symbol,
        channel=channel_override or args.channel,
        websocket_url=websocket_url,
        subscription_style=subscription_style,
        ping_message=ping_message,
        ping_interval_seconds=ping_interval_seconds,
        idle_timeout_seconds=float(getattr(args, "idle_timeout_seconds", 0.0) or 0.0),
        message_decoder=message_decoder,
    )
    collector = GenericWebsocketCollector(config=config)
    source_name = _build_source_name(source_base, getattr(args, "source_suffix", ""))
    run_paths = prepare_run_paths(output_root=config.output_root, source=source_name)
    fsync_events, fsync_ms = _fsync_intervals(args)
    pipeline = CollectorPipeline(
        collector=collector,
        normalizer=normalizer,
        # Depth events carry no top-level price/size/side, so the trades QualityGate
        # doesn't apply; MetadataQualityGate gates on parse errors (and update-range
        # for sequence feeds), which is the right bar for a depth diff stream.
        quality_gate=MetadataQualityGate(),
        run_paths=run_paths,
        normalized_root=_resolve_normalized_root(args, "market"),
        jsonl_fsync=bool(getattr(args, "jsonl_fsync", True)),
        fsync_interval_events=fsync_events,
        fsync_interval_ms=fsync_ms,
    )
    pipeline_summary = await pipeline.run(
        limit=args.count,
        deadline_utc=getattr(args, "deadline_utc", None),
    )

    events_path = run_paths.base / "clean" / "events.jsonl"
    if events_path.exists():
        replay_summary = replay_depth_stream_run(
            run_paths.base,
            write_summary=True,
            sequence_metadata_key=sequence_metadata_key,
            chain_sequence=chain_sequence,
            checksum_metadata_key=checksum_metadata_key,
            checksum_price_precision=checksum_price_precision,
            checksum_qty_precision=checksum_qty_precision,
            book_depth=book_depth,
        )
        replayable = replay_summary.replayable
        replay_findings = list(replay_summary.findings)
        replay_summary_path = replay_summary.summary_path
    else:
        replayable = False
        replay_findings = ["no_clean_events"]
        replay_summary_path = None

    return {
        "raw_messages": pipeline_summary.raw_messages,
        "clean_events": pipeline_summary.clean_events,
        "quarantined_events": pipeline_summary.quarantined_events,
        "run_path": str(run_paths.base),
        "replayable": replayable,
        "replay_findings": replay_findings,
        "replay_summary_path": replay_summary_path,
        "deadline_reached": bool(pipeline_summary.deadline_reached),
        "idle_timeout_count": collector.idle_timeout_count,
    }


_BINANCE_FUTURES_REST_STREAMS = ("trades", "depth", "funding")


def _binance_futures_rest_stream(args: argparse.Namespace) -> str:
    stream = str(getattr(args, "stream", "trades") or "trades").lower()
    if stream not in _BINANCE_FUTURES_REST_STREAMS:
        raise SystemExit(
            f"--stream must be one of {', '.join(_BINANCE_FUTURES_REST_STREAMS)} (got {stream!r})"
        )
    return stream


async def collect_binance_futures_rest_segment(args: argparse.Namespace) -> dict[str, object]:
    """Collect Binance USDT-M futures via REST polling. fstream (WS) market data is blocked
    in some jurisdictions while the fapi REST data API works, so this lane polls REST and
    feeds the same pipeline + curation chain as every WS lane. One worker, three streams
    selected by --stream:

      trades  -> gapless aggTrades (fromId paging; `a` is a dense counter so the run is a
                 provable `sequence` feed via replay_trades_run). The pager resumes from the
                 previous segment's last durably-written `a` (persisted per lane under
                 _cursors/), so rotations stay gapless and overlap-free. perp:binance-futures:*
      depth   -> per-poll full-book snapshots (none_native, replay_depth_stream_run) — same
                 model as the MEXC limit-depth lane; lower fidelity than a WS L2 delta stream
      funding -> premiumIndex mark/index/funding metric (none_native, replay_funding_run)
    """
    stream = _binance_futures_rest_stream(args)
    symbol = str(args.symbol).upper()
    poll_interval = float(getattr(args, "poll_interval_seconds", 1.0) or 1.0)

    if stream == "trades":
        normalizer: object = BinanceTradeNormalizer(instrument_type="perp")
        source_base = "binance_perp_trades"
        normalized_dataset = "trades"
    elif stream == "depth":
        poll = make_depth_poll(symbol, limit=int(getattr(args, "depth", 1000)))
        normalizer = BinanceDepthNormalizer(instrument_type="perp")
        source_base = "binance_perp_depth"
        normalized_dataset = "market"
    else:  # funding
        poll = make_funding_poll(symbol)
        normalizer = BinanceFuturesFundingNormalizer()
        source_base = "binance_perp_funding"
        normalized_dataset = "funding"

    source_name = _build_source_name(source_base, getattr(args, "source_suffix", ""))
    run_paths = prepare_run_paths(output_root=args.output_root, source=source_name)

    # The aggTrades lane resumes its dense-`a` pager from the previous segment's last
    # durably-written id, so segment rotations stay gapless and overlap-free (see
    # binance_futures_rest.aggtrades_resume_from_id). Depth/funding are stateless snapshots
    # and need no cursor.
    cursor_path = aggtrades_cursor_path(args.output_root, source_name)
    cursor_findings: list[str] = []
    if stream == "trades":
        resume_now = datetime.now(tz=UTC)
        max_resume_gap_seconds = float(getattr(args, "max_resume_gap_seconds", 21_600.0))
        initial_from_id, reset_finding = aggtrades_resume_from_id(
            read_aggtrades_cursor(cursor_path),
            symbol=symbol,
            now=resume_now,
            max_resume_gap_seconds=max_resume_gap_seconds,
        )
        if reset_finding:
            cursor_findings.append(reset_finding)
        # Crash-resume duplicate guard: the cursor only advances when a segment
        # completes normally, so after a hard kill the durable high-water on disk
        # can run AHEAD of the cursor — resuming from the cursor would re-fetch
        # that range into a new run and promotion (run-keyed, no row dedup) would
        # curate it twice. Raise the floor to the highest id already written in
        # the lane's recent runs (age-bounded by the same resume-gap rule).
        disk_high = max_agg_id_in_recent_runs(
            args.output_root,
            source_name,
            now=resume_now,
            max_age_seconds=max_resume_gap_seconds,
        )
        if disk_high is not None and (initial_from_id is None or disk_high + 1 > initial_from_id):
            initial_from_id = disk_high + 1
            cursor_findings.append("resume_floor_raised_from_disk")
        poll = make_aggtrades_poll(
            symbol,
            page_limit=int(getattr(args, "page_limit", 1000)),
            initial_from_id=initial_from_id,
        )

    collector = RestPollingCollector(
        source="binance-futures", poll=poll, poll_interval_seconds=poll_interval
    )
    if stream == "trades":
        quality_gate: object = QualityGate(
            max_delay_ms=int(getattr(args, "max_delay_ms", 60_000)),
            max_future_skew_ms=int(getattr(args, "max_future_skew_ms", 5_000)),
            session_id=run_paths.base.name,
        )
    else:
        # Depth events carry no top-level price/size; funding has no size. The trades
        # gate doesn't apply — MetadataQualityGate gates on parse errors, the right bar.
        quality_gate = MetadataQualityGate()

    fsync_events, fsync_ms = _fsync_intervals(args)
    pipeline = CollectorPipeline(
        collector=collector,
        normalizer=normalizer,
        quality_gate=quality_gate,
        run_paths=run_paths,
        normalized_root=(
            _resolve_normalized_root(args, normalized_dataset)
            if bool(getattr(args, "normalized_parquet", True))
            else None
        ),
        jsonl_fsync=bool(getattr(args, "jsonl_fsync", True)),
        fsync_interval_events=fsync_events,
        fsync_interval_ms=fsync_ms,
    )
    pipeline_summary = await pipeline.run(
        limit=args.count, deadline_utc=getattr(args, "deadline_utc", None)
    )

    events_path = run_paths.base / "clean" / "events.jsonl"
    if events_path.exists():
        if stream == "depth":
            replay_summary = replay_depth_stream_run(run_paths.base, write_summary=True)
        elif stream == "funding":
            replay_summary = replay_funding_run(
                run_paths.base,
                max_clock_skew_ms=float(getattr(args, "max_clock_skew_ms", 60_000.0)),
                write_summary=True,
            )
        else:
            replay_summary = replay_trades_run(
                run_paths.base,
                max_clock_skew_ms=float(getattr(args, "max_clock_skew_ms", 60_000.0)),
                write_summary=True,
            )
        replayable = replay_summary.replayable
        replay_findings = list(replay_summary.findings)
        replay_summary_path = replay_summary.summary_path
    else:
        replayable = False
        replay_findings = ["no_clean_events"]
        replay_summary_path = None

    # Advance the lane cursor only after this segment's clean events are durably on disk,
    # to the highest id actually written (not the pager's fetched high-water, which can run
    # ahead of what was persisted when a segment ends mid-batch). This keeps resume
    # at-least-once: a crash before this point leaves the cursor unchanged, so the next
    # segment re-fetches — risking a bounded dup, never a gap.
    if stream == "trades" and events_path.exists():
        highest_agg_id = max_agg_id_in_events(events_path)
        if highest_agg_id is not None:
            write_aggtrades_cursor(cursor_path, symbol=symbol, last_agg_id=highest_agg_id)

    return {
        "raw_messages": pipeline_summary.raw_messages,
        "clean_events": pipeline_summary.clean_events,
        "quarantined_events": pipeline_summary.quarantined_events,
        "run_path": str(run_paths.base),
        "replayable": replayable,
        "replay_findings": replay_findings,
        "replay_summary_path": replay_summary_path,
        "cursor_findings": cursor_findings,
        "deadline_reached": bool(pipeline_summary.deadline_reached),
        "idle_timeout_count": collector.idle_timeout_count,
    }


def run_binance_futures_rest_worker(args: argparse.Namespace) -> None:
    _run_segmented_worker(
        args=args,
        default_worker_name="binance-futures-rest-worker",
        worker_type="binance-futures-rest-worker",
        venue="binance-futures",
        build_segment_args=lambda source_args: SimpleNamespace(
            symbol=source_args.symbol,
            stream=getattr(source_args, "stream", "trades"),
            poll_interval_seconds=getattr(source_args, "poll_interval_seconds", 1.0),
            page_limit=getattr(source_args, "page_limit", 1000),
            max_resume_gap_seconds=getattr(source_args, "max_resume_gap_seconds", 21_600.0),
            depth=getattr(source_args, "depth", 1000),
            count=source_args.segment_count,
            output_root=source_args.output_root,
            # Defaults track the resume gap (see the bfr_parser comment): a 60s gate
            # quarantined every cursor-resumed backfill after an outage.
            max_delay_ms=getattr(source_args, "max_delay_ms", 21_600_000),
            max_future_skew_ms=getattr(source_args, "max_future_skew_ms", 5_000),
            max_clock_skew_ms=getattr(source_args, "max_clock_skew_ms", 21_600_000.0),
            jsonl_fsync=getattr(source_args, "jsonl_fsync", True),
            normalized_parquet=getattr(source_args, "normalized_parquet", True),
            source_suffix=getattr(source_args, "source_suffix", ""),
            deadline_utc=None,  # _run_segmented_worker overrides this when rotate_at_midnight is set
        ),
        collect_segment=collect_binance_futures_rest_segment,
        progress_message=lambda segment_index, summary: (
            "binance futures rest segment finished: "
            f"segment={segment_index} clean_events={summary['clean_events']} "
            f"replayable={summary.get('replayable')} run_path={summary['run_path']}"
        ),
    )


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
            source_suffix=getattr(source_args, "source_suffix", ""),
            deadline_utc=None,  # _run_segmented_worker overrides this when rotate_at_midnight is set
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
        # A --market futures run captures perp:binance-futures:* data; its heartbeat
        # must group under that venue (the REST worker already does), not "binance".
        venue="binance-futures" if _binance_trades_market(args) == "futures" else "binance",
        build_segment_args=lambda source_args: SimpleNamespace(
            symbol=source_args.symbol,
            channel=source_args.channel,
            market=getattr(source_args, "market", "spot"),
            count=source_args.segment_count,
            output_root=source_args.output_root,
            max_delay_ms=source_args.max_delay_ms,
            max_future_skew_ms=getattr(source_args, "max_future_skew_ms", 5_000),
            max_clock_skew_ms=getattr(source_args, "max_clock_skew_ms", 60_000.0),
            source_suffix=getattr(source_args, "source_suffix", ""),
            deadline_utc=None,  # _run_segmented_worker overrides this when rotate_at_midnight is set
        ),
        collect_segment=collect_binance_trades_segment,
        progress_message=lambda segment_index, summary: (
            "binance trades segment finished: "
            f"segment={segment_index} clean_events={summary['clean_events']} "
            f"replayable={summary.get('replayable')} run_path={summary['run_path']}"
        ),
    )


def run_coinbase_trades_worker(args: argparse.Namespace) -> None:
    _run_segmented_worker(
        args=args,
        default_worker_name="coinbase-trades-worker",
        worker_type="coinbase-trades-worker",
        venue="coinbase",
        build_segment_args=lambda source_args: SimpleNamespace(
            symbol=source_args.symbol,
            channel=source_args.channel,
            count=source_args.segment_count,
            output_root=source_args.output_root,
            max_delay_ms=source_args.max_delay_ms,
            max_future_skew_ms=getattr(source_args, "max_future_skew_ms", 5_000),
            max_clock_skew_ms=getattr(source_args, "max_clock_skew_ms", 60_000.0),
            source_suffix=getattr(source_args, "source_suffix", ""),
            deadline_utc=None,  # _run_segmented_worker overrides this when rotate_at_midnight is set
        ),
        collect_segment=collect_coinbase_trades_segment,
        progress_message=lambda segment_index, summary: (
            "coinbase trades segment finished: "
            f"segment={segment_index} clean_events={summary['clean_events']} "
            f"replayable={summary.get('replayable')} run_path={summary['run_path']}"
        ),
    )


def run_coinbase_depth_worker(args: argparse.Namespace) -> None:
    _run_segmented_worker(
        args=args,
        default_worker_name="coinbase-depth-worker",
        worker_type="coinbase-depth-worker",
        venue="coinbase",
        build_segment_args=lambda source_args: SimpleNamespace(
            symbol=source_args.symbol,
            channel=source_args.channel,
            count=source_args.segment_count,
            output_root=source_args.output_root,
            source_suffix=getattr(source_args, "source_suffix", ""),
            deadline_utc=None,  # _run_segmented_worker overrides this when rotate_at_midnight is set
        ),
        collect_segment=collect_coinbase_depth_segment,
        progress_message=lambda segment_index, summary: (
            "coinbase depth segment finished: "
            f"segment={segment_index} clean_events={summary['clean_events']} "
            f"replayable={summary.get('replayable')} run_path={summary['run_path']}"
        ),
    )


def run_kraken_trades_worker(args: argparse.Namespace) -> None:
    _run_segmented_worker(
        args=args,
        default_worker_name="kraken-trades-worker",
        worker_type="kraken-trades-worker",
        venue="kraken",
        build_segment_args=lambda source_args: SimpleNamespace(
            symbol=source_args.symbol,
            channel=source_args.channel,
            count=source_args.segment_count,
            output_root=source_args.output_root,
            max_delay_ms=source_args.max_delay_ms,
            max_future_skew_ms=getattr(source_args, "max_future_skew_ms", 5_000),
            max_clock_skew_ms=getattr(source_args, "max_clock_skew_ms", 60_000.0),
            source_suffix=getattr(source_args, "source_suffix", ""),
            deadline_utc=None,  # _run_segmented_worker overrides this when rotate_at_midnight is set
        ),
        collect_segment=collect_kraken_trades_segment,
        progress_message=lambda segment_index, summary: (
            "kraken trades segment finished: "
            f"segment={segment_index} clean_events={summary['clean_events']} "
            f"replayable={summary.get('replayable')} run_path={summary['run_path']}"
        ),
    )


def run_okx_trades_worker(args: argparse.Namespace) -> None:
    _run_segmented_worker(
        args=args,
        default_worker_name="okx-trades-worker",
        worker_type="okx-trades-worker",
        venue="okx",
        build_segment_args=lambda source_args: SimpleNamespace(
            symbol=source_args.symbol,
            channel=source_args.channel,
            # `market` MUST be carried explicitly — the enumeration trap that once
            # dropped it for bybit (PR #6) silently ran perp lanes as spot.
            market=getattr(source_args, "market", "spot"),
            count=source_args.segment_count,
            output_root=source_args.output_root,
            max_delay_ms=source_args.max_delay_ms,
            max_future_skew_ms=getattr(source_args, "max_future_skew_ms", 5_000),
            max_clock_skew_ms=getattr(source_args, "max_clock_skew_ms", 60_000.0),
            source_suffix=getattr(source_args, "source_suffix", ""),
            deadline_utc=None,  # _run_segmented_worker overrides this when rotate_at_midnight is set
        ),
        collect_segment=collect_okx_trades_segment,
        progress_message=lambda segment_index, summary: (
            "okx trades segment finished: "
            f"segment={segment_index} clean_events={summary['clean_events']} "
            f"replayable={summary.get('replayable')} run_path={summary['run_path']}"
        ),
    )


def run_okx_depth_worker(args: argparse.Namespace) -> None:
    _run_segmented_worker(
        args=args,
        default_worker_name="okx-depth-worker",
        worker_type="okx-depth-worker",
        venue="okx",
        build_segment_args=lambda source_args: SimpleNamespace(
            symbol=source_args.symbol,
            channel=source_args.channel,
            market=getattr(source_args, "market", "spot"),
            count=source_args.segment_count,
            output_root=source_args.output_root,
            source_suffix=getattr(source_args, "source_suffix", ""),
            deadline_utc=None,  # _run_segmented_worker overrides this when rotate_at_midnight is set
        ),
        collect_segment=collect_okx_depth_segment,
        progress_message=lambda segment_index, summary: (
            "okx depth segment finished: "
            f"segment={segment_index} clean_events={summary['clean_events']} "
            f"replayable={summary.get('replayable')} run_path={summary['run_path']}"
        ),
    )


def run_bybit_trades_worker(args: argparse.Namespace) -> None:
    _run_segmented_worker(
        args=args,
        default_worker_name="bybit-trades-worker",
        worker_type="bybit-trades-worker",
        venue="bybit",
        build_segment_args=lambda source_args: SimpleNamespace(
            symbol=source_args.symbol,
            channel=source_args.channel,
            market=getattr(source_args, "market", "spot"),
            count=source_args.segment_count,
            output_root=source_args.output_root,
            max_delay_ms=source_args.max_delay_ms,
            max_future_skew_ms=getattr(source_args, "max_future_skew_ms", 5_000),
            max_clock_skew_ms=getattr(source_args, "max_clock_skew_ms", 60_000.0),
            source_suffix=getattr(source_args, "source_suffix", ""),
            deadline_utc=None,  # _run_segmented_worker overrides this when rotate_at_midnight is set
        ),
        collect_segment=collect_bybit_trades_segment,
        progress_message=lambda segment_index, summary: (
            "bybit trades segment finished: "
            f"segment={segment_index} clean_events={summary['clean_events']} "
            f"replayable={summary.get('replayable')} run_path={summary['run_path']}"
        ),
    )


def run_bybit_depth_worker(args: argparse.Namespace) -> None:
    _run_segmented_worker(
        args=args,
        default_worker_name="bybit-depth-worker",
        worker_type="bybit-depth-worker",
        venue="bybit",
        build_segment_args=lambda source_args: SimpleNamespace(
            symbol=source_args.symbol,
            channel=source_args.channel,
            market=getattr(source_args, "market", "spot"),
            count=source_args.segment_count,
            output_root=source_args.output_root,
            source_suffix=getattr(source_args, "source_suffix", ""),
            deadline_utc=None,  # _run_segmented_worker overrides this when rotate_at_midnight is set
        ),
        collect_segment=collect_bybit_depth_segment,
        progress_message=lambda segment_index, summary: (
            "bybit depth segment finished: "
            f"segment={segment_index} clean_events={summary['clean_events']} "
            f"replayable={summary.get('replayable')} run_path={summary['run_path']}"
        ),
    )


def run_kraken_depth_worker(args: argparse.Namespace) -> None:
    _run_segmented_worker(
        args=args,
        default_worker_name="kraken-depth-worker",
        worker_type="kraken-depth-worker",
        venue="kraken",
        build_segment_args=lambda source_args: SimpleNamespace(
            symbol=source_args.symbol,
            channel=source_args.channel,
            count=source_args.segment_count,
            output_root=source_args.output_root,
            source_suffix=getattr(source_args, "source_suffix", ""),
            deadline_utc=None,  # _run_segmented_worker overrides this when rotate_at_midnight is set
        ),
        collect_segment=collect_kraken_depth_segment,
        progress_message=lambda segment_index, summary: (
            "kraken depth segment finished: "
            f"segment={segment_index} clean_events={summary['clean_events']} "
            f"replayable={summary.get('replayable')} run_path={summary['run_path']}"
        ),
    )


def run_mexc_trades_worker(args: argparse.Namespace) -> None:
    _run_segmented_worker(
        args=args,
        default_worker_name="mexc-trades-worker",
        worker_type="mexc-trades-worker",
        venue="mexc",
        build_segment_args=lambda source_args: SimpleNamespace(
            symbol=source_args.symbol,
            channel=source_args.channel,
            interval=getattr(source_args, "interval", "100ms"),
            count=source_args.segment_count,
            output_root=source_args.output_root,
            max_delay_ms=source_args.max_delay_ms,
            max_future_skew_ms=getattr(source_args, "max_future_skew_ms", 5_000),
            max_clock_skew_ms=getattr(source_args, "max_clock_skew_ms", 60_000.0),
            source_suffix=getattr(source_args, "source_suffix", ""),
            deadline_utc=None,  # _run_segmented_worker overrides this when rotate_at_midnight is set
        ),
        collect_segment=collect_mexc_trades_segment,
        progress_message=lambda segment_index, summary: (
            "mexc trades segment finished: "
            f"segment={segment_index} clean_events={summary['clean_events']} "
            f"replayable={summary.get('replayable')} run_path={summary['run_path']}"
        ),
    )


def run_mexc_depth_worker(args: argparse.Namespace) -> None:
    _run_segmented_worker(
        args=args,
        default_worker_name="mexc-depth-worker",
        worker_type="mexc-depth-worker",
        venue="mexc",
        build_segment_args=lambda source_args: SimpleNamespace(
            symbol=source_args.symbol,
            channel=source_args.channel,
            depth=getattr(source_args, "depth", 20),
            count=source_args.segment_count,
            output_root=source_args.output_root,
            source_suffix=getattr(source_args, "source_suffix", ""),
            deadline_utc=None,  # _run_segmented_worker overrides this when rotate_at_midnight is set
        ),
        collect_segment=collect_mexc_depth_segment,
        progress_message=lambda segment_index, summary: (
            "mexc depth segment finished: "
            f"segment={segment_index} clean_events={summary['clean_events']} "
            f"replayable={summary.get('replayable')} run_path={summary['run_path']}"
        ),
    )


def _segment_deadline_utc(
    started_at: datetime,
    *,
    rotate_at_midnight: bool,
    max_segment_seconds: float,
) -> datetime | None:
    """Wall-clock deadline at which the current segment rotates cleanly (the stream
    loop checks this and finalizes — flush/replay-summary/metrics all still run).
    `rotate_at_midnight` (day-bounded files) takes precedence; otherwise a positive
    `max_segment_seconds` bounds each segment by TIME so a lane rotates on a fixed
    cadence regardless of message volume — this is what lets every lane record
    continuously (one finalized segment per window, no per-hour idle gap). A
    zero/None `max_segment_seconds` with no midnight rotation = no time bound."""
    if rotate_at_midnight:
        return _next_utc_midnight(started_at)
    if max_segment_seconds and max_segment_seconds > 0:
        return started_at + timedelta(seconds=max_segment_seconds)
    return None


def _run_segmented_worker(
    *,
    args: argparse.Namespace,
    default_worker_name: str,
    worker_type: str,
    build_segment_args,
    collect_segment,
    progress_message,
    venue: str = "binance",
) -> None:
    worker_name = str(getattr(args, "worker_name", default_worker_name) or default_worker_name)
    runtime = StandaloneWorkerRuntime(
        args.ops_root,
        worker_name=worker_name,
        worker_type=worker_type,
        venue=venue,
        symbol=str(args.symbol).upper(),
        heartbeat_interval_seconds=float(getattr(args, "heartbeat_interval_seconds", 30.0)),
    )
    completed_segments = 0
    last_run_path: str | None = None
    rotate_at_midnight = bool(getattr(args, "rotate_at_midnight", False))
    with StandaloneWorkerLock(args.ops_root, worker_name=worker_name):
        runtime.record_event(
            "worker_started",
            details={
                "max_segments": args.max_segments,
                "output_root": str(args.output_root),
                "rotate_at_midnight": rotate_at_midnight,
            },
        )
        runtime.write_heartbeat(status="idle", last_segment_index=0)
        try:
            while args.max_segments is None or completed_segments < args.max_segments:
                segment_index = completed_segments + 1
                segment_started_at = utc_now()
                # A segment may rotate on a wall-clock deadline instead of (or before)
                # --segment-count: day-bounded (midnight) or a fixed max_segment_seconds
                # cadence for continuous capture. The segment function checks the deadline
                # in its inner stream loop and exits cleanly when crossed, so the parquet
                # flush / replay summary / metrics write all still run.
                segment_deadline_utc = _segment_deadline_utc(
                    segment_started_at,
                    rotate_at_midnight=rotate_at_midnight,
                    max_segment_seconds=float(getattr(args, "max_segment_seconds", 0.0) or 0.0),
                )
                stop_event, heartbeat_thread = runtime.start_segment_heartbeat(
                    segment_index=segment_index,
                    started_at=segment_started_at,
                    last_segment_index=completed_segments,
                    last_run_path=last_run_path,
                )
                try:
                    segment_args = build_segment_args(args)
                    if segment_deadline_utc is not None:
                        segment_args.deadline_utc = segment_deadline_utc
                    # Thread the per-lane data-arrival watchdog timeout onto each segment
                    # (0.0 = off). Centralized here so every build_segment_args stays
                    # untouched; the generic-WS segments read it via getattr.
                    segment_args.idle_timeout_seconds = float(
                        getattr(args, "idle_timeout_seconds", 0.0) or 0.0
                    )
                    # Thread the JSONL durability posture onto every segment centrally —
                    # like idle_timeout_seconds — so no per-worker build_segment_args
                    # lambda can silently drop it (the same enumeration trap that dropped
                    # `market`). fsync defaults on and BATCHED, so a hot lane gets
                    # crash-durable writes without per-event fsync capping its throughput.
                    segment_args.jsonl_fsync = bool(getattr(args, "jsonl_fsync", True))
                    fsync_events, fsync_ms = _fsync_intervals(args)
                    segment_args.fsync_interval_events = fsync_events
                    segment_args.fsync_interval_ms = fsync_ms
                    # Thread the normalized-parquet root centrally too: this was the one
                    # output path NOT carried by the ops config, so the 2026-06-08 disk
                    # migration missed it and workers kept writing normalized parquet to
                    # the retired drive via the env/default fallback.
                    segment_args.normalized_root = getattr(args, "normalized_root", None)
                    # And the normalized-parquet on/off toggle: it was enumerated by
                    # only one build_segment_args lambda, so --no-normalized-parquet
                    # (and the config key) was silently inert on every other lane.
                    segment_args.normalized_parquet = bool(
                        getattr(args, "normalized_parquet", True)
                    )
                    # Binance-depth-only snapshot-anchor budget, threaded centrally per
                    # the repo rule (lambdas drop fields); other segments ignore it.
                    segment_args.snapshot_anchor_timeout_seconds = float(
                        getattr(args, "snapshot_anchor_timeout_seconds", 10.0)
                    )
                    summary = asyncio.run(collect_segment(segment_args))
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
        runner = OpsRunner(
            args.ops_root,
            runner_name="market-data-plant",
            poll_seconds=args.poll_seconds,
            collector_concurrency=getattr(args, "collector_concurrency", 1),
        )
        executed = runner.run(jobs, execute_job=_execute_ops_job, max_runs=args.max_runs, stop_on_error=args.stop_on_error)
    print(f"ops runner finished: {executed} job runs -> {args.ops_root}")


# Collector lanes run in their OWN OS process for true parallelism: the runner's pool is
# thread-based, and the GIL otherwise serializes the per-event work of concurrent
# high-volume collectors, which backs up their feeds and quarantines valid-but-late trades
# (coinbase hit ~0.39 in-thread vs ~0.0 isolated). The supervising pool thread just waits
# on the subprocess. Maintenance jobs (quarantine/promote/manifest/health/...) stay
# in-process — they're light and already serialized by the runner.
_COLLECTOR_SUBPROCESS_TIMEOUT_SECONDS = 7200.0


def _execute_ops_job(job: JobSpec) -> JobExecutionResult | str | None:
    if job.job_type in COLLECTOR_JOB_TYPES:
        return _run_collector_in_subprocess(job)
    return _execute_ops_job_inprocess(job)


def _collector_subprocess_timeout_seconds(job: JobSpec) -> float:
    """Per-job subprocess timeout. The 7200s default is sized for 1800s WS segments;
    a short-interval poll job (kalshi quote sampling runs every ~60s) hanging for two
    hours would silently gap its lane while holding a pool slot, so non-segmented
    jobs get a timeout scaled to their cadence instead. An explicit
    `subprocess_timeout_seconds` job arg wins either way."""
    configured = job.args.get("subprocess_timeout_seconds")
    if configured is not None:
        return float(configured)
    if job.args.get("max_segment_seconds") or job.job_type.endswith("-worker"):
        return _COLLECTOR_SUBPROCESS_TIMEOUT_SECONDS
    interval = max(0, int(job.interval_seconds))
    return min(_COLLECTOR_SUBPROCESS_TIMEOUT_SECONDS, max(300.0, 4.0 * interval))


def _run_collector_in_subprocess(job: JobSpec) -> str:
    """Run one collector segment in a fresh `crypto_collector.cli run-job` process so
    concurrent collectors don't contend on the GIL. Raises on non-zero exit / timeout so
    the runner records an error result (JobExecutionResult carries no status field)."""
    payload = json.dumps(
        {
            "name": job.name,
            "job_type": job.job_type,
            "interval_seconds": job.interval_seconds,
            "args": job.args,
        }
    )
    timeout_seconds = _collector_subprocess_timeout_seconds(job)
    try:
        proc = subprocess.run(
            [sys.executable, "-m", "crypto_collector.cli", "run-job", "--job-json", payload],
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            f"collector subprocess for {job.name} timed out after "
            f"{timeout_seconds:.0f}s"
        ) from exc
    if proc.returncode != 0:
        tail = (proc.stderr or proc.stdout or "").strip()[-800:]
        raise RuntimeError(
            f"collector subprocess for {job.name} ({job.job_type}) exited "
            f"{proc.returncode}: {tail}"
        )
    return f"{job.job_type} completed (process-isolated)"


def run_single_job(args: argparse.Namespace) -> None:
    """Child entrypoint for process-isolated collection: reconstruct one JobSpec from JSON
    and run it in THIS process. Non-zero exit on failure (the worker raises)."""
    payload = json.loads(args.job_json)
    job = JobSpec(
        name=str(payload.get("name", "job")),
        job_type=str(payload["job_type"]),
        interval_seconds=int(payload.get("interval_seconds", 0)),
        args=dict(payload.get("args", {})),
    )
    _execute_ops_job_inprocess(job)


def _execute_ops_job_inprocess(job: JobSpec) -> JobExecutionResult | str | None:
    args = _job_args(job)
    # Time-based segment rotation knob for continuous-capture collectors. Injected here
    # (rather than in each of the 10 _job_args builders) so every collector lane picks it
    # up uniformly; _run_segmented_worker reads it via getattr and ignores it when
    # unset/zero, and non-collector jobs simply never look at it.
    args.max_segment_seconds = job.args.get("max_segment_seconds")
    # Batched-fsync cadence knobs, injected here (rather than in each of the _job_args
    # builders) so every collector lane picks them up uniformly; unset -> None, which the
    # worker / pipeline resolves to the safe pipeline default. Non-collector jobs ignore
    # them.
    args.fsync_interval_events = job.args.get("fsync_interval_events")
    args.fsync_interval_ms = job.args.get("fsync_interval_ms")
    # Same uniform injection for the fsync ON/OFF switch and the normalized-parquet
    # toggle. These existed in the parsers and in SOME _job_args builders (trades yes,
    # depth no), so a config `jsonl_fsync: false` on a depth lane silently stayed
    # durable and `normalized_parquet: false` was dropped entirely — the per-job-type
    # enumeration trap, killed centrally like the cadence knobs above. Only override
    # when the config actually sets the key, so _job_args/parser defaults still win.
    if "jsonl_fsync" in job.args:
        args.jsonl_fsync = bool(job.args["jsonl_fsync"])
    if "normalized_parquet" in job.args:
        args.normalized_parquet = bool(job.args["normalized_parquet"])
    if job.job_type == "mock":
        asyncio.run(run_mock(args))
        return "mock completed"
    if job.job_type == "binance-depth-worker":
        run_binance_depth_worker(args)
        return "binance depth worker completed"
    if job.job_type == "binance-trades-worker":
        run_binance_trades_worker(args)
        return "binance trades worker completed"
    if job.job_type == "binance-futures-rest-worker":
        run_binance_futures_rest_worker(args)
        return "binance futures rest worker completed"
    if job.job_type == "coinbase-trades-worker":
        run_coinbase_trades_worker(args)
        return "coinbase trades worker completed"
    if job.job_type == "coinbase-depth-worker":
        run_coinbase_depth_worker(args)
        return "coinbase depth worker completed"
    if job.job_type == "kraken-trades-worker":
        run_kraken_trades_worker(args)
        return "kraken trades worker completed"
    if job.job_type == "bybit-trades-worker":
        run_bybit_trades_worker(args)
        return "bybit trades worker completed"
    if job.job_type == "bybit-depth-worker":
        run_bybit_depth_worker(args)
        return "bybit depth worker completed"
    if job.job_type == "okx-trades-worker":
        run_okx_trades_worker(args)
        return "okx trades worker completed"
    if job.job_type == "okx-depth-worker":
        run_okx_depth_worker(args)
        return "okx depth worker completed"
    if job.job_type == "kraken-depth-worker":
        run_kraken_depth_worker(args)
        return "kraken depth worker completed"
    if job.job_type == "mexc-trades-worker":
        run_mexc_trades_worker(args)
        return "mexc trades worker completed"
    if job.job_type == "mexc-depth-worker":
        run_mexc_depth_worker(args)
        return "mexc depth worker completed"
    if job.job_type == "kalshi-discover-crypto":
        run_kalshi_discover_crypto(args)
        return "kalshi crypto discovery completed"
    if job.job_type == "kalshi-collect-crypto-quotes":
        run_kalshi_collect_crypto_quotes(args)
        return "kalshi crypto quote collection completed"
    if job.job_type == "kalshi-summarize-crypto-quotes":
        run_kalshi_summarize_crypto_quotes(args)
        return "kalshi crypto quote summary completed"
    if job.job_type == "book-sync-health":
        run_book_sync_health(args)
        return "book sync health completed"
    if job.job_type == "backfill-replay":
        run_backfill_replay(args)
        return "backfill replay completed"
    if job.job_type == "backfill-trades-replay":
        run_backfill_trades_replay(args)
        return "backfill trades replay completed"
    if job.job_type == "backfill-stream-depth":
        run_backfill_stream_depth(args)
        return "backfill stream depth completed"
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
    if job.job_type == "archive-offload":
        run_archive_offload(args)
        return "archive offload completed"
    raise ValueError(f"Unsupported job_type: {job.job_type}")


# Trades freshness (stale) gate. This archive is replayed by exchange_time, so a
# late-but-valid trade is GOOD data, not garbage. Under disk-I/O backlog (concurrent
# collectors writing D:\market_archive), delivery lateness reaches ~500s on the
# high-volume lanes, so quarantining at 60s discards thousands of valid trades. Widen
# the stale bound to 15 min to KEEP them; the tight 5s future-skew bound (separate) still
# catches genuine clock skew. Tighten back toward 60s once on faster storage (NVMe), which
# removes the backlog at the source. Config can still override per lane via max_delay_ms.
_TRADES_STALE_WINDOW_MS = 900_000


def _job_args(job: JobSpec) -> SimpleNamespace:
    raw_args = dict(job.args)
    if "output_root" in raw_args:
        raw_args["output_root"] = Path(raw_args["output_root"])
    if "normalized_root" in raw_args:
        raw_args["normalized_root"] = Path(raw_args["normalized_root"])
    if "input_path" in raw_args:
        raw_args["input_path"] = Path(raw_args["input_path"])
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
            normalized_root=raw_args.get("normalized_root"),
            worker_name=raw_args.get("worker_name", "binance-depth-worker"),
            heartbeat_interval_seconds=raw_args.get("heartbeat_interval_seconds", 30.0),
            snapshot_limit=raw_args.get("snapshot_limit", 1000),
            connect_retries=raw_args.get("connect_retries", 3),
            retry_backoff_seconds=raw_args.get("retry_backoff_seconds", 2.0),
            max_backoff_seconds=raw_args.get("max_backoff_seconds", 60.0),
            snapshot_anchor_timeout_seconds=raw_args.get("snapshot_anchor_timeout_seconds", 10.0),
            snapshot_base_url=raw_args.get("snapshot_base_url", "https://api.binance.com/api/v3/depth"),
            # Phase 2 lane/rotation flags must flow through the ops-runner too — without
            # these the ETH lane example would silently collide with the BTC lane in
            # binance_depth/ because the segment builder reads them via getattr.
            source_suffix=raw_args.get("source_suffix", ""),
            rotate_at_midnight=raw_args.get("rotate_at_midnight", False),
            # Per-lane data-arrival watchdog (0.0 = off; see
            # CollectorConfig.idle_timeout_seconds). Honored by the generic-WS lanes;
            # the Binance depth lane runs its own socket loop and ignores it.
            idle_timeout_seconds=raw_args.get("idle_timeout_seconds", 0.0),
        )
    if job.job_type == "binance-trades-worker":
        return SimpleNamespace(
            symbol=raw_args.get("symbol", "btcusdt"),
            channel=raw_args.get("channel", "trade"),
            market=raw_args.get("market", "spot"),
            segment_count=raw_args.get("segment_count", 5000),
            max_segments=raw_args.get("max_segments"),
            cooldown_seconds=raw_args.get("cooldown_seconds", 1.0),
            output_root=raw_args.get("output_root", default_output_root()),
            ops_root=Path(raw_args.get("ops_root", default_ops_root())),
            normalized_root=raw_args.get("normalized_root"),
            worker_name=raw_args.get("worker_name", "binance-trades-worker"),
            heartbeat_interval_seconds=raw_args.get("heartbeat_interval_seconds", 30.0),
            max_delay_ms=raw_args.get("max_delay_ms", _TRADES_STALE_WINDOW_MS),
            max_future_skew_ms=raw_args.get("max_future_skew_ms", 5_000),
            max_clock_skew_ms=raw_args.get("max_clock_skew_ms", 60_000.0),
            source_suffix=raw_args.get("source_suffix", ""),
            rotate_at_midnight=raw_args.get("rotate_at_midnight", False),
            # Per-lane data-arrival watchdog (0.0 = off; see
            # CollectorConfig.idle_timeout_seconds). Honored by the generic-WS lanes;
            # the Binance depth lane runs its own socket loop and ignores it.
            idle_timeout_seconds=raw_args.get("idle_timeout_seconds", 0.0),
            jsonl_fsync=raw_args.get("jsonl_fsync", True),
            normalized_parquet=raw_args.get("normalized_parquet", True),
        )
    if job.job_type == "binance-futures-rest-worker":
        return SimpleNamespace(
            symbol=raw_args.get("symbol", "BTCUSDT"),
            stream=raw_args.get("stream", "trades"),
            poll_interval_seconds=raw_args.get("poll_interval_seconds", 1.0),
            page_limit=raw_args.get("page_limit", 1000),
            # Was silently absent here (the documented per-job-type enumeration trap):
            # a config-set max_resume_gap_seconds never reached the worker.
            max_resume_gap_seconds=raw_args.get("max_resume_gap_seconds", 21_600.0),
            depth=raw_args.get("depth", 1000),
            segment_count=raw_args.get("segment_count", 5000),
            max_segments=raw_args.get("max_segments"),
            cooldown_seconds=raw_args.get("cooldown_seconds", 1.0),
            output_root=raw_args.get("output_root", default_output_root()),
            ops_root=Path(raw_args.get("ops_root", default_ops_root())),
            normalized_root=raw_args.get("normalized_root"),
            worker_name=raw_args.get("worker_name", "binance-futures-rest-worker"),
            heartbeat_interval_seconds=raw_args.get("heartbeat_interval_seconds", 30.0),
            # Staleness windows track the resume gap (see the bfr_parser comment): a
            # 60s gate quarantined every cursor-resumed backfill after an outage.
            max_delay_ms=raw_args.get("max_delay_ms", 21_600_000),
            max_future_skew_ms=raw_args.get("max_future_skew_ms", 5_000),
            max_clock_skew_ms=raw_args.get("max_clock_skew_ms", 21_600_000.0),
            source_suffix=raw_args.get("source_suffix", ""),
            rotate_at_midnight=raw_args.get("rotate_at_midnight", False),
            jsonl_fsync=raw_args.get("jsonl_fsync", True),
            normalized_parquet=raw_args.get("normalized_parquet", True),
        )
    if job.job_type == "coinbase-trades-worker":
        return SimpleNamespace(
            symbol=raw_args.get("symbol", "BTC-USD"),
            channel=raw_args.get("channel", "matches"),
            segment_count=raw_args.get("segment_count", 5000),
            max_segments=raw_args.get("max_segments"),
            cooldown_seconds=raw_args.get("cooldown_seconds", 1.0),
            output_root=raw_args.get("output_root", default_output_root()),
            ops_root=Path(raw_args.get("ops_root", default_ops_root())),
            normalized_root=raw_args.get("normalized_root"),
            worker_name=raw_args.get("worker_name", "coinbase-trades-worker"),
            # Durable batched JSONL by default: fsync is now BATCHED in the pipeline (every
            # line is flushed, but the disk-blocking fsync is amortized over
            # fsync_interval_events / fsync_interval_ms), so a high-volume lane gets
            # crash-durable writes without the per-event-fsync throughput ceiling that used
            # to grow the backlog past the 60s freshness gate and quarantine valid trades as
            # stale. Opt all the way out to buffered, never-fsynced JSONL with jsonl_fsync:false.
            jsonl_fsync=raw_args.get("jsonl_fsync", True),
            heartbeat_interval_seconds=raw_args.get("heartbeat_interval_seconds", 30.0),
            max_delay_ms=raw_args.get("max_delay_ms", _TRADES_STALE_WINDOW_MS),
            max_future_skew_ms=raw_args.get("max_future_skew_ms", 5_000),
            max_clock_skew_ms=raw_args.get("max_clock_skew_ms", 60_000.0),
            source_suffix=raw_args.get("source_suffix", ""),
            rotate_at_midnight=raw_args.get("rotate_at_midnight", False),
            # Per-lane data-arrival watchdog (0.0 = off; see
            # CollectorConfig.idle_timeout_seconds). Honored by the generic-WS lanes;
            # the Binance depth lane runs its own socket loop and ignores it.
            idle_timeout_seconds=raw_args.get("idle_timeout_seconds", 0.0),
        )
    if job.job_type == "coinbase-depth-worker":
        return SimpleNamespace(
            # level2_50 is the unauthenticated public depth channel; the plain
            # level2/level2_batch channels now require Coinbase auth. level2_50 emits
            # the same snapshot/l2update frames the none-native depth replay validates.
            symbol=raw_args.get("symbol", "BTC-USD"),
            channel=raw_args.get("channel", "level2_50"),
            segment_count=raw_args.get("segment_count", 5000),
            max_segments=raw_args.get("max_segments"),
            cooldown_seconds=raw_args.get("cooldown_seconds", 1.0),
            output_root=raw_args.get("output_root", default_output_root()),
            ops_root=Path(raw_args.get("ops_root", default_ops_root())),
            normalized_root=raw_args.get("normalized_root"),
            worker_name=raw_args.get("worker_name", "coinbase-depth-worker"),
            heartbeat_interval_seconds=raw_args.get("heartbeat_interval_seconds", 30.0),
            source_suffix=raw_args.get("source_suffix", ""),
            rotate_at_midnight=raw_args.get("rotate_at_midnight", False),
            # Per-lane data-arrival watchdog (0.0 = off; see
            # CollectorConfig.idle_timeout_seconds). Honored by the generic-WS lanes;
            # the Binance depth lane runs its own socket loop and ignores it.
            idle_timeout_seconds=raw_args.get("idle_timeout_seconds", 0.0),
        )
    if job.job_type == "kraken-trades-worker":
        return SimpleNamespace(
            symbol=raw_args.get("symbol", "BTC/USD"),
            channel=raw_args.get("channel", "trade"),
            segment_count=raw_args.get("segment_count", 5000),
            max_segments=raw_args.get("max_segments"),
            cooldown_seconds=raw_args.get("cooldown_seconds", 1.0),
            output_root=raw_args.get("output_root", default_output_root()),
            ops_root=Path(raw_args.get("ops_root", default_ops_root())),
            normalized_root=raw_args.get("normalized_root"),
            worker_name=raw_args.get("worker_name", "kraken-trades-worker"),
            # Durable batched JSONL by default — see coinbase-trades-worker.
            jsonl_fsync=raw_args.get("jsonl_fsync", True),
            heartbeat_interval_seconds=raw_args.get("heartbeat_interval_seconds", 30.0),
            max_delay_ms=raw_args.get("max_delay_ms", _TRADES_STALE_WINDOW_MS),
            max_future_skew_ms=raw_args.get("max_future_skew_ms", 5_000),
            max_clock_skew_ms=raw_args.get("max_clock_skew_ms", 60_000.0),
            source_suffix=raw_args.get("source_suffix", ""),
            rotate_at_midnight=raw_args.get("rotate_at_midnight", False),
            # Per-lane data-arrival watchdog (0.0 = off; see
            # CollectorConfig.idle_timeout_seconds). Honored by the generic-WS lanes;
            # the Binance depth lane runs its own socket loop and ignores it.
            idle_timeout_seconds=raw_args.get("idle_timeout_seconds", 0.0),
        )
    if job.job_type == "bybit-trades-worker":
        return SimpleNamespace(
            symbol=raw_args.get("symbol", "BTCUSDT"),
            channel=raw_args.get("channel", "publicTrade"),
            market=raw_args.get("market", "spot"),
            segment_count=raw_args.get("segment_count", 5000),
            max_segments=raw_args.get("max_segments"),
            cooldown_seconds=raw_args.get("cooldown_seconds", 1.0),
            output_root=raw_args.get("output_root", default_output_root()),
            ops_root=Path(raw_args.get("ops_root", default_ops_root())),
            normalized_root=raw_args.get("normalized_root"),
            worker_name=raw_args.get("worker_name", "bybit-trades-worker"),
            # Durable batched JSONL by default — see coinbase-trades-worker.
            jsonl_fsync=raw_args.get("jsonl_fsync", True),
            heartbeat_interval_seconds=raw_args.get("heartbeat_interval_seconds", 30.0),
            max_delay_ms=raw_args.get("max_delay_ms", _TRADES_STALE_WINDOW_MS),
            max_future_skew_ms=raw_args.get("max_future_skew_ms", 5_000),
            max_clock_skew_ms=raw_args.get("max_clock_skew_ms", 60_000.0),
            source_suffix=raw_args.get("source_suffix", ""),
            rotate_at_midnight=raw_args.get("rotate_at_midnight", False),
            # Per-lane data-arrival watchdog (0.0 = off; see
            # CollectorConfig.idle_timeout_seconds). Honored by the generic-WS lanes;
            # the Binance depth lane runs its own socket loop and ignores it.
            idle_timeout_seconds=raw_args.get("idle_timeout_seconds", 0.0),
        )
    if job.job_type == "bybit-depth-worker":
        return SimpleNamespace(
            symbol=raw_args.get("symbol", "BTCUSDT"),
            # orderbook.<depth>; the symbol is appended to form the full topic.
            channel=raw_args.get("channel", "orderbook.50"),
            market=raw_args.get("market", "spot"),
            segment_count=raw_args.get("segment_count", 5000),
            max_segments=raw_args.get("max_segments"),
            cooldown_seconds=raw_args.get("cooldown_seconds", 1.0),
            output_root=raw_args.get("output_root", default_output_root()),
            ops_root=Path(raw_args.get("ops_root", default_ops_root())),
            normalized_root=raw_args.get("normalized_root"),
            worker_name=raw_args.get("worker_name", "bybit-depth-worker"),
            heartbeat_interval_seconds=raw_args.get("heartbeat_interval_seconds", 30.0),
            source_suffix=raw_args.get("source_suffix", ""),
            rotate_at_midnight=raw_args.get("rotate_at_midnight", False),
            # Per-lane data-arrival watchdog (0.0 = off; see
            # CollectorConfig.idle_timeout_seconds). Honored by the generic-WS lanes;
            # the Binance depth lane runs its own socket loop and ignores it.
            idle_timeout_seconds=raw_args.get("idle_timeout_seconds", 0.0),
        )
    if job.job_type == "okx-trades-worker":
        return SimpleNamespace(
            symbol=raw_args.get("symbol", "BTC-USDT"),
            channel=raw_args.get("channel", "trades"),
            market=raw_args.get("market", "spot"),
            segment_count=raw_args.get("segment_count", 5000),
            max_segments=raw_args.get("max_segments"),
            cooldown_seconds=raw_args.get("cooldown_seconds", 1.0),
            output_root=raw_args.get("output_root", default_output_root()),
            ops_root=Path(raw_args.get("ops_root", default_ops_root())),
            normalized_root=raw_args.get("normalized_root"),
            worker_name=raw_args.get("worker_name", "okx-trades-worker"),
            # Durable batched JSONL by default — see coinbase-trades-worker.
            jsonl_fsync=raw_args.get("jsonl_fsync", True),
            heartbeat_interval_seconds=raw_args.get("heartbeat_interval_seconds", 30.0),
            max_delay_ms=raw_args.get("max_delay_ms", _TRADES_STALE_WINDOW_MS),
            max_future_skew_ms=raw_args.get("max_future_skew_ms", 5_000),
            max_clock_skew_ms=raw_args.get("max_clock_skew_ms", 60_000.0),
            source_suffix=raw_args.get("source_suffix", ""),
            rotate_at_midnight=raw_args.get("rotate_at_midnight", False),
            idle_timeout_seconds=raw_args.get("idle_timeout_seconds", 0.0),
        )
    if job.job_type == "okx-depth-worker":
        return SimpleNamespace(
            symbol=raw_args.get("symbol", "BTC-USDT"),
            # OKX `books`: 400-level in-stream snapshot + seqId/prevSeqId chain.
            channel=raw_args.get("channel", "books"),
            market=raw_args.get("market", "spot"),
            segment_count=raw_args.get("segment_count", 5000),
            max_segments=raw_args.get("max_segments"),
            cooldown_seconds=raw_args.get("cooldown_seconds", 1.0),
            output_root=raw_args.get("output_root", default_output_root()),
            ops_root=Path(raw_args.get("ops_root", default_ops_root())),
            normalized_root=raw_args.get("normalized_root"),
            worker_name=raw_args.get("worker_name", "okx-depth-worker"),
            heartbeat_interval_seconds=raw_args.get("heartbeat_interval_seconds", 30.0),
            source_suffix=raw_args.get("source_suffix", ""),
            rotate_at_midnight=raw_args.get("rotate_at_midnight", False),
            idle_timeout_seconds=raw_args.get("idle_timeout_seconds", 0.0),
        )
    if job.job_type == "kraken-depth-worker":
        return SimpleNamespace(
            symbol=raw_args.get("symbol", "BTC/USD"),
            channel=raw_args.get("channel", "book"),
            segment_count=raw_args.get("segment_count", 5000),
            max_segments=raw_args.get("max_segments"),
            cooldown_seconds=raw_args.get("cooldown_seconds", 1.0),
            output_root=raw_args.get("output_root", default_output_root()),
            ops_root=Path(raw_args.get("ops_root", default_ops_root())),
            normalized_root=raw_args.get("normalized_root"),
            worker_name=raw_args.get("worker_name", "kraken-depth-worker"),
            heartbeat_interval_seconds=raw_args.get("heartbeat_interval_seconds", 30.0),
            source_suffix=raw_args.get("source_suffix", ""),
            rotate_at_midnight=raw_args.get("rotate_at_midnight", False),
            # Per-lane data-arrival watchdog (0.0 = off; see
            # CollectorConfig.idle_timeout_seconds). Honored by the generic-WS lanes;
            # the Binance depth lane runs its own socket loop and ignores it.
            idle_timeout_seconds=raw_args.get("idle_timeout_seconds", 0.0),
        )
    if job.job_type == "mexc-trades-worker":
        return SimpleNamespace(
            symbol=raw_args.get("symbol", "BTCUSDT"),
            # Full protobuf channel prefix; the interval + symbol are appended to the topic.
            channel=raw_args.get("channel", MEXC_DEALS_CHANNEL),
            interval=raw_args.get("interval", "100ms"),
            segment_count=raw_args.get("segment_count", 5000),
            max_segments=raw_args.get("max_segments"),
            cooldown_seconds=raw_args.get("cooldown_seconds", 1.0),
            output_root=raw_args.get("output_root", default_output_root()),
            ops_root=Path(raw_args.get("ops_root", default_ops_root())),
            normalized_root=raw_args.get("normalized_root"),
            worker_name=raw_args.get("worker_name", "mexc-trades-worker"),
            # Durable batched JSONL by default — see coinbase-trades-worker.
            jsonl_fsync=raw_args.get("jsonl_fsync", True),
            heartbeat_interval_seconds=raw_args.get("heartbeat_interval_seconds", 30.0),
            max_delay_ms=raw_args.get("max_delay_ms", _TRADES_STALE_WINDOW_MS),
            max_future_skew_ms=raw_args.get("max_future_skew_ms", 5_000),
            max_clock_skew_ms=raw_args.get("max_clock_skew_ms", 60_000.0),
            source_suffix=raw_args.get("source_suffix", ""),
            rotate_at_midnight=raw_args.get("rotate_at_midnight", False),
            # Per-lane data-arrival watchdog (0.0 = off; see
            # CollectorConfig.idle_timeout_seconds). Honored by the generic-WS lanes.
            idle_timeout_seconds=raw_args.get("idle_timeout_seconds", 0.0),
        )
    if job.job_type == "mexc-depth-worker":
        return SimpleNamespace(
            symbol=raw_args.get("symbol", "BTCUSDT"),
            # Full protobuf channel prefix; the symbol + depth are appended to the topic.
            channel=raw_args.get("channel", MEXC_LIMIT_DEPTH_CHANNEL),
            depth=raw_args.get("depth", 20),
            segment_count=raw_args.get("segment_count", 5000),
            max_segments=raw_args.get("max_segments"),
            cooldown_seconds=raw_args.get("cooldown_seconds", 1.0),
            output_root=raw_args.get("output_root", default_output_root()),
            ops_root=Path(raw_args.get("ops_root", default_ops_root())),
            normalized_root=raw_args.get("normalized_root"),
            worker_name=raw_args.get("worker_name", "mexc-depth-worker"),
            heartbeat_interval_seconds=raw_args.get("heartbeat_interval_seconds", 30.0),
            source_suffix=raw_args.get("source_suffix", ""),
            rotate_at_midnight=raw_args.get("rotate_at_midnight", False),
            # Per-lane data-arrival watchdog (0.0 = off; see
            # CollectorConfig.idle_timeout_seconds). Honored by the generic-WS lanes.
            idle_timeout_seconds=raw_args.get("idle_timeout_seconds", 0.0),
        )
    if job.job_type == "kalshi-discover-crypto":
        return SimpleNamespace(
            category=raw_args.get("category", DEFAULT_KALSHI_CATEGORY),
            target_assets=raw_args.get("target_assets", DEFAULT_KALSHI_TARGET_ASSETS),
            target_frequencies=raw_args.get("target_frequencies", DEFAULT_KALSHI_TARGET_FREQUENCIES),
            markets_per_series=raw_args.get("markets_per_series", DEFAULT_KALSHI_MARKETS_PER_SERIES),
            output_root=raw_args.get("output_root", default_curated_root("kalshi_crypto_binary_options")),
            format=raw_args.get("format", "text"),
        )
    if job.job_type == "kalshi-collect-crypto-quotes":
        return SimpleNamespace(
            category=raw_args.get("category", DEFAULT_KALSHI_CATEGORY),
            target_assets=raw_args.get("target_assets", DEFAULT_KALSHI_TARGET_ASSETS),
            target_frequencies=raw_args.get("target_frequencies", DEFAULT_KALSHI_TARGET_FREQUENCIES),
            markets_per_series=raw_args.get("markets_per_series", DEFAULT_KALSHI_MARKETS_PER_SERIES),
            duration_seconds=raw_args.get("duration_seconds", DEFAULT_KALSHI_DURATION_SECONDS),
            sample_count=raw_args.get("sample_count"),
            poll_interval_seconds=raw_args.get("poll_interval_seconds", DEFAULT_KALSHI_POLL_INTERVAL_SECONDS),
            stale_after_seconds=raw_args.get("stale_after_seconds", DEFAULT_KALSHI_STALE_AFTER_SECONDS),
            output_root=raw_args.get("output_root", default_kalshi_output_root()),
            normalized_root=raw_args.get("normalized_root", default_kalshi_normalized_root()),
            jsonl_fsync=raw_args.get("jsonl_fsync", True),
            normalized_parquet=raw_args.get("normalized_parquet", True),
            format=raw_args.get("format", "text"),
        )
    if job.job_type == "kalshi-summarize-crypto-quotes":
        return SimpleNamespace(
            input_path=raw_args.get("input_path", default_output_root() / "kalshi_crypto_quotes"),
            format=raw_args.get("format", "text"),
        )
    if job.job_type in {"book-sync-health", "backfill-replay"}:
        return SimpleNamespace(
            source_root=Path(raw_args.get("source_root", default_archive_root() / "raw" / "market" / "binance_depth")),
            limit=raw_args.get("limit", 50),
            max_age_hours=raw_args.get("max_age_hours", 24.0),
            overwrite=raw_args.get("overwrite", False),
            format=raw_args.get("format", "text"),
        )
    if job.job_type == "backfill-trades-replay":
        # Trades scorer (run_backfill_trades_replay) — write replay summaries only, no
        # promotion. stream=True selects the none_native UUID scorer (Bybit/MEXC);
        # default is the dense sequence-bearing scorer (Binance/Coinbase/Kraken).
        return SimpleNamespace(
            source_root=Path(raw_args.get("source_root", default_archive_root() / "raw" / "market" / "binance_trades")),
            limit=raw_args.get("limit", 50),
            max_age_hours=raw_args.get("max_age_hours", 24.0),
            overwrite=raw_args.get("overwrite", False),
            stream=raw_args.get("stream", False),
            format=raw_args.get("format", "text"),
        )
    if job.job_type == "backfill-stream-depth":
        # Non-binance depth scorer. Defaults to score_only so the live catch-up job
        # writes replay summaries without promoting — promotion stays in the
        # quarantine-aware promote-replayable jobs (single promoter, no dup rows).
        # A config that sets `source` to a bare string (instead of a list) would
        # char-iterate downstream — each letter "lane" skipped, job green, scorer
        # silently dead. Coerce to a one-element list.
        configured_source = raw_args.get("source", ["coinbase_depth", "bybit_depth", "kraken_depth"])
        if isinstance(configured_source, str):
            configured_source = [configured_source]
        return SimpleNamespace(
            raw_root=Path(raw_args.get("raw_root", default_output_root())),
            source=configured_source,
            target_root=Path(raw_args.get("target_root", default_curated_root("market_replayable"))),
            limit=raw_args.get("limit", 200),
            max_age_hours=raw_args.get("max_age_hours", 720.0),
            apply=raw_args.get("apply", False),
            score_only=raw_args.get("score_only", True),
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
    if job.job_type == "archive-offload":
        # No defaulted cold_root/lanes on purpose: a wrong default here silently
        # moves (then deletes) raw data somewhere unintended, so the config must
        # spell both out. Missing keys fail the job loudly at dispatch.
        if "cold_root" not in raw_args or "lanes" not in raw_args:
            raise ValueError("archive-offload job requires 'cold_root' and 'lanes' args")
        return SimpleNamespace(
            raw_root=Path(raw_args.get("raw_root", default_output_root())),
            cold_root=Path(raw_args["cold_root"]),
            lanes=raw_args["lanes"],
            min_age_days=raw_args.get("min_age_days", 14.0),
            limit=raw_args.get("limit", 200),
            apply=raw_args.get("apply", False),
            format=raw_args.get("format", "text"),
        )
    raise ValueError(f"Unsupported job_type: {job.job_type}")


def _default_ops_config_path() -> Path | None:
    """Locate the runner's ops config relative to this repo so `health` reports on the
    same jobs the runner runs even when --config is omitted. Mirrors run_ops_runner.ps1:
    prefer ops.live.local.json, fall back to ops.live.example.json."""
    repo_root = Path(__file__).resolve().parents[2]
    for name in ("ops.live.local.json", "ops.live.example.json"):
        candidate = repo_root / name
        if candidate.exists():
            return candidate
    return None


def _ops_root_from_jobs(jobs: list | None) -> Path | None:
    """Derive the ops root the runner actually writes to from the discovered config's
    job args. Without this, a bare `health` (no --ops-root, no env var) reads the stale
    pre-migration root via default_ops_root() and falsely reports status=error; the
    config is the source of truth for the live collection root. Returns the most common
    ops_root across jobs, or None if the config carries none."""
    if not jobs:
        return None
    counts: dict[str, int] = {}
    for job in jobs:
        root = getattr(job, "args", {}).get("ops_root")
        if root:
            key = str(root)
            counts[key] = counts.get(key, 0) + 1
    if not counts:
        return None
    return Path(max(counts.items(), key=lambda kv: kv[1])[0])


def _normalized_root_from_jobs(jobs: list | None) -> Path | None:
    """Derive the normalized root the runner actually writes to from the discovered
    config, so a bare `health` checks the live partition tree rather than the stale
    env/default (pre-migration D:\\market_archive\\normalized) root. The ops_root in the
    job args sits directly under the archive root (archive/ops) and normalized data sits
    beside it (archive/normalized) — same layout default_normalized_root() builds. Returns
    None when the config carries no ops_root, so build_health_report falls back to the
    env/default root (unchanged behavior for configs that predate the migration)."""
    ops_root = _ops_root_from_jobs(jobs)
    if ops_root is None:
        return None
    return ops_root.parent / "normalized"


def run_health(args: argparse.Namespace) -> None:
    # Without an explicit --config the report was blind to interval jobs (poll-based
    # lanes like Kalshi never appear in standalone_workers), so auto-discover the
    # runner's config and report on the same job set the runner runs.
    config_path = args.config or _default_ops_config_path()
    jobs = load_ops_config(config_path) if config_path and config_path.exists() else None
    # When --ops-root is omitted, follow the discovered config's live root instead of the
    # env/default fallback, so a bare `health` reports on the running collection rather
    # than a stale pre-migration root.
    ops_root = args.ops_root or _ops_root_from_jobs(jobs) or default_ops_root()
    # Follow the discovered config's normalized root too (mirrors the ops_root logic
    # above), so a bare `health` checks the live partition tree instead of the stale
    # env/default root. None => build_health_report uses the env/default fallback.
    normalized_root = _normalized_root_from_jobs(jobs)
    report = build_health_report(
        ops_root=ops_root,
        jobs=jobs,
        stale_after_seconds=args.stale_after_seconds,
        job_stale_multiplier=args.job_stale_multiplier,
        recent_failure_window_seconds=args.recent_failure_window_seconds,
        min_disk_free_gb=args.min_disk_free_gb,
        quarantine_ratio_threshold=float(getattr(args, "quarantine_ratio_threshold", 0.20)),
        normalized_root=normalized_root,
    )
    if args.format == "json":
        print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
        return
    print(f"status={report.status}")
    print(f"heartbeat_age_seconds={report.heartbeat_age_seconds}")
    print(f"disk_free_gb={report.disk_free_gb:.2f}" if report.disk_free_gb is not None else "disk_free_gb=None")
    print(f"findings={','.join(report.findings) if report.findings else 'none'}")
    if report.poll_lanes:
        print("poll_lanes:")
        for lane in report.poll_lanes:
            age = lane.get("age_seconds")
            age_str = f"{age:.0f}s" if isinstance(age, (int, float)) else "n/a"
            print(
                f"  {lane.get('name')}: age={age_str}"
                f" interval={lane.get('interval_seconds')}s"
                f" status={lane.get('status')}"
                f" stale={lane.get('stale')}"
                f" next_run_at={lane.get('next_run_at') or '?'}"
            )
    else:
        print("poll_lanes=none")


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


def run_archive_offload(args: argparse.Namespace) -> None:
    # Lanes arrive either inline (ops-runner job args) or as a JSON file (manual CLI).
    raw_lanes = getattr(args, "lanes", None)
    if raw_lanes is None:
        lanes_file: Path = args.lanes_file
        raw_lanes = json.loads(lanes_file.read_text(encoding="utf-8"))
    lanes = [OffloadLaneSpec.from_dict(item) for item in raw_lanes]
    report = offload_accounted_runs(
        raw_root=args.raw_root,
        cold_root=args.cold_root,
        lanes=lanes,
        min_age_days=args.min_age_days,
        limit=args.limit,
        apply=args.apply,
    )
    if args.format == "json":
        print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
    else:
        print(f"status={report.status}")
        print(f"mode={report.mode}")
        print(f"scanned_run_count={report.scanned_run_count}")
        print(f"eligible_count={report.eligible_count}")
        print(f"moved_count={report.moved_count}")
        print(f"moved_bytes={report.moved_bytes}")
        print(f"failed_count={report.failed_count}")
        print(f"stuck_unaccounted_count={report.stuck_unaccounted_count}")
        print(f"findings={','.join(report.findings) if report.findings else 'none'}")
        for lane in report.lanes:
            if lane.scanned_count or lane.stuck_unaccounted_count:
                print(
                    f"  {lane.source}: scanned={lane.scanned_count}"
                    f" eligible={lane.eligible_count} moved={lane.moved_count}"
                    f" stuck={lane.stuck_unaccounted_count}"
                )
    # Unlike the report-only maintenance jobs, this one DELETES hot data after
    # verification — a verify/copy failure must register as a job failure in the
    # runner (and thus in health's recent_job_failures), not scroll by in a log.
    if report.failed_count:
        raise RuntimeError(f"archive-offload had {report.failed_count} failed moves")


def run_ops_prune_stale_workers(args: argparse.Namespace) -> None:
    jobs = load_ops_config(args.config) if args.config and args.config.exists() else None
    managed_names: set[str] = set()
    # Every collector lane in the config is managed — the worker name defaults to
    # the job type (each run_*_worker's default_worker_name). The old version
    # enumerated only the two Binance types (it predated the multi-venue
    # expansion), so 19 of 21 live lanes were prunable as "unmanaged".
    for job in jobs or []:
        if job.job_type in COLLECTOR_JOB_TYPES:
            managed_names.add(str(job.args.get("worker_name") or job.job_type))
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


def run_backfill_trades_replay(args: argparse.Namespace) -> None:
    replay_fn = replay_trades_stream_run if getattr(args, "stream", False) else replay_trades_run
    report = backfill_replay_summaries(
        args.source_root,
        limit=args.limit,
        max_age_hours=args.max_age_hours,
        overwrite=args.overwrite,
        replay_fn=replay_fn,
    )
    if args.format == "json":
        print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
        return
    print(f"status={report.status}")
    print(f"created_count={report.created_count}")
    print(f"updated_count={report.updated_count}")
    print(f"skipped_count={report.skipped_count}")
    print(f"failed_count={report.failed_count}")
    print(f"findings={','.join(report.findings) if report.findings else 'none'}")


def _backfill_first_event_product(run_dir: Path) -> str | None:
    """Read the venue product (e.g. 'BTC/USD') from a run's first clean event so the
    backfill can select the right per-venue replay kwargs (Kraken precision)."""
    events_path = run_dir / "clean" / "events.jsonl"
    if not events_path.exists():
        return None
    try:
        with events_path.open(encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)
                product = row.get("product")
                return str(product) if product is not None else None
    except (OSError, json.JSONDecodeError):
        return None
    return None


def _backfill_run_started_at(run_dir: Path) -> datetime | None:
    try:
        return datetime.strptime(run_dir.name, "%Y%m%d_%H%M%S").replace(tzinfo=UTC)
    except ValueError:
        return None


def run_backfill_stream_depth(args: argparse.Namespace) -> None:
    """Re-replay already-collected stream-snapshot depth runs with the current
    multi-anchor logic and (with --apply) promote the replayable ones. The live
    collector writes its own replay summary at collection time; this rescues the
    backlog collected before the multi-anchor logic existed.

    Three modes:
    - dry-run (default): score in memory, write nothing.
    - --score-only: write each run's metrics/replay_summary.json but do NOT promote.
      This is the mode the live ops catch-up job uses so the quarantine-aware
      promote-replayable jobs stay the single promoter into the curated parquet (two
      concurrent promoters writing the same dataset = duplicate curated rows, since
      the promotion index can't dedup a run it hasn't recorded yet).
    - --apply: score AND promote here (manual one-shot; ignores the quarantine index).
    score-only wins if both flags are passed."""
    raw_root: Path = args.raw_root
    apply = bool(getattr(args, "apply", False))
    score_only = bool(getattr(args, "score_only", False))
    # score_only writes summaries without promoting; apply (only when not score_only)
    # both writes summaries and promotes. Dry run writes nothing.
    write_summaries = apply or score_only
    do_promote = apply and not score_only
    mode = "score_only" if score_only else ("apply" if apply else "dry_run")
    summary_rows: list[dict[str, object]] = []
    for source_dir_name in args.source:
        # Lane dirs are "<venue>[_perp]_depth[_<instrument-suffix>]" and the replay
        # kwargs are keyed by the BARE venue regardless of market (the live
        # collector hardcodes it). The first underscore token IS the venue for
        # every such name — including per-instrument suffixed lanes like
        # "bybit_depth_ethusdt", which the old strip-trailing-"_depth" logic missed
        # entirely (the whole dir name fell through to the none_native default,
        # silently downgrading a provable-sequence lane on re-score).
        venue = source_dir_name.split("_", 1)[0]
        source_root = raw_root / source_dir_name
        scanned = 0
        replayable = 0
        skipped_scored = 0
        finding_counts: dict[str, int] = {}
        overwrite = bool(getattr(args, "overwrite", False))
        cutoff = datetime.now(tz=UTC) - timedelta(hours=args.max_age_hours)
        run_dirs: list[Path] = []
        candidate_limit = max(0, int(args.limit))
        if source_root.is_dir() and candidate_limit > 0:
            for run_dir in sorted(
                (path for path in source_root.iterdir() if path.is_dir()),
                key=lambda path: path.name,
                reverse=True,
            ):
                started_at = _backfill_run_started_at(run_dir)
                if started_at is not None and started_at < cutoff:
                    continue
                if not (run_dir / "clean" / "events.jsonl").exists():
                    continue
                # Apply the work limit AFTER excluding already-scored runs. Otherwise,
                # N newer scored runs permanently hide an older cut-off run from this
                # self-healing pass.
                if (
                    not overwrite
                    and (run_dir / "metrics" / "replay_summary.json").exists()
                ):
                    skipped_scored += 1
                    continue
                run_dirs.append(run_dir)
                if len(run_dirs) >= candidate_limit:
                    break
        for run_dir in run_dirs:
            symbol = _backfill_first_event_product(run_dir)
            kwargs = _stream_depth_replay_kwargs(venue, symbol)
            try:
                result = replay_depth_stream_run(run_dir, write_summary=write_summaries, **kwargs)
            except Exception:  # noqa: BLE001 - keep going; tally as a finding
                finding_counts["replay_error"] = finding_counts.get("replay_error", 0) + 1
                continue
            scanned += 1
            if result.replayable:
                replayable += 1
            else:
                for finding in result.findings:
                    finding_counts[finding] = finding_counts.get(finding, 0) + 1
        promotion: dict[str, int] | None = None
        if do_promote and replayable > 0:
            report = promote_replayable_runs(
                source_root=source_root,
                target_root=args.target_root,
                limit=args.limit,
                max_age_hours=args.max_age_hours,
            )
            promotion = {
                "promoted_run_count": report.promoted_run_count,
                "promoted_row_count": report.promoted_row_count,
                "skipped_count": report.skipped_count,
                "failed_count": report.failed_count,
            }
        summary_rows.append(
            {
                "source": source_dir_name,
                "venue": venue,
                "scanned": scanned,
                "replayable": replayable,
                "not_replayable": scanned - replayable,
                "skipped_already_scored": skipped_scored,
                "findings": finding_counts,
                "promotion": promotion,
            }
        )

    payload = {
        "mode": mode,
        "raw_root": str(raw_root),
        "target_root": str(args.target_root),
        "sources": summary_rows,
    }
    if args.format == "json":
        print(json.dumps(payload, indent=2, sort_keys=True))
        return
    mode_label = {
        "apply": "APPLY (score + promote)",
        "score_only": "SCORE-ONLY (write summaries, no promote)",
        "dry_run": "dry-run (no writes)",
    }[mode]
    print(f"backfill-stream-depth mode={mode_label}")
    print(f"raw_root={raw_root}")
    if do_promote:
        print(f"target_root={args.target_root}")
    for row in summary_rows:
        print(
            f"  {row['source']:16s} scanned={row['scanned']:4d} "
            f"replayable={row['replayable']:4d} not_replayable={row['not_replayable']:4d}"
        )
        if row["findings"]:
            findings_str = ", ".join(f"{k}={v}" for k, v in sorted(row["findings"].items()))
            print(f"      findings: {findings_str}")
        if row["promotion"] is not None:
            p = row["promotion"]
            print(
                f"      promoted: runs={p['promoted_run_count']} rows={p['promoted_row_count']} "
                f"skipped={p['skipped_count']} failed={p['failed_count']}"
            )
    if mode == "dry_run":
        print(
            "Dry run only (no files written). Re-run with --score-only to regenerate "
            "replay summaries (promotion left to the promote-replayable jobs), or "
            "--apply to also promote the replayable runs here."
        )


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


def run_kalshi_discover_crypto(args: argparse.Namespace) -> None:
    report = discover_kalshi_crypto_markets(
        output_root=args.output_root,
        category=args.category,
        target_assets=args.target_assets,
        target_frequencies=args.target_frequencies,
        markets_per_series=args.markets_per_series,
    )
    if args.format == "json":
        print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
        return
    print(f"status={report.status}")
    print(f"series_count={report.series_count}")
    print(f"selected_series_count={report.selected_series_count}")
    print(f"active_market_count={report.active_market_count}")
    print(f"shortest_frequency={report.shortest_frequency or ''}")
    print(f"shortest_frequency_seconds={report.shortest_frequency_seconds or ''}")
    print(f"findings={','.join(report.findings) if report.findings else 'none'}")
    print(f"output_root={args.output_root}")


def run_kalshi_collect_crypto_quotes(args: argparse.Namespace) -> None:
    # Thread the batched-fsync cadence through: the flags were exposed on this
    # subparser (and injected by the ops runner) but never reached the collector,
    # so the heaviest raw-volume lane silently stayed per-event fsync.
    fsync_events, fsync_ms = _fsync_intervals(args)
    summary = collect_kalshi_crypto_quotes(
        output_root=args.output_root,
        normalized_root=args.normalized_root,
        category=args.category,
        target_assets=args.target_assets,
        target_frequencies=args.target_frequencies,
        duration_seconds=args.duration_seconds,
        sample_count=args.sample_count,
        poll_interval_seconds=args.poll_interval_seconds,
        stale_after_seconds=args.stale_after_seconds,
        markets_per_series=args.markets_per_series,
        jsonl_fsync=args.jsonl_fsync,
        normalized_parquet=args.normalized_parquet,
        fsync_interval_events=fsync_events,
        fsync_interval_ms=fsync_ms,
    )
    if args.format == "json":
        print(json.dumps(summary.to_dict(), indent=2, sort_keys=True))
        return
    print(f"status={summary.status}")
    print(f"run_path={summary.run_path}")
    print(f"sample_count={summary.sample_count}")
    print(f"quote_count={summary.quote_count}")
    print(f"side_symbol_count={summary.side_symbol_count}")
    print(f"first_observed_quote_count={summary.first_observed_quote_count}")
    print(f"quote_update_count={summary.quote_update_count}")
    print(f"repeated_quote_count={summary.repeated_quote_count}")
    print(f"stale_quote_count={summary.stale_quote_count}")
    print(f"findings={','.join(summary.findings) if summary.findings else 'none'}")


def run_kalshi_summarize_crypto_quotes(args: argparse.Namespace) -> None:
    summary = summarize_kalshi_quote_rows(args.input_path)
    if args.format == "json":
        print(json.dumps(summary, indent=2, sort_keys=True))
        return
    print(f"input_path={summary['input_path']}")
    print(f"quote_count={summary['quote_count']}")
    print(f"side_symbol_count={summary['side_symbol_count']}")
    print(f"first_observed_quote_count={summary['first_observed_quote_count']}")
    print(f"quote_update_count={summary['quote_update_count']}")
    print(f"repeated_quote_count={summary['repeated_quote_count']}")
    print(f"stale_quote_count={summary['stale_quote_count']}")


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


def _binance_buffer_bridges_snapshot(
    buffered: list[RawMessage], snapshot_last_update_id: int
) -> bool | None:
    """Classify a depth buffer against a snapshot's lastUpdateId (L) for bootstrap anchoring.

    Binance's diff-depth recipe requires a buffered delta whose ``[U, u]`` straddles
    ``L + 1``. Returns:
      ``True``  — a kept delta bridges the snapshot (the first delta with ``u > L`` has
                  ``U <= L + 1``); the run can anchor on this snapshot with no gap.
      ``None``  — every buffered delta predates the snapshot (all ``u <= L``), so ``L`` is
                  ahead of the buffer: keep reading until the stream catches up.
      ``False`` — the earliest kept delta starts past ``L + 1`` (``U > L + 1``): the buffer
                  has run ahead of the snapshot, so a NEWER snapshot is needed to close
                  the gap (matches replay's ``snapshot_anchor_gap`` condition).
    """
    aligned = _align_binance_buffered_events(buffered, snapshot_last_update_id)
    if not aligned:
        return None
    first_window = _binance_update_window(aligned[0].payload)
    if first_window is None:
        return None
    return first_window[0] <= snapshot_last_update_id + 1


async def _capture_binance_snapshot_and_buffer(
    websocket: object,
    *,
    product: str,
    snapshot_limit: int,
    snapshot_base_url: str,
    snapshot_anchor_timeout_seconds: float = 10.0,
    max_snapshot_fetches: int = 6,
) -> tuple[dict[str, object], list[RawMessage]]:
    """Fetch a REST snapshot and buffer the surrounding deltas so the run anchors with NO
    gap between ``snapshot.lastUpdateId`` and the first kept delta.

    The diff-depth stream only emits every ~100ms while the REST snapshot can return
    faster, so we must NEVER stop buffering on snapshot arrival. (The original bug: a fast
    snapshot left an empty buffer, the bridging delta was missed, and replay flagged a
    ``snapshot_anchor_gap`` with the book crossed on nearly every event.) We keep reading
    until a buffered delta bridges the snapshot, refetching a newer snapshot — in the
    background, so buffering never pauses — whenever the buffer has already advanced past
    the current one. If we cannot anchor within ``snapshot_anchor_timeout_seconds`` we
    return what we have and let replay flag it, as before.
    """
    buffered: list[RawMessage] = []

    def _fetch() -> asyncio.Task:
        return asyncio.create_task(
            asyncio.to_thread(
                fetch_binance_order_book_snapshot,
                symbol=product,
                limit=snapshot_limit,
                base_url=snapshot_base_url,
            )
        )

    async def _read_into_buffer() -> None:
        try:
            message = await asyncio.wait_for(websocket.recv(), timeout=0.05)
        except asyncio.TimeoutError:
            return
        payload = json.loads(message)
        if _is_binance_depth_payload(payload):
            buffered.append(RawMessage(source="binance", received_at=utc_now(), payload=payload))

    # Buffer deltas while the initial snapshot is in flight so a fast REST response can't
    # leave us with an empty buffer.
    snapshot_task = _fetch()
    while not snapshot_task.done():
        await _read_into_buffer()
    snapshot = await snapshot_task
    snapshot_last_update_id = int(snapshot.get("lastUpdateId", 0))
    fetches = 1

    loop = asyncio.get_event_loop()
    deadline = loop.time() + max(0.0, snapshot_anchor_timeout_seconds)
    refetch_task: asyncio.Task | None = None
    while loop.time() < deadline:
        # Adopt a completed background refetch before re-classifying the buffer.
        if refetch_task is not None and refetch_task.done():
            snapshot = refetch_task.result()
            snapshot_last_update_id = int(snapshot.get("lastUpdateId", 0))
            refetch_task = None

        bridges = _binance_buffer_bridges_snapshot(buffered, snapshot_last_update_id)
        if bridges is True:
            break
        if bridges is False and refetch_task is None and fetches < max_snapshot_fetches:
            # Buffer ran ahead of the snapshot — fetch a newer one WITHOUT pausing
            # buffering, so the bridging delta can't slip past us during the HTTP call.
            refetch_task = _fetch()
            fetches += 1
        await _read_into_buffer()

    if refetch_task is not None:
        refetch_task.cancel()
        try:
            await refetch_task
        except (asyncio.CancelledError, Exception):  # noqa: BLE001 — best-effort cleanup
            pass

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
    snapshot_anchor_timeout_seconds: float = 10.0,
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
                snapshot_anchor_timeout_seconds=snapshot_anchor_timeout_seconds,
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


def _next_utc_midnight(now: datetime) -> datetime:
    """Return the next UTC midnight strictly after `now`. Used by day-bounded run
    rotation: a segment that starts at 14:00 UTC ends at the upcoming 00:00 UTC
    so the run directory aligns with the curated event_date partition contract."""
    base = now.astimezone(UTC) if now.tzinfo is not None else now.replace(tzinfo=UTC)
    next_day = base.date() + timedelta(days=1)
    return datetime.combine(next_day, datetime.min.time(), tzinfo=UTC)


def _build_source_name(base: str, suffix: str | None) -> str:
    """Compose the on-disk source-directory name. When `suffix` is empty we keep the
    legacy single-symbol layout (`binance_depth/`) — important because the live BTC
    collector writes there and migrations during active collection are risky. When
    `suffix` is set, the run lands in `binance_depth_<sanitized>/` so independent
    per-instrument lanes don't interleave timestamps."""
    cleaned = "" if suffix is None else str(suffix).strip().lower()
    if not cleaned:
        return base
    # Normalize: only allow lowercase letters, digits, and hyphen/underscore.
    safe = "".join(ch if (ch.isalnum() or ch in {"-", "_"}) else "_" for ch in cleaned)
    return f"{base}_{safe}"


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
    elif args.command == "binance-futures-rest-worker":
        run_binance_futures_rest_worker(args)
    elif args.command == "coinbase-trades-worker":
        run_coinbase_trades_worker(args)
    elif args.command == "coinbase-depth-worker":
        run_coinbase_depth_worker(args)
    elif args.command == "kraken-trades-worker":
        run_kraken_trades_worker(args)
    elif args.command == "bybit-trades-worker":
        run_bybit_trades_worker(args)
    elif args.command == "bybit-depth-worker":
        run_bybit_depth_worker(args)
    elif args.command == "okx-trades-worker":
        run_okx_trades_worker(args)
    elif args.command == "okx-depth-worker":
        run_okx_depth_worker(args)
    elif args.command == "kraken-depth-worker":
        run_kraken_depth_worker(args)
    elif args.command == "mexc-trades-worker":
        run_mexc_trades_worker(args)
    elif args.command == "mexc-depth-worker":
        run_mexc_depth_worker(args)
    elif args.command == "kalshi-discover-crypto":
        run_kalshi_discover_crypto(args)
    elif args.command == "kalshi-collect-crypto-quotes":
        run_kalshi_collect_crypto_quotes(args)
    elif args.command == "kalshi-summarize-crypto-quotes":
        run_kalshi_summarize_crypto_quotes(args)
    elif args.command == "ops-runner":
        run_ops_runner(args)
    elif args.command == "run-job":
        run_single_job(args)
    elif args.command == "health":
        run_health(args)
    elif args.command == "cleanup":
        run_cleanup_command(args)
    elif args.command == "archive-offload":
        run_archive_offload(args)
    elif args.command == "ops-prune-stale-workers":
        run_ops_prune_stale_workers(args)
    elif args.command == "replay":
        run_replay(args)
    elif args.command == "book-sync-health":
        run_book_sync_health(args)
    elif args.command == "backfill-replay":
        run_backfill_replay(args)
    elif args.command == "backfill-trades-replay":
        run_backfill_trades_replay(args)
    elif args.command == "backfill-stream-depth":
        run_backfill_stream_depth(args)
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
