from __future__ import annotations

import argparse
import asyncio
import json
import logging
import shutil
import sys
import threading
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
from .config import (
    DEFAULT_ARCHIVE_ROOT,
    CollectorConfig,
    default_archive_root,
    default_curated_root,
    default_normalized_root,
    default_ops_root,
    default_output_root,
)
from .market_normalizers import (
    BinanceDepthNormalizer,
    BinanceTradeNormalizer,
    BybitDepthNormalizer,
    BybitTradeNormalizer,
    CoinbaseDepthNormalizer,
    CoinbaseTradeNormalizer,
    KrakenDepthNormalizer,
    KrakenTradeNormalizer,
    MexcDepthNormalizer,
    MexcTradeNormalizer,
)
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
from .replay import (
    backfill_replay_summaries,
    build_book_sync_health_report,
    replay_depth_run,
    replay_depth_stream_run,
    replay_trades_run,
    replay_trades_stream_run,
)
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

    health_parser = subparsers.add_parser("health", help="Inspect runner and archive health")
    health_parser.add_argument("--ops-root", type=Path, default=default_ops_root())
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
    source_name = _build_source_name("binance_depth", getattr(args, "source_suffix", ""))
    run_paths = prepare_run_paths(output_root=config.output_root, source=source_name)
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


async def collect_binance_trades_segment(args: argparse.Namespace) -> dict[str, object]:
    return await _collect_trades_segment(
        args,
        source="binance",
        websocket_url="wss://stream.binance.com:9443/ws",
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


async def collect_bybit_trades_segment(args: argparse.Namespace) -> dict[str, object]:
    # Bybit spot trade id is a UUID (not a dense counter), so gaplessness is
    # unprovable: curate as a non-sequence ("none_native") feed — structurally clean
    # only, NOT gap-proof (STANDARDS §4.3).
    return await _collect_trades_segment(
        args,
        source="bybit",
        websocket_url="wss://stream.bybit.com/v5/public/spot",
        subscription_style="bybit",
        normalizer=BybitTradeNormalizer(),
        source_base="bybit_trades",
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
    ping_message: dict | None = None,
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
            default_normalized_root("trades")
            if bool(getattr(args, "normalized_parquet", True))
            else None
        ),
        jsonl_fsync=bool(getattr(args, "jsonl_fsync", True)),
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
    # and blocks promotion (STANDARDS 4.1 / 4.3).
    return await _collect_depth_stream_segment(
        args,
        source="bybit",
        websocket_url="wss://stream.bybit.com/v5/public/spot",
        subscription_style="bybit",
        normalizer=BybitDepthNormalizer(),
        source_base="bybit_depth",
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
    - kraken -> provable `checksum` (when the pair precision is known) + depth-bounded book
    - others -> none_native (structural-only)
    """
    if source == "bybit":
        return {"sequence_metadata_key": "bybit_update_id"}
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
    ping_message: dict | None = None,
    ping_interval_seconds: float = 0.0,
    sequence_metadata_key: str | None = None,
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
    pipeline = CollectorPipeline(
        collector=collector,
        normalizer=normalizer,
        # Depth events carry no top-level price/size/side, so the trades QualityGate
        # doesn't apply; MetadataQualityGate gates on parse errors (and update-range
        # for sequence feeds), which is the right bar for a depth diff stream.
        quality_gate=MetadataQualityGate(),
        run_paths=run_paths,
        normalized_root=default_normalized_root("market"),
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


def run_bybit_trades_worker(args: argparse.Namespace) -> None:
    _run_segmented_worker(
        args=args,
        default_worker_name="bybit-trades-worker",
        worker_type="bybit-trades-worker",
        venue="bybit",
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
                # When the worker is in day-bounded mode, each segment runs until
                # midnight UTC instead of until --segment-count. The segment
                # function checks the deadline in its inner stream loop and exits
                # cleanly when it's crossed, so the parquet flush / replay summary
                # / metrics write all still run.
                segment_deadline_utc = _next_utc_midnight(segment_started_at) if rotate_at_midnight else None
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
            segment_count=raw_args.get("segment_count", 5000),
            max_segments=raw_args.get("max_segments"),
            cooldown_seconds=raw_args.get("cooldown_seconds", 1.0),
            output_root=raw_args.get("output_root", default_output_root()),
            ops_root=Path(raw_args.get("ops_root", default_ops_root())),
            worker_name=raw_args.get("worker_name", "binance-trades-worker"),
            heartbeat_interval_seconds=raw_args.get("heartbeat_interval_seconds", 30.0),
            max_delay_ms=raw_args.get("max_delay_ms", 60_000),
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
    if job.job_type == "coinbase-trades-worker":
        return SimpleNamespace(
            symbol=raw_args.get("symbol", "BTC-USD"),
            channel=raw_args.get("channel", "matches"),
            segment_count=raw_args.get("segment_count", 5000),
            max_segments=raw_args.get("max_segments"),
            cooldown_seconds=raw_args.get("cooldown_seconds", 1.0),
            output_root=raw_args.get("output_root", default_output_root()),
            ops_root=Path(raw_args.get("ops_root", default_ops_root())),
            worker_name=raw_args.get("worker_name", "coinbase-trades-worker"),
            # Buffered JSONL (no per-event fsync) by default: per-event fsync throttles the
            # consumer below high-volume feeds, so the backlog grows past the 60s freshness
            # gate and valid trades get quarantined as stale (binance-trades already opted
            # out). raw JSONL flushes every 100 rows instead.
            jsonl_fsync=raw_args.get("jsonl_fsync", False),
            heartbeat_interval_seconds=raw_args.get("heartbeat_interval_seconds", 30.0),
            max_delay_ms=raw_args.get("max_delay_ms", 60_000),
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
            worker_name=raw_args.get("worker_name", "kraken-trades-worker"),
            # Buffered JSONL (no per-event fsync) by default — see coinbase-trades-worker.
            jsonl_fsync=raw_args.get("jsonl_fsync", False),
            heartbeat_interval_seconds=raw_args.get("heartbeat_interval_seconds", 30.0),
            max_delay_ms=raw_args.get("max_delay_ms", 60_000),
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
            segment_count=raw_args.get("segment_count", 5000),
            max_segments=raw_args.get("max_segments"),
            cooldown_seconds=raw_args.get("cooldown_seconds", 1.0),
            output_root=raw_args.get("output_root", default_output_root()),
            ops_root=Path(raw_args.get("ops_root", default_ops_root())),
            worker_name=raw_args.get("worker_name", "bybit-trades-worker"),
            # Buffered JSONL (no per-event fsync) by default — see coinbase-trades-worker.
            jsonl_fsync=raw_args.get("jsonl_fsync", False),
            heartbeat_interval_seconds=raw_args.get("heartbeat_interval_seconds", 30.0),
            max_delay_ms=raw_args.get("max_delay_ms", 60_000),
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
            segment_count=raw_args.get("segment_count", 5000),
            max_segments=raw_args.get("max_segments"),
            cooldown_seconds=raw_args.get("cooldown_seconds", 1.0),
            output_root=raw_args.get("output_root", default_output_root()),
            ops_root=Path(raw_args.get("ops_root", default_ops_root())),
            worker_name=raw_args.get("worker_name", "bybit-depth-worker"),
            heartbeat_interval_seconds=raw_args.get("heartbeat_interval_seconds", 30.0),
            source_suffix=raw_args.get("source_suffix", ""),
            rotate_at_midnight=raw_args.get("rotate_at_midnight", False),
            # Per-lane data-arrival watchdog (0.0 = off; see
            # CollectorConfig.idle_timeout_seconds). Honored by the generic-WS lanes;
            # the Binance depth lane runs its own socket loop and ignores it.
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
            worker_name=raw_args.get("worker_name", "mexc-trades-worker"),
            # Buffered JSONL (no per-event fsync) by default — see coinbase-trades-worker.
            jsonl_fsync=raw_args.get("jsonl_fsync", False),
            heartbeat_interval_seconds=raw_args.get("heartbeat_interval_seconds", 30.0),
            max_delay_ms=raw_args.get("max_delay_ms", 60_000),
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


def run_health(args: argparse.Namespace) -> None:
    # Without an explicit --config the report was blind to interval jobs (poll-based
    # lanes like Kalshi never appear in standalone_workers), so auto-discover the
    # runner's config and report on the same job set the runner runs.
    config_path = args.config or _default_ops_config_path()
    jobs = load_ops_config(config_path) if config_path and config_path.exists() else None
    report = build_health_report(
        ops_root=args.ops_root,
        jobs=jobs,
        stale_after_seconds=args.stale_after_seconds,
        job_stale_multiplier=args.job_stale_multiplier,
        recent_failure_window_seconds=args.recent_failure_window_seconds,
        min_disk_free_gb=args.min_disk_free_gb,
        quarantine_ratio_threshold=float(getattr(args, "quarantine_ratio_threshold", 0.20)),
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


def run_backfill_stream_depth(args: argparse.Namespace) -> None:
    """Re-replay already-collected stream-snapshot depth runs with the current
    multi-anchor logic and (with --apply) promote the replayable ones. The live
    collector writes its own replay summary at collection time; this rescues the
    backlog collected before the multi-anchor logic existed."""
    raw_root: Path = args.raw_root
    apply = bool(getattr(args, "apply", False))
    summary_rows: list[dict[str, object]] = []
    for source_dir_name in args.source:
        venue = (
            source_dir_name[: -len("_depth")]
            if source_dir_name.endswith("_depth")
            else source_dir_name
        )
        source_root = raw_root / source_dir_name
        run_dirs = (
            sorted(d for d in source_root.glob("*") if d.is_dir())[-args.limit :]
            if source_root.is_dir()
            else []
        )
        scanned = 0
        replayable = 0
        finding_counts: dict[str, int] = {}
        for run_dir in run_dirs:
            if not (run_dir / "clean" / "events.jsonl").exists():
                continue
            symbol = _backfill_first_event_product(run_dir)
            kwargs = _stream_depth_replay_kwargs(venue, symbol)
            try:
                result = replay_depth_stream_run(run_dir, write_summary=apply, **kwargs)
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
        if apply and replayable > 0:
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
                "findings": finding_counts,
                "promotion": promotion,
            }
        )

    payload = {
        "mode": "apply" if apply else "dry_run",
        "raw_root": str(raw_root),
        "target_root": str(args.target_root),
        "sources": summary_rows,
    }
    if args.format == "json":
        print(json.dumps(payload, indent=2, sort_keys=True))
        return
    print(f"backfill-stream-depth mode={'APPLY' if apply else 'dry-run (no writes)'}")
    print(f"raw_root={raw_root}")
    if apply:
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
    if not apply:
        print(
            "Dry run only (no files written). Re-run with --apply to regenerate "
            "replay summaries and promote the replayable runs."
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
