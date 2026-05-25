from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path


DEFAULT_ARCHIVE_ROOT = Path(r"D:\market_archive")
DEFAULT_MARKET_OUTPUT_ROOT = DEFAULT_ARCHIVE_ROOT / "raw" / "market"
DEFAULT_NORMALIZED_ROOT = DEFAULT_ARCHIVE_ROOT / "normalized"
DEFAULT_CURATED_ROOT = DEFAULT_ARCHIVE_ROOT / "curated" / "research"
DEFAULT_OPS_ROOT = DEFAULT_ARCHIVE_ROOT / "ops"


@dataclass(slots=True)
class CollectorConfig:
    source: str
    output_root: Path
    product: str = "BTC-USD"
    channel: str = "full"
    websocket_url: str | None = None
    subscription_style: str = "coinbase"
    max_delay_ms: int = 5_000
    require_monotonic_sequence: bool = True


@dataclass(slots=True)
class RunConfig:
    collector: CollectorConfig
    count: int | None = None


def default_output_root() -> Path:
    configured = os.environ.get("MARKET_DATA_OUTPUT_ROOT") or os.environ.get("CRYPTO_COLLECTOR_OUTPUT_ROOT")
    if configured:
        return Path(configured)
    archive_root = default_archive_root()
    if archive_root.exists():
        return archive_root / "raw" / "market"
    if DEFAULT_ARCHIVE_ROOT.exists():
        return DEFAULT_MARKET_OUTPUT_ROOT
    return Path("data")


def default_normalized_root(dataset: str) -> Path:
    configured = os.environ.get("MARKET_DATA_NORMALIZED_ROOT") or os.environ.get("CRYPTO_COLLECTOR_NORMALIZED_ROOT")
    if configured:
        return Path(configured) / dataset
    archive_root = default_archive_root()
    if archive_root.exists():
        return archive_root / "normalized" / dataset
    if DEFAULT_ARCHIVE_ROOT.exists():
        return DEFAULT_NORMALIZED_ROOT / dataset
    return Path("normalized") / dataset


def default_curated_root(dataset: str) -> Path:
    configured = os.environ.get("MARKET_DATA_CURATED_ROOT") or os.environ.get("CRYPTO_COLLECTOR_CURATED_ROOT")
    if configured:
        return Path(configured) / dataset
    archive_root = default_archive_root()
    if archive_root.exists():
        return archive_root / "curated" / "research" / dataset
    if DEFAULT_ARCHIVE_ROOT.exists():
        return DEFAULT_CURATED_ROOT / dataset
    return Path("curated") / dataset


def default_ops_root() -> Path:
    configured = os.environ.get("MARKET_DATA_OPS_ROOT") or os.environ.get("CRYPTO_COLLECTOR_OPS_ROOT")
    if configured:
        return Path(configured)
    archive_root = default_archive_root()
    if archive_root.exists():
        return archive_root / "ops"
    if DEFAULT_ARCHIVE_ROOT.exists():
        return DEFAULT_OPS_ROOT
    return Path("ops")


def default_archive_root() -> Path:
    configured = os.environ.get("MARKET_DATA_ARCHIVE_ROOT") or os.environ.get("CRYPTO_COLLECTOR_ARCHIVE_ROOT")
    if configured:
        return Path(configured)
    return DEFAULT_ARCHIVE_ROOT
