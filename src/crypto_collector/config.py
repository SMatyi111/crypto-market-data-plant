from __future__ import annotations

from dataclasses import dataclass
import os
import warnings
from pathlib import Path


DEFAULT_ARCHIVE_ROOT = Path(r"D:\market_archive")

# Contract version for the data the plant produces. Mirrors `STANDARDS_VERSION`
# in STANDARDS.md (repo root); bump both together when the schema, partition
# layout, or the definition of "replayable" changes. The research manifest tags
# its output with this so downstream readers can pin to a known contract.
STANDARDS_VERSION = 1

_FALLBACK_WARNED: set[str] = set()


def _warn_implicit_fallback(env_vars: tuple[str, ...], chosen: Path) -> None:
    """Warn once per process when no env override is set and we resort to a default path.

    A misconfigured operator writing to the wrong disk is hard to notice — this turns
    a silent fallback into a UserWarning that ops scripts can surface in logs.
    """
    key = "|".join(env_vars) + "->" + str(chosen)
    if key in _FALLBACK_WARNED:
        return
    _FALLBACK_WARNED.add(key)
    warnings.warn(
        f"market-data-plant: no value set for {' or '.join(env_vars)}; "
        f"falling back to {chosen}",
        UserWarning,
        stacklevel=3,
    )
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
    max_delay_ms: int = 60_000
    max_future_skew_ms: int = 5_000
    require_monotonic_sequence: bool = True
    connect_retries: int = 8
    retry_backoff_seconds: float = 1.0
    max_backoff_seconds: float = 60.0
    subscription_ack_timeout_seconds: float = 5.0
    # App-level keepalive. When ping_interval_seconds > 0 AND ping_message is set,
    # the collector sends ping_message on the open socket every interval. Default
    # OFF — venues rely on the websockets library's protocol-level ping/pong unless
    # they need an application-level heartbeat. Bybit is the one that does: it drops
    # idle public connections after ~10 min and documents a {"op":"ping"} roughly
    # every 20 s, so its lanes opt in. Leaving this off preserves the exact behavior
    # of every other (incl. the live Binance) collector.
    ping_interval_seconds: float = 0.0
    ping_message: dict | None = None


@dataclass(slots=True)
class RunConfig:
    collector: CollectorConfig
    count: int | None = None


_OUTPUT_ENV = ("MARKET_DATA_OUTPUT_ROOT", "CRYPTO_COLLECTOR_OUTPUT_ROOT")
_NORMALIZED_ENV = ("MARKET_DATA_NORMALIZED_ROOT", "CRYPTO_COLLECTOR_NORMALIZED_ROOT")
_CURATED_ENV = ("MARKET_DATA_CURATED_ROOT", "CRYPTO_COLLECTOR_CURATED_ROOT")
_OPS_ENV = ("MARKET_DATA_OPS_ROOT", "CRYPTO_COLLECTOR_OPS_ROOT")


def default_output_root() -> Path:
    configured = os.environ.get(_OUTPUT_ENV[0]) or os.environ.get(_OUTPUT_ENV[1])
    if configured:
        return Path(configured)
    archive_root = default_archive_root()
    if archive_root.exists():
        chosen = archive_root / "raw" / "market"
    elif DEFAULT_ARCHIVE_ROOT.exists():
        chosen = DEFAULT_MARKET_OUTPUT_ROOT
    else:
        chosen = Path("data")
    _warn_implicit_fallback(_OUTPUT_ENV, chosen)
    return chosen


def default_normalized_root(dataset: str) -> Path:
    configured = os.environ.get(_NORMALIZED_ENV[0]) or os.environ.get(_NORMALIZED_ENV[1])
    if configured:
        return Path(configured) / dataset
    archive_root = default_archive_root()
    if archive_root.exists():
        chosen = archive_root / "normalized" / dataset
    elif DEFAULT_ARCHIVE_ROOT.exists():
        chosen = DEFAULT_NORMALIZED_ROOT / dataset
    else:
        chosen = Path("normalized") / dataset
    _warn_implicit_fallback(_NORMALIZED_ENV, chosen)
    return chosen


def default_curated_root(dataset: str) -> Path:
    configured = os.environ.get(_CURATED_ENV[0]) or os.environ.get(_CURATED_ENV[1])
    if configured:
        return Path(configured) / dataset
    archive_root = default_archive_root()
    if archive_root.exists():
        chosen = archive_root / "curated" / "research" / dataset
    elif DEFAULT_ARCHIVE_ROOT.exists():
        chosen = DEFAULT_CURATED_ROOT / dataset
    else:
        chosen = Path("curated") / dataset
    _warn_implicit_fallback(_CURATED_ENV, chosen)
    return chosen


def default_ops_root() -> Path:
    configured = os.environ.get(_OPS_ENV[0]) or os.environ.get(_OPS_ENV[1])
    if configured:
        return Path(configured)
    archive_root = default_archive_root()
    if archive_root.exists():
        chosen = archive_root / "ops"
    elif DEFAULT_ARCHIVE_ROOT.exists():
        chosen = DEFAULT_OPS_ROOT
    else:
        chosen = Path("ops")
    _warn_implicit_fallback(_OPS_ENV, chosen)
    return chosen


def default_archive_root() -> Path:
    configured = os.environ.get("MARKET_DATA_ARCHIVE_ROOT") or os.environ.get("CRYPTO_COLLECTOR_ARCHIVE_ROOT")
    if configured:
        return Path(configured)
    return DEFAULT_ARCHIVE_ROOT
