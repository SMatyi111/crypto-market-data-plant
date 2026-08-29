from __future__ import annotations

import gzip
import hashlib
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from ..models import utc_now


BASE_URL = "https://eapi.binance.com"
SOURCE_NAME = "binance_options_chain"
USER_AGENT = "crypto-market-data-plant-binance-options/0.1"
DEFAULT_UNDERLYINGS = ("BTCUSDT", "ETHUSDT")

FetchFn = Callable[[str], bytes]

# Module seam so tests can observe/skip the retry backoff.
_sleep = time.sleep


def fetch_url(url: str, *, timeout_seconds: float = 60.0) -> bytes:
    """Fetch one eapi endpoint as raw bytes.

    Raw bytes, not parsed rows: the chain snapshot is archival reference data
    whose value is the exact point-in-time payload (STANDARDS section 4.9).
    Parsing happens at read time, never at capture time.
    """
    delay = 0.0
    for attempt in range(1, 4):
        request = Request(
            url,
            method="GET",
            headers={"Accept": "application/json", "User-Agent": USER_AGENT},
        )
        try:
            with urlopen(request, timeout=timeout_seconds) as response:
                return response.read()
        except HTTPError as exc:
            # 418 is Binance's escalation for clients that keep hammering
            # through 429s; retrying it extends the IP ban. Same rule as the
            # fapi REST collector - never retry 418.
            if exc.code == 418 or exc.code not in {429, 500, 502, 503, 504} or attempt == 3:
                raise
            raw_retry = exc.headers.get("Retry-After") if exc.headers is not None else None
            try:
                delay = min(60.0, max(0.0, float(raw_retry)))
            except (TypeError, ValueError):
                delay = min(60.0, 5.0 * attempt)
        except URLError:
            if attempt == 3:
                raise
            delay = min(60.0, 5.0 * attempt)
        _sleep(delay)
    raise RuntimeError("unreachable binance options retry loop")


@dataclass(frozen=True, slots=True)
class OptionsChainSnapshotResult:
    run_path: str
    fetched_at: str
    payload_count: int
    total_raw_bytes: int
    option_symbol_count: int | None
    parse_ok: bool
    failures: tuple[str, ...]


def _chain_payloads(underlyings: tuple[str, ...]) -> list[tuple[str, str]]:
    payloads = [
        ("exchange_info", f"{BASE_URL}/eapi/v1/exchangeInfo"),
        ("mark", f"{BASE_URL}/eapi/v1/mark"),
        ("ticker", f"{BASE_URL}/eapi/v1/ticker"),
    ]
    for underlying in underlyings:
        payloads.append((f"index_{underlying}", f"{BASE_URL}/eapi/v1/index?underlying={underlying}"))
    return payloads


def _validate_payload(
    name: str, parsed: Any, underlyings: tuple[str, ...]
) -> tuple[int | None, str | None]:
    """Return (row_count, failure_reason) for one parsed payload."""
    if name == "exchange_info":
        symbols = parsed.get("optionSymbols") if isinstance(parsed, dict) else None
        if not isinstance(symbols, list) or not symbols:
            return None, "exchange_info has no optionSymbols"
        return len(symbols), None
    if name in {"mark", "ticker"}:
        if not isinstance(parsed, list) or not parsed:
            return None, f"{name} payload is not a non-empty list"
        if name == "ticker":
            # Option symbols look like BTC-260828-60000-C; every requested
            # underlying must have at least one live contract in the payload.
            prefixes = {u: u.removesuffix("USDT") + "-" for u in underlyings}
            for underlying, prefix in prefixes.items():
                if not any(
                    isinstance(row, dict) and str(row.get("symbol", "")).startswith(prefix)
                    for row in parsed
                ):
                    return len(parsed), f"ticker has no contracts for {underlying}"
        return len(parsed), None
    if name.startswith("index_"):
        if not isinstance(parsed, dict) or "indexPrice" not in parsed:
            return None, f"{name} payload has no indexPrice"
        return 1, None
    return None, f"unknown payload {name}"


def _write_gzip_atomic(path: Path, raw: bytes) -> None:
    temporary = path.with_name(f"{path.name}.tmp")
    with gzip.open(temporary, "wb") as handle:
        handle.write(raw)
    temporary.replace(path)


def snapshot_binance_options_chain(
    output_root: Path | str,
    *,
    underlyings: tuple[str, ...] = DEFAULT_UNDERLYINGS,
    fetch: FetchFn = fetch_url,
    clock: Callable[[], Any] = utc_now,
) -> OptionsChainSnapshotResult:
    """Capture one full Binance options chain snapshot into its own run dir.

    Raw-only reference lane (STANDARDS section 4.9): exact upstream bytes per
    endpoint, gzipped, sha256 recorded, no clean/quarantine split, no
    normalization, no promotion. A malformed or empty payload still archives
    the raw bytes but FAILS the job so runner job-status counters surface it.
    """
    fetched_at = clock()
    run_id = fetched_at.strftime("%Y%m%d_%H%M%S")
    run_dir = Path(output_root) / SOURCE_NAME / run_id
    raw_dir = run_dir / "raw"
    metrics_dir = run_dir / "metrics"
    raw_dir.mkdir(parents=True, exist_ok=True)
    metrics_dir.mkdir(parents=True, exist_ok=True)

    payload_summaries: dict[str, dict[str, Any]] = {}
    failures: list[str] = []
    total_raw_bytes = 0
    option_symbol_count: int | None = None

    for name, url in _chain_payloads(underlyings):
        raw = fetch(url)
        total_raw_bytes += len(raw)
        _write_gzip_atomic(raw_dir / f"{name}.json.gz", raw)

        row_count: int | None = None
        failure: str | None = None
        try:
            parsed = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, ValueError):
            failure = f"{name} payload is not valid JSON"
        else:
            row_count, failure = _validate_payload(name, parsed, underlyings)
        if name == "exchange_info":
            option_symbol_count = row_count
        if failure is not None:
            failures.append(failure)
        payload_summaries[name] = {
            "url": url,
            "raw_bytes": len(raw),
            "sha256": hashlib.sha256(raw).hexdigest(),
            "row_count": row_count,
            "parse_ok": failure is None,
        }

    parse_ok = not failures
    summary = {
        "source": SOURCE_NAME,
        "fetched_at": fetched_at.isoformat(),
        "underlyings": list(underlyings),
        "payloads": payload_summaries,
        "total_raw_bytes": total_raw_bytes,
        "option_symbol_count": option_symbol_count,
        "parse_ok": parse_ok,
        "failures": failures,
    }
    summary_path = metrics_dir / "summary.json"
    summary_tmp = summary_path.with_name(f"{summary_path.name}.tmp")
    summary_tmp.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    summary_tmp.replace(summary_path)

    if not parse_ok:
        # Raw payloads are already durably archived above (never discard
        # evidence); failing the job surfaces a malformed or empty chain in
        # the runner's job-status counters instead of silently archiving
        # garbage forever.
        raise ValueError(
            f"binance options chain snapshot failed validation: {'; '.join(failures)}"
        )

    return OptionsChainSnapshotResult(
        run_path=str(run_dir),
        fetched_at=fetched_at.isoformat(),
        payload_count=len(payload_summaries),
        total_raw_bytes=total_raw_bytes,
        option_symbol_count=option_symbol_count,
        parse_ok=parse_ok,
        failures=tuple(failures),
    )
