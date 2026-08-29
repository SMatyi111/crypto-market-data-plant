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


BASE_URL = "https://www.deribit.com/api/v2"
SOURCE_NAME = "deribit_options"
USER_AGENT = "crypto-market-data-plant-deribit-options/0.1"
DEFAULT_CURRENCIES = ("BTC", "ETH")

FetchFn = Callable[[str], bytes]

# Module seam so tests can observe/skip the retry backoff.
_sleep = time.sleep


def fetch_url(url: str, *, timeout_seconds: float = 60.0) -> bytes:
    """Fetch one Deribit public endpoint as raw bytes.

    Raw bytes, not parsed rows: the snapshot is archival reference data whose
    value is the exact point-in-time payload (STANDARDS section 4.9). Parsing
    happens at read time, never at capture time.
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
            if exc.code not in {429, 500, 502, 503, 504} or attempt == 3:
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
    raise RuntimeError("unreachable deribit options retry loop")


@dataclass(frozen=True, slots=True)
class DeribitOptionsSnapshotResult:
    run_path: str
    fetched_at: str
    payload_count: int
    total_raw_bytes: int
    option_summary_count: int | None
    parse_ok: bool
    failures: tuple[str, ...]


def _snapshot_payloads(currencies: tuple[str, ...]) -> list[tuple[str, str]]:
    payloads: list[tuple[str, str]] = []
    for currency in currencies:
        payloads.extend(
            [
                (
                    f"instruments_{currency}",
                    f"{BASE_URL}/public/get_instruments?currency={currency}&kind=option&expired=false",
                ),
                (
                    f"option_summaries_{currency}",
                    f"{BASE_URL}/public/get_book_summary_by_currency?currency={currency}&kind=option",
                ),
                (
                    f"future_summaries_{currency}",
                    f"{BASE_URL}/public/get_book_summary_by_currency?currency={currency}&kind=future",
                ),
            ]
        )
    return payloads


def _validate_payload(name: str, parsed: Any) -> tuple[int | None, str | None]:
    """Return (row_count, failure_reason) for one parsed JSON-RPC payload."""
    if not isinstance(parsed, dict):
        return None, f"{name} payload is not a JSON object"
    if "error" in parsed:
        return None, f"{name} returned a JSON-RPC error: {parsed['error']!r}"
    result = parsed.get("result")
    if not isinstance(result, list) or not result:
        return None, f"{name} result is not a non-empty list"
    return len(result), None


def _write_gzip_atomic(path: Path, raw: bytes) -> None:
    temporary = path.with_name(f"{path.name}.tmp")
    with gzip.open(temporary, "wb") as handle:
        handle.write(raw)
    temporary.replace(path)


def snapshot_deribit_options(
    output_root: Path | str,
    *,
    currencies: tuple[str, ...] = DEFAULT_CURRENCIES,
    fetch: FetchFn = fetch_url,
    clock: Callable[[], Any] = utc_now,
) -> DeribitOptionsSnapshotResult:
    """Capture one Deribit options snapshot into its own run dir.

    Raw-only reference lane (STANDARDS section 4.9): per currency it archives
    the live option instrument list plus the option and future book summaries,
    exact upstream bytes gzipped with sha256 recorded. No clean/quarantine
    split, no normalization, no promotion. A malformed or empty payload still
    archives the raw bytes but FAILS the job so runner job-status counters
    surface it.
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
    option_summary_count = 0
    saw_option_summaries = False

    for name, url in _snapshot_payloads(currencies):
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
            row_count, failure = _validate_payload(name, parsed)
        if name.startswith("option_summaries_") and row_count is not None:
            option_summary_count += row_count
            saw_option_summaries = True
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
        "currencies": list(currencies),
        "payloads": payload_summaries,
        "total_raw_bytes": total_raw_bytes,
        "option_summary_count": option_summary_count if saw_option_summaries else None,
        "parse_ok": parse_ok,
        "failures": failures,
    }
    summary_path = metrics_dir / "summary.json"
    summary_tmp = summary_path.with_name(f"{summary_path.name}.tmp")
    summary_tmp.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    summary_tmp.replace(summary_path)

    if not parse_ok:
        # Raw payloads are already durably archived above (never discard
        # evidence); failing the job surfaces a malformed or empty snapshot in
        # the runner's job-status counters instead of silently archiving
        # garbage forever.
        raise ValueError(
            f"deribit options snapshot failed validation: {'; '.join(failures)}"
        )

    return DeribitOptionsSnapshotResult(
        run_path=str(run_dir),
        fetched_at=fetched_at.isoformat(),
        payload_count=len(payload_summaries),
        total_raw_bytes=total_raw_bytes,
        option_summary_count=option_summary_count if saw_option_summaries else None,
        parse_ok=parse_ok,
        failures=tuple(failures),
    )
