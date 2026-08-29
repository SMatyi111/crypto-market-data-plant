"""Shared skeleton for multi-payload raw-only reference snapshot lanes.

STANDARDS section 4.9 lanes (Binance options chain, Deribit options) archive
several REST payloads per run under one contract: exact upstream bytes gzipped
per endpoint with sha256 recorded, `metrics/summary.json` always written (fetch
errors included, so a run directory without a summary means the process was
killed, never that a payload merely failed), and the job FAILING loudly when
any payload is missing, malformed, or empty. The per-lane modules supply only
their endpoint list and validator; everything contract-shaped lives here so the
on-disk layout cannot drift between lanes.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from ..models import utc_now
from ..storage import write_text_atomic


FetchFn = Callable[[str], bytes]
# (payload_name, parsed_json) -> (row_count, failure_reason)
ValidateFn = Callable[[str, Any], tuple[int | None, str | None]]

_RETRY_MAX_ATTEMPTS = 3
_RETRY_MAX_SLEEP_SECONDS = 15.0
_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})

# Module seam so tests can record instead of really sleeping.
_sleep = time.sleep


def fetch_bytes(
    url: str,
    *,
    user_agent: str,
    timeout_seconds: float = 20.0,
    never_retry_codes: frozenset[int] = frozenset(),
) -> bytes:
    """Fetch one public endpoint as raw bytes with a bounded retry ladder.

    The budgets are deliberately tight: a snapshot lane's real retry is its next
    scheduled interval, and the worst-case wall clock of ALL payloads' retries
    must stay well inside the runner's subprocess timeout (4x interval for
    one-shot jobs) or a venue brownout gets the job killed mid-write instead of
    failing cleanly with a summary on disk.
    """
    delay = 0.0
    for attempt in range(1, _RETRY_MAX_ATTEMPTS + 1):
        request = Request(
            url,
            method="GET",
            headers={"Accept": "application/json", "User-Agent": user_agent},
        )
        try:
            with urlopen(request, timeout=timeout_seconds) as response:
                return response.read()
        except HTTPError as exc:
            if (
                exc.code in never_retry_codes
                or exc.code not in _RETRYABLE_STATUS
                or attempt == _RETRY_MAX_ATTEMPTS
            ):
                raise
            raw_retry = exc.headers.get("Retry-After") if exc.headers is not None else None
            try:
                delay = min(_RETRY_MAX_SLEEP_SECONDS, max(0.0, float(raw_retry)))
            except (TypeError, ValueError):
                delay = min(_RETRY_MAX_SLEEP_SECONDS, 5.0 * attempt)
        except URLError:
            if attempt == _RETRY_MAX_ATTEMPTS:
                raise
            delay = min(_RETRY_MAX_SLEEP_SECONDS, 5.0 * attempt)
        _sleep(delay)
    raise RuntimeError("unreachable raw snapshot retry loop")


def require_safe_tokens(values: tuple[str, ...], what: str) -> None:
    """Reject empty/unsafe lane config before any network or filesystem work.

    Tokens are interpolated into both query strings and payload filenames, so
    anything beyond alphanumerics (path separators, URL metachars) must fail
    loudly here rather than truncate a query or explode mid-run.
    """
    if not values:
        raise ValueError(f"no {what} configured")
    bad = [value for value in values if not value.isalnum()]
    if bad:
        raise ValueError(f"invalid {what} (must be alphanumeric): {bad}")


@dataclass(frozen=True, slots=True)
class RawSnapshotResult:
    run_path: str
    fetched_at: str
    payload_count: int
    total_raw_bytes: int
    row_counts: dict[str, int | None]


def _write_gzip_atomic(path: Path, raw: bytes) -> None:
    # pid-suffixed tmp for the same reason as storage.write_text_atomic: a shared
    # fixed .tmp makes the loser's open handle fail the winner's replace on
    # Windows when two writers land in the same run dir. compresslevel 6: level 9
    # costs ~3x the CPU for ~1% size on JSON, and these lanes run every 5-15 min.
    temporary = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    with gzip.open(temporary, "wb", compresslevel=6) as handle:
        handle.write(raw)
    temporary.replace(path)


def capture_raw_snapshot(
    output_root: Path | str,
    *,
    source: str,
    payloads: list[tuple[str, str]],
    validate: ValidateFn,
    fetch: FetchFn,
    clock: Callable[[], Any] = utc_now,
    extra_summary: dict[str, Any] | None = None,
) -> RawSnapshotResult:
    """Capture one multi-payload snapshot into its own run directory.

    Every payload outcome — archived bytes, validation verdict, or fetch
    error — is recorded in metrics/summary.json BEFORE the job fails, so a
    partial run is always distinguishable from a complete one at read time.
    Raw bytes that did arrive are never discarded.
    """
    fetched_at = clock()
    run_id = fetched_at.strftime("%Y%m%d_%H%M%S")
    run_dir = Path(output_root) / source / run_id
    raw_dir = run_dir / "raw"
    metrics_dir = run_dir / "metrics"
    raw_dir.mkdir(parents=True, exist_ok=True)
    metrics_dir.mkdir(parents=True, exist_ok=True)

    payload_summaries: dict[str, dict[str, Any]] = {}
    failures: list[str] = []
    row_counts: dict[str, int | None] = {}
    total_raw_bytes = 0
    archived_count = 0

    for name, url in payloads:
        payload_fetched_at = clock()
        raw: bytes | None = None
        failure: str | None = None
        row_count: int | None = None
        try:
            raw = fetch(url)
        except OSError as exc:  # HTTPError/URLError/socket errors
            failure = f"{name} fetch failed: {exc}"
        if raw is not None:
            total_raw_bytes += len(raw)
            archived_count += 1
            _write_gzip_atomic(raw_dir / f"{name}.json.gz", raw)
            try:
                parsed = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, ValueError):
                failure = f"{name} payload is not valid JSON"
            else:
                row_count, failure = validate(name, parsed)
        if failure is not None:
            failures.append(failure)
        row_counts[name] = row_count
        payload_summaries[name] = {
            "url": url,
            "fetched_at": payload_fetched_at.isoformat(),
            "raw_bytes": len(raw) if raw is not None else None,
            "sha256": hashlib.sha256(raw).hexdigest() if raw is not None else None,
            "row_count": row_count,
            "parse_ok": failure is None,
            "error": failure,
        }

    summary: dict[str, Any] = {
        "source": source,
        "fetched_at": fetched_at.isoformat(),
        "payloads": payload_summaries,
        "total_raw_bytes": total_raw_bytes,
        "parse_ok": not failures,
        "failures": failures,
    }
    if extra_summary:
        summary.update(extra_summary)
    write_text_atomic(
        metrics_dir / "summary.json", json.dumps(summary, indent=2, sort_keys=True)
    )

    if failures:
        # Whatever raw bytes arrived are already durably archived above and the
        # summary records every per-payload outcome (never discard evidence);
        # failing the job makes the runner's job-status counters surface the
        # problem instead of silently archiving a partial or garbage snapshot.
        raise ValueError(f"{source} snapshot failed validation: {'; '.join(failures)}")

    return RawSnapshotResult(
        run_path=str(run_dir),
        fetched_at=fetched_at.isoformat(),
        payload_count=archived_count,
        total_raw_bytes=total_raw_bytes,
        row_counts=row_counts,
    )
