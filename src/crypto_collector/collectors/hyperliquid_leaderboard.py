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


LEADERBOARD_URL = "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard"
SOURCE_NAME = "hyperliquid_leaderboard"
USER_AGENT = "crypto-market-data-plant-hyperliquid-leaderboard/0.1"

FetchFn = Callable[[], bytes]


def fetch_leaderboard(*, timeout_seconds: float = 120.0) -> bytes:
    """Fetch the public Hyperliquid leaderboard snapshot as raw bytes.

    Raw bytes, not parsed rows: the snapshot is archival reference data whose
    value is the exact point-in-time payload (point-in-time cohort selection is
    impossible to reconstruct retroactively — see STANDARDS §4.8). Parsing
    happens at read time, never at capture time.
    """
    for attempt in range(1, 4):
        request = Request(
            LEADERBOARD_URL,
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
        time.sleep(delay)
    raise RuntimeError("unreachable leaderboard retry loop")


@dataclass(frozen=True, slots=True)
class LeaderboardSnapshotResult:
    run_path: str
    snapshot_path: str
    fetched_at: str
    raw_bytes: int
    sha256: str
    row_count: int | None
    parse_ok: bool


def snapshot_leaderboard(
    output_root: Path | str,
    *,
    fetch: FetchFn = fetch_leaderboard,
    clock: Callable[[], Any] = utc_now,
) -> LeaderboardSnapshotResult:
    """Capture one leaderboard snapshot into its own run directory.

    Layout mirrors the market lanes (`<lane>/<run_id>/raw/...` + `metrics/`)
    so hygiene tooling recognizes it, but this is a raw-only REFERENCE lane:
    no clean/quarantine split, no normalization, no replay verdict, and no
    promotion — the archived snapshot itself is the deliverable.
    """
    fetched_at = clock()
    raw = fetch()
    digest = hashlib.sha256(raw).hexdigest()
    row_count: int | None = None
    parse_ok = False
    try:
        payload = json.loads(raw.decode("utf-8"))
        rows = payload.get("leaderboardRows")
        if isinstance(rows, list):
            row_count = len(rows)
            parse_ok = row_count > 0
    except (UnicodeDecodeError, ValueError):
        parse_ok = False

    run_id = fetched_at.strftime("%Y%m%d_%H%M%S")
    run_dir = Path(output_root) / SOURCE_NAME / run_id
    raw_dir = run_dir / "raw"
    metrics_dir = run_dir / "metrics"
    raw_dir.mkdir(parents=True, exist_ok=True)
    metrics_dir.mkdir(parents=True, exist_ok=True)

    snapshot_path = raw_dir / "leaderboard.json.gz"
    temporary = snapshot_path.with_name(f"{snapshot_path.name}.tmp")
    with gzip.open(temporary, "wb") as handle:
        handle.write(raw)
    temporary.replace(snapshot_path)

    summary = {
        "source": SOURCE_NAME,
        "url": LEADERBOARD_URL,
        "fetched_at": fetched_at.isoformat(),
        "raw_bytes": len(raw),
        "sha256": digest,
        "row_count": row_count,
        "parse_ok": parse_ok,
    }
    summary_path = metrics_dir / "summary.json"
    summary_tmp = summary_path.with_name(f"{summary_path.name}.tmp")
    summary_tmp.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    summary_tmp.replace(summary_path)

    if not parse_ok:
        # The raw payload is already durably archived above (never discard
        # evidence); failing the job makes the runner's job-status counters
        # surface a malformed or empty snapshot instead of silently archiving
        # garbage forever.
        raise ValueError(
            f"leaderboard snapshot failed validation: raw_bytes={len(raw)} row_count={row_count}"
        )

    return LeaderboardSnapshotResult(
        run_path=str(run_dir),
        snapshot_path=str(snapshot_path),
        fetched_at=fetched_at.isoformat(),
        raw_bytes=len(raw),
        sha256=digest,
        row_count=row_count,
        parse_ok=parse_ok,
    )
