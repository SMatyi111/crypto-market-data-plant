"""Bounded stage-one alt canary controls. Default off; prepared explicitly; never a launcher.

One prepared control directory is one immutable attempt: a fixed symbol, a fixed set of
lane jobs (at most one Bybit linear trades lane, one depth lane and the daily universe
snapshot), a shared byte allowance for everything those lanes write under their run
directories (raw, clean, quarantine, metrics and the replay summary), and a duration
that starts at the first grant. Lane workers claim a grant per segment, reserve bytes
from the shared pool BEFORE writing (chunked, with a per-frame margin), stop their own
segment when the pool, the clock or a durable stop says so, and leave one immutable
receipt per grant. Nothing here retries, resets, renews, deletes data or touches any
other worker. A stop here is a durable refusal plus, only on explicit request, the
termination of exactly the workers this canary recorded, validated by pid AND process
creation time through the handle used to terminate (a reused pid is never a target).

The control database follows `evidence_lease`: SQLite, DELETE journaling, 4 KiB pages,
bounded page count, `BEGIN IMMEDIATE` for every reservation, `synchronous=FULL`.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import os
import socket
import sqlite3
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from shutil import disk_usage

from .evidence_budget import GIB, MIB, required_free_bytes
from .evidence_lease import ID

logger = logging.getLogger(__name__)

VERSION = 2  # control-DB schema: v2 added max_grants and the recorded worker image
DEFAULT_MAX_BYTES = 2 * GIB
DEFAULT_MAX_SECONDS = 86_400
MAX_MAX_BYTES = 64 * GIB
MAX_MAX_SECONDS = 30 * 86_400
# Reserved out of the shared allowance for the control DB, its rollback journal and
# every receipt (one small JSON per grant plus the final one). The DB is capped at
# DB_MAX_PAGES x 4 KiB; `prepare` refuses an attempt whose worst-case grant count
# (every lane re-claiming at its shortest segment for the whole duration) would not
# fit, so a long attempt cannot wedge its own control plane.
METADATA_BYTES = 32 * MIB
DB_MAX_PAGES = 2048
GRANT_ROW_BYTES = 640
GRANT_SLACK = 8
# Stream lanes reserve this much from the pool at a time (pre-write). Up to one chunk
# per open stream grant is in flight (reserved, not yet written) at any moment.
CHUNK_BYTES = 16 * MIB
# A frame is admitted only while the open grant still covers this margin. The canary
# caps inbound WS frames at MAX_FRAME_BYTES; one frame's raw line plus its clean and
# quarantine fan-out plus a metrics row are assumed to fit 4x that. If a frame ever
# exceeds the margin the grant is overshot by at most that one frame, the segment stops
# at once and the receipt records `overshoot_bytes`; it is never silent.
MAX_FRAME_BYTES = MIB
FRAME_MARGIN_BYTES = 4 * MAX_FRAME_BYTES
# Per grant: replay_summary.json, the final metrics row and filesystem slack.
SEGMENT_OVERHEAD_BYTES = MIB
# The universe snapshot writes at most five 1 MiB bodies (gzipped) plus a summary.
SNAPSHOT_RESERVE_BYTES = 6 * MIB
# Space the canary needs ABOVE the operator floor and the unspent pool: allocation
# granularity, ops logs (heartbeats, job runs, worker events) and runner stdout.
EXTERNAL_OVERHEAD_BYTES = 256 * MIB
MIN_CLAIM_SECONDS = 60
DEFAULT_MIN_FREE_GIB = 215
CLOCK_TOLERANCE_SECONDS = 0.5
# Cross-process clock agreement is checked against a checkpoint of arbitrary age, so
# the tolerance grows with the gap (time-service slew is gradual, a step is not).
CLOCK_SLEW_FRACTION = 0.01
STREAM_LANES = frozenset({"bybit-trades-worker", "bybit-depth-worker"})
SNAPSHOT_LANES = frozenset({"bybit-linear-universe-snapshot"})
# A canary segment that ended this soon after it started (socket refused, closed after
# the ack, immediate exception) makes the worker hold before exiting, so a venue outage
# burns grants at most once a minute per lane instead of once per runner tick.
SHORT_SEGMENT_SECONDS = 60.0


class CanaryRefused(ValueError):
    """Reason token only; never a payload, path or pid."""


def _is_lock_contention(exc: sqlite3.Error) -> bool:
    code = getattr(exc, "sqlite_errorcode", None)
    if code is not None:
        # Extended codes (SQLITE_BUSY_TIMEOUT, SQLITE_LOCKED_SHAREDCACHE...) keep the
        # primary code in the low byte.
        return (int(code) & 0xFF) in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED)
    text = str(exc).lower()
    return "locked" in text or "busy" in text


def _retry_locked(operation, *, attempts: int = 3):
    """Run a control-DB operation, retrying transient lock contention with a sibling
    lane. Raises CanaryRefused('canary_control_busy') when it persists and
    CanaryRefused('canary_control_db') for every other SQLite failure."""
    attempts = max(1, int(attempts))
    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except sqlite3.Error as exc:
            if not _is_lock_contention(exc):
                raise CanaryRefused("canary_control_db") from exc
            if attempt == attempts:
                raise CanaryRefused("canary_control_busy") from exc
            time.sleep(1.0)


def _clock_disagrees(now, last, mono, last_mono) -> bool:
    gap = max(0.0, mono - last_mono)
    return abs((now - last) - (mono - last_mono)) > CLOCK_TOLERANCE_SECONDS + CLOCK_SLEW_FRACTION * gap


# --------------------------------------------------------------------------- identity


def process_identity(pid: int) -> str | None:
    """Creation-time identity of a live process, or None when it does not exist.

    Raises PermissionError when the process exists but cannot be opened (a non-elevated
    shell looking at the SYSTEM task's workers): callers must treat that as UNKNOWN,
    never as exited. Windows: kernel creation FILETIME via an opened handle. Linux:
    the start-time tick from /proc/<pid>/stat. Other platforms: PermissionError
    (identity unavailable, so liveness can never be asserted).
    """
    if pid <= 0:
        return None
    if sys.platform == "win32":
        return _identity_windows(pid)
    if sys.platform.startswith("linux"):
        try:
            text = Path(f"/proc/{pid}/stat").read_text()
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise PermissionError(str(exc)) from exc
        return "linux:" + text.rsplit(")", 1)[1].split()[19]
    raise PermissionError("process identity unavailable on this platform")


def _kernel32():
    import ctypes
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    kernel32.GetProcessTimes.argtypes = [wintypes.HANDLE] + [ctypes.POINTER(wintypes.FILETIME)] * 4
    kernel32.GetProcessTimes.restype = wintypes.BOOL
    kernel32.GetExitCodeProcess.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
    kernel32.GetExitCodeProcess.restype = wintypes.BOOL
    kernel32.TerminateProcess.argtypes = [wintypes.HANDLE, wintypes.UINT]
    kernel32.TerminateProcess.restype = wintypes.BOOL
    kernel32.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    kernel32.WaitForSingleObject.restype = wintypes.DWORD
    kernel32.QueryFullProcessImageNameW.argtypes = [wintypes.HANDLE, wintypes.DWORD, wintypes.LPWSTR,
                                                    ctypes.POINTER(wintypes.DWORD)]
    kernel32.QueryFullProcessImageNameW.restype = wintypes.BOOL
    return kernel32


def _image_path_windows(kernel32, handle) -> str | None:
    import ctypes
    from ctypes import wintypes

    size = wintypes.DWORD(32767)
    buffer = ctypes.create_unicode_buffer(size.value)
    if not kernel32.QueryFullProcessImageNameW(handle, 0, buffer, ctypes.byref(size)):
        return None
    return os.path.normcase(os.path.realpath(buffer.value))


def process_image(pid: int) -> str | None:
    """Kernel image path of a live process (realpath, case-folded), or None when it
    cannot be read. A worker records its own at claim; `terminate_owned` compares the
    target's image through the terminating handle against that record."""
    try:
        if sys.platform == "win32":
            kernel32 = _kernel32()
            handle = kernel32.OpenProcess(0x1000, False, pid)
            if not handle:
                return None
            try:
                return _image_path_windows(kernel32, handle)
            finally:
                kernel32.CloseHandle(handle)
        return os.path.normcase(os.path.realpath(os.readlink(f"/proc/{pid}/exe")))
    except OSError:
        return None


def _identity_windows(pid: int) -> str | None:
    import ctypes
    from ctypes import wintypes

    kernel32 = _kernel32()
    handle = kernel32.OpenProcess(0x1000, False, pid)  # PROCESS_QUERY_LIMITED_INFORMATION
    if not handle:
        error = ctypes.get_last_error()
        if error == 87:  # ERROR_INVALID_PARAMETER: no such process
            return None
        raise PermissionError(f"OpenProcess failed with error {error}")
    try:
        return _identity_from_handle(kernel32, handle, wintypes)
    finally:
        kernel32.CloseHandle(handle)


def _identity_from_handle(kernel32, handle, wintypes) -> str | None:
    import ctypes

    created, exited, kernel, user = (wintypes.FILETIME() for _ in range(4))
    if not kernel32.GetProcessTimes(handle, ctypes.byref(created), ctypes.byref(exited),
                                    ctypes.byref(kernel), ctypes.byref(user)):
        raise PermissionError(f"GetProcessTimes failed with error {ctypes.get_last_error()}")
    code = wintypes.DWORD()
    if kernel32.GetExitCodeProcess(handle, ctypes.byref(code)) and code.value != 259:  # STILL_ACTIVE
        return None
    return f"win:{(created.dwHighDateTime << 32) | created.dwLowDateTime:x}"


def owned_process_alive(pid: int, identity: str) -> bool | None:
    """True = that exact process is running; False = gone or pid reused; None = unknown."""
    try:
        current = process_identity(pid)
    except PermissionError:
        return None
    return current is not None and current == identity


def terminate_owned(pid: int, identity: str, image: str | None, *, wait_seconds: float = 10.0) -> dict:
    """Terminate exactly the process whose creation-time identity AND kernel image path
    match what the worker recorded at claim, through the handle used for the check
    (no window for pid reuse). Never a name or tree sweep; an unrecorded image refuses."""
    if not image:
        return {"terminated": False, "reason": "image_unrecorded"}
    if sys.platform == "win32":
        return _terminate_windows(pid, identity, image, wait_seconds)
    try:
        if owned_process_alive(pid, identity) is not True:
            return {"terminated": False, "reason": "identity_mismatch_or_exited"}
        if process_image(pid) != image:
            return {"terminated": False, "reason": "image_mismatch"}
        import signal
        os.kill(pid, signal.SIGTERM)
    except (PermissionError, ProcessLookupError) as exc:
        return {"terminated": False, "reason": type(exc).__name__}
    deadline = time.monotonic() + wait_seconds
    while time.monotonic() < deadline:
        if owned_process_alive(pid, identity) is False:
            return {"terminated": True, "reason": "exited"}
        time.sleep(0.05)
    return {"terminated": False, "reason": "still_running_after_wait"}


def _terminate_windows(pid: int, identity: str, image: str, wait_seconds: float) -> dict:
    import ctypes
    from ctypes import wintypes

    kernel32 = _kernel32()
    access = 0x0001 | 0x1000 | 0x00100000  # TERMINATE | QUERY_LIMITED | SYNCHRONIZE
    handle = kernel32.OpenProcess(access, False, pid)
    if not handle:
        error = ctypes.get_last_error()
        return {"terminated": False, "reason": "exited" if error == 87 else f"open_failed_{error}"}
    try:
        try:
            current = _identity_from_handle(kernel32, handle, wintypes)
        except PermissionError as exc:
            return {"terminated": False, "reason": str(exc)}
        if current != identity:
            return {"terminated": False, "reason": "identity_mismatch_or_exited"}
        # The control DB names the target; the kernel must also report the image the
        # worker recorded for itself. A row describing another process is refused.
        if _image_path_windows(kernel32, handle) != image:
            return {"terminated": False, "reason": "image_mismatch"}
        if not kernel32.TerminateProcess(handle, 1):
            return {"terminated": False, "reason": f"terminate_failed_{ctypes.get_last_error()}"}
        result = kernel32.WaitForSingleObject(handle, int(max(0.0, wait_seconds) * 1000))
        if result == 0:
            return {"terminated": True, "reason": "exited"}
        return {"terminated": False, "reason": "still_running_after_wait"}
    finally:
        kernel32.CloseHandle(handle)


# --------------------------------------------------------------------------- control DB


def job_identity(job: dict) -> str:
    """Digest of one config job row minus `enabled`, so the activation flip keeps it."""
    row = {"name": job["name"], "job_type": job["job_type"],
           "interval_seconds": int(job.get("interval_seconds", 0)), "args": job.get("args", {})}
    return hashlib.sha256(json.dumps(row, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _connect(directory: Path, *, check_version: bool = True) -> sqlite3.Connection:
    """Every surface (workers, status, stop, finalize, preflight) opens the control DB
    here, so a directory written by another schema version is refused everywhere."""
    db = directory / "canary.sqlite3"
    if not db.is_file() or db.stat().st_size > DB_MAX_PAGES * 4096:
        raise CanaryRefused("canary_missing_or_oversize")
    conn = sqlite3.connect(db.resolve().as_uri() + "?mode=rw", uri=True, timeout=5)
    try:
        if conn.execute("PRAGMA journal_mode").fetchone()[0] != "delete":
            raise CanaryRefused("canary_journal_mode")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute(f"PRAGMA max_page_count={DB_MAX_PAGES}")
        if conn.execute("PRAGMA page_size").fetchone()[0] != 4096:
            raise CanaryRefused("canary_page_size")
        if check_version:
            rows = conn.execute("SELECT version FROM canary").fetchall()
            if len(rows) != 1 or rows[0][0] != VERSION:
                raise CanaryRefused("canary_corrupt")
        return conn
    except BaseException:
        conn.close()
        raise


def _stop(conn: sqlite3.Connection, reason: str, now: float) -> None:
    conn.execute("UPDATE canary SET stopped=COALESCE(stopped, ?), stopped_utc=COALESCE(stopped_utc, ?)",
                 (reason, now))
    conn.commit()
    raise CanaryRefused(reason)


def _lane_kind(job_type: str) -> str:
    if job_type in STREAM_LANES:
        return "stream"
    if job_type in SNAPSHOT_LANES:
        return "snapshot"
    raise ValueError(f"canary_unsupported_lane:{job_type}")


def _validate_lane_job(job: dict, *, symbol: str, directory: Path) -> str:
    kind = _lane_kind(str(job["job_type"]))
    args = job.get("args", {})
    control = args.get("canary_control")
    if not isinstance(control, str) or Path(control).resolve() != directory.resolve():
        raise ValueError(f"{job['name']}: args.canary_control must name the prepared directory")
    if kind == "stream":
        if args.get("symbol") != symbol or args.get("market") != "linear":
            raise ValueError(f"{job['name']}: stream lane must be {symbol} linear")
        if str(args.get("source_suffix", "")).lower() != symbol.lower():
            raise ValueError(f"{job['name']}: source_suffix must isolate the symbol directory")
        if args.get("normalized_parquet", True) is not False:
            raise ValueError(f"{job['name']}: canary lanes are raw-only (normalized_parquet false)")
        if args.get("rotate_at_midnight") or not 0 < float(args.get("max_segment_seconds") or 0) <= 1800:
            raise ValueError(f"{job['name']}: max_segment_seconds must be in (0, 1800] without midnight rotation")
    else:
        panel = args.get("panel_symbols")
        if not isinstance(panel, list) or symbol not in panel:
            raise ValueError(f"{job['name']}: panel_symbols must include {symbol}")
    return kind


def load_jobs(config_path: Path) -> list[dict]:
    jobs = json.loads(Path(config_path).read_text(encoding="utf-8"))["jobs"]
    return [job for job in jobs if isinstance(job, dict) and "name" in job and "job_type" in job]


def prepare(directory: Path, canary_id: str, *, config_path: Path, symbol: str, job_names: list[str],
            max_bytes: int = DEFAULT_MAX_BYTES, max_seconds: int = DEFAULT_MAX_SECONDS) -> dict:
    """One-time operator action; refuses any existing directory. Creates control state only."""
    if not ID.fullmatch(canary_id):
        raise ValueError("Invalid canary ID")
    if not isinstance(symbol, str) or not symbol.isalnum() or not symbol.isupper():
        raise ValueError("Symbol must be an upper-case alphanumeric venue symbol")
    if type(max_bytes) is not int or not METADATA_BYTES + CHUNK_BYTES + SEGMENT_OVERHEAD_BYTES <= max_bytes <= MAX_MAX_BYTES:
        raise ValueError("max_bytes out of range")
    if type(max_seconds) is not int or not MIN_CLAIM_SECONDS <= max_seconds <= MAX_MAX_SECONDS:
        raise ValueError("max_seconds out of range")
    if not job_names or len(set(job_names)) != len(job_names):
        raise ValueError("At least one distinct lane job is required")
    by_name = {job["name"]: job for job in load_jobs(config_path)}
    lanes = []
    kinds: dict[str, int] = {}
    worst_case_grants = 0
    for name in job_names:
        if not ID.fullmatch(name):
            raise ValueError(f"{name!r}: lane job names are bounded tokens (they name receipt files)")
        if name not in by_name:
            raise ValueError(f"{name}: not in {config_path}")
        job = by_name[name]
        kind = _validate_lane_job(job, symbol=symbol, directory=directory)
        key = kind if kind == "snapshot" else str(job["job_type"])
        kinds[key] = kinds.get(key, 0) + 1
        if kinds[key] > 1:
            raise ValueError(f"{name}: stage one allows one lane per kind")
        if kind == "stream":
            worst_case_grants += math.ceil(max_seconds / float(job["args"]["max_segment_seconds"])) + 1
        else:
            worst_case_grants += math.ceil(max_seconds / max(1, int(job.get("interval_seconds", 86400)))) + 1
        lanes.append((name, str(job["job_type"]), kind, job_identity(job)))
    # Segments can end early (idle timeout, socket close, exception) and the runner
    # re-dispatches on its cadence, so the planned count is not a bound. The attempt
    # therefore carries a hard grant ceiling: GRANT_SLACK times the planned count, still
    # within the DB, after which claims are a durable `canary_grants_exhausted` stop.
    max_grants = worst_case_grants * GRANT_SLACK
    if max_grants * GRANT_ROW_BYTES > (DB_MAX_PAGES - 16) * 4096:
        raise ValueError(f"control DB cannot hold {max_grants} grants ({worst_case_grants} planned x {GRANT_SLACK}): "
                         "shorten max_seconds or lengthen the lanes' segments")
    directory.mkdir(exist_ok=False)
    (directory / "receipts").mkdir()
    with (directory / "canary.sqlite3").open("xb"):
        pass
    conn = _connect(directory, check_version=False)
    try:
        with conn:
            conn.execute("CREATE TABLE canary (id TEXT PRIMARY KEY, version INTEGER, symbol TEXT, "
                         "max_bytes INTEGER, max_seconds INTEGER, metadata_bytes INTEGER, max_grants INTEGER, "
                         "prepared_utc REAL, hostname TEXT, config_name TEXT, first_utc REAL, first_mono REAL, "
                         "last_utc REAL, last_mono REAL, stopped TEXT, stopped_utc REAL, finalized_utc REAL)")
            conn.execute("CREATE TABLE lanes (lane TEXT PRIMARY KEY, job_type TEXT, kind TEXT, identity TEXT)")
            conn.execute("CREATE TABLE grants (id INTEGER PRIMARY KEY AUTOINCREMENT, lane TEXT, pid INTEGER, "
                         "identity TEXT, image TEXT, hostname TEXT, started_utc REAL, reserved INTEGER, "
                         "consumed INTEGER, accounted INTEGER, overshoot INTEGER, closed_utc REAL, reason TEXT, "
                         "replayable INTEGER, run_path TEXT, receipt TEXT)")
            conn.execute("INSERT INTO canary VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
                         (canary_id, VERSION, symbol, max_bytes, max_seconds, METADATA_BYTES, max_grants, time.time(),
                          socket.gethostname(), Path(config_path).name))
            conn.executemany("INSERT INTO lanes VALUES (?, ?, ?, ?)", lanes)
    finally:
        conn.close()
    return {"canary_id": canary_id, "directory": str(directory), "symbol": symbol,
            "lanes": [{"lane": n, "job_type": t, "kind": k, "identity": i} for n, t, k, i in lanes],
            "max_bytes": max_bytes, "max_seconds": max_seconds, "metadata_bytes": METADATA_BYTES,
            "max_grants": max_grants}


def _committed_bytes(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COALESCE(SUM(CASE WHEN closed_utc IS NULL THEN reserved ELSE consumed END), 0) "
                       "FROM grants").fetchone()
    return int(row[0])


def _closed_consumed(conn: sqlite3.Connection) -> int:
    return int(conn.execute("SELECT COALESCE(SUM(consumed), 0) FROM grants WHERE closed_utc IS NOT NULL").fetchone()[0])


def measure_run(run_path: str | None) -> tuple[int, list[dict]]:
    """Bytes of every regular file under the run directory, as written on disk."""
    if not run_path:
        return 0, []
    root = Path(run_path)
    if not root.is_dir():
        return 0, []
    files = []
    total = 0
    for path in sorted(p for p in root.rglob("*") if p.is_file()):
        size = path.stat().st_size
        total += size
        files.append({"path": path.relative_to(root).as_posix(), "bytes": size})
    return total, files


def _reconcile_open_grants(conn: sqlite3.Connection, now: float, *, lane: str | None = None) -> list[int]:
    """Close grants whose recorded process is provably gone (pid reused counts as gone),
    charging what its run directory holds. Unknown liveness stays open. Returns the
    ids of grants that are still open AND alive-or-unknown."""
    blocking: list[int] = []
    query = "SELECT id, lane, pid, identity, reserved, run_path FROM grants WHERE closed_utc IS NULL"
    rows = conn.execute(query + (" AND lane=?" if lane else ""), (lane,) if lane else ()).fetchall()
    for grant_id, _, pid, identity, reserved, run_path in rows:
        alive = owned_process_alive(int(pid), str(identity))
        if alive is False:
            measured, _ = measure_run(run_path)
            conn.execute("UPDATE grants SET closed_utc=?, consumed=?, overshoot=?, reason='abandoned', "
                         "replayable=NULL WHERE id=?",
                         (now, measured, max(0, measured - int(reserved)), grant_id))
        else:
            blocking.append(int(grant_id))
    return blocking


class CanaryGrant:
    """One lane's claim on the canary for one run. Claimed before any run file exists."""

    def __init__(self, directory: Path, *, lane: str, identity: str, kind: str = "stream",
                 min_free_bytes: int = DEFAULT_MIN_FREE_GIB * GIB) -> None:
        self.directory = Path(directory)
        self.lane, self.identity, self.kind = lane, identity, kind
        self.min_free_bytes = int(min_free_bytes)
        self.grant_id: int | None = None
        self.canary_id: str | None = None
        self.deadline: float | None = None
        self.monotonic_deadline: float | None = None
        self.max_bytes = 0
        self.run_path: Path | None = None
        self.reserved = 0
        self.accounted = 0
        self.overshoot = 0
        self.stop_reason: str | None = None
        self.terminal = False
        self._last_clock: tuple[float, float] | None = None
        self._last_checkpoint = 0.0
        self.receipt_path: Path | None = None
        self.pid: int | None = None
        self.process_identity: str | None = None
        self.image: str | None = None

    # -- claim ------------------------------------------------------------------

    def claim(self) -> None:
        pid = os.getpid()
        try:
            self_identity = process_identity(pid)
        except PermissionError:
            self_identity = None
        if self_identity is None:
            raise CanaryRefused("canary_identity_unavailable")
        image = process_image(pid)
        # Lock contention with a sibling lane is transient: a short retry here, a
        # short hold in the worker, never the full refusal hold.
        _retry_locked(lambda: self._claim(pid, self_identity, image))

    def _claim(self, pid: int, self_identity: str, image: str | None) -> None:
        conn = _connect(self.directory)
        try:
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                # Sampled while the lock is held: a sibling lane's checkpoint committed
                # before this transaction can never carry a later time than this sample.
                now, mono = time.time(), time.monotonic()
                row = conn.execute("SELECT id, version, symbol, max_bytes, max_seconds, metadata_bytes, max_grants, "
                                   "first_utc, first_mono, last_utc, last_mono, stopped FROM canary").fetchall()
                if len(row) != 1 or row[0][1] != VERSION:
                    raise CanaryRefused("canary_corrupt")
                (canary_id, _, _, max_bytes, max_seconds, metadata, max_grants, first, first_mono,
                 last, last_mono, stopped) = row[0]
                if stopped is not None:
                    raise CanaryRefused(str(stopped))
                lane = conn.execute("SELECT kind, identity FROM lanes WHERE lane=?", (self.lane,)).fetchone()
                if lane is None or lane[0] != self.kind or lane[1] != self.identity:
                    raise CanaryRefused("canary_identity")
                if first is not None:
                    if (not all(isinstance(v, (int, float)) and math.isfinite(v) for v in (first, first_mono, last, last_mono))
                            or not (first <= last and 0 <= first_mono <= last_mono)):
                        _stop(conn, "canary_corrupt", now)
                    if now < last or mono < last_mono or _clock_disagrees(now, last, mono, last_mono):
                        _stop(conn, "canary_clock", now)
                    if now >= first + max_seconds or mono >= first_mono + max_seconds:
                        _stop(conn, "canary_expired", now)
                    if now + MIN_CLAIM_SECONDS > first + max_seconds:
                        raise CanaryRefused("canary_expiring")
                if _reconcile_open_grants(conn, now, lane=self.lane):
                    raise CanaryRefused("canary_lane_busy")
                if conn.execute("SELECT COUNT(*) FROM grants").fetchone()[0] >= int(max_grants):
                    _stop(conn, "canary_grants_exhausted", now)
                initial = (CHUNK_BYTES + SEGMENT_OVERHEAD_BYTES) if self.kind == "stream" else SNAPSHOT_RESERVE_BYTES
                pool = int(max_bytes) - int(metadata)
                if pool - _closed_consumed(conn) < initial:
                    _stop(conn, "canary_exhausted", now)
                if pool - _committed_bytes(conn) < initial:
                    raise CanaryRefused("canary_budget_low")
                cursor = conn.execute("INSERT INTO grants (lane, pid, identity, image, hostname, started_utc, "
                                      "reserved, consumed, accounted, overshoot) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, 0)",
                                      (self.lane, pid, self_identity, image, socket.gethostname(), now, initial))
                first = first if first is not None else now
                first_mono = first_mono if first_mono is not None else mono
                conn.execute("UPDATE canary SET first_utc=?, first_mono=?, last_utc=?, last_mono=?",
                             (first, first_mono, now, mono))
                self.grant_id = int(cursor.lastrowid)
            self.canary_id = str(canary_id)
            self.max_bytes = int(max_bytes)
            self.reserved = initial
            self.deadline = first + int(max_seconds)
            self.monotonic_deadline = first_mono + int(max_seconds)
            self._last_clock = (now, mono)
            self._last_checkpoint = mono
            self.pid, self.process_identity, self.image = pid, self_identity, image
        finally:
            conn.close()

    @property
    def deadline_utc(self) -> datetime:
        if self.deadline is None:
            raise CanaryRefused("canary_not_claimed")
        return datetime.fromtimestamp(self.deadline, tz=UTC)

    def remaining(self) -> float:
        if self.deadline is None:
            return 0.0
        return max(0.0, min(self.deadline - time.time(), self.monotonic_deadline - time.monotonic()))

    def bind_run(self, run_path: Path) -> None:
        self.run_path = Path(run_path)

        def update():
            conn = _connect(self.directory)
            try:
                with conn:
                    conn.execute("UPDATE grants SET run_path=? WHERE id=?", (str(self.run_path), self.grant_id))
            finally:
                conn.close()

        try:
            _retry_locked(update)
        except CanaryRefused as exc:
            # The first admission ends the segment cleanly; the receipt still names the run.
            self.stop_reason = str(exc)
            logger.warning("canary bind failed: %s", exc)

    # -- per-frame admission (stream lanes) ----------------------------------------

    def _durable_stop(self, reason: str, now: float) -> str:
        self.stop_reason, self.terminal = reason, True
        try:
            conn = _connect(self.directory)
            try:
                conn.execute("UPDATE canary SET stopped=COALESCE(stopped, ?), stopped_utc=COALESCE(stopped_utc, ?)",
                             (reason, now))
                conn.commit()
            finally:
                conn.close()
        except (CanaryRefused, sqlite3.Error):
            pass  # the in-process stop stands; the next claim re-detects the condition
        return reason

    def admit_frame(self) -> str | None:
        """None admits the next frame; otherwise the reason the segment must end now.
        Clock and expiry every frame; the control DB at most once per second or when
        the local reservation no longer covers one worst-case frame."""
        if self.grant_id is None:
            return "canary_not_claimed"
        if self.stop_reason is not None:
            return self.stop_reason
        now, mono = time.time(), time.monotonic()
        old_wall, old_mono = self._last_clock
        if now < old_wall or mono < old_mono or _clock_disagrees(now, old_wall, mono, old_mono):
            return self._durable_stop("canary_clock", now)
        self._last_clock = (now, mono)
        if now >= self.deadline or mono >= self.monotonic_deadline:
            return self._durable_stop("canary_expired", now)
        need_chunk = self.reserved - self.accounted < FRAME_MARGIN_BYTES + SEGMENT_OVERHEAD_BYTES
        if not need_chunk and mono - self._last_checkpoint < 1.0:
            return None
        try:
            conn = _connect(self.directory)
        except (CanaryRefused, sqlite3.Error) as exc:
            self.stop_reason = str(exc) if isinstance(exc, CanaryRefused) else "canary_control_db"
            return self.stop_reason
        try:
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                now, mono = time.time(), time.monotonic()  # see claim(): sampled under the lock
                row = conn.execute("SELECT stopped, last_utc, max_bytes, metadata_bytes FROM canary").fetchone()
                if row is None:
                    raise CanaryRefused("canary_corrupt")
                if row[0] is not None:
                    self.stop_reason, self.terminal = str(row[0]), True
                    return self.stop_reason
                if now < float(row[1]):
                    self.stop_reason, self.terminal = "canary_clock", True
                    _stop(conn, "canary_clock", now)
                conn.execute("UPDATE canary SET last_utc=?, last_mono=?", (now, mono))
                self._last_checkpoint = mono
                if need_chunk:
                    pool = int(row[2]) - int(row[3])
                    available = pool - _committed_bytes(conn)
                    chunk = min(CHUNK_BYTES, available)
                    if chunk < FRAME_MARGIN_BYTES:
                        self.stop_reason = "canary_budget"
                        return self.stop_reason
                    if self.run_path is not None:
                        free = disk_usage(self.run_path).free
                        if free < required_free_bytes(pool - _closed_consumed(conn), 0, self.min_free_bytes):
                            self.stop_reason, self.terminal = "canary_headroom", True
                            _stop(conn, "canary_headroom", now)
                    conn.execute("UPDATE grants SET reserved=reserved+? WHERE id=?", (chunk, self.grant_id))
                    self.reserved += chunk
        except CanaryRefused as exc:
            self.stop_reason = str(exc)
            return self.stop_reason
        except sqlite3.Error:
            # A wedged or unwritable control DB cannot authorize further writes.
            self.stop_reason = "canary_control_db"
            return self.stop_reason
        finally:
            conn.close()
        # Opening and checkpointing the control DB can block; re-check the clock so a
        # stall never admits a frame past the deadline.
        now, mono = time.time(), time.monotonic()
        if now >= self.deadline or mono >= self.monotonic_deadline:
            return self._durable_stop("canary_expired", now)
        return None

    def consumed(self, nbytes: int) -> str | None:
        """Account bytes the sinks wrote for the admitted frame. Returns a stop reason
        when the grant was overshot (bounded by that one frame; recorded, never silent)."""
        self.accounted += int(nbytes)
        if self.accounted > self.reserved:
            self.overshoot = self.accounted - self.reserved
            self.stop_reason = self.stop_reason or "canary_overshoot"
            return self.stop_reason
        return None

    # -- close -----------------------------------------------------------------------

    def close(self, *, reason: str, replayable: bool | None, run_path: str | Path | None = None) -> dict:
        """Measure the run on disk, write the immutable receipt, then close the grant.
        A receipt that cannot be written closes the grant with `receipt_failed` so the
        final receipt reports it as unverified. Never deletes or truncates anything."""
        if self.grant_id is None:
            raise CanaryRefused("canary_not_claimed")
        if run_path is not None:
            self.run_path = Path(run_path)
        now = time.time()
        measured, files = measure_run(str(self.run_path) if self.run_path else None)
        overshoot = max(self.overshoot, measured - self.reserved, 0)
        receipt = {"version": VERSION, "canary_id": self.canary_id, "lane": self.lane, "kind": self.kind,
                   "grant_id": self.grant_id, "pid": self.pid, "process_identity": self.process_identity,
                   "image": self.image,
                   "hostname": socket.gethostname(), "run_path": str(self.run_path) if self.run_path else None,
                   "closed_utc": datetime.fromtimestamp(now, tz=UTC).isoformat(),
                   "deadline_utc": self.deadline_utc.isoformat(), "reason": reason, "replayable": replayable,
                   "reserved_bytes": self.reserved, "accounted_bytes": self.accounted,
                   "measured_bytes": measured, "overshoot_bytes": overshoot, "files": files,
                   "stop_reason": self.stop_reason, "shared_max_bytes": self.max_bytes}
        receipt_path = self.directory / "receipts" / f"{self.lane}__{self.grant_id}.json"
        receipt_error = None
        try:
            if receipt_path.is_file():
                # A previous close of this very grant wrote its receipt and then lost
                # the row update to lock contention: the receipt stands, the row closes.
                self.receipt_path = receipt_path
            else:
                payload = (json.dumps(receipt, indent=2, sort_keys=True) + "\n").encode("utf-8")
                with receipt_path.open("xb") as stream:
                    stream.write(payload)
                    stream.flush()
                    os.fsync(stream.fileno())
                self.receipt_path = receipt_path
        except Exception as exc:  # noqa: BLE001 - the DB row must still close
            receipt_error = type(exc).__name__

        def update():
            conn = _connect(self.directory)
            try:
                with conn:
                    conn.execute("UPDATE grants SET closed_utc=?, consumed=?, accounted=?, overshoot=?, reason=?, "
                                 "replayable=?, run_path=?, receipt=? WHERE id=? AND closed_utc IS NULL",
                                 (now, measured, self.accounted, overshoot,
                                  reason if receipt_error is None else f"{reason}:receipt_failed:{receipt_error}",
                                  None if replayable is None else int(bool(replayable)),
                                  str(self.run_path) if self.run_path else None,
                                  str(receipt_path) if receipt_error is None else None, self.grant_id))
            finally:
                conn.close()

        _retry_locked(update)
        receipt["receipt_error"] = receipt_error
        return receipt


# --------------------------------------------------------------------------- operator surfaces


def _snapshot(directory: Path) -> dict:
    conn = _connect(directory)
    try:
        canary = conn.execute("SELECT * FROM canary").fetchone()
        columns = [d[0] for d in conn.execute("SELECT * FROM canary").description]
        row = dict(zip(columns, canary, strict=True))
        lanes = [dict(zip(["lane", "job_type", "kind", "identity"], r, strict=True))
                 for r in conn.execute("SELECT lane, job_type, kind, identity FROM lanes ORDER BY lane")]
        grant_columns = [d[0] for d in conn.execute("SELECT * FROM grants").description]
        grants = [dict(zip(grant_columns, r, strict=True)) for r in conn.execute("SELECT * FROM grants ORDER BY id")]
        closed_consumed = _closed_consumed(conn)
        committed = _committed_bytes(conn)
    finally:
        conn.close()
    now = time.time()
    pool = int(row["max_bytes"]) - int(row["metadata_bytes"])
    deadline = None if row["first_utc"] is None else float(row["first_utc"]) + int(row["max_seconds"])
    for grant in grants:
        if grant["closed_utc"] is None:
            alive = owned_process_alive(int(grant["pid"]), str(grant["identity"]))
            grant["alive"] = alive  # True / False / None (unknown: cannot open the process)
    return {"canary": row, "lanes": lanes, "grants": grants, "pool_bytes": pool,
            "closed_consumed_bytes": closed_consumed, "committed_bytes": committed,
            "unspent_bytes": pool - closed_consumed, "deadline_utc": deadline,
            "remaining_seconds": None if deadline is None else max(0.0, deadline - now),
            "expired": deadline is not None and now >= deadline,
            "open_grants": [g["id"] for g in grants if g["closed_utc"] is None],
            "writers_alive": [g["id"] for g in grants if g["closed_utc"] is None and g.get("alive") is True],
            "writers_unknown": [g["id"] for g in grants if g["closed_utc"] is None and g.get("alive") is None]}


def _open_writers(directory: Path) -> list[int]:
    """Grant ids whose recorded process is alive or of unknown liveness (one cheap
    query plus one identity probe per open grant; used while a stop waits)."""
    conn = _connect(directory)
    try:
        rows = conn.execute("SELECT id, pid, identity FROM grants WHERE closed_utc IS NULL").fetchall()
    finally:
        conn.close()
    return [int(gid) for gid, pid, identity in rows if owned_process_alive(int(pid), str(identity)) is not False]


def status(directory: Path) -> dict:
    return _snapshot(directory)


def stop(directory: Path, *, reason: str = "operator_stop", apply: bool = False, wait_seconds: float = 120.0,
         terminate: bool = False, terminate_fn=terminate_owned, sleep=time.sleep, monotonic=time.monotonic) -> dict:
    """Dry run by default. `apply`: durable stop (no new grant or admission from the next
    checkpoint on), then a bounded wait for the recorded writers to close their grants.
    `terminate` (with apply): terminate only still-alive recorded writers by validated
    identity. Reports every outcome; never finalizes, never deletes."""
    before = _snapshot(directory)
    plan = {"apply": apply, "terminate": terminate, "reason": reason, "wait_seconds": wait_seconds,
            "open_grants_before": before["open_grants"], "writers_alive_before": before["writers_alive"],
            "writers_unknown_before": before["writers_unknown"], "already_stopped": before["canary"]["stopped"]}
    if not apply:
        plan["dry_run"] = True
        return plan
    if not ID.fullmatch(reason):
        raise ValueError("stop reason must be a bounded token")
    now = time.time()
    conn = _connect(directory)
    try:
        with conn:
            conn.execute("UPDATE canary SET stopped=COALESCE(stopped, ?), stopped_utc=COALESCE(stopped_utc, ?)",
                         (reason, now))
    finally:
        conn.close()
    deadline = monotonic() + max(0.0, float(wait_seconds))
    while _open_writers(directory) and monotonic() < deadline:
        sleep(0.5)
    snapshot = _snapshot(directory)
    terminations = []
    if terminate:
        for grant in snapshot["grants"]:
            if grant["closed_utc"] is None and grant.get("alive") is True:
                result = terminate_fn(int(grant["pid"]), str(grant["identity"]), grant.get("image"))
                terminations.append({"grant_id": grant["id"], "lane": grant["lane"], "pid": grant["pid"], **result})
        snapshot = _snapshot(directory)
    plan.update({"dry_run": False, "stopped": snapshot["canary"]["stopped"], "terminations": terminations,
                 "open_grants_after": snapshot["open_grants"], "writers_alive_after": snapshot["writers_alive"],
                 "writers_unknown_after": snapshot["writers_unknown"],
                 "verified_exited": not snapshot["writers_alive"] and not snapshot["writers_unknown"]})
    return plan


def finalize(directory: Path) -> dict:
    """Write the immutable final receipt once the canary is terminal, every recorded
    writer has provably exited and every grant is closed (abandoned grants are charged
    what their run holds). Refuses otherwise with the exact blocker."""
    now = time.time()
    conn = _connect(directory)
    try:
        with conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT id, first_utc, max_seconds, stopped, finalized_utc FROM canary").fetchone()
            if row is None:
                raise CanaryRefused("canary_corrupt")
            canary_id, first, max_seconds, stopped, finalized = row
            if finalized is not None:
                raise CanaryRefused("canary_already_finalized")
            if stopped is None:
                if first is not None and now >= float(first) + int(max_seconds):
                    conn.execute("UPDATE canary SET stopped='canary_expired', stopped_utc=?", (now,))
                else:
                    raise CanaryRefused("canary_not_terminal")
            blocking = _reconcile_open_grants(conn, now)
            if blocking:
                raise CanaryRefused("canary_writers_alive_or_unknown")
            conn.execute("UPDATE canary SET finalized_utc=?", (now,))
    finally:
        conn.close()
    snapshot = _snapshot(directory)
    receipts = sorted(p.name for p in (directory / "receipts").glob("*.json"))
    unverified = [g["id"] for g in snapshot["grants"] if g["receipt"] is None]
    final = {"version": VERSION, "canary": snapshot["canary"], "lanes": snapshot["lanes"],
             "grants": snapshot["grants"], "pool_bytes": snapshot["pool_bytes"],
             "consumed_bytes": snapshot["closed_consumed_bytes"],
             "replayable_runs": [g["run_path"] for g in snapshot["grants"] if g["replayable"] == 1],
             "unreplayable_or_unscored_runs": [g["run_path"] for g in snapshot["grants"] if g["replayable"] != 1],
             "grants_without_receipt": unverified, "receipts": receipts,
             "finalized_utc": datetime.fromtimestamp(now, tz=UTC).isoformat(),
             "note": "Receipts describe bytes as measured on disk at close; partial runs are preserved, "
                     "never reclaimed. Replayability is the lane scorer's verdict per run, not a canary claim."}
    payload = (json.dumps(final, indent=2, sort_keys=True, default=str) + "\n").encode("utf-8")
    with (directory / "receipt.json").open("xb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    return final


def _concurrency_default(script: Path) -> int | None:
    import re
    match = re.search(r"\[int\]\$CollectorConcurrency\s*=\s*(\d+)", script.read_text(encoding="utf-8"))
    return int(match.group(1)) if match else None


def preflight(directory: Path, *, config_path: Path, min_free_bytes: int = DEFAULT_MIN_FREE_GIB * GIB,
              scripts_root: Path | None = None, disk_usage_fn=disk_usage) -> dict:
    """Read-only. Samples free space now; it reserves nothing and proves nothing about
    later. Refuses enabled promoters for the canary's source directories (promotion
    stays gated during the canary) and reports the pool-cap arithmetic for activation."""
    from .ops import COLLECTOR_JOB_TYPES
    findings: list[str] = []
    snapshot = _snapshot(directory)
    jobs = load_jobs(config_path)
    by_name = {job["name"]: job for job in jobs}
    lanes = []
    sources: set[str] = set()
    roots: set[str] = set()
    for lane in snapshot["lanes"]:
        job = by_name.get(lane["lane"])
        if job is None:
            findings.append(f"lane_missing_in_config:{lane['lane']}")
            continue
        identity_ok = job_identity(job) == lane["identity"]
        if not identity_ok:
            findings.append(f"identity_mismatch:{lane['lane']}")
        try:
            _validate_lane_job(job, symbol=snapshot["canary"]["symbol"], directory=directory)
        except ValueError as exc:
            findings.append(f"lane_invalid:{exc}")
        args = job.get("args", {})
        if lane["kind"] == "stream":
            base = "bybit_perp_trades" if job["job_type"] == "bybit-trades-worker" else "bybit_perp_depth"
            sources.add(f"{base}_{str(args.get('source_suffix', '')).lower()}")
        else:
            sources.add("bybit_linear_universe")
        if args.get("output_root"):
            roots.add(str(args["output_root"]))
        lanes.append({"lane": lane["lane"], "kind": lane["kind"], "enabled": bool(job.get("enabled", True)),
                      "identity_ok": identity_ok})
    for job in jobs:
        if not job.get("enabled", True):
            continue
        args = job.get("args", {})
        listed = args.get("source")
        targets = [str(args.get("source_root", ""))] + ([str(s) for s in listed] if isinstance(listed, list) else [])
        targets = [t.replace("\\", "/").rstrip("/").lower() for t in targets]
        if job["job_type"] in {"promote-replayable", "backfill-trades-replay", "backfill-stream-depth"} and any(
                t.endswith("/" + s) or t == s for t in targets for s in sources):
            findings.append(f"promotion_or_scoring_enabled_for_canary_source:{job['name']}")
    unspent = snapshot["unspent_bytes"]
    required = int(min_free_bytes) + unspent + EXTERNAL_OVERHEAD_BYTES
    disks = []
    for root in sorted(roots):
        try:
            free = int(disk_usage_fn(root).free)
        except OSError as exc:
            findings.append(f"disk_unavailable:{root}:{type(exc).__name__}")
            continue
        disks.append({"root": root, "free_bytes": free, "required_bytes": required, "ok": free >= required})
        if free < required:
            findings.append(f"insufficient_headroom:{root}")
    enabled_lanes = sum(1 for job in jobs if job.get("enabled", True) and job["job_type"] in COLLECTOR_JOB_TYPES)
    pending = sum(1 for lane in lanes if not lane["enabled"])
    needed = enabled_lanes + pending + 2
    caps = {}
    if scripts_root is not None:
        for name in ("run_ops_runner.ps1", "redeploy_runner.ps1"):
            script = Path(scripts_root) / name
            caps[name] = _concurrency_default(script) if script.is_file() else None
            if caps[name] is None or caps[name] < needed:
                findings.append(f"pool_cap_below_needed:{name}")
    if snapshot["canary"]["stopped"] is not None:
        findings.append(f"canary_terminal:{snapshot['canary']['stopped']}")
    if snapshot["expired"]:
        findings.append("canary_expired")
    return {"ok": not findings, "findings": findings, "sampled_utc": datetime.now(tz=UTC).isoformat(),
            "lanes": lanes, "canary_sources": sorted(sources), "disks": disks,
            "headroom_model": {"min_free_bytes": int(min_free_bytes), "unspent_pool_bytes": unspent,
                               "external_overhead_bytes": EXTERNAL_OVERHEAD_BYTES, "required_free_bytes": required,
                               "in_flight_per_stream_grant_bytes": CHUNK_BYTES + SEGMENT_OVERHEAD_BYTES,
                               "frame_margin_bytes": FRAME_MARGIN_BYTES, "snapshot_reserve_bytes": SNAPSHOT_RESERVE_BYTES,
                               "metadata_bytes": METADATA_BYTES,
                               "note": "sampled free space, not a reservation; activation needs a fresh run"},
            "pool_cap": {"enabled_collector_lanes_in_config": enabled_lanes, "canary_lanes_pending": pending,
                         "needed": needed, "scripts": caps},
            "canary": {"id": snapshot["canary"]["id"], "symbol": snapshot["canary"]["symbol"],
                       "stopped": snapshot["canary"]["stopped"], "remaining_seconds": snapshot["remaining_seconds"]}}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Bounded alt canary controls (prepare/preflight/status/stop/finalize). "
                                                 "Never launches collection.")
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("prepare", help="create unused control state; refuses an existing directory")
    p.add_argument("directory", type=Path)
    p.add_argument("canary_id")
    p.add_argument("--config", type=Path, required=True)
    p.add_argument("--symbol", required=True)
    p.add_argument("--job", action="append", required=True, dest="jobs")
    p.add_argument("--max-gib", type=int, default=2)
    p.add_argument("--max-hours", type=int, default=24)
    for name in ("preflight", "status", "finalize"):
        q = sub.add_parser(name)
        q.add_argument("directory", type=Path)
        if name == "preflight":
            q.add_argument("--config", type=Path, required=True)
            q.add_argument("--min-free-gib", type=int, default=DEFAULT_MIN_FREE_GIB)
            q.add_argument("--scripts-root", type=Path, default=None)
    s = sub.add_parser("stop", help="dry run unless --apply; --terminate acts only on recorded writers")
    s.add_argument("directory", type=Path)
    s.add_argument("--reason", default="operator_stop")
    s.add_argument("--apply", action="store_true")
    s.add_argument("--terminate", action="store_true")
    s.add_argument("--wait-seconds", type=float, default=120.0)
    args = parser.parse_args(argv)
    if args.command == "prepare":
        result = prepare(args.directory, args.canary_id, config_path=args.config, symbol=args.symbol,
                         job_names=args.jobs, max_bytes=args.max_gib * GIB, max_seconds=args.max_hours * 3600)
    elif args.command == "preflight":
        result = preflight(args.directory, config_path=args.config, min_free_bytes=args.min_free_gib * GIB,
                           scripts_root=args.scripts_root)
    elif args.command == "status":
        result = status(args.directory)
    elif args.command == "stop":
        result = stop(args.directory, reason=args.reason, apply=args.apply, terminate=args.terminate,
                      wait_seconds=args.wait_seconds)
    else:
        result = finalize(args.directory)
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0 if result.get("ok", True) is not False else 1


if __name__ == "__main__":
    sys.exit(main())
