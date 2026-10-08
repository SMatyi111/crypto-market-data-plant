"""Explicitly prepared, nonrenewable optional-evidence allowance. No capture CLI."""
from __future__ import annotations

import argparse
import math
import os
import re
import sqlite3
import time
from pathlib import Path

from .evidence_budget import MAX_BYTES, MIB

SLOTS = 2
SECONDS = 3600
METADATA_BYTES = 8 * MIB
# Shared DB + rollback journal + lock fit within this allowance in EACH slot.
# DB max 256 KiB, rollback journal < 512 KiB; no WAL/temp tables/attached DBs.
CONTROL_BYTES = MIB
MANIFEST_BYTES = METADATA_BYTES - CONTROL_BYTES
RESERVATION_BYTES = MAX_BYTES + METADATA_BYTES
TOTAL_BYTES = SLOTS * RESERVATION_BYTES
ID = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}\Z")


class LeaseRefused(ValueError):
    pass


def _connect(directory: Path):
    db = directory / "lease.sqlite3"
    if not db.is_file() or db.stat().st_size > 256 * 1024:
        raise LeaseRefused("lease_missing_or_oversize")
    conn = sqlite3.connect(db.resolve().as_uri() + "?mode=rw", uri=True, timeout=0)
    try:
        if conn.execute("PRAGMA journal_mode").fetchone()[0] != "delete":
            raise LeaseRefused("lease_journal_mode")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA max_page_count=64")
        if conn.execute("PRAGMA page_size").fetchone()[0] != 4096:
            raise LeaseRefused("lease_page_size")
        return conn
    except BaseException:
        conn.close()
        raise


def prepare(directory: Path, trial_id: str) -> None:
    """One-time operator action. Existing/partial directories are NEVER repaired."""
    if not ID.fullmatch(trial_id):
        raise ValueError("Invalid trial ID")
    directory.mkdir(exist_ok=False)
    with (directory / "writer.lock").open("xb") as stream:
        stream.write(b"0")
        stream.flush()
        os.fsync(stream.fileno())
    with (directory / "lease.sqlite3").open("xb"):
        pass
    conn = _connect(directory)
    try:
        with conn:
            conn.execute("CREATE TABLE trial (id TEXT PRIMARY KEY, version INTEGER, "
                         "first_utc REAL, last_utc REAL, reserved INTEGER, "
                         "first_mono REAL, last_mono REAL, stopped TEXT)")
            conn.execute("CREATE TABLE claims (slot INTEGER PRIMARY KEY, run_id TEXT UNIQUE, "
                         "started_utc REAL, reserved INTEGER)")
            conn.execute("INSERT INTO trial VALUES (?, 1, NULL, NULL, 0, NULL, NULL, NULL)", (trial_id,))
    finally:
        conn.close()


def _writer_lock(directory):
    # Lock an EXISTING byte, never create or unlink it on capture/restart.
    stream = (directory / "writer.lock").open("r+b")
    try:
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        return stream
    except BaseException:
        stream.close()
        raise


def _stop(conn, reason):
    conn.execute("UPDATE trial SET stopped=?", (reason,))
    conn.commit()  # Durable refusal must survive the exception/outer rollback.
    raise LeaseRefused(reason)


class EvidenceLease:
    """Claim on the existing journal thread; lock held through terminal publication.

    Every committed slot is spent even if writer acquisition/capture fails.
    Uncommitted claims cannot start capture. No resets, refunds or renewals.
    """

    def __init__(self, directory: Path, trial_id: str, run_id: str):
        if not ID.fullmatch(trial_id) or not ID.fullmatch(run_id):
            raise LeaseRefused("lease_identity")
        self.directory, self.trial_id, self.run_id = directory, trial_id, run_id
        self.writer = None
        self.deadline = self.monotonic_deadline = None
        self.last_clock = None
        self.last_checkpoint = 0.0
        self.slot = None

    def claim(self):
        now, mono = time.time(), time.monotonic()
        if not math.isfinite(now) or now <= 0:
            raise LeaseRefused("lease_clock")
        conn = _connect(self.directory)
        try:
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                rows = conn.execute("SELECT * FROM trial").fetchall()
                if len(rows) != 1 or rows[0][:2] != (self.trial_id, 1):
                    raise LeaseRefused("lease_identity")
                _, _, first, last, reserved, first_mono, last_mono, stopped = rows[0]
                if stopped is not None:
                    raise LeaseRefused("lease_stopped")
                claims = conn.execute("SELECT slot,run_id,started_utc,reserved FROM claims ORDER BY slot").fetchall()
                if (len(claims) > SLOTS or reserved != len(claims) * RESERVATION_BYTES
                        or any(r[0] != i or r[3] != RESERVATION_BYTES
                               or not isinstance(r[1], str) or not ID.fullmatch(r[1])
                               for i, r in enumerate(claims, 1))):
                    raise LeaseRefused("lease_corrupt")
                if claims:
                    if (not isinstance(first, (int, float)) or not math.isfinite(first)
                            or not isinstance(last, (int, float)) or not math.isfinite(last)
                            or first <= 0 or last < first or now < last
                            or claims[0][2] != first
                            or any(not isinstance(r[2], (int, float)) or not first <= r[2] <= last for r in claims)
                            or not isinstance(first_mono, (int, float)) or not math.isfinite(first_mono)
                            or not isinstance(last_mono, (int, float)) or not math.isfinite(last_mono)
                            or not 0 <= first_mono <= last_mono <= mono
                            or abs((now - last) - (mono - last_mono)) > 0.5):
                        _stop(conn, "lease_clock_or_corrupt")
                    if now >= first + SECONDS or mono >= first_mono + SECONDS:
                        _stop(conn, "lease_expired")
                elif any(v is not None for v in (first, last, first_mono, last_mono)):
                    raise LeaseRefused("lease_corrupt")
                if len(claims) >= SLOTS:
                    raise LeaseRefused("lease_spent")
                if any(r[1] == self.run_id for r in claims):
                    raise LeaseRefused("lease_run_already_spent")
                first = first if first is not None else now
                first_mono = first_mono if first_mono is not None else mono
                self.slot = len(claims) + 1
                conn.execute("INSERT INTO claims VALUES (?, ?, ?, ?)",
                             (self.slot, self.run_id, now, RESERVATION_BYTES))
                conn.execute("UPDATE trial SET first_utc=?, last_utc=?, reserved=?, first_mono=?, last_mono=?",
                             (first, now, self.slot * RESERVATION_BYTES, first_mono, mono))
            # Only after durable commit can the writer obtain a usable grant.
            self.deadline = first + SECONDS
            self.monotonic_deadline = first_mono + SECONDS
            self.last_clock = (now, mono)
            self.last_checkpoint = mono
            self.writer = _writer_lock(self.directory)
        finally:
            conn.close()

    def remaining(self):
        if self.deadline is None:
            return 0.0
        return max(0.0, min(self.deadline - time.time(), self.monotonic_deadline - time.monotonic()))

    def _check_clock(self, now, mono):
        old_wall, old_mono = self.last_clock
        reason = None
        if (not math.isfinite(now) or now < old_wall or mono < old_mono
                or abs((now - old_wall) - (mono - old_mono)) > 0.5):
            reason = "lease_clock"
        elif now >= self.deadline or mono >= self.monotonic_deadline:
            reason = "lease_expired"
        if reason:
            conn = _connect(self.directory)
            try:
                _stop(conn, reason)
            finally:
                conn.close()

    def _observe_clock(self):
        # Writer thread only. Producer consults remaining() without I/O.
        if self.writer is None:
            raise LeaseRefused("lease_not_claimed")
        now, mono = time.time(), time.monotonic()
        self._check_clock(now, mono)
        self.last_clock = now, mono
        return now, mono

    def verify_clock(self, *, control=False):
        """Expiry/reversal check without a durable checkpoint (no fsync).

        Terminal publication calls this after blocking file I/O so a stall cannot
        authorize a manifest past the deadline. `control=True` additionally reads
        the trial row (read-only, no commit) and refuses a trial that another
        process has durably stopped meanwhile, which the former checkpoint did.
        """
        self._observe_clock()
        if control:
            conn = _connect(self.directory)
            try:
                row = conn.execute("SELECT stopped FROM trial WHERE id=?", (self.trial_id,)).fetchone()
            finally:
                conn.close()
            if row is None:
                raise LeaseRefused("lease_clock_or_missing")
            if row[0] is not None:
                raise LeaseRefused("lease_stopped")

    def check(self, *, checkpoint=False):
        now, mono = self._observe_clock()
        if checkpoint or mono - self.last_checkpoint >= 1:
            conn = _connect(self.directory)
            try:
                with conn:
                    conn.execute("BEGIN IMMEDIATE")
                    row = conn.execute("SELECT last_utc, stopped FROM trial WHERE id=?", (self.trial_id,)).fetchone()
                    if row is None or now < row[0] or row[1] is not None:
                        raise LeaseRefused("lease_clock_or_missing")
                    conn.execute("UPDATE trial SET last_utc=?, last_mono=? WHERE id=?", (now, mono, self.trial_id))
                self.last_checkpoint = mono
            finally:
                conn.close()
            # Opening/checkpointing/closing control state can block past expiry.
            # That must not authorize a NEW payload write or manifest rename.
            now, mono = time.time(), time.monotonic()
            self._check_clock(now, mono)
            self.last_clock = now, mono

    def close(self):
        if self.writer is not None:
            self.writer.close()  # OS releases byte lock, including on process crash.
            self.writer = None

    def receipt(self):
        return {"version": 1, "trial_id": self.trial_id, "slot": self.slot,
                "deadline_utc": self.deadline, "reservation_bytes": RESERVATION_BYTES,
                "total_bytes": TOTAL_BYTES, "metadata_bytes": METADATA_BYTES}


def main():
    parser = argparse.ArgumentParser(description="Prepare an unused evidence lease; does not activate capture")
    parser.add_argument("directory", type=Path)
    parser.add_argument("trial_id")
    args = parser.parse_args()
    prepare(args.directory, args.trial_id)


if __name__ == "__main__":
    main()
