"""Opt-in Bybit connection evidence. Never a venue completeness or fill guarantee."""
from __future__ import annotations

import hashlib
import json
import logging
import os
import queue
import threading
import time
from datetime import datetime
from pathlib import Path
from shutil import disk_usage
from uuid import uuid4

from .models import utc_now
from .evidence_budget import (DEFAULT_MAX_BYTES, MAX_BYTES, DEFAULT_MIN_FREE_BYTES,
                              DISK_CHECK_BYTES, GIB, required_free_bytes)

logger = logging.getLogger(__name__)
ENDPOINT = "wss://stream.bybit.com/v5/public/linear"
TOPIC = "orderbook.50.BTCUSDT"
VERSION = 1
TICKER = "tickers.BTCUSDT"
_WRITER_SLOT = threading.BoundedSemaphore(1)


def canonical(row: dict) -> bytes:
    return json.dumps(row, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def raw_files(root: Path) -> list[Path]:
    parts = sorted((root / "raw").glob("messages.[0-9]*.jsonl"),
                   key=lambda p: int(p.name.split(".")[1]))
    active = root / "raw/messages.jsonl"
    return parts + ([active] if active.exists() else [])


def file_info(path: Path, root: Path) -> dict:
    sha = hashlib.sha256()
    size = rows = 0
    with path.open("rb") as stream:
        for line in stream:
            sha.update(line)
            size += len(line)
            rows += 1
            if not line.endswith(b"\n"):
                raise ValueError("Torn evidence/raw line")
    return {"path": path.relative_to(root).as_posix(), "bytes": size,
            "rows": rows, "sha256": sha.hexdigest()}


class SessionEvidence:
    """One daemon writer, bounded byte queue; capture never waits on journal I/O.

    Terminal publication waits at most two seconds for that writer. An unavailable
    sink or timeout refuses evidence instead of interrupting legacy market writes.
    """

    def __init__(self, root: Path, *, max_bytes=DEFAULT_MAX_BYTES, queue_bytes=1024**2,
                 reference_mode=False, min_free_bytes=DEFAULT_MIN_FREE_BYTES,
                 lease_directory=None, trial_id=None):
        if type(max_bytes) is not int or not 1 <= max_bytes <= MAX_BYTES:
            raise ValueError("Invalid journal byte budget")
        if type(min_free_bytes) is not int or not DEFAULT_MIN_FREE_BYTES <= min_free_bytes <= 4096 * GIB:
            raise ValueError("Invalid journal free-space floor")
        self.min_free_bytes = min_free_bytes
        if bool(lease_directory) != bool(trial_id):
            raise ValueError("Evidence lease directory and trial ID are required together")
        self.lease = None
        if lease_directory:
            from .evidence_lease import EvidenceLease
            if max_bytes != MAX_BYTES:
                raise ValueError("Leased evidence requires the fixed 512 MiB reservation")
            self.lease = EvidenceLease(Path(lease_directory), trial_id, root.name)
        self.ready = threading.Event()
        self._disk_checks = 0
        self._initial_free = self._last_free = None
        self.reference_mode = reference_mode
        self.root = root
        self.directory = root / "session_evidence"
        self.max_bytes, self.queue_bytes = max_bytes, queue_bytes
        self.process_session = uuid4().hex
        self.connection = 0
        self.sequence = 0
        self.raw_count = 0
        self.book_received = 0
        self.ack = False
        self.anchor = False
        self.issues: set[str] = set()
        self.error: str | None = None
        self._last_clock = None
        self._queued = self._total = 0
        self._lock = threading.Lock()
        self._queue: queue.Queue = queue.Queue()
        self._thread = None
        self._finished = False
        try:
            if not self.lease:
                self.directory.mkdir(exist_ok=False)
            if not _WRITER_SLOT.acquire(blocking=False):
                self._fail("previous_writer_pending")
                return
            self._thread = threading.Thread(target=self._write, daemon=True,
                                            name="bybit-session-evidence")
            try:
                self._thread.start()
            except Exception:
                _WRITER_SLOT.release()
                raise
        except Exception as exc:
            self._fail(type(exc).__name__)

    def _fail(self, reason: str) -> None:
        if self.error is None:
            self.error = reason
            logger.warning("session evidence unavailable: %s", reason)

    def issue(self, reason: str) -> None:
        self.issues.add(reason)

    def event(self, kind: str, *, clock=None, **fields) -> int:
        if self.error or self._finished:
            return 0
        if self.lease and self.ready.is_set() and not self.capture_available():
            return 0
        try:
            utc, mono = clock or (utc_now(), time.monotonic_ns())
            current = (utc.timestamp(), mono)
            if self._last_clock:
                dt = current[0] - self._last_clock[0]
                dm = (current[1] - self._last_clock[1]) / 1e9
                if dm < 0 or abs(dt - dm) > 0.5:
                    self.issue("clock_discontinuity")
            self._last_clock = current
            self.sequence += 1
            row = {"seq": self.sequence, "kind": kind, "connection": self.connection,
                   "utc": utc.isoformat(), "monotonic_ns": mono, **fields}
            line = canonical(row) + b"\n"
            with self._lock:
                if self._total + len(line) > self.max_bytes:
                    self._fail("byte_cap")
                    return 0
                if self._queued + len(line) > self.queue_bytes:
                    self._fail("queue_overflow")
                    return 0
                self._queued += len(line)
                self._total += len(line)
            self._queue.put_nowait(line)
            return self.sequence
        except Exception as exc:
            self._fail(type(exc).__name__)
            return 0

    def _check_headroom(self, written_bytes: int) -> bool:
        # Writer thread only: a stalled OS disk query must never block the book
        # producer. The bounded queue and writer slot still fail closed.
        try:
            free = disk_usage(self.root).free
            if type(free) is not int or free < 0:
                raise ValueError("Invalid free-space result")
            self._disk_checks += 1
            self._last_free = free
            if self._initial_free is None:
                self._initial_free = free
            if free < required_free_bytes(self.max_bytes, written_bytes, self.min_free_bytes):
                self._fail("insufficient_disk_headroom")
                return False
            return True
        except Exception:
            self._fail("disk_headroom_unavailable")
            return False

    def _write(self) -> None:
        try:
            if self.lease:
                from .evidence_lease import TOTAL_BYTES
                self.lease.claim()
                # Full initial allocation, not just this segment. Sample only.
                if disk_usage(self.root).free < self.min_free_bytes + TOTAL_BYTES:
                    self._fail("insufficient_trial_headroom")
                    return
                self.lease.check(checkpoint=True)
                if self.error:
                    return
                self.directory.mkdir(exist_ok=False)
            self.ready.set()
            if self.error:
                return
            if not self._check_headroom(0):
                return
            with (self.directory / "events.jsonl").open("xb") as stream:
                count, synced = 0, time.monotonic()
                written_bytes = checked_bytes = 0
                while True:
                    if self.lease:
                        self.lease.check()
                    try:
                        line = self._queue.get(timeout=0.25)
                    except queue.Empty:
                        continue
                    if line is None:
                        break
                    if self.error:
                        return
                    if written_bytes - checked_bytes + len(line) >= DISK_CHECK_BYTES:
                        if not self._check_headroom(written_bytes):
                            return
                        checked_bytes = written_bytes
                    if self.lease:
                        self.lease.check()
                    stream.write(line)
                    written_bytes += len(line)
                    stream.flush()
                    count += 1
                    if count % 64 == 0 or time.monotonic() - synced >= 0.2:
                        os.fsync(stream.fileno())
                        synced = time.monotonic()
                    with self._lock:
                        self._queued -= len(line)
                stream.flush()
                os.fsync(stream.fileno())
            if self._check_headroom(written_bytes):
                self._publish()
        except Exception as exc:
            self._fail(type(exc).__name__)
        finally:
            self.ready.set()
            if self.lease:
                self.lease.close()
            _WRITER_SLOT.release()

    def capture_available(self) -> bool:
        """No I/O on the market/reference producer. Durable checks use writer."""
        if self.error:
            return False
        if self.lease and self.ready.is_set() and self.lease.remaining() <= 0:
            self._fail("lease_expired")
            return False
        return True

    def opened(self, endpoint: str) -> None:
        self.connection += 1
        self.ack = self.anchor = False
        if endpoint != ENDPOINT:
            self.issue("wrong_endpoint")
        self.event("connection_open", endpoint=endpoint, process_session=self.process_session,
                   run_id=self.root.name)

    def boundary(self, reason: str) -> None:
        self.anchor = self.ack = False
        self.issue(reason)
        self.event("connection_boundary", reason=reason)

    def received(self, message, clock) -> dict:
        wire = message.encode() if isinstance(message, str) else bytes(message)
        seq = self.event("receive", clock=clock, wire_sha256=digest(wire), wire_bytes=len(wire))
        return {"receive_seq": seq, "connection": self.connection,
                "utc": clock[0].isoformat(), "monotonic_ns": clock[1]}

    def decoded(self, payload, receipt: dict) -> None:
        if not isinstance(payload, dict):
            self.issue("non_object_frame")
            return
        self.event("decoded", receive_seq=receipt["receive_seq"],
                   payload_sha256=digest(canonical(payload)), topic=payload.get("topic"),
                   book_type=payload.get("type"),
                   symbol=payload.get("data", {}).get("symbol" if payload.get("topic") == TICKER else "s")
                   if isinstance(payload.get("data"), dict) else None)
        if payload.get("op") == "subscribe":
            self.ack = (payload.get("success") is True and
                        (not self.reference_mode or payload.get("req_id") == self.process_session))
            self.event("ack", receive_seq=receipt["receive_seq"], payload=payload)
            if not self.ack:
                self.issue("rejected_ack")
        if self.reference_mode and payload.get("topic") == TICKER:
            self.event("ticker", receipt=receipt.copy(), payload=payload)
            return
        if "topic" in payload:
            self.book_received += 1
            if (payload.get("topic") != TOPIC or not isinstance(payload.get("data"), dict)
                    or payload["data"].get("s") != "BTCUSDT"):
                self.issue("unexpected_topic_or_symbol")
                self.anchor = False
            elif payload.get("type") == "snapshot":
                self.anchor = True
                self.event("snapshot", receive_seq=receipt["receive_seq"])
            elif payload.get("type") != "delta":
                self.issue("unknown_book_type")
                self.anchor = False
        receipt["anchored"] = self.anchor

    def written(self, raw, ordinal: int) -> None:
        receipt = raw._capture
        if receipt is None:
            self.issue("missing_receipt")
            return
        eligible = (self.ack and receipt.get("anchored", False)
                    and receipt["connection"] == self.connection)
        if not eligible:
            self.issue("unanchored_or_unacknowledged_row")
        self.raw_count += 1
        self.event("raw_written", ordinal=ordinal, receipt=receipt,
                   envelope_sha256=digest(canonical(raw.to_dict())), eligible=eligible)

    def finish(self, *, reason: str, sinks_closed: bool) -> None:
        if self._finished:
            return
        self.event("terminal", reason=reason, sinks_closed=sinks_closed)
        self._finished = True
        self._reason, self._sinks_closed = reason, sinks_closed
        self._publish_deadline = time.monotonic() + 2
        if self._thread is not None and self._thread.ident is not None:
            self._queue.put_nowait(None)
            self._thread.join(timeout=2)
            if self._thread.is_alive():
                self._fail("writer_close_timeout")

    def _publish(self) -> None:
        # Runs only on the daemon writer, after journal close and all market sinks
        # close. A process-wide slot prevents stuck writers accumulating per run.
        if self.error:
            return
        if self.lease:
            self.lease.check(checkpoint=True)
        files = [file_info(p, self.root) for p in raw_files(self.root)]
        journal = file_info(self.directory / "events.jsonl", self.root)
        if self.book_received != self.raw_count:
            self.issue("unpersisted_book_frames")
        if sum(f["rows"] for f in files) != self.raw_count:
            self.issue("raw_count_mismatch")
        complete = self._sinks_closed and self._reason in {"limit", "deadline"}
        manifest = {"version": 2 if self.reference_mode else VERSION,
                    "reference_mode": self.reference_mode, "process_session": self.process_session,
                    "run_id": self.root.name, "reason": self._reason,
                    "capture_complete": complete, "issues": sorted(self.issues),
                    "session_admitted": complete and not self.issues and self.raw_count > 0,
                    "economic_admission": False, "raw_count": self.raw_count,
                    "journal": journal, "raw_files": files,
                    "resource_budget": {"max_bytes": self.max_bytes, "queue_bytes": self.queue_bytes,
                                        "min_free_bytes": self.min_free_bytes,
                                        "disk_check_bytes": DISK_CHECK_BYTES,
                                        "disk_check_count": self._disk_checks,
                                        "initial_free_bytes": self._initial_free,
                                        "last_free_bytes": self._last_free}}
        # Check again AFTER potentially blocking file I/O/fsync. Never rename a
        # timed-out preparation into a terminal manifest. A leftover tmp is refused.
        if self.lease:
            manifest["evidence_lease"] = self.lease.receipt()
        payload = (json.dumps(manifest, indent=2) + "\n").encode("utf-8")
        if self.lease:
            from .evidence_lease import MANIFEST_BYTES
            if len(payload) > MANIFEST_BYTES:
                self._fail("metadata_cap")
                return
            self.lease.check(checkpoint=True)
        tmp = self.directory / "manifest.tmp"
        with tmp.open("xb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        if self.error or time.monotonic() > self._publish_deadline:
            self._fail("writer_close_timeout")
            return
        if self.lease:
            self.lease.check(checkpoint=True)
        tmp.replace(self.directory / "manifest.json")


def verify_session_evidence(root: Path) -> dict:
    """Verify internal integrity, not authenticity against wholesale replacement.

    Wire hashes identify received bytes; only decoded-envelope hashes can be
    independently reconciled with legacy raw, which is not a wire-exact archive.
    """
    manifest = json.loads((root / "session_evidence/manifest.json").read_text())
    references = manifest.get("reference_mode", False)
    if (manifest["version"] != (2 if references else VERSION) or not manifest["session_admitted"]
            or not manifest["capture_complete"] or manifest["economic_admission"]
            or manifest["issues"] or manifest["run_id"] != root.name
            or type(manifest["raw_count"]) is not int or manifest["raw_count"] <= 0):
        raise ValueError("Session evidence not admitted")
    journal_path = root / "session_evidence/events.jsonl"
    if file_info(journal_path, root) != manifest["journal"]:
        raise ValueError("Journal hash/count mismatch")
    budget = manifest.get("resource_budget")
    if budget is not None:
        if (type(budget.get("max_bytes")) is not int or not 1 <= budget["max_bytes"] <= MAX_BYTES
                or type(budget.get("min_free_bytes")) is not int
                or not DEFAULT_MIN_FREE_BYTES <= budget["min_free_bytes"] <= 4096 * GIB
                or manifest["journal"]["bytes"] > budget["max_bytes"]):
            raise ValueError("Invalid evidence resource budget")
    lease = manifest.get("evidence_lease")
    if lease is not None:
        from .evidence_lease import ID, MANIFEST_BYTES, METADATA_BYTES, RESERVATION_BYTES, TOTAL_BYTES
        import math
        if (lease.get("version") != 1 or type(lease.get("slot")) is not int or lease["slot"] not in (1, 2)
                or not isinstance(lease.get("trial_id"), str) or not ID.fullmatch(lease["trial_id"])
                or not isinstance(lease.get("deadline_utc"), (int, float))
                or not math.isfinite(lease["deadline_utc"]) or lease["deadline_utc"] <= 0
                or lease.get("reservation_bytes") != RESERVATION_BYTES
                or lease.get("total_bytes") != TOTAL_BYTES or lease.get("metadata_bytes") != METADATA_BYTES
                or budget is None or budget["max_bytes"] != MAX_BYTES
                or (root / "session_evidence/manifest.json").stat().st_size > MANIFEST_BYTES):
            raise ValueError("Invalid evidence lease receipt")
    files = raw_files(root)
    if [file_info(p, root) for p in files] != manifest["raw_files"]:
        raise ValueError("Raw file set/hash/count mismatch")
    bindings, receipts, decoded = [], {}, {}
    acknowledged = anchored = subscribed = False
    connection = 0
    terminal = previous = None
    used = set()
    tickers = set()
    with journal_path.open() as stream:
        for seq, line in enumerate(stream, 1):
            row = json.loads(line)
            if row["seq"] != seq:
                raise ValueError("Journal sequence mismatch")
            utc = datetime.fromisoformat(row["utc"])
            if utc.utcoffset() is None:
                raise ValueError("Naive receipt clock")
            if lease is not None and utc.timestamp() >= lease["deadline_utc"]:
                raise ValueError("Journal extends past evidence lease")
            if type(row["monotonic_ns"]) is not int or row["monotonic_ns"] < 0:
                raise ValueError("Invalid monotonic clock")
            current = utc.timestamp(), row["monotonic_ns"]
            if previous:
                dt, dm = current[0] - previous[0], (current[1] - previous[1]) / 1e9
                if dm < 0 or abs(dt - dm) > 0.5:
                    raise ValueError("Clock discontinuity")
            previous = current
            kind = row["kind"]
            if terminal is not None:
                raise ValueError("Records after terminal")
            if kind == "connection_open":
                if connection or row["connection"] != 1 or row["endpoint"] != ENDPOINT:
                    raise ValueError("Wrong or multiple connection")
                if (row["process_session"] != manifest["process_session"]
                        or row["run_id"] != root.name):
                    raise ValueError("Session identity mismatch")
                connection = 1
            elif not connection or row["connection"] != connection:
                raise ValueError("Missing connection")
            elif kind == "subscribe_sent":
                expected_subscription = {"op": "subscribe", "args": [TOPIC]}
                if references:
                    expected_subscription.update(args=[TOPIC, TICKER], req_id=manifest["process_session"])
                if row["payload"] != expected_subscription or subscribed:
                    raise ValueError("Wrong subscription")
                subscribed = True
            elif kind in {"connection_boundary", "decode_error"}:
                raise ValueError("Discontinuous session")
            elif kind == "receive":
                receipts[seq] = row
            elif kind == "decoded":
                rid = row["receive_seq"]
                if rid not in receipts or rid in decoded:
                    raise ValueError("Unbound decoded payload")
                if row["topic"] is not None:
                    if (row["topic"] not in ({TOPIC, TICKER} if references else {TOPIC}) or row["symbol"] != "BTCUSDT"
                            or row["book_type"] not in {"snapshot", "delta"}):
                        raise ValueError("Wrong book identity/type")
                    if row["book_type"] == "snapshot" and row["topic"] == TOPIC:
                        anchored = True
                decoded[rid] = dict(row, anchored=anchored)
            elif kind == "ack":
                item = decoded.get(row["receive_seq"])
                if (not subscribed or item is None or row["payload"].get("op") != "subscribe"
                        or row["payload"].get("success") is not True
                        or (references and row["payload"].get("req_id") != manifest["process_session"])
                        or item["payload_sha256"] != digest(canonical(row["payload"]))):
                    raise ValueError("Unbound acknowledgement")
                acknowledged = True
            elif kind == "snapshot":
                item = decoded.get(row["receive_seq"])
                if item is None or item["book_type"] != "snapshot" or item["topic"] != TOPIC:
                    raise ValueError("Unbound snapshot")
            elif kind == "raw_written":
                receipt = row["receipt"]
                rid = receipt["receive_seq"]
                original, item = receipts.get(rid), decoded.get(rid)
                if (not row["eligible"] or not acknowledged or item is None
                        or not item["anchored"] or rid in used or item["topic"] != TOPIC
                        or receipt["connection"] != connection or original is None
                        or original["utc"] != receipt["utc"]
                        or original["monotonic_ns"] != receipt["monotonic_ns"]):
                    raise ValueError("Unanchored or unbound row")
                used.add(rid)
                bindings.append(row)
            elif kind == "ticker" and references:
                receipt = row["receipt"]
                rid = receipt["receive_seq"]
                original, item = receipts.get(rid), decoded.get(rid)
                if (original is None or item is None or rid in tickers
                        or item["topic"] != TICKER or receipt["connection"] != connection
                        or original["utc"] != receipt["utc"]
                        or original["monotonic_ns"] != receipt["monotonic_ns"]
                        or item["payload_sha256"] != digest(canonical(row["payload"]))):
                    raise ValueError("Unbound ticker")
                tickers.add(rid)
            elif kind == "http_reference" and references:
                pass  # Separate reference validator checks HTTP semantics and timing.
            elif kind == "terminal":
                terminal = row
            elif kind != "connection_end":
                raise ValueError("Unknown session record")
    if {rid for rid, item in decoded.items() if item["topic"] == TICKER} != tickers:
        raise ValueError("Unpersisted ticker")
    if {rid for rid, item in decoded.items() if item["topic"] == TOPIC} != used:
        raise ValueError("Unpersisted book frames or snapshot anchor")
    if (terminal is None or not terminal["sinks_closed"]
            or terminal["reason"] not in {"limit", "deadline"}
            or terminal["reason"] != manifest["reason"]
            or len(bindings) != manifest["raw_count"]):
        raise ValueError("Terminal/count mismatch")
    ordinal = 0
    for path in files:
        with path.open() as stream:
            for line in stream:
                ordinal += 1
                if ordinal > len(bindings):
                    raise ValueError("Unbound raw row")
                binding = bindings[ordinal - 1]
                raw = json.loads(line)
                item = decoded[binding["receipt"]["receive_seq"]]
                payload = raw["payload"]
                if (binding["ordinal"] != ordinal
                        or binding["envelope_sha256"] != digest(canonical(raw))
                        or raw["received_at"] != binding["receipt"]["utc"]
                        or raw["source"] != "bybit" or payload["topic"] != TOPIC
                        or payload["data"]["s"] != "BTCUSDT"
                        or payload["type"] != item["book_type"]
                        or item["payload_sha256"] != digest(canonical(payload))):
                    raise ValueError("Raw envelope binding mismatch")
    if ordinal != len(bindings):
        raise ValueError("Missing raw row")
    return manifest
