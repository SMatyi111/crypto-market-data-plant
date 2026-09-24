"""Disabled-by-default public Bybit references for the existing BTC depth lane."""
from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from .models import utc_now
from .session_evidence import TICKER, digest

BODY_LIMIT = 1024**2
HTTP_TIMEOUT = 10
MAX_CAPTURE_SECONDS = 1860
FINALIZE_TIMEOUT = 3 * HTTP_TIMEOUT + 3
_ACTIVE = set()
_ACTIVE_LOCK = threading.Lock()
PATHS = {"rules_before": "/v5/market/instruments-info",
         "rules_after": "/v5/market/instruments-info",
         "funding_history": "/v5/market/funding/history"}


def request_url(stage: str, start_ms=None, end_ms=None) -> str:
    params = {"category": "linear", "symbol": "BTCUSDT"}
    if stage == "funding_history":
        if (type(start_ms) is not int or type(end_ms) is not int
                or start_ms < 0 or end_ms > 253402300799999
                or not start_ms <= end_ms <= start_ms + 1860_000):
            raise ValueError("Invalid reference interval")
        params.update(startTime=start_ms, endTime=end_ms, limit=200)
    return "https://api.bybit.com" + PATHS[stage] + "?" + urllib.parse.urlencode(params)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ValueError("Redirect refused")


def http_child(stage: str, start_ms=None, end_ms=None) -> None:
    """Only fixed public GETs; no secrets, proxy inheritance, retries or redirects."""
    url = request_url(stage, start_ms, end_ms)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    with opener.open(url, timeout=8) as response:
        body = response.read(BODY_LIMIT + 1)
        if len(body) > BODY_LIMIT:
            raise ValueError("HTTP body limit")
        print(json.dumps({"status": response.status,
                          "body_base64": base64.b64encode(body).decode(),
                          "body_sha256": digest(body)}))


class PublicHTTP:
    """Own the exact helper handle, including cleanup on terminal drain expiry."""

    def __init__(self):
        self.lock = threading.Lock()
        self.process = None
        self.cancelled = False

    def cancel(self):
        with self.lock:
            self.cancelled = True
            if self.process is not None and self.process.poll() is None:
                self.process.kill()

    def __call__(self, stage: str, start_ms=None, end_ms=None) -> dict:
        request_url(stage, start_ms, end_ms)
        args = [sys.executable, "-m", "crypto_collector.bybit_references", stage]
        if stage == "funding_history":
            args.extend([str(start_ms), str(end_ms)])
        with self.lock:
            if self.cancelled:
                raise RuntimeError("Reference helper cancelled")
            process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                       creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
            self.process = process
        try:
            try:
                stdout, _ = process.communicate(timeout=HTTP_TIMEOUT)
            except BaseException:
                if process.poll() is None:
                    process.kill()
                process.communicate(timeout=2)
                raise
            if process.returncode != 0:
                raise ValueError("Reference helper failed")
            if len(stdout) > 2 * BODY_LIMIT:
                raise ValueError("Helper output limit")
            return json.loads(stdout)
        finally:
            with self.lock:
                self.process = None


def public_get(stage: str, start_ms=None, end_ms=None) -> dict:
    return PublicHTTP()(stage, start_ms, end_ms)


def drain_reference_finalizers() -> None:
    """Only terminal worker exit, never between segments or on the market loop."""
    with _ACTIVE_LOCK:
        active = list(_ACTIVE)
    deadline = time.monotonic() + FINALIZE_TIMEOUT
    for controller in active:
        if not controller.closed.is_set():
            controller.evidence._fail("unclosed_reference_at_worker_exit")
            controller.close("worker_exit", False)
        controller.thread.join(max(0, deadline - time.monotonic()))
        if controller.thread.is_alive():
            controller.evidence._fail("reference_finalize_timeout")
            controller.client.cancel()



class BybitReferences:
    """One background lifecycle, <=3 HTTP attempts; never writes market rows.

    Start overlaps book collection. At segment close the caller signals and returns;
    post references and journal finalization run here. An unclosed capture expires.
    """

    def __init__(self, evidence, *, fetch=None):
        self.evidence = evidence
        self.client = PublicHTTP()
        self.fetch = fetch or self.client
        self.closed = threading.Event()
        self.thread = None
        self.records = []
        self.reason, self.sinks_closed = "reference_timeout", False

    def start(self) -> None:
        if self.thread is not None or self.evidence.error:
            return
        self.started_at = utc_now()
        self.start_monotonic = time.monotonic()
        self.thread = threading.Thread(target=self._run, name="bybit-reference-http", daemon=True)
        try:
            with _ACTIVE_LOCK:
                _ACTIVE.add(self)
            self.thread.start()
        except Exception as exc:
            with _ACTIVE_LOCK:
                _ACTIVE.discard(self)
            self.evidence._fail(type(exc).__name__)

    def close(self, reason: str, sinks_closed: bool) -> None:
        self.ended_at = utc_now()
        self.reason, self.sinks_closed = reason, sinks_closed
        self.closed.set()
        if self.thread is None or self.thread.ident is None:
            self.evidence.finish(reason=reason, sinks_closed=sinks_closed)

    def _fetch(self, stage: str, start_ms=None, end_ms=None) -> None:
        started, mono = utc_now(), time.monotonic_ns()
        row = {"stage": stage, "started_at": started.isoformat(),
               "start_monotonic_ns": mono, "url": request_url(stage, start_ms, end_ms)}
        try:
            if self.evidence.error:
                raise RuntimeError("Reference evidence already unavailable")
            response = self.fetch(stage, start_ms, end_ms)
            body = base64.b64decode(response["body_base64"], validate=True)
            if len(body) > BODY_LIMIT or digest(body) != response["body_sha256"]:
                raise ValueError("HTTP byte/hash mismatch")
            row.update(status=response["status"], body_base64=response["body_base64"],
                       body_sha256=response["body_sha256"])
        except Exception as exc:
            row["error"] = type(exc).__name__  # no arbitrary exception/URL/secret text
        row.update(received_at=utc_now().isoformat(), end_monotonic_ns=time.monotonic_ns())
        self.records.append(row)

    def _run(self) -> None:
        try:
            self._fetch("rules_before")
            if not self.closed.wait(max(0, MAX_CAPTURE_SECONDS - (time.monotonic() - self.start_monotonic))):
                self.evidence._fail("reference_capture_timeout")
                return
            if self.reason in {"limit", "deadline"} and self.sinks_closed:
                self._fetch("rules_after")
                self._fetch("funding_history", int(self.started_at.timestamp() * 1000),
                            int(self.ended_at.timestamp() * 1000))
            for row in self.records:
                # The market producer is closed now: one sequencer, no cross-thread
                # clock reordering. HTTP availability lives in received_at, not seq.
                self.evidence.event("http_reference", **row)
        except Exception as exc:
            self.evidence._fail(type(exc).__name__)
        finally:
            try:
                self.evidence.finish(reason=self.reason, sinks_closed=self.sinks_closed)
            finally:
                with _ACTIVE_LOCK:
                    _ACTIVE.discard(self)


def positive(value) -> Decimal:
    d = Decimal(str(value))
    if not d.is_finite() or d <= 0:
        raise ValueError("Invalid positive reference field")
    return d


def rules_projection(body: dict) -> dict:
    result = body["result"]
    if body.get("retCode") != 0 or result["category"] != "linear" or len(result["list"]) != 1:
        raise ValueError("Invalid instrument response")
    item = result["list"][0]
    if (item["symbol"] != "BTCUSDT" or item["status"] != "Trading"
            or item["contractType"] != "LinearPerpetual" or item["settleCoin"] != "USDT"
            or item["quoteCoin"] != "USDT" or result.get("nextPageCursor")):
        raise ValueError("Wrong contract")
    fields = {"tickSize": item["priceFilter"]["tickSize"],
              **{k: item["lotSizeFilter"][k] for k in
                 ["minOrderQty", "minNotionalValue", "qtyStep", "maxMktOrderQty"]},
              "fundingInterval": item["fundingInterval"]}
    for value in fields.values():
        positive(value)
    interval = positive(fields["fundingInterval"])
    if interval > 10080 or interval != interval.to_integral_value():
        raise ValueError("Non-integer funding interval")
    if positive(fields["minOrderQty"]) > positive(fields["maxMktOrderQty"]):
        raise ValueError("Inverted quantity limits")
    return fields


def ticker_step(state, payload):
    if (payload.get("topic") != TICKER or payload.get("type") not in {"snapshot", "delta"}
            or not isinstance(payload.get("data"), dict) or payload["data"].get("symbol") != "BTCUSDT"):
        raise ValueError("Invalid ticker identity")
    if payload["type"] == "snapshot":
        state = {}
    elif state is None:
        raise ValueError("Ticker delta without snapshot")
    state = {**state, **payload["data"]}
    for key in ["markPrice", "indexPrice", "nextFundingTime"]:
        positive(state[key])
    funding_time(state["nextFundingTime"])
    if not Decimal(str(state["fundingRate"])).is_finite():
        raise ValueError("Invalid funding rate")
    return state


def funding_time(value) -> int:
    text = str(value)
    if not text.isascii() or not text.isdecimal() or len(text) > 15:
        raise ValueError("Invalid funding timestamp")
    result = int(text)
    if not 0 < result <= 253402300799999:
        raise ValueError("Funding timestamp out of range")
    return result


def timestamp(value: str) -> float:
    dt = datetime.fromisoformat(value)
    if dt.utcoffset() is None:
        raise ValueError("Naive reference time")
    return dt.timestamp()


def verify_bybit_references(root: Path) -> dict:
    """Validate references only after session binding; never emit economic admission."""
    from .session_evidence import verify_session_evidence
    manifest = verify_session_evidence(root)
    if not manifest.get("reference_mode"):
        raise ValueError("No reference capture in this run")
    rows = [json.loads(line) for line in (root / "session_evidence/events.jsonl").read_text(encoding="utf-8").splitlines()]
    http = [r for r in rows if r["kind"] == "http_reference"]
    if [r["stage"] for r in http] != ["rules_before", "rules_after", "funding_history"]:
        raise ValueError("Missing/duplicate reference attempts")
    bodies = []
    last_http_end = None
    for row in http:
        if "error" in row or row.get("status") != 200:
            raise ValueError("Failed HTTP reference")
        if any(type(row[k]) is not int or row[k] < 0 for k in ["start_monotonic_ns", "end_monotonic_ns"]):
            raise ValueError("Invalid HTTP monotonic clock")
        for utc_key, mono_key in [("started_at", "start_monotonic_ns"), ("received_at", "end_monotonic_ns")]:
            dt = timestamp(row["utc"]) - timestamp(row[utc_key])
            dm = (row["monotonic_ns"] - row[mono_key]) / 1e9
            if dm < 0 or abs(dt - dm) > 0.5:
                raise ValueError("HTTP clock not bound to journal")
        if last_http_end is not None and row["start_monotonic_ns"] < last_http_end:
            raise ValueError("Overlapping HTTP attempts")
        last_http_end = row["end_monotonic_ns"]
        duration = (row["end_monotonic_ns"] - row["start_monotonic_ns"]) / 1e9
        wall = (datetime.fromisoformat(row["received_at"]) - datetime.fromisoformat(row["started_at"])).total_seconds()
        if not 0 <= duration <= 10.5 or abs(wall - duration) > 0.5:
            raise ValueError("HTTP reference timing")
        body = base64.b64decode(row["body_base64"], validate=True)
        if len(body) > BODY_LIMIT or digest(body) != row["body_sha256"]:
            raise ValueError("HTTP byte/hash mismatch")
        bodies.append(json.loads(body))
    rules = rules_projection(bodies[0])
    if rules != rules_projection(bodies[1]):
        raise ValueError("Rules changed inside reference interval")
    for row in http[:2]:
        if row["url"] != request_url(row["stage"]):
            raise ValueError("Wrong rules endpoint")
    bounds = urllib.parse.parse_qs(urllib.parse.urlsplit(http[2]["url"]).query)
    start_ms, end_ms = int(bounds["startTime"][0]), int(bounds["endTime"][0])
    if http[2]["url"] != request_url("funding_history", start_ms, end_ms):
        raise ValueError("Wrong funding endpoint")
    state, first_ticker, scheduled = None, None, set()
    ticker_times = []
    for row in rows:
        if row["kind"] == "ticker":
            state = ticker_step(state, row["payload"])
            at = datetime.fromisoformat(row["receipt"]["utc"]).timestamp()
            first_ticker = at if first_ticker is None else first_ticker
            scheduled.add(funding_time(state["nextFundingTime"]))
            if funding_time(state["nextFundingTime"]) < (at - 5) * 1000:
                raise ValueError("Stale funding schedule")
            ticker_times.append(at)
    written = [r for r in rows if r["kind"] == "raw_written"]
    if state is None or not written:
        raise ValueError("Missing ticker or book anchor")
    begin = max(datetime.fromisoformat(http[0]["received_at"]).timestamp(), first_ticker,
                datetime.fromisoformat(written[0]["receipt"]["utc"]).timestamp())
    end = datetime.fromisoformat(written[-1]["receipt"]["utc"]).timestamp()
    if not start_ms / 1000 <= begin < end <= end_ms / 1000 + 0.001:
        raise ValueError("Reference interval does not cover eligible book interval")
    if any(b - a > 5 for a, b in zip(ticker_times, ticker_times[1:], strict=False)) or end - ticker_times[-1] > 5:
        raise ValueError("Ticker coverage gap")
    if datetime.fromisoformat(http[1]["started_at"]).timestamp() < end:
        raise ValueError("Post rules precede book closure")
    result = bodies[2]["result"]
    if bodies[2].get("retCode") != 0 or result["category"] != "linear" or len(result["list"]) >= 200:
        raise ValueError("Incomplete funding page")
    actual = set()
    for item in result["list"]:
        at = funding_time(item["fundingRateTimestamp"])
        if (item["symbol"] != "BTCUSDT" or at in actual or not start_ms <= at <= end_ms
                or not Decimal(str(item["fundingRate"])).is_finite()):
            raise ValueError("Invalid funding settlement")
        actual.add(at)
    expected = {t for t in scheduled if begin * 1000 <= t <= end * 1000}
    if expected != {t for t in actual if begin * 1000 <= t <= end * 1000}:
        raise ValueError("Missing or unexpected funding settlements")
    # Boundary tolerance and only future schedules for a zero-funding interval.
    no_settlement = not actual and all(t > (end + 5) * 1000 for t in scheduled)
    return {"reference_integrity": True, "rules": rules, "eligible_start": begin,
            "eligible_end": end, "settled_rate_count": len(actual),
            "no_settlement_supported": no_settlement,
            "funding_cashflow_complete": no_settlement, "economic_admission": False,
            "limitations": ["Unchanged rule snapshots support an interval assumption only.",
                            "Crossed settlements lack an exact settlement mark.",
                            "Account fees, fills and strategy gates remain separate."]}


if __name__ == "__main__":
    # A killed parent cannot orphan an indefinitely trickling HTTP child. This
    # watchdog belongs to this short-lived helper only, never a market worker.
    watchdog = threading.Timer(HTTP_TIMEOUT, os._exit, args=(124,))
    watchdog.daemon = True
    watchdog.start()
    stage = sys.argv[1]
    if stage == "funding_history" and len(sys.argv) == 4:
        http_child(stage, int(sys.argv[2]), int(sys.argv[3]))
    elif stage in {"rules_before", "rules_after"} and len(sys.argv) == 2:
        http_child(stage)
    else:
        raise SystemExit("Invalid fixed public request")
