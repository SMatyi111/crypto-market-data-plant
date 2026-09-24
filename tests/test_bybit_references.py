from __future__ import annotations

import asyncio
import base64
import json
import subprocess
import threading
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from crypto_collector import bybit_references as refs
from crypto_collector import session_evidence as journal
from crypto_collector.models import RawMessage
from test_session_evidence import ACK, book, setup


@pytest.fixture(autouse=True)
def no_real_http_children(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("Tests must inject a synthetic HTTP child")
    monkeypatch.setattr(subprocess, "Popen", forbidden)


def instrument():
    return {"retCode": 0, "result": {"category": "linear", "list": [{
        "symbol": "BTCUSDT", "status": "Trading", "contractType": "LinearPerpetual",
        "settleCoin": "USDT", "quoteCoin": "USDT", "fundingInterval": 480,
        "priceFilter": {"tickSize": "0.1"}, "lotSizeFilter": {
            "minOrderQty": "0.001", "qtyStep": "0.001", "minNotionalValue": "5",
            "maxMktOrderQty": "100"}}]}}


def response(body):
    raw = json.dumps(body).encode()
    return {"status": 200, "body_base64": base64.b64encode(raw).decode(),
            "body_sha256": journal.digest(raw)}


def ticker(at, kind="snapshot", **fields):
    data = {"symbol": "BTCUSDT", "markPrice": "60000", "indexPrice": "60001",
            "fundingRate": "0.0001", "nextFundingTime": str(at)} if kind == "snapshot" else {"symbol": "BTCUSDT"}
    return {"topic": refs.TICKER, "type": kind, "data": {**data, **fields}}


@pytest.fixture
def capture(tmp_path, monkeypatch):
    """Synthetic clocks, HTTP bytes and two raw rows; never read market history."""
    origin = datetime(2026, 9, 24, tzinfo=timezone.utc)
    clock = [0.0]
    def utc():
        return origin + timedelta(seconds=clock[0])
    def mono():
        return 1_000_000_000 + int(clock[0] * 1e9)
    monkeypatch.setattr(journal, "utc_now", utc)
    monkeypatch.setattr(journal.time, "monotonic_ns", mono)

    def build(*, crossed=False, modify=None, first_kind="snapshot", gap=False):
        root = tmp_path / "run"
        (root / "raw").mkdir(parents=True)
        evidence = journal.SessionEvidence(root, reference_mode=True)
        clock[0] = .2
        evidence.opened(journal.ENDPOINT)
        evidence.event("subscribe_sent", payload={"op": "subscribe", "args": [journal.TOPIC, refs.TICKER],
                                                  "req_id": evidence.process_session})
        def decoded(payload, at):
            clock[0] = at
            receipt = evidence.received(json.dumps(payload), (utc(), mono()))
            evidence.decoded(payload, receipt)
            return receipt
        decoded({**ACK, "req_id": evidence.process_session}, .3)
        base = int(origin.timestamp() * 1000)
        decoded(ticker(base + (1000 if crossed else 10000), kind=first_kind), .4)
        raw_rows = []
        for ordinal, at in [(1, .5), (2, 8 if gap else 2)]:
            if ordinal == 2:
                decoded(ticker(base + 10000, "delta", nextFundingTime=str(base + 10000)), 1.5)
            payload = book("snapshot" if ordinal == 1 else "delta", ordinal)
            receipt = decoded(payload, at)
            raw = RawMessage(source="bybit", received_at=utc(), payload=payload, _capture=receipt)
            raw_rows.append(json.dumps(raw.to_dict()))
            evidence.written(raw, ordinal)
        (root / "raw/messages.jsonl").write_text("\n".join(raw_rows) + "\n", encoding="utf-8")
        clock[0] = 8.1 if gap else 2.1
        evidence.event("connection_end")
        history = {"retCode": 0, "result": {"category": "linear", "list": []}}
        if crossed:
            history["result"]["list"].append({"symbol": "BTCUSDT", "fundingRateTimestamp": str(base + 1000),
                                               "fundingRate": "0.0001"})
        records = []
        offset = 6 if gap else 0
        for stage, start, end, body in [("rules_before", 0, .1, instrument()),
                                       ("rules_after", 2.2 + offset, 2.3 + offset, instrument()),
                                       ("funding_history", 2.4 + offset, 2.5 + offset, history)]:
            records.append({"stage": stage, "started_at": (origin + timedelta(seconds=start)).isoformat(),
                            "received_at": (origin + timedelta(seconds=end)).isoformat(),
                            "start_monotonic_ns": 1_000_000_000 + int(start * 1e9),
                            "end_monotonic_ns": 1_000_000_000 + int(end * 1e9),
                            "url": refs.request_url(stage, base, base + 2100 + offset * 1000), **response(body)})
        if modify:
            modify(records)
        clock[0] = 2.6 + offset
        for record in records:
            evidence.event("http_reference", **record)
        evidence.finish(reason="limit", sinks_closed=True)
        return root
    return build


def test_complete_reference_interval_is_not_economic_admission(capture):
    result = refs.verify_bybit_references(capture())
    assert result["no_settlement_supported"] and result["funding_cashflow_complete"]
    assert result["settled_rate_count"] == 0
    assert not result["economic_admission"]


def test_crossed_funding_requires_exact_mark_not_nearby_ticker(capture):
    result = refs.verify_bybit_references(capture(crossed=True))
    assert result["settled_rate_count"] == 1
    assert not result["no_settlement_supported"] and not result["funding_cashflow_complete"]


@pytest.mark.parametrize("case", ["changed_rules", "missing", "timeout", "hash", "full_page", "duplicate", "missing_settlement",
                                 "wrong_endpoint", "future_http", "clock_jump", "naive_time", "wrong_symbol"])
def test_refuse_incomplete_or_unbound_references(capture, case):
    def modify(rows):
        if case == "missing":
            rows.pop()
        elif case == "timeout":
            rows[0]["error"] = "TimeoutExpired"
        elif case == "hash":
            rows[0]["body_sha256"] = "bad"
        elif case == "wrong_endpoint":
            rows[0]["url"] = "https://example.com"
        elif case == "future_http":
            rows[0]["end_monotonic_ns"] += 100_000_000_000
        elif case == "clock_jump":
            rows[0]["received_at"] = "2026-09-24T00:00:01+00:00"
        elif case == "naive_time":
            rows[0]["started_at"] = "2026-09-24T00:00:00"
        else:
            row = rows[1] if case == "changed_rules" else rows[2]
            body = json.loads(base64.b64decode(row["body_base64"]))
            if case == "changed_rules":
                body["result"]["list"][0]["priceFilter"]["tickSize"] = "1"
            elif case == "full_page":
                body["result"]["list"] *= 200
            elif case == "duplicate":
                body["result"]["list"] *= 2
            elif case == "missing_settlement":
                body["result"]["list"] = []
            elif case == "wrong_symbol":
                body["result"]["list"][0]["symbol"] = "ETHUSDT"
            row.update(response(body))
    with pytest.raises(ValueError):
        refs.verify_bybit_references(capture(crossed=True, modify=modify))


@pytest.mark.parametrize("kwargs", [{"first_kind": "delta"}, {"gap": True}])
def test_ticker_anchor_and_coverage_required(capture, kwargs):
    with pytest.raises(ValueError):
        refs.verify_bybit_references(capture(**kwargs))


def test_ticker_omitted_fields_carry_and_bad_time_refused():
    state = refs.ticker_step(None, ticker(100000))
    assert refs.ticker_step(state, ticker(0, "delta", markPrice="60002"))["nextFundingTime"] == "100000"
    with pytest.raises(ValueError):
        refs.ticker_step(state, ticker(0, "delta", nextFundingTime="1.5"))
    with pytest.raises(ValueError):
        refs.ticker_step(None, ticker(0, "delta"))


def test_fixed_http_helper_deadline_and_argv(monkeypatch):
    calls = []
    class Process:
        returncode = 0
        def communicate(self, timeout):
            assert timeout == 10
            return json.dumps(response({})).encode(), b""
    def popen(args, **kwargs):
        calls.append((args, kwargs))
        return Process()
    monkeypatch.setattr(subprocess, "Popen", popen)
    refs.public_get("funding_history", 0, 1000)
    args, kwargs = calls[0]
    assert args[1:] == ["-m", "crypto_collector.bybit_references", "funding_history", "0", "1000"]
    assert kwargs["stdout"] == subprocess.PIPE and "shell" not in kwargs
    for values in [(0, 1860001), (-1, 1), (1.1, 2), (2, 1)]:
        with pytest.raises(ValueError):
            refs.public_get("funding_history", *values)
    assert len(calls) == 1


def test_http_child_body_cap_and_no_redirects(monkeypatch):
    class Response:
        status = 200
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def read(self, n):
            assert n == refs.BODY_LIMIT + 1
            return b"x" * n
    monkeypatch.setattr(refs.urllib.request, "build_opener", lambda *a: SimpleNamespace(open=lambda *a, **kw: Response()))
    with pytest.raises(ValueError, match="body limit"):
        refs.http_child("rules_before")
    with pytest.raises(ValueError, match="Redirect"):
        refs.NoRedirect().redirect_request(None, None, 302, None, None, "https://example.com")


def reference_setup(tmp_path, monkeypatch, frames):
    paths, pipeline, evidence, sockets = setup(tmp_path, monkeypatch, [frames], reference_mode=True)
    sockets[0].frames.insert(0, {**ACK, "req_id": evidence.process_session})
    return paths, pipeline, evidence, sockets


def test_ticker_multiplex_never_enters_raw(tmp_path, monkeypatch):
    paths, pipeline, evidence, sockets = reference_setup(tmp_path, monkeypatch,
        [ticker(1900000000000), book(), ticker(0, "delta", markPrice="60002"), book("delta", 2)])
    result = asyncio.run(pipeline.run(limit=2))
    manifest = journal.verify_session_evidence(paths.base)
    assert result.raw_messages == manifest["raw_count"] == 2
    events = [json.loads(line) for line in (paths.base / "session_evidence/events.jsonl").read_text().splitlines()]
    assert len([r for r in events if r["kind"] == "ticker"]) == 2
    assert pipeline.collector._subscription_message() == {"op": "subscribe", "args": [journal.TOPIC, refs.TICKER],
                                                          "req_id": evidence.process_session}
    assert not pipeline.collector._is_subscription_ack(ACK)
    assert sockets[0].closed


def test_ticker_traffic_does_not_mask_depth_idle(tmp_path, monkeypatch):
    paths, pipeline, evidence, sockets = reference_setup(tmp_path, monkeypatch, [])
    pipeline.collector.config.idle_timeout_seconds = .03
    async def stream_ticker():
        await asyncio.sleep(.004)
        return json.dumps(ticker(1900000000000))
    monkeypatch.setattr(sockets[0], "__anext__", stream_ticker)  # collector calls explicit __anext__
    asyncio.run(pipeline.run(limit=1))
    assert pipeline.collector.idle_timeout_count == 1
    assert evidence.raw_count == 0
    with pytest.raises(ValueError):
        journal.verify_session_evidence(paths.base)


def test_http_start_and_post_do_not_block_books_or_shutdown(tmp_path, monkeypatch):
    paths, pipeline, evidence, sockets = reference_setup(tmp_path, monkeypatch, [book()])
    release = threading.Event()
    calls = []
    def fetch(stage, *args):
        calls.append(stage)
        assert release.wait(2)
        return response(instrument() if stage != "funding_history" else
                        {"retCode": 0, "result": {"category": "linear", "list": []}})
    controller = refs.BybitReferences(evidence, fetch=fetch)
    pipeline.collector.reference_evidence = controller
    try:
        started = time.monotonic()
        assert asyncio.run(pipeline.run(limit=1)).raw_messages == 1
        assert time.monotonic() - started < 1
        assert sockets[0].closed and controller.thread.is_alive()
    finally:
        release.set()
        controller.thread.join(3)
    assert calls == ["rules_before", "rules_after", "funding_history"]
    assert journal.verify_session_evidence(paths.base)["reference_mode"]
    with pytest.raises(ValueError):  # no ticker / initial rules weren't available to the single book
        refs.verify_bybit_references(paths.base)


def test_http_timeout_is_evidence_failure_not_market_failure(tmp_path, monkeypatch):
    paths, pipeline, evidence, _ = reference_setup(tmp_path, monkeypatch, [book()])
    def fetch(*args):
        raise subprocess.TimeoutExpired("fixed-helper", 10)
    controller = refs.BybitReferences(evidence, fetch=fetch)
    pipeline.collector.reference_evidence = controller
    assert asyncio.run(pipeline.run(limit=1)).raw_messages == 1
    controller.thread.join(3)
    assert journal.verify_session_evidence(paths.base)["reference_mode"]
    with pytest.raises(ValueError, match="Failed HTTP"):
        refs.verify_bybit_references(paths.base)


@pytest.mark.parametrize("enabled", [True, False])
def test_dispatch_carries_reference_flag_without_network(tmp_path, monkeypatch, enabled):
    import crypto_collector.cli as cli
    from crypto_collector.ops import JobSpec
    captured = []
    async def segment(args):
        captured.append((args.session_evidence, args.reference_evidence))
        return {"run_path": str(tmp_path / "run"), "clean_events": 0, "replayable": True}
    monkeypatch.setattr(cli, "collect_bybit_depth_segment", segment)
    job = JobSpec(name="test", job_type="bybit-depth-worker", interval_seconds=60,
                  args={"session_evidence": enabled, "reference_evidence": enabled,
                        "market": "linear", "max_segments": 1,
                        "output_root": str(tmp_path / "data"), "ops_root": str(tmp_path / "ops")})
    cli._execute_ops_job_inprocess(job)
    assert captured == [(enabled, enabled)]


@pytest.mark.parametrize("session,market,symbol", [(False, "linear", "BTCUSDT"),
                                                  (True, "spot", "BTCUSDT"), (True, "linear", "ETHUSDT")])
def test_reference_scope_rejected_before_side_effects(tmp_path, session, market, symbol):
    from crypto_collector.cli import collect_bybit_depth_segment
    args = SimpleNamespace(market=market, session_evidence=session, reference_evidence=True,
                           symbol=symbol, channel="orderbook.50", output_root=tmp_path)
    with pytest.raises(ValueError):
        asyncio.run(collect_bybit_depth_segment(args))
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("mutation", ["drop_ticker", "ticker_hash", "receipt", "req_id"])
def test_refuse_self_consistent_journal_with_broken_ticker_binding(capture, mutation):
    root = capture()
    path = root / "session_evidence/events.jsonl"
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    item = next(r for r in rows if r["kind"] == "ticker")
    if mutation == "drop_ticker":
        rows.remove(item)
    elif mutation == "ticker_hash":
        item["payload"]["data"]["markPrice"] = "1"
    elif mutation == "receipt":
        item["receipt"]["receive_seq"] = 999
    else:
        ack = next(r for r in rows if r["kind"] == "ack")
        ack["payload"]["req_id"] = "wrong"
        next(r for r in rows if r["kind"] == "decoded" and r["receive_seq"] == ack["receive_seq"])["payload_sha256"] = journal.digest(journal.canonical(ack["payload"]))
    for seq, row in enumerate(rows, 1):
        row["seq"] = seq
    path.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
    manifest_path = root / "session_evidence/manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["journal"] = journal.file_info(path, root)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError):
        refs.verify_bybit_references(root)


def test_reference_lifetime_expires_without_stopping_market(tmp_path, monkeypatch):
    root = tmp_path / "run"
    root.mkdir()
    evidence = journal.SessionEvidence(root, reference_mode=True)
    monkeypatch.setattr(refs, "MAX_CAPTURE_SECONDS", .02)
    controller = refs.BybitReferences(evidence, fetch=lambda *a: response(instrument()))
    controller.start()
    controller.thread.join(1)
    assert not controller.thread.is_alive()
    assert evidence.error == "reference_capture_timeout"
    assert not (root / "session_evidence/manifest.json").exists()
    controller.close("limit", True)  # delayed producer closure remains harmless


@pytest.mark.parametrize("value", ["1e1000000000", "99999999999999999999999999", "1.5", "NaN", "Infinity", "-1"])
def test_untrusted_numeric_magnitudes_rejected_without_integer_expansion(value):
    with pytest.raises((ValueError, ArithmeticError)):
        refs.ticker_step(None, ticker(value))
    body = instrument()
    body["result"]["list"][0]["fundingInterval"] = value
    with pytest.raises((ValueError, ArithmeticError)):
        refs.rules_projection(body)


def test_helper_timeout_kills_only_its_owned_process(monkeypatch):
    calls = []
    class Process:
        returncode = None
        def communicate(self, timeout):
            calls.append(("communicate", timeout))
            if self.returncode is None:
                raise subprocess.TimeoutExpired("owned", timeout)
            return b"", b""
        def poll(self):
            return self.returncode
        def kill(self):
            calls.append("kill")
            self.returncode = -1
    monkeypatch.setattr(subprocess, "Popen", lambda *a, **kw: Process())
    with pytest.raises(subprocess.TimeoutExpired):
        refs.public_get("rules_before")
    assert calls == [("communicate", 10), "kill", ("communicate", 2)]


def test_terminal_worker_exit_drains_pending_references(tmp_path, monkeypatch):
    import crypto_collector.cli as cli
    from crypto_collector.ops import JobSpec
    controllers = []
    async def segment(args):
        root = tmp_path / "evidence-run"
        root.mkdir()
        evidence = journal.SessionEvidence(root, reference_mode=True)
        def fetch(*args):
            time.sleep(.01)
            return response(instrument())
        controller = refs.BybitReferences(evidence, fetch=fetch)
        controller.start()
        controller.close("limit", True)
        monkeypatch.setattr(controller.client, "cancel", lambda: pytest.fail("unexpected helper cancel"))
        controllers.append(controller)
        return {"run_path": str(root), "clean_events": 0, "replayable": True}
    monkeypatch.setattr(cli, "collect_bybit_depth_segment", segment)
    job = JobSpec(name="test", job_type="bybit-depth-worker", interval_seconds=60,
                  args={"session_evidence": True, "reference_evidence": True,
                        "market": "linear", "max_segments": 1,
                        "output_root": str(tmp_path / "data"), "ops_root": str(tmp_path / "ops")})
    cli._execute_ops_job_inprocess(job)
    assert not controllers[0].thread.is_alive()
    assert not refs._ACTIVE


def test_drain_expiry_disables_evidence_and_cancels_owned_client(tmp_path, monkeypatch):
    root = tmp_path / "run"
    root.mkdir()
    evidence = journal.SessionEvidence(root, reference_mode=True)
    release = threading.Event()
    def fetch(*args):
        release.wait(1)
        return response(instrument())
    controller = refs.BybitReferences(evidence, fetch=fetch)
    controller.start()
    controller.close("limit", True)
    cancelled = []
    monkeypatch.setattr(controller.client, "cancel", lambda: cancelled.append(True))
    monkeypatch.setattr(refs, "FINALIZE_TIMEOUT", .01)
    try:
        refs.drain_reference_finalizers()
        assert cancelled == [True]
        assert evidence.error == "reference_finalize_timeout"
    finally:
        release.set()
        controller.thread.join(2)
    assert not (root / "session_evidence/manifest.json").exists()
