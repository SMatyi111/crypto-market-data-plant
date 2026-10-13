"""Bybit linear universe snapshot lane (STANDARDS 4.17): hermetic fixtures only.

Covers bounded pagination (completeness, bounded maximum, repeated cursor, duplicate
symbols), strict retCode, spot-cursor refusal, panel membership semantics (present /
absent only under complete coverage / unknown otherwise), fetch and truncated-read
failures still writing a summary, the wall budget, request/byte/time bounds, and the ops
job registration + arg threading. No socket is opened: every fetch is a dict lookup.
"""

from __future__ import annotations

import gzip
import http.client
import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

import crypto_collector.cli as cli
import crypto_collector.collectors.bybit_universe as universe
from crypto_collector.cli import _job_args
from crypto_collector.collectors.bybit_universe import snapshot_bybit_linear_universe
from crypto_collector.ops import COLLECTOR_JOB_TYPES, POLL_LANE_JOB_TYPES


NOW = datetime(2026, 10, 14, 0, 5, 0, tzinfo=UTC)
PANEL = ("XLMUSDT", "GRAMUSDT")


def page(rows, category, cursor=None, ret_code=0):
    body = {"retCode": ret_code, "result": {"category": category, "list": rows}}
    if cursor is not None:
        body["result"]["nextPageCursor"] = cursor
    return json.dumps(body).encode("utf-8")


def lin(symbol, delivery="0", status="Trading", interval=240):
    return {"symbol": symbol, "status": status, "deliveryTime": delivery, "fundingInterval": interval,
            "lastPrice": "SENTINEL_PRICE"}


def payloads(*, pages=2, spot_cursor=None, panel_present=True, ret_code=0):
    """Two-page linear universe by default; the panel symbols live on page 2."""
    second = [lin("GRAMUSDT", delivery="1800000000000"), lin("XLMUSDT")] if panel_present else [lin("OTHERUSDT")]
    urls = {}
    linear = [[lin("AAAUSDT"), lin("BBBUSDT")], second, [lin("CCCUSDT")]][:pages]
    cursor = None
    for index, rows in enumerate(linear, start=1):
        url = universe.LINEAR_URL if cursor is None else universe._cursor_url(cursor)
        next_cursor = f"c{index}" if index < pages else None
        urls[url] = page(rows, "linear", cursor=next_cursor, ret_code=ret_code)
        cursor = next_cursor
    urls[universe.SPOT_URL] = page([{"symbol": "XLMUSDT", "status": "Trading"}], "spot", cursor=spot_cursor)
    ticker_rows = [{"symbol": "XLMUSDT", "turnover24h": "1"}] + ([{"symbol": "GRAMUSDT", "turnover24h": "1"}] if panel_present else [])
    urls[universe.TICKERS_URL] = page(ticker_rows, "linear")
    return urls


def run_dir(tmp_path: Path) -> Path:
    return tmp_path / universe.SOURCE_NAME / "20261014_000500"


def summary(tmp_path: Path) -> dict:
    return json.loads((run_dir(tmp_path) / "metrics" / "summary.json").read_text(encoding="utf-8"))


def test_complete_two_page_snapshot_archives_verbatim_and_records_membership(tmp_path: Path) -> None:
    urls = payloads()
    calls: list[str] = []

    def fetch(url: str) -> bytes:
        calls.append(url)
        return urls[url]

    result = snapshot_bybit_linear_universe(tmp_path, panel_symbols=PANEL, fetch=fetch, clock=lambda: NOW)
    assert result.payload_count == 4
    assert calls == [universe.LINEAR_URL, universe._cursor_url("c1"), universe.SPOT_URL, universe.TICKERS_URL]
    assert len(calls) == 4  # four requests, three payload families, no second fetch by the skeleton
    with gzip.open(run_dir(tmp_path) / "raw" / "instruments_linear_p2.json.gz", "rb") as handle:
        assert handle.read() == urls[universe._cursor_url("c1")]
    data = summary(tmp_path)
    assert data["parse_ok"] is True
    assert data["request_count"] == 4
    assert data["payload_families"] == 3
    assert data["linear_pages_fetched"] == 2
    assert data["linear_pagination_complete"] is True
    assert data["panel_membership_coverage"] == "complete"
    assert data["panel_membership"]["GRAMUSDT"] == {
        "present_in_instruments": True, "status": "Trading", "delivery_time": "1800000000000",
        "funding_interval": 240, "present_in_tickers": True,
    }
    assert data["panel_absent"] == [] and data["panel_unknown"] == []
    assert data["payloads"]["instruments_linear_p1"]["row_count"] == 2
    assert data["limits"] == {"max_response_bytes": 1024 * 1024, "timeout_seconds": 10.0, "wall_budget_seconds": 120.0}
    assert set(data["payload_fetched_at"]) == {"instruments_linear_p1", "instruments_linear_p2", "instruments_spot", "tickers_linear"}
    assert "SENTINEL_PRICE" not in json.dumps(data)  # summary holds counts and membership, not quotes


def test_pagination_bound_yields_incomplete_with_unknown_membership(tmp_path: Path) -> None:
    urls = payloads(pages=3)
    urls[universe._cursor_url("c2")] = page([lin("CCCUSDT")], "linear", cursor="c3")  # still more
    with pytest.raises(ValueError, match="pagination incomplete after 3 page\\(s\\)"):
        snapshot_bybit_linear_universe(tmp_path, panel_symbols=PANEL, fetch=lambda u: urls[u], clock=lambda: NOW)
    data = summary(tmp_path)
    assert data["parse_ok"] is False
    assert data["linear_pages_fetched"] == 3
    assert data["request_count"] == 5  # 3 linear pages + spot + tickers, never a 4th page
    assert data["linear_pagination_complete"] is False
    assert data["panel_membership_coverage"] == "incomplete"
    # GRAM and XLM were on page 2 (positive evidence); nothing is marked absent under incomplete coverage.
    assert data["panel_membership"]["XLMUSDT"]["present_in_instruments"] is True
    assert data["panel_absent"] == []
    assert (run_dir(tmp_path) / "raw" / "instruments_linear_p3.json.gz").exists()
    assert (run_dir(tmp_path) / "raw" / "tickers_linear.json.gz").exists()


def test_symbol_missing_under_incomplete_coverage_is_unknown_not_absent(tmp_path: Path) -> None:
    urls = payloads(pages=3, panel_present=False)
    urls[universe._cursor_url("c2")] = page([lin("CCCUSDT")], "linear", cursor="c3")
    with pytest.raises(ValueError, match="pagination incomplete"):
        snapshot_bybit_linear_universe(tmp_path, panel_symbols=PANEL, fetch=lambda u: urls[u], clock=lambda: NOW)
    data = summary(tmp_path)
    assert data["panel_membership"]["XLMUSDT"]["present_in_instruments"] is None
    assert data["panel_absent"] == []
    assert data["panel_unknown"] == ["XLMUSDT", "GRAMUSDT"]


@pytest.mark.parametrize("bad", [False, True, "0", 0.0, None])
def test_ret_code_must_be_a_strict_integer_zero(tmp_path: Path, bad) -> None:
    urls = payloads()
    body = json.loads(urls[universe.LINEAR_URL])
    body["retCode"] = bad
    urls[universe.LINEAR_URL] = json.dumps(body).encode("utf-8")
    with pytest.raises(ValueError, match="retCode is not a strict integer"):
        snapshot_bybit_linear_universe(tmp_path, panel_symbols=PANEL, fetch=lambda u: urls[u], clock=lambda: NOW)
    data = summary(tmp_path)
    assert data["payloads"]["instruments_linear_p1"]["parse_ok"] is False
    assert data["request_count"] == 3  # page 1 failed, no page 2, spot and tickers still requested
    assert data["panel_unknown"] == ["XLMUSDT", "GRAMUSDT"]


def test_spot_cursor_and_repeated_cursor_and_duplicate_symbol_are_failures(tmp_path: Path) -> None:
    urls = payloads(spot_cursor="unexpected")
    with pytest.raises(ValueError, match="unexpected cursor on spot"):
        snapshot_bybit_linear_universe(tmp_path, panel_symbols=PANEL, fetch=lambda u: urls[u], clock=lambda: NOW)

    urls = payloads()
    urls[universe._cursor_url("c1")] = page([lin("XLMUSDT")], "linear", cursor="c1")  # repeats c1
    with pytest.raises(ValueError, match="repeated cursor"):
        snapshot_bybit_linear_universe(tmp_path / "b", panel_symbols=PANEL, fetch=lambda u: urls[u], clock=lambda: NOW)

    urls = payloads()
    urls[universe._cursor_url("c1")] = page([lin("AAAUSDT"), lin("XLMUSDT")], "linear")
    with pytest.raises(ValueError, match="repeated across pages"):
        snapshot_bybit_linear_universe(tmp_path / "c", panel_symbols=PANEL, fetch=lambda u: urls[u], clock=lambda: NOW)


def test_absent_panel_symbol_under_complete_coverage_is_recorded_not_fatal(tmp_path: Path) -> None:
    urls = payloads(panel_present=False)
    result = snapshot_bybit_linear_universe(tmp_path, panel_symbols=PANEL, fetch=lambda u: urls[u], clock=lambda: NOW)
    assert result.payload_count == 4
    data = summary(tmp_path)
    assert data["parse_ok"] is True
    assert data["panel_membership_coverage"] == "complete"
    assert data["panel_absent"] == ["XLMUSDT", "GRAMUSDT"]
    assert data["panel_unknown"] == []
    assert data["panel_membership"]["XLMUSDT"]["present_in_instruments"] is False
    assert data["panel_membership"]["XLMUSDT"]["present_in_tickers"] is True  # tickers still list it
    assert data["panel_membership"]["GRAMUSDT"]["present_in_tickers"] is False


def test_fetch_failure_on_page_one_still_writes_summary_and_skips_further_pages(tmp_path: Path) -> None:
    urls = payloads()
    calls: list[str] = []

    def fetch(url: str) -> bytes:
        calls.append(url)
        if url == universe.LINEAR_URL:
            raise OSError("boom")
        return urls[url]

    with pytest.raises(ValueError, match="instruments_linear_p1 fetch failed"):
        snapshot_bybit_linear_universe(tmp_path, panel_symbols=PANEL, fetch=fetch, clock=lambda: NOW)
    assert calls == [universe.LINEAR_URL, universe.SPOT_URL, universe.TICKERS_URL]
    data = summary(tmp_path)
    assert data["payloads"]["instruments_linear_p1"]["raw_bytes"] is None
    assert data["payloads"]["tickers_linear"]["parse_ok"] is True
    assert data["panel_membership"]["XLMUSDT"] == {
        "present_in_instruments": None, "status": None, "delivery_time": None, "funding_interval": None,
        "present_in_tickers": True,
    }
    assert data["panel_unknown"] == ["XLMUSDT", "GRAMUSDT"] and data["panel_absent"] == []


def test_truncated_read_is_a_recorded_fetch_failure_with_summary(tmp_path: Path) -> None:
    """http.client.IncompleteRead is not an OSError; it must still land in the summary."""
    urls = payloads()

    def fetch(url: str) -> bytes:
        if url == universe.TICKERS_URL:
            raise http.client.IncompleteRead(b"partial", 10)
        return urls[url]

    with pytest.raises(ValueError, match="tickers_linear fetch failed: IncompleteRead"):
        snapshot_bybit_linear_universe(tmp_path, panel_symbols=PANEL, fetch=fetch, clock=lambda: NOW)
    data = summary(tmp_path)
    assert data["payloads"]["instruments_linear_p2"]["parse_ok"] is True  # earlier bytes kept
    assert data["payloads"]["tickers_linear"]["raw_bytes"] is None
    assert data["panel_membership"]["XLMUSDT"]["present_in_tickers"] is None  # unknown, not false
    assert data["panel_membership"]["XLMUSDT"]["present_in_instruments"] is True


def test_wall_budget_stops_remaining_requests_without_sending_them(tmp_path: Path) -> None:
    urls = payloads()
    ticks = iter([0.0, 0.0, 200.0, 200.0, 200.0, 200.0, 200.0])
    calls: list[str] = []

    def fetch(url: str) -> bytes:
        calls.append(url)
        return urls[url]

    with pytest.raises(ValueError, match="wall budget"):
        snapshot_bybit_linear_universe(
            tmp_path, panel_symbols=PANEL, fetch=fetch, clock=lambda: NOW, monotonic=lambda: next(ticks)
        )
    assert calls == [universe.LINEAR_URL]  # page 2, spot and tickers were never sent
    data = summary(tmp_path)
    assert data["request_count"] == 4  # attempted entries recorded, three as budget failures
    assert "wall budget" in data["payloads"]["instruments_spot"]["error"]
    assert data["panel_membership_coverage"] == "incomplete"


def test_fetch_url_applies_one_mib_cap_and_ten_second_timeout(monkeypatch) -> None:
    captured = {}

    def fake_fetch_bytes(url, **kwargs):
        captured.update(kwargs)
        return b"{}"

    monkeypatch.setattr(universe, "fetch_bytes", fake_fetch_bytes)
    universe.fetch_url(universe.TICKERS_URL)
    assert captured["max_response_bytes"] == 1024 * 1024
    assert captured["timeout_seconds"] == 10.0
    assert captured["user_agent"] == universe.USER_AGENT


def test_unsafe_or_empty_panel_fails_before_any_fetch(tmp_path: Path) -> None:
    def fetch(url: str) -> bytes:
        raise AssertionError("must not fetch")

    with pytest.raises(ValueError):
        snapshot_bybit_linear_universe(tmp_path, panel_symbols=(), fetch=fetch)
    with pytest.raises(ValueError):
        snapshot_bybit_linear_universe(tmp_path, panel_symbols=("XLM USDT",), fetch=fetch)


def test_ops_job_registration_and_arg_threading(tmp_path: Path, monkeypatch) -> None:
    assert "bybit-linear-universe-snapshot" in COLLECTOR_JOB_TYPES
    assert "bybit-linear-universe-snapshot" in POLL_LANE_JOB_TYPES
    args = _job_args(SimpleNamespace(job_type="bybit-linear-universe-snapshot",
                                     args={"output_root": str(tmp_path / "raw"), "panel_symbols": ["XLMUSDT"], "format": "json"}))
    assert args.output_root == Path(tmp_path / "raw")
    assert args.panel_symbol == ["XLMUSDT"]
    assert args.format == "json"
    captured = {}

    def fake(output_root, *, panel_symbols):
        captured["output_root"] = output_root
        captured["panel_symbols"] = panel_symbols
        return SimpleNamespace(run_path="r", fetched_at=NOW.isoformat(), payload_count=4, total_raw_bytes=10,
                               row_counts={"instruments_linear_p1": 2})

    monkeypatch.setattr(cli, "snapshot_bybit_linear_universe", fake)
    cli.run_bybit_linear_universe_snapshot(args)
    assert captured == {"output_root": Path(tmp_path / "raw"), "panel_symbols": ("XLMUSDT",)}
    defaults = _job_args(SimpleNamespace(job_type="bybit-linear-universe-snapshot", args={}))
    assert tuple(defaults.panel_symbol) == universe.DEFAULT_PANEL
