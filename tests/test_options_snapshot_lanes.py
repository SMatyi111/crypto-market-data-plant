"""Options-IV raw-only snapshot lanes (STANDARDS 4.9).

Mirrors the leaderboard-lane tests: exact-bytes archival, fail-loud-but-archive
on malformed payloads, ops job registration + arg threading (the documented
per-job-type enumeration trap), and the Binance never-retry-418 rule inherited
from the fapi REST collector.
"""

from __future__ import annotations

import gzip
import hashlib
import json
from datetime import UTC, datetime
from email.message import Message
from pathlib import Path
from types import SimpleNamespace
from urllib.error import HTTPError

import pytest

import crypto_collector.cli as cli
import crypto_collector.collectors.binance_options as binance_options
import crypto_collector.collectors.deribit_options as deribit_options
from crypto_collector.cli import _job_args
from crypto_collector.collectors.binance_options import (
    snapshot_binance_options_chain,
)
from crypto_collector.collectors.deribit_options import snapshot_deribit_options
from crypto_collector.ops import COLLECTOR_JOB_TYPES


NOW = datetime(2026, 8, 29, 12, 0, 0, tzinfo=UTC)


def _binance_payloads(*, drop_eth_from_ticker: bool = False) -> dict[str, bytes]:
    base = binance_options.BASE_URL
    ticker = [{"symbol": "BTC-260828-60000-C", "lastPrice": "100"}]
    if not drop_eth_from_ticker:
        ticker.append({"symbol": "ETH-260828-3000-C", "lastPrice": "10"})
    payloads = {
        f"{base}/eapi/v1/exchangeInfo": {
            "optionSymbols": [
                {"symbol": "BTC-260828-60000-C"},
                {"symbol": "ETH-260828-3000-C"},
            ]
        },
        f"{base}/eapi/v1/mark": [{"symbol": "BTC-260828-60000-C", "markIV": "0.5"}],
        f"{base}/eapi/v1/ticker": ticker,
        f"{base}/eapi/v1/index?underlying=BTCUSDT": {"indexPrice": "65000"},
        f"{base}/eapi/v1/index?underlying=ETHUSDT": {"indexPrice": "3000"},
    }
    return {url: json.dumps(body).encode("utf-8") for url, body in payloads.items()}


def test_binance_snapshot_archives_each_payload_verbatim(tmp_path: Path) -> None:
    payloads = _binance_payloads()
    result = snapshot_binance_options_chain(
        tmp_path, fetch=lambda url: payloads[url], clock=lambda: NOW
    )
    assert result.parse_ok is True
    assert result.payload_count == 5
    assert result.option_symbol_count == 2

    run_dir = tmp_path / binance_options.SOURCE_NAME / "20260829_120000"
    assert Path(result.run_path) == run_dir
    base = binance_options.BASE_URL
    with gzip.open(run_dir / "raw" / "exchange_info.json.gz", "rb") as handle:
        assert handle.read() == payloads[f"{base}/eapi/v1/exchangeInfo"]
    with gzip.open(run_dir / "raw" / "index_BTCUSDT.json.gz", "rb") as handle:
        assert handle.read() == payloads[f"{base}/eapi/v1/index?underlying=BTCUSDT"]

    summary = json.loads((run_dir / "metrics" / "summary.json").read_text(encoding="utf-8"))
    assert summary["parse_ok"] is True
    assert summary["option_symbol_count"] == 2
    assert summary["payloads"]["ticker"]["row_count"] == 2
    expected_sha = hashlib.sha256(payloads[f"{base}/eapi/v1/mark"]).hexdigest()
    assert summary["payloads"]["mark"]["sha256"] == expected_sha
    assert summary["fetched_at"] == NOW.isoformat()


def test_binance_snapshot_fails_but_archives_when_underlying_missing(tmp_path: Path) -> None:
    payloads = _binance_payloads(drop_eth_from_ticker=True)
    with pytest.raises(ValueError, match="no contracts for ETHUSDT"):
        snapshot_binance_options_chain(
            tmp_path, fetch=lambda url: payloads[url], clock=lambda: NOW
        )
    run_dir = tmp_path / binance_options.SOURCE_NAME / "20260829_120000"
    # Evidence archived even though the job fails.
    with gzip.open(run_dir / "raw" / "ticker.json.gz", "rb") as handle:
        assert handle.read() == payloads[f"{binance_options.BASE_URL}/eapi/v1/ticker"]
    summary = json.loads((run_dir / "metrics" / "summary.json").read_text(encoding="utf-8"))
    assert summary["parse_ok"] is False
    assert summary["payloads"]["ticker"]["parse_ok"] is False


def test_binance_snapshot_fails_on_non_json_payload(tmp_path: Path) -> None:
    payloads = _binance_payloads()
    payloads[f"{binance_options.BASE_URL}/eapi/v1/mark"] = b"<html>upstream error</html>"
    with pytest.raises(ValueError, match="mark payload is not valid JSON"):
        snapshot_binance_options_chain(
            tmp_path, fetch=lambda url: payloads[url], clock=lambda: NOW
        )


def _http_error(url: str, code: int, retry_after: str | None = None) -> HTTPError:
    headers = Message()
    if retry_after is not None:
        headers["Retry-After"] = retry_after
    return HTTPError(url, code, f"http {code}", headers, None)


def test_binance_fetch_never_retries_418(monkeypatch) -> None:
    calls = {"n": 0}

    def fake_urlopen(request, timeout):
        calls["n"] += 1
        raise _http_error(request.full_url, 418)

    sleeps: list[float] = []
    monkeypatch.setattr(binance_options, "urlopen", fake_urlopen)
    monkeypatch.setattr(binance_options, "_sleep", sleeps.append)
    with pytest.raises(HTTPError):
        binance_options.fetch_url("https://eapi.binance.com/eapi/v1/mark")
    # 418 is the hammering-escalation ban signal; one attempt, zero backoff.
    assert calls["n"] == 1
    assert sleeps == []


def test_binance_fetch_honors_retry_after_on_429(monkeypatch) -> None:
    calls = {"n": 0}

    def fake_urlopen(request, timeout):
        calls["n"] += 1
        if calls["n"] == 1:
            raise _http_error(request.full_url, 429, retry_after="7")

        class _Response:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def read(self):
                return b"[]"

        return _Response()

    sleeps: list[float] = []
    monkeypatch.setattr(binance_options, "urlopen", fake_urlopen)
    monkeypatch.setattr(binance_options, "_sleep", sleeps.append)
    assert binance_options.fetch_url("https://eapi.binance.com/eapi/v1/mark") == b"[]"
    assert calls["n"] == 2
    assert sleeps == [7.0]


def _deribit_payloads() -> dict[str, bytes]:
    base = deribit_options.BASE_URL
    payloads: dict[str, object] = {}
    for currency in ("BTC", "ETH"):
        payloads[
            f"{base}/public/get_instruments?currency={currency}&kind=option&expired=false"
        ] = {"result": [{"instrument_name": f"{currency}-29AUG26-60000-C"}]}
        payloads[
            f"{base}/public/get_book_summary_by_currency?currency={currency}&kind=option"
        ] = {"result": [{"instrument_name": f"{currency}-29AUG26-60000-C", "mark_iv": 50.0}]}
        payloads[
            f"{base}/public/get_book_summary_by_currency?currency={currency}&kind=future"
        ] = {"result": [{"instrument_name": f"{currency}-PERPETUAL"}]}
    return {url: json.dumps(body).encode("utf-8") for url, body in payloads.items()}


def test_deribit_snapshot_archives_each_payload_verbatim(tmp_path: Path) -> None:
    payloads = _deribit_payloads()
    result = snapshot_deribit_options(
        tmp_path, fetch=lambda url: payloads[url], clock=lambda: NOW
    )
    assert result.parse_ok is True
    assert result.payload_count == 6
    assert result.option_summary_count == 2

    run_dir = tmp_path / deribit_options.SOURCE_NAME / "20260829_120000"
    assert Path(result.run_path) == run_dir
    url = (
        f"{deribit_options.BASE_URL}"
        "/public/get_book_summary_by_currency?currency=BTC&kind=option"
    )
    with gzip.open(run_dir / "raw" / "option_summaries_BTC.json.gz", "rb") as handle:
        assert handle.read() == payloads[url]
    summary = json.loads((run_dir / "metrics" / "summary.json").read_text(encoding="utf-8"))
    assert summary["parse_ok"] is True
    assert summary["payloads"]["option_summaries_BTC"]["row_count"] == 1
    assert summary["option_summary_count"] == 2


def test_deribit_jsonrpc_error_fails_job_but_preserves_raw(tmp_path: Path) -> None:
    payloads = _deribit_payloads()
    url = (
        f"{deribit_options.BASE_URL}"
        "/public/get_instruments?currency=ETH&kind=option&expired=false"
    )
    payloads[url] = json.dumps({"error": {"code": 10028, "message": "too many requests"}}).encode(
        "utf-8"
    )
    with pytest.raises(ValueError, match="instruments_ETH returned a JSON-RPC error"):
        snapshot_deribit_options(tmp_path, fetch=lambda url: payloads[url], clock=lambda: NOW)
    run_dir = tmp_path / deribit_options.SOURCE_NAME / "20260829_120000"
    with gzip.open(run_dir / "raw" / "instruments_ETH.json.gz", "rb") as handle:
        assert handle.read() == payloads[url]
    summary = json.loads((run_dir / "metrics" / "summary.json").read_text(encoding="utf-8"))
    assert summary["parse_ok"] is False


def test_empty_deribit_result_is_a_failure_not_a_quiet_success(tmp_path: Path) -> None:
    payloads = _deribit_payloads()
    url = (
        f"{deribit_options.BASE_URL}"
        "/public/get_book_summary_by_currency?currency=BTC&kind=option"
    )
    payloads[url] = json.dumps({"result": []}).encode("utf-8")
    with pytest.raises(ValueError, match="option_summaries_BTC result is not a non-empty list"):
        snapshot_deribit_options(tmp_path, fetch=lambda url: payloads[url], clock=lambda: NOW)


def test_binance_ops_job_registration_and_arg_threading(tmp_path: Path, monkeypatch) -> None:
    assert "binance-options-chain-snapshot" in COLLECTOR_JOB_TYPES
    args = _job_args(
        SimpleNamespace(
            job_type="binance-options-chain-snapshot",
            args={
                "output_root": str(tmp_path / "raw"),
                "underlyings": ["BTCUSDT"],
                "format": "json",
            },
        )
    )
    assert args.output_root == Path(tmp_path / "raw")
    assert args.underlying == ["BTCUSDT"]
    assert args.format == "json"

    captured = {}

    def fake(output_root, *, underlyings):
        captured["output_root"] = output_root
        captured["underlyings"] = underlyings
        return SimpleNamespace(
            run_path="r",
            fetched_at=NOW.isoformat(),
            payload_count=4,
            total_raw_bytes=10,
            option_symbol_count=2,
            parse_ok=True,
            failures=(),
        )

    monkeypatch.setattr(cli, "snapshot_binance_options_chain", fake)
    cli.run_binance_options_chain_snapshot(args)
    assert captured["output_root"] == Path(tmp_path / "raw")
    assert captured["underlyings"] == ("BTCUSDT",)


def test_deribit_ops_job_registration_and_arg_threading(tmp_path: Path, monkeypatch) -> None:
    assert "deribit-options-snapshot" in COLLECTOR_JOB_TYPES
    args = _job_args(
        SimpleNamespace(
            job_type="deribit-options-snapshot",
            args={"output_root": str(tmp_path / "raw"), "currencies": ["BTC"]},
        )
    )
    assert args.output_root == Path(tmp_path / "raw")
    assert args.currency == ["BTC"]
    assert args.format == "text"

    captured = {}

    def fake(output_root, *, currencies):
        captured["output_root"] = output_root
        captured["currencies"] = currencies
        return SimpleNamespace(
            run_path="r",
            fetched_at=NOW.isoformat(),
            payload_count=3,
            total_raw_bytes=10,
            option_summary_count=5,
            parse_ok=True,
            failures=(),
        )

    monkeypatch.setattr(cli, "snapshot_deribit_options", fake)
    cli.run_deribit_options_snapshot(args)
    assert captured["output_root"] == Path(tmp_path / "raw")
    assert captured["currencies"] == ("BTC",)
