from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path
import time

import pyarrow.dataset as ds
import pytest

from crypto_collector.cli import _job_args, build_parser
from crypto_collector.collectors.kalshi import (
    KalshiApiResponse,
    build_kalshi_discovery_report,
    collect_kalshi_crypto_quotes,
    discover_kalshi_crypto_markets,
    summarize_kalshi_quote_rows,
)
from crypto_collector.ops import JobSpec


def test_kalshi_discovery_selects_btc_eth_and_filters_frequencies() -> None:
    report = build_kalshi_discovery_report(
        series_payloads=[
            _series("KXBTC15M", "Bitcoin every 15 minutes", frequency="fifteen_min", tags=["BTC"]),
            _series("KXBTCH", "Bitcoin hourly", frequency="hourly", tags=["BTC"]),
            _series("KXETH15M", "Ethereum every 15 minutes", frequency="fifteen_min", tags=["ETH"]),
            _series("KXBTCD", "Bitcoin daily", frequency="daily", tags=["BTC"]),
            _series("KXSOL15M", "Solana every 15 minutes", frequency="fifteen_min", tags=["SOL"]),
        ],
        market_loader=lambda ticker: [_market(ticker=f"{ticker}-M1", series_ticker=ticker)],
        target_assets=["BTC", "ETH"],
        target_frequencies=["fifteen_min", "hourly"],
    )

    assert report.status == "ok"
    assert report.series_count == 5
    assert report.selected_series_count == 3
    assert report.market_count == 3
    assert report.shortest_frequency == "fifteen_min"
    assert report.shortest_frequency_seconds == 900
    assert all(market.series_ticker != "KXBTCD" for market in report.markets)


def test_kalshi_discovery_writes_report_artifacts(tmp_path: Path) -> None:
    report = discover_kalshi_crypto_markets(
        output_root=tmp_path,
        client=FakeKalshiClient(),
        target_assets=["BTC", "ETH"],
        target_frequencies=["fifteen_min", "hourly"],
    )

    assert report.selected_series_count == 3
    assert (tmp_path / "kalshi_crypto_discovery_latest.json").exists()
    assert (tmp_path / "kalshi_crypto_discovery_latest.md").exists()


def test_kalshi_quote_collector_writes_raw_clean_parquet_and_side_rows(tmp_path: Path) -> None:
    summary = collect_kalshi_crypto_quotes(
        output_root=tmp_path / "raw" / "market",
        normalized_root=tmp_path / "normalized" / "binary_options",
        target_assets=["BTC", "ETH"],
        target_frequencies=["fifteen_min", "hourly"],
        sample_count=2,
        duration_seconds=10,
        poll_interval_seconds=0,
        stale_after_seconds=3,
        client=FakeKalshiClient(),
        jsonl_fsync=False,
    )

    run_path = Path(summary.run_path)
    raw_rows = _read_jsonl(run_path / "raw" / "messages.jsonl")
    clean_rows = _read_jsonl(run_path / "clean" / "events.jsonl")

    assert summary.status == "ok"
    assert summary.sample_count == 2
    assert summary.selected_series_count == 3
    assert summary.market_fetch_count == 6
    assert summary.quote_count == 12
    assert summary.side_symbol_count == 6
    assert summary.first_observed_quote_count == 6
    assert summary.quote_update_count == 0
    assert summary.repeated_quote_count == 6
    assert len(raw_rows) == 7  # one /series response plus 3 market responses per sample
    assert len(clean_rows) == 12

    first_yes = next(row for row in clean_rows if row["side"] == "YES")
    assert first_yes["venue"] == "kalshi"
    assert first_yes["event_type"] == "quote"
    assert first_yes["symbol"].endswith(":YES")
    assert first_yes["payout_ratio"] == pytest.approx((1 - 0.47) / 0.47)
    assert any(row["side"] == "NO" and row["symbol"].endswith(":NO") for row in clean_rows)
    assert first_yes["quote_first_observed"] is True
    assert first_yes["quote_changed"] is False

    dataset = ds.dataset(tmp_path / "normalized" / "binary_options", format="parquet", partitioning="hive")
    parquet_rows = dataset.to_table().to_pylist()
    assert len(parquet_rows) == 12
    assert {row["source"] for row in parquet_rows} == {"kalshi"}


def test_kalshi_quote_collector_marks_stale_repeated_quotes(tmp_path: Path) -> None:
    summary = collect_kalshi_crypto_quotes(
        output_root=tmp_path,
        normalized_root=None,
        target_assets=["BTC"],
        target_frequencies=["hourly"],
        sample_count=2,
        duration_seconds=10,
        poll_interval_seconds=0.02,
        stale_after_seconds=0.001,
        client=FakeKalshiClient(),
        jsonl_fsync=False,
    )

    rows = _read_jsonl(Path(summary.run_path) / "clean" / "events.jsonl")
    assert summary.quote_count == 4
    assert summary.stale_quote_count == 2
    assert any(row["quote_repeated"] is True and row["quote_stale"] is True for row in rows)


def test_kalshi_quote_summary_counts_subsequent_transitions_per_symbol(tmp_path: Path) -> None:
    input_path = tmp_path / "events.jsonl"
    rows = [
        _quote_row("2026-06-01T00:00:00+00:00", "BTC:YES", "btc-yes-1"),
        _quote_row("2026-06-01T00:00:00+00:00", "BTC:NO", "btc-no-1"),
        _quote_row("2026-06-01T00:00:01+00:00", "BTC:YES", "btc-yes-1"),
        _quote_row("2026-06-01T00:00:01+00:00", "BTC:NO", "btc-no-1"),
        _quote_row("2026-06-01T00:00:02+00:00", "BTC:YES", "btc-yes-2"),
    ]
    input_path.write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")

    summary = summarize_kalshi_quote_rows(input_path)

    assert summary["quote_count"] == 5
    assert summary["side_symbol_count"] == 2
    assert summary["first_observed_quote_count"] == 2
    assert summary["quote_update_count"] == 1


def test_kalshi_cli_parser_and_job_args_defaults() -> None:
    parser = build_parser()
    args = parser.parse_args(["kalshi-collect-crypto-quotes", "--sample-count", "1"])

    assert args.command == "kalshi-collect-crypto-quotes"
    assert args.target_assets == ["BTC", "ETH"]
    assert args.target_frequencies == ["fifteen_min", "hourly"]
    assert args.poll_interval_seconds == 5.0
    assert args.stale_after_seconds == 3.0

    job_args = _job_args(
        JobSpec(
            name="kalshi",
            job_type="kalshi-collect-crypto-quotes",
            interval_seconds=300,
            args={"sample_count": 1, "normalized_parquet": False},
        )
    )
    assert job_args.sample_count == 1
    assert job_args.normalized_parquet is False


class FakeKalshiClient:
    base_url = "https://fake.kalshi"

    def fetch_series(self, *, category: str) -> KalshiApiResponse:
        return _response(
            endpoint="/series",
            params={"category": category},
            payload={
                "series": [
                    _series("KXBTC15M", "Bitcoin every 15 minutes", frequency="fifteen_min", tags=["BTC"]),
                    _series("KXBTCH", "Bitcoin hourly", frequency="hourly", tags=["BTC"]),
                    _series("KXETH15M", "Ethereum every 15 minutes", frequency="fifteen_min", tags=["ETH"]),
                    _series("KXBTCD", "Bitcoin daily", frequency="daily", tags=["BTC"]),
                ]
            },
        )

    def fetch_markets(
        self,
        *,
        series_ticker: str,
        status: str = "open",
        limit: int = 100,
        cursor: str | None = None,
    ) -> KalshiApiResponse:
        return _response(
            endpoint="/markets",
            params={"series_ticker": series_ticker, "status": status, "limit": limit},
            payload={"markets": [_market(ticker=f"{series_ticker}-M1", series_ticker=series_ticker)]},
        )


def _response(*, endpoint: str, params: dict, payload: dict) -> KalshiApiResponse:
    now = datetime.now(tz=UTC)
    return KalshiApiResponse(
        endpoint=endpoint,
        params=params,
        url=f"https://fake.kalshi{endpoint}",
        payload=payload,
        request_sent_at=now,
        response_received_at=now,
        latency_ms=10.0,
        http_status=200,
    )


def _series(ticker: str, title: str, *, frequency: str, tags: list[str]) -> dict:
    return {
        "ticker": ticker,
        "title": title,
        "category": "Crypto",
        "frequency": frequency,
        "tags": tags,
        "volume_fp": "1000",
    }


def _market(*, ticker: str, series_ticker: str) -> dict:
    return {
        "ticker": ticker,
        "series_ticker": series_ticker,
        "event_ticker": f"{series_ticker}-EVENT",
        "title": "Bitcoin above threshold?",
        "yes_sub_title": "Above",
        "status": "open",
        "market_type": "binary",
        "close_time": "2026-06-07T01:00:00Z",
        "expiration_time": "2026-06-07T01:00:00Z",
        "yes_bid_dollars": "0.45",
        "yes_ask_dollars": "0.47",
        "no_bid_dollars": "0.52",
        "no_ask_dollars": "0.54",
        "yes_ask_size_fp": "10",
        "no_ask_size_fp": "12",
        "volume_fp": "100",
        "volume_24h_fp": "10",
        "open_interest_fp": "50",
        "liquidity_dollars": "1000",
        "fractional_trading_enabled": True,
    }


def _quote_row(event_time: str, symbol: str, quote_id: str) -> dict:
    return {
        "source": "kalshi",
        "venue": "kalshi",
        "event_type": "quote",
        "event_time": event_time,
        "symbol": symbol,
        "quote_id": quote_id,
        "quote_repeated": False,
        "quote_stale": False,
    }


def _read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
