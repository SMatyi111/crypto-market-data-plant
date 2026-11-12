"""Synthetic receipt-clock and preservation tests; no archive dependencies."""

import copy
import datetime as dt
import json

import pytest

from crypto_collector.market_normalizers import (
    BinanceFuturesFundingNormalizer,
    BinanceOpenInterestNormalizer,
)
from crypto_collector.models import RawMessage
from crypto_collector.replay import replay_funding_run

BASE = dt.datetime(2026, 11, 1, tzinfo=dt.timezone.utc)


def observation(event_ms=0, receipt_ms=3000, symbol="BTCUSDT", funding=False):
    payload = {
        "symbol": symbol,
        "time": int(BASE.timestamp() * 1000) + event_ms,
        "markPrice": "100",
        "openInterest": "10",
    }
    normalizer = (
        BinanceFuturesFundingNormalizer()
        if funding
        else BinanceOpenInterestNormalizer()
    )
    return normalizer.normalize(
        RawMessage(
            source="binance-futures",
            payload=payload,
            received_at=BASE + dt.timedelta(milliseconds=receipt_ms),
        )
    ).to_dict()


def write_run(tmp_path, rows):
    path = tmp_path / "run" / "clean" / "events.jsonl"
    path.parent.mkdir(parents=True)
    path.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
    return path.parent.parent


def test_venue_regression_is_diagnostic_and_source_bytes_unchanged(tmp_path):
    run = write_run(
        tmp_path,
        [observation(2000, 3000), observation(0, 4000), observation(1000, 5000)],
    )
    source = run / "clean/events.jsonl"
    before = source.read_bytes()
    verdict = replay_funding_run(run, write_summary=False)
    assert verdict.replayable and verdict.findings == []
    assert verdict.mode == "open_interest_receipt_order_none_native"
    assert verdict.informational_findings == ["non_monotonic_event_time"]
    assert verdict.non_monotonic_count == 2  # below running venue-time high-water
    assert verdict.non_monotonic_received_at_count == 0
    assert verdict.gap_detection == "none_native"
    assert source.read_bytes() == before
    assert not (run / "metrics/replay_summary.json").exists()


def test_receipt_regression_refuses_even_when_venue_time_increases(tmp_path):
    verdict = replay_funding_run(
        write_run(tmp_path, [observation(0, 4000), observation(1000, 3000)])
    )
    assert not verdict.replayable
    assert "non_monotonic_received_at" in verdict.findings
    assert verdict.non_monotonic_received_at_count == 1


def test_interleaved_products_use_separate_receipt_clocks(tmp_path):
    rows = [
        observation(0, 4000),
        observation(0, 3000, "ETHUSDT"),
        observation(0, 5000),
        observation(0, 4000, "ETHUSDT"),
    ]
    assert replay_funding_run(write_run(tmp_path, rows)).replayable


@pytest.mark.parametrize("field", ["exchange_time", "received_at"])
@pytest.mark.parametrize("value", [None, "", "broken", "2026-11-01T00:00:00", 123])
def test_missing_malformed_or_naive_clock_refuses(tmp_path, field, value):
    row = observation()
    row[field] = value
    verdict = replay_funding_run(write_run(tmp_path, [row]))
    assert not verdict.replayable and verdict.invalid_timestamp_count == 1
    assert "invalid_timestamp" in verdict.findings


@pytest.mark.parametrize("event_ms", [-60001, 60001])
def test_stale_or_future_observation_keeps_skew_gate(tmp_path, event_ms):
    verdict = replay_funding_run(write_run(tmp_path, [observation(event_ms, 0)]))
    assert not verdict.replayable and "excessive_clock_skew" in verdict.findings


@pytest.mark.parametrize("value", [None, -1, float("nan"), float("inf")])
def test_invalid_quantity_refuses(tmp_path, value):
    row = observation()
    row["size"] = value
    verdict = replay_funding_run(write_run(tmp_path, [row]))
    assert not verdict.replayable and "invalid_open_interest" in verdict.findings


@pytest.mark.parametrize("mutation", ["source", "product", "identity", "price"])
def test_wrong_identity_and_price_leak_refuse(tmp_path, mutation):
    row = observation()
    if mutation == "identity":
        row["metadata"]["instrument_id"] = "spot:binance:BTCUSDT"
    elif mutation == "price":
        row["price"] = 10
    elif mutation == "product":
        row["product"] = None
    else:
        row["source"] = "other"
    assert not replay_funding_run(write_run(tmp_path, [row])).replayable


def test_zero_quantity_and_duplicate_timestamps_are_preserved(tmp_path):
    row = observation()
    row["size"] = 0
    verdict = replay_funding_run(write_run(tmp_path, [row, copy.deepcopy(row)]))
    assert verdict.replayable and verdict.event_count == 2


def test_funding_venue_order_is_still_required(tmp_path):
    verdict = replay_funding_run(
        write_run(
            tmp_path,
            [observation(2000, 3000, funding=True), observation(0, 4000, funding=True)],
        )
    )
    assert not verdict.replayable and "non_monotonic_event_time" in verdict.findings
    assert verdict.mode == "funding_none_native"


def test_mixed_metric_channels_refuse(tmp_path):
    verdict = replay_funding_run(
        write_run(tmp_path, [observation(), observation(funding=True)])
    )
    assert not verdict.replayable and "mixed_metric_channels" in verdict.findings


def test_empty_run_refuses(tmp_path):
    verdict = replay_funding_run(write_run(tmp_path, []))
    assert not verdict.replayable and verdict.findings == ["no_events"]


@pytest.mark.parametrize("limit", [-1, float("nan"), float("inf")])
def test_invalid_skew_limit_refuses(tmp_path, limit):
    with pytest.raises(ValueError, match="finite and nonnegative"):
        replay_funding_run(
            write_run(tmp_path, [observation()]), max_clock_skew_ms=limit
        )


def test_new_oi_run_promotes_original_clocks_and_quarantine_is_not_bypassed(tmp_path):
    import pyarrow.parquet as pq
    from crypto_collector.promotion import promote_replayable_runs

    rows = [observation(2000, 3000), observation(0, 4000)]
    run = write_run(tmp_path / "source", rows)
    verdict = replay_funding_run(run)
    assert verdict.replayable
    target = tmp_path / "curated"
    report = promote_replayable_runs(run.parent, target)
    assert report.promoted_row_count == 2
    files = list(target.rglob("*.parquet"))
    assert len(files) == 1
    stored = (
        pq.ParquetFile(files[0])
        .read(columns=["exchange_time", "received_at", "size"])
        .to_pylist()
    )
    assert stored == [
        {k: r[k] for k in ["exchange_time", "received_at", "size"]} for r in rows
    ]
    # Even a new passing verdict must not silently revive an indexed quarantine.
    quarantine = tmp_path / "quarantine.jsonl"
    quarantine.write_text(json.dumps({"run_path": str(run)}) + "\n")
    blocked = promote_replayable_runs(
        run.parent, tmp_path / "blocked", quarantine_index_path=quarantine
    )
    assert blocked.promoted_row_count == 0
