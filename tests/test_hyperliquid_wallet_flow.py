from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import crypto_collector.cli as cli
from crypto_collector.cli import _job_args
from crypto_collector.collectors.hyperliquid_wallet_flow import (
    HyperliquidWalletFillNormalizer,
    HyperliquidWalletFlowPoller,
    load_wallet_flow_cohort,
    scan_durable_wallet_fills,
)
from crypto_collector.models import RawMessage
from crypto_collector.ops import COLLECTOR_JOB_TYPES


ADDRESS_A = "0x" + "1" * 40
ADDRESS_B = "0x" + "2" * 40
START = datetime(2026, 8, 9, tzinfo=UTC)


def _write_cohort(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "prospective_start_at": START.isoformat(),
                "target_coins": ["BTC", "ETH", "SOL"],
                "wallets": [
                    {"address": ADDRESS_A, "candidate_rank": 2, "cohort_rank": 1},
                    {"address": ADDRESS_B, "candidate_rank": 5, "cohort_rank": 2},
                ],
            }
        ),
        encoding="utf-8",
    )


def _fill(*, trade_id: int, timestamp_ms: int, coin: str = "BTC") -> dict:
    return {
        "coin": coin,
        "px": "100000",
        "sz": "0.01",
        "side": "B",
        "time": timestamp_ms,
        "startPosition": "0",
        "dir": "Open Long",
        "closedPnl": "0",
        "hash": "0xabc",
        "oid": 10,
        "crossed": True,
        "fee": "0.5",
        "tid": trade_id,
        "feeToken": "USDC",
    }


def test_load_cohort_and_normalize_fill(tmp_path: Path) -> None:
    path = tmp_path / "cohort.json"
    _write_cohort(path)
    cohort = load_wallet_flow_cohort(path)
    assert [wallet.address for wallet in cohort.wallets] == [ADDRESS_A, ADDRESS_B]
    assert cohort.target_coins == {"BTC", "ETH", "SOL"}

    payload = _fill(trade_id=123, timestamp_ms=int(START.timestamp() * 1000))
    payload.update(
        {
            "_wallet": ADDRESS_A,
            "_candidate_rank": 2,
            "_cohort_rank": 1,
            "_trade_key": f"{ADDRESS_A}:123",
            "_prospective_start_at": START.isoformat(),
            "_cohort_sha256": cohort.sha256,
        }
    )
    event = HyperliquidWalletFillNormalizer().normalize(
        RawMessage(source="hyperliquid", received_at=START, payload=payload)
    )
    assert event.product == "BTC"
    assert event.side == "buy"
    assert event.trade_id == f"{ADDRESS_A}:123"
    assert event.sequence is None
    assert event.metadata["wallet"] == ADDRESS_A
    assert event.metadata["instrument_id"] == "perp:hyperliquid:BTCUSDC"
    assert event.metadata["canonical_symbol"] == "BTC/USDC-PERP"


def test_poller_deduplicates_and_isolates_one_wallet_error(tmp_path: Path) -> None:
    cohort_path = tmp_path / "cohort.json"
    _write_cohort(cohort_path)
    cohort = load_wallet_flow_cohort(cohort_path)
    now = datetime(2026, 8, 9, 0, 1, tzinfo=UTC)
    timestamp_ms = int(now.timestamp() * 1000) - 1_000

    def fetch(payload):
        if payload["user"] == ADDRESS_B:
            raise RuntimeError("temporary failure")
        return [_fill(trade_id=7, timestamp_ms=timestamp_ms)]

    poller = HyperliquidWalletFlowPoller(
        cohort=cohort,
        source_root=tmp_path / "raw" / "hyperliquid_wallet_flow",
        state_path=tmp_path / "state.json",
        fetch=fetch,
        request_pause_seconds=0,
        clock=lambda: now,
    )
    first, more_pending = asyncio.run(poller.poll())
    second, _ = asyncio.run(poller.poll())
    assert more_pending is False
    assert len(first) == 1
    assert first[0]["_trade_key"] == f"{ADDRESS_A}:7"
    assert second == []
    state = json.loads((tmp_path / "state.json").read_text(encoding="utf-8"))
    assert state["last_poll_complete"] is False
    assert state["poll_error_count"] == 2
    assert state["duplicate_count"] == 1
    assert state["per_wallet"][ADDRESS_B]["last_response_complete"] is False


def test_capped_response_pages_forward_instead_of_stalling(tmp_path: Path) -> None:
    cohort_path = tmp_path / "cohort.json"
    _write_cohort(cohort_path)
    cohort = load_wallet_flow_cohort(cohort_path)
    base_ms = int(START.timestamp() * 1000)
    now = datetime(2026, 8, 9, 0, 5, tzinfo=UTC)

    ledger = [
        _fill(trade_id=1, timestamp_ms=base_ms + 1_000),
        _fill(trade_id=2, timestamp_ms=base_ms + 2_000),
        _fill(trade_id=3, timestamp_ms=base_ms + 3_000),
        _fill(trade_id=4, timestamp_ms=base_ms + 4_000),
    ]

    def fetch(payload):
        if payload["user"] != ADDRESS_A:
            return []
        window = [fill for fill in ledger if fill["time"] >= payload["startTime"]]
        return window[:3]  # server truncates at its cap (3 here)

    poller = HyperliquidWalletFlowPoller(
        cohort=cohort,
        source_root=tmp_path / "raw" / "hyperliquid_wallet_flow",
        state_path=tmp_path / "state.json",
        fetch=fetch,
        request_pause_seconds=0,
        overlap_seconds=0,
        response_cap=3,
        clock=lambda: now,
    )
    first, _ = asyncio.run(poller.poll())
    # The boundary-timestamp fill (tid=3) is dropped from the truncated page but
    # the high-water advances to it, so the next poll re-enters at the boundary.
    assert [fill["tid"] for fill in first] == [1, 2]
    assert poller.highwater[ADDRESS_A] == base_ms + 3_000
    state = json.loads((tmp_path / "state.json").read_text(encoding="utf-8"))
    assert state["last_poll_complete"] is False
    assert state["per_wallet"][ADDRESS_A]["last_response_capped"] is True
    assert state["per_wallet"][ADDRESS_A]["last_response_complete"] is False
    assert state["poll_error_count"] == 0

    second, _ = asyncio.run(poller.poll())
    assert [fill["tid"] for fill in second] == [3, 4]
    state = json.loads((tmp_path / "state.json").read_text(encoding="utf-8"))
    assert state["last_poll_complete"] is True
    assert state["per_wallet"][ADDRESS_A]["last_response_capped"] is False


def test_capped_single_timestamp_page_is_an_error_not_data_loss(tmp_path: Path) -> None:
    cohort_path = tmp_path / "cohort.json"
    _write_cohort(cohort_path)
    cohort = load_wallet_flow_cohort(cohort_path)
    base_ms = int(START.timestamp() * 1000)
    now = datetime(2026, 8, 9, 0, 5, tzinfo=UTC)

    def fetch(payload):
        if payload["user"] != ADDRESS_A:
            return []
        return [
            _fill(trade_id=index, timestamp_ms=base_ms + 1_000) for index in range(3)
        ]

    poller = HyperliquidWalletFlowPoller(
        cohort=cohort,
        source_root=tmp_path / "raw" / "hyperliquid_wallet_flow",
        state_path=tmp_path / "state.json",
        fetch=fetch,
        request_pause_seconds=0,
        response_cap=3,
        clock=lambda: now,
    )
    emitted, _ = asyncio.run(poller.poll())
    assert emitted == []
    assert ADDRESS_A not in poller.highwater
    state = json.loads((tmp_path / "state.json").read_text(encoding="utf-8"))
    assert state["poll_error_count"] == 1
    assert "single-timestamp" in state["per_wallet"][ADDRESS_A]["last_error"]


def test_scan_partial_run_prevents_cross_run_duplicate(tmp_path: Path) -> None:
    source_root = tmp_path / "hyperliquid_wallet_flow"
    events = source_root / "20260809_000100" / "clean" / "events.jsonl"
    events.parent.mkdir(parents=True)
    trade_key = f"{ADDRESS_A}:99"
    events.write_text(
        json.dumps(
            {
                "trade_id": trade_key,
                "metadata": {
                    "wallet": ADDRESS_A,
                    "hyperliquid_timestamp_ms": int(START.timestamp() * 1000) + 1_000,
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    seen, highwater = scan_durable_wallet_fills(
        source_root,
        prospective_start_at=START,
    )
    assert seen == {trade_key}
    assert highwater[ADDRESS_A] == int(START.timestamp() * 1000) + 1_000


def test_cli_and_ops_thread_every_wallet_flow_field(tmp_path: Path, monkeypatch) -> None:
    assert "hyperliquid-wallet-flow-worker" in COLLECTOR_JOB_TYPES
    cohort_path = tmp_path / "cohort.json"
    _write_cohort(cohort_path)
    args = _job_args(
        SimpleNamespace(
            job_type="hyperliquid-wallet-flow-worker",
            args={
                "cohort_path": str(cohort_path),
                "poll_interval_seconds": 45.0,
                "request_pause_seconds": 0.25,
                "overlap_seconds": 600.0,
                "response_cap": 1234,
                "segment_count": 777,
                "max_segments": 1,
                "cooldown_seconds": 0.0,
                "output_root": str(tmp_path / "raw"),
                "ops_root": str(tmp_path / "ops"),
                "worker_name": "hl-test",
                "max_delay_ms": 456000,
                "max_future_skew_ms": 4000,
                "max_clock_skew_ms": 456000.0,
                "jsonl_fsync": False,
                "normalized_parquet": False,
            },
        )
    )
    captured = {}

    async def fake(segment_args):
        for name in (
            "cohort_path",
            "poll_interval_seconds",
            "request_pause_seconds",
            "overlap_seconds",
            "response_cap",
            "max_delay_ms",
            "max_future_skew_ms",
            "max_clock_skew_ms",
            "jsonl_fsync",
            "normalized_parquet",
        ):
            captured[name] = getattr(segment_args, name)
        return {"run_path": str(tmp_path / "run"), "clean_events": 0, "replayable": False}

    monkeypatch.setattr(cli, "collect_hyperliquid_wallet_flow_segment", fake)
    cli.run_hyperliquid_wallet_flow_worker(args)
    assert captured == {
        "cohort_path": cohort_path,
        "poll_interval_seconds": 45.0,
        "request_pause_seconds": 0.25,
        "overlap_seconds": 600.0,
        "response_cap": 1234,
        "max_delay_ms": 456000,
        "max_future_skew_ms": 4000,
        "max_clock_skew_ms": 456000.0,
        "jsonl_fsync": False,
        "normalized_parquet": False,
    }


def _write_clean_run(run_dir: Path, rows: list[dict]) -> None:
    events = run_dir / "clean" / "events.jsonl"
    events.parent.mkdir(parents=True)
    events.write_text(
        "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
    )


def _clean_row(
    *, wallet: str | None, event_ms: int, received_ms: int | None = None
) -> dict:
    event_at = datetime.fromtimestamp(event_ms / 1000, tz=UTC)
    received_at = datetime.fromtimestamp(
        (received_ms if received_ms is not None else event_ms) / 1000, tz=UTC
    )
    metadata = {"hyperliquid_timestamp_ms": event_ms}
    if wallet is not None:
        metadata["wallet"] = wallet
    return {
        "source": "hyperliquid",
        "product": "BTC",
        "trade_id": f"{wallet}:{event_ms}",
        "exchange_time": event_at.isoformat(),
        "received_at": received_at.isoformat(),
        "price": 100000.0,
        "size": 0.01,
        "metadata": metadata,
    }


def test_wallet_flow_replay_orders_per_wallet_not_globally(tmp_path: Path) -> None:
    from crypto_collector.replay import replay_wallet_flow_run

    base_ms = int(START.timestamp() * 1000)
    run_dir = tmp_path / "20260817_000001"
    # Wallet B's catch-up fills are older than wallet A's live fills — globally
    # non-monotonic, but monotonic within each wallet: structurally clean.
    _write_clean_run(
        run_dir,
        [
            _clean_row(wallet=ADDRESS_A, event_ms=base_ms + 500_000),
            _clean_row(wallet=ADDRESS_A, event_ms=base_ms + 600_000),
            _clean_row(wallet=ADDRESS_B, event_ms=base_ms + 1_000),
            _clean_row(wallet=ADDRESS_B, event_ms=base_ms + 2_000),
        ],
    )
    summary = replay_wallet_flow_run(run_dir)
    assert summary.replayable is True
    assert summary.findings == []
    assert summary.mode == "wallet_flow_none_native"
    assert (run_dir / "metrics" / "replay_summary.json").exists()


def test_wallet_flow_replay_fails_within_wallet_regression_and_missing_wallet(
    tmp_path: Path,
) -> None:
    from crypto_collector.replay import replay_wallet_flow_run

    base_ms = int(START.timestamp() * 1000)
    regressed = tmp_path / "20260817_000002"
    _write_clean_run(
        regressed,
        [
            _clean_row(wallet=ADDRESS_A, event_ms=base_ms + 2_000),
            _clean_row(wallet=ADDRESS_A, event_ms=base_ms + 1_000),
        ],
    )
    summary = replay_wallet_flow_run(regressed)
    assert summary.replayable is False
    assert "non_monotonic_event_time" in summary.findings

    unattributed = tmp_path / "20260817_000003"
    _write_clean_run(unattributed, [_clean_row(wallet=None, event_ms=base_ms + 1_000)])
    summary = replay_wallet_flow_run(unattributed)
    assert summary.replayable is False
    assert "missing_wallet_metadata" in summary.findings


def test_wallet_flow_replay_clock_skew_gate(tmp_path: Path) -> None:
    from crypto_collector.replay import replay_wallet_flow_run

    base_ms = int(START.timestamp() * 1000)
    run_dir = tmp_path / "20260817_000004"
    # A fill refetched 7 days after the event: passes the lane default gate,
    # fails a 60s gate.
    _write_clean_run(
        run_dir,
        [
            _clean_row(
                wallet=ADDRESS_A,
                event_ms=base_ms + 1_000,
                received_ms=base_ms + 1_000 + 7 * 86_400_000,
            )
        ],
    )
    assert replay_wallet_flow_run(run_dir).replayable is True
    summary = replay_wallet_flow_run(run_dir, max_clock_skew_ms=60_000.0)
    assert summary.replayable is False
    assert "excessive_clock_skew" in summary.findings


def test_backfill_trades_replay_threads_wallet_flow_scorer(
    tmp_path: Path, monkeypatch
) -> None:
    args = _job_args(
        SimpleNamespace(
            job_type="backfill-trades-replay",
            args={
                "source_root": str(tmp_path),
                "wallet_flow": True,
                "max_clock_skew_ms": 123_000.0,
                "max_age_hours": 336,
                "limit": 10,
                "format": "json",
            },
        )
    )
    assert args.wallet_flow is True
    assert args.max_clock_skew_ms == 123_000.0

    base_ms = int(START.timestamp() * 1000)
    run_dir = tmp_path / datetime.now(tz=UTC).strftime("%Y%m%d_%H%M%S")
    _write_clean_run(
        run_dir,
        [
            _clean_row(wallet=ADDRESS_A, event_ms=base_ms + 500_000),
            _clean_row(wallet=ADDRESS_B, event_ms=base_ms + 1_000),
        ],
    )
    cli.run_backfill_trades_replay(args)
    summary = json.loads(
        (run_dir / "metrics" / "replay_summary.json").read_text(encoding="utf-8")
    )
    # Per-wallet scorer verdict (the stream scorer would have failed this run
    # as globally non-monotonic), with the threaded skew gate recorded.
    assert summary["mode"] == "wallet_flow_none_native"
    assert summary["replayable"] is True
