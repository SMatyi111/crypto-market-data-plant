"""backfill-wallet-flow-from-node: recover a cohort wallet's fills from the local
node archive into a normal lane run that the existing score/promote chain lands.

Fixtures reproduce the live shape in miniature: a hive-partitioned
`date=YYYYMMDD/hour=H.parquet` archive with the extractor's string-decimal schema,
a frozen two-wallet cohort, and a lane raw root that already holds some durable
rows (the dedup source). The tool must take only the cohort's target coins for the
named wallet, skip rows already durable, mark provenance via `raw_type`, keep
event time untouched, and be idempotent.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest

from crypto_collector import cli
from crypto_collector.promotion import promote_replayable_runs
from crypto_collector.wallet_flow_backfill import (
    BACKFILL_RAW_TYPE,
    backfill_wallet_flow_from_node,
    node_date_dirs,
)

ADDRESS_A = "0x" + "a1" * 20
ADDRESS_B = "0x" + "b2" * 20
START = datetime(2026, 8, 9, 0, 45, 48, tzinfo=UTC)
NOW = datetime(2026, 9, 14, 13, 0, tzinfo=UTC)


def _write_cohort(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "prospective_start_at": START.isoformat(),
                "target_coins": ["BTC", "ETH", "SOL"],
                "wallets": [
                    {"address": ADDRESS_A, "candidate_rank": 18, "cohort_rank": 1},
                    {"address": ADDRESS_B, "candidate_rank": 5, "cohort_rank": 2},
                ],
            }
        ),
        encoding="utf-8",
    )


def _node_row(*, user: str, tid: int, at: datetime, coin: str = "BTC", liquidation: str | None = None) -> dict:
    return {
        "block_number": 1_000 + tid,
        "block_time": at.isoformat(),
        "local_time": at.isoformat(),
        "user": user,
        "coin": coin,
        "px": "65000.5",
        "sz": "0.25",
        "side": "B" if tid % 2 else "A",
        "time": int(at.timestamp() * 1000),
        "startPosition": "1.5",
        "dir": "Open Long",
        "closedPnl": "0.0",
        "hash": f"0x{tid:064x}",
        "oid": 500 + tid,
        "crossed": True,
        "fee": "-0.01",
        "tid": tid,
        "feeToken": "USDC",
        "cloid": None,
        "builder": None,
        "builderFee": None,
        "liquidation": liquidation,
        "extra_json": None,
    }


_SCHEMA = pa.schema(
    [
        ("block_number", pa.int64()),
        ("block_time", pa.string()),
        ("local_time", pa.string()),
        ("user", pa.string()),
        ("coin", pa.string()),
        ("px", pa.string()),
        ("sz", pa.string()),
        ("side", pa.string()),
        ("time", pa.int64()),
        ("startPosition", pa.string()),
        ("dir", pa.string()),
        ("closedPnl", pa.string()),
        ("hash", pa.string()),
        ("oid", pa.int64()),
        ("crossed", pa.bool_()),
        ("fee", pa.string()),
        ("tid", pa.int64()),
        ("feeToken", pa.string()),
        ("cloid", pa.string()),
        ("builder", pa.string()),
        ("builderFee", pa.string()),
        ("liquidation", pa.string()),
        ("extra_json", pa.string()),
    ]
)


def _write_node_archive(root: Path, rows: list[dict]) -> None:
    by_file: dict[tuple[str, int], list[dict]] = {}
    for row in rows:
        at = datetime.fromtimestamp(row["time"] / 1000, tz=UTC)
        by_file.setdefault((at.strftime("%Y%m%d"), at.hour), []).append(row)
    for (day, hour), group in by_file.items():
        target = root / f"date={day}" / f"hour={hour}.parquet"
        target.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(group, schema=_SCHEMA), target)


def _write_durable_run(source_root: Path, *, wallet: str, tid: int, at: datetime) -> None:
    run_dir = source_root / (at - timedelta(minutes=1)).strftime("%Y%m%d_%H%M%S")
    (run_dir / "clean").mkdir(parents=True, exist_ok=True)
    row = {
        "source": "hyperliquid",
        "exchange_time": at.isoformat(),
        "received_at": (at + timedelta(seconds=30)).isoformat(),
        "trade_id": f"{wallet}:{tid}",
        "metadata": {"wallet": wallet, "hyperliquid_timestamp_ms": int(at.timestamp() * 1000)},
    }
    (run_dir / "clean" / "events.jsonl").write_text(json.dumps(row) + "\n", encoding="utf-8")


GAP_START = datetime(2026, 8, 25, 0, 47, 3, tzinfo=UTC)


def _setup(tmp_path: Path):
    cohort_path = tmp_path / "cohort.json"
    _write_cohort(cohort_path)
    node_root = tmp_path / "node_fills_by_block"
    source_root = tmp_path / "raw" / "market" / "hyperliquid_wallet_flow"
    source_root.mkdir(parents=True)
    rows = [
        # already durable in the lane -> must be skipped
        _node_row(user=ADDRESS_A, tid=1, at=GAP_START - timedelta(hours=2)),
        # the gap: two target-coin fills on different days, one with a liquidation blob
        _node_row(user=ADDRESS_A, tid=2, at=GAP_START + timedelta(minutes=1)),
        _node_row(
            user=ADDRESS_A,
            tid=4,
            at=GAP_START + timedelta(days=3, hours=5),
            coin="ETH",
            liquidation=json.dumps({"liquidatedUser": ADDRESS_B, "markPx": "64000", "method": "market"}),
        ),
        # non-target coin -> ignored
        _node_row(user=ADDRESS_A, tid=3, at=GAP_START + timedelta(minutes=2), coin="PUMP"),
        # other cohort wallet -> not this wallet's backfill
        _node_row(user=ADDRESS_B, tid=5, at=GAP_START + timedelta(minutes=3)),
        # before the prospective boundary -> ignored even if requested
        _node_row(user=ADDRESS_A, tid=6, at=START - timedelta(days=1)),
        # mixed-case address in the archive must still match
        {**_node_row(user=ADDRESS_A.upper().replace("0X", "0x"), tid=7, at=GAP_START + timedelta(days=1), coin="SOL")},
    ]
    _write_node_archive(node_root, rows)
    _write_durable_run(source_root, wallet=ADDRESS_A, tid=1, at=GAP_START - timedelta(hours=2))
    return cohort_path, node_root, source_root


def test_node_date_dirs_selects_inclusive_day_range(tmp_path):
    root = tmp_path / "node"
    for day in ("20260824", "20260825", "20260901", "20260915"):
        (root / f"date={day}").mkdir(parents=True)
    (root / "derived").mkdir()
    start = int(datetime(2026, 8, 25, 0, 47, tzinfo=UTC).timestamp() * 1000)
    end = int(datetime(2026, 9, 1, 23, 59, tzinfo=UTC).timestamp() * 1000)
    assert [p.name for p in node_date_dirs(root, start_ms=start, end_ms=end)] == [
        "date=20260825",
        "date=20260901",
    ]


def test_dry_run_reports_only_missing_target_fills_and_writes_nothing(tmp_path):
    cohort_path, node_root, source_root = _setup(tmp_path)
    report = backfill_wallet_flow_from_node(
        node_root=node_root,
        cohort_path=cohort_path,
        source_root=source_root,
        wallet=ADDRESS_A,
        start_ms=int((START - timedelta(days=5)).timestamp() * 1000),  # clamped to the boundary
        now=NOW,
    )
    assert report.mode == "dry-run" and report.status == "ok"
    assert report.start_ms == int(START.timestamp() * 1000)
    assert report.node_fill_count == 5  # tids 1,2,3,4,7 (6 is before the boundary, 5 is wallet B)
    assert report.target_fill_count == 4  # PUMP excluded
    assert report.already_durable_count == 1
    assert report.missing_count == 3
    assert report.missing_per_coin == {"BTC": 1, "ETH": 1, "SOL": 1}
    assert report.missing_per_day == {"2026-08-25": 1, "2026-08-26": 1, "2026-08-28": 1}
    assert report.run_path is None and report.written_rows == 0
    assert sorted(p.name for p in source_root.iterdir()) == [
        (GAP_START - timedelta(hours=2, minutes=1)).strftime("%Y%m%d_%H%M%S")
    ]


def test_apply_writes_a_lane_run_the_promoter_lands_with_provenance(tmp_path):
    cohort_path, node_root, source_root = _setup(tmp_path)
    report = backfill_wallet_flow_from_node(
        node_root=node_root, cohort_path=cohort_path, source_root=source_root,
        wallet=ADDRESS_A, apply=True, now=NOW,
    )
    assert report.mode == "apply" and report.status == "ok"
    assert report.written_rows == 3 and report.replayable is True and report.replay_findings == []
    run_dir = Path(report.run_path)
    assert run_dir.parent == source_root and run_dir.name == NOW.strftime("%Y%m%d_%H%M%S")

    raw = [json.loads(x) for x in (run_dir / "raw" / "messages.jsonl").read_text(encoding="utf-8").splitlines()]
    clean = [json.loads(x) for x in (run_dir / "clean" / "events.jsonl").read_text(encoding="utf-8").splitlines()]
    assert [r["payload"]["tid"] for r in raw] == [2, 7, 4]  # event-time order
    assert all(r["received_at"] == NOW.isoformat() and r["payload"]["_capture_source"] == BACKFILL_RAW_TYPE for r in raw)
    assert raw[2]["payload"]["liquidation"] == {"liquidatedUser": ADDRESS_B, "markPx": "64000", "method": "market"}
    assert [c["trade_id"] for c in clean] == [f"{ADDRESS_A}:2", f"{ADDRESS_A}:7", f"{ADDRESS_A}:4"]
    assert all(c["raw_type"] == BACKFILL_RAW_TYPE for c in clean)
    assert all(c["metadata"]["wallet"] == ADDRESS_A and c["metadata"]["cohort_rank"] == 1 for c in clean)
    assert clean[0]["exchange_time"] == (GAP_START + timedelta(minutes=1)).isoformat()
    assert clean[0]["received_at"] == NOW.isoformat()
    assert clean[1]["product"] == "SOL" and clean[1]["metadata"]["instrument_id"] == "perp:hyperliquid:SOLUSDC"
    # the same metadata keys a polled row carries - no new struct children in curated
    assert set(clean[0]["metadata"]) <= {
        "instrument_id", "canonical_symbol", "wallet", "candidate_rank", "cohort_rank", "cohort_sha256",
        "prospective_start_at", "hyperliquid_timestamp_ms", "hyperliquid_trade_id", "hyperliquid_order_id",
        "transaction_hash", "direction", "start_position", "closed_pnl", "crossed", "fee", "fee_token",
    }

    replay = json.loads((run_dir / "metrics" / "replay_summary.json").read_text(encoding="utf-8"))
    assert replay["replayable"] is True and replay["mode"] == "wallet_flow_none_native" and replay["event_count"] == 3
    summary = [json.loads(x) for x in (run_dir / "metrics" / "summary.jsonl").read_text(encoding="utf-8").splitlines()]
    assert summary[-1]["raw_messages"] == 3 and summary[-1]["clean_events"] == 3
    assert summary[-1]["capture_source"] == BACKFILL_RAW_TYPE and summary[-1]["already_durable_count"] == 1

    # The ordinary promoter lands it; curated rows keep the provenance marker and
    # partition by the true fill date, not the backfill date.
    target_root = tmp_path / "curated" / "trades_replayable"
    promo = promote_replayable_runs(source_root, target_root, limit=10, max_age_hours=24 * 365 * 100)
    assert promo.promoted_row_count == 3
    curated = ds.dataset(target_root, format="parquet", partitioning="hive").to_table().to_pylist()
    assert sorted(r["raw_type"] for r in curated) == [BACKFILL_RAW_TYPE] * 3
    assert sorted(str(r["event_date"]) for r in curated) == ["2026-08-25", "2026-08-26", "2026-08-28"]

    # Idempotent: the run's rows are now durable, so a second pass finds nothing.
    again = backfill_wallet_flow_from_node(
        node_root=node_root, cohort_path=cohort_path, source_root=source_root,
        wallet=ADDRESS_A, apply=True, now=NOW + timedelta(minutes=5),
    )
    assert again.missing_count == 0 and again.status == "nothing_to_backfill"
    assert again.already_durable_count == 4 and again.run_path is None
    assert len([p for p in source_root.iterdir() if p.is_dir()]) == 2


def test_window_end_bounds_the_backfill(tmp_path):
    cohort_path, node_root, source_root = _setup(tmp_path)
    report = backfill_wallet_flow_from_node(
        node_root=node_root, cohort_path=cohort_path, source_root=source_root,
        wallet=ADDRESS_A, end_ms=int((GAP_START + timedelta(hours=1)).timestamp() * 1000), now=NOW,
    )
    assert report.missing_count == 1 and report.missing_per_coin == {"BTC": 1}


def test_wallet_outside_cohort_is_refused(tmp_path):
    cohort_path, node_root, source_root = _setup(tmp_path)
    with pytest.raises(ValueError, match="not a member"):
        backfill_wallet_flow_from_node(
            node_root=node_root, cohort_path=cohort_path, source_root=source_root,
            wallet="0x" + "9" * 40, now=NOW,
        )


def test_cli_handler_defaults_to_every_cohort_wallet_dry_run(tmp_path, capsys):
    cohort_path, node_root, source_root = _setup(tmp_path)
    cli.run_backfill_wallet_flow_from_node(
        SimpleNamespace(
            node_root=node_root, cohort_path=cohort_path, source_root=source_root,
            wallet=None, start=None, end=None, apply=False, format="json",
        )
    )
    reports = json.loads(capsys.readouterr().out)
    assert [r["wallet"] for r in reports] == [ADDRESS_A, ADDRESS_B]
    assert [r["missing_count"] for r in reports] == [3, 1]
    assert all(r["mode"] == "dry-run" and r["run_path"] is None for r in reports)
    assert not any(p.is_dir() and p.name.startswith("2026091") for p in source_root.iterdir())


def test_cli_handler_text_output_and_iso_window(tmp_path, capsys):
    cohort_path, node_root, source_root = _setup(tmp_path)
    cli.run_backfill_wallet_flow_from_node(
        SimpleNamespace(
            node_root=node_root, cohort_path=cohort_path, source_root=source_root,
            wallet=[ADDRESS_A], start=GAP_START.isoformat(),
            end=(GAP_START + timedelta(hours=1)).isoformat(), apply=False, format="text",
        )
    )
    out = capsys.readouterr().out
    assert "mode=dry-run" in out and ADDRESS_A in out and "missing=1" in out
