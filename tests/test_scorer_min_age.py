"""Live-segment floor for the two DEPTH scorers.

The 2026-09-07 ops audit measured ~1/3 of promoted depth runs truncated: the hourly
`backfill-replay` / `backfill-stream-depth` jobs scored the 1800 s segment the
collector was still writing, the 300 s promoter indexed the partial rows, and the
run-keyed promotion index never revisited it. PR #54 gave the trades/text scorers a
`min_age_hours` floor (default 1 h); these tests pin the same floor on the depth
paths - both the ops-runner dispatch defaults and the scorer behaviour itself.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

from crypto_collector.cli import _job_args, build_parser, run_backfill_replay, run_backfill_stream_depth
from crypto_collector.ops import JobSpec


def _run_name(started_at: datetime) -> str:
    return started_at.strftime("%Y%m%d_%H%M%S")


def _write_binance_depth_run(run_path: Path) -> None:
    clean_path = run_path / "clean"
    clean_path.mkdir(parents=True)
    (clean_path / "events.jsonl").write_text(
        json.dumps(
            {
                "source": "binance",
                "product": "BTCUSDT",
                "event_time": "2026-04-06T00:00:00+00:00",
                "received_at": "2026-04-06T00:00:00.100000+00:00",
                "first_update_id": 1,
                "final_update_id": 1,
                "instrument": {"instrument_id": "spot:binance:BTCUSDT"},
                "bids": [[100.0, 1.0]],
                "asks": [[101.0, 1.0]],
            }
        )
        + "\n",
        encoding="utf-8",
    )


def _write_bybit_stream_run(run_path: Path) -> None:
    clean_path = run_path / "clean"
    clean_path.mkdir(parents=True)
    rows = [
        {
            "source": "bybit",
            "product": "BTCUSDT",
            "event_type": "snapshot",
            "event_time": "2026-04-06T00:00:00+00:00",
            "received_at": "2026-04-06T00:00:00.100000+00:00",
            "sequence": 100,
            "update_id": 100,
            "instrument": {"instrument_id": "spot:bybit:BTCUSDT"},
            "bids": [[100.0, 1.0]],
            "asks": [[101.0, 2.0]],
            "metadata": {"u": 100, "seq": 100},
        },
        {
            "source": "bybit",
            "product": "BTCUSDT",
            "event_type": "delta",
            "event_time": "2026-04-06T00:00:01+00:00",
            "received_at": "2026-04-06T00:00:01.100000+00:00",
            "sequence": 101,
            "update_id": 101,
            "instrument": {"instrument_id": "spot:bybit:BTCUSDT"},
            "bids": [],
            "asks": [[101.0, 1.5]],
            "metadata": {"u": 101, "seq": 101},
        },
    ]
    (clean_path / "events.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
    )


# --- dispatch: the ops runner must hand the floor to both depth scorers ----------


def test_job_args_backfill_replay_defaults_min_age_1h_and_survives_override() -> None:
    defaults = _job_args(JobSpec(name="x", job_type="backfill-replay", interval_seconds=3600, args={}))
    assert defaults.min_age_hours == 1.0

    overridden = _job_args(
        JobSpec(name="x", job_type="backfill-replay", interval_seconds=3600, args={"min_age_hours": 2.5})
    )
    assert overridden.min_age_hours == 2.5


def test_job_args_backfill_stream_depth_defaults_min_age_1h_and_survives_override() -> None:
    defaults = _job_args(JobSpec(name="x", job_type="backfill-stream-depth", interval_seconds=3600, args={}))
    assert defaults.min_age_hours == 1.0
    # The floor must not disturb the single-promoter default.
    assert defaults.score_only is True

    overridden = _job_args(
        JobSpec(name="x", job_type="backfill-stream-depth", interval_seconds=3600, args={"min_age_hours": 0})
    )
    assert overridden.min_age_hours == 0


def test_cli_parsers_expose_min_age_hours_with_1h_default() -> None:
    parser = build_parser()
    depth = parser.parse_args(["backfill-replay"])
    assert depth.min_age_hours == 1.0
    stream = parser.parse_args(["backfill-stream-depth", "--min-age-hours", "0.5"])
    assert stream.min_age_hours == 0.5


# --- behaviour: a run younger than the floor is not scored ------------------------


def test_backfill_replay_skips_live_run_until_floor_disabled(tmp_path: Path, capsys) -> None:
    source_root = tmp_path / "raw" / "market" / "binance_depth"
    live_run = source_root / _run_name(datetime.now(tz=UTC) - timedelta(minutes=10))
    old_run = source_root / _run_name(datetime.now(tz=UTC) - timedelta(hours=3))
    _write_binance_depth_run(live_run)
    _write_binance_depth_run(old_run)

    def args(min_age_hours: float) -> SimpleNamespace:
        return SimpleNamespace(
            source_root=source_root,
            limit=10,
            max_age_hours=24.0,
            overwrite=False,
            min_age_hours=min_age_hours,
            format="json",
        )

    run_backfill_replay(args(1.0))
    report = json.loads(capsys.readouterr().out)
    assert report["created_count"] == 1
    assert (old_run / "metrics" / "replay_summary.json").exists()
    assert not (live_run / "metrics" / "replay_summary.json").exists()
    actions = {Path(row["run_path"]).name: row["action"] for row in report["runs"]}
    assert actions[live_run.name] == "skipped_too_recent"

    # Explicit 0 disables the floor - the operator's re-score escape hatch.
    run_backfill_replay(args(0.0))
    report = json.loads(capsys.readouterr().out)
    assert report["created_count"] == 1
    assert (live_run / "metrics" / "replay_summary.json").exists()


def test_backfill_stream_depth_skips_live_run_and_reports_it(tmp_path: Path, capsys) -> None:
    raw_root = tmp_path / "raw"
    lane = raw_root / "bybit_depth"
    live_run = lane / _run_name(datetime.now(tz=UTC) - timedelta(minutes=10))
    old_run = lane / _run_name(datetime.now(tz=UTC) - timedelta(hours=3))
    _write_bybit_stream_run(live_run)
    _write_bybit_stream_run(old_run)

    def args(min_age_hours: float) -> SimpleNamespace:
        return SimpleNamespace(
            raw_root=raw_root,
            source=["bybit_depth"],
            target_root=tmp_path / "curated",
            limit=200,
            max_age_hours=24.0,
            min_age_hours=min_age_hours,
            apply=False,
            score_only=True,
            format="json",
        )

    run_backfill_stream_depth(args(1.0))
    src = json.loads(capsys.readouterr().out)["sources"][0]
    assert src["scanned"] == 1
    assert src["skipped_too_recent"] == 1
    assert (old_run / "metrics" / "replay_summary.json").exists()
    assert not (live_run / "metrics" / "replay_summary.json").exists()

    run_backfill_stream_depth(args(0.0))
    src = json.loads(capsys.readouterr().out)["sources"][0]
    # The old run is now already scored; only the previously-live run is new work.
    assert src["scanned"] == 1
    assert src["skipped_too_recent"] == 0
    assert src["skipped_already_scored"] == 1
    assert (live_run / "metrics" / "replay_summary.json").exists()


def test_backfill_stream_depth_floor_defaults_to_1h_when_attribute_absent(tmp_path: Path, capsys) -> None:
    """Older callers build args without min_age_hours; the safe default must apply."""
    raw_root = tmp_path / "raw"
    live_run = raw_root / "bybit_depth" / _run_name(datetime.now(tz=UTC) - timedelta(minutes=5))
    _write_bybit_stream_run(live_run)
    run_backfill_stream_depth(
        SimpleNamespace(
            raw_root=raw_root,
            source=["bybit_depth"],
            target_root=tmp_path / "curated",
            limit=200,
            max_age_hours=24.0,
            apply=False,
            score_only=True,
            format="json",
        )
    )
    src = json.loads(capsys.readouterr().out)["sources"][0]
    assert src["scanned"] == 0
    assert src["skipped_too_recent"] == 1
