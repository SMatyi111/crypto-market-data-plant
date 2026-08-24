from __future__ import annotations

import gzip
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

import crypto_collector.cli as cli
from crypto_collector.cli import _job_args
from crypto_collector.collectors.hyperliquid_leaderboard import (
    SOURCE_NAME,
    snapshot_leaderboard,
)
from crypto_collector.ops import COLLECTOR_JOB_TYPES


NOW = datetime(2026, 8, 17, 15, 0, 0, tzinfo=UTC)


def _payload(rows: int = 3) -> bytes:
    return json.dumps(
        {
            "leaderboardRows": [
                {
                    "ethAddress": "0x" + f"{index:040x}",
                    "accountValue": "1000.0",
                    "windowPerformances": [
                        ["day", {"pnl": "1", "roi": "0.01", "vlm": "100"}],
                        ["month", {"pnl": "30", "roi": "0.3", "vlm": "3000"}],
                    ],
                    "prize": 0,
                    "displayName": None,
                }
                for index in range(rows)
            ]
        }
    ).encode("utf-8")


def test_snapshot_archives_raw_bytes_verbatim(tmp_path: Path) -> None:
    raw = _payload(rows=5)
    result = snapshot_leaderboard(tmp_path, fetch=lambda: raw, clock=lambda: NOW)
    assert result.row_count == 5
    assert result.parse_ok is True
    assert result.sha256 == hashlib.sha256(raw).hexdigest()

    run_dir = tmp_path / SOURCE_NAME / "20260817_150000"
    assert Path(result.run_path) == run_dir
    with gzip.open(run_dir / "raw" / "leaderboard.json.gz", "rb") as handle:
        assert handle.read() == raw

    summary = json.loads((run_dir / "metrics" / "summary.json").read_text(encoding="utf-8"))
    assert summary["row_count"] == 5
    assert summary["sha256"] == result.sha256
    assert summary["fetched_at"] == NOW.isoformat()


def test_snapshot_fails_job_but_preserves_malformed_raw(tmp_path: Path) -> None:
    raw = b"<html>upstream error page</html>"
    with pytest.raises(ValueError, match="failed validation"):
        snapshot_leaderboard(tmp_path, fetch=lambda: raw, clock=lambda: NOW)
    # The evidence is archived even though the job fails.
    run_dir = tmp_path / SOURCE_NAME / "20260817_150000"
    with gzip.open(run_dir / "raw" / "leaderboard.json.gz", "rb") as handle:
        assert handle.read() == raw
    summary = json.loads((run_dir / "metrics" / "summary.json").read_text(encoding="utf-8"))
    assert summary["parse_ok"] is False


def test_empty_leaderboard_is_a_failure_not_a_quiet_success(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="failed validation"):
        snapshot_leaderboard(tmp_path, fetch=lambda: _payload(rows=0), clock=lambda: NOW)


def test_ops_job_registration_and_arg_threading(tmp_path: Path, monkeypatch) -> None:
    assert "hyperliquid-leaderboard-snapshot" in COLLECTOR_JOB_TYPES
    args = _job_args(
        SimpleNamespace(
            job_type="hyperliquid-leaderboard-snapshot",
            args={"output_root": str(tmp_path / "raw"), "format": "json"},
        )
    )
    assert args.output_root == Path(tmp_path / "raw")
    assert args.format == "json"

    captured = {}

    def fake(output_root):
        captured["output_root"] = output_root
        return SimpleNamespace(
            run_path="r",
            snapshot_path="s",
            fetched_at=NOW.isoformat(),
            raw_bytes=10,
            sha256="x",
            row_count=3,
            parse_ok=True,
        )

    monkeypatch.setattr(cli, "snapshot_leaderboard", fake)
    cli.run_hyperliquid_leaderboard_snapshot(args)
    assert captured["output_root"] == Path(tmp_path / "raw")
