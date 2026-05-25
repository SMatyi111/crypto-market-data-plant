from __future__ import annotations

from pathlib import Path

from crypto_collector import config


def test_default_output_root_uses_archive_when_present(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("CRYPTO_COLLECTOR_OUTPUT_ROOT", raising=False)
    archive_root = tmp_path / "archive-root"
    archive_root.mkdir()
    monkeypatch.setattr(config, "DEFAULT_ARCHIVE_ROOT", archive_root)
    monkeypatch.setattr(config, "DEFAULT_MARKET_OUTPUT_ROOT", archive_root / "raw" / "market")
    assert config.default_output_root() == archive_root / "raw" / "market"


def test_default_output_root_uses_env_override(monkeypatch) -> None:
    monkeypatch.setenv("CRYPTO_COLLECTOR_OUTPUT_ROOT", r"D:\custom_output")
    assert config.default_output_root() == Path(r"D:\custom_output")


def test_default_curated_root_uses_env_override(monkeypatch) -> None:
    monkeypatch.setenv("CRYPTO_COLLECTOR_CURATED_ROOT", r"D:\curated_root")
    assert config.default_curated_root("market_replayable") == Path(r"D:\curated_root") / "market_replayable"


def test_default_normalized_root_prefers_archive_root_when_available(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("CRYPTO_COLLECTOR_NORMALIZED_ROOT", raising=False)
    archive_root = tmp_path / "archive-root"
    archive_root.mkdir()
    monkeypatch.setattr(config, "DEFAULT_ARCHIVE_ROOT", archive_root)
    monkeypatch.setattr(config, "DEFAULT_NORMALIZED_ROOT", archive_root / "normalized")
    assert config.default_normalized_root("market") == archive_root / "normalized" / "market"


def test_default_ops_root_prefers_archive_root_when_available(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("CRYPTO_COLLECTOR_OPS_ROOT", raising=False)
    archive_root = tmp_path / "archive-root"
    archive_root.mkdir()
    monkeypatch.setattr(config, "DEFAULT_ARCHIVE_ROOT", archive_root)
    monkeypatch.setattr(config, "DEFAULT_OPS_ROOT", archive_root / "ops")
    assert config.default_ops_root() == archive_root / "ops"
