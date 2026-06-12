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


def test_default_output_root_emits_warning_when_no_env_override_is_set(monkeypatch, recwarn) -> None:
    monkeypatch.delenv("MARKET_DATA_OUTPUT_ROOT", raising=False)
    monkeypatch.delenv("CRYPTO_COLLECTOR_OUTPUT_ROOT", raising=False)
    config._FALLBACK_WARNED.clear()
    config.default_output_root()
    fallback_warnings = [w for w in recwarn.list if "MARKET_DATA_OUTPUT_ROOT" in str(w.message)]
    assert fallback_warnings


def test_default_output_root_does_not_warn_when_env_override_is_set(monkeypatch, recwarn) -> None:
    monkeypatch.setenv("CRYPTO_COLLECTOR_OUTPUT_ROOT", r"D:\custom_output")
    config._FALLBACK_WARNED.clear()
    config.default_output_root()
    fallback_warnings = [w for w in recwarn.list if "MARKET_DATA_OUTPUT_ROOT" in str(w.message)]
    assert fallback_warnings == []


def test_default_ops_root_prefers_archive_root_when_available(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("CRYPTO_COLLECTOR_OPS_ROOT", raising=False)
    archive_root = tmp_path / "archive-root"
    archive_root.mkdir()
    monkeypatch.setattr(config, "DEFAULT_ARCHIVE_ROOT", archive_root)
    monkeypatch.setattr(config, "DEFAULT_OPS_ROOT", archive_root / "ops")
    assert config.default_ops_root() == archive_root / "ops"


def test_configured_archive_root_is_honored_even_when_missing(tmp_path: Path, monkeypatch) -> None:
    """Regression: a configured-but-not-yet-mounted archive root used to silently
    re-route every derived root to the default disk (existence probe), while
    default_archive_root() kept returning the configured path — a split-brain where
    collectors wrote one tree and the manifest read another. An EXPLICIT env root is
    honored unconditionally; a missing drive fails loudly at first write instead."""
    missing = tmp_path / "not-mounted-yet" / "archive"
    monkeypatch.setenv("MARKET_DATA_ARCHIVE_ROOT", str(missing))
    monkeypatch.delenv("MARKET_DATA_OUTPUT_ROOT", raising=False)
    monkeypatch.delenv("CRYPTO_COLLECTOR_OUTPUT_ROOT", raising=False)

    assert config.default_archive_root() == missing
    assert config.default_output_root() == missing / "raw" / "market"
    assert config.default_normalized_root("market") == missing / "normalized" / "market"
    assert config.default_curated_root("trades") == missing / "curated" / "research" / "trades"
    assert config.default_ops_root() == missing / "ops"
