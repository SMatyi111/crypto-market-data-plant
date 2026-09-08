"""Suite-wide hermeticity guard.

Every `default_*_root()` in `crypto_collector.config` falls back to the LIVE
archive (`G:\\market_archive`) when no env override is set. Tests that build args
without an explicit root - the `run_ops_runner` tests dispatching `mock` jobs, the
health tests reading a default normalized root - therefore wrote into the
production tree: the 2026-09-07 audit counted 56 test-debris run dirs under
`raw/market/mock/`, +2 per `pytest` run, and a permanent `unconfigured_lane:mock`
offload warning. CLAUDE.md requires hermetic tests; this fixture makes that true
by construction instead of by every test remembering to pass a root.

Order of precedence in config.py: per-root env (OUTPUT/NORMALIZED/CURATED/OPS)
beats MARKET_DATA_ARCHIVE_ROOT, which beats the DEFAULT_* constants. So this
fixture (1) clears any per-root override inherited from the developer's shell,
and (2) points the archive root at a per-test temp dir. Tests that exercise the
env chain itself set or clear these variables on top via `monkeypatch`.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

PER_ROOT_OVERRIDES = (
    "MARKET_DATA_OUTPUT_ROOT",
    "CRYPTO_COLLECTOR_OUTPUT_ROOT",
    "MARKET_DATA_NORMALIZED_ROOT",
    "CRYPTO_COLLECTOR_NORMALIZED_ROOT",
    "MARKET_DATA_CURATED_ROOT",
    "CRYPTO_COLLECTOR_CURATED_ROOT",
    "MARKET_DATA_OPS_ROOT",
    "CRYPTO_COLLECTOR_OPS_ROOT",
    "CRYPTO_COLLECTOR_ARCHIVE_ROOT",
)


def apply_hermetic_env(monkeypatch: pytest.MonkeyPatch, root: Path) -> None:
    """Scrub inherited per-root overrides, then pin the archive root at `root`.

    Kept as a plain function (not only a fixture body) so a test can exercise it
    against a deliberately polluted environment - the autouse fixture has already
    run by the time a test body executes, which would otherwise make the scrubbing
    loop untestable.
    """
    for name in PER_ROOT_OVERRIDES:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("MARKET_DATA_ARCHIVE_ROOT", str(root))


@pytest.fixture(autouse=True)
def hermetic_archive_root(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Route every implicit archive path at a per-test temp dir.

    Returns the root so a test can assert against it. The dir is NOT pre-created:
    `prepare_run_paths` and friends mkdir on first write, exactly as they do on a
    fresh production disk.
    """
    root = tmp_path / "hermetic_archive"
    apply_hermetic_env(monkeypatch, root)
    return root


@pytest.fixture
def hermetic_env() -> SimpleNamespace:
    """Expose the guard's parts to the test that pins them (tests/test_config.py)."""
    return SimpleNamespace(apply=apply_hermetic_env, overrides=PER_ROOT_OVERRIDES)
