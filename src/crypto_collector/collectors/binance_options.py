from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from ..models import utc_now
from .raw_snapshot import (
    FetchFn,
    RawSnapshotResult,
    capture_raw_snapshot,
    fetch_bytes,
    require_safe_tokens,
)


BASE_URL = "https://eapi.binance.com"
SOURCE_NAME = "binance_options_chain"
USER_AGENT = "crypto-market-data-plant-binance-options/0.1"
DEFAULT_UNDERLYINGS = ("BTCUSDT", "ETHUSDT")


def fetch_url(url: str, *, timeout_seconds: float = 20.0) -> bytes:
    # 418 is Binance's escalation for clients that keep hammering through 429s;
    # retrying it extends the IP ban. Same rule as the fapi REST collector -
    # never retry 418.
    return fetch_bytes(
        url,
        user_agent=USER_AGENT,
        timeout_seconds=timeout_seconds,
        never_retry_codes=frozenset({418}),
    )


def _chain_payloads(underlyings: tuple[str, ...]) -> list[tuple[str, str]]:
    payloads = [
        ("exchange_info", f"{BASE_URL}/eapi/v1/exchangeInfo"),
        ("mark", f"{BASE_URL}/eapi/v1/mark"),
        ("ticker", f"{BASE_URL}/eapi/v1/ticker"),
    ]
    for underlying in underlyings:
        payloads.append((f"index_{underlying}", f"{BASE_URL}/eapi/v1/index?underlying={underlying}"))
    return payloads


def _validate_payload(
    name: str, parsed: Any, underlyings: tuple[str, ...]
) -> tuple[int | None, str | None]:
    """Return (row_count, failure_reason) for one parsed payload."""
    if name == "exchange_info":
        symbols = parsed.get("optionSymbols") if isinstance(parsed, dict) else None
        if not isinstance(symbols, list) or not symbols:
            return None, "exchange_info has no optionSymbols"
        return len(symbols), None
    if name in {"mark", "ticker"}:
        if not isinstance(parsed, list) or not parsed:
            return None, f"{name} payload is not a non-empty list"
        if name == "ticker":
            # Option symbols look like BTC-260828-60000-C; every requested
            # underlying must have at least one live contract in the payload.
            # Report ALL missing underlyings, not just the first.
            missing = [
                underlying
                for underlying in underlyings
                if not any(
                    isinstance(row, dict)
                    and str(row.get("symbol", "")).startswith(
                        underlying.removesuffix("USDT") + "-"
                    )
                    for row in parsed
                )
            ]
            if missing:
                return len(parsed), f"ticker has no contracts for {', '.join(missing)}"
        return len(parsed), None
    if name.startswith("index_"):
        if not isinstance(parsed, dict) or "indexPrice" not in parsed:
            return None, f"{name} payload has no indexPrice"
        return 1, None
    return None, f"unknown payload {name}"


def snapshot_binance_options_chain(
    output_root: Path | str,
    *,
    underlyings: tuple[str, ...] = DEFAULT_UNDERLYINGS,
    fetch: FetchFn = fetch_url,
    clock: Callable[[], Any] = utc_now,
) -> RawSnapshotResult:
    """Capture one full Binance options chain snapshot into its own run dir.

    Raw-only reference lane (STANDARDS section 4.9); the shared skeleton in
    raw_snapshot.py owns the on-disk contract. The headline contract count is
    `result.row_counts["exchange_info"]`.
    """
    require_safe_tokens(underlyings, "underlyings")
    return capture_raw_snapshot(
        output_root,
        source=SOURCE_NAME,
        payloads=_chain_payloads(underlyings),
        validate=lambda name, parsed: _validate_payload(name, parsed, underlyings),
        fetch=fetch,
        clock=clock,
        extra_summary={"underlyings": list(underlyings)},
    )
