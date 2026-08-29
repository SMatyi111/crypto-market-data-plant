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


BASE_URL = "https://www.deribit.com/api/v2"
SOURCE_NAME = "deribit_options"
USER_AGENT = "crypto-market-data-plant-deribit-options/0.1"
DEFAULT_CURRENCIES = ("BTC", "ETH")


def fetch_url(url: str, *, timeout_seconds: float = 20.0) -> bytes:
    return fetch_bytes(url, user_agent=USER_AGENT, timeout_seconds=timeout_seconds)


def _snapshot_payloads(currencies: tuple[str, ...]) -> list[tuple[str, str]]:
    payloads: list[tuple[str, str]] = []
    for currency in currencies:
        payloads.extend(
            [
                (
                    f"instruments_{currency}",
                    f"{BASE_URL}/public/get_instruments?currency={currency}&kind=option&expired=false",
                ),
                (
                    f"option_summaries_{currency}",
                    f"{BASE_URL}/public/get_book_summary_by_currency?currency={currency}&kind=option",
                ),
                (
                    f"future_summaries_{currency}",
                    f"{BASE_URL}/public/get_book_summary_by_currency?currency={currency}&kind=future",
                ),
            ]
        )
    return payloads


def _validate_payload(name: str, parsed: Any) -> tuple[int | None, str | None]:
    """Return (row_count, failure_reason) for one parsed JSON-RPC payload.

    Deribit signals throttling/errors as a JSON-RPC `error` body (sometimes over
    HTTP 200), so this is the lane's real error channel; failing the job loudly
    is the intended backoff - the next scheduled interval is the retry.
    """
    if not isinstance(parsed, dict):
        return None, f"{name} payload is not a JSON object"
    if "error" in parsed:
        return None, f"{name} returned a JSON-RPC error: {parsed['error']!r}"
    result = parsed.get("result")
    if not isinstance(result, list) or not result:
        return None, f"{name} result is not a non-empty list"
    return len(result), None


def option_summary_count(result: RawSnapshotResult) -> int:
    return sum(
        count
        for name, count in result.row_counts.items()
        if name.startswith("option_summaries_") and count
    )


def snapshot_deribit_options(
    output_root: Path | str,
    *,
    currencies: tuple[str, ...] = DEFAULT_CURRENCIES,
    fetch: FetchFn = fetch_url,
    clock: Callable[[], Any] = utc_now,
) -> RawSnapshotResult:
    """Capture one Deribit options snapshot into its own run dir.

    Raw-only reference lane (STANDARDS section 4.9); per currency it archives
    the live option instrument list plus the option and future book summaries.
    The shared skeleton in raw_snapshot.py owns the on-disk contract.
    """
    require_safe_tokens(currencies, "currencies")
    return capture_raw_snapshot(
        output_root,
        source=SOURCE_NAME,
        payloads=_snapshot_payloads(currencies),
        validate=_validate_payload,
        fetch=fetch,
        clock=clock,
        extra_summary={"currencies": list(currencies)},
    )
