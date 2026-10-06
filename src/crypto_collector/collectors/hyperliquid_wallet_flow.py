from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from ..asset_registry import resolve_perp_instrument
from ..models import NormalizedL3Event, RawMessage, utc_now
from ..storage import JsonlSink


INFO_URL = "https://api.hyperliquid.xyz/info"
SOURCE_NAME = "hyperliquid_wallet_flow"
USER_AGENT = "crypto-market-data-plant-hyperliquid-wallet-flow/0.1"
DEFAULT_RESPONSE_CAP = 2_000
DEFAULT_OVERLAP_SECONDS = 300.0
# userTwapSliceFills has no time window: it returns the most recent fills only.
TWAP_RESPONSE_CAP = 2_000
# Lane default cadence (argparse, ops job args and the segment builder all use
# it). The poller class itself defaults to 0 = off for library/test callers.
DEFAULT_TWAP_EVERY_POLLS = 5
# Slices older than the oldest hot run (+ margin) may already be stored in a run
# that was offloaded to cold storage, which the start-up dedup scan cannot see.
TWAP_DEDUP_HORIZON_MARGIN_MS = 6 * 3_600_000
FILLS_ENDPOINT = "userFillsByTime"
TWAP_ENDPOINT = "userTwapSliceFills"

FetchFn = Callable[[dict[str, Any]], Any]


@dataclass(frozen=True, slots=True)
class CohortWallet:
    address: str
    candidate_rank: int
    cohort_rank: int


@dataclass(frozen=True, slots=True)
class WalletFlowCohort:
    prospective_start_at: datetime
    wallets: tuple[CohortWallet, ...]
    target_coins: frozenset[str]
    sha256: str


def load_wallet_flow_cohort(path: Path | str) -> WalletFlowCohort:
    cohort_path = Path(path)
    raw = cohort_path.read_bytes()
    payload = json.loads(raw.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("cohort config must be a JSON object")
    start = _parse_datetime(payload.get("prospective_start_at"))
    if start is None:
        raise ValueError("cohort config requires timezone-aware prospective_start_at")
    target_coins = frozenset(str(coin).upper() for coin in payload.get("target_coins", []))
    if not target_coins:
        raise ValueError("cohort config requires at least one target coin")

    wallets: list[CohortWallet] = []
    seen: set[str] = set()
    for item in payload.get("wallets", []):
        if not isinstance(item, dict):
            raise ValueError("cohort wallets must be JSON objects")
        address = str(item.get("address", "")).lower()
        if not _valid_address(address):
            raise ValueError(f"invalid cohort wallet address: {address!r}")
        if address in seen:
            raise ValueError(f"duplicate cohort wallet address: {address}")
        candidate_rank = _positive_int(item.get("candidate_rank"), "candidate_rank")
        cohort_rank = _positive_int(item.get("cohort_rank"), "cohort_rank")
        wallets.append(
            CohortWallet(
                address=address,
                candidate_rank=candidate_rank,
                cohort_rank=cohort_rank,
            )
        )
        seen.add(address)
    if not wallets:
        raise ValueError("cohort config requires at least one wallet")
    wallets.sort(key=lambda wallet: wallet.cohort_rank)
    if [wallet.cohort_rank for wallet in wallets] != list(range(1, len(wallets) + 1)):
        raise ValueError("cohort_rank must be contiguous starting at 1")
    return WalletFlowCohort(
        prospective_start_at=start,
        wallets=tuple(wallets),
        target_coins=target_coins,
        sha256=hashlib.sha256(raw).hexdigest(),
    )


def post_info(payload: dict[str, Any], *, timeout_seconds: float = 30.0) -> Any:
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    for attempt in range(1, 4):
        request = Request(
            INFO_URL,
            data=body,
            method="POST",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": USER_AGENT,
            },
        )
        try:
            with urlopen(request, timeout=timeout_seconds) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            if exc.code not in {429, 500, 502, 503, 504} or attempt == 3:
                raise
            raw_retry = exc.headers.get("Retry-After") if exc.headers is not None else None
            try:
                delay = min(30.0, max(0.0, float(raw_retry)))
            except (TypeError, ValueError):
                delay = min(30.0, 2.0**attempt)
        except URLError:
            if attempt == 3:
                raise
            delay = min(30.0, 2.0**attempt)
        time.sleep(delay)
    raise RuntimeError("unreachable Hyperliquid retry loop")


def scan_durable_wallet_fills(
    source_root: Path | str,
    *,
    prospective_start_at: datetime,
) -> tuple[set[str], dict[str, int]]:
    """Recover dedup/high-water state from every durable clean row.

    TWAP slice rows (`raw_type="userTwapSliceFills"`) feed dedup only.

    This includes unfinished runs. If a worker dies after writing part of a poll but
    before finalizing its replay summary, the next worker suppresses those rows rather
    than re-emitting them into a second run that may later be promoted as a duplicate.
    """
    root = Path(source_root)
    seen: set[str] = set()
    highwater: dict[str, int] = {}
    if not root.exists():
        return seen, highwater
    for run_dir in sorted(path for path in root.iterdir() if path.is_dir()):
        events_path = run_dir / "clean" / "events.jsonl"
        if not events_path.exists():
            continue
        try:
            handle = events_path.open("r", encoding="utf-8")
        except OSError:
            continue
        with handle:
            for line in handle:
                try:
                    row = json.loads(line)
                except (json.JSONDecodeError, ValueError):
                    continue
                if not isinstance(row, dict):
                    continue
                metadata = row.get("metadata")
                if not isinstance(metadata, dict):
                    continue
                wallet = str(metadata.get("wallet", "")).lower()
                timestamp_ms = _optional_int(metadata.get("hyperliquid_timestamp_ms"))
                trade_key = str(row.get("trade_id") or "")
                if not _valid_address(wallet) or timestamp_ms is None or not trade_key:
                    continue
                event_at = datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC)
                if event_at < prospective_start_at:
                    continue
                seen.add(trade_key)
                if row.get("raw_type") == TWAP_ENDPOINT:
                    # TWAP slice rows come from a second endpoint with no time
                    # window; they must not move the userFillsByTime cursor (a
                    # restart would otherwise skip an unfinished capped region).
                    continue
                highwater[wallet] = max(timestamp_ms, highwater.get(wallet, timestamp_ms))
    return seen, highwater


class HyperliquidWalletFlowPoller:
    def __init__(
        self,
        *,
        cohort: WalletFlowCohort,
        source_root: Path,
        state_path: Path,
        fetch: FetchFn = post_info,
        request_pause_seconds: float = 0.1,
        overlap_seconds: float = DEFAULT_OVERLAP_SECONDS,
        response_cap: int = DEFAULT_RESPONSE_CAP,
        clock: Callable[[], datetime] = utc_now,
        poll_history_path: Path | None = None,
        twap_every_polls: int = 0,
    ) -> None:
        self.cohort = cohort
        self.source_root = Path(source_root)
        self.state_path = Path(state_path)
        self.fetch = fetch
        self.request_pause_seconds = max(0.0, float(request_pause_seconds))
        self.overlap_ms = max(0, int(float(overlap_seconds) * 1000))
        self.response_cap = max(1, int(response_cap))
        self.clock = clock
        self.poll_history_path = Path(poll_history_path) if poll_history_path is not None else None
        # Same per-line fsync posture as metrics/summary.jsonl (the metrics sink).
        self.poll_history_sink = (
            JsonlSink(self.poll_history_path.parent, self.poll_history_path.name)
            if self.poll_history_path is not None
            else None
        )
        self.poll_history_error_count = 0
        self.last_poll_history_error: str | None = None
        self.seen, self.highwater = scan_durable_wallet_fills(
            self.source_root,
            prospective_start_at=cohort.prospective_start_at,
        )
        self.poll_count = 0
        self.poll_error_count = 0
        self.emitted_count = 0
        self.duplicate_count = 0
        # A capped page continues at its inclusive boundary, without subtracting
        # the normal overlap again. This is deliberately in-memory only: a poll
        # prepares rows before the pipeline writes them, so restoring this cursor
        # from _collector_state.json could skip an interrupted batch on restart.
        self.page_starts: dict[str, int] = {}
        # Start of the uncapped page that ended the last continuation. Ordinary
        # overlap re-entry never goes below it: the capped pages before it were
        # consumed whole, and re-entering them re-caps and re-walks the same dense
        # region on every poll cycle. In-memory only, like page_starts.
        self.resume_floors: dict[str, int] = {}
        self.capped_response_count = 0
        self.incomplete_poll_count = 0
        # TWAP slice fills never appear in userFillsByTime (found 2026-10-06: the
        # "silent per-wallet stalls" of 09-14 and 09-15..09-19 were TWAP-executed
        # BTC/ETH fills). 0 disables the second endpoint.
        self.twap_every_polls = max(0, int(twap_every_polls))
        self.twap_request_count = 0
        self.twap_error_count = 0
        self.twap_emitted_count = 0
        self.twap_duplicate_count = 0
        self.twap_window_gap_count = 0
        # TWAP slices are their own ordered stream per wallet (the replay gate
        # orders per (wallet, endpoint)). Newest slice emitted by THIS poller (one
        # poller per run): a newly visible slice older than it is deferred to the
        # next run instead of breaking the stream order.
        self.twap_last_emitted_ms: dict[str, int] = {}
        self.twap_deferred_keys: set[str] = set()
        # Newest slice seen per wallet (all coins), restored from the previous
        # run's state file so the first read after a restart can prove a gap.
        # Diagnostic only - never a cursor - so restoring it is safe.
        self.twap_newest_seen_ms: dict[str, int] = _restore_twap_newest(self.state_path)
        self.twap_dedup_horizon_ms = _twap_dedup_horizon_ms(self.source_root, clock)
        self.twap_beyond_horizon_count = 0
        # Wallets whose last TWAP read failed, showed a gap or left slices
        # deferred; keeps later non-TWAP polls honest about completeness.
        self.twap_incomplete_wallets: set[str] = set()
        self.last_poll_complete: bool | None = None
        self.per_wallet: dict[str, dict[str, Any]] = {
            wallet.address: {"candidate_rank": wallet.candidate_rank, "cohort_rank": wallet.cohort_rank}
            for wallet in cohort.wallets
        }

    async def poll(self) -> tuple[list[dict], bool]:
        now = self.clock().astimezone(UTC)
        end_ms = int(now.timestamp() * 1000)
        start_floor_ms = int(self.cohort.prospective_start_at.timestamp() * 1000)
        emitted: list[dict[str, Any]] = []
        complete = True
        for index, wallet in enumerate(self.cohort.wallets):
            highwater = self.highwater.get(wallet.address)
            start_ms = start_floor_ms if highwater is None else max(start_floor_ms, highwater - self.overlap_ms)
            if wallet.address in self.page_starts:
                start_ms = self.page_starts[wallet.address]
            else:
                start_ms = max(start_ms, self.resume_floors.get(wallet.address, start_ms))
            request_payload = {
                "type": "userFillsByTime",
                "user": wallet.address,
                "startTime": start_ms,
                "endTime": end_ms,
                "aggregateByTime": False,
            }
            attempted_at = self.clock().astimezone(UTC)
            state = self.per_wallet[wallet.address]
            state["last_attempt_at"] = attempted_at.isoformat()
            state.update(
                last_request_start_ms=start_ms,
                last_request_end_ms=end_ms,
                last_response_rows=None,
                last_response_capped=None,
                last_response_complete=False,
                last_new_rows=0,
                last_poll_status="failed",
            )
            try:
                response = await asyncio.to_thread(self.fetch, request_payload)
                if not isinstance(response, list):
                    raise ValueError("userFillsByTime response is not a list")
                fills = [fill for fill in response if isinstance(fill, dict)]
                capped = len(response) >= self.response_cap
                state["last_response_rows"] = len(response)
                state["last_response_capped"] = capped
                if capped:
                    self.capped_response_count += 1
                boundary_ms: int | None = None
                if capped:
                    # The server truncated the window. Page forward instead of
                    # stalling: a raise here never advances the high-water, so a
                    # wallet whose un-covered window once exceeds the cap would
                    # error on every subsequent poll forever. Keep only fills
                    # strictly older than the newest returned timestamp (that
                    # millisecond may be cut mid-batch), advance the high-water to
                    # that boundary, and let the next poll re-enter at the exact
                    # boundary. Subtracting overlap here can repeat the same dense
                    # page forever. A page whose fills all share a single
                    # timestamp has no safe boundary — error as before rather than
                    # silently dropping same-millisecond fills beyond the cap.
                    stamps = {
                        stamp
                        for stamp in (_optional_int(fill.get("time")) for fill in fills)
                        if stamp is not None
                    }
                    if len(stamps) < 2:
                        state["last_poll_status"] = "unpageable_timestamp"
                        raise RuntimeError(
                            f"response_cap_reached:{len(response)}; "
                            "single-timestamp page cannot advance"
                        )
                    boundary_ms = max(stamps)
                    fills = [
                        fill
                        for fill in fills
                        if (stamp := _optional_int(fill.get("time"))) is not None
                        and stamp < boundary_ms
                    ]
            except Exception as exc:  # noqa: BLE001 - isolate one public wallet failure
                complete = False
                self.poll_error_count += 1
                state["last_error_at"] = self.clock().astimezone(UTC).isoformat()
                state["last_error"] = f"{type(exc).__name__}: {exc}"
            else:
                wallet_new = 0
                if capped:
                    complete = False
                state["last_success_at"] = self.clock().astimezone(UTC).isoformat()
                state["last_response_complete"] = not capped
                state["last_poll_status"] = "capped" if capped else "response_uncapped"
                state.pop("last_error", None)
                for fill in fills:
                    timestamp_ms = _optional_int(fill.get("time"))
                    coin = str(fill.get("coin") or "").upper()
                    if (
                        timestamp_ms is None
                        or timestamp_ms < start_floor_ms
                        or coin not in self.cohort.target_coins
                    ):
                        continue
                    trade_key = wallet_trade_key(wallet.address, fill)
                    if trade_key in self.seen:
                        self.duplicate_count += 1
                        continue
                    emitted.append(self._build_payload(wallet, fill, trade_key))
                    self.seen.add(trade_key)
                    self.highwater[wallet.address] = max(
                        timestamp_ms,
                        self.highwater.get(wallet.address, timestamp_ms),
                    )
                    wallet_new += 1
                if boundary_ms is not None:
                    # Advance past the consumed page even when it emitted nothing
                    # (all duplicates or non-target coins) — otherwise the window
                    # re-caps identically on every poll and never progresses.
                    self.highwater[wallet.address] = max(
                        boundary_ms, self.highwater.get(wallet.address, boundary_ms)
                    )
                    self.page_starts[wallet.address] = boundary_ms
                elif self.page_starts.pop(wallet.address, None) is not None:
                    # An uncapped page finishes this continuation. Ordinary polls
                    # resume with the configured overlap for late rows, clamped to
                    # this page's start (see resume_floors).
                    self.resume_floors[wallet.address] = start_ms
                state["last_new_rows"] = wallet_new
                state["highwater_timestamp_ms"] = self.highwater.get(wallet.address)
            if self.twap_every_polls and self.poll_count % self.twap_every_polls == 0:
                if self.request_pause_seconds:
                    await asyncio.sleep(self.request_pause_seconds)
                await self._poll_twap(wallet, start_floor_ms, state, emitted)
            state["page_start_timestamp_ms"] = self.page_starts.get(wallet.address)
            state["resume_floor_timestamp_ms"] = self.resume_floors.get(wallet.address)
            if index + 1 < len(self.cohort.wallets) and self.request_pause_seconds:
                await asyncio.sleep(self.request_pause_seconds)

        emitted.sort(
            key=lambda row: (
                _optional_int(row.get("time")) or -1,
                str(row.get("_trade_key") or ""),
            )
        )
        if self.twap_incomplete_wallets:
            complete = False
        self.poll_count += 1
        self.emitted_count += len(emitted)
        self.last_poll_complete = complete
        if not complete:
            self.incomplete_poll_count += 1
        snapshot = {
            "updated_at": self.clock().astimezone(UTC).isoformat(),
            "prospective_start_at": self.cohort.prospective_start_at.isoformat(),
            "cohort_sha256": self.cohort.sha256,
            "wallet_count": len(self.cohort.wallets),
            "target_coins": sorted(self.cohort.target_coins),
            "last_poll_complete": complete,
            "poll_count": self.poll_count,
            "poll_error_count": self.poll_error_count,
            "emitted_count": self.emitted_count,
            "duplicate_count": self.duplicate_count,
            "capped_response_count": self.capped_response_count,
            "incomplete_poll_count": self.incomplete_poll_count,
            "twap_every_polls": self.twap_every_polls,
            "twap_request_count": self.twap_request_count,
            "twap_error_count": self.twap_error_count,
            "twap_emitted_count": self.twap_emitted_count,
            "twap_duplicate_count": self.twap_duplicate_count,
            "twap_deferred_count": self.twap_deferred_count,
            "twap_window_gap_count": self.twap_window_gap_count,
            "twap_beyond_horizon_count": self.twap_beyond_horizon_count,
            "twap_dedup_horizon_ms": self.twap_dedup_horizon_ms,
            "twap_incomplete_wallets": sorted(self.twap_incomplete_wallets),
            "per_wallet": self.per_wallet,
        }
        if self.poll_history_sink is not None:
            # Diagnostics must never abort capture: a failed append is counted in
            # the state file and the prepared rows are still returned.
            try:
                self.poll_history_sink.path.parent.mkdir(parents=True, exist_ok=True)
                self.poll_history_sink.write({
                    "diagnostic_version": 1,
                    "delivery_status": "prepared_not_acknowledged",
                    "historical_capture_complete": None,
                    **snapshot,
                })
            except (OSError, TypeError, ValueError) as exc:
                self.poll_history_error_count += 1
                self.last_poll_history_error = f"{type(exc).__name__}: {exc}"
        snapshot["poll_history_error_count"] = self.poll_history_error_count
        snapshot["last_poll_history_error"] = self.last_poll_history_error
        write_wallet_flow_state(self.state_path, snapshot)
        return emitted, False

    @property
    def twap_deferred_count(self) -> int:
        """Distinct slices currently held back for the next run."""
        return len(self.twap_deferred_keys)

    def _build_payload(
        self,
        wallet: CohortWallet,
        fill: dict[str, Any],
        trade_key: str,
        endpoint: str | None = None,
    ) -> dict[str, Any]:
        payload = dict(fill)
        payload["_wallet"] = wallet.address
        payload["_candidate_rank"] = wallet.candidate_rank
        payload["_cohort_rank"] = wallet.cohort_rank
        payload["_trade_key"] = trade_key
        payload["_prospective_start_at"] = self.cohort.prospective_start_at.isoformat()
        payload["_cohort_sha256"] = self.cohort.sha256
        if endpoint is not None:
            payload["_fill_endpoint"] = endpoint
        return payload

    async def _poll_twap(
        self,
        wallet: CohortWallet,
        start_floor_ms: int,
        state: dict[str, Any],
        emitted: list[dict[str, Any]],
    ) -> None:
        """Read the wallet's most recent TWAP slices into the TWAP stream."""
        address = wallet.address
        self.twap_request_count += 1
        state["last_twap_attempt_at"] = self.clock().astimezone(UTC).isoformat()
        state.update(last_twap_rows=None, last_twap_new_rows=0, last_twap_deferred=None)
        try:
            response = await asyncio.to_thread(
                self.fetch, {"type": TWAP_ENDPOINT, "user": address}
            )
            if not isinstance(response, list):
                raise ValueError("userTwapSliceFills response is not a list")
        except Exception as exc:  # noqa: BLE001 - isolate one public wallet failure
            self.twap_error_count += 1
            self.twap_incomplete_wallets.add(address)
            state["last_twap_status"] = "failed"
            state["last_twap_error"] = f"{type(exc).__name__}: {exc}"
            return
        fills: list[dict[str, Any]] = []
        for item in response:
            if not isinstance(item, dict) or not isinstance(item.get("fill"), dict):
                continue
            fill = dict(item["fill"])
            if fill.get("twapId") in (None, "") and item.get("twapId") not in (None, ""):
                fill["twapId"] = item["twapId"]
            if _optional_int(fill.get("time")) is not None:
                fills.append(fill)
        fills.sort(key=lambda fill: (_optional_int(fill.get("time")), str(fill.get("tid"))))
        state["last_twap_rows"] = len(response)
        state.pop("last_twap_error", None)
        status = "ok"
        if response and not fills:
            # A non-empty answer we cannot read is a silent stall in the making.
            status = "unparseable_response"
        previous_newest = self.twap_newest_seen_ms.get(address)
        if fills:
            oldest = _optional_int(fills[0].get("time"))
            newest = _optional_int(fills[-1].get("time"))
            if (
                len(response) >= TWAP_RESPONSE_CAP
                and previous_newest is not None
                and oldest is not None
                and oldest >= previous_newest
            ):
                # The most-recent window no longer reaches back past the newest
                # slice seen before (this run, or the previous run's state file):
                # slices in between - or tied at that millisecond - may be lost.
                self.twap_window_gap_count += 1
                status = "window_gap"
            if newest is not None:
                self.twap_newest_seen_ms[address] = max(
                    newest, previous_newest if previous_newest is not None else newest
                )
        floor = self.twap_last_emitted_ms.get(address)
        new_rows = 0
        for fill in fills:
            timestamp_ms = _optional_int(fill.get("time"))
            coin = str(fill.get("coin") or "").upper()
            if (
                timestamp_ms is None
                or timestamp_ms < start_floor_ms
                or coin not in self.cohort.target_coins
            ):
                continue
            trade_key = wallet_trade_key(address, fill)
            if trade_key in self.seen:
                self.twap_duplicate_count += 1
                continue
            if timestamp_ms < self.twap_dedup_horizon_ms:
                # Cannot be deduplicated against offloaded runs; never re-emit.
                self.twap_beyond_horizon_count += 1
                continue
            if floor is not None and timestamp_ms < floor:
                self.twap_deferred_keys.add(trade_key)
                continue
            emitted.append(self._build_payload(wallet, fill, trade_key, TWAP_ENDPOINT))
            self.seen.add(trade_key)
            self.twap_deferred_keys.discard(trade_key)
            self.twap_last_emitted_ms[address] = timestamp_ms
            new_rows += 1
        self.twap_emitted_count += new_rows
        prefix = f"{address}:"
        deferred = sum(1 for key in self.twap_deferred_keys if key.startswith(prefix))
        if status == "ok" and deferred:
            status = "deferred_to_next_run"
        if status == "ok":
            self.twap_incomplete_wallets.discard(address)
        else:
            self.twap_incomplete_wallets.add(address)
        state["last_twap_status"] = status
        state["last_twap_new_rows"] = new_rows
        state["last_twap_deferred"] = deferred
        state["last_twap_success_at"] = self.clock().astimezone(UTC).isoformat()
        state["twap_newest_seen_ms"] = self.twap_newest_seen_ms.get(address)


class HyperliquidWalletFillNormalizer:
    def normalize(self, raw: RawMessage) -> NormalizedL3Event:
        payload = raw.payload
        parse_errors: list[str] = []
        wallet = str(payload.get("_wallet") or "").lower()
        coin = str(payload.get("coin") or "UNKNOWN").upper()
        timestamp_ms = _optional_int(payload.get("time"))
        exchange_time = (
            datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC)
            if timestamp_ms is not None
            else None
        )
        if exchange_time is None:
            parse_errors.append("invalid_exchange_time")
        price = _optional_float(payload.get("px"))
        size = _optional_float(payload.get("sz"))
        side_value = str(payload.get("side") or "").upper()
        side = {"B": "buy", "A": "sell"}.get(side_value)
        if side is None:
            parse_errors.append("invalid_side")
        instrument = resolve_perp_instrument(f"{coin}USDC", venue="hyperliquid")
        trade_key = str(payload.get("_trade_key") or wallet_trade_key(wallet, payload))
        order_id = payload.get("oid")
        metadata: dict[str, Any] = {
            "instrument_id": instrument.instrument_id if instrument is not None else None,
            "canonical_symbol": instrument.canonical_symbol if instrument is not None else None,
            "wallet": wallet,
            "candidate_rank": _optional_int(payload.get("_candidate_rank")),
            "cohort_rank": _optional_int(payload.get("_cohort_rank")),
            "cohort_sha256": payload.get("_cohort_sha256"),
            "prospective_start_at": payload.get("_prospective_start_at"),
            "hyperliquid_timestamp_ms": timestamp_ms,
            "hyperliquid_trade_id": payload.get("tid"),
            "hyperliquid_order_id": order_id,
            "transaction_hash": payload.get("hash"),
            "direction": payload.get("dir"),
            "start_position": _optional_float(payload.get("startPosition")),
            "closed_pnl": _optional_float(payload.get("closedPnl")),
            "crossed": payload.get("crossed") if isinstance(payload.get("crossed"), bool) else None,
            "fee": _optional_float(payload.get("fee")),
            "fee_token": payload.get("feeToken"),
            "client_order_id": payload.get("cloid"),
            "twap_id": payload.get("twapId"),
        }
        if parse_errors:
            metadata["parse_errors"] = parse_errors
        return NormalizedL3Event(
            source="hyperliquid",
            product=coin,
            channel="trades",
            event_type="user_fill",
            exchange_time=exchange_time,
            received_at=raw.received_at,
            side=side,
            price=price,
            size=size,
            order_id=f"{wallet}:{order_id}" if order_id not in (None, "") else None,
            trade_id=trade_key,
            sequence=None,
            raw_type=(
                TWAP_ENDPOINT
                if payload.get("_fill_endpoint") == TWAP_ENDPOINT
                else FILLS_ENDPOINT
            ),
            metadata={key: value for key, value in metadata.items() if value is not None},
        )


def wallet_trade_key(wallet: str, fill: dict[str, Any]) -> str:
    trade_id = fill.get("tid")
    if trade_id not in (None, ""):
        return f"{wallet.lower()}:{trade_id}"
    fallback = "|".join(
        str(fill.get(key, "")) for key in ("time", "coin", "side", "px", "sz", "oid", "hash")
    )
    return f"{wallet.lower()}:fallback:{hashlib.sha256(fallback.encode('utf-8')).hexdigest()}"


def write_wallet_flow_state(path: Path | str, payload: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f"{target.name}.{os.getpid()}.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(target)


def _restore_twap_newest(state_path: Path) -> dict[str, int]:
    try:
        payload = json.loads(Path(state_path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    restored: dict[str, int] = {}
    per_wallet = payload.get("per_wallet") if isinstance(payload, dict) else None
    if isinstance(per_wallet, dict):
        for address, wallet_state in per_wallet.items():
            if isinstance(wallet_state, dict):
                stamp = _optional_int(wallet_state.get("twap_newest_seen_ms"))
                if stamp is not None:
                    restored[str(address).lower()] = stamp
    return restored


def _twap_dedup_horizon_ms(source_root: Path, clock: Callable[[], datetime]) -> int:
    """Oldest exchange time a TWAP slice may have and still be deduplicated.

    Run directories are named by creation time (YYYYMMDD_HHMMSS) and offloaded
    oldest first, so a fill in an offloaded run is older than the oldest hot run
    plus one segment. With no hot run there is nothing to dedup against: only
    slices from now on are safe.
    """
    oldest: datetime | None = None
    root = Path(source_root)
    if root.exists():
        for path in root.iterdir():
            if not path.is_dir():
                continue
            try:
                created = datetime.strptime(path.name[:15], "%Y%m%d_%H%M%S").replace(tzinfo=UTC)
            except ValueError:
                continue
            if oldest is None or created < oldest:
                oldest = created
    if oldest is None:
        return int(clock().astimezone(UTC).timestamp() * 1000)
    return int(oldest.timestamp() * 1000) + TWAP_DEDUP_HORIZON_MARGIN_MS


def _parse_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(UTC)


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return None


def _positive_int(value: Any, name: str) -> int:
    parsed = _optional_int(value)
    if parsed is None or parsed <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return parsed


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if parsed == parsed and abs(parsed) != float("inf") else None


def _valid_address(value: str) -> bool:
    if len(value) != 42 or not value.startswith("0x"):
        return False
    try:
        int(value[2:], 16)
    except ValueError:
        return False
    return True
