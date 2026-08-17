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


INFO_URL = "https://api.hyperliquid.xyz/info"
SOURCE_NAME = "hyperliquid_wallet_flow"
USER_AGENT = "crypto-market-data-plant-hyperliquid-wallet-flow/0.1"
DEFAULT_RESPONSE_CAP = 2_000
DEFAULT_OVERLAP_SECONDS = 300.0

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
    ) -> None:
        self.cohort = cohort
        self.source_root = Path(source_root)
        self.state_path = Path(state_path)
        self.fetch = fetch
        self.request_pause_seconds = max(0.0, float(request_pause_seconds))
        self.overlap_ms = max(0, int(float(overlap_seconds) * 1000))
        self.response_cap = max(1, int(response_cap))
        self.clock = clock
        self.seen, self.highwater = scan_durable_wallet_fills(
            self.source_root,
            prospective_start_at=cohort.prospective_start_at,
        )
        self.poll_count = 0
        self.poll_error_count = 0
        self.emitted_count = 0
        self.duplicate_count = 0
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
            try:
                response = await asyncio.to_thread(self.fetch, request_payload)
                if not isinstance(response, list):
                    raise ValueError("userFillsByTime response is not a list")
                if len(response) >= self.response_cap:
                    raise RuntimeError(
                        f"response_cap_reached:{len(response)}; interval incomplete"
                    )
            except Exception as exc:  # noqa: BLE001 - isolate one public wallet failure
                complete = False
                self.poll_error_count += 1
                state["last_error_at"] = self.clock().astimezone(UTC).isoformat()
                state["last_error"] = f"{type(exc).__name__}: {exc}"
                state["last_response_complete"] = False
            else:
                wallet_new = 0
                state["last_success_at"] = self.clock().astimezone(UTC).isoformat()
                state["last_response_rows"] = len(response)
                state["last_response_complete"] = True
                state.pop("last_error", None)
                for fill in response:
                    if not isinstance(fill, dict):
                        continue
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
                    payload = dict(fill)
                    payload["_wallet"] = wallet.address
                    payload["_candidate_rank"] = wallet.candidate_rank
                    payload["_cohort_rank"] = wallet.cohort_rank
                    payload["_trade_key"] = trade_key
                    payload["_prospective_start_at"] = self.cohort.prospective_start_at.isoformat()
                    payload["_cohort_sha256"] = self.cohort.sha256
                    emitted.append(payload)
                    self.seen.add(trade_key)
                    self.highwater[wallet.address] = max(
                        timestamp_ms,
                        self.highwater.get(wallet.address, timestamp_ms),
                    )
                    wallet_new += 1
                state["last_new_rows"] = wallet_new
                state["highwater_timestamp_ms"] = self.highwater.get(wallet.address)
            if index + 1 < len(self.cohort.wallets) and self.request_pause_seconds:
                await asyncio.sleep(self.request_pause_seconds)

        emitted.sort(
            key=lambda row: (
                _optional_int(row.get("time")) or -1,
                str(row.get("_trade_key") or ""),
            )
        )
        self.poll_count += 1
        self.emitted_count += len(emitted)
        write_wallet_flow_state(
            self.state_path,
            {
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
                "per_wallet": self.per_wallet,
            },
        )
        return emitted, False


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
            raw_type="userFillsByTime",
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
