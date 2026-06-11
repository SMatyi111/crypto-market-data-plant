from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
import functools
import hashlib
import inspect
import json
import math
from pathlib import Path
import sys
import time
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from ..config import default_curated_root, default_normalized_root, default_output_root
from ..storage import JsonlSink, ParquetDatasetSink, RotatingJsonlSink, prepare_run_paths


DEFAULT_KALSHI_BASE_URL = "https://external-api.kalshi.com/trade-api/v2"
DEFAULT_KALSHI_CATEGORY = "Crypto"
DEFAULT_KALSHI_TARGET_ASSETS = ["BTC", "ETH"]
DEFAULT_KALSHI_TARGET_FREQUENCIES = ["fifteen_min", "hourly"]
DEFAULT_KALSHI_MARKETS_PER_SERIES = 100
DEFAULT_KALSHI_POLL_INTERVAL_SECONDS = 5.0
DEFAULT_KALSHI_STALE_AFTER_SECONDS = 3.0
DEFAULT_KALSHI_DURATION_SECONDS = 60.0
_DEFAULT_ROOT = object()


def default_kalshi_discovery_root() -> Path:
    return default_curated_root("kalshi_crypto_binary_options")


def default_kalshi_output_root() -> Path:
    return default_output_root()


def default_kalshi_normalized_root() -> Path:
    return default_normalized_root("binary_options")


@dataclass(slots=True)
class KalshiApiResponse:
    endpoint: str
    params: dict[str, Any]
    url: str
    payload: dict[str, Any]
    request_sent_at: datetime
    response_received_at: datetime
    latency_ms: float
    http_status: int
    success: bool = True
    error_type: str | None = None
    error: str | None = None

    def to_raw_row(self) -> dict[str, Any]:
        row = asdict(self)
        row["source"] = "kalshi"
        row["venue"] = "kalshi"
        row["event_type"] = "api_response"
        row["request_sent_at"] = self.request_sent_at.isoformat()
        row["response_received_at"] = self.response_received_at.isoformat()
        row["received_at"] = self.response_received_at.isoformat()
        row["rate_limited"] = self.http_status == 429
        return row


class KalshiApiError(RuntimeError):
    def __init__(self, *, response: KalshiApiResponse) -> None:
        super().__init__(response.error or f"Kalshi API error {response.http_status}")
        self.response = response


class KalshiPublicRestClient:
    """Unauthenticated Kalshi public REST client used by the data plant.

    The collector intentionally uses only public endpoints. If authenticated orderbook
    or trade endpoints are enabled later, credentials must be read from environment
    variables at request time and must never be written to raw payloads or metrics.
    """

    def __init__(self, *, base_url: str = DEFAULT_KALSHI_BASE_URL, timeout_seconds: float = 15.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def fetch_series(self, *, category: str = DEFAULT_KALSHI_CATEGORY) -> KalshiApiResponse:
        return self._get_json("/series", {"category": category})

    def fetch_markets(
        self,
        *,
        series_ticker: str,
        status: str = "open",
        limit: int = DEFAULT_KALSHI_MARKETS_PER_SERIES,
        cursor: str | None = None,
    ) -> KalshiApiResponse:
        params: dict[str, Any] = {
            "series_ticker": series_ticker,
            "status": status,
            "limit": int(limit),
        }
        if cursor:
            params["cursor"] = cursor
        return self._get_json("/markets", params)

    def series(self, *, category: str = DEFAULT_KALSHI_CATEGORY, include_volume: bool = True) -> list[dict[str, Any]]:
        response = self.fetch_series(category=category)
        values = response.payload.get("series") if isinstance(response.payload, dict) else []
        return [item for item in values if isinstance(item, dict)]

    def markets(
        self,
        *,
        series_ticker: str,
        status: str = "open",
        limit: int = DEFAULT_KALSHI_MARKETS_PER_SERIES,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            response = self.fetch_markets(
                series_ticker=series_ticker,
                status=status,
                limit=limit,
                cursor=cursor,
            )
            values = response.payload.get("markets") if isinstance(response.payload, dict) else []
            rows.extend(item for item in values if isinstance(item, dict))
            cursor = _optional_str(response.payload.get("cursor"))
            if not cursor or len(rows) >= limit:
                return rows[:limit]

    def fetch_orderbook(self, *, market_ticker: str) -> KalshiApiResponse:
        return self._get_json(f"/markets/{market_ticker}/orderbook", {})

    def fetch_trades(self, *, market_ticker: str, limit: int = 100) -> KalshiApiResponse:
        return self._get_json(f"/markets/{market_ticker}/trades", {"limit": int(limit)})

    def _get_json(self, path: str, params: dict[str, Any]) -> KalshiApiResponse:
        query = urlencode({key: value for key, value in params.items() if value not in (None, "")})
        url = f"{self.base_url}{path}" + (f"?{query}" if query else "")
        request = Request(url, headers={"User-Agent": "crypto-market-data-plant/kalshi-public"})
        request_sent_at = datetime.now(tz=UTC)
        started = time.monotonic()
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:  # noqa: S310 - fixed public API URL.
                raw_body = response.read().decode("utf-8")
                payload = json.loads(raw_body) if raw_body else {}
                if not isinstance(payload, dict):
                    payload = {"value": payload}
                response_received_at = datetime.now(tz=UTC)
                return KalshiApiResponse(
                    endpoint=path,
                    params=dict(params),
                    url=url,
                    payload=payload,
                    request_sent_at=request_sent_at,
                    response_received_at=response_received_at,
                    latency_ms=(time.monotonic() - started) * 1000.0,
                    http_status=int(getattr(response, "status", 200)),
                )
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            try:
                payload = json.loads(body) if body else {}
            except json.JSONDecodeError:
                payload = {"body": body}
            if not isinstance(payload, dict):
                payload = {"value": payload}
            response = KalshiApiResponse(
                endpoint=path,
                params=dict(params),
                url=url,
                payload=payload,
                request_sent_at=request_sent_at,
                response_received_at=datetime.now(tz=UTC),
                latency_ms=(time.monotonic() - started) * 1000.0,
                http_status=int(exc.code),
                success=False,
                error_type=type(exc).__name__,
                error=str(exc),
            )
            raise KalshiApiError(response=response) from exc
        except Exception as exc:
            response = KalshiApiResponse(
                endpoint=path,
                params=dict(params),
                url=url,
                payload={},
                request_sent_at=request_sent_at,
                response_received_at=datetime.now(tz=UTC),
                latency_ms=(time.monotonic() - started) * 1000.0,
                http_status=0,
                success=False,
                error_type=type(exc).__name__,
                error=str(exc),
            )
            raise KalshiApiError(response=response) from exc


@dataclass(slots=True)
class KalshiSeriesCandidate:
    ticker: str
    title: str | None
    category: str | None
    frequency: str | None
    tags: list[str]
    volume: float | None
    selected: bool
    selection_reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class KalshiMarketCandidate:
    ticker: str
    series_ticker: str | None
    event_ticker: str | None
    title: str | None
    yes_sub_title: str | None
    status: str | None
    market_type: str | None
    close_time: str | None
    expiration_time: str | None
    seconds_to_close: float | None
    yes_bid: float | None
    yes_ask: float | None
    no_bid: float | None
    no_ask: float | None
    yes_ask_size: float | None
    no_ask_size: float | None
    volume: float | None
    volume_24h: float | None
    open_interest: float | None
    liquidity: float | None
    fractional_trading_enabled: bool | None
    findings: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class KalshiDiscoveryReport:
    status: str
    built_at: str
    output_root: str | None
    base_url: str
    category: str
    target_assets: list[str]
    target_frequencies: list[str]
    series_count: int
    selected_series_count: int
    market_count: int
    active_market_count: int
    shortest_frequency: str | None
    shortest_frequency_seconds: int | None
    findings: list[str]
    series: list[KalshiSeriesCandidate]
    markets: list[KalshiMarketCandidate]

    def to_dict(self) -> dict[str, Any]:
        return {
            **{
                key: value
                for key, value in asdict(self).items()
                if key not in {"series", "markets"}
            },
            "series": [item.to_dict() for item in self.series],
            "markets": [item.to_dict() for item in self.markets],
        }


@dataclass(slots=True)
class KalshiCollectionSummary:
    status: str
    built_at: str
    run_path: str
    raw_path: str
    clean_path: str
    normalized_root: str | None
    base_url: str
    category: str
    target_assets: list[str]
    target_frequencies: list[str]
    poll_interval_seconds: float
    stale_after_seconds: float
    markets_per_series: int
    sample_count: int
    series_count: int
    selected_series_count: int
    market_fetch_count: int
    raw_messages: int
    clean_events: int
    error_events: int
    quote_count: int
    side_symbol_count: int
    first_observed_quote_count: int
    quote_update_count: int
    repeated_quote_count: int
    stale_quote_count: int
    started_at: str | None
    ended_at: str | None
    findings: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def discover_kalshi_crypto_markets(
    *,
    output_root: Path | None | object = _DEFAULT_ROOT,
    category: str = DEFAULT_KALSHI_CATEGORY,
    target_assets: list[str] | None = None,
    target_frequencies: list[str] | None = None,
    markets_per_series: int = DEFAULT_KALSHI_MARKETS_PER_SERIES,
    client: Any | None = None,
) -> KalshiDiscoveryReport:
    api = client or KalshiPublicRestClient()
    resolved_output_root = default_kalshi_discovery_root() if output_root is _DEFAULT_ROOT else output_root
    series_payloads = _extract_series(_fetch_series_response(api, category=category).payload)

    def market_loader(ticker: str) -> list[dict[str, Any]]:
        return _load_markets(api, series_ticker=ticker, markets_per_series=markets_per_series)[0]

    report = build_kalshi_discovery_report(
        series_payloads=series_payloads,
        market_loader=market_loader,
        output_root=resolved_output_root,
        base_url=getattr(api, "base_url", DEFAULT_KALSHI_BASE_URL),
        category=category,
        target_assets=target_assets,
        target_frequencies=target_frequencies,
    )
    if resolved_output_root is not None:
        write_kalshi_discovery_report(report, output_root=resolved_output_root)
    return report


def build_kalshi_discovery_report(
    *,
    series_payloads: list[dict[str, Any]],
    market_loader: Any,
    output_root: Path | str | None = None,
    base_url: str = DEFAULT_KALSHI_BASE_URL,
    category: str = DEFAULT_KALSHI_CATEGORY,
    target_assets: list[str] | None = None,
    target_frequencies: list[str] | None = None,
) -> KalshiDiscoveryReport:
    assets = _normalize_assets(target_assets)
    frequencies = _normalize_frequencies(target_frequencies)
    series = [_series_candidate(row, target_assets=assets) for row in series_payloads]
    selected_series = [
        item for item in series if item.selected and (item.frequency or "").lower() in set(frequencies)
    ]
    markets: list[KalshiMarketCandidate] = []
    findings: list[str] = []
    if not series:
        findings.append("no_crypto_series")
    if not any(item.selected for item in series):
        findings.append("no_target_asset_series")
    if not selected_series:
        findings.append("no_target_frequency_series")
    for item in selected_series:
        try:
            market_payloads = market_loader(item.ticker)
        except Exception as exc:  # noqa: BLE001
            findings.append(f"market_fetch_failed:{item.ticker}:{type(exc).__name__}")
            continue
        markets.extend(_market_candidate(row, sampled_at=datetime.now(tz=UTC)) for row in market_payloads)
    active_markets = [item for item in markets if _is_open_market(item)]
    frequency_pairs = [
        (item.frequency, _frequency_seconds(item.frequency))
        for item in selected_series
        if _frequency_seconds(item.frequency) is not None
    ]
    frequency_pairs.sort(key=lambda item: item[1] or 10**12)
    if not active_markets:
        findings.append("no_active_target_markets")
    if not any(_has_quote(item) for item in active_markets):
        findings.append("no_active_markets_with_quotes")
    return KalshiDiscoveryReport(
        status="ok" if not findings else "warn",
        built_at=datetime.now(tz=UTC).isoformat(),
        output_root=str(output_root) if output_root is not None else None,
        base_url=base_url,
        category=category,
        target_assets=assets,
        target_frequencies=frequencies,
        series_count=len(series),
        selected_series_count=len(selected_series),
        market_count=len(markets),
        active_market_count=len(active_markets),
        shortest_frequency=frequency_pairs[0][0] if frequency_pairs else None,
        shortest_frequency_seconds=frequency_pairs[0][1] if frequency_pairs else None,
        findings=sorted(set(findings)),
        series=series,
        markets=sorted(active_markets, key=_market_sort_key),
    )


def collect_kalshi_crypto_quotes(
    *,
    output_root: Path | object = _DEFAULT_ROOT,
    normalized_root: Path | None | object = _DEFAULT_ROOT,
    category: str = DEFAULT_KALSHI_CATEGORY,
    target_assets: list[str] | None = None,
    target_frequencies: list[str] | None = None,
    duration_seconds: float = DEFAULT_KALSHI_DURATION_SECONDS,
    sample_count: int | None = None,
    poll_interval_seconds: float = DEFAULT_KALSHI_POLL_INTERVAL_SECONDS,
    stale_after_seconds: float = DEFAULT_KALSHI_STALE_AFTER_SECONDS,
    markets_per_series: int = DEFAULT_KALSHI_MARKETS_PER_SERIES,
    client: Any | None = None,
    jsonl_fsync: bool = True,
    normalized_parquet: bool = True,
    fsync_interval_events: int = 1,
    fsync_interval_ms: float = 0.0,
) -> KalshiCollectionSummary:
    if duration_seconds <= 0:
        raise ValueError("duration_seconds must be positive")
    if sample_count is not None and sample_count <= 0:
        raise ValueError("sample_count must be positive")
    if poll_interval_seconds < 0:
        raise ValueError("poll_interval_seconds must be non-negative")
    if stale_after_seconds <= 0:
        raise ValueError("stale_after_seconds must be positive")
    if markets_per_series <= 0:
        raise ValueError("markets_per_series must be positive")

    api = client or KalshiPublicRestClient()
    resolved_output_root = default_kalshi_output_root() if output_root is _DEFAULT_ROOT else output_root
    resolved_normalized_root = (
        default_kalshi_normalized_root() if normalized_root is _DEFAULT_ROOT else normalized_root
    )
    assets = _normalize_assets(target_assets)
    frequencies = _normalize_frequencies(target_frequencies)
    run_paths = prepare_run_paths(output_root=resolved_output_root, source="kalshi_crypto_quotes")
    # One durability posture shared by all three data sinks (metrics stays per-event).
    sink_durability = {
        "fsync": jsonl_fsync,
        "fsync_interval_events": fsync_interval_events,
        "fsync_interval_ms": fsync_interval_ms,
    }
    raw_sink = RotatingJsonlSink(run_paths.raw, "messages.jsonl", **sink_durability)
    clean_sink = JsonlSink(run_paths.clean, "events.jsonl", **sink_durability)
    quarantine_sink = JsonlSink(run_paths.quarantine, "events.jsonl", **sink_durability)
    metrics_sink = JsonlSink(run_paths.metrics, "summary.jsonl")
    parquet_sink = (
        ParquetDatasetSink(resolved_normalized_root)
        if resolved_normalized_root is not None and normalized_parquet
        else None
    )
    finding_set: set[str] = set()
    last_quote_ids: dict[str, str] = {}
    last_quote_state_seen_at: dict[str, float] = {}
    counters = {
        "raw_messages": 0,
        "clean_events": 0,
        "error_events": 0,
        "quote_count": 0,
        "first_observed_quote_count": 0,
        "quote_update_count": 0,
        "repeated_quote_count": 0,
        "stale_quote_count": 0,
        "market_fetch_count": 0,
    }
    started_at: str | None = None
    ended_at: str | None = None
    selected_series: list[KalshiSeriesCandidate] = []
    all_series: list[KalshiSeriesCandidate] = []
    samples_done = 0
    deadline = time.monotonic() + duration_seconds
    try:
        series_response = _fetch_series_response(api, category=category)
        raw_sink.write(series_response.to_raw_row())
        counters["raw_messages"] += 1
        all_series = [_series_candidate(row, target_assets=assets) for row in _extract_series(series_response.payload)]
        selected_series = [
            item for item in all_series if item.selected and (item.frequency or "").lower() in set(frequencies)
        ]
        if not selected_series:
            finding_set.add("no_target_series")

        while selected_series and time.monotonic() < deadline:
            samples_done += 1
            sample_started = datetime.now(tz=UTC)
            started_at = started_at or sample_started.isoformat()
            for series in selected_series:
                market_payloads, responses = _load_markets(
                    api,
                    series_ticker=series.ticker,
                    markets_per_series=markets_per_series,
                )
                for response in responses:
                    raw_sink.write(response.to_raw_row())
                    counters["raw_messages"] += 1
                    if response.success:
                        counters["market_fetch_count"] += 1
                    else:
                        finding_set.add("market_fetch_errors")
                        error_row = _error_event_row(
                            series=series,
                            response=response,
                            sample_index=samples_done,
                        )
                        clean_sink.write(error_row)
                        counters["clean_events"] += 1
                        counters["error_events"] += 1
                        if parquet_sink is not None:
                            parquet_sink.write(error_row)
                for payload in market_payloads:
                    market = _market_candidate(payload, sampled_at=datetime.now(tz=UTC))
                    if market.series_ticker is None:
                        market.series_ticker = series.ticker
                    if not _is_open_market(market):
                        continue
                    for row in _quote_rows_for_market(
                        market,
                        frequency=series.frequency,
                        sample_index=samples_done,
                        response=responses[-1],
                        stale_after_seconds=stale_after_seconds,
                        last_quote_ids=last_quote_ids,
                        last_quote_state_seen_at=last_quote_state_seen_at,
                    ):
                        clean_sink.write(row)
                        counters["clean_events"] += 1
                        counters["quote_count"] += 1
                        if row["quote_first_observed"]:
                            counters["first_observed_quote_count"] += 1
                        if row["quote_changed"]:
                            counters["quote_update_count"] += 1
                        if row["quote_repeated"]:
                            counters["repeated_quote_count"] += 1
                        if row["quote_stale"]:
                            counters["stale_quote_count"] += 1
                        if parquet_sink is not None:
                            parquet_sink.write(row)
            ended_at = datetime.now(tz=UTC).isoformat()
            if sample_count is not None and samples_done >= sample_count:
                break
            sleep_seconds = poll_interval_seconds - (datetime.now(tz=UTC) - sample_started).total_seconds()
            # Cap the sleep at the time remaining instead of skipping it: skipping
            # made the loop poll back-to-back (unthrottled) for the final
            # poll_interval of every run once a full sleep no longer fit.
            sleep_for = min(sleep_seconds, deadline - time.monotonic())
            if sleep_for > 0:
                time.sleep(sleep_for)
    except KalshiApiError as exc:
        raw_sink.write(exc.response.to_raw_row())
        counters["raw_messages"] += 1
        counters["error_events"] += 1
        finding_set.add("api_errors")
    finally:
        # An exception other than the handled KalshiApiError (TypeError, Ctrl-C, ...)
        # is still in flight when this block runs — record the run honestly instead
        # of writing status="ok" for an aborted run.
        aborted = sys.exc_info()[0] is not None
        if aborted:
            finding_set.add("aborted_run")
        if counters["quote_count"] == 0:
            finding_set.add("no_quote_rows")
        summary = KalshiCollectionSummary(
            status="error" if aborted else ("ok" if not finding_set else "warn"),
            built_at=datetime.now(tz=UTC).isoformat(),
            run_path=str(run_paths.base),
            raw_path=str(run_paths.raw / "messages.jsonl"),
            clean_path=str(run_paths.clean / "events.jsonl"),
            normalized_root=str(resolved_normalized_root) if resolved_normalized_root is not None and normalized_parquet else None,
            base_url=getattr(api, "base_url", DEFAULT_KALSHI_BASE_URL),
            category=category,
            target_assets=assets,
            target_frequencies=frequencies,
            poll_interval_seconds=poll_interval_seconds,
            stale_after_seconds=stale_after_seconds,
            markets_per_series=markets_per_series,
            sample_count=samples_done,
            series_count=len(all_series),
            selected_series_count=len(selected_series),
            side_symbol_count=len(last_quote_ids),
            started_at=started_at,
            ended_at=ended_at,
            findings=sorted(finding_set),
            **counters,
        )
        try:
            try:
                # Flush buffered normalized rows BEFORE the summary writes, and keep
                # the summary writes in a finally: previously a failing metrics write
                # skipped both the parquet flush (dropping up to a full batch of
                # normalized rows) and every sink close() (losing batched-fsync tails).
                if parquet_sink is not None:
                    parquet_sink.flush()
            finally:
                metrics_sink.write({**summary.to_dict(), "partial": False})
                (run_paths.metrics / "collection_summary.json").write_text(
                    json.dumps(summary.to_dict(), indent=2, sort_keys=True),
                    encoding="utf-8",
                )
        finally:
            for sink in (raw_sink, clean_sink, quarantine_sink, metrics_sink):
                close = getattr(sink, "close", None)
                if callable(close):
                    close()
    return summary


def summarize_kalshi_quote_rows(input_path: Path) -> dict[str, Any]:
    rows = [
        row for row in _load_jsonl_rows(input_path)
        if str(row.get("venue") or row.get("source") or "").lower() == "kalshi"
        and str(row.get("event_type") or "").lower() == "quote"
    ]
    by_symbol: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        symbol = str(row.get("symbol") or "")
        quote_id = row.get("quote_id")
        if not symbol or not quote_id:
            continue
        by_symbol.setdefault(symbol, []).append(row)
    quote_update_count = 0
    for symbol_rows in by_symbol.values():
        previous: str | None = None
        for row in sorted(symbol_rows, key=lambda item: str(item.get("event_time") or item.get("sampled_at") or "")):
            quote_id = str(row.get("quote_id") or "")
            if previous is None:
                previous = quote_id
                continue
            if quote_id != previous:
                quote_update_count += 1
                previous = quote_id
    return {
        "input_path": str(input_path),
        "quote_count": len(rows),
        "side_symbol_count": len(by_symbol),
        "quote_update_count": quote_update_count,
        "first_observed_quote_count": len(by_symbol),
        "repeated_quote_count": sum(1 for row in rows if row.get("quote_repeated")),
        "stale_quote_count": sum(1 for row in rows if row.get("quote_stale")),
        "symbols": sorted(by_symbol),
    }


def write_kalshi_discovery_report(report: KalshiDiscoveryReport, *, output_root: Path) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    timestamp = _safe_timestamp(report.built_at)
    payload = json.dumps(report.to_dict(), indent=2, sort_keys=True)
    markdown = render_kalshi_discovery_markdown(report)
    (output_root / f"kalshi_crypto_discovery_{timestamp}.json").write_text(payload, encoding="utf-8")
    (output_root / f"kalshi_crypto_discovery_{timestamp}.md").write_text(markdown, encoding="utf-8")
    (output_root / "kalshi_crypto_discovery_latest.json").write_text(payload, encoding="utf-8")
    (output_root / "kalshi_crypto_discovery_latest.md").write_text(markdown, encoding="utf-8")


def render_kalshi_discovery_markdown(report: KalshiDiscoveryReport) -> str:
    lines = [
        "# Kalshi Crypto Binary Market Discovery",
        "",
        f"- Built at: `{report.built_at}`",
        f"- Status: `{report.status}`",
        f"- Category: `{report.category}`",
        f"- Target assets: `{','.join(report.target_assets)}`",
        f"- Target frequencies: `{','.join(report.target_frequencies)}`",
        f"- Series: `{report.series_count}` / selected `{report.selected_series_count}`",
        f"- Markets: `{report.market_count}` / active `{report.active_market_count}`",
        f"- Shortest frequency: `{report.shortest_frequency or ''}` / `{_fmt(report.shortest_frequency_seconds)}` seconds",
        f"- Findings: `{','.join(report.findings) if report.findings else 'none'}`",
        "",
        "## Selected Series",
        "",
        "| Ticker | Title | Frequency | Tags | Selected | Reason |",
        "|---|---|---|---|---:|---|",
    ]
    for item in [row for row in report.series if row.selected]:
        lines.append(
            f"| {item.ticker} | {item.title or ''} | {item.frequency or ''} | "
            f"{','.join(item.tags)} | {item.selected} | {item.selection_reason} |"
        )
    lines.extend(
        [
            "",
            "## Active Markets",
            "",
            "| Ticker | Series | Event | Title | Close | Yes Bid | Yes Ask | No Bid | No Ask | Volume | OI | Findings |",
            "|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---|",
        ]
    )
    for item in report.markets[:100]:
        lines.append(
            f"| {item.ticker} | {item.series_ticker or ''} | {item.event_ticker or ''} | "
            f"{item.title or ''} | {item.close_time or ''} | {_fmt(item.yes_bid)} | "
            f"{_fmt(item.yes_ask)} | {_fmt(item.no_bid)} | {_fmt(item.no_ask)} | "
            f"{_fmt(item.volume)} | {_fmt(item.open_interest)} | {','.join(item.findings)} |"
        )
    lines.extend(
        [
            "",
            "## Scope",
            "",
            "- Public discovery and quote snapshots only.",
            "- No orders are placed and no credentials are required.",
            "- Repeated REST snapshots are samples, not proof of live quote changes.",
            "",
        ]
    )
    return "\n".join(lines)


def _fetch_series_response(api: Any, *, category: str) -> KalshiApiResponse:
    fetch = getattr(api, "fetch_series", None)
    if callable(fetch):
        return fetch(category=category)
    request_sent_at = datetime.now(tz=UTC)
    started = time.monotonic()
    rows = api.series(category=category)
    response_received_at = datetime.now(tz=UTC)
    return KalshiApiResponse(
        endpoint="/series",
        params={"category": category},
        url=getattr(api, "base_url", "fake") + "/series",
        payload={"series": rows},
        request_sent_at=request_sent_at,
        response_received_at=response_received_at,
        latency_ms=(time.monotonic() - started) * 1000.0,
        http_status=200,
    )


def _load_markets(
    api: Any,
    *,
    series_ticker: str,
    markets_per_series: int,
) -> tuple[list[dict[str, Any]], list[KalshiApiResponse]]:
    fetch = getattr(api, "fetch_markets", None)
    if not callable(fetch):
        request_sent_at = datetime.now(tz=UTC)
        started = time.monotonic()
        rows = api.markets(series_ticker=series_ticker, limit=markets_per_series)
        response_received_at = datetime.now(tz=UTC)
        return rows, [
            KalshiApiResponse(
                endpoint="/markets",
                params={"series_ticker": series_ticker, "status": "open", "limit": markets_per_series},
                url=getattr(api, "base_url", "fake") + "/markets",
                payload={"markets": rows},
                request_sent_at=request_sent_at,
                response_received_at=response_received_at,
                latency_ms=(time.monotonic() - started) * 1000.0,
                http_status=200,
            )
        ]
    rows: list[dict[str, Any]] = []
    responses: list[KalshiApiResponse] = []
    cursor: str | None = None
    # Decide cursor support from the signature instead of an exception-driven
    # TypeError fallback: under the old `except TypeError` retry, (a) a genuine
    # TypeError raised INSIDE fetch silently re-fetched and duplicated page 1, and
    # (b) a KalshiApiError raised by the fallback call escaped the sibling except
    # clause (exceptions inside an except handler skip its siblings) and aborted
    # the whole sampling iteration.
    passes_cursor = _fetch_accepts_cursor(fetch)
    while True:
        try:
            if passes_cursor:
                response = fetch(
                    series_ticker=series_ticker, status="open", limit=markets_per_series, cursor=cursor
                )
            else:
                response = fetch(series_ticker=series_ticker, status="open", limit=markets_per_series)
        except KalshiApiError as exc:
            # Keep the pages already received: their markets are real data and their
            # responses must still reach the raw archive — only the failing page is
            # represented by the error response.
            return rows[:markets_per_series], [*responses, exc.response]
        responses.append(response)
        payload = response.payload if isinstance(response.payload, dict) else {}
        values = payload.get("markets") or []
        rows.extend(item for item in values if isinstance(item, dict))
        cursor = _optional_str(payload.get("cursor"))
        if not cursor or len(rows) >= markets_per_series:
            return rows[:markets_per_series], responses


@functools.lru_cache(maxsize=8)
def _fetch_accepts_cursor(fetch: Any) -> bool:
    """True when `fetch` can take a `cursor=` kwarg (named param or **kwargs).
    Unknowable signatures default to True (the real client accepts it). Cached:
    _load_markets runs once per series per sample (every few seconds for the whole
    run) against the same client, and the answer never changes for a given fetch."""
    try:
        parameters = inspect.signature(fetch).parameters.values()
    except (TypeError, ValueError):
        return True
    return any(
        param.name == "cursor" or param.kind is inspect.Parameter.VAR_KEYWORD
        for param in parameters
    )


def _extract_series(payload: dict[str, Any]) -> list[dict[str, Any]]:
    # `or []`: a dict payload missing the key (or carrying null) must yield no
    # series, not a TypeError from iterating None.
    values = (payload.get("series") or []) if isinstance(payload, dict) else []
    return [item for item in values if isinstance(item, dict)]


def _series_candidate(row: dict[str, Any], *, target_assets: list[str]) -> KalshiSeriesCandidate:
    ticker = str(row.get("ticker") or "")
    title = _optional_str(row.get("title"))
    tags = [str(item).upper() for item in row.get("tags") or []]
    haystack = " ".join([ticker, title or "", *tags]).upper()
    matched_assets = [asset for asset in target_assets if asset in haystack]
    selected = bool(matched_assets)
    return KalshiSeriesCandidate(
        ticker=ticker,
        title=title,
        category=_optional_str(row.get("category")),
        frequency=_optional_str(row.get("frequency")),
        tags=tags,
        volume=_optional_float(row.get("volume_fp") or row.get("volume")),
        selected=selected,
        selection_reason="asset_match:" + ",".join(matched_assets) if selected else "no_target_asset_match",
    )


def _market_candidate(row: dict[str, Any], *, sampled_at: datetime) -> KalshiMarketCandidate:
    close_time = _parse_dt(row.get("close_time"))
    yes_bid = _price(row, "yes_bid")
    yes_ask = _price(row, "yes_ask")
    no_bid = _price(row, "no_bid")
    no_ask = _price(row, "no_ask")
    findings: list[str] = []
    if yes_ask in (None, 0.0) and no_ask in (None, 0.0):
        findings.append("empty_best_ask")
    return KalshiMarketCandidate(
        ticker=str(row.get("ticker") or ""),
        series_ticker=_optional_str(row.get("series_ticker")),
        event_ticker=_optional_str(row.get("event_ticker")),
        title=_optional_str(row.get("title")),
        yes_sub_title=_optional_str(row.get("yes_sub_title")),
        status=_optional_str(row.get("status")),
        market_type=_optional_str(row.get("market_type")),
        close_time=_optional_str(row.get("close_time")),
        expiration_time=_optional_str(row.get("expiration_time")),
        seconds_to_close=(close_time - sampled_at).total_seconds() if close_time is not None else None,
        yes_bid=yes_bid,
        yes_ask=yes_ask,
        no_bid=no_bid,
        no_ask=no_ask,
        yes_ask_size=_optional_float(row.get("yes_ask_size_fp") or row.get("yes_ask_size")),
        no_ask_size=_optional_float(row.get("no_ask_size_fp") or row.get("no_ask_size")),
        volume=_optional_float(row.get("volume_fp") or row.get("volume")),
        volume_24h=_optional_float(row.get("volume_24h_fp") or row.get("volume_24h")),
        open_interest=_optional_float(row.get("open_interest_fp") or row.get("open_interest")),
        liquidity=_optional_float(row.get("liquidity_dollars") or row.get("liquidity")),
        fractional_trading_enabled=_optional_bool(row.get("fractional_trading_enabled")),
        findings=findings,
    )


def _quote_rows_for_market(
    market: KalshiMarketCandidate,
    *,
    frequency: str | None,
    sample_index: int,
    response: KalshiApiResponse,
    stale_after_seconds: float,
    last_quote_ids: dict[str, str],
    last_quote_state_seen_at: dict[str, float],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    sampled_at = response.response_received_at
    for side, bid, ask, ask_size in [
        ("YES", market.yes_bid, market.yes_ask, market.yes_ask_size),
        ("NO", market.no_bid, market.no_ask, market.no_ask_size),
    ]:
        if ask is None or ask <= 0.0:
            continue
        symbol = f"{market.ticker}:{side}".upper()
        quote_basis = "|".join([symbol, _quote_part(bid), _quote_part(ask), _quote_part(ask_size), market.close_time or ""])
        quote_id = hashlib.sha256(quote_basis.encode("utf-8")).hexdigest()
        now_monotonic = time.monotonic()
        previous_quote_id = last_quote_ids.get(symbol)
        first_observed = previous_quote_id is None
        quote_changed = previous_quote_id is not None and previous_quote_id != quote_id
        quote_repeated = previous_quote_id == quote_id
        if first_observed or quote_changed:
            last_quote_ids[symbol] = quote_id
            last_quote_state_seen_at[symbol] = now_monotonic
            quote_age_ms = 0.0
        else:
            quote_age_ms = (now_monotonic - last_quote_state_seen_at.get(symbol, now_monotonic)) * 1000.0
        row = {
            "source": "kalshi",
            "venue": "kalshi",
            "event_type": "quote",
            "event_time": sampled_at.isoformat(),
            "sampled_at": sampled_at.isoformat(),
            "request_sent_at": response.request_sent_at.isoformat(),
            "response_received_at": response.response_received_at.isoformat(),
            "latency_ms": response.latency_ms,
            "series_ticker": market.series_ticker,
            "event_ticker": market.event_ticker,
            "market_ticker": market.ticker,
            "product": symbol,
            "symbol": symbol,
            "instrument": {
                "instrument_id": f"binary_option:kalshi:{symbol}",
                "canonical_symbol": symbol,
            },
            "side": side,
            "frequency": frequency,
            "sample_index": sample_index,
            "close_time": market.close_time,
            "expiration_time": market.expiration_time,
            "seconds_to_close": market.seconds_to_close,
            "bid": bid,
            "ask": ask,
            "ask_size": ask_size,
            "payout_ratio": (1.0 - ask) / ask,
            "quote_id": quote_id,
            "quote_age_ms": quote_age_ms,
            "max_quote_age_ms": stale_after_seconds * 1000.0,
            "quote_stale": quote_age_ms > stale_after_seconds * 1000.0,
            "quote_first_observed": first_observed,
            "quote_changed": quote_changed,
            "quote_repeated": quote_repeated,
            "volume": market.volume,
            "volume_24h": market.volume_24h,
            "open_interest": market.open_interest,
            "liquidity": market.liquidity,
            "fractional_trading_enabled": market.fractional_trading_enabled,
            "success": response.success,
            "rate_limited": response.http_status == 429,
            "http_status": response.http_status,
            "error_type": response.error_type,
            "error": response.error,
        }
        rows.append(row)
    return rows


def _error_event_row(*, series: KalshiSeriesCandidate, response: KalshiApiResponse, sample_index: int) -> dict[str, Any]:
    return {
        "source": "kalshi",
        "venue": "kalshi",
        "event_type": "api_error",
        "event_time": response.response_received_at.isoformat(),
        "sampled_at": response.response_received_at.isoformat(),
        "request_sent_at": response.request_sent_at.isoformat(),
        "response_received_at": response.response_received_at.isoformat(),
        "latency_ms": response.latency_ms,
        "series_ticker": series.ticker,
        "product": series.ticker,
        "symbol": series.ticker,
        "sample_index": sample_index,
        "success": False,
        "rate_limited": response.http_status == 429,
        "http_status": response.http_status,
        "error_type": response.error_type,
        "error": response.error,
    }


def _load_jsonl_rows(input_path: Path) -> list[dict[str, Any]]:
    if input_path.is_dir():
        if (input_path / "clean" / "events.jsonl").exists():
            paths = [input_path / "clean" / "events.jsonl"]
        else:
            paths = sorted(input_path.rglob("*.jsonl"))
    else:
        paths = [input_path]
    rows: list[dict[str, Any]] = []
    for path in paths:
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            payload = json.loads(line)
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def _is_open_market(market: KalshiMarketCandidate) -> bool:
    return (market.status or "").lower() in {"active", "open"}


def _has_quote(market: KalshiMarketCandidate) -> bool:
    return (market.yes_ask or 0.0) > 0.0 or (market.no_ask or 0.0) > 0.0


def _market_sort_key(item: KalshiMarketCandidate) -> tuple[float, str]:
    return (item.seconds_to_close if item.seconds_to_close is not None else float("inf"), item.ticker)


def _normalize_assets(values: list[str] | None) -> list[str]:
    return [str(item).upper() for item in (values or DEFAULT_KALSHI_TARGET_ASSETS)]


def _normalize_frequencies(values: list[str] | None) -> list[str]:
    return [str(item).lower() for item in (values or DEFAULT_KALSHI_TARGET_FREQUENCIES)]


def _frequency_seconds(value: str | None) -> int | None:
    return {
        "fifteen_min": 900,
        "hourly": 3600,
        "daily": 86400,
        "weekly": 604800,
        "monthly": 2_592_000,
        "annual": 31_536_000,
    }.get((value or "").lower())


def _price(row: dict[str, Any], key: str) -> float | None:
    dollar_value = _optional_float(row.get(f"{key}_dollars"))
    if dollar_value is not None:
        return dollar_value
    raw = row.get(key)
    value = _optional_float(raw)
    if value is None:
        return None
    # The API's bare price fields are integer CENTS (1..100). Integers always divide:
    # the old magnitude heuristic (`> 1.0`) read a 1-cent quote as $1.00 — and 1c is
    # exactly where deep-OTM near-expiry binary quotes sit. The magnitude fallback
    # remains only for non-int values that may already be dollars.
    if isinstance(raw, int) and not isinstance(raw, bool):
        return value / 100.0
    return value / 100.0 if abs(value) > 1.0 else value


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _optional_str(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _optional_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int | float):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
    return None


def _parse_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.astimezone(UTC)
    if not value:
        return None
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
        except ValueError:
            return None
    return None


def _quote_part(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.10f}"
    return str(value)


def _safe_timestamp(value: str) -> str:
    return value.replace(":", "").replace("-", "").replace("+", "Z").replace(".", "_")


def _fmt(value: float | int | None) -> str:
    if value is None:
        return ""
    return f"{float(value):.6f}"
