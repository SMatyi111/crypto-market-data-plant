from __future__ import annotations

from crypto_collector.models import RawMessage, utc_now
from crypto_collector.pipeline import _normalize_events


class _SingleEventNormalizer:
    """Mirrors the existing Binance/Coinbase normalizers: one frame -> one event."""

    def __init__(self) -> None:
        self.calls: list[RawMessage] = []

    def normalize(self, raw: RawMessage) -> str:
        self.calls.append(raw)
        return f"event:{raw.payload['id']}"


class _BatchedNormalizer:
    """Mirrors Bybit/Kraken: one frame's `data` array -> many events."""

    def normalize_many(self, raw: RawMessage) -> list[str]:
        return [f"event:{item}" for item in raw.payload["data"]]


def _raw(payload: dict) -> RawMessage:
    return RawMessage(source="test", received_at=utc_now(), payload=payload)


def test_normalize_events_wraps_single_event_normalizer() -> None:
    # Backward compatibility: a normalizer exposing only `normalize` must still yield
    # exactly one event so the live Binance/Coinbase lanes are unaffected.
    normalizer = _SingleEventNormalizer()
    events = _normalize_events(normalizer, _raw({"id": 7}))
    assert events == ["event:7"]
    assert len(normalizer.calls) == 1


def test_normalize_events_fans_out_batched_normalizer() -> None:
    # A `normalize_many` normalizer fans one frame out to several events, which is how
    # the batched venues (Bybit publicTrade, Kraken trade) deliver trades.
    events = _normalize_events(_BatchedNormalizer(), _raw({"data": [1, 2, 3]}))
    assert events == ["event:1", "event:2", "event:3"]


def test_normalize_events_prefers_normalize_many_when_both_present() -> None:
    class _Both:
        def normalize(self, raw: RawMessage) -> str:
            return "single"

        def normalize_many(self, raw: RawMessage) -> list[str]:
            return ["a", "b"]

    assert _normalize_events(_Both(), _raw({})) == ["a", "b"]


def test_normalize_events_empty_batch_yields_nothing() -> None:
    # A frame whose data array is empty (e.g. a keepalive-shaped trade frame) must not
    # raise and must contribute zero events.
    assert _normalize_events(_BatchedNormalizer(), _raw({"data": []})) == []
