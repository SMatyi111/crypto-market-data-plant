from __future__ import annotations

from datetime import UTC, datetime, timedelta

from crypto_collector.models import NormalizedL3Event
from crypto_collector.normalizer import GenericL3Normalizer
from crypto_collector.models import RawMessage
from crypto_collector.quality import QualityGate


def make_event(**overrides: object) -> NormalizedL3Event:
    base_time = datetime.now(tz=UTC)
    event = NormalizedL3Event(
        source="test",
        product="BTC-USD",
        channel="full",
        event_type="open",
        exchange_time=base_time,
        received_at=base_time,
        side="buy",
        price=100_000.0,
        size=0.01,
        order_id="abc",
        sequence=1,
    )
    for key, value in overrides.items():
        setattr(event, key, value)
    return event


def test_quality_gate_accepts_valid_event() -> None:
    gate = QualityGate()
    result = gate.validate(make_event())
    assert result.accepted is True
    assert result.reasons == []


def test_quality_gate_rejects_non_monotonic_sequence() -> None:
    gate = QualityGate()
    gate.validate(make_event(sequence=10))
    result = gate.validate(make_event(sequence=9))
    assert result.accepted is False
    assert "non_monotonic_sequence" in result.reasons


def test_quality_gate_keeps_highest_sequence_after_rejection() -> None:
    gate = QualityGate()
    assert gate.validate(make_event(sequence=10)).accepted is True
    assert gate.validate(make_event(sequence=9)).accepted is False
    result = gate.validate(make_event(sequence=9)).accepted
    assert result is False
    assert gate.validate(make_event(sequence=11)).accepted is True


def test_quality_gate_is_safe_under_concurrent_validate_calls() -> None:
    import threading

    gate = QualityGate()
    barrier = threading.Barrier(8)
    sequences_seen: list[bool] = []

    def worker(start: int) -> None:
        barrier.wait()
        for offset in range(50):
            result = gate.validate(make_event(sequence=start + offset))
            sequences_seen.append(result.accepted)

    threads = [threading.Thread(target=worker, args=(i * 1000,)) for i in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    # The accepted count is non-deterministic across threads but the gate must
    # not have raised and metrics() must remain readable.
    assert len(sequences_seen) == 8 * 50
    assert isinstance(gate.metrics(), dict)


def test_quality_gate_accepts_duplicate_sequence_as_idempotent_replay() -> None:
    gate = QualityGate()
    assert gate.validate(make_event(sequence=10)).accepted is True
    duplicate = gate.validate(make_event(sequence=10))
    assert duplicate.accepted is True
    assert "non_monotonic_sequence" not in duplicate.reasons


def test_quality_gate_rejects_clock_skew() -> None:
    gate = QualityGate(max_delay_ms=100)
    event = make_event(exchange_time=datetime.now(tz=UTC) - timedelta(seconds=1))
    result = gate.validate(event)
    assert result.accepted is False
    assert "stale_or_clock_skew" in result.reasons


def test_quality_gate_allows_bounded_future_exchange_time_skew() -> None:
    gate = QualityGate(max_delay_ms=100, max_future_skew_ms=10_000)
    event = make_event(exchange_time=datetime.now(tz=UTC) + timedelta(seconds=5))
    result = gate.validate(event)
    assert result.accepted is True


def test_quality_gate_rejects_excessive_future_exchange_time_skew() -> None:
    gate = QualityGate(max_delay_ms=100, max_future_skew_ms=1_000)
    event = make_event(exchange_time=datetime.now(tz=UTC) + timedelta(seconds=5))
    result = gate.validate(event)
    assert result.accepted is False
    assert "stale_or_clock_skew" in result.reasons


def test_normalizer_quarantines_parse_errors_instead_of_crashing() -> None:
    normalizer = GenericL3Normalizer()
    gate = QualityGate()
    raw = RawMessage(
        source="test",
        received_at=datetime.now(tz=UTC),
        payload={
            "type": "open",
            "product_id": "BTC-USD",
            "channel": "full",
            "time": "not-a-timestamp",
            "price": "broken",
            "size": "0.1",
            "sequence": "oops",
        },
    )
    event = normalizer.normalize(raw)
    result = gate.validate(event)
    assert result.accepted is False
    assert "invalid_time" in result.reasons
    assert "invalid_price" in result.reasons
    assert "invalid_sequence" in result.reasons
