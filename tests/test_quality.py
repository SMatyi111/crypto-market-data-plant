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


def test_quality_gate_session_id_isolates_per_run_sequence_state() -> None:
    older_run = QualityGate(session_id="20260101_000000")
    older_run.validate(make_event(sequence=999_999))
    newer_run = QualityGate(session_id="20260102_000000")
    # A fresh session starting from a smaller sequence (e.g. exchange-side reset)
    # must not see the prior run's high-water mark.
    assert newer_run.validate(make_event(sequence=1)).accepted is True


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


def test_quality_gate_quarantines_subscribe_replay_tagged_events() -> None:
    """Prints a venue re-delivers at subscribe time (Kraken's trade snapshot frame,
    Coinbase's last_match) are tagged by the normalizers; the gate must keep them out
    of clean — promotion has no cross-run row dedup, so letting them through landed
    duplicate prints in curated trades_replayable on every segment boundary."""
    gate = QualityGate()
    event = make_event(channel="trades", event_type="trade", metadata={"subscribe_replay": True})
    result = gate.validate(event)
    assert result.accepted is False
    assert "subscribe_replay" in result.reasons
    # Untagged (live) prints are unaffected.
    assert gate.validate(make_event(channel="trades", event_type="trade", sequence=2)).accepted


def test_quality_gate_rejects_zero_or_missing_price_size_on_trades_channel() -> None:
    """The promotion bar (replay_trades_run) fails a WHOLE run when any clean print has
    a missing or non-positive price/size; the live gate must quarantine those per-event
    so one odd print doesn't cost the full segment at scoring time."""
    gate = QualityGate()
    assert "invalid_trade_size" in gate.validate(
        make_event(channel="trades", event_type="trade", size=0.0)
    ).reasons
    assert "invalid_trade_size" in gate.validate(
        make_event(channel="trades", event_type="trade", size=None)
    ).reasons
    assert "invalid_trade_price" in gate.validate(
        make_event(channel="trades", event_type="trade", price=None)
    ).reasons
    # Coinbase's event types count as prints too (channel is the discriminator).
    assert "invalid_trade_size" in gate.validate(
        make_event(channel="trades", event_type="match", size=0.0)
    ).reasons
    # Non-trades channels keep the old semantics: size 0 / None price stay acceptable
    # (L3 lifecycle events legitimately carry them).
    assert gate.validate(make_event(channel="full", event_type="done", size=0.0)).accepted
    assert gate.validate(
        make_event(channel="full", event_type="open", price=None, sequence=2)
    ).accepted


def test_subscribe_replay_above_run_high_water_heals_reconnect_gap() -> None:
    """A replayed print ABOVE the run's sequence cursor is genuinely new data (it
    covers a mid-run reconnect window). Quarantining it would punch a provable id
    gap into clean and fail the WHOLE run at scoring — so it must pass, while
    replays at/below the cursor (and all segment-start replays, cursor empty)
    stay quarantined."""
    gate = QualityGate(session_id="run-1")
    # Live capture up to id 100.
    assert gate.validate(make_event(channel="trades", event_type="trade", sequence=100)).accepted
    # Mid-run reconnect: the venue replays ids 99-102 in its snapshot frame.
    replay = {"subscribe_replay": True}
    assert "subscribe_replay" in gate.validate(
        make_event(channel="trades", event_type="trade", sequence=99, metadata=dict(replay))
    ).reasons
    assert "subscribe_replay" in gate.validate(
        make_event(channel="trades", event_type="trade", sequence=100, metadata=dict(replay))
    ).reasons
    # 101-102 printed while disconnected — only the replay carries them.
    assert gate.validate(
        make_event(channel="trades", event_type="trade", sequence=101, metadata=dict(replay))
    ).accepted
    assert gate.validate(
        make_event(channel="trades", event_type="trade", sequence=102, metadata=dict(replay))
    ).accepted
    # Cursor advanced through the replayed prints: live stream resumes seamlessly.
    assert gate.validate(make_event(channel="trades", event_type="trade", sequence=103)).accepted

    # Segment start (fresh gate, no cursor): replays quarantine wholesale — their
    # originals live in the previous run and promotion has no cross-run dedup.
    fresh = QualityGate(session_id="run-2")
    assert "subscribe_replay" in fresh.validate(
        make_event(channel="trades", event_type="trade", sequence=50, metadata=dict(replay))
    ).reasons
