from __future__ import annotations

from pathlib import Path

import pyarrow.dataset as ds

from crypto_collector.storage import JsonlSink, ParquetDatasetSink, RotatingJsonlSink


def test_rotating_jsonl_sink_rolls_files_at_byte_threshold(tmp_path: Path) -> None:
    sink = RotatingJsonlSink(tmp_path, "messages.jsonl", max_bytes=50)
    # Each row encodes to about 30 bytes including the newline.
    for i in range(6):
        sink.write({"i": i, "payload": "abcdef"})
    files = sorted(tmp_path.glob("messages*.jsonl"))
    assert len(files) >= 2
    # The active file is always messages.jsonl; rotated files use messages.N.jsonl.
    assert (tmp_path / "messages.jsonl").exists()
    rotated = [p.name for p in files if p.name != "messages.jsonl"]
    assert all(name.startswith("messages.") and name.endswith(".jsonl") for name in rotated)


def test_rotating_jsonl_sink_resumes_after_existing_rotated_files(tmp_path: Path) -> None:
    (tmp_path / "messages.1.jsonl").write_text("{}\n", encoding="utf-8")
    (tmp_path / "messages.2.jsonl").write_text("{}\n", encoding="utf-8")
    sink = RotatingJsonlSink(tmp_path, "messages.jsonl", max_bytes=10)
    sink.write({"a": 1})
    sink.write({"a": 2})
    sink.write({"a": 3})
    assert (tmp_path / "messages.3.jsonl").exists()


def test_jsonl_sink_flushes_each_write_so_partial_lines_dont_accumulate(tmp_path: Path) -> None:
    sink = JsonlSink(tmp_path, "events.jsonl")
    sink.write({"a": 1})
    sink.write({"a": 2})
    # Read without closing any other handle — every write must be durably on disk already.
    lines = (tmp_path / "events.jsonl").read_text(encoding="utf-8").splitlines()
    assert lines == ['{"a": 1}', '{"a": 2}']


def test_parquet_dataset_sink_writes_partitioned_dataset(tmp_path: Path) -> None:
    sink = ParquetDatasetSink(tmp_path / "market", schema_version="v1", batch_size=10)
    sink.write(
        {
            "source": "binance",
            "event_time": "2026-04-06T08:00:00+00:00",
            "received_at": "2026-04-06T08:00:01+00:00",
            "instrument_id": "spot:binance:BTCUSDT",
            "price": 70000.0,
        }
    )
    sink.flush()

    dataset = ds.dataset(tmp_path / "market", format="parquet", partitioning="hive")
    rows = dataset.to_table().to_pylist()
    assert len(rows) == 1
    assert rows[0]["source"] == "binance"
    assert rows[0]["schema_version"] == "v1"
    assert rows[0]["event_date"] == "2026-04-06"


def test_parquet_dataset_sink_handles_optional_fields_across_batches(tmp_path: Path) -> None:
    sink = ParquetDatasetSink(tmp_path / "trades", schema_version="v1", batch_size=1)
    sink.write(
        {
            "source": "binance",
            "event_time": "2026-04-06T08:00:00+00:00",
            "received_at": "2026-04-06T08:00:01+00:00",
            "instrument": {"instrument_id": "spot:binance:BTCUSDT"},
            "metadata": {"trade_id": "1", "buyer_is_maker": None},
        }
    )
    sink.write(
        {
            "source": "binance",
            "event_time": "2026-04-06T08:00:02+00:00",
            "received_at": "2026-04-06T08:00:03+00:00",
            "instrument": {"instrument_id": "spot:binance:BTCUSDT"},
            "metadata": {"trade_id": "2", "buyer_is_maker": True},
        }
    )
    sink.flush()

    dataset = ds.dataset(tmp_path / "trades", format="parquet", partitioning="hive")
    rows = dataset.to_table().to_pylist()
    assert len(rows) == 2
    assert sorted(row["metadata"]["trade_id"] for row in rows) == ["1", "2"]
