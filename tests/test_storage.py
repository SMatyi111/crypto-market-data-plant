from __future__ import annotations

from pathlib import Path

import pyarrow.dataset as ds

from crypto_collector.storage import ParquetDatasetSink


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
