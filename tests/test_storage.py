from __future__ import annotations

from pathlib import Path

import pyarrow.dataset as ds

from crypto_collector.pipeline import CollectorPipeline
from crypto_collector.quality import QualityGate
from crypto_collector.storage import JsonlSink, ParquetDatasetSink, RotatingJsonlSink
from crypto_collector.storage import RunPaths


def test_parquet_dataset_sink_keeps_column_whose_first_value_is_null(tmp_path: Path) -> None:
    # Regression: a numeric column that is None in the FIRST row but populated later
    # must NOT be dropped. pa.Table.from_pylist infers a column's type from its leading
    # value and types an early-None column as `null`, silently dropping it on write —
    # which made trade_aggressor_imbalance_* / trade_vwap disappear on days whose
    # opening buckets had no trades. The sink must scan all values per column.
    sink = ParquetDatasetSink(tmp_path / "feat", schema_version="v1", batch_size=100)
    base = {"source": "research_features", "event_time": "2026-06-05T00:00:00+00:00"}
    sink.write({**base, "imbalance": None, "always": 0.0})
    sink.write({**base, "imbalance": None, "always": 0.0})
    sink.write({**base, "imbalance": -0.5, "always": 1.0})
    sink.flush()

    dataset = ds.dataset(tmp_path / "feat", format="parquet", partitioning="hive")
    names = dataset.schema.names
    assert "imbalance" in names, f"early-None column dropped; got {names}"
    nonnull = [v for v in dataset.to_table().to_pylist() if v["imbalance"] is not None]
    assert [r["imbalance"] for r in nonnull] == [-0.5]


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


def test_jsonl_sink_non_fsync_buffers_and_flushes_on_close(tmp_path: Path) -> None:
    sink = JsonlSink(tmp_path, "events.jsonl", fsync=False, flush_every=100)
    sink.write({"a": 1})
    sink.write({"a": 2})

    sink.close()

    lines = (tmp_path / "events.jsonl").read_text(encoding="utf-8").splitlines()
    assert lines == ['{"a": 1}', '{"a": 2}']


def test_pipeline_can_disable_data_jsonl_fsync_without_touching_metrics(tmp_path: Path) -> None:
    paths = RunPaths(
        base=tmp_path,
        raw=tmp_path / "raw",
        clean=tmp_path / "clean",
        quarantine=tmp_path / "quarantine",
        metrics=tmp_path / "metrics",
    )
    for path in (paths.raw, paths.clean, paths.quarantine, paths.metrics):
        path.mkdir()

    pipeline = CollectorPipeline(
        collector=object(),
        normalizer=object(),
        quality_gate=QualityGate(),
        run_paths=paths,
        jsonl_fsync=False,
    )

    assert pipeline.raw_sink._fsync is False
    assert pipeline.clean_sink._fsync is False
    assert pipeline.quarantine_sink._fsync is False
    assert pipeline.metrics_sink._fsync is True


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


def test_parquet_dataset_sink_v2_adds_instrument_partition(tmp_path: Path) -> None:
    """v2 (default) adds an `instrument=` partition keyed by the sanitized canonical
    symbol, and preserves the resolved InstrumentRef detail under `instrument_ref`."""
    sink = ParquetDatasetSink(tmp_path / "market", batch_size=10)  # default schema_version=v2
    assert sink.schema_version == "v2"
    sink.write(
        {
            "source": "binance",
            "event_time": "2026-04-06T08:00:00+00:00",
            "received_at": "2026-04-06T08:00:01+00:00",
            "product": "BTCUSDT",
            "instrument": {"instrument_id": "spot:binance:BTCUSDT", "canonical_symbol": "BTC/USDT"},
            "price": 70000.0,
        }
    )
    sink.flush()

    # The partition directory uses the sanitized canonical symbol (slash -> dash).
    assert (tmp_path / "market" / "schema_version=v2" / "source=binance").exists()
    instrument_dirs = sorted(p.name for p in (tmp_path / "market").rglob("instrument=*"))
    assert instrument_dirs == ["instrument=BTC-USDT"]

    dataset = ds.dataset(tmp_path / "market", format="parquet", partitioning="hive")
    rows = dataset.to_table().to_pylist()
    assert len(rows) == 1
    assert rows[0]["schema_version"] == "v2"
    assert rows[0]["instrument"] == "BTC-USDT"
    # The nested InstrumentRef detail is preserved under instrument_ref.
    assert rows[0]["instrument_ref"]["instrument_id"] == "spot:binance:BTCUSDT"


def test_parquet_dataset_sink_v2_instrument_falls_back_to_product_then_unknown(tmp_path: Path) -> None:
    """With no resolved instrument, the partition falls back to the venue product, then
    to 'unknown' — so every v2 row always has an instrument partition value."""
    sink = ParquetDatasetSink(tmp_path / "m", batch_size=10)
    sink.write({"source": "kraken", "received_at": "2026-04-06T08:00:01+00:00", "product": "BTC/USD"})
    sink.write({"source": "kraken", "received_at": "2026-04-06T08:00:02+00:00"})  # no product
    sink.flush()

    dataset = ds.dataset(tmp_path / "m", format="parquet", partitioning="hive")
    instruments = sorted(r["instrument"] for r in dataset.to_table().to_pylist())
    assert instruments == ["BTC-USD", "unknown"]  # product sanitized; missing -> unknown


def test_parquet_dataset_sink_v1_keeps_legacy_layout_without_instrument(tmp_path: Path) -> None:
    """A v1-tagged sink must keep the legacy 3-level layout (no instrument partition) so
    existing on-disk v1 data stays consistent."""
    sink = ParquetDatasetSink(tmp_path / "m", schema_version="v1", batch_size=10)
    sink.write(
        {
            "source": "binance",
            "received_at": "2026-04-06T08:00:01+00:00",
            "instrument": {"instrument_id": "spot:binance:BTCUSDT", "canonical_symbol": "BTC/USDT"},
        }
    )
    sink.flush()

    assert not list((tmp_path / "m").rglob("instrument=*"))  # no instrument partition dir
    rows = ds.dataset(tmp_path / "m", format="parquet", partitioning="hive").to_table().to_pylist()
    # v1 leaves the nested instrument field as-is (no instrument_ref rename).
    assert rows[0]["instrument"]["instrument_id"] == "spot:binance:BTCUSDT"


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
