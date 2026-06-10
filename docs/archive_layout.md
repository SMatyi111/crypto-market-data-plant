# Archive Layout

The archive is split by data lifecycle.

## Raw Runs

Raw collection runs are timestamped directories under:

```text
G:\market_archive\raw\market\<source>\<run_id>
```

`<source>` is the **lane** directory `<venue>_<dataset>[_<suffix>]`:
`binance_depth`, `binance_trades`, `coinbase_trades`, `coinbase_depth`,
`kraken_trades`, `kraken_depth`, `bybit_trades`, `bybit_depth`, `mexc_trades`,
`mexc_depth`, `okx_trades`, `okx_depth`, plus an optional per-instrument suffix
(`binance_trades_btcusdc`). Perp lanes get their own `<venue>_perp_<dataset>`
directories (`bybit_perp_trades`, `okx_perp_depth`, `binance_perp_trades`,
`binance_perp_depth`, `binance_perp_funding`).

Aged raw runs are verify-moved to the cold tier `D:\market_archive_cold` by the
`archive-offload` ops job once promoted or quarantined; every move is recorded in
the lane's `_offload_index.jsonl` (see [`../STANDARDS.md`](../STANDARDS.md) §7).

Each run contains:

- `raw/messages.jsonl`: exact received source payloads with receive timestamps
- `clean/events.jsonl`: normalized events accepted by quality gates
- `quarantine/events.jsonl`: normalized events rejected by quality gates, with reasons
- `metrics/summary.jsonl`: collection counters and quality metrics
- `metrics/replay_summary.json`: the curation verdict (`replayable` + `gap_detection`),
  for every run
- `snapshots/book_snapshot.json`: depth REST snapshot anchor, for Binance depth
  runs only (other depth feeds carry the snapshot in-stream)

## Normalized Datasets

Normalized datasets are append-only Parquet datasets (every collected run, before
curation):

```text
G:\market_archive\normalized\<dataset>\schema_version=v2\source=<venue>\instrument=<canonical>\event_date=<YYYY-MM-DD>
```

Datasets:

- `market`: depth (order book) updates from all venues
- `trades`: public trades from all venues
- `funding`: perp funding / mark-price metric rows (Binance USDT-M REST poll)
- `binary_options`: Kalshi crypto binary-option quote telemetry

Since the `schema_version=v2` cutover the path carries an `instrument=` partition
(the sanitized canonical symbol, e.g. `BTC-USDT`), so per-instrument lanes of the
same venue no longer share partitions — pull by `(venue, instrument, event_date)`.
Legacy `v1` data (venue-only, no `instrument=` level) coexists for pre-cutover
history. The resolved `InstrumentRef` detail is kept in the `instrument_ref` column.

## Curated Datasets

Curated datasets contain only quality-gated research inputs:

```text
G:\market_archive\curated\research\market_replayable     # depth
G:\market_archive\curated\research\trades_replayable     # trades
G:\market_archive\curated\research\funding                # perp funding/mark-price
G:\market_archive\curated\research\manifests              # readiness manifests
```

A run is promoted only when its `metrics/replay_summary.json` marks it
`replayable: true` and it is not listed in the quarantine index. **What
"replayable" guarantees depends on the feed's `gap_detection` class**
(`sequence` = provable gaplessness; `none_native` = structurally clean only) —
see [`../STANDARDS.md`](../STANDARDS.md) §4 for the per-class definition.

The live ops config writes `research_manifest_latest.json`,
`research_manifest_latest.md`, and timestamped manifest snapshots to
`G:\market_archive\curated\research\manifests`. Treat
`G:\market_archive\manifests` as legacy/manual output unless the active config
explicitly uses it.

## Ops State

Ops files live under:

```text
G:\market_archive\ops
```

Important files:

- `heartbeat.json`
- `heartbeat_history.jsonl`
- `job_runs.jsonl`
- `worker_events.jsonl`
- `runner.log`
