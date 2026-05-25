# Archive Layout

The archive is split by data lifecycle.

## Raw Runs

Raw collection runs are timestamped directories under:

```text
D:\market_archive\raw\market\<source>\<run_id>
```

Each run contains:

- `raw/messages.jsonl`: exact received source payloads with receive timestamps
- `clean/events.jsonl`: normalized events accepted by quality gates
- `quarantine/events.jsonl`: normalized events rejected by quality gates, with reasons
- `metrics/summary.jsonl`: collection counters and quality metrics
- `metrics/replay_summary.json`: depth replay diagnostics, for depth runs
- `snapshots/book_snapshot.json`: depth snapshot anchor, for depth runs

## Normalized Datasets

Normalized datasets are append-only Parquet datasets:

```text
D:\market_archive\normalized\<dataset>\schema_version=v1\source=<source>\event_date=<YYYY-MM-DD>
```

Production datasets:

- `market`: Binance depth updates
- `trades`: Binance public trades

## Curated Datasets

Curated datasets contain only quality-gated research inputs:

```text
D:\market_archive\curated\research\market_replayable
```

Depth runs are promoted only when replay diagnostics mark them replayable and they are not listed in the quarantine index.

## Ops State

Ops files live under:

```text
D:\market_archive\ops
```

Important files:

- `heartbeat.json`
- `heartbeat_history.jsonl`
- `job_runs.jsonl`
- `worker_events.jsonl`
- `runner.log`
