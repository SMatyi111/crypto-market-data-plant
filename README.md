# Crypto Market Data Plant

Research-grade public crypto market data collection for a Windows workstation.

This repo is a data plant, not a trading bot. It runs public collectors, writes durable segmented raw chunks, replays and quality-checks them, quarantines bad chunks, and promotes good chunks into deterministic curated datasets for research.

## Design Goals

- collect continuously from public endpoints
- resume automatically on Windows boot
- log every operational event and job result
- keep raw, clean, quarantine, normalized, curated, and ops data separated
- promote only replayable data into research storage
- never include API keys, signed endpoints, live orders, paper trading, or model experiments

## Supported Production Feeds

Enabled in the live deployment today:

- Binance `BTCUSDT` public depth stream
- Binance `BTCUSDT` public trade stream

Additional venue adapters are **implemented and tested** but ship **disabled**
(`enabled: false` in `ops.live.example.json`) so the live Binance collector is
unaffected. Enable them per lane when you want them:

| Venue    | Trades | Depth | Gap-detection class |
| -------- | ------ | ----- | ------------------- |
| Binance  | ✅ live | ✅ live | sequence (gap-proof) |
| Coinbase | ✅      | ✅      | trades = sequence; depth = `none_native` |
| Kraken   | ✅      | ✅      | trades = sequence; depth = `checksum` (CRC32) |
| Bybit    | ✅      | ✅      | trades = `none_native`; depth = sequence (`data.u` +1) |
| MEXC     | ✅      | ✅      | trades = `none_native`; depth = `none_native` (protobuf) |

`none_native` lanes are curated as *structurally clean*, **not** gap-proof — see
[`STANDARDS.md`](STANDARDS.md) §4.3. The mock feed exists only for local smoke
tests.

**MEXC** is implemented and tested but ships **disabled and unverified-against-live**.
MEXC retired its JSON websocket on 2025-08-04, so its public market data is now
**Protocol Buffers** on `wss://wbs-api.mexc.com/ws` — the only binary-transport venue
here. Frames are decoded through vendored, generated protobuf bindings (the MEXC lane
needs the `protobuf` runtime: `pip install -e ".[mexc]"`); the JSON ack/PING frames are
unchanged. Both MEXC lanes are `none_native`: the aggregated-deals stream has no
per-trade id, and limit-depth pushes independent full top-N books (each a snapshot)
whose `version` is kept as metadata but not used to prove gaplessness. The vendored
schema was built from MEXC's published proto + docs, **not** a live capture — verify
against real frames before enabling (see
[`src/crypto_collector/proto/mexc/README.md`](src/crypto_collector/proto/mexc/README.md)).

## Data Contract

[`STANDARDS.md`](STANDARDS.md) is the canonical contract: datasets, on-disk
layout, event schemas, the precise definition of "replayable" per feed class,
the per-lane `gap_detection` tag, retention, and the consumer API (read curated,
check the manifest, pin `schema_version`). Read it before building anything that
consumes this data.

## Data Availability

This repository ships collection, quality-control, and curation code only. It does
not include archive data, logs, local manifests, notebooks, private research
outputs, credentials, or signed endpoint code.

The maintainer deployment runs on a Windows workstation with archive root
`D:\market_archive`. The public data-plant startup task was installed on
2026-05-25 and began collecting Binance `BTCUSDT` depth and trade chunks under
the archive contract documented below.

Local archive status is reported by:

```powershell
market-data-plant research-manifest --archive-root D:\market_archive --output-root D:\market_archive\curated\research\manifests
```

Generated manifest files are local operational artifacts and are intentionally
not tracked in git.

## Archive Layout

Default archive root:

```text
D:\market_archive
```

Main outputs:

```text
D:\market_archive
  raw\
    market\
      <lane>\<run_id>\                 # lane = <venue>_<dataset>[_<suffix>]
        raw\messages.jsonl            # every WS frame, fsync per line, size-rotated
        clean\events.jsonl            # normalized events that passed the live gate
        quarantine\events.jsonl       # normalized events that failed, with reasons
        snapshots\book_snapshot.json  # depth REST anchor (Binance depth only)
        metrics\summary.jsonl
        metrics\replay_summary.json   # curation verdict (replayable + gap_detection)
  normalized\                         # all runs, pre-curation Parquet
    market\schema_version=v2\source=<venue>\instrument=<canonical>\event_date=YYYY-MM-DD\   # depth
    trades\schema_version=v2\source=<venue>\instrument=<canonical>\event_date=YYYY-MM-DD\
  curated\
    research\
      market_replayable\schema_version=v2\source=<venue>\instrument=<canonical>\event_date=YYYY-MM-DD\   # depth
      trades_replayable\schema_version=v2\source=<venue>\instrument=<canonical>\event_date=YYYY-MM-DD\
      manifests\
  quarantine\
    market\<lane>\
  ops\
    heartbeat.json
    heartbeat_history.jsonl
    job_runs.jsonl
    runner.log
    worker_events.jsonl
```

Raw/quarantine lane directories are `<venue>_<dataset>[_<suffix>]` —
`binance_depth`, `binance_trades`, `coinbase_trades`, `coinbase_depth`,
`kraken_trades`, `kraken_depth`, `bybit_trades`, `bybit_depth`, `mexc_trades`,
`mexc_depth`, plus an optional per-instrument suffix (`binance_trades_ethusdt`).
Depth lanes promote into
`market_replayable`; trades lanes promote into `trades_replayable`. Since the
`schema_version=v2` cutover, normalized and curated Parquet carry an
`instrument=<canonical>` partition (the sanitized canonical symbol, e.g.
`BTC-USDT`), so you can pull by `(venue, instrument, event_date)` straight off the
path; legacy `v1` data (venue-only) coexists for pre-cutover history. See
[`STANDARDS.md`](STANDARDS.md) §2 for the full layout contract.

Override roots with environment variables:

- `MARKET_DATA_ARCHIVE_ROOT`
- `MARKET_DATA_OUTPUT_ROOT`
- `MARKET_DATA_NORMALIZED_ROOT`
- `MARKET_DATA_CURATED_ROOT`
- `MARKET_DATA_OPS_ROOT`

## Setup

```powershell
python -m venv .venv
.\.venv\Scripts\activate
pip install -e ".[dev]"
```

`[dev]` includes the `protobuf` runtime, so the MEXC tests run out of the box. To run
a live MEXC worker without the dev extras, install the `[mexc]` extra instead
(`pip install -e ".[mexc]"`).

Check the installation:

```powershell
market-data-plant state
market-data-plant mock --count 5
pytest -q
```

## Manual Collection

Run one Binance depth segment:

```powershell
market-data-plant binance-depth-worker --symbol btcusdt --speed 100ms --segment-count 5000 --max-segments 1
```

Run one Binance trade segment:

```powershell
market-data-plant binance-trades-worker --symbol btcusdt --channel trade --segment-count 300 --max-segments 1 --no-jsonl-fsync --no-normalized-parquet
```

Other venue workers (same `--segment-count` / `--max-segments` / `--cooldown-seconds`
/ `--output-root` / `--ops-root` / `--worker-name` flags). Venue symbol formats
differ — Coinbase/Kraken are separated (`BTC-USD`, `BTC/USD`), Bybit/Binance are not:

```powershell
market-data-plant coinbase-trades-worker --symbol BTC-USD --channel matches --max-segments 1
market-data-plant coinbase-depth-worker  --symbol BTC-USD --channel level2_50 --max-segments 1
market-data-plant kraken-trades-worker   --symbol BTC/USD --channel trade --max-segments 1
market-data-plant kraken-depth-worker    --symbol BTC/USD --channel book --max-segments 1
market-data-plant bybit-trades-worker    --symbol BTCUSDT --channel publicTrade --max-segments 1
market-data-plant bybit-depth-worker     --symbol BTCUSDT --channel orderbook.50 --max-segments 1
```

MEXC is protobuf-only (`pip install -e ".[mexc]"` for the `protobuf` runtime). The
worker builds the full topic from the channel prefix plus `--interval` (trades) or
`--depth` (depth):

```powershell
market-data-plant mexc-trades-worker --symbol BTCUSDT --interval 100ms --max-segments 1
market-data-plant mexc-depth-worker  --symbol BTCUSDT --depth 20 --max-segments 1
```

Two cross-cutting lane flags (both default to legacy behavior, so the live BTC
collector is unaffected):

- `--source-suffix <name>` — write to a per-instrument lane
  `<venue>_<dataset>_<name>` instead of the bare lane. Empty preserves the legacy
  single-symbol layout.
- `--rotate-at-midnight` — end the run at the UTC day boundary instead of after
  `--segment-count` frames, so a run directory never straddles two `event_date`s.
  `--segment-count` then acts as a soft memory cap.

## Continuous Ops

Copy the example manifest if you need local edits:

```powershell
Copy-Item .\ops.live.example.json .\ops.live.local.json
```

Start the runner manually:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_ops_runner.ps1
```

Install automatic resume on Windows startup from an elevated PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\install_startup_task.ps1
```

If you cannot use an elevated shell, install a per-user logon task:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\install_startup_task.ps1 -TriggerMode Logon
```

## Health And Curation

```powershell
market-data-plant health --config .\ops.live.example.json
market-data-plant book-sync-health --source-root D:\market_archive\raw\market\binance_depth
market-data-plant backfill-replay --source-root D:\market_archive\raw\market\binance_depth --limit 50
market-data-plant quarantine-runs --source-root D:\market_archive\raw\market\binance_depth --quarantine-root D:\market_archive\quarantine\market\binance_depth
market-data-plant promote-replayable --source-root D:\market_archive\raw\market\binance_depth --target-root D:\market_archive\curated\research\market_replayable
market-data-plant research-manifest --archive-root D:\market_archive --output-root D:\market_archive\curated\research\manifests
market-data-plant cleanup --raw-days 14
```

The quarantine → promote chain is per-lane: point `--source-root` at any lane
directory (`...\raw\market\<lane>`) and the matching `--target-root`. Depth lanes
promote into `curated\research\market_replayable`; trades lanes into
`curated\research\trades_replayable`. In the live deployment the ops runner does
this automatically per enabled lane — these commands are for manual/backfill use.

`cleanup` is dry-run by default. It removes files only when `--apply` is passed;
per-dataset retention is set via the cleanup job's `raw_policy`
(`market/<lane>=<days>`).

## Publication Safety

Before publishing:

```powershell
git status --short
pytest -q
```

Do not commit:

- `.env`
- `ops.live.local.json`
- archive data
- logs
- notebooks or one-off research outputs
- API keys or credentials
- live-order or signed-endpoint code

See [docs/publication_safety.md](docs/publication_safety.md).
