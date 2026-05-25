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

- Binance `BTCUSDT` public depth stream
- Binance `BTCUSDT` public trade stream

The mock feed exists only for local smoke tests.

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
      binance_depth\<run_id>\
        raw\messages.jsonl
        clean\events.jsonl
        quarantine\events.jsonl
        snapshots\book_snapshot.json
        metrics\summary.jsonl
        metrics\replay_summary.json
      binance_trades\<run_id>\
        raw\messages.jsonl
        clean\events.jsonl
        quarantine\events.jsonl
        metrics\summary.jsonl
  normalized\
    market\schema_version=v1\source=binance\event_date=YYYY-MM-DD\
    trades\schema_version=v1\source=binance\event_date=YYYY-MM-DD\
  curated\
    research\
      market_replayable\schema_version=v1\source=binance\event_date=YYYY-MM-DD\
      manifests\
  quarantine\
    market\binance_depth\
  ops\
    heartbeat.json
    heartbeat_history.jsonl
    job_runs.jsonl
    runner.log
    worker_events.jsonl
```

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

Check the installation:

```powershell
market-data-plant state
market-data-plant mock --count 5
pytest -q
```

## Manual Collection

Run one depth segment:

```powershell
market-data-plant binance-depth-worker --symbol btcusdt --speed 100ms --segment-count 5000 --max-segments 1
```

Run one trade segment:

```powershell
market-data-plant binance-trades-worker --symbol btcusdt --channel trade --segment-count 5000 --max-segments 1
```

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

`cleanup` is dry-run by default. It removes files only when `--apply` is passed.

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
