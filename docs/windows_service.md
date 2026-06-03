# Windows Startup Operation

This repo uses Windows Task Scheduler, not a custom Windows service.

## Install

Run from an elevated PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\install_startup_task.ps1
```

This registers task `CryptoMarketDataPlant` with an `-AtStartup` trigger, running
as `SYSTEM` at highest privilege, and invokes:

```powershell
scripts\run_ops_runner.ps1
```

The runner selects `ops.live.local.json` if present, otherwise `ops.live.example.json`.

The task is installed with no execution time limit. This matters because Windows
Task Scheduler's default limit is 72 hours; a continuous data plant can otherwise
be stopped by Windows after three days even though the runner is healthy. Re-run
the installer after pulling changes to update an existing task.

### Wake-from-sleep caveat

The installer sets `-WakeToRun` on the task, but with only an `-AtStartup` (or
`-AtLogOn`) trigger that flag is a **no-op**: there is no time-based trigger for
it to wake the machine for. The task resumes collection on boot/logon, not from
S3/S4 sleep. If you need the workstation to wake on a schedule and resume
collecting, add a time-based trigger (e.g. via `Set-ScheduledTask`); only then
does `-WakeToRun` take effect.

## Per-User Fallback

If you do not want an elevated task:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\install_startup_task.ps1 -TriggerMode Logon
```

This registers an `-AtLogOn` trigger running as the current user at limited
privilege. (Startup mode requires an elevated shell; the installer says so and
points you here if it hits an access-denied error.)

## Runner behavior

`run_ops_runner.ps1` does more than launch Python — relevant when diagnosing a
task that "ran" but produced nothing:

- **Config selection**: prefers `ops.live.local.json`, falls back to
  `ops.live.example.json`. Pass `-ConfigPath` to override.
- **Preflight validation**: the config is parsed before the runner starts. If it
  is invalid JSON, has zero jobs, or has any job missing `name`/`job_type`, the
  script logs the reason to `runner.log` and exits non-zero (so Task Scheduler
  surfaces a failure instead of a silent no-op).
- **Single instance**: a global mutex (`Global\CryptoMarketDataPlantOpsRunner`)
  guarantees only one runner is active. A second invocation logs
  "ops runner already active, exiting" and exits 0 — so a boot trigger firing
  while a logon-triggered runner is already up is harmless.
- **Parallel collection**: the script passes `-CollectorConcurrency 4` (override
  with the param), so up to 4 collector jobs (the `*-worker` types) run at once
  in a thread pool. Maintenance jobs (quarantine, promote, manifest, cleanup,
  health) stay serialized in the scheduler loop — at most one runs at a time, and
  it may run alongside active collectors but never alongside another maintenance
  job. A given job is never launched a second time while its previous run is still
  in flight. Default concurrency is `1` (fully serial) when the flag is omitted, so
  nothing changes for callers that do not pass it. The live BTC Binance lanes are
  unaffected by the change in dispatch order.
- **Heartbeat active set**: `heartbeat.json` reports `current_jobs` — the full list
  of in-flight jobs (`name`, `job_type`, `started_at`) — and keeps `current_job`
  pointing at the oldest active job (or `null` when idle) for older readers. Health
  treats any job in `current_jobs` as in progress, so long-running collectors are
  not mistaken for stale.

## Logs

Default runner log:

```text
D:\market_archive\ops\runner.log
```

Operational state:

```text
D:\market_archive\ops\heartbeat.json
D:\market_archive\ops\job_runs.jsonl
D:\market_archive\ops\worker_events.jsonl
```

## Check

```powershell
Get-ScheduledTask -TaskName CryptoMarketDataPlant
market-data-plant health --config .\ops.live.local.json
```

Point `health` at the **same** config the runner uses. On the maintainer
deployment that is `ops.live.local.json` (it takes precedence when present);
fall back to `ops.live.example.json` only if no local config exists. Checking the
example config when the runner is actually using the local one will report on the
wrong job set.

## Deploying code changes (restart the runner)

The runner reads the ops config **and loads the Python code once at process
start**. Pulling new code or editing `ops.live.local.json` therefore has **no
effect until the runner is restarted**. To deploy:

```powershell
Stop-ScheduledTask  -TaskName CryptoMarketDataPlant   # stops the running runner
Start-ScheduledTask -TaskName CryptoMarketDataPlant   # relaunches with new code/config
market-data-plant health --config .\ops.live.local.json
```

> ⚠️ The restart briefly interrupts **every** lane, including the live Binance
> BTCUSDT collector — its current segment ends and a fresh one starts (a clean
> segment boundary, not data loss). Only restart when you intend to deploy.

## Backfilling stream-depth replay

`replay_summary.json` is written by the collector at segment-collection time, so
runs collected **before** a replay-logic change keep their old verdict. To rescore
the already-collected Coinbase/Bybit/Kraken depth backlog with the current logic
(e.g. after the multi-anchor / Kraken depth-bounded fix) and promote what now
qualifies:

```powershell
# Dry run first — read-only, regenerates nothing, just reports counts:
market-data-plant backfill-stream-depth --raw-root D:\market_archive\raw\market

# Apply — regenerate each run's replay_summary.json AND promote the replayable ones:
market-data-plant backfill-stream-depth --raw-root D:\market_archive\raw\market --apply
```

Defaults cover `coinbase_depth`, `bybit_depth`, `kraken_depth` (override with
`--source`), the most recent `--limit 200` runs within `--max-age-hours 720`, and
promote into the curated `market_replayable` root. The backfill does **not** touch
the live collector; it only reads raw runs and writes curated Parquet, so it is safe
to run while collection continues.

## Remove

```powershell
Unregister-ScheduledTask -TaskName CryptoMarketDataPlant -Confirm:$false
```
