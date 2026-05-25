# Windows Startup Operation

This repo uses Windows Task Scheduler, not a custom Windows service.

## Install

Run from an elevated PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\install_startup_task.ps1
```

This registers task `CryptoMarketDataPlant` with a startup trigger and runs:

```powershell
scripts\run_ops_runner.ps1
```

The runner selects `ops.live.local.json` if present, otherwise `ops.live.example.json`.

## Per-User Fallback

If you do not want an elevated task:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\install_startup_task.ps1 -TriggerMode Logon
```

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
market-data-plant health --config .\ops.live.example.json
```

## Remove

```powershell
Unregister-ScheduledTask -TaskName CryptoMarketDataPlant -Confirm:$false
```
