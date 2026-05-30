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

## Remove

```powershell
Unregister-ScheduledTask -TaskName CryptoMarketDataPlant -Confirm:$false
```
