<#
.SYNOPSIS
  Redeploy the live ops-runner with the current on-disk code WITHOUT a full PC reboot.

  Why this exists: run_ops_runner.ps1 guards single-instance with a Global\ mutex that
  a non-elevated/interactive shell can't recreate once the original (SYSTEM/boot) runner
  holds it -> "Access denied". This script bypasses that by launching the Python runner
  directly (it uses a file lock, not the Global mutex), after cleanly stopping the old
  process and clearing its stale lock.

  The relaunched runner runs until the next reboot; on reboot your normal boot-startup
  mechanism relaunches it (also with the fixed code, since the fix is on disk + committed),
  so durability is unchanged.

.NOTES
  Run from an ELEVATED PowerShell if the current runner is elevated/SYSTEM (so Stop-Process
  can terminate it). Interrupts every lane briefly (clean segment boundary, not data loss).
#>
param(
    [string]$OpsRoot = "G:\market_archive\ops",
    # Match run_ops_runner.ps1's live default (one slot per collector lane). Keep these
    # in sync -- a redeploy with a lower value silently throttles coverage until reboot.
    [int]$CollectorConcurrency = 21
)
$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot
$python = Join-Path $repo ".venv\Scripts\python.exe"
$config = Join-Path $repo "ops.live.local.json"
$lockPath = Join-Path $OpsRoot "ops-runner.lock"
$heartbeatPath = Join-Path $OpsRoot "heartbeat.json"

if (-not (Test-Path $python)) { throw "venv python not found: $python" }
if (-not (Test-Path $config)) { throw "ops config not found: $config" }

# Guard BEFORE stopping anything: more enabled collector lanes than pool slots means
# the lanes sorting last are never dispatched (silent starvation -- shipped twice).
# Pool-dispatched job types all end in -worker (pinned by tests/test_repo_hygiene.py).
$configPayload = Get-Content -LiteralPath $config -Raw -Encoding utf8 | ConvertFrom-Json
$collectorLanes = @($configPayload.jobs | Where-Object {
    $_.job_type -like "*-worker" -and ($null -eq $_.enabled -or $_.enabled)
})
if ($collectorLanes.Count -gt $CollectorConcurrency) {
    throw "$($collectorLanes.Count) enabled collector lanes exceed CollectorConcurrency=$CollectorConcurrency. Raise the default in run_ops_runner.ps1 AND redeploy_runner.ps1 before redeploying."
}

# 1. Stop the current runner (if any) named in the lock.
if (Test-Path $lockPath) {
    $lock = Get-Content $lockPath -Raw | ConvertFrom-Json
    Write-Host "Current runner: pid=$($lock.pid) created_at=$($lock.created_at)"
    if ($lock.pid) {
        & taskkill /F /T /PID $lock.pid 2>$null | Out-Null
        Start-Sleep -Seconds 3
    }
}

# 2. Clear the lock only if no PLANT python process is alive (avoid stomping a live
#    runner/worker). Scoped to THIS plant's processes, NOT every python.exe on the box:
#    on 2026-06-10 an any-python version of this check saw unrelated crypto-modelling
#    backtest pythons AFTER step 1 had already killed the runner, aborted before
#    relaunching, and left collection down ~30 min with a stale lock. The runner and
#    every worker it spawns run `-m crypto_collector.cli ...` via this repo's venv
#    python (cli.py _run_collector_in_subprocess), so plant processes are matched by
#    'crypto_collector' in the command line or the repo root in either path field.
$repoPattern = "*$([System.Management.Automation.WildcardPattern]::Escape($repo))*"
function Select-PlantPython([object[]]$Processes) {
    @($Processes | Where-Object {
        $_.CommandLine -like '*crypto_collector*' -or
        $_.CommandLine -like $repoPattern -or
        $_.ExecutablePath -like $repoPattern
    })
}
# A python whose CommandLine AND ExecutablePath are both unreadable (null) belongs to
# another principal (typically SYSTEM -- e.g. the boot-task runner seen from a
# non-elevated shell). It could be the live runner, and step 1's taskkill would have
# failed against it for the same reason, so it must still block the redeploy.
function Select-UnreadablePython([object[]]$Processes) {
    @($Processes | Where-Object { -not $_.CommandLine -and -not $_.ExecutablePath })
}

# taskkill /T fans out across the worker tree, so poll briefly: kill latency must not
# read as a live double runner.
$deadline = (Get-Date).AddSeconds(12)
while ($true) {
    $pythons = @(Get-CimInstance Win32_Process -Filter "Name='python.exe'" -ErrorAction SilentlyContinue)
    $plant = Select-PlantPython $pythons
    $unreadable = Select-UnreadablePython $pythons
    if (($plant.Count -eq 0 -and $unreadable.Count -eq 0) -or ((Get-Date) -ge $deadline)) { break }
    Start-Sleep -Seconds 2
}
if ($plant.Count -gt 0) {
    Write-Warning "Plant python still running (pids: $($plant.ProcessId -join ',')). Aborting to avoid a double runner."
    exit 1
}
if ($unreadable.Count -gt 0) {
    Write-Warning "python.exe pids $($unreadable.ProcessId -join ',') have an unreadable CommandLine/ExecutablePath (owned by another principal, e.g. the SYSTEM boot runner) -- can't rule out the plant. Rerun from an elevated shell. Aborting."
    exit 1
}
# No plant process is alive past this point: if a lock file remains (e.g. step 1
# killed the runner, or it died earlier), it is stale by definition -- clear it and
# proceed. Unrelated python.exe processes no longer block this.
Remove-Item $lockPath -Force -ErrorAction SilentlyContinue

# 3. Relaunch directly (no wrapper mutex), loading this repo's src via PYTHONPATH.
$env:PYTHONPATH = Join-Path $repo "src"
Start-Process -WindowStyle Hidden -WorkingDirectory $repo -FilePath $python `
    -ArgumentList '-m','crypto_collector.cli','ops-runner','--config',$config,'--ops-root',$OpsRoot,'--collector-concurrency',$CollectorConcurrency

# 4. Verify it came up (heartbeat advances + a fresh lock pid).
Start-Sleep -Seconds 12
$ok = $false
if (Test-Path $lockPath) {
    $newLock = Get-Content $lockPath -Raw | ConvertFrom-Json
    $hb = Get-Content $heartbeatPath -Raw | ConvertFrom-Json
    $age = ([DateTime]::UtcNow - [DateTime]::Parse($hb.last_seen).ToUniversalTime()).TotalSeconds
    Write-Host ("New runner: pid={0} heartbeat_age={1:N0}s status={2}" -f $newLock.pid, $age, $hb.status)
    if ($age -lt 60) { $ok = $true }
}
if ($ok) {
    Write-Host "Redeploy OK. Trades workers will write buffered (low-quarantine) runs as they cycle." -ForegroundColor Green
} else {
    Write-Warning "Runner did not confirm healthy within 12s. Check $OpsRoot\runner.log."
    exit 1
}
