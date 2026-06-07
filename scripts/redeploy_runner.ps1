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
    [string]$OpsRoot = "D:\market_archive\ops"
)
$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot
$python = Join-Path $repo ".venv\Scripts\python.exe"
$config = Join-Path $repo "ops.live.local.json"
$lockPath = Join-Path $OpsRoot "ops-runner.lock"
$heartbeatPath = Join-Path $OpsRoot "heartbeat.json"

if (-not (Test-Path $python)) { throw "venv python not found: $python" }
if (-not (Test-Path $config)) { throw "ops config not found: $config" }

# 1. Stop the current runner (if any) named in the lock.
if (Test-Path $lockPath) {
    $lock = Get-Content $lockPath -Raw | ConvertFrom-Json
    Write-Host "Current runner: pid=$($lock.pid) created_at=$($lock.created_at)"
    if ($lock.pid) {
        & taskkill /F /T /PID $lock.pid 2>$null | Out-Null
        Start-Sleep -Seconds 3
    }
}

# 2. Clear the lock only if no python runner is alive (avoid stomping a live one).
$alive = Get-CimInstance Win32_Process -Filter "Name='python.exe'" -ErrorAction SilentlyContinue
if ($alive) {
    Write-Warning "python.exe still running (pids: $($alive.ProcessId -join ',')). Aborting to avoid a double runner."
    exit 1
}
Remove-Item $lockPath -Force -ErrorAction SilentlyContinue

# 3. Relaunch directly (no wrapper mutex), loading this repo's src via PYTHONPATH.
$env:PYTHONPATH = Join-Path $repo "src"
Start-Process -WindowStyle Hidden -WorkingDirectory $repo -FilePath $python `
    -ArgumentList '-m','crypto_collector.cli','ops-runner','--config',$config,'--ops-root',$OpsRoot,'--collector-concurrency','4'

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
