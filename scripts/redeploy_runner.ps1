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
    # Match run_ops_runner.ps1's live default (one slot per pooled lane: 21 workers +
    # 2 kalshi REST jobs). Keep these in sync -- a redeploy with a lower value silently
    # throttles coverage until reboot.
    [int]$CollectorConcurrency = 23
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
# Pool-dispatched job types all end in -worker (pinned by tests/test_repo_hygiene.py)
# plus the two pooled kalshi REST jobs, enumerated EXPLICITLY: a kalshi- prefix wildcard
# also matched the maintenance job kalshi-summarize-crypto-quotes, so adding that to
# the config would have tripped this preflight and refused a valid boot.
$kalshiPoolTypes = @("kalshi-collect-crypto-quotes", "kalshi-discover-crypto")
$configPayload = Get-Content -LiteralPath $config -Raw -Encoding utf8 | ConvertFrom-Json
$collectorLanes = @($configPayload.jobs | Where-Object {
    ($_.job_type -like "*-worker" -or $kalshiPoolTypes -contains $_.job_type) -and ($null -eq $_.enabled -or $_.enabled)
})
if ($collectorLanes.Count -gt $CollectorConcurrency) {
    throw "$($collectorLanes.Count) enabled collector lanes exceed CollectorConcurrency=$CollectorConcurrency. Raise the default in run_ops_runner.ps1 AND redeploy_runner.ps1 before redeploying."
}
# Duplicate ENABLED job names collapse into one scheduler slot (state is keyed by
# name) -- one of the copies silently never runs. The runner now refuses such a
# config at load, so catch it here BEFORE killing the old runner.
$dupNames = @($configPayload.jobs | Where-Object { $null -eq $_.enabled -or $_.enabled } |
    Group-Object -Property name | Where-Object { $_.Count -gt 1 })
if ($dupNames.Count -gt 0) {
    throw "Duplicate enabled job names in ${config}: $(($dupNames.Name) -join ', '). Fix before redeploying."
}

# 1. Stop the current runner (if any) named in the lock. PS-native kill-tree:
#    taskkill writes to stderr when any process in the tree is unkillable or already
#    gone, and PS 5.1 wraps that as a terminating NativeCommandError even with
#    2>$null under ErrorActionPreference=Stop -- which aborted two redeploys on
#    2026-06-11 BEFORE the relaunch, leaving collection down with a stale lock.
#    Stop-Process -ErrorAction SilentlyContinue cannot abort the script.
#    Root-FIRST ordering: snapshot the children, kill the root (so the scheduler
#    cannot dispatch a fresh worker mid-kill -- a child spawned between a
#    children-first sweep and the root kill survived with its lane lock and forced
#    an abort with NO runner running), then sweep the snapshot. Any straggler that
#    slipped the snapshot is caught by the Select-PlantPython sweep below.
function Stop-PlantProcessTree([int]$RootPid) {
    $children = @(Get-CimInstance Win32_Process -Filter "ParentProcessId=$RootPid" -ErrorAction SilentlyContinue)
    Stop-Process -Id $RootPid -Force -ErrorAction SilentlyContinue
    foreach ($child in $children) { Stop-PlantProcessTree ([int]$child.ProcessId) }
}
# Heartbeat baseline BEFORE the kill: the post-relaunch check must prove the NEW
# runner wrote a heartbeat, not re-read the dead runner's last one (<=30s old).
$preKillHeartbeatUtc = $null
if (Test-Path $heartbeatPath) {
    try {
        $preHb = Get-Content $heartbeatPath -Raw | ConvertFrom-Json
        $preKillHeartbeatUtc = [DateTime]::Parse($preHb.last_seen).ToUniversalTime()
    } catch {
        $preKillHeartbeatUtc = $null
    }
}
if (Test-Path $lockPath) {
    try {
        $lock = Get-Content $lockPath -Raw | ConvertFrom-Json
    } catch {
        $lock = $null  # torn/garbage lock: nothing to kill by pid; the sweep below still runs
    }
    if ($lock -and $lock.pid) {
        Write-Host "Current runner: pid=$($lock.pid) created_at=$($lock.created_at)"
        Stop-PlantProcessTree ([int]$lock.pid)
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

# Kill fan-out has latency, so poll briefly: a closing process must not read as a
# live double runner. Any READABLE plant python still alive after the wait is a
# straggler that slipped the kill-tree snapshot -- finish the job with the matcher
# we already trust instead of aborting with no runner running (the worst case the
# 2026-06-10 outage taught us to avoid).
$deadline = (Get-Date).AddSeconds(12)
while ($true) {
    $pythons = @(Get-CimInstance Win32_Process -Filter "Name='python.exe'" -ErrorAction SilentlyContinue)
    $plant = Select-PlantPython $pythons
    $unreadable = Select-UnreadablePython $pythons
    if (($plant.Count -eq 0 -and $unreadable.Count -eq 0) -or ((Get-Date) -ge $deadline)) { break }
    Start-Sleep -Seconds 2
}
if ($plant.Count -gt 0) {
    Write-Warning "Plant python still running (pids: $($plant.ProcessId -join ',')) -- killing stragglers."
    foreach ($proc in $plant) { Stop-PlantProcessTree ([int]$proc.ProcessId) }
    Start-Sleep -Seconds 3
    $pythons = @(Get-CimInstance Win32_Process -Filter "Name='python.exe'" -ErrorAction SilentlyContinue)
    $plant = Select-PlantPython $pythons
    $unreadable = Select-UnreadablePython $pythons
}
if ($plant.Count -gt 0) {
    Write-Warning "Plant python STILL running after straggler sweep (pids: $($plant.ProcessId -join ',')). Aborting to avoid a double runner."
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
# Same logic for the per-lane worker locks: zero plant pythons means every one of
# them is stale. Clearing them here (and ONLY here, after the gate) saves each lane
# the up-to-600s lock-freshness self-heal wait after every redeploy, and removes any
# torn lock file a kill mid-acquire left behind. Heartbeat *.json files stay --
# health/history read them.
Remove-Item (Join-Path $OpsRoot "standalone_workers\*.lock") -Force -ErrorAction SilentlyContinue

# 3. Relaunch directly (no wrapper mutex), loading this repo's src via PYTHONPATH.
$env:PYTHONPATH = Join-Path $repo "src"
Start-Process -WindowStyle Hidden -WorkingDirectory $repo -FilePath $python `
    -ArgumentList '-m','crypto_collector.cli','ops-runner','--config',$config,'--ops-root',$OpsRoot,'--collector-concurrency',$CollectorConcurrency

# 4. Verify it came up. Three proofs, all required: a lock whose pid is ALIVE, a
#    heartbeat STRICTLY NEWER than the pre-kill baseline (the dead runner's last
#    heartbeat is <=30s old and would pass a bare age check), and a readable
#    heartbeat at all (a fresh ops root must fail the check, not throw past it).
Start-Sleep -Seconds 12
$ok = $false
if (Test-Path $lockPath) {
    try {
        $newLock = Get-Content $lockPath -Raw | ConvertFrom-Json
        $hb = Get-Content $heartbeatPath -Raw | ConvertFrom-Json
        $hbUtc = [DateTime]::Parse($hb.last_seen).ToUniversalTime()
        $age = ([DateTime]::UtcNow - $hbUtc).TotalSeconds
        $pidAlive = $false
        if ($newLock.pid) {
            $pidAlive = $null -ne (Get-Process -Id ([int]$newLock.pid) -ErrorAction SilentlyContinue)
        }
        $advanced = ($null -eq $preKillHeartbeatUtc) -or ($hbUtc -gt $preKillHeartbeatUtc)
        Write-Host ("New runner: pid={0} alive={1} heartbeat_age={2:N0}s advanced={3} status={4}" -f `
            $newLock.pid, $pidAlive, $age, $advanced, $hb.status)
        if ($age -lt 60 -and $pidAlive -and $advanced) { $ok = $true }
    } catch {
        Write-Warning "Post-relaunch verification failed to read lock/heartbeat: $_"
    }
}
if ($ok) {
    Write-Host "Redeploy OK. Trades workers will write buffered (low-quarantine) runs as they cycle." -ForegroundColor Green
} else {
    Write-Warning "Runner did not confirm healthy within 12s. Check $OpsRoot\runner.log."
    exit 1
}
