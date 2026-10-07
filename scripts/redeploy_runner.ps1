<#
.SYNOPSIS
  Preflight the existing SYSTEM task; use -Apply for one bounded restart.
  No PID termination, lock deletion, ad-hoc runner, config edit or lease reset.
#>
param(
    [string]$OpsRoot = "G:\market_archive\ops",
    [int]$CollectorConcurrency = 49,
    [switch]$Apply
)
$ErrorActionPreference = 'Stop'

function Assert-PlantAdministrator {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($identity)
    if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
        throw 'Run in an elevated PowerShell. Nothing has been stopped.'
    }
}

function Assert-PlantConfig([string]$Repo, [string]$Config) {
    $python = Join-Path $Repo '.venv\Scripts\python.exe'
    if (-not (Test-Path -LiteralPath $python)) { throw 'Plant Python runtime missing. Nothing has been stopped.' }
    $savedPythonPath = $env:PYTHONPATH
    try {
        $env:PYTHONPATH = Join-Path $Repo 'src'
        # Use the same schema, worker-lock and path checks as the runner. This
        # short-lived command runs before Stop; no long-lived stream redirect.
        $check = 'import sys; from pathlib import Path; from crypto_collector.ops import load_ops_config; assert load_ops_config(Path(sys.argv[1]))'
        & $python -c $check $Config
        if ($LASTEXITCODE -ne 0) { throw 'Runner config validation failed. Nothing has been stopped.' }
    } finally {
        $env:PYTHONPATH = $savedPythonPath
    }
}

function Assert-PlantTaskPreflight([string]$Repo, [string]$Root, [int]$Concurrency) {
    Assert-PlantAdministrator
    $task = Get-ScheduledTask -TaskName 'CryptoMarketDataPlant' -TaskPath '\'
    if ($task.State -ne 'Running' -or $task.Principal.UserId -notin @('SYSTEM','S-1-5-18')) {
        throw 'Expected a running SYSTEM plant task. Nothing has been stopped.'
    }
    if ([string]$task.Settings.MultipleInstances -notin @('IgnoreNew','2')) {
        throw 'Task must ignore concurrent starts. Nothing has been stopped.'
    }
    $config = Join-Path $Repo 'ops.live.local.json'
    if (-not (Test-Path -LiteralPath $config)) { $config = Join-Path $Repo 'ops.live.example.json' }
    $wrapper = Join-Path $Repo 'scripts\run_ops_runner.ps1'
    $baseArgs = '-NoProfile -ExecutionPolicy Bypass -File "' + $wrapper + '"'
    $explicitArgs = $baseArgs + ' -ConfigPath "' + $config + '" -OpsRoot "' + $Root + '"'
    $allowed = @($explicitArgs)
    if ($Root -eq 'G:\market_archive\ops') { $allowed += $baseArgs }
    $actions = @($task.Actions)
    if ($actions.Count -ne 1 -or [IO.Path]::GetFileName($actions[0].Execute) -ne 'powershell.exe' -or $actions[0].Arguments -notin $allowed) {
        throw 'Task action does not match this checkout/config/root. Nothing has been stopped.'
    }
    $scriptText = [IO.File]::ReadAllText($wrapper)
    $capacity = [regex]::Match($scriptText, '\[int\]\$CollectorConcurrency\s*=\s*(\d+)')
    if (-not $capacity.Success -or [int]$capacity.Groups[1].Value -ne $Concurrency) {
        throw 'Concurrency differs from SYSTEM wrapper default. Nothing has been stopped.'
    }
    $payload = Get-Content -LiteralPath $config -Raw -Encoding UTF8 | ConvertFrom-Json
    $jobs = @($payload.jobs | Where-Object { $null -eq $_.enabled -or $_.enabled })
    if ($jobs.Count -eq 0 -or @($jobs | Where-Object { -not $_.name -or -not $_.job_type }).Count -gt 0) {
        throw 'Invalid jobs. Nothing has been stopped.'
    }
    if (@($jobs | Group-Object name | Where-Object Count -gt 1).Count -gt 0) {
        throw 'Duplicate job names. Nothing has been stopped.'
    }
    $nonWorkerPoolTypes = @("kalshi-collect-crypto-quotes","kalshi-discover-crypto","hyperliquid-leaderboard-snapshot","hyperliquid-universe-positions-snapshot","binance-options-chain-snapshot","deribit-options-snapshot")
    $lanes = @($jobs | Where-Object { $_.job_type -like '*-worker' -or $_.job_type -in $nonWorkerPoolTypes })
    if ($lanes.Count -gt $Concurrency) { throw 'Collector capacity exceeded. Nothing has been stopped.' }
    Assert-PlantConfig $Repo $config
    # Observation guards are all BEFORE Stop. Shared-venv research processes
    # are irrelevant: only the named task owns this restart, never a PID scan.
    $old = Get-Content -LiteralPath (Join-Path $Root 'ops-runner.lock') -Raw | ConvertFrom-Json
    $hb = Get-Content -LiteralPath (Join-Path $Root 'heartbeat.json') -Raw | ConvertFrom-Json
    $age = ([DateTime]::UtcNow - ([DateTimeOffset]::Parse($hb.last_seen)).UtcDateTime).TotalSeconds
    if ($age -lt 0 -or $age -gt 30 -or $hb.status -ne 'running' -or -not $old.created_at -or -not $old.pid) {
        throw 'Current heartbeat/identity unverified. Nothing has been stopped.'
    }
    return $old
}

function Wait-PlantTaskRestart([string]$Root, $Old, [DateTime]$Started, [ValidateRange(1,60)][int]$TimeoutSeconds = 60) {
    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    $firstSeen = $null
    $observedIdentity = $null
    $newRunnerSeen = $false
    $startReconciled = $false
    do {
        Start-Sleep -Seconds 2
        try {
            $task = Get-ScheduledTask -TaskName 'CryptoMarketDataPlant' -TaskPath '\'
            $current = $null
            $hb = $null
            try {
                $current = Get-Content -LiteralPath (Join-Path $Root 'ops-runner.lock') -Raw | ConvertFrom-Json
                $hb = Get-Content -LiteralPath (Join-Path $Root 'heartbeat.json') -Raw | ConvertFrom-Json
            } catch { }
            if ($current.created_at -and $current.created_at -ne $Old.created_at) { $newRunnerSeen = $true }
            # IgnoreNew can discard the initial Start during a delayed Stop.
            # Reconcile ONCE only after Ready, and only before observing a new
            # runner identity. Never restart a newly observed failed runner.
            if ($task.State -eq 'Ready' -and -not $newRunnerSeen -and -not $startReconciled) {
                $startReconciled = $true
                Start-ScheduledTask -TaskName 'CryptoMarketDataPlant' -TaskPath '\' -ErrorAction Stop
                continue
            }
            if ($null -eq $current -or $null -eq $hb) { continue }
            $seen = ([DateTimeOffset]::Parse($hb.last_seen)).UtcDateTime
            $created = ([DateTimeOffset]::Parse($current.created_at)).UtcDateTime
            $age = ([DateTime]::UtcNow - $seen).TotalSeconds
            if ($task.State -eq 'Running' -and $hb.status -eq 'running' -and $current.pid -and
                $current.created_at -ne $Old.created_at -and $created -ge $Started -and
                $seen -ge $created -and $age -ge 0 -and $age -le 15) {
                $identity = "$($current.pid)/$($current.created_at)"
                if ($observedIdentity -ne $identity) { $firstSeen = $null; $observedIdentity = $identity }
                if ($null -ne $firstSeen -and $seen -gt $firstSeen) {
                    $current | Add-Member -NotePropertyName start_reconciled -NotePropertyValue $startReconciled -Force
                    return $current
                }
                $firstSeen = $seen
            }
        } catch {
            # Missing/torn lock or heartbeat during startup is transient, not a
            # reason to restart again. Keep the single bounded observation wait.
        }
    } while ([DateTime]::UtcNow -lt $deadline)
    throw 'Task start/reconciliation did not produce a fresh advancing heartbeat. Inspect the task/log; do not repeat Stop.'
}

function Wait-PlantTaskStopped {
    $deadline = [DateTime]::UtcNow.AddSeconds(15)
    do {
        if ((Get-ScheduledTask -TaskName 'CryptoMarketDataPlant' -TaskPath '\').State -ne 'Running') { return }
        Start-Sleep -Milliseconds 250
    } while ([DateTime]::UtcNow -lt $deadline)
    throw 'Task stop did not settle within 15 seconds'
}

function Invoke-PlantTaskRestart([string]$Repo, [string]$Root, [int]$Concurrency, [switch]$Execute) {
    $old = Assert-PlantTaskPreflight $Repo $Root $Concurrency
    if (-not $Execute) { return @{status='preflight_passed'; apply=$false; old_runner=$old} }
    $started = [DateTime]::UtcNow
    $stopError = $null
    try {
        Stop-ScheduledTask -TaskName 'CryptoMarketDataPlant' -TaskPath '\' -ErrorAction Stop
    } catch {
        $stopError = $_.Exception.GetType().Name
    } finally {
        # Once Stop is attempted, ALWAYS attempt the matching Start in this
        # process. No logging, process inventory or other fallible work here.
        # IgnoreNew and the wrapper mutex prevent a second task instance if
        # Stop failed before stopping the old task. Never launch a fallback.
        try {
            Wait-PlantTaskStopped
        } catch {
            $stopError = $_.Exception.GetType().Name
        } finally {
            Start-ScheduledTask -TaskName 'CryptoMarketDataPlant' -TaskPath '\' -ErrorAction Stop
        }
    }
    $current = Wait-PlantTaskRestart $Root $old $started
    return @{status='restarted_verified'; apply=$true; old_runner=$old; new_runner=$current; stop_error_type=$stopError}
}

# Dot sourcing exposes only functions for hermetic tests; it never stops a task.
if ($MyInvocation.InvocationName -ne '.') {
    Invoke-PlantTaskRestart -Repo (Split-Path -Parent $PSScriptRoot) -Root $OpsRoot -Concurrency $CollectorConcurrency -Execute:$Apply |
        ConvertTo-Json -Depth 5
}
