param(
    [string]$ConfigPath,
    [string]$OpsRoot = "G:\market_archive\ops",
    [string]$LogPath,
    # Live default: run every collector lane concurrently so each records CONTINUOUSLY.
    # The fleet is now 21 collector lanes (BTC spot x5 venues + BTC/USDC on Binance +
    # Bybit linear perp + Binance USDT-M perp via REST x3 + OKX spot & linear perp x4),
    # so 21 gives one slot per lane. With fewer slots than lanes, the lanes sorting LAST
    # in the config are never dispatched (starved) -> coverage gaps, so this MUST be
    # bumped by one per collector lane added. WS collectors are I/O-bound (measured
    # ~0.1 core total for 12 live lanes) and process-isolated, so 21 stays light on the
    # 8-physical / 16-logical-core box. Maintenance jobs (quarantine/promote/manifest/
    # cleanup/health) run on the runner's dedicated single-slot maintenance executor,
    # NOT the pool, so they don't consume collector slots.
    # 21 market worker lanes + the 2 kalshi REST jobs (pool-dispatched since the
    # 2026-06-11 scheduler-stall incident) + the 2 text-capture lanes (text-rss +
    # text-reddit, ROADMAP item 15) = 25 slots, one per pooled lane.
    [int]$CollectorConcurrency = 25
)

$ErrorActionPreference = "Stop"

$workspaceRoot = Split-Path -Parent $PSScriptRoot
$pythonPath = Join-Path $workspaceRoot ".venv\Scripts\python.exe"

if (-not (Test-Path -LiteralPath $pythonPath)) {
    throw "Python runtime not found at $pythonPath"
}

if (-not $ConfigPath) {
    $localConfig = Join-Path $workspaceRoot "ops.live.local.json"
    $sharedConfig = Join-Path $workspaceRoot "ops.live.example.json"
    if (Test-Path -LiteralPath $localConfig) {
        $ConfigPath = $localConfig
    }
    elseif (Test-Path -LiteralPath $sharedConfig) {
        $ConfigPath = $sharedConfig
    }
    else {
        throw "No ops config found. Expected ops.live.local.json or ops.live.example.json in $workspaceRoot"
    }
}

$resolvedConfig = (Resolve-Path -LiteralPath $ConfigPath).Path
$resolvedOpsRoot = [System.IO.Path]::GetFullPath($OpsRoot)
New-Item -ItemType Directory -Force -Path $resolvedOpsRoot | Out-Null

if (-not $LogPath) {
    $LogPath = Join-Path $resolvedOpsRoot "runner.log"
}

# Preflight: validate the ops config before handing control to the runner. Without
# this, a corrupt JSON file or a config with zero jobs would still launch python,
# silently no-op (or crash with a stack trace), and Task Scheduler would never
# surface the actual reason.
try {
    $configPayload = Get-Content -LiteralPath $resolvedConfig -Raw -Encoding utf8 | ConvertFrom-Json
}
catch {
    "[$(Get-Date -Format o)] ops config invalid JSON ($resolvedConfig): $_" | Out-File -FilePath $LogPath -Append -Encoding utf8
    throw "ops config is not valid JSON: $resolvedConfig"
}

if ($null -eq $configPayload.jobs -or @($configPayload.jobs).Count -eq 0) {
    "[$(Get-Date -Format o)] ops config has no jobs ($resolvedConfig)" | Out-File -FilePath $LogPath -Append -Encoding utf8
    throw "ops config has no jobs: $resolvedConfig"
}

$invalidJobs = @($configPayload.jobs) | Where-Object {
    [string]::IsNullOrWhiteSpace($_.name) -or [string]::IsNullOrWhiteSpace($_.job_type)
}
if ($invalidJobs.Count -gt 0) {
    "[$(Get-Date -Format o)] ops config contains jobs missing name or job_type ($resolvedConfig)" | Out-File -FilePath $LogPath -Append -Encoding utf8
    throw "ops config jobs missing name or job_type: $resolvedConfig"
}

# Guard: more enabled collector lanes than pool slots means the lanes sorting last in
# the config are NEVER dispatched (silent starvation -- shipped twice: 12<17, 17<21).
# Pool-dispatched job types end in -worker, plus the two pooled kalshi REST jobs
# enumerated EXPLICITLY (pinned by tests/test_repo_hygiene.py): a kalshi- prefix wildcard
# also matched the maintenance job kalshi-summarize-crypto-quotes, so adding that to
# the config would have tripped this preflight and refused a valid boot.
$kalshiPoolTypes = @("kalshi-collect-crypto-quotes", "kalshi-discover-crypto")
$collectorLanes = @($configPayload.jobs | Where-Object {
    ($_.job_type -like "*-worker" -or $kalshiPoolTypes -contains $_.job_type) -and ($null -eq $_.enabled -or $_.enabled)
})
if ($collectorLanes.Count -gt $CollectorConcurrency) {
    "[$(Get-Date -Format o)] $($collectorLanes.Count) enabled collector lanes exceed CollectorConcurrency=$CollectorConcurrency ($resolvedConfig)" | Out-File -FilePath $LogPath -Append -Encoding utf8
    throw "$($collectorLanes.Count) enabled collector lanes exceed CollectorConcurrency=$CollectorConcurrency. Raise the default in run_ops_runner.ps1 AND redeploy_runner.ps1 (one slot per lane)."
}

# Guard: duplicate ENABLED job names collapse into one scheduler slot (state is keyed
# by name) -- one copy silently never runs. The runner refuses such a config at load;
# catching it here surfaces the reason in the boot log instead of a python stack trace.
$dupNames = @($configPayload.jobs | Where-Object { $null -eq $_.enabled -or $_.enabled } |
    Group-Object -Property name | Where-Object { $_.Count -gt 1 })
if ($dupNames.Count -gt 0) {
    "[$(Get-Date -Format o)] duplicate enabled job names in ${resolvedConfig}: $(($dupNames.Name) -join ', ')" | Out-File -FilePath $LogPath -Append -Encoding utf8
    throw "Duplicate enabled job names in ${resolvedConfig}: $(($dupNames.Name) -join ', ')"
}

$mutex = New-Object System.Threading.Mutex($false, "Global\CryptoMarketDataPlantOpsRunner")
$hasHandle = $false

try {
    $hasHandle = $mutex.WaitOne(0, $false)
    if (-not $hasHandle) {
        try {
            "[$(Get-Date -Format o)] ops runner already active, exiting" | Out-File -FilePath $LogPath -Append -Encoding utf8
        }
        catch {
            # The active runner may already hold the log file open for native stream redirection.
        }
        exit 0
    }

    $env:PYTHONPATH = Join-Path $workspaceRoot "src"
    Push-Location $workspaceRoot
    try {
        "[$(Get-Date -Format o)] starting ops runner with $resolvedConfig" | Out-File -FilePath $LogPath -Append -Encoding utf8
        # PowerShell 5.1 quirk: with ErrorActionPreference=Stop, any line python writes to
        # stderr (e.g. UserWarning from default_*_root fallbacks) gets wrapped as a
        # NativeCommandError and terminates the script before python finishes. Temporarily
        # downgrade so stderr lines are non-fatal; we still gate success on $LASTEXITCODE.
        $savedErrorActionPreference = $ErrorActionPreference
        $ErrorActionPreference = "Continue"
        try {
            & $pythonPath -m crypto_collector.cli ops-runner --config $resolvedConfig --ops-root $resolvedOpsRoot --collector-concurrency $CollectorConcurrency *>> $LogPath
            $exitCode = $LASTEXITCODE
        }
        finally {
            $ErrorActionPreference = $savedErrorActionPreference
        }
        if ($exitCode -ne 0) {
            throw "ops runner exited with code $exitCode"
        }
    }
    finally {
        Pop-Location
    }
}
finally {
    if ($hasHandle) {
        $mutex.ReleaseMutex() | Out-Null
    }
    $mutex.Dispose()
}
