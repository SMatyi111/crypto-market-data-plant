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
    # 21 existing market workers + the 2 kalshi REST jobs (pool-dispatched since
    # the 2026-06-11 scheduler-stall incident) + the 2 text-capture lanes + the
    # frozen-cohort Hyperliquid worker + the daily Hyperliquid leaderboard
    # snapshot + 5 liquidation lanes (3 bybit symbols, okx all-swap, binance
    # all-market) + 3 Binance open-interest lanes + 2 options-IV snapshot lanes
    # (binance-options-chain-snapshot, deribit-options-snapshot) = 37, plus the
    # 2026-09-17/18 ETH+SOL completeness build: 2 Binance mark/index/funding lanes
    # (the liquidation TRIGGER variable, previously BTC-only while ETH and SOL
    # liquidations were already being recorded) + 8 ETH/SOL perp lanes (Bybit and
    # OKX x trades and depth) giving those two symbols a price series at last
    # = 48 lanes CONFIGURED, of which 44 are ENABLED: the 37-count above includes
    # 4 that are disabled or off (the 2 kalshi REST jobs, text-reddit, and the
    # binance all-market liquidations lane that fstream never delivers from this
    # host). The preflight counts ENABLED lanes, so 44 is the number that must fit;
    # 46 leaves 2 slots of headroom. Raise it by one per lane added, in BOTH this
    # script and redeploy_runner.ps1.
    [int]$CollectorConcurrency = 46
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
# Pool-dispatched job types end in -worker, plus the pooled non-worker REST jobs
# enumerated EXPLICITLY (pinned by tests/test_repo_hygiene.py): a kalshi- prefix wildcard
# also matched the maintenance job kalshi-summarize-crypto-quotes, so adding that to
# the config would have tripped this preflight and refused a valid boot.
$nonWorkerPoolTypes = @("kalshi-collect-crypto-quotes", "kalshi-discover-crypto", "hyperliquid-leaderboard-snapshot", "hyperliquid-universe-positions-snapshot", "binance-options-chain-snapshot", "deribit-options-snapshot")
$collectorLanes = @($configPayload.jobs | Where-Object {
    ($_.job_type -like "*-worker" -or $nonWorkerPoolTypes -contains $_.job_type) -and ($null -eq $_.enabled -or $_.enabled)
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
        "[$(Get-Date -Format o)] starting ops runner with $resolvedConfig (wrapper powershell pid $PID)" | Out-File -FilePath $LogPath -Append -Encoding utf8
        # 2026-09-20 incident: the previous form (`& python ... *>> $LogPath`) routed the
        # runner's stdout/stderr through THIS PowerShell 5.1 host, one pipeline object
        # per line, for the life of the runner. Windows' Resource-Exhaustion-Detector
        # named a powershell.exe at 230 GB of virtual memory (pagefile peak 102 GB) and
        # plant jobs failed with '[Errno 22] Invalid argument' and MemoryError; the
        # boot-task host, alive ~34 h, was the only long-lived PowerShell and died with
        # the runner at the redeploy that freed the memory, and the identical redeploy
        # wrapper measurably grows (~0.3 MB/min at ~60 log lines/min). cmd.exe now owns
        # the append redirect natively (stdout+stderr into one file); PowerShell only
        # waits and reads the exit code, which cmd /c propagates from python. `--%`
        # hands the rest of the line to cmd verbatim; the paths travel as environment
        # variables so no PowerShell re-quoting can touch them, and /s makes cmd strip
        # exactly the outer quote pair. PYTHONIOENCODING/PYTHONUTF8 keep the appended
        # bytes UTF-8, matching the utf8 marker lines (the old *>> wrote UTF-16LE into
        # the same file). The wrapper pid in the marker lets a future 2004 event be
        # attributed instead of inferred.
        $env:PLANT_PYTHON = $pythonPath
        $env:PLANT_CONFIG = $resolvedConfig
        $env:PLANT_OPS = $resolvedOpsRoot
        $env:PLANT_CAP = "$CollectorConcurrency"
        $env:PLANT_LOG = $LogPath
        $env:PYTHONIOENCODING = "utf-8"
        $env:PYTHONUTF8 = "1"
        $savedErrorActionPreference = $ErrorActionPreference
        $ErrorActionPreference = "Continue"
        try {
            & cmd.exe --% /d /s /c ""%PLANT_PYTHON%" -m crypto_collector.cli ops-runner --config "%PLANT_CONFIG%" --ops-root "%PLANT_OPS%" --collector-concurrency %PLANT_CAP% >> "%PLANT_LOG%" 2>&1"
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
