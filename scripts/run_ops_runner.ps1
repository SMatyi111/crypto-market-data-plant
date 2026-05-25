param(
    [string]$ConfigPath,
    [string]$OpsRoot = "D:\market_archive\ops",
    [string]$LogPath
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
        & $pythonPath -m crypto_collector.cli ops-runner --config $resolvedConfig --ops-root $resolvedOpsRoot *>> $LogPath
        $exitCode = $LASTEXITCODE
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
