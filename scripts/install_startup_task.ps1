param(
    [string]$TaskName = "CryptoMarketDataPlant",
    [string]$ConfigPath,
    [string]$OpsRoot = "D:\market_archive\ops",
    [ValidateSet("Startup", "Logon")]
    [string]$TriggerMode = "Startup"
)

$ErrorActionPreference = "Stop"

$workspaceRoot = Split-Path -Parent $PSScriptRoot
$runnerScript = Join-Path $PSScriptRoot "run_ops_runner.ps1"

if (-not (Test-Path -LiteralPath $runnerScript)) {
    throw "Runner script missing at $runnerScript"
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
$resolvedRunner = (Resolve-Path -LiteralPath $runnerScript).Path

$action = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$resolvedRunner`" -ConfigPath `"$resolvedConfig`" -OpsRoot `"$OpsRoot`""
$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -WakeToRun
# -WakeToRun lets a time-based or repetition trigger pull the PC out of S3/S4
# sleep so collection resumes without a manual lid-open. With only an
# -AtStartup / -AtLogOn trigger (the default below) this flag is a no-op —
# add a time-based trigger via Set-ScheduledTask if you need autonomous wake.

if ($TriggerMode -eq "Startup") {
    $trigger = New-ScheduledTaskTrigger -AtStartup
    $principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest
}
else {
    $trigger = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
    $principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited
}

try {
    Register-ScheduledTask `
        -TaskName $TaskName `
        -Action $action `
        -Trigger $trigger `
        -Settings $settings `
        -Principal $principal `
        -Force | Out-Null
}
catch {
    if ($TriggerMode -eq "Startup" -and $_.Exception.Message -like "*Access is denied*") {
        throw "Startup mode requires an elevated PowerShell session. Re-run this installer as Administrator, or use -TriggerMode Logon to keep the current per-user resume behavior."
    }
    throw
}

Write-Host "registered scheduled task $TaskName trigger=$TriggerMode"
