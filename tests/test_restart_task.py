"""Exercise the restart transaction with fake task commands, never the real task."""
from pathlib import Path
import shutil
import subprocess

import pytest

SHELL = shutil.which('powershell.exe')
SCRIPT = Path(__file__).resolve().parents[1] / 'scripts/redeploy_runner.ps1'
pytestmark = pytest.mark.skipif(SHELL is None, reason='Windows PowerShell required')


def run_ps(tmp_path, body):
    path = tmp_path / 'check.ps1'
    source = str(SCRIPT).replace("'", "''")
    path.write_text("$ErrorActionPreference='Stop'\n. '" + source + "'\n" + body, encoding='ascii')
    result = subprocess.run([SHELL, '-NoProfile', '-File', str(path)],
                            capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize('fault', ['none', 'preflight', 'stop', 'stop_observation', 'start', 'verify'])
def test_start_attempt_is_not_skipped_after_stop(tmp_path, fault):
    run_ps(tmp_path, """
$script:events = @()
$fault = 'FAULT'
function Assert-PlantTaskPreflight {
    $script:events += 'preflight'
    if ($fault -eq 'preflight') { throw 'preflight failed' }
    return @{pid=100;created_at='old'}
}
function Stop-ScheduledTask {
    $script:events += 'stop'
    if ($fault -eq 'stop') { throw 'stop failed' }
}
function Wait-PlantTaskStopped {
    $script:events += 'settle'
    if ($fault -eq 'stop_observation') { throw 'observation failed' }
}
function Start-ScheduledTask {
    $script:events += 'start'
    if ($fault -eq 'start') { throw 'start failed' }
}
function Wait-PlantTaskRestart {
    $script:events += 'verify'
    if ($fault -eq 'verify') { throw 'verify failed' }
    return @{pid=200;created_at='new'}
}
$failed=$false
try { $result=Invoke-PlantTaskRestart -Repo fake -Root fake -Concurrency 49 -Execute } catch { $failed=$true }
$joined=$script:events -join ','
if ($fault -eq 'preflight') { $expected='preflight' }
elseif ($fault -eq 'start') { $expected='preflight,stop,settle,start' }
else { $expected='preflight,stop,settle,start,verify' }
if ($joined -ne $expected) { throw "Wrong operation order: $joined" }
if ($failed -ne ($fault -in @('preflight','start','verify'))) { throw 'Wrong final status' }
""".replace('FAULT', fault))


def test_default_preflight_does_not_stop_or_start(tmp_path):
    run_ps(tmp_path, """
function Assert-PlantTaskPreflight { return @{pid=100;created_at='old'} }
function Stop-ScheduledTask { throw 'must not stop' }
function Start-ScheduledTask { throw 'must not start' }
$result=Invoke-PlantTaskRestart -Repo fake -Root fake -Concurrency 49
if ($result.status -ne 'preflight_passed' -or $result.apply) { throw 'bad dry-run result' }
""")


def test_real_preflight_checks_task_and_config_before_stop(tmp_path):
    # Fake only OS privilege/task surfaces. Actual config/heartbeat checks read temp files.
    repo = tmp_path / 'repo'
    (repo / 'scripts').mkdir(parents=True)
    (repo / 'scripts/run_ops_runner.ps1').write_text('[int]$CollectorConcurrency = 49')
    (repo / 'ops.live.local.json').write_text('{"jobs":[{"name":"one","job_type":"bybit-depth-worker"}]}')
    root = tmp_path / 'ops'
    root.mkdir()
    (root / 'ops-runner.lock').write_text('{"pid":100,"created_at":"2026-01-01T00:00:00Z"}')
    run_ps(tmp_path, """
$repo='REPO'
$root='ROOT'
@{last_seen=[DateTime]::UtcNow.ToString('o');status='running'} | ConvertTo-Json | Set-Content (Join-Path $root 'heartbeat.json')
$task=[pscustomobject]@{State='Running';Principal=@{UserId='SYSTEM'};Settings=@{MultipleInstances='IgnoreNew'};Actions=@(@{Execute='powershell.exe';Arguments=('-NoProfile -ExecutionPolicy Bypass -File "'+$repo+'\\scripts\\run_ops_runner.ps1" -ConfigPath "'+$repo+'\\ops.live.local.json" -OpsRoot "'+$root+'"')})}
function Assert-PlantAdministrator {}
function Assert-PlantConfig { $script:validated=$true }
function Get-ScheduledTask { return $task }
function Get-CimInstance { throw 'Must not inspect shared venv processes' }
function Stop-ScheduledTask { throw 'Must not stop in preflight' }
function Start-ScheduledTask { throw 'Must not start in preflight' }
$result=Invoke-PlantTaskRestart -Repo $repo -Root $root -Concurrency 49
if ($result.status -ne 'preflight_passed') { throw 'Valid preflight refused' }
if (-not $script:validated) { throw 'Runner config validation skipped' }
$task.Actions[0].Arguments += ' -Unexpected value'
$refused=$false
try { Invoke-PlantTaskRestart -Repo $repo -Root $root -Concurrency 49 -Execute } catch { $refused=$true }
if (-not $refused) { throw 'Changed action admitted' }
""".replace('REPO', str(repo).replace("'", "''")).replace('ROOT', str(root).replace("'", "''")))


def test_delayed_stop_ignored_start_is_reconciled_without_second_stop(tmp_path):
    run_ps(tmp_path, """
$script:stops=0
$script:starts=0
$script:polls=0
$script:recovered=$false
$script:state='Running'
function Assert-PlantTaskPreflight { return @{pid=100;created_at='2026-01-01T00:00:00Z'} }
function Stop-ScheduledTask { $script:stops++ }
function Wait-PlantTaskStopped { throw 'delayed task stop' }
function Start-ScheduledTask {
    $script:starts++
    if ($script:state -eq 'Ready') {
        $script:recovered=$true
        $script:created=[DateTime]::UtcNow.ToString('o')
    }
}
function Start-Sleep { [Threading.Thread]::Sleep(10) }
function Get-ScheduledTask {
    $script:polls++
    if ($script:recovered) { $script:state='Running' }
    elseif ($script:polls -ge 2) { $script:state='Ready' }
    return @{State=$script:state}
}
function Get-Content {
    param($LiteralPath,[switch]$Raw)
    if ($LiteralPath -like '*ops-runner.lock') {
        if ($script:recovered) { return (@{pid=200;created_at=$script:created}|ConvertTo-Json) }
        return '{"pid":100,"created_at":"2026-01-01T00:00:00Z"}'
    }
    return (@{status='running';last_seen=[DateTime]::UtcNow.ToString('o')}|ConvertTo-Json)
}
$result=Invoke-PlantTaskRestart -Repo fake -Root fake -Concurrency 49 -Execute
if ($script:stops -ne 1 -or $script:starts -ne 2) { throw 'Wrong stop/start counts' }
if ($result.status -ne 'restarted_verified' -or -not $result.new_runner.start_reconciled) { throw 'Recovery not verified' }
""")


@pytest.mark.parametrize('new_identity', [False, True])
def test_reconciliation_is_bounded_and_does_not_restart_new_failed_runner(tmp_path, new_identity):
    run_ps(tmp_path, """
$script:starts=0
function Start-Sleep { [Threading.Thread]::Sleep(10) }
function Get-ScheduledTask { return @{State='Ready'} }
function Start-ScheduledTask { $script:starts++ }
function Get-Content {
    param($LiteralPath,[switch]$Raw)
    if ($LiteralPath -like '*ops-runner.lock') {
        return '{"pid":200,"created_at":"IDENTITY"}'
    }
    return '{"status":"running","last_seen":"2026-01-01T00:00:00Z"}'
}
$failed=$false
try { Wait-PlantTaskRestart -Root fake -Old @{created_at='2026-01-01T00:00:00Z'} -Started ([DateTime]::UtcNow) -TimeoutSeconds 1 } catch { $failed=$true }
if (-not $failed -or $script:starts -ne EXPECTED) { throw 'Unbounded reconciliation or false success' }
""".replace('IDENTITY', '2026-01-02T00:00:00Z' if new_identity else '2026-01-01T00:00:00Z').replace('EXPECTED', '0' if new_identity else '1'))
