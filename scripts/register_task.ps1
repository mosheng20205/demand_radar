param(
    [string]$TaskName = "DemandRadar",
    [string]$ConfigPath = "config\\sources.real.sample.json",
    [int]$IntervalMinutes = 30,
    [string]$PythonExe = "python",
    [string]$WorkingDirectory = ""
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($WorkingDirectory)) {
    $WorkingDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
    $WorkingDirectory = Split-Path -Parent $WorkingDirectory
}

$resolvedWorkingDirectory = (Resolve-Path $WorkingDirectory).Path
$resolvedConfigPath = Join-Path $resolvedWorkingDirectory $ConfigPath
$runnerScriptPath = Join-Path $resolvedWorkingDirectory "scripts\\run_radar.ps1"

if (-not (Test-Path $resolvedConfigPath)) {
    throw "Config file not found: $resolvedConfigPath"
}

$taskArgs = "-ExecutionPolicy Bypass -File `"$runnerScriptPath`" -ConfigPath `"$ConfigPath`" -PythonExe `"$PythonExe`" -WorkingDirectory `"$resolvedWorkingDirectory`""
$action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument $taskArgs
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1)
$trigger.Repetition = New-ScheduledTaskRepetitionSettingsSet -Interval (New-TimeSpan -Minutes $IntervalMinutes) -Duration ([TimeSpan]::MaxValue)
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings -Force | Out-Null

Write-Host "Registered scheduled task:"
Write-Host "  TaskName: $TaskName"
Write-Host "  WorkingDirectory: $resolvedWorkingDirectory"
Write-Host "  ConfigPath: $resolvedConfigPath"
Write-Host "  RunnerScript: $runnerScriptPath"
Write-Host "  IntervalMinutes: $IntervalMinutes"
