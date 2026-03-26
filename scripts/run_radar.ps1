param(
    [string]$ConfigPath = "config\\sources.real.sample.json",
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
$envScriptPath = Join-Path $resolvedWorkingDirectory "scripts\\set_env.ps1"

Set-Location $resolvedWorkingDirectory

if (Test-Path $envScriptPath) {
    . $envScriptPath
}

& $PythonExe -m radar.cli --config $resolvedConfigPath
