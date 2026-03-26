param(
    [string]$TaskName = "DemandRadar"
)

$ErrorActionPreference = "Stop"
Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
Write-Host "Removed scheduled task: $TaskName"
