param(
    [string]$TaskName = "DemandRadar"
)

$ErrorActionPreference = "Stop"
Get-ScheduledTask -TaskName $TaskName | Get-ScheduledTaskInfo | Format-List *
