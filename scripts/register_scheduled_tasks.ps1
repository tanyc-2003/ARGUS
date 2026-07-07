<#
Registers the two ARGUS scheduled tasks on Windows Task Scheduler:

  ARGUS Nightly  - fires daily at 23:45 local time (comfortably after US close year-round,
                   including the US/UK DST misalignment weeks). The runner itself resolves
                   the trade date from the exchange calendar, so the local firing time only
                   needs to be "late enough".
  ARGUS Catch-up - fires at every logon. The runner is idempotent per trade date, so
                   double-firing is harmless; this recovers nights lost to sleep/shutdown.

Both tasks are configured with StartWhenAvailable so a missed schedule runs as soon as
the machine is next awake.

Usage (elevated PowerShell not required for per-user tasks):
  .\register_scheduled_tasks.ps1 -ArgusExe "C:\argus-data\venv\Scripts\argus.exe"
#>
param(
    [Parameter(Mandatory = $true)]
    [string]$ArgusExe,

    [string]$NightlyTime = "23:45"
)

if (-not (Test-Path $ArgusExe)) {
    throw "argus executable not found at '$ArgusExe' - create the venv and 'pip install -e .' first."
}

$action = New-ScheduledTaskAction -Execute $ArgusExe -Argument "nightly"
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -WakeToRun `
    -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Hours 4)

$nightlyTrigger = New-ScheduledTaskTrigger -Daily -At $NightlyTime
Register-ScheduledTask -TaskName "ARGUS Nightly" -Action $action -Trigger $nightlyTrigger `
    -Settings $settings -Description "ARGUS nightly data pipeline (idempotent per trade date)" -Force

$logonTrigger = New-ScheduledTaskTrigger -AtLogOn
Register-ScheduledTask -TaskName "ARGUS Catch-up" -Action $action -Trigger $logonTrigger `
    -Settings $settings -Description "ARGUS catch-up run after sleep/reboot (no-op if already sealed)" -Force

Write-Output "Registered 'ARGUS Nightly' (daily $NightlyTime) and 'ARGUS Catch-up' (at logon)."
Write-Output "Verify in Task Scheduler; run once by hand with: Start-ScheduledTask -TaskName 'ARGUS Nightly'"
