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

    # repo root: argus resolves config/ relative to its working directory, and
    # Task Scheduler defaults to System32 — this MUST be set or capture jobs
    # fail with FileNotFoundError (found the hard way on the first real night).
    # Resolved in the body below, NOT as a param default: a $PSScriptRoot
    # reference in a default is empty when a Mandatory parameter precedes it
    # (PowerShell evaluates the default before $PSScriptRoot is populated),
    # which made Split-Path throw "empty string" and aborted the whole script.
    [string]$RepoRoot,

    [string]$NightlyTime = "23:45"
)

if (-not $RepoRoot) {
    $RepoRoot = Split-Path -Parent $PSScriptRoot
}

if (-not (Test-Path $ArgusExe)) {
    throw "argus executable not found at '$ArgusExe' - create the venv and 'pip install -e .' first."
}
if (-not (Test-Path (Join-Path $RepoRoot "config\watchlist.yaml"))) {
    throw "'$RepoRoot' does not look like the ARGUS repo (config\watchlist.yaml missing)."
}

$action = New-ScheduledTaskAction -Execute $ArgusExe -Argument "nightly" -WorkingDirectory $RepoRoot
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -WakeToRun `
    -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Hours 4)

$nightlyTrigger = New-ScheduledTaskTrigger -Daily -At $NightlyTime
Register-ScheduledTask -TaskName "ARGUS Nightly" -Action $action -Trigger $nightlyTrigger `
    -Settings $settings -Description "ARGUS nightly data pipeline (idempotent per trade date)" -Force

# -AtLogOn with no -User means "any user logon", which is an all-users task and
# requires elevation to register; scoping it to the current user keeps it a
# per-user task (no elevation), matching the Nightly trigger and this script's intent.
$logonTrigger = New-ScheduledTaskTrigger -AtLogOn -User "$env:USERDOMAIN\$env:USERNAME"
Register-ScheduledTask -TaskName "ARGUS Catch-up" -Action $action -Trigger $logonTrigger `
    -Settings $settings -Description "ARGUS catch-up run after sleep/reboot (no-op if already sealed)" -Force

Write-Output "Registered 'ARGUS Nightly' (daily $NightlyTime) and 'ARGUS Catch-up' (at logon)."
Write-Output "Verify in Task Scheduler; run once by hand with: Start-ScheduledTask -TaskName 'ARGUS Nightly'"