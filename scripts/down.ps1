# Stop the cluster. -Wipe also deletes HDFS data (forces a namenode reformat).
param([switch]$Wipe)
$ErrorActionPreference = 'Continue'
. (Join-Path $PSScriptRoot "_common.ps1")
Push-Location (Join-Path $PSScriptRoot "..")
try {
    if ($Wipe) { docker compose down -v } else { docker compose down }
    Pop-Location; exit 0
}
catch { Write-Fail $_.Exception.Message; Pop-Location; exit 1 }
