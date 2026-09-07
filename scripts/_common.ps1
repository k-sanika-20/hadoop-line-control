# Shared guards.
#
# Deliberately NOT using $ErrorActionPreference = "Stop". In Windows PowerShell
# 5.1 that setting turns any stderr output from a native program into a
# terminating error - and Hadoop writes all of its INFO logging to stderr, as
# does `docker info` (cgroup warnings). Under "Stop" a perfectly healthy
# `hadoop jar` run aborts on its own log output.
#
# Instead: exit codes are checked explicitly after every native call, which is
# the only reliable signal a native program gives.

$ErrorActionPreference = 'Continue'

function Assert-Docker {
    $null = docker info 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "Docker engine is not reachable. Start Docker Desktop, wait for the whale icon in the system tray to stop animating, then retry."
    }
}

function Assert-Cluster {
    # A direct functional test beats parsing `docker compose ps`, whose flags
    # differ between Compose versions.
    $null = docker compose exec -T client true 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "The cluster is not reachable. Start it with:  powershell -ExecutionPolicy Bypass -File .\scripts\up.ps1"
    }
    $null = docker compose exec -T client hdfs dfsadmin -safemode get 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "HDFS is not answering. Check:  docker compose ps   and   docker compose logs namenode"
    }
}

function Invoke-Checked {
    param(
        [Parameter(Mandatory = $true)][scriptblock]$Script,
        [string]$What = "step"
    )
    & $Script
    if ($LASTEXITCODE -ne 0) {
        throw "$What failed (exit code $LASTEXITCODE)"
    }
}

function Write-Fail {
    param([string]$Message)
    Write-Host ""
    Write-Host "FAILED: $Message" -ForegroundColor Red
}
