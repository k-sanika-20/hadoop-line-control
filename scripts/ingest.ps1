# Load a simulator run into HDFS, partitioned by date and station.
#
# No copy step: docker-compose.yml bind-mounts ./data to /work/data in the client
# container, so a run written on the host is already visible there.
#
# The bash below is a LITERAL here-string (@'...'@) with __RUN__ substituted
# afterwards. An expandable here-string (@"..."@) would let PowerShell interpret
# $(...) and $VAR meant for bash and run them on Windows instead.
param([Parameter(Mandatory = $true)][string]$Run)
$ErrorActionPreference = 'Continue'
. (Join-Path $PSScriptRoot "_common.ps1")
Push-Location (Join-Path $PSScriptRoot "..")
try {
    Assert-Docker
    Assert-Cluster

    $local = "data/run=$Run"
    if (-not (Test-Path $local)) {
        throw "no such run: $local`n  generate it first:  python sim\line_sim.py --policy policy_A.json --run $Run --days 7 --hz 1"
    }

    Write-Host "checking the bind mount ..." -ForegroundColor Cyan
    Invoke-Checked -What "run visible in container" -Script {
        docker compose exec -T client bash -lc "test -d '/work/data/run=$Run/telemetry' && test -d '/work/data/run=$Run/part_events' && test -f '/work/data/run=$Run/maintenance/maintenance.csv'"
    }

    $put = @'
set -euo pipefail
hdfs dfs -rm -r -f '/factory/run=__RUN__'
hdfs dfs -mkdir -p '/factory/run=__RUN__'
hdfs dfs -put '/work/data/run=__RUN__/telemetry'   '/factory/run=__RUN__/telemetry'
hdfs dfs -put '/work/data/run=__RUN__/part_events' '/factory/run=__RUN__/part_events'
hdfs dfs -put '/work/data/run=__RUN__/maintenance' '/factory/run=__RUN__/maintenance'
echo
echo "--- what landed in HDFS ---"
hdfs dfs -du -h -s '/factory/run=__RUN__'/*
echo
echo "--- block placement of one telemetry file ---"
F=$(hdfs dfs -ls -R '/factory/run=__RUN__/telemetry' | awk '/part-0.csv/ {print $NF; exit}')
hdfs fsck "$F" -files -blocks -locations | tail -8
'@ -replace '__RUN__', $Run

    Write-Host "putting into HDFS ..." -ForegroundColor Cyan
    Invoke-Checked -What "hdfs put" -Script {
        docker compose exec -T client bash -lc $put
    }

    Write-Host "`ningested run=$Run" -ForegroundColor Green
    Pop-Location; exit 0
}
catch { Write-Fail $_.Exception.Message; Pop-Location; exit 1 }
