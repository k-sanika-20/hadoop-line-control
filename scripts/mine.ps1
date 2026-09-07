# Run the four MapReduce jobs over an ingested run, collect the output, and
# synthesise the next policy.
param(
  [Parameter(Mandatory = $true)][string]$Run,
  [double]$MinValue = 5.0    # torque below this = machine stopped, excluded from J3/J4
)
$ErrorActionPreference = 'Continue'
. (Join-Path $PSScriptRoot "_common.ps1")
Push-Location (Join-Path $PSScriptRoot "..")
try {
    Assert-Docker
    Assert-Cluster

    $jar  = "/work/jobs/target/line-control-jobs.jar"
    $base = "/factory/run=$Run"
    $out  = "/factory/mined/run=$Run"

    Invoke-Checked -What "jar present in container" -Script {
        docker compose exec -T client bash -lc "test -f $jar"
    }
    Invoke-Checked -What "run present in HDFS (did ingest.ps1 succeed?)" -Script {
        docker compose exec -T client bash -lc "hdfs dfs -test -d '$base/part_events'"
    }

    # Nameplate ideal cycle times, passed to J1 so OEE performance is measured
    # against the rated rate rather than the fastest cycle ever observed.
    $pol = Get-Content policy_A.json -Raw | ConvertFrom-Json
    $ideal = ($pol.stations.PSObject.Properties |
              ForEach-Object { "-D lc.ideal.$($_.Name)=$($_.Value.ideal_cycle_s)" }) -join ' '

    Write-Host "clearing previous job output ..." -ForegroundColor Cyan
    Invoke-Checked -What "hdfs rm" -Script {
        docker compose exec -T client bash -lc "hdfs dfs -rm -r -f '$out'"
    }

    $jobs = @(
        @{ n = "J1 oee";            c = "oee $ideal '$base/part_events' '$out/oee'" },
        @{ n = "J2 bottleneck";     c = "bottleneck '$base/part_events' '$out/bottleneck'" },
        @{ n = "J3 control limits"; c = "limits -D lc.signal=torque -D lc.sigma=3.0 -D lc.min.value=$MinValue '$base/telemetry' '$out/limits'" },
        @{ n = "J4 degradation";    c = "degradation -D lc.signal=torque -D lc.bucket.seconds=1800 -D lc.min.value=$MinValue '$base/telemetry' '$base/maintenance/maintenance.csv' '$out/degradation'" }
    )
    foreach ($j in $jobs) {
        Write-Host "`n=== $($j.n) ===" -ForegroundColor Cyan
        $cmd = $j.c
        $label = $j.n
        Invoke-Checked -What $label -Script {
            docker compose exec -T client bash -lc "hadoop jar $jar $cmd"
        }
    }

    $fetch = @'
set -euo pipefail
rm -rf /work/data/mined
for j in oee bottleneck limits degradation; do
  mkdir -p "/work/data/mined/$j"
  hdfs dfs -get '/factory/mined/run=__RUN__'/"$j"/part-* "/work/data/mined/$j/"
done
find /work/data/mined -type f | sort
'@ -replace '__RUN__', $Run

    Write-Host "`ncollecting job output ..." -ForegroundColor Cyan
    Invoke-Checked -What "hdfs get" -Script {
        docker compose exec -T client bash -lc $fetch
    }

    if (-not (Test-Path "data\mined\limits")) {
        throw "job output did not appear at data\mined - is ./data still bind-mounted to /work/data in docker-compose.yml?"
    }

    New-Item -ItemType Directory -Force -Path artifacts | Out-Null
    if (Test-Path artifacts\mined) { Remove-Item -Recurse -Force artifacts\mined }
    Copy-Item -Recurse data\mined artifacts\mined

    Write-Host "`nsynthesising policy B ..." -ForegroundColor Cyan
    Invoke-Checked -What "build_policy.py" -Script {
        python sim\build_policy.py --mined artifacts\mined --base policy_A.json --out artifacts\policy_B.json
    }

    Write-Host "`nCheck against data\run=$Run\run_summary.json:" -ForegroundColor Yellow
    Write-Host "  J2 must name S03 as the highest-utilisation station" -ForegroundColor Yellow
    Write-Host "  J4 must give S04 the steepest degradation slope" -ForegroundColor Yellow
    Write-Host "  J1 OEE per station must now MATCH the summary (it did not before)" -ForegroundColor Yellow
    Pop-Location; exit 0
}
catch { Write-Fail $_.Exception.Message; Pop-Location; exit 1 }
