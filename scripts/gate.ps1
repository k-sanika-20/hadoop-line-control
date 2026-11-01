# Prove the cluster is real: a stock example job, then this project's own jar,
# both on YARN. Every step is exit-code checked, so a pass means something.
$ErrorActionPreference = 'Continue'
. (Join-Path $PSScriptRoot "_common.ps1")
Push-Location (Join-Path $PSScriptRoot "..")
try {
    Assert-Docker
    Assert-Cluster

    Write-Host "`n=== 1/3  stock example on YARN ===" -ForegroundColor Cyan
    Invoke-Checked -What "stock pi example" -Script {
        docker compose exec -T client bash -lc 'hadoop jar /opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.4.1.jar pi 3 100'
    }

    # 200k synthetic torque rows over six stations, plus one row with an empty
    # station so the MALFORMED counter has something to count.
    #
    # No double quotes anywhere in this bash: Windows PowerShell 5.1 strips
    # embedded double quotes from native-command arguments, which silently
    # breaks awk string literals. They are passed in with -v instead.
    $seed = @'
set -euo pipefail
awk -v seed=$RANDOM -v hdr=timestamp,station_id,signal,value -v sfmt=S%02d -v rfmt=%d,%s,torque,%.3f\\n -v bad=1756600000,,torque, 'BEGIN { srand(seed); print hdr; for (i = 0; i < 200000; i++) { s = sprintf(sfmt, int(rand() * 6) + 1); sum = 0; for (j = 0; j < 12; j++) sum += rand(); printf rfmt, 1756600000 + i, s, 40 + (sum - 6) * 3 } print bad }' > /tmp/seed.csv
wc -l /tmp/seed.csv
hdfs dfs -mkdir -p /factory/seed
hdfs dfs -rm -r -f /factory/seed/telemetry.csv /factory/out/smoke
hdfs dfs -put -f /tmp/seed.csv /factory/seed/telemetry.csv
'@ -replace "`r", ''

    Write-Host "`n=== 2/3  seed a CSV into HDFS ===" -ForegroundColor Cyan
    Invoke-Checked -What "seed CSV" -Script {
        docker compose exec -T client bash -lc $seed
    }

    # The jar's Main-Class is the ProgramDriver, so jobs are named, not classed.
    Write-Host "`n=== 3/3  your own jar on YARN ===" -ForegroundColor Cyan
    Invoke-Checked -What "smoke job" -Script {
        docker compose exec -T client bash -lc 'hadoop jar /work/jobs/target/line-control-jobs.jar smoke /factory/seed /factory/out/smoke'
    }

    Write-Host "`n--- output ---" -ForegroundColor Cyan
    Invoke-Checked -What "output check (expected 200000 good records)" -Script {
        docker compose exec -T client bash -lc 'hdfs dfs -cat /factory/out/smoke/part-* | sort | awk ''{ print; t += $2 } END { print t; exit (t == 200000 ? 0 : 1) }'''
    }

    Write-Host "`nGATE PASSED - cluster is real, your code runs on it." -ForegroundColor Green
    Pop-Location; exit 0
}
catch { Write-Fail $_.Exception.Message; Pop-Location; exit 1 }
