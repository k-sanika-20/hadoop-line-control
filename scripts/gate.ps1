$ErrorActionPreference = "Stop"
Push-Location (Join-Path $PSScriptRoot "..")

Write-Host "`n=== 1/3  stock example on YARN ===" -ForegroundColor Cyan
docker compose exec -T client bash -lc `
  'hadoop jar /opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.4.1.jar pi 3 100'

Write-Host "`n=== 2/3  seed a CSV into HDFS ===" -ForegroundColor Cyan
docker compose exec -T client bash -lc "set -e; awk -v seed=`$RANDOM 'BEGIN { srand(seed); print `"timestamp,station_id,signal,value`"; for(i=0;i<200000;i++) { station=sprintf(`"S%02d`", int(rand()*6)+1); sum=0; for(j=0;j<12;j++) sum+=rand(); val=40 + (sum-6)*3; printf `"%d,%s,torque,%.3f\n`", 1756600000+i, station, val; } print `"1756600000,,torque,`"; }' > /tmp/seed.csv; hdfs dfs -mkdir -p /factory/seed; hdfs dfs -rm -r -f /factory/seed/telemetry.csv /factory/out/smoke; hdfs dfs -put -f /tmp/seed.csv /factory/seed/telemetry.csv"

Write-Host "`n=== 3/3  your own jar on YARN ===" -ForegroundColor Cyan
docker compose exec -T client bash -lc `
  'hadoop jar /work/jobs/target/line-control-jobs.jar lc.hadoop.SmokeCount /factory/seed /factory/out/smoke'

Write-Host "`n--- output ---" -ForegroundColor Cyan
docker compose exec -T client bash -lc 'hdfs dfs -cat /factory/out/smoke/part-* | sort'

Write-Host "`nGATE PASSED - cluster is real, your code runs on it." -ForegroundColor Green
Pop-Location
