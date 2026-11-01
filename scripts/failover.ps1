# Fault-tolerance demo: kill a worker while J3 is running, and show that
#   1. the job still finishes, with output identical to the undisturbed run, and
#   2. HDFS notices the dead datanode and re-replicates its blocks.
#
# Run mine.ps1 first - its J3 output is the reference the result is diffed against.
#
#   .\scripts\failover.ps1                 # kill worker2, restart it at the end
#   .\scripts\failover.ps1 -LeaveDown      # leave it dead, to show it in the web UIs
#
# docker kill is SIGKILL: no clean shutdown, the closest thing to pulling the plug.
# Every bash string below avoids double quotes - see the note in ingest.ps1.
param(
  [string]$Run = "baseline",
  [string]$Victim = "worker2",
  [int]$KillAfterSeconds = 25,
  [switch]$LeaveDown
)
$ErrorActionPreference = 'Continue'
. (Join-Path $PSScriptRoot "_common.ps1")
Push-Location (Join-Path $PSScriptRoot "..")

function Get-LiveDatanodes {
    $r = (docker compose exec -T client hdfs dfsadmin -report 2>&1) | Out-String
    if ($r -match "Live datanodes \((\d+)\)") { return [int]$Matches[1] }
    return -1
}

function Get-UnderReplicated {
    $r = (docker compose exec -T client hdfs fsck "/factory/run=$Run" 2>&1) | Out-String
    if ($r -match "Under-replicated blocks:\s+(\d+)") { return [int]$Matches[1] }
    return -1
}

try {
    Assert-Docker
    Assert-Cluster

    $ref = "/factory/mined/run=$Run/limits"
    $out = "/factory/failover/limits"
    $jar = "/work/jobs/target/line-control-jobs.jar"
    Invoke-Checked -What "reference J3 output present (run mine.ps1 -Run $Run first)" -Script {
        docker compose exec -T client bash -lc "hdfs dfs -test -d $ref"
    }

    $live = Get-LiveDatanodes
    Write-Host "live datanodes before: $live" -ForegroundColor Cyan
    if ($live -lt 3) { throw "expected 3 live datanodes, found $live - restart the stopped worker with: docker start $Victim" }

    Write-Host "`n=== 1/4  start J3 control limits in the background ===" -ForegroundColor Cyan
    $launch = "rm -f /tmp/failover.rc /tmp/failover.log; hdfs dfs -rm -r -f -skipTrash $out > /dev/null 2>&1; " +
              "hadoop jar $jar limits -D lc.signal=torque -D lc.sigma=3.0 -D lc.min.value=5.0 " +
              "/factory/run=$Run/telemetry $out > /tmp/failover.log 2>&1; echo `$? > /tmp/failover.rc"
    Invoke-Checked -What "launch J3" -Script {
        docker compose exec -d client bash -lc $launch
    }

    Write-Host "letting it get going for $KillAfterSeconds s ..."
    Start-Sleep -Seconds $KillAfterSeconds
    $progress = (docker compose exec -T client bash -lc 'grep -o ''map [0-9]*% reduce [0-9]*%'' /tmp/failover.log | tail -1') | Out-String
    Write-Host "job progress at kill time: $($progress.Trim())"

    Write-Host "`n=== 2/4  kill $Victim (datanode + nodemanager) ===" -ForegroundColor Red
    Invoke-Checked -What "docker kill $Victim" -Script { docker kill $Victim }
    $killedAt = Get-Date

    Write-Host "`n=== 3/4  wait for the job to finish without it ===" -ForegroundColor Cyan
    $deadline = (Get-Date).AddMinutes(15)
    $rc = $null
    $reportedDead = $false
    while ((Get-Date) -lt $deadline) {
        Start-Sleep -Seconds 10
        $elapsed = [int]((Get-Date) - $killedAt).TotalSeconds
        $live = Get-LiveDatanodes
        $p = ((docker compose exec -T client bash -lc 'grep -o ''map [0-9]*% reduce [0-9]*%'' /tmp/failover.log | tail -1') | Out-String).Trim()
        Write-Host ("  t+{0,4}s   live datanodes: {1}   job: {2}" -f $elapsed, $live, $p)
        if ($live -eq 2 -and -not $reportedDead) {
            Write-Host "  -> NameNode has declared $Victim dead after ~$elapsed s" -ForegroundColor Yellow
            $reportedDead = $true
        }
        $rcOut = ((docker compose exec -T client bash -lc 'cat /tmp/failover.rc 2>/dev/null') | Out-String).Trim()
        if ($rcOut -ne "") { $rc = [int]$rcOut; break }
    }
    if ($null -eq $rc) { throw "J3 did not finish within 15 minutes of the kill" }
    if ($rc -ne 0) {
        docker compose exec -T client bash -lc 'tail -30 /tmp/failover.log'
        throw "J3 failed after the kill (exit code $rc)"
    }
    $took = [int]((Get-Date) - $killedAt).TotalSeconds
    Write-Host "`nJ3 completed successfully $took s after $Victim was killed." -ForegroundColor Green
    docker compose exec -T client bash -lc 'grep -E ''Launched map tasks|Failed map tasks|Killed map tasks|Launched reduce tasks|Failed reduce tasks'' /tmp/failover.log'

    Invoke-Checked -What "output identical to the undisturbed run" -Script {
        docker compose exec -T client bash -lc "diff <(hdfs dfs -cat $ref/part-* | sort) <(hdfs dfs -cat $out/part-* | sort) && echo OUTPUT IDENTICAL to the undisturbed J3 run"
    }

    Write-Host "`n=== 4/4  HDFS heals itself ===" -ForegroundColor Cyan
    $deadline = (Get-Date).AddMinutes(5)
    while ((Get-Date) -lt $deadline) {
        $live = Get-LiveDatanodes
        $under = Get-UnderReplicated
        Write-Host "  live datanodes: $live   under-replicated blocks in run=${Run}: $under"
        if ($live -eq 2 -and $under -eq 0) { break }
        Start-Sleep -Seconds 10
    }
    if ($under -eq 0 -and $live -eq 2) {
        Write-Host "Every block is back to 2 replicas on the 2 surviving nodes - no data lost." -ForegroundColor Green
    } else {
        Write-Host "re-replication still in progress - check http://localhost:9870" -ForegroundColor Yellow
    }

    if ($LeaveDown) {
        Write-Host "`n$Victim left down. Bring it back with:  docker start $Victim" -ForegroundColor Yellow
    } else {
        Write-Host "`nrestarting $Victim ..." -ForegroundColor Cyan
        Invoke-Checked -What "docker start $Victim" -Script { docker start $Victim }
        $deadline = (Get-Date).AddMinutes(2)
        while ((Get-Date) -lt $deadline -and (Get-LiveDatanodes) -lt 3) { Start-Sleep -Seconds 5 }
        Write-Host "live datanodes: $(Get-LiveDatanodes)" -ForegroundColor Green
    }

    Write-Host "`nFAILOVER DEMO PASSED" -ForegroundColor Green
    Pop-Location; exit 0
}
catch { Write-Fail $_.Exception.Message; Pop-Location; exit 1 }
