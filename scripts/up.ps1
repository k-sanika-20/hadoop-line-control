# Bring the cluster up and wait until HDFS has left safe mode.
$ErrorActionPreference = 'Continue'
. (Join-Path $PSScriptRoot "_common.ps1")
Push-Location (Join-Path $PSScriptRoot "..")
try {
    Assert-Docker
    Invoke-Checked -What "docker compose up" -Script { docker compose up -d }

    Write-Host "`nwaiting for HDFS to leave safe mode ..." -ForegroundColor Cyan
    $ok = $false
    $deadline = (Get-Date).AddMinutes(5)
    while ((Get-Date) -lt $deadline) {
        $out = (docker compose exec -T client hdfs dfsadmin -safemode get 2>&1) | Out-String
        if ($out -match "Safe mode is OFF") { $ok = $true; break }
        Start-Sleep -Seconds 6
    }
    if (-not $ok) { throw "HDFS did not leave safe mode within 5 minutes. Check:  docker compose logs namenode" }
    Write-Host "HDFS is up." -ForegroundColor Green

    Write-Host "`n--- datanodes ---" -ForegroundColor Cyan
    (docker compose exec -T client hdfs dfsadmin -report 2>&1) | Select-String -Pattern "Live datanodes|^Name:"

    Write-Host "`n--- yarn nodes ---" -ForegroundColor Cyan
    (docker compose exec -T client yarn node -list 2>&1) | Select-Object -Last 8

    Write-Host "`nNameNode UI        http://localhost:9870" -ForegroundColor Yellow
    Write-Host "ResourceManager UI http://localhost:8088" -ForegroundColor Yellow
    Pop-Location
    exit 0
}
catch {
    Write-Fail $_.Exception.Message
    Pop-Location
    exit 1
}
