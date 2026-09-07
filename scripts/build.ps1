# Compile the MapReduce jobs inside a Maven container - no local JDK required.
$ErrorActionPreference = 'Continue'
. (Join-Path $PSScriptRoot "_common.ps1")
Push-Location (Join-Path $PSScriptRoot "..")
try {
    Assert-Docker
    Invoke-Checked -What "maven build" -Script {
        docker run --rm `
            -v "${PWD}/jobs:/src" `
            -v "lc-maven-repo:/root/.m2" `
            -w /src `
            maven:3.9-eclipse-temurin-11 `
            mvn -B package
    }
    $jar = "jobs/target/line-control-jobs.jar"
    if (-not (Test-Path $jar)) { throw "maven reported success but $jar is missing" }
    Write-Host "`nbuilt $jar" -ForegroundColor Green
    Pop-Location; exit 0
}
catch { Write-Fail $_.Exception.Message; Pop-Location; exit 1 }
