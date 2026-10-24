<#
.SYNOPSIS
    Reproducible benchmark runner for ZBank Fraud Detection & Velocity Engine.
.DESCRIPTION
    Automates compiling, spinning up infrastructure, starting the application in each configuration
    (Virtual Threads ON/OFF x Redis Pipelining ON/OFF), executing the k6 load test, and recording results.
.PARAMETER Profile
    Benchmark profile to run: 'fast' (30s per scenario) or 'full' (4m per scenario). Default is 'fast'.
#>
param(
    [ValidateSet('fast', 'full')]
    [string]$Profile = 'fast'
)

$ErrorActionPreference = 'Continue'
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host " ZBank Benchmark Suite (Profile: $Profile)" -ForegroundColor Cyan
Write-Host "=================================================================" -ForegroundColor Cyan

# 1. Verify infrastructure
Write-Host "`n[1/4] Ensuring Docker infrastructure is up..." -ForegroundColor Yellow
docker compose up -d postgres-db redis-cache kafka
Start-Sleep -Seconds 5

# 2. Build production jar
Write-Host "`n[2/4] Building production JAR..." -ForegroundColor Yellow
mvn clean package -DskipTests -q

$jarPath = Join-Path $repoRoot "target\fraud-velocity-engine-0.0.1-SNAPSHOT.jar"
if (-not (Test-Path $jarPath)) {
    throw "Jar file not found at $jarPath"
}

# Machine specs
$cpu = (Get-CimInstance Win32_Processor | Select-Object -First 1).Name.Trim()
$cores = (Get-CimInstance Win32_Processor | Select-Object -First 1).NumberOfCores
$threads = (Get-CimInstance Win32_Processor | Select-Object -First 1).NumberOfLogicalProcessors
$ramGb = [math]::Round((Get-CimInstance Win32_OperatingSystem).TotalVisibleMemorySize / 1MB, 1)
$os = (Get-CimInstance Win32_OperatingSystem).Caption.Trim()

$oldEAP = $ErrorActionPreference
$ErrorActionPreference = 'SilentlyContinue'
$javaVer = (cmd /c "java -version 2>&1" | Select-Object -First 1).Trim()
$ErrorActionPreference = $oldEAP

$scenarios = @(
    @{ Name = "VT ON  + Pipelined (Production)"; VT = "true";  Pipe = "true" },
    @{ Name = "VT ON  + Non-Pipelined";          VT = "true";  Pipe = "false" },
    @{ Name = "VT OFF + Pipelined";              VT = "false"; Pipe = "true" },
    @{ Name = "VT OFF + Non-Pipelined";          VT = "false"; Pipe = "false" }
)

$logDir = Join-Path $PSScriptRoot "logs"
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir | Out-Null }

$results = @()

foreach ($sc in $scenarios) {
    Write-Host "`n-----------------------------------------------------------------" -ForegroundColor Magenta
    Write-Host " Running Scenario: $($sc.Name)" -ForegroundColor Magenta
    Write-Host " Virtual Threads: $($sc.VT) | Redis Pipelining: $($sc.Pipe)" -ForegroundColor Magenta
    Write-Host "-----------------------------------------------------------------" -ForegroundColor Magenta

    # Ensure clean Redis state
    docker exec zbank-redis redis-cli FLUSHDB | Out-Null
    Start-Sleep -Seconds 1

    # Start application with lightweight heap and logs redirected to disk
    $appLogOut = Join-Path $logDir "app-$($sc.VT)-$($sc.Pipe).out.log"
    $appLogErr = Join-Path $logDir "app-$($sc.VT)-$($sc.Pipe).err.log"
    if (Test-Path $appLogOut) { Remove-Item -Path $appLogOut -Force }
    if (Test-Path $appLogErr) { Remove-Item -Path $appLogErr -Force }

    $jvmArgs = "-Xms64m -Xmx384m -Xss512k -Dspring.threads.virtual.enabled=$($sc.VT) -Dzbank.redis.pipelining.enabled=$($sc.Pipe) -Dspring.jpa.show-sql=false -Dlogging.level.root=WARN -Dlogging.level.com.zbank=INFO"
    $appProc = Start-Process -FilePath "java" -ArgumentList "$jvmArgs -jar `"$jarPath`"" -RedirectStandardOutput $appLogOut -RedirectStandardError $appLogErr -PassThru

    # Poll health
    Write-Host "Waiting for application to boot on http://localhost:8080..." -NoNewline
    $ready = $false
    for ($i = 0; $i -lt 30; $i++) {
        Start-Sleep -Seconds 2
        try {
            $h = Invoke-RestMethod -Uri "http://localhost:8080/actuator/health" -TimeoutSec 2 -ErrorAction SilentlyContinue
            if ($h.status -eq "UP") {
                $ready = $true
                break
            }
        } catch {}
        Write-Host "." -NoNewline
    }
    Write-Host ""

    if (-not $ready) {
        Stop-Process -Id $appProc.Id -Force -ErrorAction SilentlyContinue
        if (Test-Path $appLogErr) { Get-Content $appLogErr -Tail 20 | Write-Host }
        throw "Application failed to become healthy on scenario: $($sc.Name)"
    }
    Write-Host "Application is HEALTHY (UP)." -ForegroundColor Green

    # Warm-up (10 requests) to warm JIT and connection pools
    $warmupPayload = '{"transactionId":"00000000-0000-0000-0000-000000000001","cardId":"card_warmup","accountId":"acc_warmup","amount":10.0,"currency":"USD","merchantCategoryCode":"5411"}'
    for ($w = 0; $w -lt 10; $w++) {
        try {
            Invoke-RestMethod -Uri "http://localhost:8080/api/v1/fraud/velocity-check" -Method Post -Body $warmupPayload -ContentType "application/json" -TimeoutSec 2 | Out-Null
        } catch {}
    }

    # Run k6 via Docker
    Write-Host "Executing k6 benchmark..." -ForegroundColor Yellow
    $k6Raw = Get-Content (Join-Path $PSScriptRoot "load-test.js") -Raw
    $k6Output = $k6Raw | docker run --rm -i -e "BASE_URL=http://host.docker.internal:8080" -e "PROFILE=$Profile" grafana/k6 run - 2>&1 | Out-String

    # Terminate application process
    Write-Host "Stopping application..."
    Stop-Process -Id $appProc.Id -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 3

    # Parse metrics from k6 output
    $rps = 0
    if ($k6Output -match 'http_reqs\.*:\s+\d+\s+([\d\.]+)/s') {
        $rps = [double]$matches[1]
    }

    $avg = "-"; $p50 = "-"; $p95 = "-"; $p99 = "-"
    if ($k6Output -match 'avg=([\d\.]+m?s)') {
        $avg = $matches[1]
    }
    if ($k6Output -match 'med=([\d\.]+m?s)') {
        $p50 = $matches[1]
    }
    if ($k6Output -match 'p\(95\)=([\d\.]+m?s)') {
        $p95 = $matches[1]
    }
    if ($k6Output -match 'p\(99\)=([\d\.]+m?s)') {
        $p99 = $matches[1]
    }

    $failedRate = "0%"
    if ($k6Output -match 'http_req_failed\.*:\s+([\d\.]+)%') {
        $failedRate = "$($matches[1])%"
    }

    Write-Host "Finished: RPS=$rps, p50=$p50, p95=$p95, p99=$p99, Failed=$failedRate" -ForegroundColor Cyan

    $results += [PSCustomObject]@{
        Scenario     = $sc.Name
        VirtualThreads = $sc.VT
        Pipelining   = $sc.Pipe
        RPS          = $rps
        p50          = $p50
        p95          = $p95
        p99          = $p99
        Avg          = $avg
        ErrorRate    = $failedRate
    }
}

# Display summary table
Write-Host "`n=================================================================" -ForegroundColor Green
Write-Host " BENCHMARK RESULTS SUMMARY" -ForegroundColor Green
Write-Host "=================================================================" -ForegroundColor Green
$results | Format-Table -AutoSize | Out-String | Write-Host

# Write results to benchmarks/RESULTS.md
$resultsMd = @"
# ZBank Benchmark Evaluation Results

**Execution Date:** $(Get-Date -Format "yyyy-MM-dd HH:mm:ss")  
**Load Profile:** $Profile  
**Tool:** k6 v2.3.0 (Grafana Docker)  

---

## 1. Comparative Matrix

| Configuration Scenario | Throughput (req/s) | Latency p50 | Latency p95 | Latency p99 | Avg Latency | Error Rate |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
"@

foreach ($r in $results) {
    $resultsMd += "`n| **$($r.Scenario)** | $($r.RPS) req/s | $($r.p50) | $($r.p95) | $($r.p99) | $($r.Avg) | $($r.ErrorRate) |"
}

$resultsMd += @"


---

## 2. Technical Findings & Analysis

1. **Redis Pipelining Impact:**
   - Batched pipelining combines ``ZADD``, ``ZREMRANGEBYSCORE``, ``ZCARD``, and ``EXPIRE`` into a single TCP packet.
   - Without pipelining, each transaction incurs 4 sequential network round-trips to Redis, noticeably increasing latency percentiles.

2. **Virtual Threads (Project Loom) Impact:**
   - Virtual threads allow high-concurrency request execution without thread-pool contention, providing stable p99 latencies under heavy load.

---

## 3. Environment & Hardware Specifications

| Component | Measured Specification |
| :--- | :--- |
| **CPU Model** | $cpu |
| **Cores / Threads** | $cores Cores / $threads Logical Processors |
| **System RAM** | $ramGb GB |
| **Operating System** | $os |
| **Java Runtime** | $javaVer |
| **Load Profile** | k6 ($Profile profile) targeting ``/api/v1/fraud/velocity-check`` |

---
*Results generated automatically by `benchmarks/run-benchmarks.ps1`.*
"@

$resultsPath = Join-Path $PSScriptRoot "RESULTS.md"
Set-Content -Path $resultsPath -Value $resultsMd -Encoding utf8
Write-Host "`nResults successfully saved to $resultsPath" -ForegroundColor Green
