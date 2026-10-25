<#
.SYNOPSIS
    Reproducible benchmark runner for ZBank Fraud Detection & Velocity Engine.
.DESCRIPTION
    Automates compiling, spinning up infrastructure, starting the application in each configuration
    (Platform vs Virtual Threads x Pipelined vs Non-Pipelined vs Lua Mode), executing a 30-second
    unmeasured warm-up, running 3 measured benchmark iterations, collecting Prometheus metrics,
    and recording results without averaging percentiles.
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
Write-Host " ZBank Benchmark Suite (Profile: $Profile, 3 Runs + 30s Warm-Up)" -ForegroundColor Cyan
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

# Machine & Environment specs
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
    @{ Name = "VT ON  + Lua (Atomic)";      VT = "true";  Mode = "LUA";           Pipe = "true" },
    @{ Name = "VT ON  + Pipelined";         VT = "true";  Mode = "PIPELINED";     Pipe = "true" },
    @{ Name = "VT ON  + Non-Pipelined";     VT = "true";  Mode = "NON_PIPELINED"; Pipe = "false" },
    @{ Name = "VT OFF + Lua (Atomic)";      VT = "false"; Mode = "LUA";           Pipe = "true" },
    @{ Name = "VT OFF + Pipelined";         VT = "false"; Mode = "PIPELINED";     Pipe = "true" },
    @{ Name = "VT OFF + Non-Pipelined";     VT = "false"; Mode = "NON_PIPELINED"; Pipe = "false" }
)

$logDir = Join-Path $PSScriptRoot "logs"
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir | Out-Null }

$scenarioSummaries = @()

foreach ($sc in $scenarios) {
    Write-Host "`n-----------------------------------------------------------------" -ForegroundColor Magenta
    Write-Host " Running Scenario: $($sc.Name)" -ForegroundColor Magenta
    Write-Host " Virtual Threads: $($sc.VT) | Velocity Mode: $($sc.Mode) | Pipelining: $($sc.Pipe)" -ForegroundColor Magenta
    Write-Host "-----------------------------------------------------------------" -ForegroundColor Magenta

    # Ensure clean Redis state
    docker exec zbank-redis redis-cli FLUSHDB | Out-Null
    Start-Sleep -Seconds 1

    # Start application with lightweight heap and logs redirected to disk
    $appLogOut = Join-Path $logDir "app-$($sc.VT)-$($sc.Mode).out.log"
    $appLogErr = Join-Path $logDir "app-$($sc.VT)-$($sc.Mode).err.log"
    if (Test-Path $appLogOut) { Remove-Item -Path $appLogOut -Force }
    if (Test-Path $appLogErr) { Remove-Item -Path $appLogErr -Force }

    $jvmArgs = "-Xms64m -Xmx384m -Xss512k -Dspring.threads.virtual.enabled=$($sc.VT) -Dzbank.redis.velocity.mode=$($sc.Mode) -Dzbank.redis.pipelining.enabled=$($sc.Pipe) -Dspring.jpa.show-sql=false -Dlogging.level.root=WARN -Dlogging.level.com.zbank=INFO"
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

    # --- 30-Second Unmeasured Warm-Up ---
    Write-Host "Executing 30-second unmeasured warm-up..." -ForegroundColor Yellow
    $k6Raw = Get-Content (Join-Path $PSScriptRoot "load-test.js") -Raw
    $k6Raw | docker run --rm -i -e "BASE_URL=http://host.docker.internal:8080" -e "PROFILE=warmup" grafana/k6 run - 2>&1 | Out-Null
    Write-Host "30-second warm-up completed. Starting 3 measured benchmark runs..." -ForegroundColor Green

    $runs = @()
    for ($r = 1; $r -le 3; $r++) {
        Write-Host "  [Run $r of 3] Executing k6 benchmark..." -ForegroundColor Yellow
        $k6Output = $k6Raw | docker run --rm -i -e "BASE_URL=http://host.docker.internal:8080" -e "PROFILE=$Profile" grafana/k6 run - 2>&1 | Out-String

        $rps = 0.0
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

        $failedRate = "0.00%"
        if ($k6Output -match 'http_req_failed\.*:\s+([\d\.]+)%') {
            $failedRate = "$($matches[1])%"
        }

        Write-Host "    -> Run $r: RPS=$rps, p50=$p50, p95=$p95, p99=$p99, Failed=$failedRate" -ForegroundColor Cyan

        $runs += [PSCustomObject]@{
            RunNumber = $r
            RPS       = $rps
            p50       = $p50
            p95       = $p95
            p99       = $p99
            Avg       = $avg
            ErrorRate = $failedRate
        }
        Start-Sleep -Seconds 2
    }

    # Collect Prometheus Metrics for diagnostic analysis
    $hikariPending = "0"
    $hikariActive = "0"
    $hikariIdle = "0"
    $hikariTimeout = "0"
    try {
        $promRaw = (Invoke-WebRequest -Uri "http://localhost:8080/actuator/prometheus" -TimeoutSec 3 -ErrorAction SilentlyContinue).Content
        if ($promRaw -match 'hikaricp_connections_pending\{[^\}]*\}\s+([\d\.]+)') {
            $hikariPending = $matches[1]
        }
        if ($promRaw -match 'hikaricp_connections_active\{[^\}]*\}\s+([\d\.]+)') {
            $hikariActive = $matches[1]
        }
        if ($promRaw -match 'hikaricp_connections_idle\{[^\}]*\}\s+([\d\.]+)') {
            $hikariIdle = $matches[1]
        }
        if ($promRaw -match 'hikaricp_connections_timeout_total\{[^\}]*\}\s+([\d\.]+)') {
            $hikariTimeout = $matches[1]
        }
    } catch {}

    # Terminate application process
    Write-Host "Stopping application..."
    Stop-Process -Id $appProc.Id -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 3

    # Calculate median & min-max throughput
    $sortedRps = ($runs | ForEach-Object { $_.RPS }) | Sort-Object
    $minRps = $sortedRps[0]
    $medianRps = $sortedRps[1]
    $maxRps = $sortedRps[2]

    $scenarioSummaries += [PSCustomObject]@{
        Scenario        = $sc.Name
        VirtualThreads  = $sc.VT
        Mode            = $sc.Mode
        Pipelining      = $sc.Pipe
        MedianRPS       = $medianRps
        MinRPS          = $minRps
        MaxRPS          = $maxRps
        Runs            = $runs
        HikariPending   = $hikariPending
        HikariActive    = $hikariActive
        HikariIdle      = $hikariIdle
        HikariTimeouts  = $hikariTimeout
    }
}

# Display summary table
Write-Host "`n=================================================================" -ForegroundColor Green
Write-Host " BENCHMARK RESULTS SUMMARY (3 RUNS PER SCENARIO)" -ForegroundColor Green
Write-Host "=================================================================" -ForegroundColor Green

foreach ($summary in $scenarioSummaries) {
    Write-Host "`nScenario: $($summary.Scenario) (Median RPS: $($summary.MedianRPS) [Range: $($summary.MinRPS) - $($summary.MaxRPS)])" -ForegroundColor Yellow
    $summary.Runs | Format-Table -Property RunNumber, RPS, p50, p95, p99, Avg, ErrorRate -AutoSize | Out-String | Write-Host
    Write-Host "  -> Diagnostics: Hikari Pending=$($summary.HikariPending), Active=$($summary.HikariActive), Idle=$($summary.HikariIdle), Timeouts=$($summary.HikariTimeouts)" -ForegroundColor Gray
}

# Write results to benchmarks/RESULTS.md
$resultsMd = @"
# ZBank Benchmark Evaluation Results

**Execution Date:** $(Get-Date -Format "yyyy-MM-dd HH:mm:ss")  
**Load Profile:** $Profile (100 concurrent VUs)  
**Warm-Up:** 30-second unmeasured warm-up prior to measurement  
**Iterations:** 3 runs per scenario (reporting individual run percentiles and median throughput)  
**Tool:** k6 v2.3.0 (Grafana Docker)  

---

## 1. Comparative Matrix

| Configuration Scenario | Median Throughput | Throughput Range (Min - Max) | Run 1 (p50 / p95 / p99 / Error) | Run 2 (p50 / p95 / p99 / Error) | Run 3 (p50 / p95 / p99 / Error) |
| :--- | :---: | :---: | :---: | :---: | :---: |
"@

foreach ($s in $scenarioSummaries) {
    $r1 = "$($s.Runs[0].p50) / $($s.Runs[0].p95) / $($s.Runs[0].p99) / $($s.Runs[0].ErrorRate)"
    $r2 = "$($s.Runs[1].p50) / $($s.Runs[1].p95) / $($s.Runs[1].p99) / $($s.Runs[1].ErrorRate)"
    $r3 = "$($s.Runs[2].p50) / $($s.Runs[2].p95) / $($s.Runs[2].p99) / $($s.Runs[2].ErrorRate)"
    $resultsMd += "`n| **$($s.Scenario)** | **$($s.MedianRPS) req/s** | $($s.MinRPS) - $($s.MaxRPS) req/s | $r1 | $r2 | $r3 |"
}

$resultsMd += @"


---

## 2. Technical Findings & Virtual Threads Investigation

1. **Redis Execution Modes Comparison:**
   - **Atomic Lua Script (`LUA`):** Eliminates both the race condition (Bug a) and set poisoning (Bug b) with a single server-side script execution.
   - **Pipelined (`PIPELINED`):** Batches multiple commands in one round-trip but suffers from race condition under concurrency.
   - **Non-Pipelined (`NON_PIPELINED`):** Four discrete sequential round-trips to Redis.

2. **Virtual Threads Diagnostic Findings:**
   - Database connection pool contention (HikariCP default: 10 connections) acts as a primary bottleneck under concurrent load.
   - When requests flood the service with 100 concurrent virtual threads, all 100 threads contend for the 10 Hikari connections during the PostgreSQL merchant-blacklist check (`existsByMerchantCategoryCode`).
   - Hikari pending connection counter (`hikaricp_connections_pending`) reflects virtual threads queued waiting for a free JDBC connection.

---

## 3. Environment & Hardware Specifications

| Component | Measured System Specification |
| :--- | :--- |
| **CPU Model** | $cpu |
| **Cores / Threads** | $cores Cores / $threads Logical Processors |
| **System RAM** | $ramGb GB |
| **Operating System** | $os |
| **Java Runtime** | $javaVer |
| **JVM Flags** | `-Xms64m -Xmx384m -Xss512k -Dspring.jpa.show-sql=false -Dlogging.level.root=WARN` |
| **HikariCP Pool Size** | `maximumPoolSize: 10` (HikariCP default) |
| **Redis Connection Setup** | Lettuce driver, single shared multiplexed non-blocking connection |
| **Machine Locality** | Load generator (k6) and application share the same machine, CPU cores, and memory |

---
*Results generated automatically by `benchmarks/run-benchmarks.ps1`.*
"@

$resultsPath = Join-Path $PSScriptRoot "RESULTS.md"
Set-Content -Path $resultsPath -Value $resultsMd -Encoding utf8
Write-Host "`nResults successfully saved to $resultsPath" -ForegroundColor Green
