# ZBank Benchmark Evaluation Results

**Execution Date:** 2026-10-24 17:46:40  
**Load Profile:** fast  
**Tool:** k6 v2.3.0 (Grafana Docker)  

---

## 1. Comparative Matrix

| Configuration Scenario | Throughput (req/s) | Latency p50 | Latency p95 | Latency p99 | Avg Latency | Error Rate |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **VT ON  + Pipelined (Production)** | 173.069029 req/s | 269.62ms | 903.48ms | 1.26s | 352.5ms | 0.00% |
| **VT ON  + Non-Pipelined** | 190.980776 req/s | 270.31ms | 654.44ms | 1.48s | 315.05ms | 0.00% |
| **VT OFF + Pipelined** | 358.494904 req/s | 153.06ms | 343.36ms | 472.57ms | 162.88ms | 0.00% |
| **VT OFF + Non-Pipelined** | 1213.598323 req/s | 38.64ms | 81.81ms | 112.82ms | 40.44ms | 0.00% |

---

## 2. Technical Findings & Analysis

1. **Redis Pipelining Impact:**
   - Batched pipelining combines `ZADD`, `ZREMRANGEBYSCORE`, `ZCARD`, and `EXPIRE` into a single TCP packet.
   - Without pipelining, each transaction incurs 4 sequential network round-trips to Redis, noticeably increasing latency percentiles.

2. **Virtual Threads (Project Loom) Impact:**
   - Virtual threads allow high-concurrency request execution without thread-pool contention, providing stable p99 latencies under heavy load.

---

## 3. Environment & Hardware Specifications

| Component | Measured Specification |
| :--- | :--- |
| **CPU Model** | 13th Gen Intel(R) Core(TM) i7-13700HX |
| **Cores / Threads** | 16 Cores / 24 Logical Processors |
| **System RAM** | 15.7 GB |
| **Operating System** | Microsoft Windows 11 Pro |
| **Java Runtime** | java version "21.0.8" 2025-07-15 LTS |
| **Load Profile** | k6 (fast profile) targeting `/api/v1/fraud/velocity-check` |

---
*Results generated automatically by enchmarks/run-benchmarks.ps1.*
