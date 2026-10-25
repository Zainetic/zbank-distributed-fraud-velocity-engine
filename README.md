# ZBank Distributed Real-Time Fraud Detection & Velocity Engine

[![CI](https://github.com/Zainetic/zbank-distributed-fraud-velocity-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/Zainetic/zbank-distributed-fraud-velocity-engine/actions/workflows/ci.yml)

A production-hardened real-time transaction processing and fraud detection service designed to evaluate payment requests against static merchant rules and dynamic sliding-window velocity limits. Built with Java 21 and Spring Boot 3.2.5, the engine utilizes Java Virtual Threads (Project Loom) for high-concurrency request handling, Redis pipelining and atomic Lua scripting for low-latency sliding-window tracking, Apache Kafka with dead-letter topic (DLT) retry for asynchronous audit streaming, and Spring Boot Actuator with Micrometer Prometheus for production observability.

---

## Architecture & Four-Layer Validation Pipeline

The engine evaluates incoming payment transactions through four distinct, hardened validation and auditing layers:

1. **Layer 1: Static Relational Blacklist (PostgreSQL):** Evaluates relational merchant constraints (e.g. blacklisted Merchant Category Codes) synchronously via JPA.
2. **Layer 2: Dynamic Rolling Velocity (Redis Sorted Sets):** Atomically slides a 60-second time window (`ZREMRANGEBYSCORE`, `ZCARD`, conditional `ZADD`, `EXPIRE`) via an atomic Lua script to enforce per-card frequency thresholds without race conditions or memory poisoning.
3. **Layer 3: Synchronous Decision API:** Returns an immediate decision (`APPROVED` or `DECLINED_*`) to the payment caller within milliseconds.
4. **Layer 4: Asynchronous Alert Streaming & Idempotent Auditing (Kafka + DLT + PostgreSQL):** On decline, dispatches an immutable `FraudAlertEvent` to Kafka. An asynchronous consumer persists the audit log idempotently into `fraud_audit_logs`, protected by retry backoff (3 attempts, 1s backoff) and an automated Dead-Letter Topic (`fraud-alerts-dlt`).

```mermaid
sequenceDiagram
    autonumber
    actor Caller as Payment Service / Client
    participant API as FraudCheckController
    participant Metrics as FraudMetrics (Micrometer)
    participant PG as PostgreSQL (blacklisted_merchants)
    participant Redis as Redis 7.2 (ZSET Sliding Window)
    participant Kafka as Kafka (fraud-alerts)
    participant DLT as Kafka DLT (fraud-alerts-dlt)
    participant Consumer as FraudAlertListener
    participant AuditDB as PostgreSQL (fraud_audit_logs)

    Caller->>API: POST /api/v1/fraud/velocity-check (TransactionRequest)
    API->>Metrics: Record request & start velocity timer
    
    %% Layer 1: Static Blacklist
    rect rgb(240, 248, 255)
    Note over API,PG: Layer 1: Static Relational Blacklist
    API->>PG: existsByMerchantCategoryCode(mcc)
    alt MCC is Blacklisted
        PG-->>API: true (Blacklisted)
        API->>Metrics: incrementDecision("DECLINED", "BLACKLISTED_MERCHANT")
        API-)Kafka: send("fraud-alerts", FraudAlertEvent)
        API-->>Caller: 200 OK (isFraudulent=true, DECLINED_BLACKLISTED_MERCHANT)
    end
    end

    %% Layer 2: Dynamic Velocity
    rect rgb(255, 250, 240)
    Note over API,Redis: Layer 2: Dynamic 60s Rolling Velocity
    API->>Redis: Atomic Lua: ZREMRANGEBYSCORE + ZCARD + conditional ZADD + EXPIRE(60s)
    Redis-->>API: allowed flag + transactionCount (within last 60 seconds)
    alt allowed is false (transactionCount > 3)
        API->>Metrics: incrementDecision("DECLINED", "VELOCITY_LIMIT_EXCEEDED")
        API-)Kafka: send("fraud-alerts", FraudAlertEvent)
        API-->>Caller: 200 OK (isFraudulent=true, DECLINED_VELOCITY_LIMIT_EXCEEDED)
    else allowed is true (transactionCount <= 3)
        API->>Metrics: incrementDecision("APPROVED", "APPROVED")
        API-->>Caller: 200 OK (isFraudulent=false, APPROVED)
    end
    end

    %% Layer 4: Asynchronous Auditing & DLT
    rect rgb(245, 255, 250)
    Note over Kafka,AuditDB: Layer 4: Asynchronous Idempotent Audit & DLT
    Kafka-)Consumer: Consume FraudAlertEvent
    Consumer->>AuditDB: existsByTransactionId(txId)
    alt Already Processed (Duplicate Event)
        Consumer-->>Consumer: Discard duplicate (Idempotent bypass)
    else New Transaction
        Consumer->>AuditDB: save(FraudAuditRecord)
        alt Retry Exhausted (3 Attempts, 1s Backoff)
            Consumer-)DLT: Route poison pill to fraud-alerts-dlt
        end
    end
    end
```

---

## Key Technical Features

- **Java 21 Virtual Threads:** Enabled via `spring.threads.virtual.enabled: true`, allowing lightweight request handling across thousands of concurrent payment connections without native thread stack exhaustion.
- **Atomic Redis Lua Scripting:** Replaced legacy pipelining with an atomic Redis script executing `ZREMRANGEBYSCORE`, `ZCARD`, and conditional `ZADD` in a single server-side step, preventing concurrent over-rejection races and sorted set poisoning.
- **Configurable Multi-Mode Velocity Runner:** Supports `LUA` (atomic, default), `PIPELINED`, and `NON_PIPELINED` modes via `zbank.redis.velocity.mode` for benchmarking and verification.
- **Self-Cleaning Redis TTL:** Enforces a 60-second Time-To-Live (TTL) on Redis velocity keys (`velocity:card:{cardId}`) to prevent orphaned keys and memory leaks.
- **Resilient Kafka Consumer & DLT:** Hardened with Spring Kafka's `DefaultErrorHandler`, `FixedBackOff(1000L, 3L)`, and `DeadLetterPublishingRecoverer` routing failed messages to `fraud-alerts-dlt`.
- **Idempotent Audit Persistence:** PostgreSQL `fraud_audit_logs` table enforces a unique constraint on `transaction_id`. Consumers verify idempotency prior to insertion and handle `DataIntegrityViolationException` gracefully.
- **Production Observability:** Integrated Spring Boot Actuator and Micrometer Prometheus metrics exposing real-time decision counters, p50/p95/p99 velocity timers, and Kafka publish failure counters.
- **Domain Immutability:** Uses Java 21 `record` types for all incoming requests, controller responses, error payloads, and Kafka events.

---

## Technology Stack

- **Runtime & Language:** Java 21 (LTS)
- **Framework:** Spring Boot 3.2.5 (Spring Web, Spring Data JPA, Spring Data Redis, Spring Kafka, Actuator, Micrometer Prometheus)
- **In-Memory Datastore:** Redis 7.2 (Alpine)
- **Relational Database:** PostgreSQL 16 (Alpine)
- **Event Broker:** Apache Kafka 3.7.0 (KRaft mode, ZooKeeper-free)
- **Testing & Quality:** JUnit 5, Mockito, Testcontainers, JaCoCo 0.8.12
- **Load Testing:** k6 v2.3.0 (Grafana Docker)
- **Containerization:** Docker & Docker Compose

---

## Local Development & Infrastructure

Infrastructure services are containerized via Docker Compose. All secrets and credentials are parameterized through environment variables.

### 1. Initialize Infrastructure

Copy `.env.example` to `.env` and start the containers:

```bash
cp .env.example .env
docker compose up -d postgres-db redis-cache kafka
```

*Note: PostgreSQL is mapped to host port `5433` to prevent port collisions.*

### 2. Run the Application

```bash
mvn spring-boot:run
```

The application boots on `http://localhost:8080`.

### 3. Observability & Health Endpoints

- **Health Status:** `GET http://localhost:8080/actuator/health`
- **Prometheus Metrics:** `GET http://localhost:8080/actuator/prometheus`
- **Key Custom Metrics:**
  - `zbank_fraud_decisions_total{status="APPROVED|DECLINED", reason="..."}`: Total fraud decisions by outcome.
  - `zbank_velocity_check_time_seconds{quantile="0.5|0.95|0.99"}`: Latency percentiles of Redis sliding-window evaluation.
  - `zbank_kafka_publish_failures_total`: Counter tracking asynchronous Kafka publish failures.

---

## API Specification

### Evaluate Transaction Velocity

- **Endpoint:** `POST /api/v1/fraud/velocity-check`
- **Content-Type:** `application/json`

#### Request Payload
```json
{
  "transactionId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "cardId": "card_554433",
  "accountId": "acc_112233",
  "amount": 150.00,
  "currency": "EUR",
  "merchantCategoryCode": "5411"
}
```

#### Approved Response (HTTP 200 OK)
```json
{
  "transactionId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "isFraudulent": false,
  "statusReason": "APPROVED"
}
```

#### Declined Response (HTTP 200 OK)
```json
{
  "transactionId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "isFraudulent": true,
  "statusReason": "DECLINED_VELOCITY_LIMIT_EXCEEDED"
}
```

#### Validation Error (HTTP 400 Bad Request)
```json
{
  "status": 400,
  "message": "Validation Failed",
  "details": {
    "amount": "Transaction amount must be strictly positive"
  },
  "timestamp": "2026-10-24T15:00:00.000Z"
}
```

---

## Verified Benchmark Results

Benchmarks are fully reproducible using the automated runner comparing 6 scenarios: **Virtual Threads (ON vs OFF)** $\times$ **Velocity Execution Mode (Atomic Lua vs Pipelined vs Non-Pipelined)**.

### Reproduce Benchmarks with One Command

```powershell
# Windows PowerShell
.\benchmarks\run-benchmarks.ps1 -Profile fast

# Linux / macOS / Git Bash
./benchmarks/run-benchmarks.sh fast
```

### Measured Comparative Matrix

*Evaluated with 30-second unmeasured warm-up + 3 runs per scenario using k6 against `/api/v1/fraud/velocity-check` (100 concurrent VUs, 80/20 card distribution):*

| Configuration Scenario | Virtual Threads | Redis Execution Mode | Median Throughput | Throughput Range (Min - Max) | Median p50 / p95 / p99 | Error Rate (Min - Max) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **VT ON + Lua (Atomic, Production)** | **true** | **LUA** | **1,154.89 req/s** | 954.19 – 1,314.42 req/s | **22.38ms / 42.39ms / 59.04ms** | **0.00% – 0.05%** |
| **VT ON + Pipelined** | **true** | **PIPELINED** | **84.52 req/s** | 47.01 – 321.66 req/s | 54.09ms / 142.29ms / 174.44ms | 16.62% – 96.66% |
| **VT ON + Non-Pipelined** | **true** | **NON_PIPELINED** | **1,305.68 req/s** | 737.57 – 1,309.28 req/s | 35.78ms / 69.75ms / 93.98ms | 0.00% – 0.64% |
| **VT OFF + Lua (Atomic)** | **false** | **LUA** | **1,035.63 req/s** | 745.60 – 1,057.06 req/s | 21.23ms / 80.11ms / 147.51ms | 0.00% – 0.00% |
| **VT OFF + Pipelined** | **false** | **PIPELINED** | **98.90 req/s** | 40.79 – 300.46 req/s | 54.92ms / 148.41ms / 210.61ms | 11.19% – 94.35% |
| **VT OFF + Non-Pipelined** | **false** | **NON_PIPELINED** | **1,023.75 req/s** | 632.84 – 1,111.58 req/s | 51.79ms / 98.05ms / 125.82ms | 0.00% – 0.01% |

### Benchmark Environment Specifications

| Component | Measured System Specification |
| :--- | :--- |
| **CPU Model** | 13th Gen Intel(R) Core(TM) i7-13700HX (16 Cores / 24 Logical Processors) |
| **RAM** | 15.7 GB DDR5 |
| **Operating System** | Microsoft Windows 11 Pro (Build 26100) |
| **Java Runtime** | Oracle Java HotSpot(TM) 64-Bit Server VM (build 21.0.8+12-LTS-250) |
| **Load Profile** | k6 with 100 concurrent VUs (30s warm-up + 3 measured iterations) |
| **Machine Locality** | Load generator (k6) and application share the same machine, CPU cores, and memory |

> [!NOTE]
> **Engineering Root-Cause Analysis (Debunking the 173 req/s vs 1,213 req/s Anomaly):**
> - **Virtual Threads are NOT the bottleneck:** Under atomic Lua, Virtual Threads achieve **1,154.89 req/s** with a p99 of **59.04ms** (outperforming Platform Threads at 1,035.63 req/s, p99 147.51ms). Under non-pipelined execution, Virtual Threads achieve **1,305.68 req/s** (p99 93.98ms).
> - **The True Bottleneck:** In Lettuce/Spring Data Redis, `executePipelined` opens dedicated TCP connections per thread instead of multiplexing over the shared connection. Under high concurrent load, this floods the OS with short-lived sockets, causing ephemeral TCP port exhaustion (`java.net.BindException: Address already in use`) and 50%+ connection drop rates on both thread models.
> - **Atomic Lua Solution:** Executing an atomic Lua script (`EVAL`) runs over Lettuce's single shared multiplexed non-blocking connection, eliminating ephemeral port exhaustion while atomically preventing velocity race conditions and sorted-set poisoning.

---

## Test Suite & Code Coverage

The test suite covers unit logic, concurrency boundaries, Spring WebMvc contracts, and live Testcontainers integration.

### Summary
- **Total Tests:** 37 automated tests (0 failures, 0 errors, 0 skipped).
- **Core Domain & Consumer Coverage:** **100% Instruction Coverage** on `FraudAlertListener`, `FraudMetrics`, `TransactionRequest`, `FraudAlertEvent`, `FraudCheckResponse`, `RedisConfig`, and **99.38%** on `VelocityFraudDetector` (100% line coverage, 88.24% branch coverage).

### Test Inventory

1. **`VelocityFraudDetectorTest`** (`src/test/java/com/zbank/fraud/service/VelocityFraudDetectorTest.java` — 9 tests):
   - `shouldApproveSingleValidTransaction`: Isolated transaction passes.
   - `shouldApproveExactlyThreeTransactionsWithinSixtySeconds`: Boundary check at limit.
   - `shouldDeclineFourthTransactionWithinSixtySeconds`: Rejection on 4th transaction.
   - `shouldApproveFourthTransactionIfOldestTransactionFellOutsideWindow`: Sliding-window pruning (`ZREMRANGEBYSCORE`).
   - `shouldTrackVelocityIndependentlyForDifferentCards`: Key namespace isolation.
   - `shouldEnforceVelocityLimitEquallyInNonPipelinedMode`: Behavioral parity verification.
   - `shouldNotRetainDeclinedTransactionInSortedSetOrCountAgainstLaterRequests`: Regression test for set poisoning (Bug b).
   - `shouldApproveOneAndDeclineOneWhenTwoConcurrentRequestsHitLastAvailableSlot`: Concurrency race condition verification (Bug a).
   - `shouldReturnAllowedStatusAndAccurateCountViaEvaluateVelocity`: Atomic decision and transaction count accuracy.

2. **`VelocityFraudDetectorUnitTest`** (`src/test/java/com/zbank/fraud/service/VelocityFraudDetectorUnitTest.java` — 12 tests):
   - Comprehensive unit test suite exercising all branching paths, mode switches (`LUA`, `PIPELINED`, `NON_PIPELINED`), null/empty script fallbacks, and boundary conditions.

3. **`RedisVelocitySlidingWindowTestcontainersTest`** (`src/test/java/com/zbank/fraud/service/RedisVelocitySlidingWindowTestcontainersTest.java` — 3 tests):
   - Hermetic Testcontainers integration with real Redis 7.2 container.
   - Sliding window expiration and multi-threaded stress testing.

4. **`FraudCheckControllerValidationTest`** (`src/test/java/com/zbank/fraud/controller/FraudCheckControllerValidationTest.java` — 7 tests):
   - `@Valid` constraints (`@Positive`, `@NotNull`, `@NotBlank`) returning HTTP 400 with standardized error schemas.
   - Spring Boot Actuator `/actuator/health` and `/actuator/prometheus` endpoint verification.

5. **`FraudAlertPublishingTest`** (`src/test/java/com/zbank/fraud/service/FraudAlertPublishingTest.java` — 6 tests):
   - Event publication to `fraud-alerts` on velocity decline and blacklisted MCC.
   - No event publication on approved transactions.
   - `FraudAlertListener` audit record mapping and persistence.
   - Idempotent deduplication check (`existsByTransactionId`).
   - Dead-letter topic routing on consumer failures.

### Execute Tests & JaCoCo Coverage

```bash
mvn clean test
```

JaCoCo HTML coverage report is generated at `target/site/jacoco/index.html`.
