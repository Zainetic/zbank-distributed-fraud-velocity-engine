# ZBank Fraud Engine — Performance Benchmarks

This directory contains load-testing scripts and instructions to evaluate the throughput, latency, and resource efficiency of the fraud detection and velocity engine under concurrent traffic.

## Workload Profile

The benchmark simulates production traffic hitting the REST endpoint `POST /api/v1/fraud/velocity-check`:

- **Ramping Schedule:**
  - 0 &rarr; 50 Virtual Users (30s ramp-up)
  - 50 &rarr; 200 Virtual Users (1m ramp-up)
  - 200 &rarr; 500 Virtual Users (2m peak sustained load)
  - 500 &rarr; 0 Virtual Users (30s ramp-down)
- **Key Distribution:**
  - **80% Random Cards:** Spanning 10,000 distinct card IDs to test concurrent pipeline operations for standard approvals.
  - **20% High-Frequency ("Hot") Cards:** Concentrated on 20 card IDs to repeatedly cross the 3-transactions-per-minute velocity limit, testing threshold breach detection and asynchronous Kafka event publishing.

---

## Step-by-Step Execution Guide

### 1. Start Infrastructure Services

In the project root, ensure Docker is running, then spin up Redis, PostgreSQL, Kafka, and pgAdmin:

```bash
docker-compose up -d
```

Verify that all four containers are up and healthy:

```bash
docker-compose ps
```

### 2. Build and Start the Application

Build the production jar and run the application with Virtual Threads enabled:

```bash
mvn clean package -DskipTests
java -jar target/fraud-velocity-engine-0.0.1-SNAPSHOT.jar
```

Alternatively, run directly with the Spring Boot plugin:

```bash
mvn spring-boot:run
```

Ensure the engine is listening on `http://localhost:8080`.

### 3. Run the k6 Load Test

#### Option A: Using Local k6 CLI

If you have k6 installed locally:

```bash
k6 run benchmarks/load-test.js
```

To target a different host or port:

```bash
k6 run -e BASE_URL=http://localhost:8080 benchmarks/load-test.js
```

#### Option B: Using Dockerized k6

If you do not have k6 installed locally, run it via the official Docker image:

```bash
docker run --rm -i --network host grafana/k6 run - < benchmarks/load-test.js
```

---

## What to Record

After the test run finishes, collect the following metrics from the k6 summary output and system environment:

1. **Requests Per Second (RPS):** From `http_reqs` (rate).
2. **p95 Latency:** From `http_req_duration` (`p(95)` value in milliseconds).
3. **p99 Latency & Average Latency:** From `http_req_duration` (`p(99)` and `avg`).
4. **Error Rate:** From `http_req_failed` (percentage of failed requests).
5. **Machine Specifications:**
   - CPU model and core/thread count
   - System RAM
   - Operating System (Windows / Linux / macOS)
6. **Java Runtime Version:**
   - Execute `java -version` and note the vendor and build version (e.g., OpenJDK 21.0.8, Temurin 21.0.8).

Transfer these recorded values into the **Results** table in the root [README.md](../README.md).
