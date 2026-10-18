# **ZBank Distributed Real-Time Fraud Detection & Velocity Engine**

A high-throughput, low-latency transaction processing and fraud mitigation engine designed to evaluate payment requests in real time. Built with Java 21 and Spring Boot 3.2.5, this system leverages Virtual Threads (Project Loom) for massive concurrency, Redis Pipelining for sub-millisecond sliding-window calculations, and Apache Kafka for asynchronous event-driven auditing.

This architecture mirrors enterprise FinTech payment gateways, protecting financial infrastructure against rapid-fire card exploitation and velocity attacks without blocking the main payment execution threads.

## Architectural Overview

The engine implements a decoupled, multi-tier validation process for every incoming transaction:

**Synchronous Static Rules (PostgreSQL):** Evaluates the payload against hardcoded relational constraints (e.g., Blacklisted Merchant Category Codes).

**Synchronous Velocity Engine (Redis):** Executes a rolling 60-second sliding-window algorithm to evaluate card activity frequency.

**Asynchronous Alerting (Apache Kafka):** If a transaction triggers a fraud rule, the API immediately returns a decline to the client while simultaneously publishing a FraudAlertEvent to a Kafka topic.

**Decoupled Auditing (Kafka Consumer -> PostgreSQL):** A background listener consumes the alert events and persists them to an audit ledger (fraud_audit_logs), ensuring database I/O latency never impacts the client-facing payment response time.

## Key Technical Features

**Virtual Thread Concurrency:** Out-of-the-box configuration for Java 21 Virtual Threads, removing standard OS thread-per-request limitations. The engine gracefully handles thousands of concurrent REST API evaluations without thread pool exhaustion.

**Optimized Network I/O via Pipelining:** Batches multiple Redis operations (ZADD, ZREMRANGEBYSCORE, ZCARD, EXPIRE) into a single network round-trip.

**Event-Driven Microservice Design:** Integrates Apache Kafka (running in KRaft mode) to completely decouple fraud detection logic from downstream compliance and notification systems.

**Domain Immutability & Boundary Validation:** Enforces strict data integrity using Java 21 record representations and Jakarta Validation for incoming payloads. Global exception handlers (@ControllerAdvice) ensure precise, standardized API contracts.

**Self-Cleaning Data Structures:** Automatic Time-To-Live (TTL) enforcement on transactional keys inside Redis to prevent memory leaks and safely manage RAM usage.

## Technology Stack

**Language:** Java 21

**Framework:** Spring Boot 3.2.5 (Web, Validation, Data JPA, Data Redis, Kafka)

**In-Memory Datastore:** Redis 7.2 (Alpine)

**Relational Database:** PostgreSQL 16 (Alpine)

**Event Streaming:** Apache Kafka 3.7.0 (Official Apache Image / KRaft Mode)

**Infrastructure Management:** Docker & Docker Compose

**Database Administration:** pgAdmin 4

## Local Development & Deployment

The entire infrastructure is containerized and managed via Docker Compose.

**1. Initialize the Infrastructure**

Ensure Docker is running. Safe local demo defaults are provided in `docker-compose.yml` and `.env.example`. You can optionally copy `.env.example` to `.env` to customize settings:

```bash
cp .env.example .env
```

Then execute the following command in the project root to spin up PostgreSQL, Redis, Apache Kafka, and pgAdmin:

_docker-compose up -d_

Note: The PostgreSQL container is mapped to host port 5433 to prevent collisions with local database installations.

**2. Boot the Spring Application**

Run the **FraudVelocityEngineApplication.java** entry point. Spring Boot will automatically connect to the Docker network and use Hibernate to initialize the blacklisted_merchants and **fraud_audit_logs** tables.

**3. Database Management (pgAdmin)**

A visual database dashboard is included in the Docker stack.

**URL:** http://localhost:5050

**Credentials (local demo only, never reuse):** admin@zbank.com / admin

**Connection Host:** postgres-db (Port: 5432, DB: zbank_fraud, User: postgres, Pass: admin) *(local demo only, never reuse)*

## API Documentation

**Evaluate Transaction Velocity**

Evaluates a single transaction against the static rules and the 60-second Redis sliding window.

**Endpoint:** POST /api/v1/fraud/velocity-check

**Content-Type:** application/json

**Request Payload:**

_JSON
{
    "transactionId": "tx_uuid_string",
    "cardId": "card_554433",
    "accountId": "acc_112233",
    "amount": 150.00,
    "currency": "EUR",
    "merchantCategoryCode": "5411"
}_

**Success Response (200 OK):**

_JSON
{
    "transactionId": "tx_uuid_string",
    "isFraudulent": false,
    "statusReason": "APPROVED"
}_

**Decline Response (200 OK - Triggers Async Kafka Event):**

_JSON
{
    "transactionId": "tx_uuid_string",
    "isFraudulent": true,
    "statusReason": "DECLINED_VELOCITY_LIMIT_EXCEEDED"
}_

**Validation Error Response (400 Bad Request):**

_JSON
{
    "status": 400,
    "message": "Validation Failed",
    "details": {
        "amount": "Transaction amount must be strictly positive"
    },
    "timestamp": "2026-06-05T12:00:00.000Z"
}_
