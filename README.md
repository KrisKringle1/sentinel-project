# Sentinel Project

**Sentinel is a real-time health monitor for a fleet of microservices.** Each service reports operational
metrics such as CPU usage, memory usage, latency, and error rate. Sentinel keeps a rolling statistical picture of
every service, raises an alert as soon as a metric crosses a threshold, and pushes live updates to a web
dashboard. You can see which service is degrading, and on which metric, within seconds.

It's built with **Spring Boot 3**, **Apache Kafka** (KRaft mode), **Apache Avro** + **Confluent Schema Registry**,
**Redis**, and a **Next.js** dashboard.

## What It Does

1. **Collects metrics over HTTP.** Any service, or the built-in traffic simulator, sends a small JSON payload
   (`serviceId`, `metricName`, `value`, `timestamp`) to the ingestion API.
2. **Buffers them durably in Kafka.** The ingestion service validates each payload, serializes it to Avro
   against a schema registered in Schema Registry, and publishes it to Kafka. Messages are keyed by `serviceId`,
   so each service's readings stay in order. If a publish fails, the event goes to a dead-letter topic
   (`service-metrics-dlq`) so it isn't lost.
3. **Aggregates per service.** The consumer service keeps a rolling window (by default the last 50 readings) for
   every service/metric pair. From that window it tracks the count, min, max, moving average, and latest value.
4. **Detects anomalies.** Each reading is checked against a configurable threshold. A breach produces an alert
   whose severity depends on how far over the threshold the value is. Each service also gets an overall status
   (`HEALTHY`, `WARNING`, or `CRITICAL`) based on its latest readings.
5. **Streams results live.** Every metric and alert is pushed to connected clients over Server-Sent Events (SSE).
   REST endpoints return the current fleet snapshot, recent alerts, and dead-lettered records.
6. **Survives restarts.** Service state, alerts, and DLQ records are saved to Redis. If Redis is unreachable,
   Sentinel switches to in-memory state and keeps running.

### Anomaly Rules

| Metric         | Default Threshold | `WARNING` when…        | `CRITICAL` when…                        |
| :------------- | :---------------- | :--------------------- | :-------------------------------------- |
| `cpu_usage`    | 90 %              | value > 90             | value ≥ 95 (or ≥ 15 % over threshold)   |
| `memory_usage` | 85 %              | value > 85             | value ≥ 95 (or ≥ 15 % over threshold)   |
| `latency_ms`   | 500 ms            | value > 500            | value ≥ 575 (15 % over threshold)       |
| `error_rate`   | 5 %               | value > 5              | value ≥ 5.75 (15 % over threshold)      |

You can change the thresholds and window size under `sentinel.consumer` in
`sentinel-consumer-service/src/main/resources/application.yml`.

### The Dashboard

The Sentinel Dashboard (`http://localhost:3000`) is the main way to watch the system:

- **Fleet health grid**: one card per service, showing its status and rolling stats for each metric.
- **Live telemetry chart**: metric values streaming in real time over SSE.
- **Anomaly alert feed**: alerts as they fire, color-coded by severity.
- **Traffic & anomaly simulator**: lets you try the platform without real services. It can:
  - Generate continuous traffic at 1, 2, or 5 metrics per second (about 12 % of readings spike above a threshold)
  - Inject a scenario with one click (CPU spike, latency surge, heap pressure, 5xx error burst)
  - Reset every service to healthy baseline values
  - Send a single custom metric

### Example: The Life of a Metric

Suppose `auth-service` reports `cpu_usage = 96.4`:

1. The ingestion service validates it and publishes an Avro record to the `service-metrics` topic.
2. The consumer service adds it to `auth-service`'s rolling CPU window and finds it is over the 90 % threshold.
3. It creates a `CRITICAL` alert, marks `auth-service` as `CRITICAL`, and saves both to Redis.
4. The dashboard receives the SSE event, turns the `auth-service` card red, and adds the alert to the feed.

## Architecture Overview

```text
[Microservices / Sentinel Dashboard Load Simulator (:3000)]
        │
        │ 1. POST /api/v1/metrics (JSON)
        ▼
┌─────────────────────────────────┐
│ sentinel-ingestion-service:8080 │
│  - Validates MetricRequest DTO  │
│  - Serializes to Apache Avro    │◄───► [Confluent Schema Registry :8085]
│  - Idempotent Kafka Producer    │
└───────────────┬─────────────────┘
                │
                │ 2. Kafka Topic: "service-metrics"
                │    (Fallback: "service-metrics-dlq")
                ▼
      ┌───────────────────┐
      │ Apache Kafka:9092 │ (KRaft mode — partitioned by serviceId)
      └─────────┬─────────┘
                │
                │ 3. @KafkaListener (Group: sentinel-metrics-consumer)
                ▼
┌─────────────────────────────────┐
│ sentinel-consumer-service:8081  │
│  - Deserializes Avro records    │
│  - Rolling window aggregations  │◄───► [Redis :6379] (with in-memory fallback)
│  - Threshold anomaly detection  │
└───────┬───────────────┬─────────┘
        │               │
        │ 4a. REST API  │ 4b. Server-Sent Events (SSE)
        ▼               ▼
┌─────────────────────────────────┐
│ sentinel-dashboard:3000         │
│  - Next.js 14 + Mantine v7 UI   │
│  - TanStack React Query + SSE   │
│  - Fleet Health & Alert Feed    │
│  - Chaos & Traffic Simulator    │
└─────────────────────────────────┘
```

## Project Structure

The project is organized as a Maven multi-module monorepo:

```text
sentinel-project/
├── sentinel-ingestion-service/     # HTTP ingestion & Kafka Avro producer (Port 8080)
│   ├── src/main/java/com/sentinel/ingestion/
│   │   ├── SentinelIngestionServiceApplication.java
│   │   ├── controller/
│   │   │   └── MetricsController.java          # POST /api/v1/metrics
│   │   ├── service/
│   │   │   └── MetricProducerService.java      # Avro producer + DLQ fallback
│   │   ├── dto/
│   │   │   ├── MetricRequest.java              # Validated request DTO
│   │   │   └── ErrorResponse.java              # Standardized error model
│   │   └── exception/
│   │       ├── GlobalExceptionHandler.java     # Centralized error handling
│   │       └── MetricProcessingException.java
│   ├── src/main/resources/
│   │   └── application.yml                     # Producer & Schema Registry config
│   ├── Dockerfile
│   └── pom.xml
│
├── sentinel-consumer-service/      # Kafka Avro consumer, anomaly engine & SSE API (Port 8081)
│   ├── src/main/java/com/sentinel/consumer/
│   │   ├── SentinelConsumerApplication.java
│   │   ├── config/
│   │   │   └── SentinelConsumerProperties.java # Window size & anomaly thresholds
│   │   ├── controller/
│   │   │   └── TelemetryController.java        # REST & SSE endpoints (/api/v1/telemetry/*)
│   │   ├── service/
│   │   │   ├── MetricConsumerService.java      # @KafkaListener for metrics & DLQ
│   │   │   ├── TelemetryStateService.java      # Rolling aggregations, alerts & Redis state
│   │   │   └── SseEmitterService.java          # Live Server-Sent Events broadcaster
│   │   └── dto/
│   │       ├── MetricSummary.java
│   │       ├── ServiceTelemetrySummary.java
│   │       ├── AnomalyAlert.java
│   │       ├── DlqMetricRecord.java
│   │       └── TelemetryStreamEvent.java
│   ├── src/main/resources/
│   │   └── application.yml                     # Consumer, Avro, Redis & threshold config
│   ├── Dockerfile
│   └── pom.xml
│
├── sentinel-common/                # Shared Avro schemas & generated classes
│   ├── src/main/avro/
│   │   └── sentinel.avsc                       # SentinelMetric Avro schema
│   └── pom.xml
│
├── sentinel-dashboard/             # Next.js 14 + TypeScript + Mantine v7 real-time UI (Port 3000)
│   ├── src/
│   │   ├── app/                                # Next.js App Router layout & dashboard page
│   │   ├── components/                         # Live charts, fleet grid, alert feed & traffic simulator
│   │   ├── hooks/
│   │   │   └── useTelemetryStream.ts           # SSE EventSource & REST state hydration hook
│   │   ├── lib/
│   │   │   └── api.ts                          # Typed client for Ingestion (:8080) & Consumer (:8081)
│   │   └── types/
│   │       └── telemetry.ts                    # TypeScript interfaces matching backend DTOs
│   ├── Dockerfile
│   └── package.json
│
├── .github/workflows/ci.yml        # GitHub Actions CI pipeline (Maven + Next.js build)
├── Makefile                        # One-command full-stack orchestration & dev targets
├── docker-compose.yml              # Multi-service orchestration
├── .pre-commit-config.yaml         # Code quality hooks
├── .env.example                    # Environment configuration template
├── mvnw / mvnw.cmd                 # Maven wrapper scripts
└── pom.xml                         # Parent POM with module & plugin management
```

### Module Overview

- **`sentinel-common`**: Shared library containing the Apache Avro schema (`SentinelMetric`) compiled into Java classes
  during the Maven build.
- **`sentinel-ingestion-service`**: RESTful ingestion microservice that validates incoming metric payloads and publishes
  strongly-typed Avro events to Kafka with producer idempotence (`acks=all`, `enable.idempotence=true`) and automatic
  Dead-Letter Queue (`service-metrics-dlq`) routing.
- **`sentinel-consumer-service`**: Downstream analytics microservice that consumes Avro events from Kafka, computes
  per-service rolling window statistics (`count`, `min`, `max`, `movingAverage`), evaluates anomaly thresholds, persists
  state to Redis (with automatic in-memory fallback), and exposes REST and real-time SSE endpoints.

## Technology Stack

- **Java**: 17 (Eclipse Temurin)
- **Framework**: Spring Boot 3.2.1 (Web, Kafka, Data Redis, Validation, Actuator)
- **Messaging**: Apache Kafka 7.5.0 (KRaft mode)
- **Serialization & Schema Management**: Apache Avro 1.11.3 & Confluent Schema Registry 7.5.0
- **State Store**: Redis (with resilient in-memory fallback)
- **Build & Code Quality**: Maven, Spotless (Google Java Format), JUnit 5, Mockito, Pre-commit
- **Containerization**: Docker & Docker Compose

## Service Ports & API Reference

| Service                     | Port   | Description                                                    |
| :-------------------------- | :----- | :------------------------------------------------------------- |
| **Sentinel Dashboard**      | `3000` | Next.js real-time SSE telemetry UI & traffic/chaos simulator.  |
| **Ingestion Service**       | `8080` | Accepts HTTP `POST /api/v1/metrics` requests.                  |
| **Consumer Service**        | `8081` | Exposes `/api/v1/telemetry/*` REST and SSE endpoints.          |
| **Schema Registry**         | `8085` | Confluent Schema Registry for Apache Avro schemas.             |
| **Kafka Broker**            | `9092` | External listener for the KRaft Kafka broker.                  |
| **Redis**                   | `6379` | Durable state store for service summaries, alerts, and DLQ.    |

### Key Endpoints

#### 1. Ingest a Metric (`POST http://localhost:8080/api/v1/metrics`)

```bash
curl -X POST http://localhost:8080/api/v1/metrics \
  -H "Content-Type: application/json" \
  -d '{
    "serviceId": "auth-service",
    "metricName": "cpu_usage",
    "value": 94.5,
    "unit": "percent",
    "timestamp": 1700000000000
  }'
```

#### 2. Query Service Health & Rolling Aggregations (`GET http://localhost:8081/api/v1/telemetry/services`)

```bash
curl http://localhost:8081/api/v1/telemetry/services
```

#### 3. Query Recent Anomaly Alerts (`GET http://localhost:8081/api/v1/telemetry/alerts`)

```bash
curl "http://localhost:8081/api/v1/telemetry/alerts?serviceId=auth-service&limit=20"
```

#### 4. Subscribe to Real-Time Telemetry SSE Stream (`GET http://localhost:8081/api/v1/telemetry/stream`)

```bash
curl -N http://localhost:8081/api/v1/telemetry/stream
```

## Prerequisites

- Docker Desktop
- Java 17+ SDK (for local development)
- Maven (optional, if using `./mvnw`)
- Python 3.8+ (for pre-commit hooks, optional but recommended)

## Getting Started

### Quick Start (One Command via Makefile)

To automatically configure `.env` (with a valid KRaft `KAFKA_CLUSTER_ID`) and build + launch the entire 6-container platform (Kafka, Schema Registry, Redis, Ingestion, Consumer, and the Next.js Dashboard):

```bash
make start
```

View all available `Makefile` commands at any time:

```bash
make help
```

### 0. Set Up Pre-Commit Hooks (Optional but Recommended)

Install pre-commit and register git hooks:

```bash
pip install pre-commit
pre-commit install
```

To run all configured checks manually:

```bash
pre-commit run --all-files
```

### 1. Environment Configuration

Create a `.env` file from the template:

```bash
cp .env.example .env
```

Generate a Kafka cluster ID:

```bash
docker run --rm confluentinc/cp-kafka:7.5.0 kafka-storage random-uuid
```

Add the generated ID to `.env`:

```dotenv
KAFKA_CLUSTER_ID=<your-generated-id>
```

### 2. Run with Docker Compose

Build and start the stack (Kafka, Schema Registry, Ingestion, and Consumer services):

```bash
docker compose up -d --build
```

Check container status and logs:

```bash
docker compose ps
docker compose logs -f
```

## Local Development & Testing

### Building and Running Tests

To compile all modules, generate Avro sources, verify Spotless formatting, and run the unit test suite:

```bash
./mvnw clean verify
```

### Running Services Locally

Start Kafka, Schema Registry, and Redis via Docker Compose:

```bash
docker compose up kafka schema-registry redis -d
```

Run the **Ingestion Service** (port 8080):

```bash
./mvnw spring-boot:run -pl sentinel-ingestion-service
```

Run the **Consumer Service** (port 8081):

```bash
./mvnw spring-boot:run -pl sentinel-consumer-service
```

Run the **Sentinel Dashboard** (port 3000):

```bash
cd sentinel-dashboard
npm install
npm run dev
```

Verify actuator health checks:

```bash
curl http://localhost:8080/actuator/health
curl http://localhost:8081/actuator/health
```

## Security

The Docker images are configured to run as a non-root user (`sentinel`) for enhanced container security.
