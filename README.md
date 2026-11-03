# Sentinel Project

Sentinel is a distributed microservices telemetry and real-time anomaly detection platform.
It utilizes **Spring Boot 3**, **Apache Kafka** (KRaft mode without ZooKeeper), **Apache Avro** with
**Confluent Schema Registry**, and **Redis** (with automatic in-memory fallback) to decouple high-throughput
metric ingestion from rolling-window aggregation, threshold alerting, and real-time Server-Sent Events (SSE)
streaming.

## Architecture Overview

```text
[Microservices / Load Simulator]
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
   GET /services    GET /stream (Real-time push stream)
   GET /alerts
   GET /dlq
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
├── .github/workflows/ci.yml        # GitHub Actions CI pipeline (./mvnw clean verify)
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

Verify actuator health checks:

```bash
curl http://localhost:8080/actuator/health
curl http://localhost:8081/actuator/health
```

## Security

The Docker images are configured to run as a non-root user (`sentinel`) for enhanced container security.
