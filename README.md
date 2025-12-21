# Sentinel Project

Sentinel is a multi-module microservices platform designed for high-throughput data ingestion and processing. It utilizes **Spring Boot 3** and **Apache Kafka** (running in KRaft mode without ZooKeeper) to decouple ingestion from processing.

## Project Structure

The project is organized as a Maven multi-module monorepo:

- **`sentinel-ingestion-service`**: A RESTful service responsible for accepting data via HTTP and publishing events to Kafka.
- **`sentinel-consumer-service`**: A background service that consumes events from Kafka for downstream processing.
- **`sentinel-common`**: A shared library containing common DTOs, Protobuf definitions, and utility logic used across services.

## Technology Stack

- **Java**: 17 (Eclipse Temurin)
- **Framework**: Spring Boot 3.2.1
- **Messaging**: Apache Kafka (KRaft mode)
- **Serialization**: Google Protocol Buffers (Protobuf)
- **Build Tool**: Maven
- **Containerization**: Docker & Docker Compose

## Prerequisites

- Docker Desktop
- Java 17 SDK (for local development)
- Maven (optional, if using the wrapper)

## Getting Started

### 1. Environment Configuration

Before running the stack, you must configure the Kafka Cluster ID. Create a `.env` file in the root directory:

```bash
touch .env
```

Add the following content to `.env`:

```dotenv
KAFKA_CLUSTER_ID=<your-generated-id>
```

### 2. Run with Docker Compose

Build and start the entire stack (Kafka, Ingestion, and Consumer services):

```bash
docker compose up -d --build
```

This command will:

1.  Compile the Maven project (leveraging Docker layer caching).
2.  Build the Docker images for the services.
3.  Start a single-node Kafka cluster in KRaft mode.
4.  Start the Ingestion and Consumer services.

### 3. Verify Services

Check the status of the containers:

```bash
docker compose ps
```

View logs to ensure everything started correctly:

```bash
docker compose logs -f
```

## Service Endpoints

| Service               | Port   | Description                           |
| :-------------------- | :----- | :------------------------------------ |
| **Ingestion Service** | `8080` | Accepts HTTP requests to ingest data. |
| **Consumer Service**  | `8081` | Processes Kafka messages.             |
| **Kafka Broker**      | `9092` | External access to the Kafka cluster. |

## Local Development

To build the project locally without Docker:

```bash
mvn clean package
```

To run individual services locally, ensure you have a Kafka instance running (or use the one from Docker Compose) and configure `application.yml` to point to `localhost:9092`.

## Security

The Docker images are configured to run as a non-root user (`sentinel`) for enhanced security.
