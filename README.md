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
- Python 3.8+ (for pre-commit hooks, optional but recommended)

## Getting Started

### 0. Set Up Pre-Commit Hooks (Optional but Recommended)

Pre-commit hooks help maintain code quality by running automated checks before each commit.

Install pre-commit:

```bash
pip install pre-commit
```

Install the git hooks:

```bash
pre-commit install
```

The hooks will now run automatically on each commit. To run them manually on all files:

```bash
pre-commit run --all-files
```

Configured checks include:
- File formatting (trailing whitespace, end-of-file newlines)
- Dockerfile linting (hadolint)
- Secrets detection
- Markdown linting
- Maven Spotless formatting
- Maven compilation and tests
- Docker Compose validation

### 1. Environment Configuration

Before running the stack, you must configure the Kafka Cluster ID. Create a `.env` file in the root directory:

```bash
touch .env
```

Generate a Kafka cluster ID:

```bash
docker run --rm confluentinc/cp-kafka:7.5.0 kafka-storage random-uuid
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

### Building the Project

To build the entire multi-module project locally without Docker:

```bash
mvn clean package
```

This will:
- Compile all three modules ([sentinel-common](sentinel-common/), [sentinel-ingestion-service](sentinel-ingestion-service/), [sentinel-consumer-service](sentinel-consumer-service/))
- Generate Protobuf classes
- Run code formatting checks (Spotless)
- Execute tests
- Package the applications as executable JARs

### Running Services Individually

To run services locally for development, follow these steps:

#### Step 1: Start Kafka via Docker Compose

First, ensure you have Kafka running. The easiest way is to use the Docker Compose Kafka instance:

```bash
docker compose up kafka -d
```

Verify Kafka is running:

```bash
docker compose ps kafka
```

#### Step 2: Configure Application Settings

The services use [application.yml](sentinel-consumer-service/src/main/resources/application.yml) files for configuration. The Kafka bootstrap server is already configured with a sensible default:

```yaml
spring:
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
```

By default, services will connect to `localhost:9092`. If you need to override this:

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

#### Step 3: Run Individual Services

Run the **Ingestion Service** (port 8080):

```bash
mvn spring-boot:run -pl sentinel-ingestion-service
```

Run the **Consumer Service** (port 8081):

```bash
mvn spring-boot:run -pl sentinel-consumer-service
```

Alternatively, you can run the packaged JARs directly:

```bash
# After running mvn clean package
java -jar sentinel-ingestion-service/target/sentinel-ingestion-service-1.0.0-SNAPSHOT.jar
java -jar sentinel-consumer-service/target/sentinel-consumer-service-1.0.0-SNAPSHOT.jar
```

#### Step 4: Verify Services are Running

Check that the services are responding:

```bash
# Ingestion Service health check
curl http://localhost:8080/actuator/health

# Consumer Service health check
curl http://localhost:8081/actuator/health
```

### Development Tips

- **Hot Reload**: Both services include Spring Boot DevTools for automatic restart during development.
- **Logs**: Each service logs to the console. Consumer service uses DEBUG level logging for `com.sentinel.consumer` package.
- **Shared Module**: Changes to [sentinel-common](sentinel-common/) require rebuilding dependent modules:
  ```bash
  mvn clean install -pl sentinel-common
  ```

## Security

The Docker images are configured to run as a non-root user (`sentinel`) for enhanced security.
