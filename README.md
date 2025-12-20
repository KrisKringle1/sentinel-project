# Sentinel Project

A multi-module monorepo for the Sentinel platform, containing microservices for data ingestion and processing.

## Project Structure

```
sentinel-project/
├── .git/                        # Single Git repository for the entire project
├── docker-compose.yml           # Orchestrates Kafka + All Services
├── pom.xml                      # Parent POM managing all modules
├── .env                         # Environment variables
├── .gitignore                   # Root-level gitignore
│
├── sentinel-ingestion-service/  # Data ingestion microservice
│   ├── pom.xml
│   └── src/
│       ├── main/
│       └── test/
│
├── sentinel-consumer-service/   # Data consumer microservice
│   ├── pom.xml
│   └── src/
│       ├── main/
│       └── test/
│
└── sentinel-common/             # Shared DTOs, models, and utilities
    ├── pom.xml
    └── src/
        ├── main/
        └── test/
```

## Modules

### sentinel-ingestion-service
Microservice responsible for ingesting data and publishing to Kafka.
- **Port**: 8080
- **Main Class**: `com.sentinel.ingestion.SentinelIngestionApplication`

### sentinel-consumer-service
Microservice responsible for consuming data from Kafka and processing it.
- **Port**: 8081
- **Main Class**: `com.sentinel.consumer.SentinelConsumerApplication`

### sentinel-common
Shared library module containing:
- Common DTOs and data models
- Protobuf definitions
- Utility classes
- Shared validators

## Getting Started

### Prerequisites
- Java 17 or higher
- Maven 3.6+
- Docker and Docker Compose

### Setup

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd sentinel-project
   ```

2. Configure environment variables:
   ```bash
   cp .env.example .env
   # Generate a Kafka cluster ID
   docker run --rm confluentinc/cp-kafka:7.5.0 kafka-storage random-uuid
   # Add the generated ID to .env file
   ```

3. Build all modules:
   ```bash
   mvn clean install
   ```

### Running the Services

#### Option 1: Using Docker Compose
```bash
docker-compose up -d
```

This will start:
- Kafka (KRaft mode) on port 9092
- Sentinel Ingestion Service on port 8080
- Sentinel Consumer Service on port 8081

#### Option 2: Running Locally

1. Start Kafka only:
   ```bash
   docker-compose up kafka -d
   ```

2. Run services individually:
   ```bash
   # Terminal 1 - Ingestion Service
   cd sentinel-ingestion-service
   mvn spring-boot:run

   # Terminal 2 - Consumer Service
   cd sentinel-consumer-service
   mvn spring-boot:run
   ```

## Development

### Building a Specific Module
```bash
mvn clean install -pl sentinel-common
mvn clean install -pl sentinel-ingestion-service
mvn clean install -pl sentinel-consumer-service
```

### Running Tests
```bash
# All modules
mvn test

# Specific module
mvn test -pl sentinel-ingestion-service
```

### Code Formatting
The project uses Spotless with Google Java Format:
```bash
# Check formatting
mvn spotless:check

# Apply formatting
mvn spotless:apply
```

## Technology Stack

- **Java**: 17
- **Spring Boot**: 3.2.1
- **Apache Kafka**: 7.5.0 (KRaft mode)
- **Protocol Buffers**: 3.25.1
- **Maven**: Multi-module project
- **Lombok**: For reducing boilerplate
- **Docker**: Container orchestration

## Contributing

1. Make changes in your feature branch
2. Ensure all tests pass: `mvn test`
3. Ensure code is formatted: `mvn spotless:apply`
4. Build the project: `mvn clean install`
5. Submit a pull request

## License

[Add your license here]
