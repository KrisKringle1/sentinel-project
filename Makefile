.PHONY: help env start up stop down restart logs status ps infra \
        dev-ingestion dev-consumer dev-dashboard \
        build test verify format clean clean-docker

DEFAULT_KAFKA_CLUSTER_ID := MkU3OEVBNTcwNTJENDM2Qk

##@ Quick Start (Full Stack in Docker)

help: ## Show this help menu
	@awk 'BEGIN {FS = ":.*##"; printf "\n\033[1mSentinel Platform — Makefile Commands\033[0m\n"} \
		/^[a-zA-Z0-9_-]+:.*?##/ { printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2 } \
		/^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)
	@echo ""

env: ## Ensure .env exists with a valid KAFKA_CLUSTER_ID
	@if [ ! -f .env ]; then \
		echo "KAFKA_CLUSTER_ID=$(DEFAULT_KAFKA_CLUSTER_ID)" > .env; \
		echo "Created .env with KAFKA_CLUSTER_ID=$(DEFAULT_KAFKA_CLUSTER_ID)"; \
	elif grep -q "^KAFKA_CLUSTER_ID=.\+" .env; then \
		echo ".env already configured."; \
	elif grep -q "^KAFKA_CLUSTER_ID=" .env; then \
		sed -i 's/^KAFKA_CLUSTER_ID=.*/KAFKA_CLUSTER_ID=$(DEFAULT_KAFKA_CLUSTER_ID)/' .env; \
		echo "Updated empty KAFKA_CLUSTER_ID in .env"; \
	else \
		echo "KAFKA_CLUSTER_ID=$(DEFAULT_KAFKA_CLUSTER_ID)" >> .env; \
		echo "Appended KAFKA_CLUSTER_ID=$(DEFAULT_KAFKA_CLUSTER_ID) to .env"; \
	fi

start: up ## Alias for 'make up' — build and start the entire platform

up: env ## Build and start all services (Kafka, Schema Registry, Redis, Ingestion, Consumer, Dashboard)
	docker compose up -d --build
	@echo ""
	@echo "=================================================================="
	@echo "  Sentinel Full-Stack Platform is starting!"
	@echo "  - Dashboard UI:      http://localhost:3000"
	@echo "  - Ingestion API:     http://localhost:8080/api/v1/metrics"
	@echo "  - Consumer API/SSE:  http://localhost:8081/api/v1/telemetry/services"
	@echo "  - Schema Registry:   http://localhost:8085"
	@echo "  Run 'make logs' to follow container output or 'make status' to check health."
	@echo "=================================================================="

stop: down ## Alias for 'make down'

down: ## Stop and remove running Docker Compose containers
	docker compose down

restart: down up ## Restart the entire Docker Compose stack

logs: ## Tail logs from all running containers
	docker compose logs -f

status: ps ## Alias for 'make ps'

ps: ## Show status and health of all containers
	docker compose ps

clean-docker: ## Stop containers and remove persistent volumes (wipes Kafka & Redis state)
	docker compose down -v

##@ Local Hybrid Development (Docker Infra + Local Services)

infra: env ## Start only Kafka, Schema Registry, and Redis in Docker for local dev
	docker compose up -d kafka schema-registry redis

dev-ingestion: ## Run sentinel-ingestion-service locally on :8080
	./mvnw spring-boot:run -pl sentinel-ingestion-service

dev-consumer: ## Run sentinel-consumer-service locally on :8081
	./mvnw spring-boot:run -pl sentinel-consumer-service

dev-dashboard: ## Run sentinel-dashboard locally on :3000 with hot reload
	npm --prefix sentinel-dashboard run dev

##@ Build, Test & Code Quality

build: ## Build backend JARs and Next.js production bundle
	./mvnw clean package -DskipTests
	npm --prefix sentinel-dashboard run build

test: verify ## Run all backend unit tests and frontend typecheck/lint

verify: ## Run Maven verify (Spotless + JUnit) and Frontend typecheck + ESLint
	./mvnw clean verify
	npm --prefix sentinel-dashboard run typecheck
	npm --prefix sentinel-dashboard run lint

format: ## Auto-format Java source files with Spotless
	./mvnw spotless:apply

clean: ## Clean Maven target directories and Next.js build cache
	./mvnw clean
	rm -rf sentinel-dashboard/.next sentinel-dashboard/tsconfig.tsbuildinfo
