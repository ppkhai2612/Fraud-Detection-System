.PHONY: up down restart status logs clean \
		setup setup-python setup-topics setup-schemas \
		build build-flink \
		deploy-jobs cancel-jobs produce-test-transaction produce-test-data \
		start-generator stop-generator logs-generator \
		start-monitoring stop-monitoring \
		check-health wait-healthy \
		open-flink open-grafana open-prometheus help

COMPOSE := docker compose -f docker-compose.yml
PYTHON := .venv/bin/python3

# ============================================================
# Core Lifecycle
# ============================================================

up: # Start all services
	$(COMPOSE) up -d
	@echo
	@echo "Services starting. Run 'make wait-healthy' to wait for readiness."

down: # Stop all services (including generator and monitoring)
	$(COMPOSE) --profile generate --profile monitoring down

status: # Show service status
	$(COMPOSE) ps

logs: # Tail all service logs
	$(COMPOSE) logs -f

clean: # Stop services, remove volumes, clean build artifacts
	$(COMPOSE) --profile generate --profile monitoring down -v --remove-orphans
	cd flink-jobs && mvn clean -q

# ============================================================
# Setup
# ============================================================

setup: setup-python setup-topics setup-schemas

setup-python: # Install Python deps via uv
	uv sync

setup-topics: # Create Kafka topics
	@bash scripts/setup-topics.sh

setup-schemas: # Register Avro schemas in Schema Registry
	@bash scripts/register-schemas.sh

# ============================================================
# Build
# ============================================================

build: build-flink # Build all components

build-flink: # Build Flink jobs (mvn package)
	cd flink-jobs && mvn clean package -DskipTests -q
	@echo "Flink jobs built successfully."

# ============================================================
# Health Checks
# ============================================================

check-health: # Check service health
	@bash scripts/check-health.sh

wait-healthy: # Wait until all services are healthy
	@echo "Waiting for services to be healthy..."
	@for i in $$(seq 1 60); do \
		if bash scripts/check-health.sh > /dev/null 2>&1; then \
			echo "All services healthy."; \
			exit 0; \
		fi; \
		echo "  Attempt $$i/60 - waiting 5s..."; \
		sleep 5; \
	done; \
	echo "Timed out waiting for services."; \
	exit 1

# ============================================================
# Deploy
# ============================================================

deploy-jobs: build-flink # Build and deploy Flink jobs to local cluster
	docker exec flink-jobmanager mkdir -p /opt/flink/usrlib
	docker cp flink-jobs/pipeline/target/pipeline-1.0.0.jar flink-jobmanager:/opt/flink/usrlib/pipeline.jar
	docker exec flink-jobmanager flink run -d /opt/flink/usrlib/pipeline.jar
	@echo "Flink job submitted. Check http://localhost:8082 for status."

cancel-jobs: # Cancel all running Flink jobs
	@for job_id in $$(curl -s http://localhost:8082/jobs/overview | $(PYTHON) -c "import json,sys; [print(j['jid']) for j in json.load(sys.stdin)['jobs'] if j['state']=='RUNNING']" 2>/dev/null); do \
		echo "Cancelling job $$job_id"; \
		curl -s -X PATCH "http://localhost:8082/jobs/$$job_id?mode=cancel"; \
	done
	@echo "All jobs cancelled."

produce-test-transaction: # Produce a test transaction to Kafka
	@bash scripts/produce-test-transaction.sh

produce-test-data: # Produce test account, merchant, and transactions for enrichment
	@bash scripts/produce-test-data.sh

# ============================================================
# Generator
# ============================================================

start-generator: # Start transaction generator
	$(COMPOSE) --profile generate up -d generator
	@echo "Generator started. View logs: make logs-generator"

stop-generator: # Stop transaction generator
	$(COMPOSE) --profile generate stop generator
	@echo "Generator stopped."

logs-generator: # Tail generator logs
	docker logs -f generator

# ============================================================
# Monitoring
# ============================================================

start-monitoring: # Start Prometheus, Grafana, and kafka-exporter
	$(COMPOSE) --profile monitoring up -d prometheus grafana kafka-exporter
	@echo "Grafana:    http://localhost:3000 (admin/admin)"
	@echo "Prometheus: http://localhost:9090"

stop-monitoring: # Stop monitoring services
	$(COMPOSE) --profile monitoring stop prometheus grafana kafka-exporter

open-flink: # Open Flink Web UI
	@echo "Opening http://localhost:8082"
	@which xdg-open > /dev/null 2>&1 && xdg-open http://localhost:8082 || echo "Open http://localhost:8082 in your browser"

open-grafana: # Open Grafana in browser
	@echo "Opening http://localhost:3000"
	@which xdg-open > /dev/null 2>&1 && xdg-open http://localhost:3000 || echo "Open http://localhost:3000 in your browser"

open-prometheus: # Open Prometheus in browser
	@echo "Opening http://localhost:9090"
	@which xdg-open > /dev/null 2>&1 && xdg-open http://localhost:9090 || echo "Open http://localhost:9090 in your browser"

# ============================================================
# Demo Scenarios
# ============================================================


# ============================================================
# Utilities
# ============================================================

help: # Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help