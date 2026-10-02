#!/bin/bash
set -euo pipefail

echo "Checking service health..."
echo ""

check_service() {
	local name="$1"
	local url="$2"

	if curl -sf "$url" > /dev/null 2>&1; then
		echo "  $name: HEALTHY"
		return 0
	else
		echo "  $name: UNHEALTHY"
		return 1
  	fi
}

FAILED=0

# Kakfa (check via topic list)
if docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list > /dev/null 2>&1; then
  	echo "  Kafka: HEALTHY"
else
	echo "  Kafka: UNHEALTHY"
	FAILED=1
fi

check_service "Schema Registry" "http://localhost:8081/subjects" || FAILED=1
check_service "Flink JobManager" "http://localhost:8082/overview" || FAILED=1
check_service "Model Service" "http://localhost:8000/health" || FAILED=1

echo ""
if [ $FAILED -eq 0 ]; then
  	echo "All services healthy."
else
	echo "Some services are unhealthy. Check docker compose logs."
	exit 1
fi