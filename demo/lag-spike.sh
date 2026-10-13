#!/bin/bash
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

BURST_DURATION=${1:-30}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo -e "${YELLOW}=== LAG SPIKE DEMO ===${NC}"
echo ""
echo "This demo floods the pipeline with 1000 TPS traffic (10x normal)"
echo "to create visible consumer lag, then stops and watches recovery."
echo ""
echo -e "Open Grafana (${GREEN}http://localhost:3000${NC}) to the Kafka Overview dashboard."
echo "Watch the 'Consumer Group Lag' and 'Messages In Per Second' panels."
echo ""
read -p "Press Enter to start the ${BURST_DURATION}s burst..."

echo -e "\n${YELLOW}Phase 1: Starting ${BURST_DURATION}s traffic burst at 1000 TPS...${NC}"
echo "(Normal rate is 100 TPS)"

# Run the generator locally from host, pointing at localhost Kafka/SR.
# Uses timeout to auto-stop after BURST_DURATION seconds.
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
SCHEMA_REGISTRY_URL=http://localhost:8081 \
TRANSACTIONS_PER_SECOND=1000 \
FRAUD_RATE=0.02 \
NUM_ACCOUNTS=100 \
NUM_MERCHANTS=20 \
timeout "${BURST_DURATION}" "${PROJECT_DIR}/.venv/bin/python3" -m generator.src.transaction_generator 2>&1 &
BURST_PID=$!

echo "Burst generator running (PID: ${BURST_PID}). Watch Grafana for lag increase..."

# Wait for the burst to finish (timeout will kill it)
wait "$BURST_PID" 2>/dev/null || true

echo -e "\n${YELLOW}Phase 2: Burst complete.${NC}"
echo -e "${GREEN}Normal generator still running at 100 TPS.${NC}"
echo ""
echo "Watch Grafana - consumer lag should now be draining."
echo "The pipeline will catch up over the next 30-60 seconds."
echo ""
echo -e "${GREEN}Demo complete.${NC}"