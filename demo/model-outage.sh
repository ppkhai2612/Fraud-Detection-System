#!/bin/bash
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

COMPOSE="docker compose -f docker-compose.yml"
OUTAGE_DURATION=${1:-30}

echo -e "${YELLOW}=== MODEL OUTAGE DEMO ===${NC}"
echo ""
echo "This demo kills the ML model service and watches the pipeline"
echo "fall back to rule-only scoring. Then restarts and watches recovery."
echo ""
echo -e "Open Grafana (${GREEN}http://localhost:3000${NC}) to the Fraud Detection dashboard."
echo "Watch the 'Decision Breakdown' and 'Fraud Rate' panels."
echo ""
echo "Also watch the Flink TaskManager logs in another terminal:"
echo "  docker logs -f flink-taskmanager 2>&1 | grep -i model"
echo ""
read -p "Press Enter to kill the model service..."

# Verify model is currently healthy
echo -e "\n${YELLOW}Checking model service is healthy...${NC}"
if curl -sf http://localhost:8000/health > /dev/null 2>&1; then
    echo -e "${GREEN}Model service is UP. Proceeding.${NC}"
else
    echo -e "${RED}Model service is already down. Start it first: make up${NC}"
    exit 1
fi

# Kill model service
echo -e "\n${YELLOW}Phase 1: Killing model service...${NC}"
docker stop model-service

echo -e "${RED}Model service is DOWN.${NC}"
echo "The pipeline will now fall back to rule-only scoring."
echo "Check Flink logs for 'Model timeout' or 'Model call failed' warnings."
echo ""
echo "Waiting ${OUTAGE_DURATION}s for fallback behavior to accumulate..."

for i in $(seq 1 "$OUTAGE_DURATION"); do
    printf "\r  %d/%ds elapsed" "$i" "$OUTAGE_DURATION"
    sleep 1
done
echo ""

# Restart model service
echo -e "\n${YELLOW}Phase 2: Restarting model service...${NC}"
${COMPOSE} up -d model-service

echo "Waiting for model service to become healthy..."
for i in $(seq 1 30); do
    if curl -sf http://localhost:8000/health > /dev/null 2>&1; then
        echo -e "${GREEN}Model service is back UP after ${i}s.${NC}"
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo -e "${RED}Model service did not recover in 30s.${NC}"
    fi
    sleep 1
done

echo ""
echo "Model service recovered. Watch Grafana for:"
echo "  - Model scores returning to non-null values"
echo "  - Decision distribution shifting back to normal"
echo "  - Pipeline never crashed during the outage"
echo ""
echo -e "${GREEN}Demo complete.${NC}"