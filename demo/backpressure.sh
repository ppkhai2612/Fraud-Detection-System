#!/bin/bash
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

THROTTLE_DURATION=${1:-30}

echo -e "${YELLOW}=== BACKPRESSURE DEMO ===${NC}"
echo ""
echo "This demo throttles the Flink TaskManager's CPU to 10%,"
echo "creating backpressure visible in the Flink Web UI and Grafana."
echo ""
echo -e "Open the Flink Web UI (${GREEN}http://localhost:8082${NC})."
echo "Navigate to the running job and watch the task backpressure indicators."
echo -e "Open Grafana (${GREEN}http://localhost:3000${NC}) Flink Jobs dashboard."
echo "Watch the 'Backpressure' panel."
echo ""
read -p "Press Enter to start throttling..."

echo -e "\n${YELLOW}Phase 1: Throttling TaskManager CPU to 10%...${NC}"
docker update --cpus 0.1 flink-taskmanager

echo -e "${RED}TaskManager throttled to 10% CPU.${NC}"
echo "Watch for:"
echo "  - Backpressure indicators turning red in the Flink UI"
echo "  - Consumer lag increasing on the Kafka Overview dashboard"
echo "  - Throughput dropping on the Flink Jobs dashboard"
echo ""
echo "Waiting ${THROTTLE_DURATION}s..."

for i in $(seq 1 "$THROTTLE_DURATION"); do
    printf "\r  %d/%ds elapsed" "$i" "$THROTTLE_DURATION"
    sleep 1
done
echo ""

echo -e "\n${YELLOW}Phase 2: Removing CPU throttle...${NC}"
docker update --cpus 0 flink-taskmanager

echo -e "${GREEN}CPU throttle removed.${NC}"
echo "Backpressure should clear and pipeline should catch up."
echo "Watch the lag drain on the Kafka Overview dashboard."
echo ""
echo -e "${GREEN}Demo complete.${NC}"