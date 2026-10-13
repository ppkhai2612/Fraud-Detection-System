#!/bin/bash
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

COMPOSE="docker compose -f docker-compose.yml"
PYTHON=".venv/bin/python3"

echo -e "${YELLOW}=== CHECKPOINT RECOVERY DEMO ===${NC}"
echo ""
echo "This demo kills the Flink TaskManager, causing a job failure."
echo "The JobManager detects the loss, restarts the TaskManager,"
echo "and the job resumes from the last checkpoint with state intact."
echo ""
echo -e "Open the Flink Web UI (${GREEN}http://localhost:8082${NC}) to watch job status."
echo -e "Open Grafana (${GREEN}http://localhost:3000${NC}) Flink Jobs dashboard."
echo ""

# Show current job status
echo "Current job status:"
curl -s http://localhost:8082/jobs/overview | ${PYTHON} -c "
import json, sys
data = json.load(sys.stdin)
for job in data.get('jobs', []):
    print(f\"  {job['name']}: {job['state']}\")
" 2>/dev/null || echo "  (could not reach Flink API)"

# Show checkpoint info
JOB_ID=$(curl -s http://localhost:8082/jobs/overview | ${PYTHON} -c "
import json, sys
data = json.load(sys.stdin)
for job in data.get('jobs', []):
    if job['state'] == 'RUNNING':
        print(job['jid'])
        break
" 2>/dev/null || true)

if [ -n "$JOB_ID" ]; then
    echo ""
    echo "Checkpoint info:"
    curl -s "http://localhost:8082/jobs/${JOB_ID}/checkpoints" | ${PYTHON} -c "
import json, sys
data = json.load(sys.stdin)
counts = data.get('counts', {})
print(f\"  Completed: {counts.get('completed', 0)}\")
print(f\"  Failed: {counts.get('failed', 0)}\")
latest = data.get('latest', {}).get('completed', {})
if latest:
    print(f\"  Latest checkpoint ID: {latest.get('id', 'N/A')}\")
    size_kb = latest.get('state_size', 0) / 1024
    print(f\"  State size: {size_kb:.1f} KB\")
" 2>/dev/null || echo "  (no checkpoint data available)"
fi

echo ""
read -p "Press Enter to kill the TaskManager..."

# Kill TaskManager
echo -e "\n${YELLOW}Phase 1: Killing TaskManager container...${NC}"
docker kill flink-taskmanager

echo -e "${RED}TaskManager is DOWN. Job will enter RESTARTING state.${NC}"
echo ""
echo "Check Flink UI - job should show RESTARTING."
echo "Waiting 5s for failure detection..."
sleep 5

# Show job status
echo -e "\n${YELLOW}Job status after TaskManager kill:${NC}"
curl -s http://localhost:8082/jobs/overview | ${PYTHON} -c "
import json, sys
data = json.load(sys.stdin)
for job in data.get('jobs', []):
    print(f\"  {job['name']}: {job['state']}\")
" 2>/dev/null || echo "  (could not reach Flink API)"

# Restart TaskManager
echo -e "\n${YELLOW}Phase 2: Restarting TaskManager...${NC}"
${COMPOSE} up -d flink-taskmanager

echo "Waiting for TaskManager to register with JobManager..."
for i in $(seq 1 30); do
    TM_COUNT=$(curl -s http://localhost:8082/taskmanagers 2>/dev/null | ${PYTHON} -c "
import json, sys
data = json.load(sys.stdin)
print(len(data.get('taskmanagers', [])))
" 2>/dev/null || echo "0")
    if [ "$TM_COUNT" -gt "0" ]; then
        echo -e "${GREEN}TaskManager registered after ${i}s.${NC}"
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo -e "${RED}TaskManager did not register in 30s.${NC}"
    fi
    sleep 1
done

# Wait for job to restart from checkpoint
echo ""
echo "Waiting for job to restart from checkpoint..."
sleep 15

echo -e "\n${YELLOW}Job status after recovery:${NC}"
curl -s http://localhost:8082/jobs/overview | ${PYTHON} -c "
import json, sys
data = json.load(sys.stdin)
for job in data.get('jobs', []):
    print(f\"  {job['name']}: {job['state']}\")
" 2>/dev/null || echo "  (could not reach Flink API)"

# Show recovery checkpoint info
NEW_JOB_ID=$(curl -s http://localhost:8082/jobs/overview | ${PYTHON} -c "
import json, sys
data = json.load(sys.stdin)
for job in data.get('jobs', []):
    if job['state'] == 'RUNNING':
        print(job['jid'])
        break
" 2>/dev/null || true)

if [ -n "$NEW_JOB_ID" ]; then
    echo ""
    echo "Post-recovery checkpoint info:"
    curl -s "http://localhost:8082/jobs/${NEW_JOB_ID}/checkpoints" | ${PYTHON} -c "
import json, sys
data = json.load(sys.stdin)
latest = data.get('latest', {}).get('restored', {})
if latest:
    print(f\"  Restored from checkpoint ID: {latest.get('id', 'N/A')}\")
    print('  State was restored successfully!')
else:
    print('  (no restore info - job may have restarted fresh)')
" 2>/dev/null || echo "  (could not get checkpoint data)"
fi

echo ""
echo "Check Flink UI for:"
echo "  - Job showing RUNNING with restored checkpoint"
echo "  - Checkpoint counter continuing"
echo "  - No data loss (Kafka consumer offsets restored)"
echo ""
echo -e "${GREEN}Demo complete.${NC}"