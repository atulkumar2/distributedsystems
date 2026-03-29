#!/usr/bin/env bash
set -euo pipefail

# ── Colours ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'

info()    { echo -e "${CYAN}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TOPIC="vehicle-telemetry"
PARTITIONS=3
BROKER="localhost:9092"

# ── 1. Preflight checks ───────────────────────────────────────────────────────
command -v docker  > /dev/null 2>&1 || error "Docker is not installed or not on PATH."
command -v mvn     > /dev/null 2>&1 || error "Maven (mvn) is not installed or not on PATH."
docker info        > /dev/null 2>&1 || error "Docker daemon is not running."

# ── 2. Start Docker Compose stack ─────────────────────────────────────────────
info "Starting Kafka + Kafka UI via Docker Compose..."
docker compose -f "$SCRIPT_DIR/docker-compose.yml" up -d

# ── 3. Wait for Kafka broker to accept connections ────────────────────────────
info "Waiting for Kafka broker to be ready..."
RETRIES=30
until docker exec kafka-local \
        /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server "$BROKER" \
        > /dev/null 2>&1; do
  RETRIES=$((RETRIES - 1))
  [[ $RETRIES -le 0 ]] && error "Kafka broker did not become ready in time."
  sleep 1
done
success "Kafka broker is ready at $BROKER"

# ── 4. Create topic (idempotent — skips if it already exists) ─────────────────
info "Ensuring topic '$TOPIC' exists ($PARTITIONS partitions)..."
EXISTING=$(docker exec kafka-local \
  /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server "$BROKER" 2>/dev/null \
  | grep -x "$TOPIC" || true)

if [[ -z "$EXISTING" ]]; then
  docker exec kafka-local \
    /opt/kafka/bin/kafka-topics.sh \
    --create \
    --topic "$TOPIC" \
    --bootstrap-server "$BROKER" \
    --partitions "$PARTITIONS" \
    --replication-factor 1
  success "Topic '$TOPIC' created."
else
  warn "Topic '$TOPIC' already exists — skipping creation."
fi

# ── 5. Compile Java project ───────────────────────────────────────────────────
info "Compiling Java project..."
mvn -q -f "$SCRIPT_DIR/pom.xml" clean compile
success "Project compiled."

# ── 6. Ready summary ──────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}  Everything is ready.${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo -e "  ${CYAN}Kafka UI${NC}     →  http://localhost:8080"
echo -e "  ${CYAN}Kafka broker${NC} →  $BROKER"
echo -e "  ${CYAN}Topic${NC}        →  $TOPIC ($PARTITIONS partitions)"
echo ""
echo -e "  Run producer:"
echo -e "    ${YELLOW}mvn exec:java -Dexec.mainClass=com.example.telematics.producer.KafkaProducerExample${NC}"
echo ""
echo -e "  Run consumer:"
echo -e "    ${YELLOW}mvn exec:java -Dexec.mainClass=com.example.telematics.consumer.KafkaConsumerExample${NC}"
echo ""
echo -e "  Run manual-commit consumer:"
echo -e "    ${YELLOW}mvn exec:java -Dexec.mainClass=com.example.telematics.consumer.KafkaManualCommitConsumerExample${NC}"
echo ""
echo -e "  Stop everything:  ${YELLOW}docker compose down${NC}"
echo ""
