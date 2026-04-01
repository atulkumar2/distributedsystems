#!/usr/bin/env bash
set -euo pipefail

# ── Colours ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'

info()    { echo -e "${CYAN}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
KAFKA_CONTAINER="kafka-web"
KAFKA_BROKER="localhost:9093"
PRODUCER_URL="http://localhost:8081"
CONSUMER_URL="http://localhost:8082"
KAFKA_UI_URL="http://localhost:8083"

# ── 1. Preflight checks ───────────────────────────────────────────────────────
command -v docker > /dev/null 2>&1 || error "Docker is not installed or not on PATH."
[[ -f "$COMPOSE_FILE" ]] || error "docker-compose.yml not found in project root."
docker compose version > /dev/null 2>&1 || error "Docker Compose is not available."
docker info > /dev/null 2>&1 || error "Docker daemon is not running."

# ── 2. Start Docker Compose stack ─────────────────────────────────────────────
info "Starting Kafka, Kafka UI, producer app, and consumer app..."

# If containers with the same names exist from a different project, remove them
# first so docker compose can recreate them cleanly here.
for cname in kafka-web kafka-ui-web telemetry-producer telemetry-consumer; do
  if docker inspect "$cname" > /dev/null 2>&1; then
    proj=$(docker inspect --format '{{index .Config.Labels "com.docker.compose.project.working_dir"}}' "$cname" 2>/dev/null || true)
    if [[ "$proj" != "$SCRIPT_DIR" ]]; then
      warn "Removing stale container '$cname' (from a different project)..."
      docker rm -f "$cname" > /dev/null
    fi
  fi
done

docker compose -f "$COMPOSE_FILE" up -d --build "$@"

# ── 3. Wait for Kafka broker to accept connections ────────────────────────────
info "Waiting for Kafka broker to be ready..."
RETRIES=45
until docker exec "$KAFKA_CONTAINER" \
        /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:29092 \
        > /dev/null 2>&1; do
  RETRIES=$((RETRIES - 1))
  [[ $RETRIES -le 0 ]] && error "Kafka broker did not become ready in time."
  sleep 1
done
success "Kafka broker is ready at $KAFKA_BROKER"

# ── 4. Ready summary ──────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}  Everything is ready.${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo -e "  ${CYAN}Producer app${NC} →  $PRODUCER_URL"
echo -e "  ${CYAN}Consumer app${NC} →  $CONSUMER_URL"
echo -e "  ${CYAN}Kafka UI${NC}     →  $KAFKA_UI_URL"
echo -e "  ${CYAN}Kafka broker${NC} →  $KAFKA_BROKER"
echo ""
echo -e "  Follow logs:"
echo -e "    ${YELLOW}docker compose -f \"$COMPOSE_FILE\" logs -f${NC}"
echo ""
echo -e "  Stop everything:"
echo -e "    ${YELLOW}docker compose -f \"$COMPOSE_FILE\" down${NC}"
echo ""
