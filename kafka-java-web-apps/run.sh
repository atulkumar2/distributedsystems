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
KAFKA_BROKER="localhost:9092"
PRODUCER_URL="http://localhost:8081"
CONSUMER_URL="http://localhost:8082"
KAFKA_UI_URL="http://localhost:8080"
STACK_CONTAINERS=(kafka-web kafka-ui-web telemetry-producer telemetry-consumer)
BLOCKER_PORTS=(9092 8080 8081 8082)

usage() {
  cat <<'EOF'
Usage: ./run.sh [option]

Options:
  --start          Start Kafka, Kafka UI, producer app, and consumer app
  --stop           Stop and remove the Docker Compose stack for this project
  --stop-blockers  Stop/remove Docker containers that would block --start
  --help           Show this help
EOF
}

ensure_prereqs() {
  command -v docker > /dev/null 2>&1 || error "Docker is not installed or not on PATH."
  [[ -f "$COMPOSE_FILE" ]] || error "docker-compose.yml not found in project root."
  docker compose version > /dev/null 2>&1 || error "Docker Compose is not available."
  docker info > /dev/null 2>&1 || error "Docker daemon is not running."
}

remove_container_if_present() {
  local cname="$1"
  if docker inspect "$cname" > /dev/null 2>&1; then
    warn "Removing container '$cname'..."
    docker rm -f "$cname" > /dev/null
  fi
}

stop_port_blockers() {
  local port cid names
  for port in "${BLOCKER_PORTS[@]}"; do
    while IFS= read -r cid; do
      [[ -n "$cid" ]] || continue
      names="$(docker inspect --format '{{.Name}}' "$cid" 2>/dev/null | sed 's#^/##' || true)"
      warn "Removing Docker container '$names' blocking host port $port..."
      docker rm -f "$cid" > /dev/null
    done < <(docker ps -q --filter "publish=$port")
  done
}

stop_blockers() {
  ensure_prereqs
  info "Stopping Docker blockers for this project..."
  for cname in "${STACK_CONTAINERS[@]}"; do
    remove_container_if_present "$cname"
  done
  stop_port_blockers
  success "Blockers cleared."
}

start_stack() {
  ensure_prereqs

  info "Starting Kafka, Kafka UI, producer app, and consumer app..."

  # If containers with the same names exist from a different project, remove them
  # first so docker compose can recreate them cleanly here.
  local cname proj
  for cname in "${STACK_CONTAINERS[@]}"; do
    if docker inspect "$cname" > /dev/null 2>&1; then
      proj=$(docker inspect --format '{{index .Config.Labels "com.docker.compose.project.working_dir"}}' "$cname" 2>/dev/null || true)
      if [[ "$proj" != "$SCRIPT_DIR" ]]; then
        warn "Removing stale container '$cname' (from a different project)..."
        docker rm -f "$cname" > /dev/null
      fi
    fi
  done

  docker compose -f "$COMPOSE_FILE" up -d --build

  info "Waiting for Kafka broker to be ready..."
  local retries=45
  until docker exec "$KAFKA_CONTAINER" \
          /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:29092 \
          > /dev/null 2>&1; do
    retries=$((retries - 1))
    [[ $retries -le 0 ]] && error "Kafka broker did not become ready in time."
    sleep 1
  done
  success "Kafka broker is ready at $KAFKA_BROKER"

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
  echo -e "    ${YELLOW}./run.sh --stop${NC}"
  echo ""
}

stop_stack() {
  ensure_prereqs
  info "Stopping Kafka, Kafka UI, producer app, and consumer app..."
  docker compose -f "$COMPOSE_FILE" down
  success "Project stack stopped."
}

ACTION="${1:-}"

case "$ACTION" in
  --start)
    start_stack
    ;;
  --stop)
    stop_stack
    ;;
  --stop-blockers)
    stop_blockers
    ;;
  --help|-h)
    usage
    ;;
  *)
    usage
    exit 1
    ;;
esac
