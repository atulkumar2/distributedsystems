#!/usr/bin/env bash
set -euo pipefail

# Shared local infrastructure for the repo.
# Services started by this script:
# - ds-telemetry-postgres on localhost:55432
# - kafka-shared on localhost:9092
# - kafka-ui-shared on localhost:8080
# Optional tooling stack (separate compose file):
# - adminer-shared on localhost:8081
# - portainer-shared on localhost:9000 and localhost:9443
# All stages attach their app containers to this common Docker network.

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'

info()    { echo -e "${CYAN}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
TOOLS_COMPOSE_FILE="$SCRIPT_DIR/docker-compose.tools.yml"
ENV_FILE="$SCRIPT_DIR/.env"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  source "$ENV_FILE"
  set +a
fi

KAFKA_CONTAINER="${KAFKA_CONTAINER_NAME:-kafka-shared}"
BLOCKER_PORTS=(
  "${POSTGRES_HOST_PORT:-55432}"
  "${KAFKA_HOST_PORT:-9092}"
  "${KAFKA_UI_HOST_PORT:-8080}"
  "${ADMINER_HOST_PORT:-8081}"
  "${PORTAINER_HTTP_PORT:-9000}"
  "${PORTAINER_HTTPS_PORT:-9443}"
)
STACK_CONTAINERS=(
  "${POSTGRES_CONTAINER_NAME:-ds-telemetry-postgres}"
  "${KAFKA_CONTAINER_NAME:-kafka-shared}"
  "${KAFKA_UI_CONTAINER_NAME:-kafka-ui-shared}"
  "${ADMINER_CONTAINER_NAME:-adminer-shared}"
  "${PORTAINER_CONTAINER_NAME:-portainer-shared}"
)
LEGACY_CONTAINERS=(kafka-local kafka-web kafka-streaming kafka-ui kafka-ui-web kafka-ui-streaming)

usage() {
  cat <<'EOF'
Usage: ./run.sh [option]

Options:
  --start          Start shared Postgres, Kafka, and Kafka UI
  --start-tools    Start optional tooling stack (Adminer and Portainer)
  --stop-tools     Stop optional tooling stack (Adminer and Portainer)
  --stop           Stop and remove core shared infra stack
  --stop-blockers  Stop/remove Docker containers that would block shared infra ports
  --help           Show this help
EOF
}

ensure_prereqs() {
  command -v docker > /dev/null 2>&1 || error "Docker is not installed or not on PATH."
  [[ -f "$COMPOSE_FILE" ]] || error "docker-compose.yml not found in infra directory."
  [[ -f "$TOOLS_COMPOSE_FILE" ]] || error "docker-compose.tools.yml not found in infra directory."
  [[ -f "$ENV_FILE" ]] || error ".env not found in infra directory."
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
  info "Stopping blockers for shared infra..."
  for cname in "${STACK_CONTAINERS[@]}" "${LEGACY_CONTAINERS[@]}"; do
    remove_container_if_present "$cname"
  done
  stop_port_blockers
  success "Blockers cleared."
}

start_stack() {
  ensure_prereqs

  info "Starting shared Postgres, Kafka, and Kafka UI..."
  for cname in "${LEGACY_CONTAINERS[@]}"; do
    if docker inspect "$cname" > /dev/null 2>&1; then
      warn "Removing legacy container '$cname' before starting shared infra..."
      docker rm -f "$cname" > /dev/null
    fi
  done

  docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" up -d

  info "Waiting for shared Kafka broker to be ready..."
  local retries=45
  until docker exec "$KAFKA_CONTAINER" \
          /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:29092 \
          > /dev/null 2>&1; do
    retries=$((retries - 1))
    [[ $retries -le 0 ]] && error "Shared Kafka broker did not become ready in time."
    sleep 1
  done

  success "Shared Kafka broker is ready at localhost:9092"
  echo ""
  echo -e "  ${CYAN}Postgres${NC}     →  postgres://${POSTGRES_USER:-telematics}:${POSTGRES_PASSWORD:-telematics}@localhost:${POSTGRES_HOST_PORT:-55432}/${POSTGRES_DB:-telemetry}"
  echo -e "  ${CYAN}Kafka broker${NC} →  localhost:${KAFKA_HOST_PORT:-9092}"
  echo -e "  ${CYAN}Kafka UI${NC}     →  http://localhost:${KAFKA_UI_HOST_PORT:-8080}"
  echo -e "  ${CYAN}Tools stack${NC}  →  ./run.sh --start-tools (Adminer/Portainer)"
  echo ""
  echo -e "  ${CYAN}Adminer login${NC} →  System: PostgreSQL | Server: ${ADMINER_DEFAULT_SERVER:-ds-telemetry-postgres} | Username: ${POSTGRES_USER:-telematics} | Password: ${POSTGRES_PASSWORD:-telematics} | Database: ${POSTGRES_DB:-telemetry}"
  echo ""
}

start_tools_stack() {
  ensure_prereqs
  info "Starting optional tooling stack (Adminer and Portainer)..."
  docker compose --env-file "$ENV_FILE" -f "$TOOLS_COMPOSE_FILE" up -d
  success "Tooling stack ready."
  echo ""
  echo -e "  ${CYAN}Adminer${NC}      →  http://localhost:${ADMINER_HOST_PORT:-8081}"
  echo -e "  ${CYAN}Portainer${NC}    →  http://localhost:${PORTAINER_HTTP_PORT:-9000} or https://localhost:${PORTAINER_HTTPS_PORT:-9443}"
  echo ""
}

stop_stack() {
  ensure_prereqs
  info "Stopping core shared infra stack..."
  docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" down
  success "Core shared infra stack stopped."
}

stop_tools_stack() {
  ensure_prereqs
  info "Stopping optional tooling stack..."
  docker compose --env-file "$ENV_FILE" -f "$TOOLS_COMPOSE_FILE" down
  success "Optional tooling stack stopped."
}

ACTION="${1:-}"

case "$ACTION" in
  --start)
    start_stack
    ;;
  --start-tools)
    start_tools_stack
    ;;
  --stop)
    stop_stack
    ;;
  --stop-tools)
    stop_tools_stack
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
