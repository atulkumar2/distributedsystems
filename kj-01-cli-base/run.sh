#!/usr/bin/env bash
set -euo pipefail

# Stage 1 helper script.
# Services started by this script:
# - shared infra if needed: kafka-shared (9092), kafka-ui-shared (8080), portainer-shared (9000/9443)
# - no stage-specific containers; this script ensures the learning topic exists and compiles the CLI module

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'

info()    { echo -e "${CYAN}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
INFRA_COMPOSE_FILE="$REPO_ROOT/infra/docker-compose.yml"
TOPIC="vehicle-telemetry"
PARTITIONS=3
BROKER="localhost:9092"
INFRA_CONTAINER="kafka-shared"
STACK_CONTAINERS=(kafka-shared kafka-ui-shared portainer-shared)
BLOCKER_PORTS=(9092 8080 9000 9443)
LEGACY_CONTAINERS=(kafka-local kafka-web kafka-streaming kafka-ui kafka-ui-web kafka-ui-streaming)

usage() {
  cat <<'EOF'
Usage: ./run.sh [option]

Options:
  --start          Start shared infra, ensure topic exists, and compile the project
  --stop           Stop and remove the shared infra stack
  --stop-blockers  Stop/remove Docker containers that would block --start
  --help           Show this help
EOF
}

ensure_prereqs() {
  command -v docker > /dev/null 2>&1 || error "Docker is not installed or not on PATH."
  docker info > /dev/null 2>&1 || error "Docker daemon is not running."
}

ensure_start_prereqs() {
  ensure_prereqs
  [[ -x "$SCRIPT_DIR/mvnw" ]] || error "Maven wrapper (mvnw) not found in project root."
  [[ -f "$INFRA_COMPOSE_FILE" ]] || error "Shared infra compose file not found at $INFRA_COMPOSE_FILE."
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
  for cname in "${STACK_CONTAINERS[@]}" "${LEGACY_CONTAINERS[@]}"; do
    remove_container_if_present "$cname"
  done
  stop_port_blockers
  success "Blockers cleared."
}

start_stack() {
  ensure_start_prereqs

  info "Starting shared infra via Docker Compose..."
  for cname in "${LEGACY_CONTAINERS[@]}"; do
    if docker inspect "$cname" > /dev/null 2>&1; then
      warn "Removing legacy container '$cname' before starting shared infra..."
      docker rm -f "$cname" > /dev/null
    fi
  done

  docker compose -f "$INFRA_COMPOSE_FILE" up -d

  info "Waiting for Kafka broker to be ready..."
  local retries=30
  until docker exec "$INFRA_CONTAINER" \
          /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:29092 \
          > /dev/null 2>&1; do
    retries=$((retries - 1))
    [[ $retries -le 0 ]] && error "Kafka broker did not become ready in time."
    sleep 1
  done
  success "Kafka broker is ready at $BROKER"

  info "Ensuring topic '$TOPIC' exists ($PARTITIONS partitions)..."
  local existing
  existing=$(docker exec "$INFRA_CONTAINER" \
    /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:29092 2>/dev/null \
    | grep -x "$TOPIC" || true)

  if [[ -z "$existing" ]]; then
    docker exec "$INFRA_CONTAINER" \
      /opt/kafka/bin/kafka-topics.sh \
      --create \
      --topic "$TOPIC" \
      --bootstrap-server localhost:29092 \
      --partitions "$PARTITIONS" \
      --replication-factor 1
    success "Topic '$TOPIC' created."
  else
    warn "Topic '$TOPIC' already exists — skipping creation."
  fi

  info "Compiling Java project..."
  "$SCRIPT_DIR/mvnw" -q -f "$SCRIPT_DIR/pom.xml" clean compile
  success "Project compiled."

  echo ""
  echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
  echo -e "${GREEN}  Everything is ready.${NC}"
  echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
  echo ""
  echo -e "  ${CYAN}Kafka UI${NC}     →  http://localhost:8080"
  echo -e "  ${CYAN}Portainer${NC}    →  http://localhost:9000"
  echo -e "  ${CYAN}Kafka broker${NC} →  $BROKER"
  echo -e "  ${CYAN}Topic${NC}        →  $TOPIC ($PARTITIONS partitions)"
  echo ""
  echo -e "  Run producer:"
  echo -e "    ${YELLOW}./mvnw exec:java -Dexec.mainClass=com.example.telematics.producer.KafkaProducerExample${NC}"
  echo ""
  echo -e "  Run consumer:"
  echo -e "    ${YELLOW}./mvnw exec:java -Dexec.mainClass=com.example.telematics.consumer.KafkaConsumerExample${NC}"
  echo ""
  echo -e "  Run manual-commit consumer:"
  echo -e "    ${YELLOW}./mvnw exec:java -Dexec.mainClass=com.example.telematics.consumer.KafkaManualCommitConsumerExample${NC}"
  echo ""
  echo -e "  Stop everything:"
  echo -e "    ${YELLOW}./run.sh --stop${NC}"
  echo ""
}

stop_stack() {
  ensure_prereqs
  [[ -f "$INFRA_COMPOSE_FILE" ]] || error "Shared infra compose file not found at $INFRA_COMPOSE_FILE."
  info "Stopping shared infra for this project..."
  docker compose -f "$INFRA_COMPOSE_FILE" down
  success "Shared infra stack stopped."
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
