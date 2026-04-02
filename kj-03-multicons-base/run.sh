#!/usr/bin/env bash
set -euo pipefail

# Stage 3 helper script.
# Services started by this script:
# - shared infra if needed: kafka-shared (9092), kafka-ui-shared (8080), portainer-shared (9000/9443)
# - telemetry-portal-hub on localhost:9500
# - telemetry-producer on localhost:9501
# - telemetry-consumer on localhost:9502
# - telemetry-alert-consumer on localhost:9503
# - telemetry-storage-consumer on localhost:9504

BOLD=$(tput bold 2>/dev/null || true)
RESET=$(tput sgr0 2>/dev/null || true)
CYAN=$(tput setaf 6 2>/dev/null || true)
GREEN=$(tput setaf 2 2>/dev/null || true)
YELLOW=$(tput setaf 3 2>/dev/null || true)
RED=$(tput setaf 1 2>/dev/null || true)

header()  { echo; echo "${BOLD}${CYAN}━━━  $*  ━━━${RESET}"; echo; }
step()    { echo "  ${BOLD}${GREEN}▸${RESET}  $*"; }
note()    { echo "  ${YELLOW}ℹ${RESET}  $*"; }
warn()    { echo "  ${RED}⚠${RESET}  $*"; }
error()   { echo "  ${RED}✖${RESET}  $*"; exit 1; }
url()     { echo "  ${BOLD}${CYAN}$*${RESET}"; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
INFRA_COMPOSE_FILE="$REPO_ROOT/infra/docker-compose.yml"
KAFKA_CONTAINER="kafka-shared"
STACK_CONTAINERS=(
  telemetry-portal-hub
  telemetry-storage-consumer
  telemetry-alert-consumer
  telemetry-producer
  telemetry-consumer
)
BLOCKER_PORTS=(9500 9501 9502 9503 9504)
HOST_IP=$(hostname -I 2>/dev/null | awk '{print $1}' || echo "YOUR_HOST_IP")
LEGACY_CONTAINERS=(kafka-local kafka-web kafka-streaming kafka-ui kafka-ui-web kafka-ui-streaming)

usage() {
  cat <<'EOF'
Usage: ./run.sh --start [--slow] [--detach|-d]
       ./run.sh --stop
       ./run.sh --stop-blockers
       ./run.sh --help

Options:
  --start          Start the app stack against shared infra
  --slow           Enable SLOW_MODE on storage-consumer (used with --start)
  --detach, -d     Start containers in the background (used with --start)
  --stop           Stop and remove the Docker Compose stack for this project
  --stop-blockers  Stop/remove Docker containers that would block --start
  --help           Show this help
EOF
}

ensure_prereqs() {
  command -v docker > /dev/null 2>&1 || error "docker not found. Please install Docker Desktop or Docker Engine."
  [[ -f "$COMPOSE_FILE" ]] || error "docker-compose.yml not found in project root."
  [[ -f "$INFRA_COMPOSE_FILE" ]] || error "Shared infra compose file not found at $INFRA_COMPOSE_FILE."
  docker compose version > /dev/null 2>&1 || error "'docker compose' (v2) not available. Install the Compose plugin."
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
  header "Stopping blockers"
  for cname in "${STACK_CONTAINERS[@]}" "${LEGACY_CONTAINERS[@]}"; do
    remove_container_if_present "$cname"
  done
  stop_port_blockers
  step "Blockers cleared."
}

show_next_steps() {
  header "Stack is up — here is what to do next"

  echo "  ${BOLD}Open these URLs in your browser:${RESET}"
  echo ""
  url "  http://localhost:9500        Portal Hub          — launch all dashboards from one page"
  url "  http://localhost:9501        Producer UI         — fill the form or click Randomise & Send"
  url "  http://localhost:9502        Consumer UI         — live event stream via SSE"
  url "  http://localhost:9503        Alert Consumer      — ALERT / WARNING / OK live feed"
  url "  http://localhost:9504        Storage Consumer    — in-memory event store + SSE feed"
  url "  http://localhost:8080        Kafka UI            — topics, partitions, consumer lag"
  url "  http://localhost:9000        Portainer           — inspect containers and volumes"
  echo ""

  if [[ "$HOST_IP" != "YOUR_HOST_IP" && "$HOST_IP" != "127.0.0.1" ]]; then
    echo "  ${BOLD}From a remote machine, replace localhost with ${CYAN}${HOST_IP}${RESET}${BOLD}:${RESET}"
    echo ""
    url "  http://${HOST_IP}:9500"
    url "  http://${HOST_IP}:9501"
    url "  http://${HOST_IP}:9502"
    url "  http://${HOST_IP}:9503"
    url "  http://${HOST_IP}:9504"
    url "  http://${HOST_IP}:8080"
    echo ""
  fi

  header "Guided learning steps"

  step "${BOLD}Step 1 — Send events${RESET}"
  note "Open http://localhost:9500 to launch any portal, then go to Producer UI on http://localhost:9501 and click 'Randomise & Send' 5-10 times."
  echo ""

  step "${BOLD}Step 2 — Watch the live stream${RESET}"
  note "Open http://localhost:9502 — events arrive in real time via SSE."
  echo ""

  step "${BOLD}Step 3 — Inspect the event store${RESET}"
  note "Open http://localhost:9504 — the table shows the latest event per vehicle."
  note "The live feed panel shows STORED (green) or DLQ (red) per record."
  echo ""

  step "${BOLD}Step 4 — Watch alert classification${RESET}"
  note "Open http://localhost:9503 — each event is tagged ALERT / WARNING / OK."
  note "Thresholds: speed > 100 km/h -> ALERT, fuelLevel < 20 % -> WARNING."
  echo ""

  step "${BOLD}Step 5 — Trigger a DLQ event${RESET}"
  note "On the producer form, set speed above 120 km/h and send."
  note "Check http://localhost:9504 feed — that event should show DLQ (red)."
  note "Confirm in Kafka UI -> Topics -> vehicle-telemetry-dlq."
  echo ""

  step "${BOLD}Step 6 — Observe fan-out (same message, two groups)${RESET}"
  note "Kafka UI -> Topics -> vehicle-telemetry -> Messages."
  note "Then open Kafka UI -> Consumer Groups and confirm both telemetry-storage-group and telemetry-alert-group are active."
  echo ""

  step "${BOLD}Step 7 — Simulate consumer lag${RESET}"
  note "If this stack is running in the foreground, stop it first with Ctrl-C or from another terminal with: ./run.sh --stop"
  note "Restart with: ./run.sh --start --slow --detach"
  note "Send events, then watch Kafka UI -> Consumer Groups -> telemetry-storage-group."
  echo ""

  step "${BOLD}Step 8 — Observe partition affinity${RESET}"
  note "Kafka UI -> Topics -> vehicle-telemetry -> Messages -> Partition column."
  note "The same vehicleId always lands on the same partition (key-based hashing)."
  echo ""

  header "Useful commands"

  echo "  Tail logs from one service:"
  echo "    docker compose -f \"$COMPOSE_FILE\" logs -f storage-consumer"
  echo "    docker compose -f \"$COMPOSE_FILE\" logs -f alert-consumer"
  echo ""
  echo "  Stop the stack:"
  echo "    ./run.sh --stop"
  echo ""
}

start_stack() {
  local slow_mode="${1:-false}"
  local detach="${2:-}"

  ensure_prereqs

  header "Pre-flight checks"
  step "Docker OK"
  step "Host IP detected: ${BOLD}${HOST_IP}${RESET}"

  if [[ "$slow_mode" == "true" ]]; then
    note "SLOW_MODE=true — storage-consumer will sleep 200-500 ms per message."
    note "Watch lag grow under: Kafka UI -> Consumer Groups -> telemetry-storage-group"
  fi

  header "Building images and starting containers"
  note "This may take a minute on first run (Maven build inside Docker)."
  echo ""

  local cname proj
  for cname in "${LEGACY_CONTAINERS[@]}"; do
    if docker inspect "$cname" > /dev/null 2>&1; then
      warn "Removing legacy container '$cname' before starting shared infra..."
      docker rm -f "$cname" > /dev/null
    fi
  done
  for cname in "${STACK_CONTAINERS[@]}"; do
    if docker inspect "$cname" > /dev/null 2>&1; then
      proj=$(docker inspect --format '{{index .Config.Labels "com.docker.compose.project.working_dir"}}' "$cname" 2>/dev/null || true)
      if [[ "$proj" != "$SCRIPT_DIR" ]]; then
        warn "Removing stale container '$cname' (from a different project)..."
        docker rm -f "$cname" > /dev/null
      fi
    fi
  done

  docker compose -f "$INFRA_COMPOSE_FILE" up -d

  local retries=45
  until docker exec "$KAFKA_CONTAINER" \
          /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:29092 \
          > /dev/null 2>&1; do
    retries=$((retries - 1))
    [[ $retries -le 0 ]] && error "Shared Kafka broker did not become ready in time."
    sleep 1
  done

  SLOW_MODE="$slow_mode" docker compose -f "$COMPOSE_FILE" up --build $detach
}

stop_stack() {
  ensure_prereqs
  header "Stopping stack"
  docker compose -f "$COMPOSE_FILE" down
  step "Project stack stopped."
}

ACTION=""
SLOW_MODE_FLAG="${SLOW_MODE:-false}"
DETACH_FLAG=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --start)
      ACTION="start"
      ;;
    --stop)
      ACTION="stop"
      ;;
    --stop-blockers)
      ACTION="stop-blockers"
      ;;
    --slow)
      SLOW_MODE_FLAG="true"
      ;;
    --detach|-d)
      DETACH_FLAG="-d"
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      usage
      exit 1
      ;;
  esac
  shift
done

case "$ACTION" in
  start)
    if [[ -n "$DETACH_FLAG" ]]; then
      start_stack "$SLOW_MODE_FLAG" "$DETACH_FLAG"
      show_next_steps
    else
      trap show_next_steps EXIT
      start_stack "$SLOW_MODE_FLAG" ""
    fi
    ;;
  stop)
    stop_stack
    ;;
  stop-blockers)
    stop_blockers
    ;;
  *)
    usage
    exit 1
    ;;
esac
