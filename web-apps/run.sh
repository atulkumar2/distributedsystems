#!/usr/bin/env bash
set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'

info()    { echo -e "${CYAN}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
STACK_CONTAINERS=(telemetry-portal-hub)
BLOCKER_PORTS=(9500)

usage() {
  cat <<'EOF'
Usage: ./run.sh [option]

Options:
  --start          Start the shared portal hub
  --stop           Stop and remove the shared portal hub
  --stop-blockers  Stop/remove Docker containers that would block port 9500
  --help           Show this help
EOF
}

ensure_prereqs() {
  command -v docker > /dev/null 2>&1 || error "Docker is not installed or not on PATH."
  [[ -f "$COMPOSE_FILE" ]] || error "docker-compose.yml not found in web-apps directory."
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
  info "Stopping blockers for the shared portal hub..."
  for cname in "${STACK_CONTAINERS[@]}"; do
    remove_container_if_present "$cname"
  done
  stop_port_blockers
  success "Blockers cleared."
}

start_stack() {
  ensure_prereqs
  info "Starting shared portal hub..."
  docker compose -f "$COMPOSE_FILE" up -d --build
  success "Portal Hub is ready at http://localhost:9500"
}

stop_stack() {
  ensure_prereqs
  info "Stopping shared portal hub..."
  docker compose -f "$COMPOSE_FILE" down
  success "Portal hub stopped."
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
