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
LEGACY_CONTAINERS=(kafka-local kafka-web kafka-streaming kafka-ui kafka-ui-web kafka-ui-streaming)
CORE_CONFIG=""
TOOLS_CONFIG=""

core_compose() {
  docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" "$@"
}

tools_compose() {
  docker compose --env-file "$ENV_FILE" -f "$TOOLS_COMPOSE_FILE" "$@"
}

refresh_compose_config() {
  CORE_CONFIG="$(core_compose config)"
  TOOLS_CONFIG="$(tools_compose config)"
}

config_container_names() {
  local config="$1"
  awk '$1 == "container_name:" { print $2 }' <<<"$config"
}

config_published_ports() {
  local config="$1"
  awk '$1 == "published:" { gsub(/"/, "", $2); print $2 }' <<<"$config"
}

config_service_container_name() {
  local config="$1"
  local service="$2"
  awk -v svc="$service" '
    /^  [^[:space:]].*:/ {
      current = $1
      sub(":", "", current)
    }
    current == svc && $1 == "container_name:" {
      print $2
      exit
    }
  ' <<<"$config"
}

config_service_published_port() {
  local config="$1"
  local service="$2"
  local target_port="$3"
  awk -v svc="$service" -v wanted="$target_port" '
    /^  [^[:space:]].*:/ {
      current = $1
      sub(":", "", current)
      target = ""
    }
    current == svc && $1 == "target:" {
      gsub(/"/, "", $2)
      target = $2
    }
    current == svc && $1 == "published:" && target == wanted {
      gsub(/"/, "", $2)
      print $2
      exit
    }
  ' <<<"$config"
}

config_service_environment_value() {
  local config="$1"
  local service="$2"
  local key="$3"
  awk -v svc="$service" -v wanted="$key" '
    /^  [^[:space:]].*:/ {
      current = $1
      sub(":", "", current)
      in_env = 0
    }
    current == svc && /^    environment:$/ {
      in_env = 1
      next
    }
    current == svc && in_env && /^    [^[:space:]].*:/ {
      in_env = 0
    }
    current == svc && in_env && /^      / {
      field = $1
      sub(":", "", field)
      if (field == wanted) {
        gsub(/"/, "", $2)
        print $2
        exit
      }
    }
  ' <<<"$config"
}

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
  while IFS= read -r port; do
    [[ -n "$port" ]] || continue
    while IFS= read -r cid; do
      [[ -n "$cid" ]] || continue
      names="$(docker inspect --format '{{.Name}}' "$cid" 2>/dev/null | sed 's#^/##' || true)"
      warn "Removing Docker container '$names' blocking host port $port..."
      docker rm -f "$cid" > /dev/null
    done < <(docker ps -q --filter "publish=$port")
  done < <(
    config_published_ports "$CORE_CONFIG"
    config_published_ports "$TOOLS_CONFIG"
  )
}

stop_blockers() {
  ensure_prereqs
  refresh_compose_config
  info "Stopping blockers for shared infra..."
  for cname in "${LEGACY_CONTAINERS[@]}"; do
    remove_container_if_present "$cname"
  done
  while IFS= read -r cname; do
    [[ -n "$cname" ]] || continue
    remove_container_if_present "$cname"
  done < <(
    config_container_names "$CORE_CONFIG"
    config_container_names "$TOOLS_CONFIG"
  )
  stop_port_blockers
  success "Blockers cleared."
}

start_stack() {
  ensure_prereqs
  refresh_compose_config

  info "Starting shared Postgres, Kafka, and Kafka UI..."
  for cname in "${LEGACY_CONTAINERS[@]}"; do
    if docker inspect "$cname" > /dev/null 2>&1; then
      warn "Removing legacy container '$cname' before starting shared infra..."
      docker rm -f "$cname" > /dev/null
    fi
  done

  core_compose up -d

  info "Waiting for shared Kafka broker to be ready..."
  local retries=45
  until core_compose exec -T kafka \
          /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:29092 \
          > /dev/null 2>&1; do
    retries=$((retries - 1))
    [[ $retries -le 0 ]] && error "Shared Kafka broker did not become ready in time."
    sleep 1
  done

  success "Shared Kafka broker is ready at localhost:9092"
  echo ""
  echo -e "  ${CYAN}Postgres${NC}     →  postgres://$(config_service_environment_value "$CORE_CONFIG" postgres POSTGRES_USER):$(config_service_environment_value "$CORE_CONFIG" postgres POSTGRES_PASSWORD)@localhost:$(config_service_published_port "$CORE_CONFIG" postgres 5432)/$(config_service_environment_value "$CORE_CONFIG" postgres POSTGRES_DB)"
  echo -e "  ${CYAN}Kafka broker${NC} →  localhost:$(config_service_published_port "$CORE_CONFIG" kafka 9092)"
  echo -e "  ${CYAN}Kafka UI${NC}     →  http://localhost:$(config_service_published_port "$CORE_CONFIG" kafka-ui 8080)"
  echo -e "  ${CYAN}Tools stack${NC}  →  ./run.sh --start-tools (Adminer/Portainer)"
  echo ""
  echo -e "  ${CYAN}Adminer login${NC} →  System: PostgreSQL | Server: $(config_service_environment_value "$TOOLS_CONFIG" adminer ADMINER_DEFAULT_SERVER) | Username: $(config_service_environment_value "$CORE_CONFIG" postgres POSTGRES_USER) | Password: $(config_service_environment_value "$CORE_CONFIG" postgres POSTGRES_PASSWORD) | Database: $(config_service_environment_value "$CORE_CONFIG" postgres POSTGRES_DB)"
  echo ""
}

start_tools_stack() {
  ensure_prereqs
  refresh_compose_config
  info "Starting optional tooling stack (Adminer and Portainer)..."
  tools_compose up -d
  success "Tooling stack ready."
  echo ""
  echo -e "  ${CYAN}Adminer${NC}      →  http://localhost:$(config_service_published_port "$TOOLS_CONFIG" adminer 8080)"
  echo -e "  ${CYAN}Portainer${NC}    →  http://localhost:$(config_service_published_port "$TOOLS_CONFIG" portainer 9000) or https://localhost:$(config_service_published_port "$TOOLS_CONFIG" portainer 9443)"
  echo ""
}

stop_stack() {
  ensure_prereqs
  info "Stopping core shared infra stack..."
  core_compose down
  success "Core shared infra stack stopped."
}

stop_tools_stack() {
  ensure_prereqs
  info "Stopping optional tooling stack..."
  tools_compose down
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
