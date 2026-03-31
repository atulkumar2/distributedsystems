#!/usr/bin/env bash
# run.sh — start the streaming-enhancements stack and guide you through next steps
set -euo pipefail

# ── helpers ───────────────────────────────────────────────────────────────────
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
url()     { echo "  ${BOLD}${CYAN}$*${RESET}"; }

# ── pre-flight ────────────────────────────────────────────────────────────────
header "Pre-flight checks"

if ! command -v docker &>/dev/null; then
  warn "docker not found — please install Docker Desktop or Docker Engine."
  exit 1
fi

if ! docker compose version &>/dev/null; then
  warn "'docker compose' (v2) not available. Install the Compose plugin."
  exit 1
fi

# Detect host IP for remote-access hints
HOST_IP=$(hostname -I 2>/dev/null | awk '{print $1}' || echo "YOUR_HOST_IP")

step "Docker OK"
step "Host IP detected: ${BOLD}${HOST_IP}${RESET}"

# ── parse args ────────────────────────────────────────────────────────────────
SLOW_MODE="${SLOW_MODE:-false}"
DETACH=""

for arg in "$@"; do
  case "$arg" in
    --slow)   SLOW_MODE=true ;;
    --detach|-d) DETACH="-d" ;;
    --help|-h)
      echo ""
      echo "Usage: ./run.sh [options]"
      echo ""
      echo "  --slow      Enable SLOW_MODE on storage-consumer (simulates consumer lag)"
      echo "  --detach|-d Start all containers in the background"
      echo "  --help      Show this help"
      echo ""
      exit 0
      ;;
  esac
done

if [[ "$SLOW_MODE" == "true" ]]; then
  note "SLOW_MODE=true — storage-consumer will sleep 200-500 ms per message."
  note "Watch lag grow under: Kafka UI → Consumer Groups → telemetry-storage-group"
fi

# ── build & start ─────────────────────────────────────────────────────────────
header "Building images and starting containers"

note "This may take a minute on first run (Maven build inside Docker)."
echo ""

SLOW_MODE="$SLOW_MODE" docker compose up --build $DETACH

# ── next-steps banner (shown after ctrl-c or when detached) ──────────────────
show_next_steps() {
  header "Stack is up — here is what to do next"

  echo "  ${BOLD}Open these URLs in your browser:${RESET}"
  echo ""
  url "  http://localhost:8081        Producer UI         — fill the form or click Randomise & Send"
  url "  http://localhost:8082        Consumer UI         — live event stream via SSE"
  url "  http://localhost:8085        Storage Consumer    — in-memory event store + SSE feed"
  url "  http://localhost:8086        Alert Consumer      — ALERT / WARNING / OK live feed"
  url "  http://localhost:8084        Kafka UI            — topics, partitions, consumer lag"
  echo ""

  if [[ "$HOST_IP" != "YOUR_HOST_IP" && "$HOST_IP" != "127.0.0.1" ]]; then
    echo "  ${BOLD}From a remote machine, replace localhost with ${CYAN}${HOST_IP}${RESET}${BOLD}:${RESET}"
    echo ""
    url "  http://${HOST_IP}:8081"
    url "  http://${HOST_IP}:8082"
    url "  http://${HOST_IP}:8085"
    url "  http://${HOST_IP}:8086"
    url "  http://${HOST_IP}:8084"
    echo ""
  fi

  header "Guided learning steps"

  step "${BOLD}Step 1 — Send events${RESET}"
  note "Go to http://localhost:8081 and click 'Randomise & Send' 5–10 times."
  echo ""

  step "${BOLD}Step 2 — Watch the live stream${RESET}"
  note "Open http://localhost:8082 — events arrive in real time via SSE."
  echo ""

  step "${BOLD}Step 3 — Inspect the event store${RESET}"
  note "Open http://localhost:8085 — the table shows the latest event per vehicle."
  note "The live feed panel shows STORED (green) or DLQ (red) per record."
  echo ""

  step "${BOLD}Step 4 — Watch alert classification${RESET}"
  note "Open http://localhost:8086 — each event is tagged ALERT / WARNING / OK."
  note "Thresholds: speed > 100 km/h → ALERT, fuelLevel < 20 % → WARNING."
  echo ""

  step "${BOLD}Step 5 — Trigger a DLQ event${RESET}"
  note "On the producer form, set speed above 120 km/h and send."
  note "Check http://localhost:8085 feed — that event should show DLQ (red)."
  note "Confirm in Kafka UI → Topics → vehicle-telemetry-dlq."
  echo ""

  step "${BOLD}Step 6 — Observe fan-out (same message, two groups)${RESET}"
  note "Kafka UI → Topics → vehicle-telemetry → Messages."
  note "Both telemetry-storage-group and telemetry-alert-group consume every message."
  echo ""

  step "${BOLD}Step 7 — Simulate consumer lag${RESET}"
  note "Stop this script, then restart with:  SLOW_MODE=true ./run.sh"
  note "Send events, then watch Kafka UI → Consumer Groups → telemetry-storage-group."
  echo ""

  step "${BOLD}Step 8 — Observe partition affinity${RESET}"
  note "Kafka UI → Topics → vehicle-telemetry → Messages → Partition column."
  note "The same vehicleId always lands on the same partition (key-based hashing)."
  echo ""

  header "Useful commands"

  echo "  Tail logs from one service:"
  echo "    docker compose logs -f storage-consumer"
  echo "    docker compose logs -f alert-consumer"
  echo ""
  echo "  Stop the stack:"
  echo "    docker compose down"
  echo ""
  echo "  Stop and remove volumes (full reset):"
  echo "    docker compose down -v"
  echo ""
}

# When running in detached mode the stack is already in background — show steps now.
# When running in foreground the user will see logs until they press Ctrl-C,
# then the EXIT trap fires and prints the steps.
if [[ -n "$DETACH" ]]; then
  show_next_steps
else
  trap show_next_steps EXIT
fi
