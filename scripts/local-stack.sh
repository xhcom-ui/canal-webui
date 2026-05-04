#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
if [ -f "$SCRIPT_DIR/verify-local-stack.sh" ]; then
  CANAL_WEB_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
else
  CANAL_WEB_DIR="$ROOT_DIR/canal-web"
fi
export CANAL_WEB_PROJECT_DIR="${CANAL_WEB_PROJECT_DIR:-$CANAL_WEB_DIR}"
LAUNCH_AGENTS_DIR="$HOME/Library/LaunchAgents"

WEB_PLIST="$LAUNCH_AGENTS_DIR/com.openclaw.canal-web-local.plist"
SERVER_PLIST="$LAUNCH_AGENTS_DIR/com.openclaw.canal-server-local.plist"
ADAPTER_PLIST="$LAUNCH_AGENTS_DIR/com.openclaw.canal-adapter-local.plist"

usage() {
  cat <<EOF
Usage: canal-web/scripts/local-stack.sh <status|start|stop|restart|verify>

Commands:
  status   Show LaunchAgent, Kafka and port status.
  start    Start canal-web, canal-server, canal-adapter and Kafka.
  stop     Stop canal-web, canal-adapter, canal-server and Kafka.
  restart  Stop then start all managed services.
  verify   Run the end-to-end local sync verification.
EOF
}

load_agent() {
  local plist="$1"
  if [ ! -f "$plist" ]; then
    echo "[WARN] missing plist: $plist"
    return 1
  fi
  launchctl load "$plist" 2>/dev/null || true
}

unload_agent() {
  local plist="$1"
  if [ -f "$plist" ]; then
    launchctl unload "$plist" 2>/dev/null || true
  fi
}

start_stack() {
  load_agent "$SERVER_PLIST"
  load_agent "$ADAPTER_PLIST"
  load_agent "$WEB_PLIST"
  brew services start kafka >/dev/null
}

stop_stack() {
  unload_agent "$WEB_PLIST"
  unload_agent "$ADAPTER_PLIST"
  unload_agent "$SERVER_PLIST"
  brew services stop kafka >/dev/null || true
}

status_stack() {
  echo "LaunchAgents:"
  launchctl list | grep 'com.openclaw.canal' || true
  echo
  echo "Kafka:"
  brew services list | grep kafka || true
  echo
  echo "Ports:"
  lsof -nP \
    -iTCP:18082 \
    -iTCP:11111 \
    -iTCP:18083 \
    -iTCP:9092 \
    -iTCP:3306 \
    -iTCP:6379 \
    -iTCP:9200 \
    -iTCP:5432 \
    -sTCP:LISTEN || true
  echo
  echo "Adapter:"
  curl -fsS http://127.0.0.1:18083/destinations 2>/dev/null || true
  echo
}

verify_stack() {
  "$CANAL_WEB_DIR/scripts/verify-local-stack.sh" "${VERIFY_ID:-}"
}

case "${1:-}" in
  status)
    status_stack
    ;;
  start)
    start_stack
    status_stack
    ;;
  stop)
    stop_stack
    status_stack
    ;;
  restart)
    stop_stack
    sleep 3
    start_stack
    sleep 8
    status_stack
    ;;
  verify)
    verify_stack
    ;;
  *)
    usage
    exit 1
    ;;
esac
