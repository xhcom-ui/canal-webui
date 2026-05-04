#!/bin/bash
set -euo pipefail

MYSQL_USER="${MYSQL_USER:-root}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-12345678}"
MYSQL_HOST="${MYSQL_HOST:-localhost}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
PG_BIN="${PG_BIN:-/Applications/Postgres.app/Contents/Versions/17/bin/psql}"
PGPASSWORD="${PGPASSWORD:-123456}"
REDIS_DB="${REDIS_DB:-6}"
ES_URL="${ES_URL:-http://127.0.0.1:9200}"
KAFKA_HOME="${KAFKA_HOME:-/opt/homebrew/opt/kafka}"
JAVA_HOME_KAFKA="${JAVA_HOME_KAFKA:-/opt/homebrew/opt/openjdk/libexec/openjdk.jdk/Contents/Home}"
ID="${1:-$(date +%H%M%S)}"
NAME="LocalVerify${ID}"
MOBILE="139${ID}"
STATUS="${STATUS:-13}"

ok() {
  printf '[OK] %s\n' "$1"
}

fail() {
  printf '[FAIL] %s\n' "$1" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "missing command: $1"
}

require_cmd mysql
require_cmd redis-cli
require_cmd curl

mysql_exec() {
  mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" -h"$MYSQL_HOST" -P"$MYSQL_PORT" -N -B -e "$1"
}

wait_until() {
  local label="$1"
  local command="$2"
  local attempts="${3:-20}"
  local delay="${4:-1}"
  for _ in $(seq 1 "$attempts"); do
    if eval "$command" >/tmp/verify-local-stack.out 2>/tmp/verify-local-stack.err; then
      ok "$label"
      return 0
    fi
    sleep "$delay"
  done
  printf '%s\n' "last stdout:" >&2
  cat /tmp/verify-local-stack.out >&2 || true
  printf '%s\n' "last stderr:" >&2
  cat /tmp/verify-local-stack.err >&2 || true
  fail "$label"
}

wait_until "canal-web login" \
  "curl -fsS -X POST http://127.0.0.1:18082/api/auth/login -H 'Content-Type: application/json' -d '{\"username\":\"admin\",\"password\":\"admin123\"}' | grep -q '\"code\":0'"

wait_until "canal-adapter status" \
  "curl -fsS http://127.0.0.1:18083/destinations | grep -q '\"status\":\"on\"'"

wait_until "Elasticsearch reachable" \
  "curl -fsS '$ES_URL' | grep -q 'cluster_name'"

wait_until "Kafka reachable" \
  "JAVA_HOME='$JAVA_HOME_KAFKA' '$KAFKA_HOME/bin/kafka-broker-api-versions' --bootstrap-server 127.0.0.1:9092 | grep -q 'localhost:9092'"

mysql_exec "USE canal_sync_verify; INSERT INTO user_profile(id, name, mobile, status, updated_at) VALUES (${ID}, '${NAME}', '${MOBILE}', ${STATUS}, NOW()) ON DUPLICATE KEY UPDATE name=VALUES(name), mobile=VALUES(mobile), status=VALUES(status), updated_at=VALUES(updated_at);"
ok "source MySQL inserted id=${ID}"

wait_until "target MySQL id=${ID}" \
  "mysql -u'$MYSQL_USER' -p'$MYSQL_PASSWORD' -h'$MYSQL_HOST' -P'$MYSQL_PORT' -N -B -e \"SELECT COUNT(*) FROM canal_sync_target.user_profile_copy WHERE id=${ID} AND name='${NAME}'\" | grep -q '^1$'"

wait_until "target PGSQL id=${ID}" \
  "PGPASSWORD='$PGPASSWORD' '$PG_BIN' -h 127.0.0.1 -p 5432 -U postgres -d postgres -tAc \"SELECT COUNT(*) FROM public.user_profile_pg_copy WHERE id=${ID} AND name='${NAME}'\" | grep -q '^1$'"

wait_until "target Redis id=${ID}" \
  "redis-cli -n '$REDIS_DB' HGET user_profile:${ID} name | grep -q '^${NAME}$'"

wait_until "target ES id=${ID}" \
  "curl -fsS '$ES_URL/user_profile_index/_doc/${ID}' | grep -q '\"found\":true' && curl -fsS '$ES_URL/user_profile_index/_doc/${ID}' | grep -q '\"name\":\"${NAME}\"'"

ok "local stack verification finished id=${ID}"
