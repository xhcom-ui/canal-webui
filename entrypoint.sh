#!/bin/sh
set -eu

export CANAL_WEB_PROJECT_DIR="${CANAL_WEB_PROJECT_DIR:-/app}"

java -jar app.jar --spring.profiles.active="${SPRING_PROFILES_ACTIVE:-prod}" --server.port="${SERVER_PORT:-18082}"
