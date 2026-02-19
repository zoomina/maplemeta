#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  COMPOSE_CMD=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE_CMD=(docker-compose)
else
  echo "ERROR: docker compose/docker-compose command not found."
  exit 1
fi

if [[ "${1:-}" == "--reset" ]]; then
  echo "[setup] reset volumes and containers"
  "${COMPOSE_CMD[@]}" down -v
fi

echo "[setup] airflow init"
"${COMPOSE_CMD[@]}" up airflow-init

echo "[setup] start services"
"${COMPOSE_CMD[@]}" up -d

echo "[setup] done"
