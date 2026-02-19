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

echo "[verify] run DW verification SQL"
"${COMPOSE_CMD[@]}" exec -T postgres psql -U airflow -d airflow < "${ROOT_DIR}/schemas/dw_verify_queries.sql"

echo "[verify] done"
