#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  COMPOSE_CMD=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE_CMD=(docker-compose)
else
  echo "ERROR: docker compose/docker-compose command not found."
  exit 1
fi

echo "[legacy backfill] run full DW load for all json targets"
"${COMPOSE_CMD[@]}" exec airflow-scheduler python /opt/airflow/scripts/legacy/load_dw_full.py

echo "[legacy backfill] done"
