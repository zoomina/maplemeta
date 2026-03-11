#!/usr/bin/env python3
"""
Supabase DM 테이블 리셋 및 재적재.

대상: dm_ability, dm_balance_score, dm_equipment, dm_force, dm_hexacore, dm_hyper,
      dm_rank, dm_shift_score, equipment_master, hyper_master
수정 제외: character_master, dm_cashshop, dm_event, dm_notice, dm_update, version_master

실행: python scripts/reset_supabase_dm_tables.py
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR / "scripts"))

try:
    from dotenv import load_dotenv
    load_dotenv(BASE_DIR / ".env")
except ImportError:
    pass

# sync_supabase 모듈의 헬퍼 재사용
from sync_supabase import (
    _get_local_conn,
    _rows_to_dicts,
    _sb_insert_batch,
    BATCH_SIZE,
    log,
)

import requests

RESET_TABLES = [
    "dm_ability",
    "dm_balance_score",
    "dm_equipment",
    "dm_force",
    "dm_hexacore",
    "dm_hyper",
    "dm_rank",
    "dm_shift_score",
    "equipment_master",
    "hyper_master",
]


def _sb_delete_by_filter(table: str, params: dict) -> None:
    """PostgREST delete with filter."""
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError(
            "SUPABASE_URL and SUPABASE_SERVICE_KEY (or SUPABASE_SERVICE_ROLE_KEY) 환경 변수 필요"
        )
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Prefer": "return=minimal",
    }
    r = requests.delete(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers=headers,
        params=params,
    )
    if r.status_code not in (200, 204):
        log.warning(f"  {table} delete {params}: {r.status_code} {r.text[:100]}")


def _reset_table(conn, table: str) -> None:
    """Supabase 테이블 전체 삭제 후 로컬 dm에서 전체 재적재."""
    schema = os.getenv("PGSCHEMA", "dm")
    cur = conn.cursor()
    cur.execute(f'SELECT * FROM "{schema}"."{table}"')
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    cur.close()

    dicts = _rows_to_dicts(rows, cols)

    # Supabase 전체 삭제 (sync_small_table과 동일 패턴)
    first_col = cols[0] if cols else None
    if first_col:
        _sb_delete_by_filter(table, {first_col: "gte.0"})
        _sb_delete_by_filter(table, {first_col: "lt.0"})
        _sb_delete_by_filter(table, {first_col: "neq.IMPOSSIBLE_PLACEHOLDER_XYZ_999"})

    # 재적재
    for i in range(0, len(dicts), BATCH_SIZE):
        _sb_insert_batch(table, dicts[i : i + BATCH_SIZE])
        time.sleep(0.05)

    log.info(f"  {table}: {len(dicts)} rows reset")


def main() -> None:
    if not os.getenv("SUPABASE_SERVICE_KEY") and not os.getenv("SUPABASE_SERVICE_ROLE_KEY"):
        print("SUPABASE_SERVICE_KEY 또는 SUPABASE_SERVICE_ROLE_KEY 환경 변수 필요")
        sys.exit(1)

    log.info("=== Supabase DM Tables Reset Start ===")
    log.info(f"Tables: {RESET_TABLES}")

    conn = _get_local_conn()
    try:
        for table in RESET_TABLES:
            try:
                _reset_table(conn, table)
            except Exception as e:
                log.error(f"  {table} reset failed: {e}")
                raise
    finally:
        conn.close()

    log.info("=== Supabase DM Tables Reset Complete ===")


if __name__ == "__main__":
    main()
