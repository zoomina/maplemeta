#!/usr/bin/env python3
"""
Supabase DM 테이블 리셋 및 재적재.

대상: dm_ability, dm_balance_score, dm_equipment, dm_force, dm_hexacore, dm_hyper,
      dm_rank, dm_shift_score, equipment_master, hyper_master
수정 제외: character_master, dm_cashshop, dm_event, dm_notice, dm_update, version_master

연결 방식:
  - SUPABASE_DB_URL 있음: Direct Postgres (pooler.supabase.com, supabase.co 차단 환경용)
  - SUPABASE_URL + SUPABASE_SERVICE_KEY: REST API

실행: python scripts/reset_supabase_dm_tables.py
      python scripts/reset_supabase_dm_tables.py --tables dm_balance_score,dm_shift_score  # 특정 테이블만
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR / "scripts"))

def _load_env() -> None:
    env_path = BASE_DIR / ".env"
    if not env_path.exists():
        return
    try:
        from dotenv import dotenv_values
        for k, v in dotenv_values(env_path).items():
            if v is not None:
                os.environ[k] = str(v)
    except ImportError:
        try:
            from dotenv import load_dotenv
            load_dotenv(env_path)
        except ImportError:
            with open(env_path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        idx = line.index("=")
                        k, v = line[:idx].strip(), line[idx + 1 :].strip().strip('"').strip("'")
                        if k and v:
                            os.environ[k] = v


_load_env()

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


def _get_supabase_pg_conn():
    """Supabase Direct Postgres 연결 (SUPABASE_DB_URL). pooler.supabase.com 사용."""
    url = os.getenv("SUPABASE_DB_URL")
    if not url:
        return None
    try:
        import psycopg2
        return psycopg2.connect(url)
    except ImportError:
        try:
            import psycopg
            return psycopg.connect(url)
        except ImportError:
            raise RuntimeError("psycopg2 또는 psycopg 필요 (pip install psycopg2-binary)")


def _reset_table_via_pg(local_conn, sb_conn, table: str) -> None:
    """로컬 dm에서 읽어 Supabase Postgres에 직접 적재 (DAG sync_supabase와 동일 흐름)."""
    schema = os.getenv("PGSCHEMA", "dm")
    cur = local_conn.cursor()
    cur.execute(f'SELECT * FROM "{schema}"."{table}"')
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    cur.close()

    if not rows:
        log.info(f"  {table}: 0 rows (skip)")
        return

    sb_cur = sb_conn.cursor()
    sb_cur.execute(f'TRUNCATE public."{table}" CASCADE')
    sb_conn.commit()

    cols_sql = ", ".join(f'"{c}"' for c in cols)
    try:
        from psycopg2.extras import execute_values
        insert_sql = f'INSERT INTO public."{table}" ({cols_sql}) VALUES %s'
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i : i + BATCH_SIZE]
            execute_values(sb_cur, insert_sql, batch, page_size=BATCH_SIZE)
            sb_conn.commit()
            time.sleep(0.02)
    except ImportError:
        placeholders = ", ".join(["%s"] * len(cols))
        insert_sql = f'INSERT INTO public."{table}" ({cols_sql}) VALUES ({placeholders})'
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i : i + BATCH_SIZE]
            for row in batch:
                sb_cur.execute(insert_sql, row)
            sb_conn.commit()
            time.sleep(0.02)
    sb_cur.close()

    log.info(f"  {table}: {len(rows)} rows reset (Direct Postgres)")


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
    use_pg = bool(os.getenv("SUPABASE_DB_URL"))
    use_rest = bool(os.getenv("SUPABASE_URL")) and (
        os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    )
    if not use_pg and not use_rest:
        print(
            "SUPABASE_DB_URL (Direct Postgres) 또는 "
            "SUPABASE_URL + SUPABASE_SERVICE_KEY 환경 변수 필요"
        )
        sys.exit(1)

    log.info("=== Supabase DM Tables Reset Start ===")
    log.info(f"Tables: {RESET_TABLES}")
    log.info(f"Mode: {'Direct Postgres (SUPABASE_DB_URL)' if use_pg else 'REST API'}")

    local_conn = _get_local_conn()
    try:
        if use_pg:
            sb_conn = _get_supabase_pg_conn()
            if not sb_conn:
                log.error("SUPABASE_DB_URL 연결 실패")
                sys.exit(1)
            try:
                for table in RESET_TABLES:
                    try:
                        _reset_table_via_pg(local_conn, sb_conn, table)
                    except Exception as e:
                        log.error(f"  {table} reset failed: {e}")
                        raise
            finally:
                sb_conn.close()
        else:
            for table in RESET_TABLES:
                try:
                    _reset_table(local_conn, table)
                except Exception as e:
                    log.error(f"  {table} reset failed: {e}")
                    raise
    finally:
        local_conn.close()

    log.info("=== Supabase DM Tables Reset Complete ===")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tables",
        type=str,
        metavar="T1,T2,...",
        help="리셋할 테이블만 지정 (예: dm_balance_score,dm_shift_score). 생략 시 전체.",
    )
    args = parser.parse_args()
    tables = RESET_TABLES
    if args.tables:
        tables = [t.strip() for t in args.tables.split(",") if t.strip()]
        invalid = [t for t in tables if t not in RESET_TABLES]
        if invalid:
            print(f"알 수 없는 테이블: {invalid}. 허용: {RESET_TABLES}")
            sys.exit(1)
    if args.tables:
        RESET_TABLES[:] = tables
    main()
