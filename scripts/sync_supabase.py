#!/usr/bin/env python3
"""
sync_supabase.py: Sync local PostgreSQL dm schema → Supabase (REST API).

Designed to run as an Airflow task after version_master_task.
Handles:
  - New patch note .md file upload to Supabase Storage
  - version_master sync (new/updated rows)
  - Small tables full sync: dm_notice, dm_update, dm_event, dm_cashshop, dm_patch_event
  - Large tables incremental sync by version: dm_rank, dm_equipment, dm_ability, etc.

Environment variables (from .env or Airflow Variables):
  PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD, PGSCHEMA
  SUPABASE_URL, SUPABASE_SERVICE_KEY
"""
from __future__ import annotations

import json
import logging
import os
import time
import datetime
import decimal
from pathlib import Path
from typing import Any

import requests

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ── Supabase config ──────────────────────────────────────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
PATCH_NOTES_BUCKET = "patch-notes"
STORAGE_BASE = f"{SUPABASE_URL}/storage/v1/object/public/{PATCH_NOTES_BUCKET}"

_REST_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal",
}
_STORAGE_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "x-upsert": "true",
}

BATCH_SIZE = 500

# Tables to fully replace on each sync (small, non-versioned)
SMALL_TABLES = ["dm_notice", "dm_update", "dm_event", "dm_cashshop", "dm_patch_event"]

# Large tables: sync only new versions
VERSIONED_TABLES = [
    "dm_ability", "dm_balance_score", "dm_equipment", "dm_force",
    "dm_hexacore", "dm_hyper", "dm_rank", "dm_seedring", "dm_shift_score",
]

# Full replace tables for dm-only sync (equipment_master, hyper_master)
DM_FULL_SYNC_TABLES = ["equipment_master", "hyper_master"]


# ── Helpers ──────────────────────────────────────────────────────────────────

def _serialize(val: Any) -> Any:
    if val is None:
        return None
    if isinstance(val, datetime.datetime):
        return val.isoformat()
    if isinstance(val, datetime.date):
        return val.isoformat()
    if isinstance(val, decimal.Decimal):
        return float(val)
    if isinstance(val, list):
        return [_serialize(v) for v in val]
    return val


def _rows_to_dicts(rows, columns: list[str]) -> list[dict]:
    return [{col: _serialize(val) for col, val in zip(columns, row)} for row in rows]


def _sb_get(path: str, params: dict = None) -> Any:
    r = requests.get(f"{SUPABASE_URL}/rest/v1/{path}", headers=_REST_HEADERS, params=params)
    r.raise_for_status()
    return r.json()


def _sb_delete_all(table: str) -> None:
    """Delete all rows - PostgREST requires at least one filter, use 'not.is.null'."""
    # Find the first column to use as filter
    r = requests.get(f"{SUPABASE_URL}/rest/v1/", headers=_REST_HEADERS)
    paths = r.json().get("paths", {})
    # Use a broad delete by matching all via a dummy filter approach
    # Simpler: delete where a NOT NULL column is not null (catches all rows)
    requests.delete(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers={**_REST_HEADERS, "Prefer": "return=minimal"},
        params={"notice_id": "gte.0"} if "notice_id" in str(paths.get(f"/{table}", "")) else None,
    )


def _sb_insert_batch(table: str, rows: list[dict]) -> None:
    if not rows:
        return
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers=_REST_HEADERS,
        data=json.dumps(rows),
    )
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Insert to {table} failed: {r.status_code} {r.text[:200]}")


def _get_local_conn():
    """Get local PostgreSQL connection."""
    try:
        import psycopg
        dsn = (
            f"host={os.getenv('PGHOST', 'localhost')} "
            f"port={os.getenv('PGPORT', '5432')} "
            f"dbname={os.getenv('PGDATABASE', 'airflow')} "
            f"user={os.getenv('PGUSER', 'airflow')} "
            f"password={os.getenv('PGPASSWORD', 'airflow')}"
        )
        return psycopg.connect(dsn)
    except ImportError:
        import psycopg2
        return psycopg2.connect(
            host=os.getenv("PGHOST", "localhost"),
            port=int(os.getenv("PGPORT", "5432")),
            dbname=os.getenv("PGDATABASE", "airflow"),
            user=os.getenv("PGUSER", "airflow"),
            password=os.getenv("PGPASSWORD", "airflow"),
        )


# ── Core sync functions ───────────────────────────────────────────────────────

def upload_patch_note(version: str, static_dir: str = "/home/jamin/static/update") -> str | None:
    """Upload {version}_patch_note.md to Supabase Storage. Returns public URL."""
    filename = f"{version}_patch_note.md"
    local_path = Path(static_dir) / filename
    if not local_path.exists():
        log.warning(f"Patch note not found: {local_path}")
        return None

    url = f"{SUPABASE_URL}/storage/v1/object/{PATCH_NOTES_BUCKET}/{filename}"
    with open(local_path, "rb") as f:
        r = requests.post(
            url,
            headers={**_STORAGE_HEADERS, "Content-Type": "text/markdown; charset=utf-8"},
            data=f,
        )
    if r.status_code in (200, 201):
        public_url = f"{STORAGE_BASE}/{filename}"
        log.info(f"Uploaded patch note: {filename}")
        return public_url
    else:
        log.error(f"Upload failed for {filename}: {r.status_code} {r.text[:100]}")
        return None


def sync_version_master(conn) -> list[str]:
    """Sync version_master from local → Supabase. Returns list of new versions."""
    schema = os.getenv("PGSCHEMA", "dm")
    cur = conn.cursor()
    cur.execute(f'SELECT * FROM "{schema}"."version_master"')
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    cur.close()

    if not rows:
        return []

    # Get existing versions from Supabase
    sb_versions = {r["version"] for r in _sb_get("version_master?select=version")}

    new_versions = []
    upsert_rows = []
    static_dir = os.getenv("PATCH_NOTE_BASE_PATH", "/home/jamin/static/update")
    for row in rows:
        d = dict(zip(cols, (_serialize(v) for v in row)))
        version = d.get("version")
        if not version:
            continue

        # Upload patch note and update URL
        if d.get("patch_note"):
            old_path = str(d["patch_note"])
            if not old_path.startswith("http"):
                # Local path → upload and replace with URL
                filename = Path(old_path).name
                public_url = upload_patch_note(version, static_dir)
                if public_url:
                    d["patch_note"] = public_url

        upsert_rows.append(d)
        if version not in sb_versions:
            new_versions.append(version)

    # Delete existing and re-insert (small table)
    requests.delete(
        f"{SUPABASE_URL}/rest/v1/version_master",
        headers={**_REST_HEADERS, "Prefer": "return=minimal"},
        params={"version": f"gte.0"},  # will match all text versions
    )
    # Use upsert via Prefer: resolution=merge-duplicates
    upsert_headers = {**_REST_HEADERS, "Prefer": "return=minimal,resolution=merge-duplicates"}
    for i in range(0, len(upsert_rows), BATCH_SIZE):
        batch = upsert_rows[i:i + BATCH_SIZE]
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/version_master",
            headers=upsert_headers,
            data=json.dumps(batch),
        )
        if r.status_code not in (200, 201):
            log.error(f"version_master upsert failed: {r.status_code} {r.text[:200]}")

    log.info(f"version_master synced: {len(upsert_rows)} rows, {len(new_versions)} new versions")
    return new_versions


def sync_small_table(conn, table: str) -> None:
    """Full replace sync for small tables."""
    schema = os.getenv("PGSCHEMA", "dm")
    cur = conn.cursor()
    cur.execute(f'SELECT * FROM "{schema}"."{table}"')
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    cur.close()

    dicts = _rows_to_dicts(rows, cols)

    # Delete all existing rows - use a workaround filter
    # Delete where 'notice_id' >= 0 (works for notice-type tables)
    # For others use a generic approach
    first_col = cols[0] if cols else None
    if first_col:
        requests.delete(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers={**_REST_HEADERS, "Prefer": "return=minimal"},
            params={first_col: "gte.0"},
        )
        requests.delete(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers={**_REST_HEADERS, "Prefer": "return=minimal"},
            params={first_col: "lt.0"},
        )
        # Also delete text-keyed rows
        requests.delete(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers={**_REST_HEADERS, "Prefer": "return=minimal"},
            params={first_col: "neq.IMPOSSIBLE_PLACEHOLDER_XYZ_999"},
        )

    # Insert new rows
    for i in range(0, len(dicts), BATCH_SIZE):
        _sb_insert_batch(table, dicts[i:i + BATCH_SIZE])
        time.sleep(0.05)

    log.info(f"  {table}: {len(dicts)} rows synced")


def sync_versioned_table(conn, table: str, new_versions: list[str]) -> None:
    """Incremental sync for large versioned tables - only new versions."""
    if not new_versions:
        log.info(f"  {table}: no new versions, skipping")
        return

    schema = os.getenv("PGSCHEMA", "dm")
    cur = conn.cursor()
    for version in new_versions:
        cur.execute(f'SELECT * FROM "{schema}"."{table}" WHERE version = %s', (version,))
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        dicts = _rows_to_dicts(rows, cols)

        for i in range(0, len(dicts), BATCH_SIZE):
            _sb_insert_batch(table, dicts[i:i + BATCH_SIZE])
            time.sleep(0.05)

        log.info(f"  {table} v{version}: {len(dicts)} rows inserted")
    cur.close()


def _sb_delete_by_version(table: str, version: str) -> None:
    """Delete rows from Supabase where version = X."""
    r = requests.delete(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers={**_REST_HEADERS, "Prefer": "return=minimal"},
        params={"version": f"eq.{version}"},
    )
    if r.status_code not in (200, 204):
        log.warning(f"  {table} delete version={version}: {r.status_code} {r.text[:100]}")


def sync_versioned_table_replace(conn, table: str, version: str) -> None:
    """Replace sync: delete Supabase rows for version, then insert from local."""
    schema = os.getenv("PGSCHEMA", "dm")
    _sb_delete_by_version(table, version)

    cur = conn.cursor()
    cur.execute(f'SELECT * FROM "{schema}"."{table}" WHERE version = %s', (version,))
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    cur.close()

    dicts = _rows_to_dicts(rows, cols)
    for i in range(0, len(dicts), BATCH_SIZE):
        _sb_insert_batch(table, dicts[i : i + BATCH_SIZE])
        time.sleep(0.05)

    log.info(f"  {table} v{version}: {len(dicts)} rows replaced")


# ── run_sync_dm_tables (dw_dm_load DAG용) ────────────────────────────────────

def run_sync_dm_tables(version: str | None = None, **kwargs) -> None:
    """
    DM 테이블만 Supabase에 싱크. version_master, dm_notice 등은 건드리지 않음.
    dw_dm_load DAG에서 refresh_shift_score 완료 후 호출.
    version: refresh_dm XCom. None이면 version_master에서 로컬 전체 버전 조회.
    """
    from dotenv import load_dotenv
    load_dotenv()

    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError(
            "SUPABASE_URL and SUPABASE_SERVICE_KEY (or SUPABASE_SERVICE_ROLE_KEY) 환경 변수 필요"
        )

    log.info("=== Supabase DM Tables Sync Start ===")

    try:
        conn = _get_local_conn()
    except Exception as e:
        log.error(f"Cannot connect to local PostgreSQL: {e}")
        raise

    try:
        schema = os.getenv("PGSCHEMA", "dm")

        # version 결정: 인자 또는 version_master에서 조회
        if version:
            versions = [version]
        else:
            cur = conn.cursor()
            cur.execute(f'SELECT version FROM "{schema}"."version_master" ORDER BY start_date')
            versions = [r[0] for r in cur.fetchall()]
            cur.close()
            if not versions:
                log.warning("version_master 비어 있음, versioned 테이블 건너뜀")

        # 1. equipment_master, hyper_master full sync
        log.info("Syncing equipment_master, hyper_master...")
        for table in DM_FULL_SYNC_TABLES:
            try:
                sync_small_table(conn, table)
            except Exception as e:
                log.error(f"  {table} sync failed: {e}")

        # 2. versioned tables (해당 version만 replace)
        if versions:
            log.info(f"Syncing versioned tables for versions: {versions}")
            for v in versions:
                for table in VERSIONED_TABLES:
                    try:
                        sync_versioned_table_replace(conn, table, v)
                    except Exception as e:
                        log.error(f"  {table} v{v} sync failed: {e}")

    finally:
        conn.close()

    log.info("=== Supabase DM Tables Sync Complete ===")


# ── Main entry point (also callable from Airflow) ────────────────────────────

def run_sync(**kwargs):
    """Main sync function. Call directly or as Airflow PythonOperator callable."""
    from dotenv import load_dotenv
    load_dotenv()

    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError(
            "SUPABASE_URL and SUPABASE_SERVICE_KEY (or SUPABASE_SERVICE_ROLE_KEY) 환경 변수 필요"
        )

    log.info("=== Supabase Sync Start ===")

    try:
        conn = _get_local_conn()
    except Exception as e:
        log.error(f"Cannot connect to local PostgreSQL: {e}")
        raise

    try:
        # 1. Sync version_master + upload patch notes
        log.info("Syncing version_master...")
        new_versions = sync_version_master(conn)
        log.info(f"New versions: {new_versions}")

        # 2. Sync small tables (full replace)
        log.info("Syncing small tables...")
        for table in SMALL_TABLES:
            try:
                sync_small_table(conn, table)
            except Exception as e:
                log.error(f"  {table} sync failed: {e}")

        # 3. Sync large versioned tables (incremental)
        if new_versions:
            log.info("Syncing versioned tables for new versions...")
            for table in VERSIONED_TABLES:
                try:
                    sync_versioned_table(conn, table, new_versions)
                except Exception as e:
                    log.error(f"  {table} sync failed: {e}")

    finally:
        conn.close()

    log.info("=== Supabase Sync Complete ===")


if __name__ == "__main__":
    run_sync()
