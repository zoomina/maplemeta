# -*- coding: utf-8 -*-
"""
DW → DM 적재 DAG.

load_character_info 종료 후 10분 뒤 실행.
"""
import os
import sys
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPTS_DIR = os.path.join(BASE_DIR, "scripts")
for path in (SCRIPTS_DIR, BASE_DIR):
    if path not in sys.path:
        sys.path.insert(0, path)

from backfill_dw_to_dm import run_incremental_for_latest, run_refresh_shift_balance_score
from dw_load_utils import get_dw_connection
from sync_supabase import run_sync_dm_tables

default_args = {
    "owner": "maplemeta",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "dw_dm_load",
    default_args=default_args,
    description="DW → DM 적재 (load_character_info 종료 10분 후)",
    schedule_interval="0 8 * * 4",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["maplemeta", "dw_dm"],
)


def _run_dm_refresh(**context):
    """load_character_info 완료 10분 대기 후 DM refresh 실행. version을 XCom으로 반환."""
    time.sleep(600)  # 10분
    return run_incremental_for_latest()


def _run_shift_score(**context):
    """DM refresh 후 shift_score 적재. version은 refresh_dm 태스크 XCom에서 조회."""
    ti = context["ti"]
    version = ti.xcom_pull(task_ids="refresh_dm")
    if not version:
        print("refresh_dm에서 version 없음, shift_score 건너뜀")
        return
    conn = get_dw_connection()
    try:
        run_refresh_shift_balance_score(conn, version)
        print(f"refresh_shift_balance_score 완료: version={version}")
    finally:
        conn.close()


def _run_supabase_sync(**context):
    """DM 적재 완료 후 Supabase에 dm 테이블 싱크."""
    ti = context["ti"]
    version = ti.xcom_pull(task_ids="refresh_dm")
    run_sync_dm_tables(version=version)


wait_load_character_info = ExternalTaskSensor(
    task_id="wait_load_character_info_complete",
    external_dag_id="load_character_info",
    external_task_id="load_character_info",
    execution_delta=timedelta(0),
    dag=dag,
)

refresh_dm = PythonOperator(
    task_id="refresh_dm",
    python_callable=_run_dm_refresh,
    dag=dag,
)

refresh_shift_score = PythonOperator(
    task_id="refresh_shift_score",
    python_callable=_run_shift_score,
    dag=dag,
)

supabase_sync = PythonOperator(
    task_id="supabase_sync",
    python_callable=_run_supabase_sync,
    dag=dag,
)

wait_load_character_info >> refresh_dm >> refresh_shift_score >> supabase_sync
