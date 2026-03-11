"""
Nexon 공지/업데이트/이벤트/캐시샵 DW/DM 백필 DAG.

단일 DAG 내 플로우:
  load -> dm_direct -> check_has_updates -> detail -> mahalil -> dw_update -> llm -> dm -> version_master

- 업데이트 신규 없을 경우: check_has_updates에서 ShortCircuit → detail 이후 태스크 모두 스킵
- 메할일 조회 실패 시: 당일 재시도 없이 태스크 실패 → 다음날 스케줄(9시)에 자동 재실행

환경변수 (.env): NEXON_API_KEY(또는 API_KEY_2), ANTHROPIC_API_KEY, DW_DATABASE_URL
"""
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for path in (os.path.join(BASE_DIR, "scripts"), BASE_DIR):
    if path not in sys.path:
        sys.path.insert(0, path)

from sync_supabase import run_sync as _run_supabase_sync
from backfill_nexon_notice import (
    _run_step_load,
    _run_step_dm_direct,
    _run_step_detail,
    _run_step_mahalil,
    _run_step_dw_update,
    _run_step_llm,
    _run_step_dm,
    _run_step_version_master,
    check_has_updates_for_dag,
)

default_args = {
    "owner": "maplemeta",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,  # 메할일 실패 시 당일 재시도 없이 다음날 재실행
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "nexon_notice_backfill",
    default_args=default_args,
    description="Nexon 공지/업데이트/이벤트/캐시샵 DW/DM 백필 (load→적재→업데이트)",
    schedule_interval="0 9 * * *",  # 매일 오전 9시 (메할일 실패 시 다음날 자동 재실행)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["maplemeta", "nexon", "notice"],
)

load_task = PythonOperator(
    task_id="load",
    python_callable=_run_step_load,
    dag=dag,
)

dm_direct_task = PythonOperator(
    task_id="dm_direct",
    python_callable=_run_step_dm_direct,
    dag=dag,
)

check_has_updates_task = ShortCircuitOperator(
    task_id="check_has_updates",
    python_callable=check_has_updates_for_dag,
    dag=dag,
)

detail_task = PythonOperator(
    task_id="detail",
    python_callable=_run_step_detail,
    dag=dag,
)

mahalil_task = PythonOperator(
    task_id="mahalil",
    python_callable=_run_step_mahalil,
    dag=dag,
)

dw_update_task = PythonOperator(
    task_id="dw_update",
    python_callable=_run_step_dw_update,
    dag=dag,
)

llm_task = PythonOperator(
    task_id="llm",
    python_callable=_run_step_llm,
    dag=dag,
)

dm_task = PythonOperator(
    task_id="dm",
    python_callable=_run_step_dm,
    dag=dag,
)

version_master_task = PythonOperator(
    task_id="version_master",
    python_callable=_run_step_version_master,
    dag=dag,
)

supabase_sync_task = PythonOperator(
    task_id="supabase_sync",
    python_callable=_run_supabase_sync,
    dag=dag,
)

load_task >> dm_direct_task >> check_has_updates_task >> detail_task >> mahalil_task >> dw_update_task >> llm_task >> dm_task >> version_master_task >> supabase_sync_task
