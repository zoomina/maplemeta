# -*- coding: utf-8 -*-
"""
캐릭터 정보 수집 DAG (API_KEY_1 전용)
- 매주 목요일 8시 KST 실행
- load_ranker -> collect_ocid -> load_ocid -> collect_character_info -> load_character_info
"""
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPTS_DIR = os.path.join(BASE_DIR, "scripts")
for path in (SCRIPTS_DIR, BASE_DIR):
    if path not in sys.path:
        sys.path.insert(0, path)

from maplemeta_dag import (
    collect_character_info_task_func,
    load_character_info_task_func,
    load_ocid_task_func,
    load_ocid_to_dw_task_func,
    load_ranker_task_func,
)

# maplemeta_dag 함수들이 api_key_name으로 task_id를 생성 (load_ranker_api_key_1 등).
# 우리 DAG는 task_id를 load_ranker 등으로 사용하므로, xcom_pull 시 매핑 필요.
TASK_ID_MAP = {
    "load_ranker_api_key_1": "load_ranker",
    "collect_ocid_api_key_1": "collect_ocid",
    "load_ocid_api_key_1": "load_ocid",
    "collect_character_info_api_key_1": "collect_character_info",
    "load_character_info_api_key_1": "load_character_info",
}


class _XComTaskIdMapper:
    """ti.xcom_pull 호출 시 task_ids를 우리 DAG의 task_id로 매핑"""

    def __init__(self, ti, task_id_map):
        self._ti = ti
        self._map = task_id_map

    def xcom_pull(self, task_ids=None, **kwargs):
        mapped = self._map.get(task_ids, task_ids) if task_ids else task_ids
        return self._ti.xcom_pull(task_ids=mapped, **kwargs)

    def __getattr__(self, name):
        return getattr(self._ti, name)


def _with_mapped_context(api_key_name, task_func, **context):
    """context['ti']를 task_id 매핑 래퍼로 교체 후 task_func 호출"""
    ti = context.get("ti")
    if ti:
        context = {**context, "ti": _XComTaskIdMapper(ti, TASK_ID_MAP)}
    return task_func(api_key_name=api_key_name, **context)


default_args = {
    "owner": "maplemeta",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "load_character_info",
    default_args=default_args,
    description="캐릭터 정보 수집 DAG (API_KEY_1 전용, 매주 목요일 8시)",
    schedule_interval="0 8 * * 4",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["maplemeta", "data_collection"],
)

load_ranker = PythonOperator(
    task_id="load_ranker",
    python_callable=load_ranker_task_func,
    op_kwargs={"api_key_name": "API_KEY_1"},
    dag=dag,
)

collect_ocid = PythonOperator(
    task_id="collect_ocid",
    python_callable=lambda **ctx: _with_mapped_context("API_KEY_1", load_ocid_task_func, **ctx),
    dag=dag,
)

load_ocid = PythonOperator(
    task_id="load_ocid",
    python_callable=lambda **ctx: _with_mapped_context("API_KEY_1", load_ocid_to_dw_task_func, **ctx),
    dag=dag,
)

collect_character_info = PythonOperator(
    task_id="collect_character_info",
    python_callable=lambda **ctx: _with_mapped_context("API_KEY_1", collect_character_info_task_func, **ctx),
    dag=dag,
)

load_character_info = PythonOperator(
    task_id="load_character_info",
    python_callable=lambda **ctx: _with_mapped_context("API_KEY_1", load_character_info_task_func, **ctx),
    dag=dag,
)

load_ranker >> collect_ocid >> load_ocid >> collect_character_info >> load_character_info
