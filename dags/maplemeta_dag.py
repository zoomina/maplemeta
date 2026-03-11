"""
메이플스토리 데이터 수집 DAG
- 매일: API_KEY(live) 단일 사이클 백필 실행
- 백필: 오늘 기준 과거 수요일 데이터를 최신부터 과거 순서로 수집
순서: load_ranker -> load_ocid -> load_character_info
"""
import os
import sys
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# scripts 디렉토리와 상위 디렉토리를 경로에 추가
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPTS_DIR = os.path.join(BASE_DIR, "scripts")
DATA_JSON_DIR = os.path.join(BASE_DIR, "data_json")

for path in (SCRIPTS_DIR, BASE_DIR):
    if path not in sys.path:
        sys.path.insert(0, path)

import config
from load_character_info import (
    collect_character_info_data,
    load_character_info_payload,
    read_character_info_payload,
    write_character_info_payload,
)
from load_ocid import (
    collect_user_ocid_data,
    load_user_ocid_payload,
    read_ocid_payload,
    write_ocid_payload,
)
from load_ranker import load_ranker
from dw_load_utils import ensure_dw_schema, get_dw_connection

# 기본 인자 설정
default_args = {
    'owner': 'maplemeta',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
dag = DAG(
    'maplemeta_data_collection',
    default_args=default_args,
    description='메이플스토리 랭킹 및 캐릭터 정보 수집 DAG (백필 모드)',
    schedule_interval='0 8 * * *',  # 매일 오전 8시 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['maplemeta', 'data_collection'],
)

def _resolve_api_key(api_key_name: str) -> str:
    return config.resolve_api_key(api_key_name)


def _resolve_current_date(**context):
    # Airflow Variable로 강제 집계일을 지정하면 우선 사용
    from airflow.models import Variable
    forced_date = Variable.get("MAPLEMETA_DATE", default_var=None)
    if forced_date:
        return datetime.strptime(forced_date, "%Y-%m-%d").date()

    logical_date = context.get("logical_date") or context.get("execution_date") or context.get("ds")
    if isinstance(logical_date, str):
        return datetime.strptime(logical_date, "%Y-%m-%d").date()
    if logical_date is None:
        return datetime.now().date()
    return logical_date.date()


def get_execution_date(**context):
    """
    execution_date를 YYYY-MM-DD 형식으로 반환
    """
    execution_date = context.get('execution_date') or context.get('ds')
    if isinstance(execution_date, str):
        return execution_date
    elif execution_date:
        return execution_date.strftime('%Y-%m-%d')
    else:
        # Airflow Variables에서 DATE 가져오기 또는 기본값 사용
        from airflow.models import Variable
        try:
            return Variable.get("MAPLEMETA_DATE", default_var=None)
        except Exception:
            return None

def get_reporting_date_by_policy(target_date):
    """
    집계일 정책:
    - 2025-06-18(포함) 이후: 해당 주 수요일
    - 2025-06-18 이전: 해당 주 일요일
    """
    cutoff_date = datetime(2025, 6, 18).date()
    if target_date >= cutoff_date:
        while target_date.weekday() != 2:  # 수요일
            target_date -= timedelta(days=1)
        return target_date

    # Python weekday(): 월=0, ... 일=6
    days_since_sunday = (target_date.weekday() + 1) % 7
    return target_date - timedelta(days=days_since_sunday)

def get_first_missing_date_backwards(data_type, max_weeks=52, **context):
    """
    최신 집계일부터 과거로 주차 단위로 탐색하여,
    데이터가 존재하지 않는 첫 주차의 집계일 1개를 반환.
    (ranker에서 이 로직으로 백필 대상을 정하고, ocid/character_info도 동일 방식 사용)
    data_type: 'ranking' | 'ocid' | 'character_info'
    """
    start_date = get_reporting_date_by_policy(_resolve_current_date(**context))
    seen_dates = set()
    for week_idx in range(max_weeks):
        check_dt = start_date - timedelta(days=7 * week_idx)
        check_date = get_reporting_date_by_policy(check_dt).strftime('%Y-%m-%d')
        if check_date in seen_dates:
            continue
        seen_dates.add(check_date)
        if data_type == 'ranking':
            if not check_data_exists(check_date, 'ranking'):
                return [check_date]
        elif data_type == 'ocid':
            if check_data_exists(check_date, 'ranking') and not check_data_exists(check_date, 'ocid'):
                return [check_date]
        elif data_type == 'character_info':
            if check_data_exists(check_date, 'ocid') and not check_data_exists(check_date, 'character_info'):
                return [check_date]
    return []

def get_first_incomplete_date_backwards(max_weeks=52, **context):
    """
    파이프라인(ranking -> ocid -> character_info) 기준으로
    가장 최근 미완료 집계일 1개를 반환.
    """
    start_date = get_reporting_date_by_policy(_resolve_current_date(**context))
    seen_dates = set()
    for week_idx in range(max_weeks):
        check_dt = start_date - timedelta(days=7 * week_idx)
        check_date = get_reporting_date_by_policy(check_dt).strftime('%Y-%m-%d')
        if check_date in seen_dates:
            continue
        seen_dates.add(check_date)

        ranking_ready = check_data_exists(check_date, 'ranking')
        ocid_ready = check_data_exists(check_date, 'ocid')
        character_ready = check_data_exists(check_date, 'character_info')

        # ranking 누락 또는 downstream 누락이면 같은 날짜를 체인 타겟으로 사용
        if (not ranking_ready) or (ranking_ready and (not ocid_ready or not character_ready)):
            return [check_date]
    return []

def check_data_exists(date, data_type='ranking'):
    """
    특정 날짜의 데이터가 DB에 이미 존재하는지 확인
    data_type: 'ranking', 'ocid', 'character_info'
    """
    conn = get_dw_connection()
    ensure_dw_schema(conn)
    try:
        with conn.cursor() as cur:
            if data_type == 'ranking':
                cur.execute("select 1 from dw.dw_rank where date = %s::date limit 1", (date,))
                return cur.fetchone() is not None

            if data_type == 'ocid':
                # 재선정 가능성을 고려하여 stage가 아닌 rank.ocid 존재 여부로 판단
                cur.execute(
                    "select 1 from dw.dw_rank where date = %s::date and ocid is not null limit 1",
                    (date,),
                )
                return cur.fetchone() is not None

            if data_type == 'character_info':
                # endpoint 5종의 distinct OCID 개수가 모두 일치하면 완료로 판단
                tables = (
                    "dw.dw_equipment",
                    "dw.dw_hexacore",
                    "dw.dw_seteffect",
                    "dw.dw_ability",
                    "dw.dw_hyperstat",
                )
                counts = []
                for table in tables:
                    cur.execute(
                        f"select count(distinct ocid) from {table} where date::date = %s::date",
                        (date,),
                    )
                    count = cur.fetchone()[0] or 0
                    counts.append(count)

                # 데이터가 전혀 없으면 미완료
                if not counts or counts[0] == 0:
                    return False

                # 5개 endpoint의 수가 모두 같아야 완료
                if any(c != counts[0] for c in counts[1:]):
                        return False
                return True

            raise ValueError(f"지원하지 않는 data_type: {data_type}")
    finally:
        conn.close()

def backfill_data(api_key, api_key_name, data_type, target_date=None, **context):
    """
    백필 로직: 데이터가 없는 주차가 나올 때까지 과거로 역순 탐색 후, 해당 1개 집계일만 수집
    data_type: 'ranking', 'ocid', 'character_info'
    """
    past_wednesdays = [target_date] if target_date else get_first_missing_date_backwards(data_type, max_weeks=52, **context)

    if not past_wednesdays:
        print(f"[{api_key_name}] {data_type} 백필할 집계일 없음 (데이터 없음 주차까지 역순 탐색 완료)")
        return True

    print(f"[{api_key_name}] {data_type} 백필 시작: 역순 탐색으로 발견한 1개 집계일 = {past_wednesdays[0]}")
    
    success_count = 0
    skip_count = 0
    
    for wednesday_date in past_wednesdays:
        # 이미 데이터가 있으면 건너뛰기
        if check_data_exists(wednesday_date, data_type):
            print(f"[{api_key_name}] {wednesday_date} ({data_type}): 이미 데이터 존재, 건너뜀")
            skip_count += 1
            continue
        
        print(f"\n[{api_key_name}] {data_type} 백필 처리 중: {wednesday_date}")
        
        try:
            if data_type == 'ranking':
                result = load_ranker(wednesday_date, api_key)
            elif data_type == 'ocid':
                # 랭킹 데이터가 없으면 건너뛰기
                if not check_data_exists(wednesday_date, 'ranking'):
                    print(f"[{api_key_name}] {wednesday_date}: 랭킹 데이터가 없어서 건너뜀")
                    continue
                result = create_user_ocid_table(wednesday_date, api_key)
            elif data_type == 'character_info':
                # OCID 데이터가 없으면 건너뛰기
                if not check_data_exists(wednesday_date, 'ocid'):
                    print(f"[{api_key_name}] {wednesday_date}: OCID 데이터가 없어서 건너뜀")
                    continue
                result = load_character_info_by_endpoint(wednesday_date, api_key)
            else:
                print(f"[{api_key_name}] 알 수 없는 data_type: {data_type}")
                continue
            
            if result is None:
                print(f"[{api_key_name}] {wednesday_date} ({data_type}) 수집 실패")
                continue
            
            success_count += 1
            print(f"[{api_key_name}] {wednesday_date} ({data_type}) 백필 완료")
            
        except Exception as e:
            print(f"[{api_key_name}] {wednesday_date} ({data_type}) 백필 오류: {e}")
            raise
    
    print(f"\n[{api_key_name}] {data_type} 백필 완료: 성공 {success_count}개, 건너뜀 {skip_count}개")
    return True

def load_ranker_task_func(api_key_name, **context):
    """
    랭킹 데이터 수집 작업 (백필 모드).
    데이터 없음 주차가 나올 때까지 과거로 역순 탐색한 뒤, 그 1개 집계일만 수집.
    """
    # 역순 탐색으로 백필 대상 집계일 1개 결정 (로직은 ranker에서 찍음)
    target_dates = get_first_incomplete_date_backwards(max_weeks=52, **context)
    if target_dates:
        print(f"[{api_key_name}] 체인 대상 집계일(역순 탐색): {target_dates[0]}")
    else:
        print(f"[{api_key_name}] 체인 대상 없음 (모든 탐색 주차에 ranking/ocid/character_info 완료)")
        return None

    api_key = _resolve_api_key(api_key_name)
    target_date = target_dates[0]

    print(f"[{api_key_name}] 백필 모드: 랭킹 데이터 수집 (체인 타겟 1회)")
    backfill_data(api_key, api_key_name, 'ranking', target_date=target_date, **context)
    return target_date

def load_ocid_task_func(api_key_name, **context):
    """
    OCID 데이터 수집 작업 (백필 모드)
    """
    api_key = _resolve_api_key(api_key_name)
    ranker_task_id = f"load_ranker_{api_key_name.lower()}"
    target_date = context["ti"].xcom_pull(task_ids=ranker_task_id)
    if not target_date:
        print(f"[{api_key_name}] 체인 대상 집계일 없음: OCID 수집 건너뜀")
        return True

    run_id = context.get("run_id")
    print(f"[{api_key_name}] 백필 모드: 집계일 1회 OCID 데이터 수집 (target={target_date})")
    payload = collect_user_ocid_data(target_date, api_key)
    payload_path = write_ocid_payload(payload, run_id=run_id)
    print(f"[{api_key_name}] OCID 수집 페이로드 저장 완료: {payload_path}")
    return payload_path

def _run_with_backoff(action_name, action_func):
    sleep_seconds = 10
    attempt = 1
    while True:
        try:
            print(f"[{action_name}] 적재 시도 {attempt}회차")
            result = action_func()
            print(f"[{action_name}] 적재 성공")
            return result
        except Exception as e:
            wait_seconds = min(sleep_seconds, 300)
            print(
                f"[{action_name}] 적재 실패(시도 {attempt}): {e}. "
                f"{wait_seconds}초 대기 후 재시도합니다."
            )
            time.sleep(wait_seconds)
            sleep_seconds = min(sleep_seconds * 2, 300)
            attempt += 1


def load_ocid_to_dw_task_func(api_key_name, **context):
    """
    OCID 적재 작업 (백필 모드)
    """
    collect_task_id = f"collect_ocid_{api_key_name.lower()}"
    payload_path = context["ti"].xcom_pull(task_ids=collect_task_id)
    if not payload_path:
        print(f"[{api_key_name}] OCID 수집 페이로드 없음: 적재 건너뜀")
        return True

    def _action():
        payload = read_ocid_payload(payload_path)
        return load_user_ocid_payload(payload, retry_delay_hours=3)

    return _run_with_backoff(f"{api_key_name}-ocid-load", _action)


def collect_character_info_task_func(api_key_name, **context):
    """
    캐릭터 정보 수집 작업 (백필 모드)
    """
    api_key = _resolve_api_key(api_key_name)
    ocid_collect_task_id = f"collect_ocid_{api_key_name.lower()}"
    ocid_payload_path = context["ti"].xcom_pull(task_ids=ocid_collect_task_id)
    target_date = None
    if ocid_payload_path:
        ocid_payload = read_ocid_payload(ocid_payload_path)
        target_date = ocid_payload.get("date")
    if not target_date:
        print(f"[{api_key_name}] 체인 대상 집계일 없음: 캐릭터 정보 수집 건너뜀")
        return True

    run_id = context.get("run_id")
    print(f"[{api_key_name}] 백필 모드: 집계일 1회 캐릭터 정보 수집 (target={target_date})")
    payload = collect_character_info_data(target_date, api_key)
    payload_path = write_character_info_payload(payload, run_id=run_id)
    print(f"[{api_key_name}] character_info 수집 페이로드 저장 완료: {payload_path}")
    return payload_path


def load_character_info_task_func(api_key_name, **context):
    """
    캐릭터 정보 적재 작업 (백필 모드)
    """
    collect_task_id = f"collect_character_info_{api_key_name.lower()}"
    payload_path = context["ti"].xcom_pull(task_ids=collect_task_id)
    if not payload_path:
        print(f"[{api_key_name}] character_info 수집 페이로드 없음: 적재 건너뜀")
        return True

    def _action():
        payload = read_character_info_payload(payload_path)
        return load_character_info_payload(payload, retry_delay_hours=3)

    return _run_with_backoff(f"{api_key_name}-character-load", _action)


# API_KEY (live) 단일 사이클 작업 정의
load_ranker_task = PythonOperator(
    task_id='load_ranker_api_key',
    python_callable=load_ranker_task_func,
    op_kwargs={'api_key_name': 'API_KEY'},
    dag=dag,
)

collect_ocid_task = PythonOperator(
    task_id='collect_ocid_api_key',
    python_callable=load_ocid_task_func,
    op_kwargs={'api_key_name': 'API_KEY'},
    dag=dag,
)

load_ocid_task = PythonOperator(
    task_id='load_ocid_api_key',
    python_callable=load_ocid_to_dw_task_func,
    op_kwargs={'api_key_name': 'API_KEY'},
    dag=dag,
)

collect_character_info_task = PythonOperator(
    task_id='collect_character_info_api_key',
    python_callable=collect_character_info_task_func,
    op_kwargs={'api_key_name': 'API_KEY'},
    dag=dag,
)

load_character_info_task = PythonOperator(
    task_id='load_character_info_api_key',
    python_callable=load_character_info_task_func,
    op_kwargs={'api_key_name': 'API_KEY'},
    dag=dag,
)

# 작업 의존성 설정
load_ranker_task >> collect_ocid_task >> load_ocid_task >> collect_character_info_task >> load_character_info_task
