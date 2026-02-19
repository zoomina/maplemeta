"""
메이플스토리 데이터 수집 DAG
- 매일: API_KEY_1과 API_KEY_2 둘 다 백필 실행 (2개 사이클)
- 백필: 오늘 기준 과거 수요일 데이터를 최신부터 과거 순서로 수집
순서: load_ranker -> load_ocid -> load_character_info (각 API_KEY별로)
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# scripts 디렉토리와 상위 디렉토리를 경로에 추가
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
scripts_dir = os.path.join(base_dir, 'scripts')
sys.path.insert(0, scripts_dir)
sys.path.insert(0, base_dir)

from load_ranker import load_ranker
from load_ocid import create_user_ocid_table
from load_character_info import load_character_info_by_endpoint
from load_dw_daily import load_dw_for_date

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
    schedule_interval=None,  # 수동 실행 또는 Airflow Variables로 스케줄 설정 가능
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['maplemeta', 'data_collection'],
)

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
        except:
            return None

def get_past_wednesdays(**context):
    """
    집계일 1개만 반환 (API 쿼리량 제한)
    - 2025-06-18(포함) 이후: 최근 수요일
    - 2025-06-18 이전: 해당 주 일요일
    """
    logical_date = context.get('logical_date') or context.get('execution_date') or context.get('ds')
    if isinstance(logical_date, str):
        current_date = datetime.strptime(logical_date, '%Y-%m-%d').date()
    elif logical_date is None:
        current_date = datetime.now().date()
    else:
        current_date = logical_date.date()
    
    cutoff_date = datetime(2025, 6, 18).date()
    if current_date >= cutoff_date:
        # 현재 날짜부터 과거로 거슬러 올라가며 가장 가까운 수요일 1개만 찾기
        while current_date.weekday() != 2:  # 수요일
            current_date -= timedelta(days=1)
    else:
        # 해당 주의 일요일(주 시작 기준) 1개만 찾기
        # Python weekday(): 월=0, ... 일=6
        days_since_sunday = (current_date.weekday() + 1) % 7
        current_date = current_date - timedelta(days=days_since_sunday)
    
    return [current_date.strftime('%Y-%m-%d')]

def _is_dw_source_ready(date: str) -> bool:
    return (
        check_data_exists(date, 'ranking')
        and check_data_exists(date, 'ocid')
        and check_data_exists(date, 'character_info')
    )


def get_reporting_dates_for_dw(**context):
    """
    DW 적재 대상 집계일 목록을 반환.
    우선순위:
    1) 현재 집계일(base_date)이 준비되면 포함
    2) 이전 주 집계일(previous_date)이 준비되면 추가 포함
    """
    results = []
    seen = set()

    def _append_if_ready(target_date: str):
        if target_date in seen:
            return
        if _is_dw_source_ready(target_date):
            results.append(target_date)
            seen.add(target_date)

    base_dates = get_past_wednesdays(**context)
    if not base_dates:
        return []
    base_date = base_dates[0]
    _append_if_ready(base_date)

    base_dt = datetime.strptime(base_date, '%Y-%m-%d').date()
    previous_dt = base_dt - timedelta(days=7)
    _append_if_ready(previous_dt.strftime('%Y-%m-%d'))

    return results


def get_first_missing_date_backwards(data_type, max_weeks=52, **context):
    """
    최신 집계일부터 과거로 주차 단위로 탐색하여,
    데이터가 존재하지 않는 첫 주차의 집계일 1개를 반환.
    (ranker에서 이 로직으로 백필 대상을 정하고, ocid/character_info도 동일 방식 사용)
    data_type: 'ranking' | 'ocid' | 'character_info'
    """
    base_dates = get_past_wednesdays(**context)
    if not base_dates:
        return []
    start_date = datetime.strptime(base_dates[0], '%Y-%m-%d').date()
    for week_idx in range(max_weeks):
        check_dt = start_date - timedelta(days=7 * week_idx)
        check_date = check_dt.strftime('%Y-%m-%d')
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

def check_data_exists(date, data_type='ranking'):
    """
    특정 날짜의 데이터 파일이 이미 존재하는지 확인
    data_type: 'ranking', 'ocid', 'character_info'
    """
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_json_dir = os.path.join(base_dir, 'data_json')
    
    if data_type == 'ranking':
        file_path = os.path.join(data_json_dir, f'dojang_ranking_{date}.json')
    elif data_type == 'ocid':
        file_path = os.path.join(data_json_dir, f'user_ocid_{date}.json')
    elif data_type == 'character_info':
        # 캐릭터 정보는 여러 파일이 있으므로 하나라도 있으면 True
        endpoints = ['equipment', 'hexamatrix', 'set_effect', 'ability', 'hyper_stat']
        for endpoint in endpoints:
            file_path = os.path.join(data_json_dir, f'character_{endpoint}_{date}.json')
            if os.path.exists(file_path):
                return True
        return False
    
    return os.path.exists(file_path)

def backfill_data(api_key, api_key_name, data_type, **context):
    """
    백필 로직: 데이터가 없는 주차가 나올 때까지 과거로 역순 탐색 후, 해당 1개 집계일만 수집
    data_type: 'ranking', 'ocid', 'character_info'
    """
    past_wednesdays = get_first_missing_date_backwards(data_type, max_weeks=52, **context)

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
            continue
    
    print(f"\n[{api_key_name}] {data_type} 백필 완료: 성공 {success_count}개, 건너뜀 {skip_count}개")
    return True

def load_ranker_task_func(api_key_name, **context):
    """
    랭킹 데이터 수집 작업 (백필 모드).
    데이터 없음 주차가 나올 때까지 과거로 역순 탐색한 뒤, 그 1개 집계일만 수집.
    """
    import config

    # 역순 탐색으로 백필 대상 집계일 1개 결정 (로직은 ranker에서 찍음)
    target_dates = get_first_missing_date_backwards('ranking', max_weeks=52, **context)
    if target_dates:
        print(f"[{api_key_name}] 백필 대상 집계일(역순 탐색): {target_dates[0]}")
    else:
        print(f"[{api_key_name}] 백필 대상 없음 (모든 탐색 주차에 랭킹 데이터 존재)")

    if api_key_name == 'API_KEY_1':
        api_key = config.API_KEY1
    else:
        api_key = config.API_KEY

    print(f"[{api_key_name}] 백필 모드: 랭킹 데이터 수집 (역순 탐색 1회)")
    backfill_data(api_key, api_key_name, 'ranking', **context)
    return True

def load_ocid_task_func(api_key_name, **context):
    """
    OCID 데이터 수집 작업 (백필 모드)
    """
    import config
    
    # API_KEY 선택
    if api_key_name == 'API_KEY_1':
        api_key = config.API_KEY1
    else:
        api_key = config.API_KEY
    
    print(f"[{api_key_name}] 백필 모드: 집계일 1회 OCID 데이터 수집")
    backfill_data(api_key, api_key_name, 'ocid', **context)
    return True

def load_character_info_task_func(api_key_name, **context):
    """
    캐릭터 정보 수집 작업 (백필 모드)
    """
    import config
    
    # API_KEY 선택
    if api_key_name == 'API_KEY_1':
        api_key = config.API_KEY1
    else:
        api_key = config.API_KEY
    
    print(f"[{api_key_name}] 백필 모드: 집계일 1회 캐릭터 정보 수집")
    backfill_data(api_key, api_key_name, 'character_info', **context)
    return True


def load_dw_task_func(**context):
    """
    DW 적재 작업: 준비된 집계일들을 순차 DW 로드
    """
    dates = get_reporting_dates_for_dw(**context)
    if not dates:
        print("DW 적재: 처리할 집계일이 없습니다.")
        return True

    for date in dates:
        print(f"DW 적재 시작: {date}")
        try:
            load_dw_for_date(date)
        except Exception as e:
            print(f"DW 적재 실패: {date}, error={e}")
            raise
        print(f"DW 적재 완료: {date}")
    return True

# API_KEY_1 작업 정의
load_ranker_task_1 = PythonOperator(
    task_id='load_ranker_api_key_1',
    python_callable=load_ranker_task_func,
    op_kwargs={'api_key_name': 'API_KEY_1'},
    dag=dag,
)

load_ocid_task_1 = PythonOperator(
    task_id='load_ocid_api_key_1',
    python_callable=load_ocid_task_func,
    op_kwargs={'api_key_name': 'API_KEY_1'},
    dag=dag,
)

load_character_info_task_1 = PythonOperator(
    task_id='load_character_info_api_key_1',
    python_callable=load_character_info_task_func,
    op_kwargs={'api_key_name': 'API_KEY_1'},
    dag=dag,
)

# API_KEY_2 작업 정의
load_ranker_task_2 = PythonOperator(
    task_id='load_ranker_api_key_2',
    python_callable=load_ranker_task_func,
    op_kwargs={'api_key_name': 'API_KEY_2'},
    dag=dag,
)

load_ocid_task_2 = PythonOperator(
    task_id='load_ocid_api_key_2',
    python_callable=load_ocid_task_func,
    op_kwargs={'api_key_name': 'API_KEY_2'},
    dag=dag,
)

load_character_info_task_2 = PythonOperator(
    task_id='load_character_info_api_key_2',
    python_callable=load_character_info_task_func,
    op_kwargs={'api_key_name': 'API_KEY_2'},
    dag=dag,
)

load_dw_task = PythonOperator(
    task_id='load_dw',
    python_callable=load_dw_task_func,
    dag=dag,
)

# 작업 의존성 설정
# API_KEY_1 순차 실행 -> API_KEY_2 순차 실행
load_ranker_task_1 >> load_ocid_task_1 >> load_character_info_task_1 >> \
load_ranker_task_2 >> load_ocid_task_2 >> load_character_info_task_2 >> load_dw_task
