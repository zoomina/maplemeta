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

def get_reporting_date_with_fallback(**context):
    """
    집계일 1개를 반환하되,
    - 랭킹/OCID/캐릭터 정보가 모두 있으면 이전 주 집계일로 이동
    - 랭킹만 있고 OCID/캐릭터 정보가 없으면 현재 집계일 유지
    """
    base_dates = get_past_wednesdays(**context)
    if not base_dates:
        return []
    
    base_date = base_dates[0]
    has_ranking = check_data_exists(base_date, 'ranking')
    has_ocid = check_data_exists(base_date, 'ocid')
    has_character = check_data_exists(base_date, 'character_info')
    
    if has_ranking and has_ocid and has_character:
        # 모두 있으면 이전 주 집계일로 이동
        base_dt = datetime.strptime(base_date, '%Y-%m-%d').date()
        previous_dt = base_dt - timedelta(days=7)
        return [previous_dt.strftime('%Y-%m-%d')]
    
    return [base_date]

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
    백필 로직: 과거 수요일 데이터를 최신부터 과거 순서로 수집
    data_type: 'ranking', 'ocid', 'character_info'
    """
    past_wednesdays = get_reporting_date_with_fallback(**context)
    
    if not past_wednesdays:
        print(f"[{api_key_name}] 백필할 수요일 데이터가 없습니다.")
        return True
    
    print(f"[{api_key_name}] {data_type} 백필 시작: 총 {len(past_wednesdays)}개 집계일")
    
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
    랭킹 데이터 수집 작업 (백필 모드)
    """
    import config
    
    # API_KEY 선택
    if api_key_name == 'API_KEY_1':
        api_key = config.API_KEY1
    else:
        api_key = config.API_KEY
    
    print(f"[{api_key_name}] 백필 모드: 집계일 1회 랭킹 데이터 수집")
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
    DW 적재 작업: 집계일 1회 DW 로드
    """
    dates = get_reporting_date_with_fallback(**context)
    if not dates:
        print("DW 적재: 처리할 집계일이 없습니다.")
        return True

    for date in dates:
        print(f"DW 적재 시작: {date}")
        load_dw_for_date(date)
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
