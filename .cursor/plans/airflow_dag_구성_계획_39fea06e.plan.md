---
name: Airflow DAG 구성 계획
overview: 로컬 도커 환경에서 Apache Airflow를 설정하고, 기존 Python 스크립트들을 DAG로 변환하여 순차 실행되도록 구성합니다.
todos:
  - id: create_docker_compose
    content: docker-compose.yml 파일 생성 (Airflow, PostgreSQL 설정)
    status: completed
  - id: create_requirements
    content: requirements.txt 파일 생성 (Python 의존성)
    status: completed
  - id: modify_config
    content: config.py 수정 (환경 변수에서 API_KEY 읽기)
    status: completed
  - id: create_scripts_dir
    content: scripts 디렉토리 생성 및 스크립트 함수화
    status: completed
  - id: create_ocid_failed_master
    content: OCID 실패 캐릭터 마스터 파일 생성 및 통합 스크립트 작성
    status: completed
  - id: modify_load_ocid_logic
    content: load_ocid.py 수정 (실패 마스터 리스트 제외 + 직업군별 30개 보장 로직 추가)
    status: completed
  - id: create_backfill_script
    content: backfill_ocid.py 백필 스크립트 생성 (수동 실행용, 직업군별 30개 미만 시 추가 조회)
    status: completed
  - id: create_dag
    content: dags/maplemeta_dag.py 생성 (DAG 정의)
    status: completed
  - id: update_gitignore
    content: .gitignore 업데이트 (.env, __pycache__ 등)
    status: completed
  - id: create_env_example
    content: .env.example 파일 생성 (환경 변수 템플릿)
    status: completed
isProject: false
---

# Airflow DAG 구성 계획

## 목표

로컬 도커 환경에서 Apache Airflow를 설정하고, `load_ranker` → `load_ocid` → `load_character_info` 순서로 실행되는 DAG를 구성합니다.

## 프로젝트 구조

```
maplemeta/
├── docker-compose.yml          # Airflow Docker Compose 설정
├── Dockerfile                  # 커스텀 Airflow 이미지 (필요시)
├── requirements.txt            # Python 의존성
├── dags/
│   └── maplemeta_dag.py       # 메인 DAG 파일
├── scripts/                    # Airflow에서 실행할 스크립트
│   ├── load_ranker.py
│   ├── load_ocid.py
│   ├── load_character_info.py
│   └── backfill_ocid.py       # 백필용 스크립트
├── config.py                   # 설정 파일 (볼륨 마운트)
├── data_json/                  # 데이터 저장 디렉토리 (볼륨 마운트)
└── .env                        # 환경 변수 (API_KEY 등)
```

## 주요 작업

### 1. Docker Compose 설정

- Apache Airflow 공식 이미지 사용 (apache/airflow:2.8.0)
- PostgreSQL을 메타데이터 DB로 사용
- 볼륨 마운트:
  - `./dags` → `/opt/airflow/dags`
  - `./scripts` → `/opt/airflow/scripts`
  - `./data_json` → `/opt/airflow/data_json`
  - `./config.py` → `/opt/airflow/config.py`
  - `./data_json/ocid_failed_master.json` → `/opt/airflow/data_json/ocid_failed_master.json` (OCID 실패 캐릭터 마스터 파일)
- 환경 변수:
  - `AIRFLOW__CORE__EXECUTOR`: LocalExecutor
  - `AIRFLOW__CORE__LOAD_EXAMPLES`: false
  - `API_KEY`: 환경 변수로 설정

### 2. Python 스크립트 수정

- `scripts/load_ranker.py`: 함수화하여 Airflow에서 호출 가능하도록 수정
- `scripts/load_ocid.py`: 
  - 함수화하여 Airflow에서 호출 가능하도록 수정
  - OCID 실패 캐릭터 마스터 파일(`data_json/ocid_failed_master.json`)에서 실패 리스트 로드
  - 실패 리스트에 있는 캐릭터는 제외하고 상위 30명 선별
  - **중간에 fail이 발생하여 누락된 수만큼 추가로 채우는 로직 추가**
  - **무조건 직업군별 30개를 채우도록 보장** (예: 26개만 성공하면 31위부터 4개 추가 조회)
  - 새로운 실패 캐릭터는 마스터 파일에 자동 추가
- `scripts/load_character_info.py`: 함수화하여 Airflow에서 호출 가능하도록 수정
- `scripts/backfill_ocid.py`: 백필용 스크립트 생성 (수동 실행용, 한 번만 실행)
  - 기존 `user_ocid_{date}.json` 파일에서 직업군별 현재 개수 확인
  - **직업군별로 30개 미만인 경우, 부족한 수만큼 추가로 조회** (예: 26개만 있으면 31위부터 4개 추가)
  - 실패 마스터 리스트 제외하고 랭킹 데이터에서 추가 캐릭터 선별
  - OCID 조회 후 기존 파일에 추가하여 업데이트
  - 날짜를 인자로 받아서 특정 날짜의 데이터 백필 가능하도록 구성
- `config.py`: 환경 변수에서 API_KEY를 읽도록 수정 (환경 변수가 없으면 기존 값 사용)

### 3. DAG 파일 생성

- `dags/maplemeta_dag.py` 생성
- 세 개의 PythonOperator 작업 정의:
  - `load_ranker_task`: load_ranker.py 실행
  - `load_ocid_task`: load_ocid.py 실행 (load_ranker_task 의존)
  - `load_character_info_task`: load_character_info.py 실행 (load_ocid_task 의존)
- DATE는 Airflow Variables에서 읽거나 execution_date 사용

### 4. 의존성 관리

- `requirements.txt` 생성:
  - requests
  - pandas
  - apache-airflow (Docker 이미지에 포함되므로 제외 가능)

### 5. 환경 설정

- `.env` 파일 생성 (선택사항, gitignore에 추가)
- `.gitignore` 업데이트: `.env`, `__pycache__`, `*.pyc` 등

## 실행 순서

1. `docker-compose up -d`로 Airflow 서비스 시작
2. Airflow 웹 UI 접속 ([http://localhost:8080](http://localhost:8080))
3. 기본 계정: admin/admin (초기 설정)
4. DAG 활성화 및 실행

## OCID 실패 캐릭터 관리

- 기존 `ocid_failed_{date}.json` 파일들을 통합하여 `ocid_failed_master.json` 파일로 관리
- 이 파일은 고정적으로 OCID 호출이 불가능한 캐릭터들의 마스터 리스트
- `load_ocid.py` 실행 시:
  - 마스터 리스트를 로드하여 제외하고 상위 30명 선별 시도
  - 중간에 fail이 발생하여 누락된 수만큼 추가로 채우기 (예: 26개만 성공하면 31위부터 4개 추가 조회)
  - **무조건 직업군별 30개를 채우도록 보장**
  - 새로운 실패 캐릭터는 마스터 파일에 자동 추가
- 백필 스크립트(`backfill_ocid.py`):
  - **수동 실행용 (한 번만 실행)**
  - 기존 `user_ocid_{date}.json` 파일에서 직업군별 개수 확인
  - 30개 미만인 직업군에 대해 부족한 수만큼 추가 조회 (예: 26개만 있으면 31위부터 4개 추가)
  - 날짜를 인자로 받아서 특정 날짜의 데이터 백필 가능

## 참고사항

- DATE 관리 로직은 나중에 추가 예정 (현재는 Airflow Variables 또는 execution_date 사용)
- SQL 배치 작업은 별도 DAG로 나중에 추가
- 데이터는 `data_json` 디렉토리에 저장되며 볼륨으로 마운트되어 호스트에서도 접근 가능
- OCID 실패 마스터 파일은 수동으로도 관리 가능 (캐릭터명 리스트 형태)

