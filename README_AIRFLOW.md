# Airflow DAG 설정 가이드

## 개요
메이플스토리 데이터 수집을 위한 Apache Airflow DAG 설정입니다.

## 사전 준비

### 1. 환경 변수 설정
`.env.example` 파일을 참고하여 `.env` 파일을 생성하고 API_KEY를 설정하세요.

**CMD (명령 프롬프트):**
```cmd
copy .env.example .env
notepad .env
```

**PowerShell:**
```powershell
Copy-Item .env.example .env
notepad .env
```

**또는 직접 생성 (PowerShell):**
```powershell
# .env 파일 생성 후 API_KEY 설정
@"
API_KEY=your_api_key_here
DATE=2025-08-13
"@ | Out-File -FilePath .env -Encoding utf8
```

### 2. OCID 실패 마스터 파일 생성 (최초 1회)
기존 `ocid_failed_{date}.json` 파일들을 통합하여 마스터 파일을 생성합니다.

**CMD 또는 PowerShell:**
```cmd
python scripts/create_ocid_failed_master.py
```

이 명령은 `data_json/ocid_failed_master.json` 파일을 생성합니다.

## Docker Compose 실행

### 1. Airflow 초기화 (최초 1회)
**CMD 또는 PowerShell:**
```cmd
# Docker Desktop이 실행 중인지 확인 후
docker compose up airflow-init
```

**참고:** 
- 최신 Docker Desktop에서는 `docker compose` (하이픈 없음)를 사용합니다
- 구버전이면 `docker-compose` (하이픈 있음)를 사용하세요
- 둘 다 안 되면 `docker-compose.exe`를 시도해보세요

### 2. Airflow 서비스 시작
**CMD 또는 PowerShell:**
```cmd
docker compose up -d
```

### 3. Airflow 웹 UI 접속
브라우저에서 http://localhost:8080 접속
- 기본 계정: airflow / airflow

## DAG 실행

### 1. DAG 활성화
Airflow 웹 UI에서 `maplemeta_data_collection` DAG를 활성화합니다.

### 2. 날짜 설정
Airflow Variables에서 `MAPLEMETA_DATE`를 설정하거나, DAG 실행 시 execution_date를 사용합니다.

### 3. 수동 실행
DAG를 선택하고 "Trigger DAG" 버튼을 클릭하여 수동 실행할 수 있습니다.

## 백필 스크립트 사용

기존 데이터에서 누락된 캐릭터를 채우기 위해 백필 스크립트를 사용할 수 있습니다.

**CMD 또는 PowerShell:**
```cmd
python scripts/backfill_ocid.py --date 2025-08-13
```

이 스크립트는:
- 기존 `user_ocid_{date}.json` 파일에서 직업군별 개수를 확인
- 30개 미만인 직업군에 대해 부족한 수만큼 추가 조회
- 실패 마스터 리스트를 제외하고 추가 캐릭터 선별

## 작업 순서

1. **load_ranker**: 도장 랭킹 데이터 수집
2. **load_ocid**: OCID 조회 및 유저 마스터 테이블 생성
   - 실패 마스터 리스트 제외
   - 직업군별 30개 보장
3. **load_character_info**: 캐릭터 상세 정보 수집

## 주요 파일

- `dags/maplemeta_dag.py`: 메인 DAG 파일
- `scripts/load_ranker.py`: 랭킹 데이터 수집
- `scripts/load_ocid.py`: OCID 조회 (실패 마스터 리스트 제외, 직업군별 30개 보장)
- `scripts/load_character_info.py`: 캐릭터 정보 수집
- `scripts/backfill_ocid.py`: 백필 스크립트 (수동 실행용)
- `scripts/create_ocid_failed_master.py`: 실패 마스터 파일 생성
- `data_json/ocid_failed_master.json`: OCID 실패 캐릭터 마스터 리스트

## 문제 해결

### DAG가 보이지 않는 경우
- `dags/` 디렉토리가 올바르게 마운트되었는지 확인
- Airflow 로그 확인: `docker compose logs airflow-scheduler`

### Windows에서 Docker 명령어
- 최신 Docker Desktop: `docker compose` (하이픈 없음)
- 구버전: `docker-compose` (하이픈 있음)
- CMD에서 안 되면: `docker-compose.exe` 시도
- 둘 다 작동하지 않으면 Docker Desktop을 최신 버전으로 업데이트하세요

### CMD vs PowerShell
- CMD: `copy`, `dir`, `docker compose` 등 기본 명령어 사용
- PowerShell: `Copy-Item`, `Get-ChildItem`, `docker compose` 등 사용 가능
- 대부분의 경우 CMD 명령어가 더 간단하고 호환성이 좋습니다

### 스크립트 실행 오류
- `config.py`에서 환경 변수가 올바르게 로드되는지 확인
- `data_json/` 디렉토리가 존재하는지 확인

### OCID 조회 실패
- 실패한 캐릭터는 자동으로 마스터 파일에 추가됩니다
- 마스터 파일을 수동으로 수정하여 특정 캐릭터를 제외할 수 있습니다

## DW 적재 로컬 검증

### 1. Docker 실행
```cmd
docker compose up airflow-init
docker compose up -d
```

### 2. 단건 DW 적재 테스트
아래 예시는 데이터가 존재하는 날짜(예: `2026-02-11`) 기준입니다.

```cmd
docker compose exec airflow-scheduler python /opt/airflow/scripts/load_dw_daily.py --date 2026-02-11
```

### 3. 적재 결과 확인(SQL)
```cmd
docker compose exec postgres psql -U airflow -d airflow -c "select count(*) from dw.dw_rank where date='2026-02-11';"
docker compose exec postgres psql -U airflow -d airflow -c "select count(*) from dw.dw_ability where date::date='2026-02-11';"
docker compose exec postgres psql -U airflow -d airflow -c "select count(*) from dw.dw_equipment where date::date='2026-02-11';"
docker compose exec postgres psql -U airflow -d airflow -c "select count(*) from dw.dw_hexacore where date::date='2026-02-11';"
docker compose exec postgres psql -U airflow -d airflow -c "select count(*) from dw.dw_hyperstat where date::date='2026-02-11';"
docker compose exec postgres psql -U airflow -d airflow -c "select count(*) from dw.dw_seteffect where date::date='2026-02-11';"
```

### 4. DAG 마지막 단계(`load_dw`) 검증
1) Airflow UI에서 `maplemeta_data_collection` DAG를 트리거  
2) 마지막 task인 `load_dw`가 `success`인지 확인  
3) 위 SQL로 해당 집계일 row count가 0보다 큰지 확인  
