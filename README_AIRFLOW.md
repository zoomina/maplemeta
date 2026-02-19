# Airflow 운영 가이드

## 개요
메이플스토리 수집 파이프라인은 `maplemeta_data_collection` DAG 기준으로 동작합니다.

- 수집 순서: `load_ranker -> load_ocid -> load_character_info`
- API 키 사이클: `API_KEY_1` 체인 실행 후 `API_KEY_2` 체인 실행
- 스케줄: 매일 오전 8시 실행 (`0 8 * * *`)
- 적재 방식: 수집 단계에서 DW 테이블로 직접 upsert

## 디렉토리 기준
운영 스크립트는 모두 `scripts/` 기준입니다.

- DAG: `dags/maplemeta_dag.py`
- 수집: `scripts/load_ranker.py`, `scripts/load_ocid.py`, `scripts/load_character_info.py`
- 백필(수동): `scripts/backfill_ocid.py`, `scripts/legacy/backfill_dw_once.sh`
- 레거시 DW 로드: `scripts/legacy/load_dw_daily.py`, `scripts/legacy/load_dw_full.py`
- 검증: `scripts/verify_dw_counts.sh`, `schemas/dw_verify_queries.sql`

## 1) 환경 변수 설정
`.env.example`를 복사해 `.env`를 만듭니다.

```bash
cp .env.example .env
```

필수 항목:

- `API_KEY_1`: 최근 수요일 수집용
- `API_KEY_2`: 백필/일반 수집용

선택 항목:

- `API_KEY`: `API_KEY_2` 별칭(하위 호환)
- `DATE`: 수동 스크립트 기본 날짜
- `DW_DATABASE_URL` 또는 `DW_PG*`

## 2) Airflow 기동

```bash
docker compose up airflow-init
docker compose up -d
```

웹 UI: `http://localhost:8080`  
기본 계정: `airflow / airflow`

## 3) DAG 실행
1. Airflow UI에서 `maplemeta_data_collection` DAG 활성화
2. 필요 시 Airflow Variable `MAPLEMETA_DATE` 설정
3. `Trigger DAG`로 수동 실행

## 4) 백필 실행

### OCID 백필(단건)
```bash
python scripts/backfill_ocid.py --date 2025-08-13
```

동작:
- `dw.stage_user_ocid`에서 직업군별 부족분 점검
- 실패 마스터(`dw.collect_failed_master`) 제외 후 추가 조회
- 성공/실패 결과를 DB에 반영

### DW 전체 백필(로컬 json 스캔)
```bash
bash scripts/legacy/backfill_dw_once.sh
```

## 5) DB 기반 수집 검증

### 날짜별 기본 건수 확인
```sql
select 'dw_rank' as table_name, count(*) from dw.dw_rank where date = '2026-02-11'
union all
select 'stage_user_ocid', count(*) from dw.stage_user_ocid where date = '2026-02-11'
union all
select 'dw_ability', count(*) from dw.dw_ability where date::date = '2026-02-11'
union all
select 'dw_equipment', count(*) from dw.dw_equipment where date::date = '2026-02-11'
union all
select 'dw_hexacore', count(*) from dw.dw_hexacore where date::date = '2026-02-11'
union all
select 'dw_seteffect', count(*) from dw.dw_seteffect where date::date = '2026-02-11'
union all
select 'dw_hyperstat', count(*) from dw.dw_hyperstat where date::date = '2026-02-11';
```

## 6) DW 단건 적재/검증 (과거 JSON 재적재용)

### 단건 적재
```bash
docker compose exec airflow-scheduler python /opt/airflow/scripts/legacy/load_dw_daily.py --date 2026-02-11
```

### 검증 SQL 실행
```bash
bash scripts/verify_dw_counts.sh
```

## 7) 문제 해결

### DAG가 안 보일 때
- `docker compose ps`로 `airflow-scheduler` 상태 확인
- `docker compose logs airflow-scheduler`로 파싱 오류 확인
- `dags/`, `scripts/`, `config.py` 볼륨 마운트 확인

### API 호출이 모두 실패할 때
- `.env`의 `API_KEY_1`, `API_KEY_2` 값 확인
- 컨테이너 재시작 후 재시도: `docker compose restart airflow-scheduler airflow-webserver`

### DW 연결 실패
- `.env`의 `DW_DATABASE_URL` 또는 `DW_PG*` 설정 확인
- 로컬 docker-compose 환경이면 기본값(`postgres:5432/airflow`) 사용 가능
