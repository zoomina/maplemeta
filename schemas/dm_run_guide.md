# DM 적재 실행 가이드

## 파일 구성
- `schemas/dm.sql`
  - `dm` 스키마, DM 마트 테이블 DDL, 공통 적재 함수 `dm.refresh_marts(p_dates date[])` 정의
- `schemas/dm_run_backfill.sql`
  - 2025-12 수요일 5회 백필 실행 전용
- `schemas/dm_run_dag.sql`
  - DAG 단일 집계일 실행 전용

## 실행 순서
1. 먼저 `schemas/dm.sql`을 실행해 테이블/함수를 생성한다.
2. 목적에 맞는 실행 파일을 선택해 실행한다.
   - 백필: `schemas/dm_run_backfill.sql`
   - 일배치(DAG): `schemas/dm_run_dag.sql`

## 모드별 실행 쿼리

### 1) 백필 모드
`schemas/dm_run_backfill.sql`은 아래 5개 날짜를 전달한다.
- `2025-12-03`
- `2025-12-10`
- `2025-12-17`
- `2025-12-24`
- `2025-12-31`

핵심 호출:
```sql
select dm.refresh_marts(
    array[
        date '2025-12-03',
        date '2025-12-10',
        date '2025-12-17',
        date '2025-12-24',
        date '2025-12-31'
    ]
);
```

### 2) DAG 모드
`schemas/dm_run_dag.sql`은 실행일 1일만 전달한다.

핵심 호출:
```sql
select dm.refresh_marts(array[date '{{ ds }}']);
```

수동 실행 시 `{{ ds }}`를 실제 날짜로 치환해서 사용한다.

## 적재 로직 요약

### 공통 원칙
- 입력: `p_dates date[]`
- idempotent: 요청 날짜 대상 데이터를 선삭제 후 재적재
- 처리 순서:
  - `mart_patch`
  - `mart_character_daily`
  - `mart_meta_daily`
  - `mart_competition_daily`
  - `mart_equipment_daily`

### `dm.mart_patch`
- 현재 DW에 패치 전용 원천 테이블이 없으므로 fallback 정책 사용
- 전달 날짜를 `weekly_snapshot` 패치로 생성
- `patch_id = YYYYMMDD` 정수값 사용

### `dm.mart_character_daily`
- 원천: `dw.dw_rank` + `dw.dw_equipment`(`equipment_list='item_equipment'`)
- `character_id`: `ocid` 우선, 없으면 `chr:<character_name>`
- `power`: 장비 옵션 가중합으로 계산
- `segment`: `ntile(3)` 기반 `top/mid/bottom`
- `period_flag`: 현재 규칙은 `dt = patch_date`면 `post`, 아니면 `pre`
- `*_delta`: `lag()` 윈도우 함수로 이전 관측치 대비 계산

### `dm.mart_meta_daily`
- `(dt, job, ranking_type)` 카운트 기반 점유율(`job_share`) 계산
- 점유율 상위 5개 직업 `top_k_flag = true`

### `dm.mart_competition_daily`
- `rank_volatility`: `stddev_samp(rank_delta)`
- `gap_top_mid`: mid 구간 평균 rank - top 구간 평균 rank
- `entropy_meta`: 직업 점유율 엔트로피
- `gini_job_share`: 직업 점유율 지니계수

### `dm.mart_equipment_daily`
- `dw.dw_equipment`과 `dm.mart_character_daily`를 캐릭터 키로 조인
- `(dt, job, equipment_slot, item_name)`별 착용수(`wear_count`) 및 점유율(`share`) 계산

## 운영 주의사항
- `dm.sql`에는 실행 트리거가 없다. 실행은 반드시 `dm_run_backfill.sql` 또는 `dm_run_dag.sql`로 분리한다.
- DAG 모드 템플릿(`{{ ds }}`)은 Airflow 컨텍스트에서 치환되어야 한다.
- 백필/일배치 재실행은 동일 날짜 기준으로 안전하게 재처리된다.
