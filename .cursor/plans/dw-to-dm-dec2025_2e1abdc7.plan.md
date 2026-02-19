---
name: dw-to-dm-dec2025
overview: "`DM 필드정의서.md` 기준으로 DW→DM 공통 변환 SQL을 작성하고, 이를 재사용하는 백필용(기간 범위)과 DAG용(단일 집계일) 실행 쿼리를 분리해 운영/확장성을 확보합니다."
todos:
  - id: create-dm-ddl
    content: dm 스키마 및 mart 5개 테이블 DDL 작성
    status: pending
  - id: build-common-transform
    content: 기간 입력만 바꿔 재사용 가능한 공통 변환 CTE/INSERT 로직 작성
    status: pending
  - id: write-character-mart
    content: dw_rank/dw_equipment 기반 mart_character_daily 적재 SQL 작성
    status: pending
  - id: write-derived-marts
    content: meta/competition/equipment/patch 마트 적재 SQL 작성
    status: pending
  - id: split-backfill-dag-queries
    content: 공통 로직을 호출하는 백필용 기간 쿼리와 DAG용 단일일자 쿼리 분리 작성
    status: pending
  - id: add-idempotent-load
    content: 재실행 안전한 DELETE+INSERT/UPSERT 블록 구성
    status: pending
  - id: add-validation-queries
    content: 날짜별 건수 및 지표 품질 검증 SQL 작성
    status: pending
isProject: false
---

# DW→DM 2025-12 추출 SQL 작성

## 목표

- `[/home/jamin/Workspace/maplemeta/DM 필드정의서.md](/home/jamin/Workspace/maplemeta/DM 필드정의서.md)`에 정의된 5개 마트(`mart_patch`, `mart_character_daily`, `mart_meta_daily`, `mart_competition_daily`, `mart_equipment_daily`)를 SQL로 생성/적재.
- 대상 기간은 수요일 5회: `2025-12-03`, `2025-12-10`, `2025-12-17`, `2025-12-24`, `2025-12-31`.
- 재실행 시 중복 없이 동일 결과가 나오도록(DELETE+INSERT 또는 UPSERT) 설계.

## 변경 파일

- 신규: `[/home/jamin/Workspace/maplemeta/schemas/dm.sql](/home/jamin/Workspace/maplemeta/schemas/dm.sql)`
- (선택) 검증 쿼리 추가: `[/home/jamin/Workspace/maplemeta/schemas/dw_verify_queries.sql](/home/jamin/Workspace/maplemeta/schemas/dw_verify_queries.sql)`

## 구현 단계

1. `dm` 스키마 및 5개 마트 테이블 DDL 정의

- 각 테이블 컬럼을 정의서와 1:1 매핑.
- PK/인덱스를 날짜 기반 분석에 맞게 추가.

1. 대상 기간 CTE를 공통으로 정의

- SQL 내부에서 대상 5일만 필터링하는 CTE를 작성해 모든 마트 적재에 재사용.
- 예시 패턴:
  - `target_dates as (select unnest(array['2025-12-03'::date, ...]) as dt)`

1. `mart_character_daily` 적재 쿼리 작성

- 원천: `dw.dw_rank` + `dw.dw_equipment`.
- `character_id`는 `ocid` 우선, 없으면 보조키(예: character_name) fallback 정책 명시.
- 전일 대비 지표(`*_delta`)는 윈도우 함수(`lag`)로 계산.
- `segment`는 분위수 기반(`ntile`)으로 top/mid/bottom 부여.
- `patch_id`, `period_flag`는 패치 기준 조인 로직으로 계산(정의서 규칙 반영).

1. 파생 마트 적재 쿼리 작성

- `mart_meta_daily`: `mart_character_daily`에서 직업 점유율 및 top_k_flag 산출.
- `mart_competition_daily`: 변동성/격차/엔트로피/지니 계산.
- `mart_equipment_daily`: `dw.dw_equipment`(`equipment_list='item_equipment'`)와 캐릭터 기준 조인 후 slot/item 점유율 산출.
- `mart_patch`: 정의서 규칙에 맞춘 패치 식별자/날짜/유형 컬럼 구성(원천 부재 시 TODO 없이 대체 정책을 SQL 주석으로 명시).

1. idempotent 적재 블록 구성

- 각 마트별로 대상 날짜 범위를 선삭제 후 재적재하거나, PK 기준 UPSERT 적용.
- 하나의 트랜잭션 단위로 실행 순서 고정:
  - patch → character → meta/competition → equipment.

1. 검증 쿼리 작성

- 날짜별 row count, null 비율(핵심 키), 점유율 합계(≈1.0), 경쟁지표 계산 가능성 점검 쿼리 추가.
- 5개 날짜 모두 데이터 생성 여부를 확인하는 체크 포함.

## 산출물 품질 기준

- SQL 단독 실행으로 `dm` 테이블 생성 및 5주치 적재 완료.
- 재실행 시 중복/충돌 없이 결과 일관성 유지.
- 정의서 컬럼과 실제 컬럼 간 불일치가 없고, 핵심 파생 지표(delta/share/entropy/gini)가 계산됨.

