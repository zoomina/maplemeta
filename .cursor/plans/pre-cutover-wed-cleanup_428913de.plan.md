---
name: pre-cutover-wed-cleanup
overview: 2025-06-18 이전에 수요일 기준으로 잘못 적재된 DW 수집 데이터만 정리하고, DB 직적재 전환 후 약해진 스킵 판정을 최소 수정으로 복원합니다.
todos:
  - id: prepare-delete-sql
    content: DW 수집 테이블 전체 대상의 pre-cutover 수요일 오적재 삭제 SQL 작성
    status: completed
  - id: harden-minimal-skip
    content: maplemeta_dag.py의 날짜 정규화/스킵 판정 최소 보강
    status: completed
  - id: validate-before-after
    content: 삭제 전후 카운트 및 DAG 스킵 동작 검증
    status: completed
isProject: false
---

# 6/18 이전 수요일 오적재 정리 + 스킵 복원

## 목표

- 잘못 들어간 집계일만 제거: `date < '2025-06-18'` 이면서 수요일(`dow=3`)인 데이터.
- 스킵 로직 최소 수정으로 복원: 이미 적재된 구간은 API 호출 없이 건너뛰고 다음 미적재 주차로 이동.

## 수정 파일

- `[/home/jamin/Workspace/maplemeta/dags/maplemeta_dag.py](/home/jamin/Workspace/maplemeta/dags/maplemeta_dag.py)`
- (검증 SQL 보관 시) `[/home/jamin/Workspace/maplemeta/schemas/dm_run_backfill.sql](/home/jamin/Workspace/maplemeta/schemas/dm_run_backfill.sql)` 또는 신규 운영 SQL 파일

## 구현 계획

1. **오적재 삭제 SQL 작성 (DW 수집 테이블 전체)**
  - 대상 테이블:
    - `dw.dw_rank`
    - `dw.stage_user_ocid`
    - `dw.dw_ability`
    - `dw.dw_equipment`
    - `dw.dw_hexacore`
    - `dw.dw_seteffect`
    - `dw.dw_hyperstat`
  - 공통 조건:
    - 날짜 기준이 `2025-06-18` 이전
    - 날짜의 요일이 수요일 (`extract(dow from ...) = 3`)
  - `dw.collect_failed_master`는 날짜 컬럼이 없는 실패 이력 테이블이므로 **삭제 대상에서 제외**.
2. **스킵 로직 최소 보강 (`maplemeta_dag.py`)**
  - 주차 탐색 시 매 주차마다 컷오버 정책(이전=일요일, 이후=수요일)으로 날짜 정규화.
  - `MAPLEMETA_DATE`가 설정된 경우 기준일 우선 적용.
  - 동일 날짜 중복 검사 방지(정규화 과정에서 발생 가능한 중복 방지).
  - 기존 구조(`get_first_missing_date_backwards` + `check_data_exists`)는 유지.
3. **안전 검증 쿼리 추가/실행 가이드**
  - 삭제 전:
    - 수요일 오적재 후보 건수 집계
  - 삭제 후:
    - `2025-06-18` 이전 수요일 데이터 0건 확인
    - `2025-06-18` 이전 일요일 데이터는 남아있는지 확인
  - 재실행 후:
    - 최신 이미 적재 주차는 스킵되고, 가장 가까운 미적재 주차로 진행되는지 DAG 로그 확인

## 실행 순서

1. 삭제 전 카운트 확인 SQL 실행
2. 오적재 삭제 SQL 실행(트랜잭션)
3. DAG 스킵 로직 최소 수정 반영
4. DAG 단건 트리거로 `load_ranker_api_key_1` 로그 확인
5. 필요 시 전체 DAG 재실행

## 검증 포인트

- `2025-06-18` 이전 수요일 데이터가 DW 수집 테이블에서 제거됨
- 컷오버 이전 구간 재수집 시 일요일만 타겟으로 선택됨
- 이미 적재된 최신 집계일(`2026-02-18` 등)은 API 호출 없이 스킵됨

