---
name: json-to-db-migration
overview: 과거 JSON은 유지하고, 앞으로의 수집은 JSON 생성 없이 DB 기반 선별/적재로 전환하는 단계적 마이그레이션 계획입니다.
todos:
  - id: add-state-tables
    content: failed master와 user_ocid stage 테이블을 dw.sql에 추가
    status: completed
  - id: ranker-direct-upsert
    content: load_ranker에서 JSON 저장 제거 후 dw_rank 직접 upsert
    status: completed
  - id: ocid-db-selection
    content: load_ocid를 dw_rank+failed_master 기반 DB 선별/저장으로 전환
    status: completed
  - id: character-db-pipeline
    content: load_character_info 입력/출력을 DB로 전환하고 JSON 출력 제거
    status: completed
  - id: dag-chain-update
    content: DB direct 적재 기준으로 DAG 체인을 정리하고 load_dw 역할 재정의
    status: completed
  - id: docs-and-smoke-check
    content: README 운영 가이드 및 기본 검증 절차 갱신
    status: completed
isProject: false
---

# JSON 없는 수집 전환 계획

## 확정된 방향

- 과거에 적재된 JSON 파일은 그대로 보관.
- **앞으로 실행되는 수집부터는 JSON 파일을 생성하지 않음.**
- `ocid_failed_master`는 JSON이 아닌 **DB 테이블**로 이전.

## 핵심 설계

- 선별 기준을 파일 존재 여부가 아니라 DB 상태로 판단.
- 수집 단계별 입력/출력을 DB로 연결:
  - Ranker: `dw.dw_rank` 직접 upsert
  - OCID 선별/저장: 랭킹 테이블 조회 + `dw.stage_user_ocid` 저장
  - Character endpoint: `dw.stage_user_ocid` 조회 후 `dw.dw_*` 직접 upsert
- 기존 `load_dw_daily.py`는 과거 JSON 재적재(백필/복구) 용도로만 유지.

## 변경 대상 파일

- `[/home/jamin/Workspace/maplemeta/dags/maplemeta_dag.py](/home/jamin/Workspace/maplemeta/dags/maplemeta_dag.py)`
- `[/home/jamin/Workspace/maplemeta/scripts/load_ranker.py](/home/jamin/Workspace/maplemeta/scripts/load_ranker.py)`
- `[/home/jamin/Workspace/maplemeta/scripts/load_ocid.py](/home/jamin/Workspace/maplemeta/scripts/load_ocid.py)`
- `[/home/jamin/Workspace/maplemeta/scripts/load_character_info.py](/home/jamin/Workspace/maplemeta/scripts/load_character_info.py)`
- `[/home/jamin/Workspace/maplemeta/scripts/dw_load_utils.py](/home/jamin/Workspace/maplemeta/scripts/dw_load_utils.py)`
- `[/home/jamin/Workspace/maplemeta/schemas/dw.sql](/home/jamin/Workspace/maplemeta/schemas/dw.sql)`
- `[/home/jamin/Workspace/maplemeta/README_AIRFLOW.md](/home/jamin/Workspace/maplemeta/README_AIRFLOW.md)`

## 구현 단계

1. **상태 테이블 추가**
  - `dw.collect_failed_master`(character_name, reason, updated_at)
  - `dw.stage_user_ocid`(date, character_name, ocid, sub_job, world, level, dojang_floor)
  - PK/인덱스 정의로 idempotent upsert 보장.
2. **Ranker DB 직적재화**
  - JSON 저장 제거.
  - 수집 결과를 즉시 `dw.dw_rank` upsert.
3. **OCID 선별 DB 전환**
  - 입력: `dw.dw_rank`(date 기준) 조회.
  - 실패 제외: `dw.collect_failed_master` 조회/갱신.
  - 출력: `dw.stage_user_ocid` upsert.
4. **Character 수집 DB 전환**
  - 입력: `dw.stage_user_ocid`.
  - endpoint 결과를 기존 파서/업서트 로직 재사용해 `dw.dw_ability`, `dw.dw_equipment`, `dw.dw_hexacore`, `dw.dw_seteffect`, `dw.dw_hyperstat`로 직접 적재.
  - endpoint JSON/failed/progress 파일 생성 제거.
5. **DAG 의존성 정리**
  - DB direct 적재가 완료되면 `load_dw` 태스크를 기본 스케줄 체인에서 제외.
  - 필요 시 과거 JSON 재적재용 별도 수동 DAG/스크립트로 유지.
6. **운영 문서/검증 업데이트**
  - 환경변수 및 실행 절차를 DB 중심으로 정리.
  - 최소 검증 쿼리(날짜별 row count, endpoint completeness) 추가.

## 검증 기준

- 스케줄 1회 실행 시 `data_json`에 신규 `*.json`이 생성되지 않음.
- 동일 날짜 재실행 시 중복 없이 upsert 동작.
- `dw.dw_rank` 존재 날짜를 기준으로 OCID/character 단계가 정상 선별됨.
- 실패 캐릭터 누적/제외 로직이 DB에서 동일하게 동작.

