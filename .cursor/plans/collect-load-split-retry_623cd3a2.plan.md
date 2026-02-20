---
name: collect-load-split-retry
overview: OCID와 character_info를 수집/적재 태스크로 분리하고, 적재 태스크에 지수 백오프 재시도를 넣어 API_KEY별 토큰 예산과 I/O 지연에 강한 구조로 전환합니다.
todos:
  - id: fix-insert-mismatch
    content: collect_api_retry_queue INSERT 컬럼/값 개수 버그 수정
    status: completed
  - id: split-ocid-tasks
    content: ocid collect/load 태스크 분리 및 데이터 전달 경로 정리
    status: completed
  - id: split-character-tasks
    content: character_info collect/load 태스크 분리 및 데이터 전달 경로 정리
    status: completed
  - id: add-retry-wrapper
    content: 적재 공통 지수 백오프 재시도 래퍼 구현(최대 300초)
    status: completed
  - id: enforce-failure-propagation
    content: 적재 실패 시 task 실패 전파로 downstream 차단
    status: completed
  - id: validate-chain-behavior
    content: API_KEY별 체인 일관성과 재시도 동작 검증
    status: completed
isProject: false
---

# 수집/적재 분리 + 재시도 계획

## 목표

- `ocid`, `character_info` 모두에서 수집과 적재를 분리해 실패 원인을 명확히 분리.
- 적재 실패 시 지수 백오프 재시도로 복구하고, 성공 전까지 downstream 진행을 차단.
- API_KEY별 체인(토큰 예산) 일관성을 유지.

## 변경 대상

- [/home/jamin/Workspace/maplemeta/dags/maplemeta_dag.py](/home/jamin/Workspace/maplemeta/dags/maplemeta_dag.py)
- [/home/jamin/Workspace/maplemeta/scripts/load_ocid.py](/home/jamin/Workspace/maplemeta/scripts/load_ocid.py)
- [/home/jamin/Workspace/maplemeta/scripts/load_character_info.py](/home/jamin/Workspace/maplemeta/scripts/load_character_info.py)
- [/home/jamin/Workspace/maplemeta/scripts/dw_load_utils.py](/home/jamin/Workspace/maplemeta/scripts/dw_load_utils.py)

## 설계

- `ocid`:
  - **collect task**: API 호출로 결과를 `stage_user_ocid`/재시도 큐 입력 버퍼 형태로 저장(적재용 페이로드 보존).
  - **load task**: `dw.dw_rank.ocid` 반영 + retry_queue 정리(성공 건 삭제/상태 전환).
- `character_info`:
  - **collect task**: endpoint 호출 결과를 적재 전 버퍼(기존 JSON/메모리 경로 정리)로 보존.
  - **load task**: DW 테이블 upsert 전담, 성공 시 버퍼/큐 정리.
- 적재 태스크는 공통 재시도 래퍼로 실행:
  - 지수 백오프: 10s → 20s → 40s ... 최대 300s
  - Airflow task 실패로 전파(성공 전 downstream 금지)

## DAG 의존성 재구성

- API_KEY_1: `ranker -> ocid_collect -> ocid_load -> character_collect -> character_load`
- API_KEY_2: 동일 체인
- API_KEY 간 순차 의존은 유지하되, 각 키 내부에서 load 실패 시 즉시 체인 중단.

## 즉시 수정(버그 픽스)

- `upsert_api_retry_queue`의 INSERT 컬럼/값 개수 불일치 수정.
- 현재 `except`에서 `True`를 반환해 성공 처리되는 경로 제거(실패 전파).

## 검증

- 강제로 적재 SQL 실패를 유도했을 때 `*_load` 태스크가 재시도(backoff) 후 실패 마킹되는지 확인.
- 동일 run에서 `ocid_load` 실패 시 `character`_*가 스케줄되지 않는지 확인.
- 적재 성공 시 큐/버퍼 정리가 이루어지는지 확인.

