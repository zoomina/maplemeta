---
name: dw-dm-count-aggregate-update
overview: DM 필드정의서_0224 기준으로 기존 DW→DM 백필 계획의 골격은 유지하되, grain/집계 산출 기준 변경(rate→count)을 반영한 실행/검증 계획으로 갱신합니다.
todos:
  - id: diff-rate-to-count
    content: 정의서와 기존 계획 비교로 rate→count 변경 포인트 확정
    status: pending
  - id: update-dm-aggregate-sql
    content: dm.sql 집계 산출식을 count 기준으로 일괄 전환
    status: pending
  - id: sync-backfill-entry
    content: dm_run_backfill.sql 실행 엔트리를 새 집계 로직과 정합화
    status: pending
  - id: refresh-run-guide
    content: dm_run_guide.md에 변경 배경/검증 절차 업데이트
    status: pending
  - id: validate-count-results
    content: grain 중복/건수 정합성/샘플 대조 검증 쿼리 수행
    status: pending
isProject: false
---

# DW→DM 계획 갱신 (rate→count 반영)

## 목표

- [DM 필드정의서_0224]( /home/jamin/Workspace/maplemeta/.cursor/docs/DM 필드정의서_0224.md ) 기준으로 DM 모델을 재정렬합니다.
- 기존 계획의 큰 틀(마스터/그레인/집계 분리, 백필 흐름, segment 확장 규칙)은 유지합니다.
- 집계 지표를 `rate`에서 `count` 기준으로 전환하고, grain 산출도 count 기반 해석과 일관되게 맞춥니다.

## 기준 문서/파일

- 기존 계획: [/home/jamin/Workspace/maplemeta/.cursor/plans/dw-dm-init-backfill-airflow_31a5da22.plan.md](/home/jamin/Workspace/maplemeta/.cursor/plans/dw-dm-init-backfill-airflow_31a5da22.plan.md)
- 필드 정의서: [/home/jamin/Workspace/maplemeta/.cursor/docs/DM 필드정의서_0224.md](/home/jamin/Workspace/maplemeta/.cursor/docs/DM 필드정의서_0224.md)
- 반영 대상 스키마: [/home/jamin/Workspace/maplemeta/schemas/dm.sql](/home/jamin/Workspace/maplemeta/schemas/dm.sql)
- 실행 SQL/가이드: [/home/jamin/Workspace/maplemeta/schemas/dm_run_backfill.sql](/home/jamin/Workspace/maplemeta/schemas/dm_run_backfill.sql), [/home/jamin/Workspace/maplemeta/schemas/dm_run_guide.md](/home/jamin/Workspace/maplemeta/schemas/dm_run_guide.md)

## 핵심 변경사항 정리

- 집계 테이블(`dm_ability`, `dm_seedring`, `dm_equipment`)의 산출 컬럼을 채택 비율(`rate`)이 아닌 채택 건수(`count`)로 고정합니다.
- 집계 grain은 정의서 기준 `(date, job, segment + 항목 차원)`을 유지하고, 계산식만 count 집계로 전환합니다.
- 기존 계획의 `update_date` 기준 pre/post 분리, segment(90+<15 시 80+ 확장), `coalesce(nullif(sub_job,''), job)` 기준은 그대로 유지합니다.

## 구현 계획

- `dm.sql`에서 집계 INSERT/CTE를 점검하여 분모 기반 비율 계산 로직(`rate = numerator/denominator`)을 제거합니다.
- 동일 grouping key 기준으로 `count(*)` 또는 `count(distinct character_name)` 중 정의서 의도(채택 카운트)에 맞는 단일 기준을 확정하여 모든 집계 테이블에 일관 적용합니다.
- `dm_force`, `dm_rank` 등 grain(character) 적재 로직에서 집계 연계 전처리 키(job/segment/date) 정합성을 재검증해 count 결과 왜곡을 방지합니다.
- `dm_run_backfill.sql` 실행 순서를 기존과 동일하게 유지하되, 집계 재생성 단계가 count 기준 SQL을 호출하도록 정리합니다.
- `dm_run_guide.md`에 변경 요약(왜 rate→count로 변경됐는지, 검증 포인트)을 명시합니다.

## 검증 계획

- 집계 테이블별로 `(version, date, job, segment, 항목)` grain 중복 여부 검사.
- count 값 음수/NULL/비정상 대값 검증 및 날짜별 분포 비교.
- 샘플 직업/세그먼트를 선정해 source(DW) raw row 수와 DM count 결과를 대조.
- 이전 rate 기반 결과와 비교 시, 분모 의존 편차가 제거되고 단순 채택량 추세가 재현되는지 확인.

## 산출물

- rate→count 반영된 DM 스키마/함수 정의
- 백필 실행 SQL 갱신본
- 운영/검증 가이드 갱신본

