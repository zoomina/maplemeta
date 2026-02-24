---
name: dw-dm-init-backfill-airflow
overview: 정의서 기준 DM 테이블로 재구성하고, 12410 버전(12/18 기준) 백필 SQL만 우선 확정해 스키마/로직 검증을 완료합니다.
todos:
  - id: redesign-dm-schema
    content: 정의서 기준 DM 테이블 DDL 및 refresh 함수(마스터/그레인/집계) 재작성
    status: completed
  - id: implement-option-segment-rules
    content: 잠재/추옵 파싱 규칙과 segment(90+<15 시 80+ 확장) SQL 로직 반영
    status: completed
  - id: wire-backfill-run-sql
    content: 12/18(12410) 기준 대상일 백필 SQL 작성 및 실행 엔트리 정리
    status: completed
  - id: add-validation-queries
    content: 적재 건수/무결성/샘플 파싱 검증 쿼리 및 실행 가이드 업데이트
    status: completed
isProject: false
---

# DW→DM 12410 백필 계획

## 목표

- 정의서 기준 DM 구조(`master + grain:character + 집계`)로 재구성
- 12/18 업데이트(버전 `12410`) 기준 대상일 데이터 백필
  - grain(character): `2025-12-24`, `2025-12-31`
  - 집계: `2025-12-10`, `2025-12-17`, `2025-12-24`, `2025-12-31`
- 적재 정상 여부를 SQL 체크 쿼리로 검증

## 변경 파일(예정)

- `[/home/jamin/Workspace/maplemeta/schemas/dm.sql](/home/jamin/Workspace/maplemeta/schemas/dm.sql)` 
- `[/home/jamin/Workspace/maplemeta/schemas/dm_run_backfill.sql](/home/jamin/Workspace/maplemeta/schemas/dm_run_backfill.sql)` 
- `[/home/jamin/Workspace/maplemeta/schemas/dm_run_guide.md](/home/jamin/Workspace/maplemeta/schemas/dm_run_guide.md)` 

## 구현 방식

- `dm.sql`에 정의서 기준 테이블 DDL + 공통 적재 함수(예: `dm.refresh_dashboard_dm(...)`) 작성
- 함수 내부는 **idempotent**(대상일 선삭제 후 재적재)
- 테이블 타겟 매핑을 아래로 고정
  - `dw_rank -> dm_rank`
  - `dw_equipment -> equipment_master`
  - `dw_hyperstat -> hyper_master`
  - `dw_equipment + dw_hexacore -> dm_force`
  - `dw_ability -> dm_ability`
  - `dw_equipment -> dm_seedring`
  - `dw_equipment -> dm_equipment`
- 마스터/그레인/집계를 분리 적재
  - `character_master`: CSV 기준 정적 적재
  - `equipment_master`: `dw.dw_equipment`에서 `distinct item_name` 추출 후 직업군/이미지 매핑(매핑 불가값은 null 허용)
  - `hyper_master`: 버전별/직업군별로 하이퍼 스탯 level 합 상위 3개
- segment 규칙 반영(모든 DM 산출에서 `dw_rank` 기준)
  - 기본: 50~69=`50층`, 90+=`상위권`, 그 외 제외
  - 단, 해당 직업군 90+ 인원이 15명 미만이면 80+를 `상위권`으로 확장
- 잠재/추옵 파싱 규칙 반영
  - 잠재: `potential_option_1~3`
  - 추옵: `additional_potential_option_1~3`
  - `+` 또는 `=` 기준 분리: 앞=label, 뒤=value
  - 기호 없으면 label만 저장, value=null
  - line 수는 null 아닌 개수
- `version` 값은 이번 샘플 구간에서 `12410` 사용
  - 이후 업데이트 DB 연동은 별도 플로우에서 확장

## 반영 완료 사항

- `character_master` 색상코드 반영
  - `[/home/jamin/Workspace/maplemeta/.cursor/docs/character_color.md](/home/jamin/Workspace/maplemeta/.cursor/docs/character_color.md)` 기준으로 직업별 HEX 매핑
- `job` 기준 일괄 변경
  - 모든 집계에서 `coalesce(nullif(sub_job, ''), job)` 사용
- `dm_force` 수집 대상 제한
  - `dw_equipment`(`item_equipment`) 미수집 캐릭터는 제외
- `dm_force` 수치 null 처리
  - `hexa_level`, `starforce`, `hyper1_value~hyper3_value`를 `0`으로 보정
- `dm_ability` 소스/레이블 규칙 변경
  - `ability_set`은 `preset1`, `preset2`, `preset3`만 사용 (`current` 제외)
  - 같은 preset 내 옵션 텍스트 존재 여부로 `type` 산정
    - `"보스 몬스터"` 포함 시 `boss`
    - `"메소 획득량"`, `"일반 몬스터"`, `"아이템 드롭률"` 포함 시 `field`
    - 둘 다 없으면 `other`
- `dm_ability` 텍스트 정규화 보정
  - 수치/기호만 제거하고 텍스트는 유지
  - 예: `보스몬스터 공격 시 데미지 19% 증가` -> `보스몬스터 공격 시 데미지 증가`

## 상세 의사코드

```text
function split_option_text(raw_text):
  if raw_text is null or blank: return (null, null)
  if contains '+' or '=':
    pos = first index of '+' or '='
    label = trim(left(raw_text, pos-1))
    value = trim(substring(raw_text, pos))
    return (label, value)
  return (trim(raw_text), null)

function build_character_master_from_csv(csv_rows):
  map columns (직업 이름, 직업군, 계열, 이미지) -> (job, group, type, img)
  upsert into dm.character_master by job

function build_equipment_master_from_dw(target_dates):
  src = select distinct item_name, item_icon from dw.dw_equipment where date in target_dates and equipment_list='item_equipment'
  infer job/type by joining character/equipment usage distribution
  upsert into dm.equipment_master(equipment_name, job, type, img, set)

function build_hyper_master(version, agg_dates):
  unpivot dw.dw_hyperstat *_level columns
  sum level by (job_group, stat_name)
  rank by total_level desc
  keep top 3 per job_group
  upsert into dm.hyper_master(version, job, hyper1, hyper2, hyper3, img)

function load_dm_rank(version, dates_character):
  src rank rows for dates_character
  enrich with character_master (group/type)
  compute sec_per_floor = record_sec / nullif(floor,0)
  insert into dm_rank(version,date,character_name,floor,clear_time,sec_per_floor,job,group,type)

function load_dm_force(version, dates_character):
  source rank + equipment + hyperstat + hexacore
  apply segment rule per (date, job)
  compute hexa_level_sum, starforce_sum
  attach job-level hyper top3 from hyper_master
  parse potential/additional options with split_option_text
  count non-null lines
  insert into dm_force

function load_aggregates(version, dates_agg):
  dm_ability: from dw.dw_ability + dw_rank segment -> rate by (job,timing,segment)
  dm_seedring: from dw.dw_equipment ring slots -> rate
  dm_equipment: from dw.dw_equipment(item/set/sub weapon) -> rate
  timing = pre/post by update_date(2025-12-18)

function refresh_dashboard_dm(mode, dates_character, dates_agg, version, update_date):
  delete target partitions for selected version/dates
  build masters
  load grain tables
  load aggregate tables
  return row counts by table
```

## 검증 계획

- 테이블별 건수/중복키/널 비율 체크 쿼리 추가
- 샘플 검증
  - `dm_force`의 잠재/추옵 label/value 파싱 샘플 20건
  - segment 확장 규칙(90+<15일 때 80+ 상위권) 직업군별 확인
  - `hyper_master` top3가 버전/직업군별 3개인지 확인
- pre/post 분포 검증
  - 집계일 4개(12/10,12/17,12/24,12/31)가 update_date 기준 올바르게 분류되는지 확인

