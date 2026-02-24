# DM 12410 백필 실행 가이드

## 파일 구성
- `schemas/dm.sql`
  - 정의서 기준 DM 스키마 + 적재 함수 `dm.refresh_dashboard_dm(...)`
- `schemas/dm_run_backfill.sql`
  - 12410 버전 고정 백필 실행 SQL
- `schemas/dm_run_dag.sql`
  - 동일 함수 호출 템플릿(운영 플로우 분리 전 참고용)

## 대상 매핑
- `dw_rank -> dm.dm_rank`
- `dw_equipment -> dm.equipment_master`
- `dw_hyperstat -> dm.hyper_master`
- `dw_equipment + dw_hexacore -> dm.dm_force`
- `dw_ability -> dm.dm_ability`
- `dw_equipment -> dm.dm_seedring`
- `dw_equipment -> dm.dm_equipment`

## 고정 실행 파라미터
- `version`: `12410`
- `update_date`: `2025-12-18`
- character(grain) 날짜: `2025-12-24`, `2025-12-31`
- aggregate 날짜: `2025-12-10`, `2025-12-17`, `2025-12-24`, `2025-12-31`

## 실행 순서
1. `schemas/dm.sql` 실행
2. `schemas/dm_run_backfill.sql` 실행

## 주요 규칙
- idempotent: 대상 `version/date`를 삭제 후 재적재
- segment(`dw_rank` 기준):
  - `50~69층 -> 50층`
  - `90층 이상 -> 상위권`
  - 단, 같은 `date+job`에서 `90층 이상` 인원이 15명 미만이면 `80층 이상`도 `상위권`
  - 그 외는 제외
- 잠재/추옵 파싱:
  - 잠재: `potential_option_1~3`
  - 추옵(요청 규칙): `additional_potential_option_1~3`
  - `+`, `=` 기준으로 앞은 label, 뒤는 value
  - 기호가 없으면 label만 적재(value는 null)

## 검증 SQL
```sql
select 'dm_rank' as table_name, count(*) as cnt
from dm.dm_rank
where version = '12410'
union all
select 'dm_force', count(*)
from dm.dm_force
where version = '12410'
union all
select 'dm_ability', count(*)
from dm.dm_ability
where version = '12410'
union all
select 'dm_seedring', count(*)
from dm.dm_seedring
where version = '12410'
union all
select 'dm_equipment', count(*)
from dm.dm_equipment
where version = '12410'
union all
select 'hyper_master', count(*)
from dm.hyper_master
where version = '12410';
```

```sql
select segment, count(*)
from dm.dm_force
where version = '12410'
group by segment
order by segment;
```
