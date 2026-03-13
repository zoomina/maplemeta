# DM TMP 12409/12410 실행 가이드

## 변경 로그

### 2026-03-12
- **dm_balance_score**: segment 삭제, filter(전체/전사/궁수/마법사/도적/해적) 추가. 전체 모수(50층+상위권) 사용, 가중평균 제거.
- **dm_shift_score**: segment(50층/상위권/전체), filter(전체~해적), job(개별+aggregate) 추가. 제논=도적+해적, 캐논슈터=캐논 마스터 통합.
- **score_drop_reload.sql** 신규: drop+create+refresh (12405~12412). `python scripts/backfill_dw_to_dm.py --versions 12405,12406,...,12412` 대안.
- **Supabase 마이그레이션**: `python scripts/supabase_score_migrate.py` (로컬 일괄 실행). 전제: .env에 SUPABASE_DB_URL 설정.

### 2026-02-25
- 집계 기준 변경: `timing + rate` -> `date + count`
  - 대상: `dm.dm_ability`, `dm.dm_seedring`, `dm.dm_equipment`
  - 비율 계산식 제거, 채택 건수(`count`) 직접 적재로 전환
- 날짜 컬럼명 통일: DM 전 테이블의 날짜 컬럼명을 `dt`에서 `date`로 통일
- `dm_force`에서 하이퍼 관련 컬럼 제거
  - 제거: `hyper1_label`, `hyper1_value`, `hyper2_label`, `hyper2_value`, `hyper3_label`, `hyper3_value`
- `dm.dm_hyper` 신규 생성
  - 소스: `dw.dw_hyperstat`
  - preset 선택 규칙: `보공(level)` 내림차순 -> `remain_point` 오름차순 -> `preset_no` 오름차순
  - 선정 사유: 무릉은 보스 몬스터가 등장하는 구조여서 하이퍼 preset 선정 시 보공을 최우선으로 반영
  - 선택된 preset의 각 `*_level` 값을 `dm_hyper` 컬럼으로 적재
- `dm_hyper` 적재 대상 제한
  - `dw_equipment(item_equipment)` 데이터가 존재하는 캐릭터만 적재
  - 캐릭터 상세 미수집 구간에서 하이퍼 값이 `0`으로 대량 적재되는 문제 방지
- 세트효과 제외 규칙 추가(`dm_equipment`)
  - `1개` 세트효과 제외
  - 세트명이 `쁘띠`로 시작하는 항목 제외
  - `__MISSING__` 제외
- `hyper_master`에 `date` 컬럼 추가
  - PK: `(version, date, job)` 기준으로 일별 top3 하이퍼 통계 관리
- 버전 관리 방식 변경: pre/post 타이밍 구분 제거 후 날짜별 버전 고정
  - `2025-12-10`, `2025-12-17` -> `12409`
  - `2025-12-24`, `2025-12-31` -> `12410`
- 적재 범위 확장: 기존 post 2주 중심에서 pre 2주 포함 4주로 확장
  - `dm_rank`, `dm_force`, `hyper_master` 모두 pre 2주 추가 적재
- 함수 시그니처 변경: `dm.refresh_dashboard_dm(...)`에서 `p_update_date` 제거
  - 집계 분기 기준을 날짜별 `version` 실행으로 대체
- 백필 스크립트 변경: `schemas/dm_run_backfill.sql`을 2회 호출 구조로 분리
  - 1차: `12409` + (`2025-12-10`, `2025-12-17`)
  - 2차: `12410` + (`2025-12-24`, `2025-12-31`)
- 안전한 재실행(idempotent) 유지
  - `dm_rank`, `dm_force`: `version + p_character_dates` 단위 삭제 후 재적재
  - 집계 3개 테이블: `version + p_agg_dates` 단위 삭제 후 재적재

## 파일 구성
- `schemas/dm_tmp.sql`
  - 테스트/실험 단계용 DM 스키마 + 적재 함수 `dm.refresh_dashboard_dm(...)`
  - dm_rank segment, dm_shift_score, dm_balance_score DDL 포함
- `schemas/score_tmp.sql`
  - dm_balance_score, dm_shift_score ETL 함수 (`dm.refresh_shift_balance_score`)
- `scripts/backfill_dw_to_dm.py`
  - 전체 백필: DW 완료 날짜 조회 → version별 refresh_dashboard_dm + refresh_shift_balance_score
  - `--shift-score-only`: shift_score만 전체 백필

## 대상 매핑
- `dw_rank -> dm.dm_rank`
- `dw_equipment -> dm.equipment_master`
- `dw_hyperstat -> dm.hyper_master`
- `dw_equipment + dw_hexacore -> dm.dm_force`
- `dw_hyperstat -> dm.dm_hyper`
- `dw_ability -> dm.dm_ability`
- `dw_equipment -> dm.dm_seedring`
- `dw_equipment -> dm.dm_equipment`
- `dw_hexacore -> dm.dm_hexacore` (version, date, job, segment별 집계)

## 고정 실행 파라미터
- 버전 매핑:
  - `12409`: `2025-12-10`, `2025-12-17`
  - `12410`: `2025-12-24`, `2025-12-31`
- character(grain) 날짜: 각 버전별 동일한 2주
- aggregate 날짜: 각 버전별 동일한 2주

## 실행 순서
1. `schemas/dm_tmp.sql` 실행
2. `schemas/score_tmp.sql` 실행 (스키마/함수)
3. `scripts/backfill_dw_to_dm.py` 실행 (refresh_dashboard_dm + refresh_shift_balance_score)

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
- 집계(`dm_ability`, `dm_seedring`, `dm_equipment`)는 `timing/rate` 대신 `date/count`를 사용
- 하이퍼 preset 선택 우선순위(`dm_hyper`, `hyper_master` 공통):
  - 1순위: `보공(level)` 높은 순
  - 2순위: `remain_point` 적은 순
  - 3순위: `preset_no` 작은 순
  - 무릉 특성상 보스 몬스터 대응이 핵심이므로 보공 기준을 최우선으로 둠
- `dm_hyper`는 `dw_equipment(item_equipment)` 존재 캐릭터만 적재
- `dm_equipment`의 세트효과는 `1개`, `쁘띠*`, `__MISSING__` 제외

## 검증 SQL
```sql
select 'dm_rank' as table_name, count(*) as cnt
from dm.dm_rank
where version in ('12409', '12410')
union all
select 'dm_force', count(*)
from dm.dm_force
where version in ('12409', '12410')
union all
select 'dm_hyper', count(*)
from dm.dm_hyper
where version in ('12409', '12410')
union all
select 'dm_ability', count(*)
from dm.dm_ability
where version in ('12409', '12410')
union all
select 'dm_seedring', count(*)
from dm.dm_seedring
where version in ('12409', '12410')
union all
select 'dm_equipment', count(*)
from dm.dm_equipment
where version in ('12409', '12410')
union all
select 'dm_hexacore', count(*)
from dm.dm_hexacore
where version in ('12409', '12410')
union all
select 'hyper_master', count(*)
from dm.hyper_master
where version in ('12409', '12410');
```

```sql
select version, segment, count(*)
from dm.dm_force
where version in ('12409', '12410')
group by version, segment
order by version, segment;
```

```sql
select version, date, count(*) as row_cnt
from dm.dm_ability
where version in ('12409', '12410')
group by version, date
order by version, date;
```
