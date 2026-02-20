-- Pre-cutover cleanup:
-- Remove wrongly collected Wednesday rows before 2025-06-18.
-- 대상: DW 수집 테이블 전체 (failed master 제외)

-- 1) 삭제 전 점검
select 'dw_rank' as table_name, count(*) as wed_rows
from dw.dw_rank
where date < date '2025-06-18'
  and extract(dow from date) = 3
union all
select 'stage_user_ocid', count(*)
from dw.stage_user_ocid
where date < date '2025-06-18'
  and extract(dow from date) = 3
union all
select 'dw_ability', count(*)
from dw.dw_ability
where date::date < date '2025-06-18'
  and extract(dow from date::date) = 3
union all
select 'dw_equipment', count(*)
from dw.dw_equipment
where date::date < date '2025-06-18'
  and extract(dow from date::date) = 3
union all
select 'dw_hexacore', count(*)
from dw.dw_hexacore
where date::date < date '2025-06-18'
  and extract(dow from date::date) = 3
union all
select 'dw_seteffect', count(*)
from dw.dw_seteffect
where date::date < date '2025-06-18'
  and extract(dow from date::date) = 3
union all
select 'dw_hyperstat', count(*)
from dw.dw_hyperstat
where date::date < date '2025-06-18'
  and extract(dow from date::date) = 3
order by table_name;

-- 2) 삭제 실행 (트랜잭션)
begin;

delete from dw.dw_hyperstat
where date::date < date '2025-06-18'
  and extract(dow from date::date) = 3;

delete from dw.dw_seteffect
where date::date < date '2025-06-18'
  and extract(dow from date::date) = 3;

delete from dw.dw_hexacore
where date::date < date '2025-06-18'
  and extract(dow from date::date) = 3;

delete from dw.dw_equipment
where date::date < date '2025-06-18'
  and extract(dow from date::date) = 3;

delete from dw.dw_ability
where date::date < date '2025-06-18'
  and extract(dow from date::date) = 3;

delete from dw.stage_user_ocid
where date < date '2025-06-18'
  and extract(dow from date) = 3;

delete from dw.dw_rank
where date < date '2025-06-18'
  and extract(dow from date) = 3;

commit;

-- 3) 삭제 후 점검 (수요일=0, 일요일은 유지되는지 확인)
select 'dw_rank_wed_remaining' as metric, count(*) as value
from dw.dw_rank
where date < date '2025-06-18'
  and extract(dow from date) = 3
union all
select 'dw_rank_sun_remaining', count(*)
from dw.dw_rank
where date < date '2025-06-18'
  and extract(dow from date) = 0;
