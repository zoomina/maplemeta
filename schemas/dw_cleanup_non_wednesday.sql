-- DW 클렌징: date가 수요일이 아닌 데이터 전부 삭제
-- 대상: dw_ability, dw_equipment, dw_hexacore, dw_hyperstat, dw_rank, dw_seteffect
-- PostgreSQL DOW: 0=일요일, 1=월, 2=화, 3=수요일, 4=목, 5=금, 6=토

-- 1) 삭제 전 점검: 수요일이 아닌 데이터 건수
select 'dw_rank' as table_name, count(*) as non_wed_rows
from dw.dw_rank
where extract(dow from date) != 3
union all
select 'dw_ability', count(*)
from dw.dw_ability
where extract(dow from date::date) != 3
union all
select 'dw_equipment', count(*)
from dw.dw_equipment
where extract(dow from date::date) != 3
union all
select 'dw_hexacore', count(*)
from dw.dw_hexacore
where extract(dow from date::date) != 3
union all
select 'dw_hyperstat', count(*)
from dw.dw_hyperstat
where extract(dow from date::date) != 3
union all
select 'dw_seteffect', count(*)
from dw.dw_seteffect
where extract(dow from date::date) != 3
order by table_name;

-- 2) 삭제 실행 (트랜잭션)
begin;

delete from dw.dw_hyperstat
where extract(dow from date::date) != 3;

delete from dw.dw_seteffect
where extract(dow from date::date) != 3;

delete from dw.dw_hexacore
where extract(dow from date::date) != 3;

delete from dw.dw_equipment
where extract(dow from date::date) != 3;

delete from dw.dw_ability
where extract(dow from date::date) != 3;

delete from dw.dw_rank
where extract(dow from date) != 3;

commit;

-- 3) 삭제 후 점검: 수요일이 아닌 데이터가 0건인지 확인
select 'dw_rank' as table_name, count(*) as remaining_non_wed
from dw.dw_rank
where extract(dow from date) != 3
union all
select 'dw_ability', count(*)
from dw.dw_ability
where extract(dow from date::date) != 3
union all
select 'dw_equipment', count(*)
from dw.dw_equipment
where extract(dow from date::date) != 3
union all
select 'dw_hexacore', count(*)
from dw.dw_hexacore
where extract(dow from date::date) != 3
union all
select 'dw_hyperstat', count(*)
from dw.dw_hyperstat
where extract(dow from date::date) != 3
union all
select 'dw_seteffect', count(*)
from dw.dw_seteffect
where extract(dow from date::date) != 3
order by table_name;
