-- 1) table existence
select table_schema, table_name
from information_schema.tables
where table_schema = 'dw'
order by table_name;

-- 2) row counts by table
select 'dw_rank' as table_name, count(*) as row_count from dw.dw_rank
union all select 'dw_ability', count(*) from dw.dw_ability
union all select 'dw_equipment', count(*) from dw.dw_equipment
union all select 'dw_hexacore', count(*) from dw.dw_hexacore
union all select 'dw_hyperstat', count(*) from dw.dw_hyperstat
union all select 'dw_seteffect', count(*) from dw.dw_seteffect
order by table_name;

-- 3) date range checks
select 'dw_rank' as table_name, min(date)::text as min_date, max(date)::text as max_date from dw.dw_rank
union all select 'dw_ability', min(date)::text, max(date)::text from dw.dw_ability
union all select 'dw_equipment', min(date)::text, max(date)::text from dw.dw_equipment
union all select 'dw_hexacore', min(date)::text, max(date)::text from dw.dw_hexacore
union all select 'dw_hyperstat', min(date)::text, max(date)::text from dw.dw_hyperstat
union all select 'dw_seteffect', min(date)::text, max(date)::text from dw.dw_seteffect
order by table_name;
