-- DW 데이터 정리: 2025-07-22 이전 날짜 데이터 전부 삭제
-- 대상: dw_rank, stage_user_ocid, dw_ability, dw_equipment, dw_hexacore, dw_seteffect, dw_hyperstat
-- 제외: collect_failed_master(날짜 없음), dw_update(공지 메타), collect_api_retry_queue(별도 처리 시 아래 주석 참고)

-- 1) 삭제 전 점검: 테이블별 삭제 대상 건수
select 'dw_rank' as table_name, count(*) as rows_before_cutoff
from dw.dw_rank
where date < date '2025-07-22'
union all
select 'stage_user_ocid', count(*)
from dw.stage_user_ocid
where date < date '2025-07-22'
union all
select 'dw_ability', count(*)
from dw.dw_ability
where date::date < date '2025-07-22'
union all
select 'dw_equipment', count(*)
from dw.dw_equipment
where date::date < date '2025-07-22'
union all
select 'dw_hexacore', count(*)
from dw.dw_hexacore
where date::date < date '2025-07-22'
union all
select 'dw_seteffect', count(*)
from dw.dw_seteffect
where date::date < date '2025-07-22'
union all
select 'dw_hyperstat', count(*)
from dw.dw_hyperstat
where date::date < date '2025-07-22'
order by table_name;

-- 2) 삭제 실행 (트랜잭션)
begin;

delete from dw.dw_hyperstat
where date::date < date '2025-07-22';

delete from dw.dw_seteffect
where date::date < date '2025-07-22';

delete from dw.dw_hexacore
where date::date < date '2025-07-22';

delete from dw.dw_equipment
where date::date < date '2025-07-22';

delete from dw.dw_ability
where date::date < date '2025-07-22';

delete from dw.stage_user_ocid
where date < date '2025-07-22';

delete from dw.dw_rank
where date < date '2025-07-22';

-- (선택) 재시도 큐에서 해당 구간 제거할 경우 아래 실행
-- delete from dw.collect_api_retry_queue
-- where target_date < date '2025-07-22';

commit;

-- 3) 삭제 후 점검: 2025-07-22 미만 데이터가 0건인지 확인
select 'dw_rank' as table_name, count(*) as remaining
from dw.dw_rank
where date < date '2025-07-22'
union all
select 'stage_user_ocid', count(*)
from dw.stage_user_ocid
where date < date '2025-07-22'
union all
select 'dw_ability', count(*)
from dw.dw_ability
where date::date < date '2025-07-22'
union all
select 'dw_equipment', count(*)
from dw.dw_equipment
where date::date < date '2025-07-22'
union all
select 'dw_hexacore', count(*)
from dw.dw_hexacore
where date::date < date '2025-07-22'
union all
select 'dw_seteffect', count(*)
from dw.dw_seteffect
where date::date < date '2025-07-22'
union all
select 'dw_hyperstat', count(*)
from dw.dw_hyperstat
where date::date < date '2025-07-22'
order by table_name;
