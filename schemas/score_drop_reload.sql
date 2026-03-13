-- score_drop_reload.sql: dm_balance_score, dm_shift_score drop + 재적재
-- 전제: dm_rank, dm_hexacore, dm_force 데이터 존재. dm.version_master 존재.
-- 실행: psql -f schemas/score_drop_reload.sql

-- 1) score 테이블 drop + create (새 스키마)
drop table if exists dm.dm_balance_score cascade;
create table dm.dm_balance_score (
    version varchar(32) not null,
    filter varchar(16) not null,
    balance_score smallint not null,
    top_job varchar(64),
    top_share float,
    cr3 float,
    top_type varchar(32),
    top_type_share float,
    primary key (version, filter)
);

drop table if exists dm.dm_shift_score cascade;
create table dm.dm_shift_score (
    version varchar(32) not null,
    segment varchar(16) not null,
    filter varchar(16) not null,
    job varchar(64) not null,
    outcome_shift float,
    stat_shift float,
    build_shift float,
    total_shift float not null,
    direction smallint,
    outcome_score_100 smallint,
    stat_score_100 smallint,
    build_score_100 smallint,
    total_score_100 smallint,
    primary key (version, segment, filter, job)
);

-- 2) score_tmp.sql 함수 적용
\i schemas/score_tmp.sql

-- 3) 12405~12412 버전 순차 refresh
do $$
declare
    v text;
    versions text[] := array['12405','12406','12407','12408','12409','12410','12411','12412'];
begin
    foreach v in array versions
    loop
        perform dm.refresh_shift_balance_score(v);
        raise notice 'refresh_shift_balance_score 완료: version=%', v;
    end loop;
end;
$$;
