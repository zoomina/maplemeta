-- Supabase dm_balance_score, dm_shift_score 스키마 마이그레이션 (2026-03-12)
-- 실행: Supabase Dashboard > SQL Editor
-- public 스키마 사용 (Supabase 기본, dm 스키마 없음)

-- 1) dm_balance_score: segment 삭제, filter 추가
drop table if exists public.dm_balance_score cascade;
create table public.dm_balance_score (
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

-- 2) dm_shift_score: segment, filter, job (PK 변경)
drop table if exists public.dm_shift_score cascade;
create table public.dm_shift_score (
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
