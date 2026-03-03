-- score_tmp.sql: dm_balance_score, dm_shift_score ETL
-- 전제: dm_tmp.sql 적용됨, dm.version_master 존재 (없으면 create table if not exists)
-- 소스: dm_rank(segment 포함), dm_hexacore, dm_ability, dm_hyper, dm_equipment, dm_force

create table if not exists dm.version_master (
    version text primary key,
    start_date date,
    end_date date,
    type text[],
    impacted_job text[],
    patch_note text
);

-- -----------------------------------------------------------------
-- dm_balance_score: 버전×세그먼트별 엔트로피 기반 밸런스 점수
-- 소스: dm_rank (version, segment, job, character_name, type)
-- segment: 50층, 상위권. total = 0.7*50층 + 0.3*상위권
-- -----------------------------------------------------------------
create or replace function dm.refresh_balance_score(p_version text)
returns void
language plpgsql
as $$
declare
    v_score_50 smallint;
    v_score_top smallint;
begin
    if p_version is null or btrim(p_version) = '' then
        raise exception 'p_version must not be empty';
    end if;

    delete from dm.dm_balance_score where version = p_version;

    -- 50층, 상위권 segment별 balance_score
    insert into dm.dm_balance_score (
        version,
        segment,
        balance_score,
        top_job,
        top_share,
        cr3,
        top_type,
        top_type_share
    )
    with job_counts as (
        select
            version,
            segment,
            job,
            type,
            count(distinct character_name)::bigint as job_cnt
        from dm.dm_rank
        where version = p_version
          and segment in ('50층', '상위권')
        group by version, segment, job, type
    ),
    totals as (
        select
            segment,
            sum(job_cnt)::float as total
        from job_counts
        group by segment
    ),
    probs as (
        select
            jc.version,
            jc.segment,
            jc.job,
            jc.type,
            jc.job_cnt,
            jc.job_cnt::float / nullif(t.total, 0) as p
        from job_counts jc
        join totals t on t.segment = jc.segment
    ),
    entropy_raw as (
        select
            segment,
            -sum(p * ln(greatest(p, 1e-10))) as h,
            count(*) filter (where job_cnt > 0)::int as k
        from probs
        group by segment
    ),
    entropy_norm as (
        select
            segment,
            case
                when k <= 1 then 0
                else h / ln(greatest(k, 2))
            end as e
        from entropy_raw
    ),
    ranked as (
        select
            p.version,
            p.segment,
            p.job,
            p.type,
            p.p,
            row_number() over (partition by p.segment order by p.p desc) as rn
        from probs p
    ),
    top3_sum as (
        select
            segment,
            sum(p) as cr3_val
        from ranked
        where rn <= 3
        group by segment
    ),
    top_job_info as (
        select
            r.version,
            r.segment,
            r.job as top_job,
            r.p as top_share,
            r.type as top_type
        from ranked r
        where r.rn = 1
    ),
    type_share as (
        select
            p.version,
            p.segment,
            p.type,
            sum(p.p) as type_share
        from probs p
        group by p.version, p.segment, p.type
    )
    select
        p_version as version,
        en.segment,
        least(100, greatest(0, round((en.e * 100)::numeric)::smallint)) as balance_score,
        tj.top_job,
        tj.top_share::float,
        ts.cr3_val::float,
        tj.top_type,
        (select tys.type_share from type_share tys
         where tys.segment = en.segment and tys.type = tj.top_type
         limit 1)::float as top_type_share
    from entropy_norm en
    left join top_job_info tj on tj.segment = en.segment
    left join top3_sum ts on ts.segment = en.segment;

    -- total 행: 0.7*50층 + 0.3*상위권
    select balance_score into v_score_50
    from dm.dm_balance_score
    where version = p_version and segment = '50층';

    select balance_score into v_score_top
    from dm.dm_balance_score
    where version = p_version and segment = '상위권';

    insert into dm.dm_balance_score (
        version,
        segment,
        balance_score,
        top_job,
        top_share,
        cr3,
        top_type,
        top_type_share
    )
    values (
        p_version,
        'total',
        least(100, greatest(0, round(0.7 * coalesce(v_score_50, 0) + 0.3 * coalesce(v_score_top, 0))::numeric)::smallint),
        null,
        null,
        null,
        null,
        null
    );
end;
$$;

-- -----------------------------------------------------------------
-- dm_shift_score: 직업×세그먼트×버전별 Total Shift (v vs v-1)
-- 소스: dm_rank, dm_hexacore, dm_force
-- Outcome: share(로그비율)
-- Stat: hexa_level_sum Δ → z-score
-- Build: starforce Δ → z-score
-- -----------------------------------------------------------------
create or replace function dm.refresh_shift_score(p_version text)
returns void
language plpgsql
as $$
declare
    v_prev text;
begin
    if p_version is null or btrim(p_version) = '' then
        raise exception 'p_version must not be empty';
    end if;

    -- 이전 버전 조회: version_master start_date 기준, 없으면 dm_rank version 정렬
    select prev_version into v_prev from (
        select version,
            lag(version) over (order by coalesce(start_date, '1970-01-01')) as prev_version
        from dm.version_master
    ) x
    where version = p_version
    limit 1;

    if v_prev is null then
        select max(version) into v_prev
        from dm.dm_rank
        where version < p_version;
    end if;

    delete from dm.dm_shift_score where version = p_version;

    -- share(로그비율) 기반 Outcome Shift. v_prev 없으면 변화량 0.
    -- 100점 척도: 버전 내 직업별 |raw| min-max 정규화 후 0~100 clamp
    insert into dm.dm_shift_score (
        version,
        job,
        segment,
        outcome_shift,
        stat_shift,
        build_shift,
        total_shift,
        direction,
        outcome_score_100,
        stat_score_100,
        build_score_100,
        total_score_100
    )
    with curr_agg as (
        select version, segment, job, count(distinct character_name) as job_cnt
        from dm.dm_rank
        where version = p_version and segment in ('50층', '상위권')
        group by version, segment, job
    ),
    curr_seg_total as (
        select version, segment, sum(job_cnt)::float as total
        from curr_agg
        group by version, segment
    ),
    curr_share as (
        select c.version, c.segment, c.job, c.job_cnt::float / nullif(t.total, 0) as share
        from curr_agg c
        join curr_seg_total t on t.version = c.version and t.segment = c.segment
    ),
    prev_agg as (
        select version, segment, job, count(distinct character_name) as job_cnt
        from dm.dm_rank
        where version = v_prev and segment in ('50층', '상위권')
        group by version, segment, job
    ),
    prev_seg_total as (
        select version, segment, sum(job_cnt)::float as total
        from prev_agg
        group by version, segment
    ),
    prev_share as (
        select p.version, p.segment, p.job, p.job_cnt::float / nullif(t.total, 0) as share
        from prev_agg p
        join prev_seg_total t on t.version = p.version and t.segment = p.segment
    ),
    delta_log as (
        select
            c.version,
            c.segment,
            c.job,
            ln(greatest(c.share, 1e-10) / greatest(coalesce(p.share, c.share), 1e-10)) as delta_share_log
        from curr_share c
        left join prev_share p on p.segment = c.segment and p.job = c.job
    ),
    z_scored as (
        select
            version,
            segment,
            job,
            (delta_share_log - avg(delta_share_log) over (partition by version, segment)) /
                nullif(stddev(delta_share_log) over (partition by version, segment), 0) as z_share
        from delta_log
    ),
    hexa_agg as (
        select version, job, segment, sum(total_level)::float as hexa_level_sum
        from dm.dm_hexacore
        where version in (p_version, v_prev)
        group by version, job, segment
    ),
    hexa_delta as (
        select
            c.job,
            c.segment,
            coalesce(c.hexa_level_sum, 0) - coalesce(p.hexa_level_sum, 0) as delta_hexa
        from (select * from hexa_agg where version = p_version) c
        left join (select * from hexa_agg where version = v_prev) p on p.job = c.job and p.segment = c.segment
    ),
    hexa_z as (
        select
            job,
            segment,
            (delta_hexa - avg(delta_hexa) over (partition by segment)) /
                nullif(stddev(delta_hexa) over (partition by segment), 0) as z_hexa
        from hexa_delta
    ),
    force_agg as (
        select version, job, segment, avg(starforce)::float as avg_starforce
        from dm.dm_force
        where version in (p_version, v_prev) and segment in ('50층', '상위권')
        group by version, job, segment
    ),
    force_delta as (
        select
            c.job,
            c.segment,
            coalesce(c.avg_starforce, 0) - coalesce(p.avg_starforce, 0) as delta_star
        from (select * from force_agg where version = p_version) c
        left join (select * from force_agg where version = v_prev) p on p.job = c.job and p.segment = c.segment
    ),
    force_z as (
        select
            job,
            segment,
            (delta_star - avg(delta_star) over (partition by segment)) /
                nullif(stddev(delta_star) over (partition by segment), 0) as z_star
        from force_delta
    ),
    base_vals as (
        select
            z.version,
            z.segment,
            z.job,
            coalesce(z.z_share, 0) as outcome_val,
            coalesce(h.z_hexa, 0) as stat_val,
            coalesce(f.z_star, 0) as build_val
        from z_scored z
        left join hexa_z h on h.job = z.job and h.segment = z.segment
        left join force_z f on f.job = z.job and f.segment = z.segment
    ),
    with_total as (
        select
            version,
            segment,
            job,
            outcome_val,
            stat_val,
            build_val,
            (case when segment = '50층' then 0.5 * outcome_val + 0.3 * stat_val + 0.2 * build_val
                  else 0.3 * outcome_val + 0.35 * stat_val + 0.35 * build_val end) as total_val
        from base_vals
    ),
    minmax as (
        select
            version,
            segment,
            min(abs(outcome_val)) as o_min,
            max(abs(outcome_val)) as o_max,
            min(abs(stat_val)) as s_min,
            max(abs(stat_val)) as s_max,
            min(abs(build_val)) as b_min,
            max(abs(build_val)) as b_max,
            min(abs(total_val)) as t_min,
            max(abs(total_val)) as t_max
        from with_total
        group by version, segment
    )
    select
        p_version as version,
        w.job,
        w.segment,
        w.outcome_val::float as outcome_shift,
        w.stat_val::float as stat_shift,
        w.build_val::float as build_shift,
        w.total_val::float as total_shift,
        sign(w.total_val)::smallint as direction,
        coalesce(least(100, greatest(0, round(100 * (abs(w.outcome_val) - m.o_min) / nullif(m.o_max - m.o_min, 0))::numeric))::smallint, 0) as outcome_score_100,
        coalesce(least(100, greatest(0, round(100 * (abs(w.stat_val) - m.s_min) / nullif(m.s_max - m.s_min, 0))::numeric))::smallint, 0) as stat_score_100,
        coalesce(least(100, greatest(0, round(100 * (abs(w.build_val) - m.b_min) / nullif(m.b_max - m.b_min, 0))::numeric))::smallint, 0) as build_score_100,
        coalesce(least(100, greatest(0, round(100 * (abs(w.total_val) - m.t_min) / nullif(m.t_max - m.t_min, 0))::numeric))::smallint, 0) as total_score_100
    from with_total w
    join minmax m on m.version = w.version and m.segment = w.segment;
end;
$$;

-- -----------------------------------------------------------------
-- refresh_shift_balance_score: balance + shift 순차 실행
-- -----------------------------------------------------------------
create or replace function dm.refresh_shift_balance_score(p_version text)
returns void
language plpgsql
as $$
begin
    perform dm.refresh_balance_score(p_version);
    perform dm.refresh_shift_score(p_version);
end;
$$;
