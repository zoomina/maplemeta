-- score_tmp.sql: dm_balance_score, dm_shift_score ETL
-- 전제: dm_tmp.sql 적용됨, dm.version_master 존재 (없으면 create table if not exists)
-- 소스: dm_rank(segment 포함), dm_hexacore, dm_ability, dm_hyper, dm_equipment, dm_force

-- -----------------------------------------------------------------
-- 직업 매핑 함수
-- -----------------------------------------------------------------
create or replace function dm.normalize_job_for_score(p_job text)
returns text
language sql
immutable
as $$
    select case
        when p_job in ('캐논 마스터', '캐논슈터') then '캐논슈터'
        else coalesce(p_job, '')
    end;
$$;

create or replace function dm.job_matches_filter(p_job text, p_type text, p_filter text)
returns boolean
language sql
immutable
as $$
    select case p_filter
        when '전체' then true
        when '전사' then coalesce(p_type, '') like '%전사%'
        when '궁수' then coalesce(p_type, '') like '%궁수%'
        when '마법사' then coalesce(p_type, '') like '%마법사%'
        when '도적' then coalesce(p_type, '') like '%도적%' or p_job = '제논'
        when '해적' then coalesce(p_type, '') like '%해적%' or p_job = '제논'
        else false
    end;
$$;

create table if not exists dm.version_master (
    version text primary key,
    start_date date,
    end_date date,
    type text[],
    impacted_job text[],
    patch_note text
);

-- -----------------------------------------------------------------
-- dm_balance_score: 버전×필터별 엔트로피 기반 밸런스 점수
-- 소스: dm_rank (version, job, character_name, type). segment 없음, 전체 모수(50층+상위권) 사용.
-- filter: 전체, 전사, 궁수, 마법사, 도적, 해적. 제논=도적+해적, 캐논슈터=캐논 마스터.
-- -----------------------------------------------------------------
create or replace function dm.refresh_balance_score(p_version text)
returns void
language plpgsql
as $$
begin
    if p_version is null or btrim(p_version) = '' then
        raise exception 'p_version must not be empty';
    end if;

    delete from dm.dm_balance_score where version = p_version;

    insert into dm.dm_balance_score (
        version,
        filter,
        balance_score,
        top_job,
        top_share,
        cr3,
        top_type,
        top_type_share
    )
    with filters as (
        select unnest(array['전체', '전사', '궁수', '마법사', '도적', '해적']) as filter
    ),
    rank_base as (
        select
            dm.normalize_job_for_score(r.job) as job,
            r.type,
            r.character_name
        from dm.dm_rank r
        where r.version = p_version
          and r.segment in ('50층', '상위권')
    ),
    filtered as (
        select
            f.filter,
            rb.job,
            rb.type,
            rb.character_name
        from rank_base rb
        cross join filters f
        where dm.job_matches_filter(rb.job, rb.type, f.filter)
    ),
    job_counts as (
        select
            filter,
            job,
            type,
            count(distinct character_name)::bigint as job_cnt
        from filtered
        group by filter, job, type
    ),
    totals as (
        select filter, sum(job_cnt)::float as total
        from job_counts
        group by filter
    ),
    probs as (
        select
            jc.filter,
            jc.job,
            jc.type,
            jc.job_cnt,
            jc.job_cnt::float / nullif(t.total, 0) as p
        from job_counts jc
        join totals t on t.filter = jc.filter
    ),
    entropy_raw as (
        select
            filter,
            -sum(p * ln(greatest(p, 1e-10))) as h,
            count(*) filter (where job_cnt > 0)::int as k
        from probs
        group by filter
    ),
    entropy_norm as (
        select
            filter,
            case when k <= 1 then 0 else h / ln(greatest(k, 2)) end as e
        from entropy_raw
    ),
    ranked as (
        select
            filter,
            job,
            type,
            p,
            row_number() over (partition by filter order by p desc) as rn
        from probs
    ),
    top3_sum as (
        select filter, sum(p) as cr3_val
        from ranked
        where rn <= 3
        group by filter
    ),
    top_job_info as (
        select filter, job as top_job, p as top_share, type as top_type
        from ranked
        where rn = 1
    ),
    type_share as (
        select filter, type, sum(p) as type_share
        from probs
        group by filter, type
    )
    select
        p_version as version,
        en.filter,
        least(100, greatest(0, round((en.e * 100)::numeric)::smallint))::smallint as balance_score,
        tj.top_job,
        tj.top_share::float,
        ts.cr3_val::float,
        tj.top_type,
        (select tys.type_share from type_share tys
         where tys.filter = en.filter and tys.type = tj.top_type
         limit 1)::float as top_type_share
    from entropy_norm en
    left join top_job_info tj on tj.filter = en.filter
    left join top3_sum ts on ts.filter = en.filter;
end;
$$;

-- -----------------------------------------------------------------
-- dm_shift_score: 직업×세그먼트×필터×버전별 Total Shift (v vs v-1)
-- segment: 50층, 상위권, 전체(50층+상위권 union). filter: 전체, 전사, 궁수, 마법사, 도적, 해적.
-- job: 개별 직업 또는 aggregate(전체~해적). Outcome: share(로그비율), Stat: hexa Δ, Build: starforce Δ.
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

    insert into dm.dm_shift_score (
        version,
        segment,
        filter,
        job,
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
    with seg_filt as (
        select seg, filt from (
            values ('50층','전체'),('50층','전사'),('50층','궁수'),('50층','마법사'),('50층','도적'),('50층','해적'),
                   ('상위권','전체'),('상위권','전사'),('상위권','궁수'),('상위권','마법사'),('상위권','도적'),('상위권','해적'),
                   ('전체','전체'),('전체','전사'),('전체','궁수'),('전체','마법사'),('전체','도적'),('전체','해적')
        ) t(seg, filt)
    ),
    rank_curr_raw as (
        select segment as seg, dm.normalize_job_for_score(job) as job, type, character_name
        from dm.dm_rank
        where version = p_version and segment in ('50층', '상위권')
    ),
    rank_curr_all as (
        select seg, job, type, character_name from rank_curr_raw
        union all
        select '전체' as seg, job, type, character_name from rank_curr_raw
    ),
    rank_prev_raw as (
        select segment as seg, dm.normalize_job_for_score(job) as job, type, character_name
        from dm.dm_rank
        where version = v_prev and segment in ('50층', '상위권')
    ),
    rank_prev_all as (
        select seg, job, type, character_name from rank_prev_raw
        union all
        select '전체' as seg, job, type, character_name from rank_prev_raw
    ),
    curr_filtered as (
        select sf.seg, sf.filt, rc.job, count(distinct rc.character_name)::bigint as job_cnt
        from rank_curr_all rc
        join seg_filt sf on sf.seg = rc.seg and dm.job_matches_filter(rc.job, rc.type, sf.filt)
        group by sf.seg, sf.filt, rc.job
    ),
    curr_totals as (
        select seg, filt, sum(job_cnt)::float as total
        from curr_filtered
        group by seg, filt
    ),
    curr_share as (
        select c.seg, c.filt, c.job, c.job_cnt::float / nullif(t.total, 0) as share
        from curr_filtered c
        join curr_totals t on t.seg = c.seg and t.filt = c.filt
    ),
    prev_filtered as (
        select sf.seg, sf.filt, rp.job, count(distinct rp.character_name)::bigint as job_cnt
        from rank_prev_all rp
        join seg_filt sf on sf.seg = rp.seg and dm.job_matches_filter(rp.job, rp.type, sf.filt)
        group by sf.seg, sf.filt, rp.job
    ),
    prev_totals as (
        select seg, filt, sum(job_cnt)::float as total
        from prev_filtered
        group by seg, filt
    ),
    prev_share as (
        select p.seg, p.filt, p.job, p.job_cnt::float / nullif(t.total, 0) as share
        from prev_filtered p
        join prev_totals t on t.seg = p.seg and t.filt = p.filt
    ),
    delta_log as (
        select
            c.seg,
            c.filt,
            c.job,
            ln(greatest(c.share, 1e-10) / greatest(coalesce(p.share, c.share), 1e-10)) as delta_share_log
        from curr_share c
        left join prev_share p on p.seg = c.seg and p.filt = c.filt and p.job = c.job
    ),
    z_scored as (
        select
            seg,
            filt,
            job,
            (delta_share_log - avg(delta_share_log) over (partition by seg, filt)) /
                nullif(stddev(delta_share_log) over (partition by seg, filt), 0) as z_share
        from delta_log
    ),
    hexa_curr as (
        select segment as seg, dm.normalize_job_for_score(job) as job, sum(total_level)::float as hexa_level_sum
        from dm.dm_hexacore
        where version = p_version and segment in ('50층', '상위권')
        group by segment, job
    ),
    hexa_curr_all as (
        select seg, job, hexa_level_sum from hexa_curr
        union all
        select '전체' as seg, job, sum(hexa_level_sum)::float from hexa_curr group by job
    ),
    hexa_prev as (
        select segment as seg, dm.normalize_job_for_score(job) as job, sum(total_level)::float as hexa_level_sum
        from dm.dm_hexacore
        where version = v_prev and segment in ('50층', '상위권')
        group by segment, job
    ),
    hexa_prev_all as (
        select seg, job, hexa_level_sum from hexa_prev
        union all
        select '전체' as seg, job, sum(hexa_level_sum)::float from hexa_prev group by job
    ),
    hexa_delta as (
        select
            c.seg,
            c.job,
            coalesce(c.hexa_level_sum, 0) - coalesce(p.hexa_level_sum, 0) as delta_hexa
        from hexa_curr_all c
        left join hexa_prev_all p on p.seg = c.seg and p.job = c.job
    ),
    hexa_z as (
        select
            seg,
            job,
            (delta_hexa - avg(delta_hexa) over (partition by seg)) /
                nullif(stddev(delta_hexa) over (partition by seg), 0) as z_hexa
        from hexa_delta
    ),
    force_curr as (
        select segment as seg, dm.normalize_job_for_score(job) as job, avg(starforce)::float as avg_starforce
        from dm.dm_force
        where version = p_version and segment in ('50층', '상위권')
        group by segment, job
    ),
    force_curr_all as (
        select seg, job, avg_starforce from force_curr
        union all
        select '전체' as seg, job, avg(avg_starforce)::float from force_curr group by job
    ),
    force_prev as (
        select segment as seg, dm.normalize_job_for_score(job) as job, avg(starforce)::float as avg_starforce
        from dm.dm_force
        where version = v_prev and segment in ('50층', '상위권')
        group by segment, job
    ),
    force_prev_all as (
        select seg, job, avg_starforce from force_prev
        union all
        select '전체' as seg, job, avg(avg_starforce)::float from force_prev group by job
    ),
    force_delta as (
        select
            c.seg,
            c.job,
            coalesce(c.avg_starforce, 0) - coalesce(p.avg_starforce, 0) as delta_star
        from force_curr_all c
        left join force_prev_all p on p.seg = c.seg and p.job = c.job
    ),
    force_z as (
        select
            seg,
            job,
            (delta_star - avg(delta_star) over (partition by seg)) /
                nullif(stddev(delta_star) over (partition by seg), 0) as z_star
        from force_delta
    ),
    base_vals as (
        select
            z.seg,
            z.filt,
            z.job,
            coalesce(z.z_share, 0) as outcome_val,
            coalesce(h.z_hexa, 0) as stat_val,
            coalesce(f.z_star, 0) as build_val
        from z_scored z
        left join hexa_z h on h.seg = z.seg and h.job = z.job
        left join force_z f on f.seg = z.seg and f.job = z.job
    ),
    with_total as (
        select
            seg,
            filt,
            job,
            outcome_val,
            stat_val,
            build_val,
            (case when seg = '50층' then 0.5 * outcome_val + 0.3 * stat_val + 0.2 * build_val
                  else 0.3 * outcome_val + 0.35 * stat_val + 0.35 * build_val end) as total_val
        from base_vals
    ),
    agg_rows as (
        select
            seg,
            filt,
            filt as job,
            sum(outcome_val * share) as outcome_val,
            sum(stat_val * share) as stat_val,
            sum(build_val * share) as build_val,
            sum(total_val * share) as total_val
        from (
            select w.*, c.share
            from with_total w
            join curr_share c on c.seg = w.seg and c.filt = w.filt and c.job = w.job
        ) x
        group by seg, filt
    ),
    combined as (
        select seg, filt, job, outcome_val, stat_val, build_val, total_val from with_total
        union all
        select seg, filt, job, outcome_val, stat_val, build_val, total_val from agg_rows
    ),
    minmax as (
        select
            seg,
            filt,
            min(abs(outcome_val)) as o_min,
            max(abs(outcome_val)) as o_max,
            min(abs(stat_val)) as s_min,
            max(abs(stat_val)) as s_max,
            min(abs(build_val)) as b_min,
            max(abs(build_val)) as b_max,
            min(abs(total_val)) as t_min,
            max(abs(total_val)) as t_max
        from combined
        group by seg, filt
    )
    select
        p_version as version,
        c.seg as segment,
        c.filt as filter,
        c.job,
        c.outcome_val::float as outcome_shift,
        c.stat_val::float as stat_shift,
        c.build_val::float as build_shift,
        c.total_val::float as total_shift,
        sign(c.total_val)::smallint as direction,
        coalesce(least(100, greatest(0, round(100 * (abs(c.outcome_val) - m.o_min) / nullif(m.o_max - m.o_min, 0))::numeric))::smallint, 0) as outcome_score_100,
        coalesce(least(100, greatest(0, round(100 * (abs(c.stat_val) - m.s_min) / nullif(m.s_max - m.s_min, 0))::numeric))::smallint, 0) as stat_score_100,
        coalesce(least(100, greatest(0, round(100 * (abs(c.build_val) - m.b_min) / nullif(m.b_max - m.b_min, 0))::numeric))::smallint, 0) as build_score_100,
        coalesce(least(100, greatest(0, round(100 * (abs(c.total_val) - m.t_min) / nullif(m.t_max - m.t_min, 0))::numeric))::smallint, 0) as total_score_100
    from combined c
    join minmax m on m.seg = c.seg and m.filt = c.filt;
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
