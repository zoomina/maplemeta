create schema if not exists dm;

create table if not exists dm.mart_patch (
    patch_id integer primary key,
    client_version text,
    title text,
    url text,
    patch_date date not null unique,
    patch_type text not null
);

create index if not exists idx_mart_patch_patch_date on dm.mart_patch (patch_date);

create table if not exists dm.mart_character_daily (
    dt date not null,
    character_id text not null,
    job text,
    sub_job text,
    guild text,
    ranking_type text not null,
    world text,
    rank integer,
    floor integer,
    record_sec integer,
    exp bigint,
    power numeric(18, 4),
    exp_delta bigint,
    power_delta numeric(18, 4),
    rank_delta integer,
    floor_delta integer,
    record_sec_delta integer,
    segment text,
    patch_id integer,
    period_flag text,
    primary key (dt, character_id, ranking_type)
);

create index if not exists idx_mart_character_daily_dt on dm.mart_character_daily (dt);
create index if not exists idx_mart_character_daily_job on dm.mart_character_daily (job);
create index if not exists idx_mart_character_daily_patch on dm.mart_character_daily (patch_id);

create table if not exists dm.mart_meta_daily (
    dt date not null,
    job text not null,
    ranking_type text not null,
    job_share numeric(18, 8) not null,
    top_k_flag boolean not null,
    primary key (dt, job, ranking_type)
);

create index if not exists idx_mart_meta_daily_dt on dm.mart_meta_daily (dt);
create index if not exists idx_mart_meta_daily_ranking_type on dm.mart_meta_daily (ranking_type);

create table if not exists dm.mart_competition_daily (
    dt date not null,
    ranking_type text not null,
    rank_volatility numeric(18, 8),
    gap_top_mid numeric(18, 8),
    entropy_meta numeric(18, 8),
    gini_job_share numeric(18, 8),
    primary key (dt, ranking_type)
);

create index if not exists idx_mart_competition_daily_dt on dm.mart_competition_daily (dt);

create table if not exists dm.mart_equipment_daily (
    dt date not null,
    job text not null,
    equipment_slot text not null,
    item_name text not null,
    wear_count integer not null,
    share numeric(18, 8) not null,
    primary key (dt, job, equipment_slot, item_name)
);

create index if not exists idx_mart_equipment_daily_dt on dm.mart_equipment_daily (dt);
create index if not exists idx_mart_equipment_daily_job on dm.mart_equipment_daily (job);

create or replace function dm.refresh_marts(p_dates date[])
returns void
language plpgsql
as $$
begin
    if p_dates is null or cardinality(p_dates) = 0 then
        raise exception 'p_dates must contain at least one date';
    end if;

    -- idempotent reload for requested dates
    delete from dm.mart_equipment_daily where dt = any (p_dates);
    delete from dm.mart_competition_daily where dt = any (p_dates);
    delete from dm.mart_meta_daily where dt = any (p_dates);
    delete from dm.mart_character_daily where dt = any (p_dates);

    /*
     Fallback policy for mart_patch:
     - If dedicated patch source table is not available in DW yet,
       build a weekly snapshot patch row from target_dates.
     - patch_type is fixed as 'weekly_snapshot' for traceability.
    */
    with target_dates as (
        select distinct unnest(p_dates)::date as dt
    ),
    patch_rows as (
        select
            to_char(td.dt, 'YYYYMMDD')::integer as patch_id,
            null::text as client_version,
            ('weekly snapshot ' || to_char(td.dt, 'YYYY-MM-DD'))::text as title,
            null::text as url,
            td.dt as patch_date,
            'weekly_snapshot'::text as patch_type
        from target_dates td
    )
    insert into dm.mart_patch (patch_id, client_version, title, url, patch_date, patch_type)
    select patch_id, client_version, title, url, patch_date, patch_type
    from patch_rows
    on conflict (patch_id) do update set
        client_version = excluded.client_version,
        title = excluded.title,
        url = excluded.url,
        patch_date = excluded.patch_date,
        patch_type = excluded.patch_type;

    with target_dates as (
        select distinct unnest(p_dates)::date as dt
    ),
    rank_src as (
        select
            r.date as dt,
            r.ocid,
            r.character_name,
            r.job,
            r.sub_job,
            r.world,
            coalesce(r.total_rank, r.world_rank) as rank_value,
            r.floor,
            r.record_sec,
            'overall'::text as ranking_type
        from dw.dw_rank r
        join target_dates td on td.dt = r.date
    ),
    equip_power as (
        select
            e.date::date as dt,
            e.ocid,
            sum(
                coalesce(e.item_total_option__attack_power, 0) * 4
                + coalesce(e.item_total_option__magic_power, 0) * 4
                + coalesce(e.item_total_option__boss_damage, 0) * 3
                + coalesce(e.item_total_option__ignore_monster_armor, 0) * 2
                + coalesce(e.item_total_option__all_stat, 0) * 2
                + coalesce(e.item_total_option__damage, 0) * 2
                + coalesce(e.item_total_option__str, 0)
                + coalesce(e.item_total_option__dex, 0)
                + coalesce(e.item_total_option__int, 0)
                + coalesce(e.item_total_option__luk, 0)
            )::numeric(18, 4) as power
        from dw.dw_equipment e
        join target_dates td on td.dt = e.date::date
        where e.equipment_list = 'item_equipment'
        group by e.date::date, e.ocid
    ),
    base_character as (
        select
            rs.dt,
            coalesce(nullif(rs.ocid, ''), 'chr:' || lower(rs.character_name)) as character_id,
            rs.ocid,
            rs.character_name,
            rs.job,
            rs.sub_job,
            null::text as guild,
            rs.ranking_type,
            rs.world,
            rs.rank_value as rank,
            rs.floor,
            rs.record_sec,
            null::bigint as exp,
            ep.power
        from rank_src rs
        left join equip_power ep
            on ep.dt = rs.dt
            and ep.ocid = rs.ocid
    ),
    segged as (
        select
            bc.*,
            case
                when ntile(3) over (partition by bc.dt, bc.ranking_type order by bc.rank asc nulls last) = 1 then 'top'
                when ntile(3) over (partition by bc.dt, bc.ranking_type order by bc.rank asc nulls last) = 2 then 'mid'
                else 'bottom'
            end as segment
        from base_character bc
    ),
    with_patch as (
        select
            s.*,
            mp.patch_id,
            case
                when s.dt = mp.patch_date then 'post'
                else 'pre'
            end as period_flag
        from segged s
        left join dm.mart_patch mp
            on mp.patch_date = s.dt
    ),
    with_delta as (
        select
            wp.dt,
            wp.character_id,
            wp.job,
            wp.sub_job,
            wp.guild,
            wp.ranking_type,
            wp.world,
            wp.rank,
            wp.floor,
            wp.record_sec,
            wp.exp,
            wp.power,
            wp.exp - lag(wp.exp) over (partition by wp.character_id, wp.ranking_type order by wp.dt) as exp_delta,
            wp.power - lag(wp.power) over (partition by wp.character_id, wp.ranking_type order by wp.dt) as power_delta,
            wp.rank - lag(wp.rank) over (partition by wp.character_id, wp.ranking_type order by wp.dt) as rank_delta,
            wp.floor - lag(wp.floor) over (partition by wp.character_id, wp.ranking_type order by wp.dt) as floor_delta,
            wp.record_sec - lag(wp.record_sec) over (partition by wp.character_id, wp.ranking_type order by wp.dt) as record_sec_delta,
            wp.segment,
            wp.patch_id,
            wp.period_flag
        from with_patch wp
    )
    insert into dm.mart_character_daily (
        dt,
        character_id,
        job,
        sub_job,
        guild,
        ranking_type,
        world,
        rank,
        floor,
        record_sec,
        exp,
        power,
        exp_delta,
        power_delta,
        rank_delta,
        floor_delta,
        record_sec_delta,
        segment,
        patch_id,
        period_flag
    )
    select
        dt,
        character_id,
        job,
        sub_job,
        guild,
        ranking_type,
        world,
        rank,
        floor,
        record_sec,
        exp,
        power,
        exp_delta,
        power_delta,
        rank_delta,
        floor_delta,
        record_sec_delta,
        segment,
        patch_id,
        period_flag
    from with_delta;

    with target_dates as (
        select distinct unnest(p_dates)::date as dt
    ),
    base as (
        select
            mc.dt,
            coalesce(nullif(mc.job, ''), 'UNKNOWN') as job,
            mc.ranking_type,
            count(*)::numeric as cnt
        from dm.mart_character_daily mc
        join target_dates td on td.dt = mc.dt
        group by mc.dt, coalesce(nullif(mc.job, ''), 'UNKNOWN'), mc.ranking_type
    ),
    with_share as (
        select
            b.dt,
            b.job,
            b.ranking_type,
            (b.cnt / nullif(sum(b.cnt) over (partition by b.dt, b.ranking_type), 0))::numeric(18, 8) as job_share
        from base b
    ),
    ranked as (
        select
            ws.*,
            dense_rank() over (partition by ws.dt, ws.ranking_type order by ws.job_share desc, ws.job) as share_rank
        from with_share ws
    )
    insert into dm.mart_meta_daily (dt, job, ranking_type, job_share, top_k_flag)
    select
        dt,
        job,
        ranking_type,
        job_share,
        (share_rank <= 5) as top_k_flag
    from ranked;

    with target_dates as (
        select distinct unnest(p_dates)::date as dt
    ),
    rank_stats as (
        select
            mc.dt,
            mc.ranking_type,
            stddev_samp(mc.rank_delta::numeric) as rank_volatility,
            (
                avg(case when mc.segment = 'mid' then mc.rank::numeric end)
                - avg(case when mc.segment = 'top' then mc.rank::numeric end)
            ) as gap_top_mid
        from dm.mart_character_daily mc
        join target_dates td on td.dt = mc.dt
        group by mc.dt, mc.ranking_type
    ),
    share_stats as (
        select
            mm.dt,
            mm.ranking_type,
            -sum(
                case when mm.job_share > 0 then mm.job_share * ln(mm.job_share) else 0 end
            )::numeric(18, 8) as entropy_meta
        from dm.mart_meta_daily mm
        join target_dates td on td.dt = mm.dt
        group by mm.dt, mm.ranking_type
    ),
    gini_pre as (
        select
            mm.dt,
            mm.ranking_type,
            mm.job_share,
            row_number() over (partition by mm.dt, mm.ranking_type order by mm.job_share asc) as rn,
            count(*) over (partition by mm.dt, mm.ranking_type) as n,
            sum(mm.job_share) over (partition by mm.dt, mm.ranking_type) as sum_share
        from dm.mart_meta_daily mm
        join target_dates td on td.dt = mm.dt
    ),
    gini_stats as (
        select
            gp.dt,
            gp.ranking_type,
            (
                (2 * sum(gp.rn * gp.job_share) / nullif(max(gp.n) * max(gp.sum_share), 0))
                - ((max(gp.n) + 1)::numeric / nullif(max(gp.n), 0))
            )::numeric(18, 8) as gini_job_share
        from gini_pre gp
        group by gp.dt, gp.ranking_type
    )
    insert into dm.mart_competition_daily (
        dt, ranking_type, rank_volatility, gap_top_mid, entropy_meta, gini_job_share
    )
    select
        rs.dt,
        rs.ranking_type,
        rs.rank_volatility::numeric(18, 8),
        rs.gap_top_mid::numeric(18, 8),
        ss.entropy_meta,
        gs.gini_job_share
    from rank_stats rs
    left join share_stats ss
        on ss.dt = rs.dt
        and ss.ranking_type = rs.ranking_type
    left join gini_stats gs
        on gs.dt = rs.dt
        and gs.ranking_type = rs.ranking_type;

    with target_dates as (
        select distinct unnest(p_dates)::date as dt
    ),
    char_job as (
        select
            mc.dt,
            mc.character_id,
            coalesce(nullif(mc.job, ''), 'UNKNOWN') as job
        from dm.mart_character_daily mc
        join target_dates td on td.dt = mc.dt
    ),
    equip_src as (
        select
            e.date::date as dt,
            coalesce(nullif(e.ocid, ''), 'chr:' || lower(e.character_name)) as character_id,
            coalesce(nullif(e.item_equipment_slot, ''), 'UNKNOWN') as equipment_slot,
            coalesce(nullif(e.item_name, ''), 'UNKNOWN') as item_name
        from dw.dw_equipment e
        join target_dates td on td.dt = e.date::date
        where e.equipment_list = 'item_equipment'
    ),
    counted as (
        select
            es.dt,
            cj.job,
            es.equipment_slot,
            es.item_name,
            count(*)::integer as wear_count
        from equip_src es
        join char_job cj
            on cj.dt = es.dt
            and cj.character_id = es.character_id
        group by es.dt, cj.job, es.equipment_slot, es.item_name
    )
    insert into dm.mart_equipment_daily (
        dt, job, equipment_slot, item_name, wear_count, share
    )
    select
        c.dt,
        c.job,
        c.equipment_slot,
        c.item_name,
        c.wear_count,
        (c.wear_count::numeric / nullif(sum(c.wear_count) over (partition by c.dt, c.job, c.equipment_slot), 0))::numeric(18, 8) as share
    from counted c;
end;
$$;

-- Execution entrypoints are separated into:
-- - schemas/dm_run_backfill.sql
-- - schemas/dm_run_dag.sql
