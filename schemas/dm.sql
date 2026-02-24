create schema if not exists dm;

create table if not exists dm.character_master (
    job text primary key,
    "group" text,
    type text,
    img text,
    color text
);

create table if not exists dm.equipment_master (
    equipment_name text primary key,
    job text,
    type text,
    img text,
    "set" text
);

create table if not exists dm.hyper_master (
    version text not null,
    job text not null,
    hyper1 text,
    hyper2 text,
    hyper3 text,
    img text,
    primary key (version, job)
);

create table if not exists dm.dm_rank (
    version text not null,
    dt date not null,
    character_name text not null,
    character_level integer,
    floor integer,
    clear_time integer,
    sec_floor numeric(18, 6),
    job text,
    "group" text,
    type text,
    primary key (version, dt, character_name)
);

alter table dm.dm_rank add column if not exists character_level integer;

create index if not exists idx_dm_rank_dt on dm.dm_rank (dt);
create index if not exists idx_dm_rank_job on dm.dm_rank (job);

create table if not exists dm.dm_force (
    version text not null,
    dt date not null,
    character_name text not null,
    job text,
    segment text,
    hexa_level integer,
    starforce integer,
    hyper1_label text,
    hyper1_value integer,
    hyper2_label text,
    hyper2_value integer,
    hyper3_label text,
    hyper3_value integer,
    additional_line integer,
    additional1_label text,
    additional1_value text,
    additional2_label text,
    additional2_value text,
    additional3_label text,
    additional3_value text,
    potential_line integer,
    potential1_label text,
    potential1_value text,
    potential2_label text,
    potential2_value text,
    potential3_label text,
    potential3_value text,
    primary key (version, dt, character_name)
);

create index if not exists idx_dm_force_dt on dm.dm_force (dt);
create index if not exists idx_dm_force_job on dm.dm_force (job);
create index if not exists idx_dm_force_segment on dm.dm_force (segment);

create table if not exists dm.dm_ability (
    version text not null,
    job text not null,
    timing text not null,
    ability text not null,
    grade text,
    segment text not null,
    type text,
    rate numeric(18, 8) not null,
    primary key (version, job, timing, ability, grade, segment, type)
);

create table if not exists dm.dm_seedring (
    version text not null,
    job text not null,
    timing text not null,
    ring text not null,
    segment text not null,
    rate numeric(18, 8) not null,
    primary key (version, job, timing, ring, segment)
);

create table if not exists dm.dm_equipment (
    version text not null,
    job text not null,
    timing text not null,
    type text not null,
    name text not null,
    segment text not null,
    rate numeric(18, 8) not null,
    primary key (version, job, timing, type, name, segment)
);

create or replace function dm.split_label_value(p_text text)
returns table(label text, value text)
language sql
immutable
as $$
    select
        case
            when p_text is null or btrim(p_text) = '' then null
            when strpos(p_text, '+') > 0 then nullif(btrim(split_part(p_text, '+', 1)), '')
            when strpos(p_text, '=') > 0 then nullif(btrim(split_part(p_text, '=', 1)), '')
            else nullif(btrim(p_text), '')
        end as label,
        case
            when p_text is null or btrim(p_text) = '' then null
            when strpos(p_text, '+') > 0 then '+' || nullif(btrim(split_part(p_text, '+', 2)), '')
            when strpos(p_text, '=') > 0 then '=' || nullif(btrim(split_part(p_text, '=', 2)), '')
            else null
        end as value;
$$;

create or replace function dm.resolve_job(p_sub_job text, p_job text)
returns text
language sql
immutable
as $$
    select coalesce(nullif(p_sub_job, ''), p_job);
$$;

create or replace function dm.segment_label(p_floor integer, p_top90_cnt bigint)
returns text
language sql
immutable
as $$
    select
        case
            when p_floor between 50 and 69 then '50층'
            when p_floor >= 90 then '상위권'
            when p_floor >= 80 and coalesce(p_top90_cnt, 0) < 15 then '상위권'
            else null
        end;
$$;

create or replace function dm.normalize_ability_text(p_text text)
returns text
language sql
immutable
as $$
    select btrim(
        regexp_replace(
            regexp_replace(
                regexp_replace(coalesce(p_text, ''), '[0-9]+(?:\\.[0-9]+)?%?', '', 'g'),
                '[+=:/,()\\[\\]~]', ' ', 'g'
            ),
            '[[:space:]]+', ' ', 'g'
        )
    );
$$;

create or replace function dm.refresh_dashboard_dm(
    p_version text,
    p_update_date date,
    p_character_dates date[],
    p_agg_dates date[]
)
returns void
language plpgsql
as $$
begin
    -- -----------------------------------------------------------------
    -- 0) Guard clauses / idempotent delete
    -- -----------------------------------------------------------------
    if p_version is null or btrim(p_version) = '' then
        raise exception 'p_version must not be empty';
    end if;

    if p_update_date is null then
        raise exception 'p_update_date must not be null';
    end if;

    if p_character_dates is null or cardinality(p_character_dates) = 0 then
        raise exception 'p_character_dates must contain at least one date';
    end if;

    if p_agg_dates is null or cardinality(p_agg_dates) = 0 then
        raise exception 'p_agg_dates must contain at least one date';
    end if;

    delete from dm.dm_rank
    where version = p_version
      and dt = any (p_character_dates);

    delete from dm.dm_force
    where version = p_version
      and dt = any (p_character_dates);

    delete from dm.dm_ability where version = p_version;
    delete from dm.dm_seedring where version = p_version;
    delete from dm.dm_equipment where version = p_version;
    delete from dm.hyper_master where version = p_version;

    insert into dm.character_master (job, "group", type, img, color)
    values
        ('카인', '노바', '궁수', 'https://lwi.nexon.com/maplestory/guide/char_info/char29.png', '#0B1F3A'),
        ('와일드헌터', '레지스탕스', '궁수', 'https://lwi.nexon.com/maplestory/guide/char_info/char27.png', '#2E7D32'),
        ('보우마스터', '모험가', '궁수', 'https://lwi.nexon.com/maplestory/guide/char_info/char23.png', '#2E7D32'),
        ('신궁', '모험가', '궁수', 'https://lwi.nexon.com/maplestory/guide/char_info/char24.png', '#1B5E20'),
        ('패스파인더', '모험가', '궁수', 'https://lwi.nexon.com/maplestory/guide/char_info/char25.png', '#00897B'),
        ('윈드브레이커', '시그너스 기사단', '궁수', 'https://lwi.nexon.com/maplestory/guide/char_info/char26.png', '#00A86B'),
        ('메르세데스', '영웅', '궁수', 'https://lwi.nexon.com/maplestory/guide/char_info/char28.png', '#66BB6A'),
        ('카데나', '노바', '도적', 'https://lwi.nexon.com/maplestory/guide/char_info/char36.png', '#3B0A45'),
        ('칼리', '레프', '도적', 'https://lwi.nexon.com/maplestory/guide/char_info/char37.png', '#880E4F'),
        ('나이트로드', '모험가', '도적', 'https://lwi.nexon.com/maplestory/guide/char_info/char30.png', '#4A148C'),
        ('섀도어', '모험가', '도적', 'https://lwi.nexon.com/maplestory/guide/char_info/char31.png', '#311B92'),
        ('듀얼블레이더', '모험가', '도적', 'https://lwi.nexon.com/maplestory/guide/char_info/char32.png', '#212121'),
        ('나이트워커', '시그너스 기사단', '도적', 'https://lwi.nexon.com/maplestory/guide/char_info/char33.png', '#1A237E'),
        ('호영', '아니마', '도적', 'https://lwi.nexon.com/maplestory/guide/char_info/char38.png', '#2E7D32'),
        ('팬텀', '영웅', '도적', 'https://lwi.nexon.com/maplestory/guide/char_info/char35.png', '#0D47A1'),
        ('제논', '레지스탕스', '도적/해적', 'https://lwi.nexon.com/maplestory/guide/char_info/char34.png', '#FF4081'),
        ('배틀메이지', '레지스탕스', '마법사', 'https://lwi.nexon.com/maplestory/guide/char_info/char17.png', '#6A1B9A'),
        ('일리움', '레프', '마법사', 'https://lwi.nexon.com/maplestory/guide/char_info/char20.png', '#00ACC1'),
        ('비숍', '모험가', '마법사', 'https://lwi.nexon.com/maplestory/guide/char_info/char15.png', '#7E57C2'),
        ('아크메이지(썬,콜)', '모험가', '마법사', 'https://lwi.nexon.com/maplestory/guide/char_info/char14.png', '#1565C0'),
        ('아크메이지(불,독)', '모험가', '마법사', 'https://lwi.nexon.com/maplestory/guide/char_info/char13.png', '#D84315'),
        ('플레임위자드', '시그너스 기사단', '마법사', 'https://lwi.nexon.com/maplestory/guide/char_info/char16.png', '#E53935'),
        ('라라', '아니마', '마법사', 'https://lwi.nexon.com/maplestory/guide/char_info/char21.png', '#A5D6A7'),
        ('루미너스', '영웅', '마법사', 'https://lwi.nexon.com/maplestory/guide/char_info/char19.png', '#B71C1C'),
        ('에반', '영웅', '마법사', 'https://lwi.nexon.com/maplestory/guide/char_info/char18.png', '#1B5E20'),
        ('키네시스', '프렌즈 월드', '마법사', 'https://lwi.nexon.com/maplestory/guide/char_info/char22.png', '#AD1457'),
        ('카이저', '노바', '전사', 'https://lwi.nexon.com/maplestory/guide/char_info/char10.png', '#C62828'),
        ('데몬어벤져', '레지스탕스', '전사', 'https://lwi.nexon.com/maplestory/guide/char_info/char8.png', '#3E003E'),
        ('데몬슬레이어', '레지스탕스', '전사', 'https://lwi.nexon.com/maplestory/guide/char_info/char7.png', '#2E0854'),
        ('블래스터', '레지스탕스', '전사', 'https://lwi.nexon.com/maplestory/guide/char_info/char6.png', '#D32F2F'),
        ('아델', '레프', '전사', 'https://lwi.nexon.com/maplestory/guide/char_info/char11.png', '#81D4FA'),
        ('히어로', '모험가', '전사', 'https://lwi.nexon.com/maplestory/guide/char_info/char1.png', '#C62828'),
        ('팔라딘', '모험가', '전사', 'https://lwi.nexon.com/maplestory/guide/char_info/char2.png', '#E53935'),
        ('다크나이트', '모험가', '전사', 'https://lwi.nexon.com/maplestory/guide/char_info/char3.png', '#8E0000'),
        ('소울마스터', '시그너스 기사단', '전사', 'https://lwi.nexon.com/maplestory/guide/char_info/char4.png', '#D4AF37'),
        ('미하일', '시그너스 기사단', '전사', 'https://lwi.nexon.com/maplestory/guide/char_info/char5.png', '#F9A825'),
        ('렌', '아니마', '전사', 'https://lwi.nexon.com/maplestory/guide/char_info/char48.png', '#00695C'),
        ('아란', '영웅', '전사', 'https://lwi.nexon.com/maplestory/guide/char_info/char9.png', '#ECEFF1'),
        ('제로', '초월자', '전사', 'https://lwi.nexon.com/maplestory/guide/char_info/char12.png', '#90CAF9'),
        ('엔젤릭버스터', '노바', '해적', 'https://lwi.nexon.com/maplestory/guide/char_info/char46.png', '#FF6EC7'),
        ('메카닉', '레지스탕스', '해적', 'https://lwi.nexon.com/maplestory/guide/char_info/char43.png', '#455A64'),
        ('아크', '레프', '해적', 'https://lwi.nexon.com/maplestory/guide/char_info/char47.png', '#4527A0'),
        ('바이퍼', '모험가', '해적', 'https://lwi.nexon.com/maplestory/guide/char_info/char39.png', '#EF6C00'),
        ('캡틴', '모험가', '해적', 'https://lwi.nexon.com/maplestory/guide/char_info/char40.png', '#FB8C00'),
        ('캐논슈터', '모험가', '해적', 'https://lwi.nexon.com/maplestory/guide/char_info/char41.png', '#F4511E'),
        ('스트라이커', '시그너스 기사단', '해적', 'https://lwi.nexon.com/maplestory/guide/char_info/char42.png', '#1565C0'),
        ('은월', '영웅', '해적', 'https://lwi.nexon.com/maplestory/guide/char_info/char45.png', '#1976D2')
    on conflict (job) do update set
        "group" = excluded."group",
        type = excluded.type,
        img = excluded.img,
        color = excluded.color;

    -- -----------------------------------------------------------------
    -- 1) Master refresh
    -- -----------------------------------------------------------------

    with src_dates as (
        select distinct unnest(p_character_dates)::date as dt
        union
        select distinct unnest(p_agg_dates)::date as dt
    ),
    equip_usage as (
        select
            e.item_name as equipment_name,
            e.item_icon as img,
            dm.resolve_job(r.sub_job, r.job) as job,
            cm.type,
            count(*) as use_cnt
        from dw.dw_equipment e
        join dw.dw_rank r
            on r.date = e.date::date
            and r.ocid = e.ocid
        left join dm.character_master cm
            on cm.job = dm.resolve_job(r.sub_job, r.job)
        join src_dates sd
            on sd.dt = e.date::date
        where e.equipment_list = 'item_equipment'
          and e.item_name is not null
          and btrim(e.item_name) <> ''
        group by e.item_name, e.item_icon, dm.resolve_job(r.sub_job, r.job), cm.type
    ),
    ranked_usage as (
        select
            eu.*,
            row_number() over (
                partition by eu.equipment_name
                order by eu.use_cnt desc, eu.job nulls last
            ) as rn
        from equip_usage eu
    )
    insert into dm.equipment_master (equipment_name, job, type, img, "set")
    select
        ru.equipment_name,
        ru.job,
        ru.type,
        ru.img,
        null::text as "set"
    from ranked_usage ru
    where ru.rn = 1
    on conflict (equipment_name) do update set
        job = excluded.job,
        type = excluded.type,
        img = excluded.img,
        "set" = excluded."set";

    with src_dates as (
        select distinct unnest(p_character_dates)::date as dt
        union
        select distinct unnest(p_agg_dates)::date as dt
    ),
    hyper_unpivot as (
        select
            hs.date::date as dt,
            hs.ocid,
            dm.resolve_job(r.sub_job, r.job) as job,
            v.stat_label,
            v.stat_level
        from dw.dw_hyperstat hs
        join dw.dw_rank r
            on r.date = hs.date::date
            and r.ocid = hs.ocid
        join src_dates sd
            on sd.dt = hs.date::date
        cross join lateral (
            values
                ('STR', hs.STR_level),
                ('DEX', hs.DEX_level),
                ('INT', hs.INT_level),
                ('LUK', hs.LUK_level),
                ('HP', hs.HP_level),
                ('MP', hs.MP_level),
                ('공격력/마력', hs.공격력_마력_level),
                ('데미지', hs.데미지_level),
                ('방어율 무시', hs.방어율_무시_level),
                ('보스 몬스터 데미지', hs.보스_몬스터_공격_시_데미지_증가_level),
                ('상태 이상 내성', hs.상태_이상_내성_level),
                ('아케인포스', hs.아케인포스_level),
                ('일반 몬스터 데미지', hs.일반_몬스터_공격_시_데미지_증가_level),
                ('크리티컬 데미지', hs.크리티컬_데미지_level),
                ('크리티컬 확률', hs.크리티컬_확률_level),
                ('획득 경험치', hs.획득_경험치_level)
        ) as v(stat_label, stat_level)
        where hs.preset_no = 1
          and coalesce(v.stat_level, 0) > 0
    ),
    hyper_ranked as (
        select
            hu.job,
            hu.stat_label,
            sum(hu.stat_level)::bigint as total_level,
            row_number() over (
                partition by hu.job
                order by sum(hu.stat_level) desc, hu.stat_label
            ) as rn
        from hyper_unpivot hu
        group by hu.job, hu.stat_label
    ),
    hyper_top3 as (
        select
            hr.job,
            max(case when hr.rn = 1 then hr.stat_label end) as hyper1,
            max(case when hr.rn = 2 then hr.stat_label end) as hyper2,
            max(case when hr.rn = 3 then hr.stat_label end) as hyper3
        from hyper_ranked hr
        where hr.rn <= 3
        group by hr.job
    )
    insert into dm.hyper_master (version, job, hyper1, hyper2, hyper3, img)
    select
        p_version as version,
        ht.job,
        ht.hyper1,
        ht.hyper2,
        ht.hyper3,
        cm.img
    from hyper_top3 ht
    left join dm.character_master cm
        on cm.job = ht.job
    on conflict (version, job) do update set
        hyper1 = excluded.hyper1,
        hyper2 = excluded.hyper2,
        hyper3 = excluded.hyper3,
        img = excluded.img;

    -- -----------------------------------------------------------------
    -- 2) Grain refresh (dm_rank, dm_force)
    -- -----------------------------------------------------------------
    with char_dates as (
        select distinct unnest(p_character_dates)::date as dt
    ),
    rank_src as (
        select
            r.date as dt,
            r.ocid,
            r.character_name,
            r.level as character_level,
            r.floor,
            r.record_sec as clear_time,
            dm.resolve_job(r.sub_job, r.job) as job
        from dw.dw_rank r
        join char_dates cd
            on cd.dt = r.date
    )
    insert into dm.dm_rank (
        version,
        dt,
        character_name,
        character_level,
        floor,
        clear_time,
        sec_floor,
        job,
        "group",
        type
    )
    select
        p_version as version,
        rs.dt,
        rs.character_name,
        rs.character_level,
        rs.floor,
        rs.clear_time,
        case
            when rs.floor is null or rs.floor = 0 then null
            else (rs.clear_time::numeric / rs.floor::numeric)::numeric(18, 6)
        end as sec_floor,
        rs.job,
        cm."group",
        cm.type
    from rank_src rs
    left join dm.character_master cm
        on cm.job = rs.job;

    with char_dates as (
        select distinct unnest(p_character_dates)::date as dt
    ),
    rank_with_seg as (
        select
            r.date as dt,
            r.ocid,
            r.character_name,
            dm.resolve_job(r.sub_job, r.job) as job,
            r.floor,
            count(*) filter (where r.floor >= 90)
                over (partition by r.date, dm.resolve_job(r.sub_job, r.job)) as top90_cnt
        from dw.dw_rank r
        join char_dates cd
            on cd.dt = r.date
    ),
    segged as (
        select
            rws.dt,
            rws.ocid,
            rws.character_name,
            rws.job,
            dm.segment_label(rws.floor, rws.top90_cnt) as segment
        from rank_with_seg rws
    ),
    hexa_sum as (
        select
            h.date::date as dt,
            h.ocid,
            sum(coalesce(h.hexa_core_level, 0))::integer as hexa_level
        from dw.dw_hexacore h
        join char_dates cd
            on cd.dt = h.date::date
        group by h.date::date, h.ocid
    ),
    starforce_sum as (
        select
            e.date::date as dt,
            e.ocid,
            sum(coalesce(e.starforce, 0))::integer as starforce
        from dw.dw_equipment e
        join char_dates cd
            on cd.dt = e.date::date
        where e.equipment_list = 'item_equipment'
        group by e.date::date, e.ocid
    ),
    weapon_opt as (
        select
            x.dt,
            x.ocid,
            x.potential_option_1,
            x.potential_option_2,
            x.potential_option_3,
            x.additional_potential_option_1,
            x.additional_potential_option_2,
            x.additional_potential_option_3
        from (
            select
                e.date::date as dt,
                e.ocid,
                e.potential_option_1,
                e.potential_option_2,
                e.potential_option_3,
                e.additional_potential_option_1,
                e.additional_potential_option_2,
                e.additional_potential_option_3,
                row_number() over (
                    partition by e.date::date, e.ocid
                    order by coalesce(e.starforce, 0) desc, e.item_name
                ) as rn
            from dw.dw_equipment e
            join char_dates cd
                on cd.dt = e.date::date
            where e.equipment_list = 'item_equipment'
              and e.item_equipment_slot = '무기'
        ) x
        where x.rn = 1
    ),
    hyper_level_pick as (
        select
            hs.date::date as dt,
            hs.ocid,
            hs.STR_level,
            hs.DEX_level,
            hs.INT_level,
            hs.LUK_level,
            hs.HP_level,
            hs.MP_level,
            hs.공격력_마력_level,
            hs.데미지_level,
            hs.방어율_무시_level,
            hs.보스_몬스터_공격_시_데미지_증가_level,
            hs.상태_이상_내성_level,
            hs.아케인포스_level,
            hs.일반_몬스터_공격_시_데미지_증가_level,
            hs.크리티컬_데미지_level,
            hs.크리티컬_확률_level,
            hs.획득_경험치_level
        from dw.dw_hyperstat hs
        join char_dates cd
            on cd.dt = hs.date::date
        where hs.preset_no = 1
    )
    insert into dm.dm_force (
        version,
        dt,
        character_name,
        job,
        segment,
        hexa_level,
        starforce,
        hyper1_label,
        hyper1_value,
        hyper2_label,
        hyper2_value,
        hyper3_label,
        hyper3_value,
        additional_line,
        additional1_label,
        additional1_value,
        additional2_label,
        additional2_value,
        additional3_label,
        additional3_value,
        potential_line,
        potential1_label,
        potential1_value,
        potential2_label,
        potential2_value,
        potential3_label,
        potential3_value
    )
    select
        p_version as version,
        s.dt,
        s.character_name,
        s.job,
        s.segment,
        coalesce(hs.hexa_level, 0) as hexa_level,
        coalesce(ss.starforce, 0) as starforce,
        hm.hyper1 as hyper1_label,
        coalesce(case hm.hyper1
            when 'STR' then hlp.STR_level
            when 'DEX' then hlp.DEX_level
            when 'INT' then hlp.INT_level
            when 'LUK' then hlp.LUK_level
            when 'HP' then hlp.HP_level
            when 'MP' then hlp.MP_level
            when '공격력/마력' then hlp.공격력_마력_level
            when '데미지' then hlp.데미지_level
            when '방어율 무시' then hlp.방어율_무시_level
            when '보스 몬스터 데미지' then hlp.보스_몬스터_공격_시_데미지_증가_level
            when '상태 이상 내성' then hlp.상태_이상_내성_level
            when '아케인포스' then hlp.아케인포스_level
            when '일반 몬스터 데미지' then hlp.일반_몬스터_공격_시_데미지_증가_level
            when '크리티컬 데미지' then hlp.크리티컬_데미지_level
            when '크리티컬 확률' then hlp.크리티컬_확률_level
            when '획득 경험치' then hlp.획득_경험치_level
            else null
        end, 0) as hyper1_value,
        hm.hyper2 as hyper2_label,
        coalesce(case hm.hyper2
            when 'STR' then hlp.STR_level
            when 'DEX' then hlp.DEX_level
            when 'INT' then hlp.INT_level
            when 'LUK' then hlp.LUK_level
            when 'HP' then hlp.HP_level
            when 'MP' then hlp.MP_level
            when '공격력/마력' then hlp.공격력_마력_level
            when '데미지' then hlp.데미지_level
            when '방어율 무시' then hlp.방어율_무시_level
            when '보스 몬스터 데미지' then hlp.보스_몬스터_공격_시_데미지_증가_level
            when '상태 이상 내성' then hlp.상태_이상_내성_level
            when '아케인포스' then hlp.아케인포스_level
            when '일반 몬스터 데미지' then hlp.일반_몬스터_공격_시_데미지_증가_level
            when '크리티컬 데미지' then hlp.크리티컬_데미지_level
            when '크리티컬 확률' then hlp.크리티컬_확률_level
            when '획득 경험치' then hlp.획득_경험치_level
            else null
        end, 0) as hyper2_value,
        hm.hyper3 as hyper3_label,
        coalesce(case hm.hyper3
            when 'STR' then hlp.STR_level
            when 'DEX' then hlp.DEX_level
            when 'INT' then hlp.INT_level
            when 'LUK' then hlp.LUK_level
            when 'HP' then hlp.HP_level
            when 'MP' then hlp.MP_level
            when '공격력/마력' then hlp.공격력_마력_level
            when '데미지' then hlp.데미지_level
            when '방어율 무시' then hlp.방어율_무시_level
            when '보스 몬스터 데미지' then hlp.보스_몬스터_공격_시_데미지_증가_level
            when '상태 이상 내성' then hlp.상태_이상_내성_level
            when '아케인포스' then hlp.아케인포스_level
            when '일반 몬스터 데미지' then hlp.일반_몬스터_공격_시_데미지_증가_level
            when '크리티컬 데미지' then hlp.크리티컬_데미지_level
            when '크리티컬 확률' then hlp.크리티컬_확률_level
            when '획득 경험치' then hlp.획득_경험치_level
            else null
        end, 0) as hyper3_value,
        (case when wo.additional_potential_option_1 is not null then 1 else 0 end
         + case when wo.additional_potential_option_2 is not null then 1 else 0 end
         + case when wo.additional_potential_option_3 is not null then 1 else 0 end) as additional_line,
        a1.label as additional1_label,
        a1.value as additional1_value,
        a2.label as additional2_label,
        a2.value as additional2_value,
        a3.label as additional3_label,
        a3.value as additional3_value,
        (case when wo.potential_option_1 is not null then 1 else 0 end
         + case when wo.potential_option_2 is not null then 1 else 0 end
         + case when wo.potential_option_3 is not null then 1 else 0 end) as potential_line,
        p1.label as potential1_label,
        p1.value as potential1_value,
        p2.label as potential2_label,
        p2.value as potential2_value,
        p3.label as potential3_label,
        p3.value as potential3_value
    from segged s
    left join hexa_sum hs
        on hs.dt = s.dt
        and hs.ocid = s.ocid
    join starforce_sum ss
        on ss.dt = s.dt
        and ss.ocid = s.ocid
    left join weapon_opt wo
        on wo.dt = s.dt
        and wo.ocid = s.ocid
    left join hyper_level_pick hlp
        on hlp.dt = s.dt
        and hlp.ocid = s.ocid
    left join dm.hyper_master hm
        on hm.version = p_version
        and hm.job = s.job
    left join lateral dm.split_label_value(wo.additional_potential_option_1) a1 on true
    left join lateral dm.split_label_value(wo.additional_potential_option_2) a2 on true
    left join lateral dm.split_label_value(wo.additional_potential_option_3) a3 on true
    left join lateral dm.split_label_value(wo.potential_option_1) p1 on true
    left join lateral dm.split_label_value(wo.potential_option_2) p2 on true
    left join lateral dm.split_label_value(wo.potential_option_3) p3 on true
    where s.segment is not null;

    -- -----------------------------------------------------------------
    -- 3) Aggregate refresh (dm_ability, dm_seedring, dm_equipment)
    -- -----------------------------------------------------------------
    with agg_dates as (
        select distinct unnest(p_agg_dates)::date as dt
    ),
    rank_with_seg as (
        select
            r.date as dt,
            r.ocid,
            dm.resolve_job(r.sub_job, r.job) as job,
            r.floor,
            count(*) filter (where r.floor >= 90)
                over (partition by r.date, dm.resolve_job(r.sub_job, r.job)) as top90_cnt
        from dw.dw_rank r
        join agg_dates ad
            on ad.dt = r.date
    ),
    segged as (
        select
            rws.dt,
            rws.ocid,
            rws.job,
            dm.segment_label(rws.floor, rws.top90_cnt) as segment
        from rank_with_seg rws
    ),
    ability_base as (
        select
            a.date::date as dt,
            a.ocid,
            a.ability_set,
            s.job,
            s.segment,
            case when a.date::date < p_update_date then 'pre' else 'post' end as timing,
            x.ability as ability,
            x.grade as grade,
            max(case when x.ability like '%보스 몬스터%' then 1 else 0 end)
                over (partition by a.date::date, a.ocid, a.ability_set) as has_boss_text,
            max(case when x.ability like '%메소 획득량%'
                       or x.ability like '%일반 몬스터%'
                       or x.ability like '%아이템 드롭률%'
                     then 1 else 0 end)
                over (partition by a.date::date, a.ocid, a.ability_set) as has_field_text
        from dw.dw_ability a
        join segged s
            on s.dt = a.date::date
            and s.ocid = a.ocid
        cross join lateral (
            values
                (a.ability_value1, a.ability_grade1),
                (a.ability_value2, a.ability_grade2),
                (a.ability_value3, a.ability_grade3)
        ) x(ability, grade)
        where s.segment is not null
          and a.ability_set in ('preset1', 'preset2', 'preset3')
          and x.ability is not null
          and btrim(x.ability) <> ''
    ),
    ability_raw as (
        select
            ab.dt,
            ab.ocid,
            ab.job,
            ab.segment,
            ab.timing,
            dm.normalize_ability_text(ab.ability) as ability,
            ab.grade,
            case
                when ab.has_boss_text = 1 then 'boss'
                when ab.has_field_text = 1 then 'field'
                else 'other'
            end as type
        from ability_base ab
    ),
    counted as (
        select
            ar.job,
            ar.timing,
            ar.ability,
            ar.grade,
            ar.segment,
            ar.type,
            count(*)::numeric as cnt
        from ability_raw ar
        where ar.ability is not null
          and ar.ability <> ''
        group by ar.job, ar.timing, ar.ability, ar.grade, ar.segment, ar.type
    )
    insert into dm.dm_ability (
        version,
        job,
        timing,
        ability,
        grade,
        segment,
        type,
        rate
    )
    select
        p_version as version,
        c.job,
        c.timing,
        c.ability,
        c.grade,
        c.segment,
        c.type,
        (c.cnt / nullif(sum(c.cnt) over (partition by c.job, c.timing, c.segment), 0))::numeric(18, 8) as rate
    from counted c;

    with agg_dates as (
        select distinct unnest(p_agg_dates)::date as dt
    ),
    rank_with_seg as (
        select
            r.date as dt,
            r.ocid,
            dm.resolve_job(r.sub_job, r.job) as job,
            r.floor,
            count(*) filter (where r.floor >= 90)
                over (partition by r.date, dm.resolve_job(r.sub_job, r.job)) as top90_cnt
        from dw.dw_rank r
        join agg_dates ad
            on ad.dt = r.date
    ),
    segged as (
        select
            rws.dt,
            rws.ocid,
            rws.job,
            dm.segment_label(rws.floor, rws.top90_cnt) as segment
        from rank_with_seg rws
    ),
    ring_raw as (
        select
            s.job,
            case when e.date::date < p_update_date then 'pre' else 'post' end as timing,
            e.item_name as ring,
            s.segment
        from dw.dw_equipment e
        join segged s
            on s.dt = e.date::date
            and s.ocid = e.ocid
        where s.segment is not null
          and e.equipment_list = 'item_equipment'
          and e.item_equipment_slot like '반지%'
          and e.item_name is not null
          and btrim(e.item_name) <> ''
    ),
    counted as (
        select
            rr.job,
            rr.timing,
            rr.ring,
            rr.segment,
            count(*)::numeric as cnt
        from ring_raw rr
        group by rr.job, rr.timing, rr.ring, rr.segment
    )
    insert into dm.dm_seedring (
        version,
        job,
        timing,
        ring,
        segment,
        rate
    )
    select
        p_version as version,
        c.job,
        c.timing,
        c.ring,
        c.segment,
        (c.cnt / nullif(sum(c.cnt) over (partition by c.job, c.timing, c.segment), 0))::numeric(18, 8) as rate
    from counted c;

    with agg_dates as (
        select distinct unnest(p_agg_dates)::date as dt
    ),
    rank_with_seg as (
        select
            r.date as dt,
            r.ocid,
            dm.resolve_job(r.sub_job, r.job) as job,
            r.floor,
            count(*) filter (where r.floor >= 90)
                over (partition by r.date, dm.resolve_job(r.sub_job, r.job)) as top90_cnt
        from dw.dw_rank r
        join agg_dates ad
            on ad.dt = r.date
    ),
    segged as (
        select
            rws.dt,
            rws.ocid,
            rws.job,
            dm.segment_label(rws.floor, rws.top90_cnt) as segment
        from rank_with_seg rws
    ),
    equip_raw as (
        select
            s.job,
            case when e.date::date < p_update_date then 'pre' else 'post' end as timing,
            case
                when e.item_equipment_slot = '무기' then '무기'
                when e.item_equipment_slot = '보조무기' then '보조무기'
                else null
            end as type,
            e.item_name as name,
            s.segment
        from dw.dw_equipment e
        join segged s
            on s.dt = e.date::date
            and s.ocid = e.ocid
        where s.segment is not null
          and e.equipment_list = 'item_equipment'
          and e.item_name is not null
          and btrim(e.item_name) <> ''
    ),
    seteffect_raw as (
        select
            s.job,
            case when se.date::date < p_update_date then 'pre' else 'post' end as timing,
            '세트효과'::text as type,
            se.set_name || ' ' || coalesce(mx.max_set_count, se.total_set_count, 1)::text || '개' as name,
            s.segment
        from dw.dw_seteffect se
        join segged s
            on s.dt = se.date::date
            and s.ocid = se.ocid
        left join lateral (
            select max((x->>'set_count')::int) as max_set_count
            from jsonb_array_elements(coalesce(se.set_effect_info, '[]'::jsonb)) x
        ) mx on true
        where s.segment is not null
          and se.set_name is not null
          and btrim(se.set_name) <> ''
    ),
    all_equipment_raw as (
        select job, timing, type, name, segment
        from equip_raw
        where type is not null
        union all
        select job, timing, type, name, segment
        from seteffect_raw
    ),
    counted as (
        select
            er.job,
            er.timing,
            er.type,
            er.name,
            er.segment,
            count(*)::numeric as cnt
        from all_equipment_raw er
        group by er.job, er.timing, er.type, er.name, er.segment
    )
    insert into dm.dm_equipment (
        version,
        job,
        timing,
        type,
        name,
        segment,
        rate
    )
    select
        p_version as version,
        c.job,
        c.timing,
        c.type,
        c.name,
        c.segment,
        (c.cnt / nullif(sum(c.cnt) over (partition by c.job, c.timing, c.segment, c.type), 0))::numeric(18, 8) as rate
    from counted c;
end;
$$;
