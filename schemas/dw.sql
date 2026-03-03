create schema if not exists dw;

create table if not exists dw.dw_rank (
    date date not null,
    world text not null,
    world_rank integer not null,
    floor integer,
    record_sec integer,
    character_name text,
    level integer,
    ocid text,
    job text,
    sub_job text,
    total_rank integer,
    primary key (date, world, world_rank)
);

alter table dw.dw_rank add column if not exists level integer;

create index if not exists idx_dw_rank_date on dw.dw_rank (date);
create index if not exists idx_dw_rank_world on dw.dw_rank (world);

create table if not exists dw.dw_ability (
    date timestamptz not null,
    ocid text not null,
    character_name text,
    ability_set text not null,
    ability_grade text,
    ability_grade1 text,
    ability_value1 text,
    ability_grade2 text,
    ability_value2 text,
    ability_grade3 text,
    ability_value3 text,
    primary key (date, ocid, ability_set)
);

create index if not exists idx_dw_ability_date on dw.dw_ability (date);
create index if not exists idx_dw_ability_ocid on dw.dw_ability (ocid);

create table if not exists dw.dw_hexacore (
    date timestamptz not null,
    ocid text not null,
    character_name text,
    hexa_core_name text not null,
    hexa_core_level integer,
    hexa_core_type text,
    linked_skill jsonb,
    primary key (date, ocid, hexa_core_name)
);

create index if not exists idx_dw_hexacore_date on dw.dw_hexacore (date);
create index if not exists idx_dw_hexacore_ocid on dw.dw_hexacore (ocid);

create table if not exists dw.dw_seteffect (
    date timestamptz not null,
    ocid text not null,
    character_name text,
    set_name text not null,
    total_set_count integer,
    set_effect_info jsonb,
    set_option_full jsonb,
    primary key (date, ocid, set_name)
);

create index if not exists idx_dw_seteffect_date on dw.dw_seteffect (date);
create index if not exists idx_dw_seteffect_ocid on dw.dw_seteffect (ocid);

create table if not exists dw.dw_equipment (
    date timestamptz not null,
    ocid text not null,
    character_name text,
    equipment_list text not null,
    item_equipment_slot text not null,
    item_equipment_part text,
    item_name text,
    item_icon text,
    item_description text,
    item_shape_name text,
    item_shape_icon text,
    item_gender text,
    item_base_option jsonb,
    potential_option_grade text,
    additional_potential_option_grade text,
    potential_option_flag boolean,
    potential_option_1 text,
    potential_option_2 text,
    potential_option_3 text,
    additional_potential_option_flag boolean,
    additional_potential_option_1 text,
    additional_potential_option_2 text,
    additional_potential_option_3 text,
    equipment_level_increase integer,
    item_exceptional_option jsonb,
    item_add_option jsonb,
    growth_exp integer,
    growth_level integer,
    scroll_upgrade integer,
    cuttable_count integer,
    golden_hammer_flag text,
    scroll_resilience_count integer,
    scroll_upgradeable_count integer,
    soul_name text,
    soul_option text,
    item_etc_option jsonb,
    starforce integer,
    starforce_scroll_flag text,
    item_starforce_option jsonb,
    special_ring_level integer,
    date_expire timestamptz,
    freestyle_flag text,
    item_total_option__str integer,
    item_total_option__dex integer,
    item_total_option__int integer,
    item_total_option__luk integer,
    item_total_option__max_hp integer,
    item_total_option__max_mp integer,
    item_total_option__attack_power integer,
    item_total_option__magic_power integer,
    item_total_option__armor integer,
    item_total_option__speed integer,
    item_total_option__jump integer,
    item_total_option__boss_damage integer,
    item_total_option__ignore_monster_armor integer,
    item_total_option__all_stat integer,
    item_total_option__damage integer,
    item_total_option__equipment_level_decrease integer,
    item_total_option__max_hp_rate integer,
    item_total_option__max_mp_rate integer,
    primary key (date, ocid, equipment_list, item_equipment_slot)
);

create index if not exists idx_dw_equipment_date on dw.dw_equipment (date);
create index if not exists idx_dw_equipment_ocid on dw.dw_equipment (ocid);

create table if not exists dw.dw_hyperstat (
    date timestamptz not null,
    ocid text not null,
    character_name text,
    use_available_hyper_stat integer,
    preset_no integer not null,
    remain_point integer,
    DEX_increase text,
    DEX_level integer,
    DEX_point integer,
    DF_TF_increase text,
    DF_TF_level integer,
    DF_TF_point integer,
    HP_increase text,
    HP_level integer,
    HP_point integer,
    INT_increase text,
    INT_level integer,
    INT_point integer,
    LUK_increase text,
    LUK_level integer,
    LUK_point integer,
    MP_increase text,
    MP_level integer,
    MP_point integer,
    STR_increase text,
    STR_level integer,
    STR_point integer,
    공격력_마력_increase text,
    공격력_마력_level integer,
    공격력_마력_point integer,
    데미지_increase text,
    데미지_level integer,
    데미지_point integer,
    방어율_무시_increase text,
    방어율_무시_level integer,
    방어율_무시_point integer,
    보스_몬스터_공격_시_데미지_증가_increase text,
    보스_몬스터_공격_시_데미지_증가_level integer,
    보스_몬스터_공격_시_데미지_증가_point integer,
    상태_이상_내성_increase text,
    상태_이상_내성_level integer,
    상태_이상_내성_point integer,
    아케인포스_increase text,
    아케인포스_level integer,
    아케인포스_point integer,
    일반_몬스터_공격_시_데미지_증가_increase text,
    일반_몬스터_공격_시_데미지_증가_level integer,
    일반_몬스터_공격_시_데미지_증가_point integer,
    크리티컬_데미지_increase text,
    크리티컬_데미지_level integer,
    크리티컬_데미지_point integer,
    크리티컬_확률_increase text,
    크리티컬_확률_level integer,
    크리티컬_확률_point integer,
    획득_경험치_increase text,
    획득_경험치_level integer,
    획득_경험치_point integer,
    primary key (date, ocid, preset_no)
);

create index if not exists idx_dw_hyperstat_date on dw.dw_hyperstat (date);
create index if not exists idx_dw_hyperstat_ocid on dw.dw_hyperstat (ocid);

create table if not exists dw.collect_failed_master (
    character_name text primary key,
    reason text,
    updated_at timestamptz not null default now()
);

create index if not exists idx_collect_failed_master_updated_at on dw.collect_failed_master (updated_at);

create table if not exists dw.stage_user_ocid (
    date date not null,
    character_name text not null,
    ocid text not null,
    sub_job text,
    world text,
    level integer,
    dojang_floor integer,
    primary key (date, character_name)
);

create index if not exists idx_stage_user_ocid_date on dw.stage_user_ocid (date);
create index if not exists idx_stage_user_ocid_ocid on dw.stage_user_ocid (ocid);

create table if not exists dw.collect_api_retry_queue (
    id bigserial primary key,
    endpoint text not null,
    target_date date not null,
    ocid text,
    character_name text,
    http_status integer,
    error_code text,
    error_name text,
    error_message text,
    api_response_body jsonb,
    retry_count integer not null default 0,
    status text not null default 'pending',
    next_retry_at timestamptz not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (endpoint, target_date, ocid)
);

create index if not exists idx_collect_api_retry_queue_status_next_retry_at
on dw.collect_api_retry_queue (status, next_retry_at);

create table if not exists dw.dw_update (
    notice_id integer primary key,
    title text,
    url text,
    date timestamptz,
    version text,
    content text,
    detail_path text,
    mahalil_path text
);

create index if not exists idx_dw_update_version on dw.dw_update (version);
