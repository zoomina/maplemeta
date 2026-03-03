create schema if not exists dm;

create table if not exists dm.character_master (
    job text primary key,
    "group" text,
    type text,
    img text,
    color text,
    description text,
    link_skill_icon text,
    link_skill_name text,
    img_full text
);

alter table dm.character_master add column if not exists description text;
alter table dm.character_master add column if not exists link_skill_icon text;
alter table dm.character_master add column if not exists link_skill_name text;
alter table dm.character_master add column if not exists img_full text;

create table if not exists dm.equipment_master (
    equipment_name text primary key,
    job text,
    type text,
    img text,
    "set" text
);

drop table if exists dm.hyper_master;
create table dm.hyper_master (
    version text not null,
    date date not null,
    job text not null,
    hyper1 text,
    hyper2 text,
    hyper3 text,
    img text,
    primary key (version, date, job)
);

drop table if exists dm.dm_rank;
create table dm.dm_rank (
    version text not null,
    date date not null,
    character_name text not null,
    character_level integer,
    floor integer,
    clear_time integer,
    sec_floor numeric(18, 6),
    job text,
    "group" text,
    type text,
    primary key (version, date, character_name)
);

alter table dm.dm_rank add column if not exists character_level integer;
alter table dm.dm_rank add column if not exists segment text;

create index if not exists idx_dm_rank_date on dm.dm_rank (date);
create index if not exists idx_dm_rank_job on dm.dm_rank (job);
create index if not exists idx_dm_rank_segment on dm.dm_rank (segment);

drop table if exists dm.dm_force;
create table dm.dm_force (
    version text not null,
    date date not null,
    character_name text not null,
    job text,
    segment text,
    hexa_level integer,
    starforce integer,
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
    primary key (version, date, character_name)
);

create index if not exists idx_dm_force_date on dm.dm_force (date);
create index if not exists idx_dm_force_job on dm.dm_force (job);
create index if not exists idx_dm_force_segment on dm.dm_force (segment);

drop table if exists dm.dm_hyper;
create table dm.dm_hyper (
    version text not null,
    date date not null,
    character_name text not null,
    job text,
    segment text,
    dex integer,
    df_tf integer,
    hp integer,
    int integer,
    luck integer,
    mpstr integer,
    공마 integer,
    데미지 integer,
    방무 integer,
    보공 integer,
    상태이상내성 integer,
    아케인포스 integer,
    일공 integer,
    크뎀 integer,
    크확 integer,
    경험치 integer,
    primary key (version, date, character_name)
);

create index if not exists idx_dm_hyper_date on dm.dm_hyper (date);
create index if not exists idx_dm_hyper_job on dm.dm_hyper (job);
create index if not exists idx_dm_hyper_segment on dm.dm_hyper (segment);

drop table if exists dm.dm_ability;
create table dm.dm_ability (
    version text not null,
    date date not null,
    job text not null,
    ability text not null,
    grade text,
    segment text not null,
    type text,
    count bigint not null,
    primary key (version, date, job, ability, grade, segment, type)
);

drop table if exists dm.dm_seedring;
create table dm.dm_seedring (
    version text not null,
    date date not null,
    job text not null,
    ring text not null,
    segment text not null,
    count bigint not null,
    primary key (version, date, job, ring, segment)
);

drop table if exists dm.dm_equipment;
create table dm.dm_equipment (
    version text not null,
    date date not null,
    job text not null,
    type text not null,
    name text not null,
    segment text not null,
    count bigint not null,
    primary key (version, date, job, type, name, segment)
);

drop table if exists dm.dm_hexacore;
create table dm.dm_hexacore (
    version text not null,
    date date not null,
    job text not null,
    segment text not null,
    hexa_core_name text not null,
    hexa_core_type text,
    count bigint not null,
    total_level bigint not null,
    primary key (version, date, job, segment, hexa_core_name)
);

create index if not exists idx_dm_hexacore_version_date on dm.dm_hexacore (version, date);

-- dm_shift_score: 직업×세그먼트×버전별 Total Shift 점수 (shift_score/엔트로피 계획)
-- 100점 척도: KPI 카드용 min-max 정규화
create table if not exists dm.dm_shift_score (
    version varchar(32) not null,
    job varchar(64) not null,
    segment varchar(16) not null,
    outcome_shift float,
    stat_shift float,
    build_shift float,
    total_shift float not null,
    direction smallint,
    outcome_score_100 smallint,
    stat_score_100 smallint,
    build_score_100 smallint,
    total_score_100 smallint,
    primary key (version, job, segment)
);
alter table dm.dm_shift_score add column if not exists outcome_score_100 smallint;
alter table dm.dm_shift_score add column if not exists stat_score_100 smallint;
alter table dm.dm_shift_score add column if not exists build_score_100 smallint;
alter table dm.dm_shift_score add column if not exists total_score_100 smallint;

-- dm_balance_score: 버전×세그먼트별 밸런스 점수(정규화 엔트로피 기반)
create table if not exists dm.dm_balance_score (
    version varchar(32) not null,
    segment varchar(16) not null,
    balance_score smallint not null,
    top_job varchar(64),
    top_share float,
    cr3 float,
    top_type varchar(32),
    top_type_share float,
    primary key (version, segment)
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

    if p_character_dates is null or cardinality(p_character_dates) = 0 then
        raise exception 'p_character_dates must contain at least one date';
    end if;

    if p_agg_dates is null or cardinality(p_agg_dates) = 0 then
        raise exception 'p_agg_dates must contain at least one date';
    end if;

    delete from dm.dm_rank
    where version = p_version
      and date = any (p_character_dates);

    delete from dm.dm_force
    where version = p_version
      and date = any (p_character_dates);

    delete from dm.dm_hyper
    where version = p_version
      and date = any (p_character_dates);

    delete from dm.dm_ability
    where version = p_version
      and date = any (p_agg_dates);

    delete from dm.dm_seedring
    where version = p_version
      and date = any (p_agg_dates);

    delete from dm.dm_equipment
    where version = p_version
      and date = any (p_agg_dates);

    delete from dm.dm_hexacore
    where version = p_version
      and date = any (p_agg_dates);

    delete from dm.hyper_master
    where version = p_version
      and (
        date = any (p_character_dates)
        or date = any (p_agg_dates)
      );

    with csv_extended(job, description, link_skill_icon, link_skill_name) as (
        values
            ('히어로', '전투의 극의', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KFPCLHODJC.png', '인빈서블 빌리프(히어로)'),
            ('팔라딘', '성전사', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KFPCLHODJD.png', '인빈서블 빌리프(팔라딘)'),
            ('다크나이트', '어둠과 계약한 창기사', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KFPCLHODJE.png', '인빈서블 빌리프(다크나이트)'),
            ('소울마스터', '고결한 빛의 기사', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KFOCLHODJF.png', '시그너스 블레스(전사)'),
            ('미하일', '여제의 기사단장', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KFKCLHPDNE.png', '빛의 수호'),
            ('블래스터', '연격의 마스터', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KFMCLHOBLH.png', '스피릿 오브 프리덤(전사)'),
            ('데몬슬레이어', '어둠의 복수자', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KFMCLGOANC.png', '데몬스 퓨리'),
            ('데몬어벤져', '분노의 복수자', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KFMCLGODIB.png', '와일드 레이지지'),
            ('아란', '폴암의 영웅', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KFNCLHODFH.png', '콤보킬 어드밴티지'),
            ('카이저', '노바의 수호자', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KFJCLHODOC.png', '아이언 윌'),
            ('아델', '검의 지휘자', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KEKCLFODIB.png', '노블레스'),
            ('제로', '신의 아이', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KEPCLHODLB.png', '륀느의 축복'),
            ('아크메이지(불,독)', '화염과 맹독의 마법사', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KFPCLHODJF.png', '임피리컬 널리지(아크메이지 불, 독)'),
            ('아크메이지(썬,콜)', '냉기와 전류의 마법사', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KFPCLHODJG.png', '임피리컬 널리지(아크메이지 썬, 콜)'),
            ('비숍', '자애와 복수의 마법사', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KFPCLHODJH.png', '임피리컬 널리지(비숍)'),
            ('플레임위자드', '순수한 화염의 기사', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KFOCLHODJG.png', '시그너스 블레스(마법사)'),
            ('배틀메이지', '최전선의 마법사', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KFMCLHOBLE.png', '스피릿 오브 프리덤(마법사)'),
            ('에반', '드래곤 마스터', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KFNCLGODFE.png', '룬 퍼시스턴스'),
            ('루미너스', '빛과 어둠의 마법사', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KFNCLDODNI.png', '퍼미에이트'),
            ('일리움', '광휘의 날개', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KEKCLHOBNH.png', '전투의 흐름'),
            ('라라', '낭만풍수사', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KEJCLGOBMB.png', '자연의 벗'),
            ('키네시스', '이세계의 초능력자', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KELCLHODFC.png', '판단'),
            ('보우마스터', '속사의 정점', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KFPCLHODJI.png', '어드벤쳐러 큐리어스(보우마스터)'),
            ('신궁', '백발백중 저격수', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KFPCLHODJJ.png', '어드벤쳐러 큐리어스(신궁)'),
            ('패스파인더', '고대 유물의 주인', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KFPCLHODKA.png', '어드벤쳐러 큐리어스(패스파인더)'),
            ('윈드브레이커', '자유로운 바람의 기사', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KFOCLHODJH.png', '시그너스 블레스(궁수)'),
            ('와일드헌터', '야성의 주인', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KFMCLHOBLF.png', '스피릿 오브 프리덤(궁수)'),
            ('메르세데스', '엘프의 왕', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KFNCLFPANA.png', '엘프의 축복'),
            ('카인', '어둠의 추격자', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KFJCLEODIB.png', '프라이어 프리퍼레이션'),
            ('나이트로드', '그림자 속에 숨은 존재', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KFPCLHODKB.png', '시프 커닝(나이트로드)'),
            ('섀도어', '어둠의 비수', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KFPCLHODKC.png', '시프 커닝(섀도어)'),
            ('듀얼블레이더', '숨겨진 각인', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KFPCLHODKD.png', '시프 커닝(듀얼블레이드)'),
            ('나이트워커', '비정한 어둠의 기사', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KFOCLHODJI.png', '시그너스 블레스(도적)'),
            ('제논', '하이브리드 전투병기', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KFMCLFODPD.png', '하이브리드 로직'),
            ('팬텀', '영웅이 된 괴도', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KFNCLEODME.png', '데들리 인스팅트'),
            ('카데나', '해방의 사슬', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KFJCLFODNI.png', '인텐시브 인썰트'),
            ('칼리', '복수의 바람', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KEKCLEODIB.png', '이네이트 기프트'),
            ('호영', '천방지축 도사', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KEJCLHOBMB.png', '자신감'),
            ('바이퍼', '바다와 공명하는 힘', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KFPCLHODKE.png', '파이렛 블레스(바이퍼)'),
            ('캡틴', '함선 위의 카리스마', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KFPCLHODKF.png', '파이렛 블레스(캡틴)'),
            ('캐논슈터', '호쾌한 포격', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KFPCLHOANA.png', '파이렛 블레스(캐논슈터)'),
            ('스트라이커', '바다와 번개의 기사', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KFOCLHODJF.png', '시그너스 블레스(해적)'),
            ('메카닉', '해방의 마이스터', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KFMCLHOBLG.png', '스피릿 오브 프리덤(해적)'),
            ('은월', '잊혀진 영웅', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KFNCLCODEG.png', '구사 일생'),
            ('엔젤릭버스터', '전장의 아이돌', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KFJCLGPDNJ.png', '소울 컨트랙트'),
            ('아크', '심연의 귀환자', 'https://open.api.nexon.com/static/maplestory/SkillIcon/KEKCLGODIB.png', '무아')
    ),
    base(job, "group", type, img, color) as (
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
    )
    insert into dm.character_master (job, "group", type, img, color, description, link_skill_icon, link_skill_name, img_full)
    select
        b.job,
        b."group",
        b.type,
        b.img,
        b.color,
        c.description,
        c.link_skill_icon,
        c.link_skill_name,
        '/static/img/character/' || b.job || '.png'
    from base b
    left join csv_extended c on c.job = b.job
    on conflict (job) do update set
        "group" = excluded."group",
        type = excluded.type,
        img = excluded.img,
        color = excluded.color,
        description = excluded.description,
        link_skill_icon = excluded.link_skill_icon,
        link_skill_name = excluded.link_skill_name,
        img_full = excluded.img_full;

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
    hyper_preset_pick as (
        select
            hs.date::date as dt,
            hs.ocid,
            hs.preset_no,
            row_number() over (
                partition by hs.date::date, hs.ocid
                order by
                    coalesce(hs.보스_몬스터_공격_시_데미지_증가_level, 0) desc,
                    coalesce(hs.remain_point, 2147483647),
                    hs.preset_no
            ) as rn
        from dw.dw_hyperstat hs
        join src_dates sd
            on sd.dt = hs.date::date
    ),
    hyper_unpivot as (
        select
            hs.date::date as dt,
            hs.ocid,
            dm.resolve_job(r.sub_job, r.job) as job,
            v.stat_label,
            v.stat_level
        from dw.dw_hyperstat hs
        join hyper_preset_pick hp
            on hp.dt = hs.date::date
            and hp.ocid = hs.ocid
            and hp.preset_no = hs.preset_no
            and hp.rn = 1
        join dw.dw_rank r
            on r.date = hs.date::date
            and r.ocid = hs.ocid
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
        where coalesce(v.stat_level, 0) > 0
    ),
    hyper_ranked as (
        select
            hu.dt,
            hu.job,
            hu.stat_label,
            sum(hu.stat_level)::bigint as total_level,
            row_number() over (
                partition by hu.dt, hu.job
                order by sum(hu.stat_level) desc, hu.stat_label
            ) as rn
        from hyper_unpivot hu
        group by hu.dt, hu.job, hu.stat_label
    ),
    hyper_top3 as (
        select
            hr.dt,
            hr.job,
            max(case when hr.rn = 1 then hr.stat_label end) as hyper1,
            max(case when hr.rn = 2 then hr.stat_label end) as hyper2,
            max(case when hr.rn = 3 then hr.stat_label end) as hyper3
        from hyper_ranked hr
        where hr.rn <= 3
        group by hr.dt, hr.job
    )
    insert into dm.hyper_master (version, date, job, hyper1, hyper2, hyper3, img)
    select
        p_version as version,
        ht.dt as date,
        ht.job,
        ht.hyper1,
        ht.hyper2,
        ht.hyper3,
        cm.img
    from hyper_top3 ht
    left join dm.character_master cm
        on cm.job = ht.job
    on conflict (version, date, job) do update set
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
    rank_with_seg as (
        select
            r.date as dt,
            r.ocid,
            r.character_name,
            r.level as character_level,
            r.floor,
            r.record_sec as clear_time,
            dm.resolve_job(r.sub_job, r.job) as job,
            count(*) filter (where r.floor >= 90)
                over (partition by r.date, dm.resolve_job(r.sub_job, r.job)) as top90_cnt
        from dw.dw_rank r
        join char_dates cd
            on cd.dt = r.date
    )
    insert into dm.dm_rank (
        version,
        date,
        character_name,
        character_level,
        floor,
        clear_time,
        sec_floor,
        job,
        "group",
        type,
        segment
    )
    select
        p_version as version,
        rws.dt as date,
        rws.character_name,
        rws.character_level,
        rws.floor,
        rws.clear_time,
        case
            when rws.floor is null or rws.floor = 0 then null
            else (rws.clear_time::numeric / rws.floor::numeric)::numeric(18, 6)
        end as sec_floor,
        rws.job,
        cm."group",
        cm.type,
        dm.segment_label(rws.floor, rws.top90_cnt) as segment
    from rank_with_seg rws
    left join dm.character_master cm
        on cm.job = rws.job;

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
    )
    insert into dm.dm_force (
        version,
        date,
        character_name,
        job,
        segment,
        hexa_level,
        starforce,
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
        s.dt as date,
        s.character_name,
        s.job,
        s.segment,
        coalesce(hs.hexa_level, 0) as hexa_level,
        coalesce(ss.starforce, 0) as starforce,
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
    left join lateral dm.split_label_value(wo.additional_potential_option_1) a1 on true
    left join lateral dm.split_label_value(wo.additional_potential_option_2) a2 on true
    left join lateral dm.split_label_value(wo.additional_potential_option_3) a3 on true
    left join lateral dm.split_label_value(wo.potential_option_1) p1 on true
    left join lateral dm.split_label_value(wo.potential_option_2) p2 on true
    left join lateral dm.split_label_value(wo.potential_option_3) p3 on true
    where s.segment is not null;

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
    hyper_pick_raw as (
        select
            hs.date::date as dt,
            hs.ocid,
            hs.preset_no,
            hs.remain_point,
            hs.DEX_level,
            hs.DF_TF_level,
            hs.HP_level,
            hs.INT_level,
            hs.LUK_level,
            hs.MP_level,
            hs.STR_level,
            hs.공격력_마력_level,
            hs.데미지_level,
            hs.방어율_무시_level,
            hs.보스_몬스터_공격_시_데미지_증가_level,
            hs.상태_이상_내성_level,
            hs.아케인포스_level,
            hs.일반_몬스터_공격_시_데미지_증가_level,
            hs.크리티컬_데미지_level,
            hs.크리티컬_확률_level,
            hs.획득_경험치_level,
            row_number() over (
                partition by hs.date::date, hs.ocid
                order by
                    coalesce(hs.보스_몬스터_공격_시_데미지_증가_level, 0) desc,
                    coalesce(hs.remain_point, 2147483647),
                    hs.preset_no
            ) as rn
        from dw.dw_hyperstat hs
        join char_dates cd
            on cd.dt = hs.date::date
    ),
    hyper_pick as (
        select *
        from hyper_pick_raw
        where rn = 1
    ),
    equipped_chars as (
        select distinct
            e.date::date as dt,
            e.ocid
        from dw.dw_equipment e
        join char_dates cd
            on cd.dt = e.date::date
        where e.equipment_list = 'item_equipment'
    )
    insert into dm.dm_hyper (
        version,
        date,
        character_name,
        job,
        segment,
        dex,
        df_tf,
        hp,
        int,
        luck,
        mpstr,
        공마,
        데미지,
        방무,
        보공,
        상태이상내성,
        아케인포스,
        일공,
        크뎀,
        크확,
        경험치
    )
    select
        p_version as version,
        s.dt as date,
        s.character_name,
        s.job,
        s.segment,
        coalesce(hp.DEX_level, 0) as dex,
        coalesce(hp.DF_TF_level, 0) as df_tf,
        coalesce(hp.HP_level, 0) as hp,
        coalesce(hp.INT_level, 0) as int,
        coalesce(hp.LUK_level, 0) as luck,
        greatest(coalesce(hp.MP_level, 0), coalesce(hp.STR_level, 0)) as mpstr,
        coalesce(hp.공격력_마력_level, 0) as 공마,
        coalesce(hp.데미지_level, 0) as 데미지,
        coalesce(hp.방어율_무시_level, 0) as 방무,
        coalesce(hp.보스_몬스터_공격_시_데미지_증가_level, 0) as 보공,
        coalesce(hp.상태_이상_내성_level, 0) as 상태이상내성,
        coalesce(hp.아케인포스_level, 0) as 아케인포스,
        coalesce(hp.일반_몬스터_공격_시_데미지_증가_level, 0) as 일공,
        coalesce(hp.크리티컬_데미지_level, 0) as 크뎀,
        coalesce(hp.크리티컬_확률_level, 0) as 크확,
        coalesce(hp.획득_경험치_level, 0) as 경험치
    from segged s
    join equipped_chars ec
        on ec.dt = s.dt
        and ec.ocid = s.ocid
    join hyper_pick hp
        on hp.dt = s.dt
        and hp.ocid = s.ocid
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
            ar.dt,
            ar.job,
            ar.ability,
            ar.grade,
            ar.segment,
            ar.type,
            count(*)::bigint as cnt
        from ability_raw ar
        where ar.ability is not null
          and ar.ability <> ''
        group by ar.dt, ar.job, ar.ability, ar.grade, ar.segment, ar.type
    )
    insert into dm.dm_ability (
        version,
        date,
        job,
        ability,
        grade,
        segment,
        type,
        count
    )
    select
        p_version as version,
        c.dt as date,
        c.job,
        c.ability,
        c.grade,
        c.segment,
        c.type,
        c.cnt as count
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
            s.dt,
            s.job,
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
            rr.dt,
            rr.job,
            rr.ring,
            rr.segment,
            count(*)::bigint as cnt
        from ring_raw rr
        group by rr.dt, rr.job, rr.ring, rr.segment
    )
    insert into dm.dm_seedring (
        version,
        date,
        job,
        ring,
        segment,
        count
    )
    select
        p_version as version,
        c.dt as date,
        c.job,
        c.ring,
        c.segment,
        c.cnt as count
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
            s.dt,
            s.job,
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
            s.dt,
            s.job,
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
          and se.set_name not like '쁘띠%'
          and se.set_name <> '__MISSING__'
          and coalesce(mx.max_set_count, se.total_set_count, 1) > 1
    ),
    all_equipment_raw as (
        select dt, job, type, name, segment
        from equip_raw
        where type is not null
        union all
        select dt, job, type, name, segment
        from seteffect_raw
    ),
    counted as (
        select
            er.dt,
            er.job,
            er.type,
            er.name,
            er.segment,
            count(*)::bigint as cnt
        from all_equipment_raw er
        group by er.dt, er.job, er.type, er.name, er.segment
    )
    insert into dm.dm_equipment (
        version,
        date,
        job,
        type,
        name,
        segment,
        count
    )
    select
        p_version as version,
        c.dt as date,
        c.job,
        c.type,
        c.name,
        c.segment,
        c.cnt as count
    from counted c;

    -- -----------------------------------------------------------------
    -- 4) dm_hexacore (dw_hexacore -> dm aggregate)
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
    hexacore_with_seg as (
        select
            h.date::date as dt,
            h.ocid,
            s.job,
            s.segment,
            h.hexa_core_name,
            h.hexa_core_type,
            coalesce(h.hexa_core_level, 0)::bigint as hexa_core_level
        from dw.dw_hexacore h
        join segged s
            on s.dt = h.date::date
            and s.ocid = h.ocid
        join agg_dates ad
            on ad.dt = h.date::date
        where s.segment is not null
          and h.hexa_core_name <> '__NO_HEXACORE__'
    ),
    hexacore_agg as (
        select
            dt,
            job,
            segment,
            hexa_core_name,
            hexa_core_type,
            count(*)::bigint as cnt,
            sum(hexa_core_level)::bigint as total_lvl
        from hexacore_with_seg
        group by dt, job, segment, hexa_core_name, hexa_core_type
    )
    insert into dm.dm_hexacore (
        version,
        date,
        job,
        segment,
        hexa_core_name,
        hexa_core_type,
        count,
        total_level
    )
    select
        p_version as version,
        ha.dt as date,
        ha.job,
        ha.segment,
        ha.hexa_core_name,
        ha.hexa_core_type,
        ha.cnt as count,
        ha.total_lvl as total_level
    from hexacore_agg ha;
end;
$$;

create or replace function dm.refresh_dm_hexacore(
    p_version text,
    p_agg_dates date[]
)
returns void
language plpgsql
as $$
begin
    if p_version is null or btrim(p_version) = '' then
        raise exception 'p_version must not be empty';
    end if;
    if p_agg_dates is null or cardinality(p_agg_dates) = 0 then
        raise exception 'p_agg_dates must contain at least one date';
    end if;

    delete from dm.dm_hexacore
    where version = p_version
      and date = any (p_agg_dates);

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
    hexacore_with_seg as (
        select
            h.date::date as dt,
            h.ocid,
            s.job,
            s.segment,
            h.hexa_core_name,
            h.hexa_core_type,
            coalesce(h.hexa_core_level, 0)::bigint as hexa_core_level
        from dw.dw_hexacore h
        join segged s
            on s.dt = h.date::date
            and s.ocid = h.ocid
        join agg_dates ad
            on ad.dt = h.date::date
        where s.segment is not null
          and h.hexa_core_name <> '__NO_HEXACORE__'
    ),
    hexacore_agg as (
        select
            dt,
            job,
            segment,
            hexa_core_name,
            hexa_core_type,
            count(*)::bigint as cnt,
            sum(hexa_core_level)::bigint as total_lvl
        from hexacore_with_seg
        group by dt, job, segment, hexa_core_name, hexa_core_type
    )
    insert into dm.dm_hexacore (
        version,
        date,
        job,
        segment,
        hexa_core_name,
        hexa_core_type,
        count,
        total_level
    )
    select
        p_version as version,
        ha.dt as date,
        ha.job,
        ha.segment,
        ha.hexa_core_name,
        ha.hexa_core_type,
        ha.cnt as count,
        ha.total_lvl as total_level
    from hexacore_agg ha;
end;
$$;
