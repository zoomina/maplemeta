import json
import os
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import psycopg2
from psycopg2.extras import Json, execute_values

SCHEMA_SQL_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "schemas", "dw.sql")


def _get_env(name: str, fallback: Optional[str] = None) -> Optional[str]:
    value = os.getenv(name)
    if value is not None and value != "":
        return value
    return fallback


def get_dw_connection():
    """
    Create a DW Postgres connection.
    Priority:
    1) DW_DATABASE_URL
    2) DW_PG* env vars
    3) PG* env vars
    """
    url = _get_env("DW_DATABASE_URL") or _get_env("DATABASE_URL")
    if url:
        return psycopg2.connect(url)

    host = _get_env("DW_PGHOST") or _get_env("PGHOST")
    port = _get_env("DW_PGPORT") or _get_env("PGPORT", "5432")
    dbname = _get_env("DW_PGDATABASE") or _get_env("PGDATABASE")
    user = _get_env("DW_PGUSER") or _get_env("PGUSER")
    password = _get_env("DW_PGPASSWORD") or _get_env("PGPASSWORD")
    sslmode = _get_env("DW_SSLMODE") or _get_env("PGSSLMODE", "require")

    if not host or not dbname or not user:
        raise ValueError("DW database connection env vars are missing.")

    return psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        sslmode=sslmode,
    )


def ensure_dw_schema(conn) -> None:
    with conn.cursor() as cur:
        with open(SCHEMA_SQL_PATH, "r", encoding="utf-8") as f:
            cur.execute(f.read())
    conn.commit()


def load_json_file(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _parse_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped == "":
            return None
        try:
            return int(float(stripped))
        except ValueError:
            return None
    return None


def _parse_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in ("true", "t", "1"):
            return True
        if lowered in ("false", "f", "0"):
            return False
    return None


def _to_json(value: Any) -> Optional[Json]:
    if value is None:
        return None
    return Json(value, dumps=lambda v: json.dumps(v, ensure_ascii=False))


def _execute_upsert(
    conn,
    table: str,
    columns: Sequence[str],
    rows: Sequence[Sequence[Any]],
    conflict_cols: Sequence[str],
) -> None:
    if not rows:
        return

    cols_sql = ", ".join(columns)
    conflict_sql = ", ".join(conflict_cols)
    update_cols = [c for c in columns if c not in conflict_cols]
    set_sql = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
    sql = f"insert into {table} ({cols_sql}) values %s on conflict ({conflict_sql}) do update set {set_sql}"

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=2000)
    conn.commit()


def parse_rank_records(data: List[Dict[str, Any]]) -> List[Tuple[Any, ...]]:
    rows = []
    for item in data:
        rows.append(
            (
                item.get("날짜"),
                item.get("월드"),
                _parse_int(item.get("서버내순위")),
                _parse_int(item.get("도장층수")),
                _parse_int(item.get("기록시간(초)")),
                item.get("캐릭터명"),
                None,
                item.get("직업군"),
                item.get("세부직업"),
                _parse_int(item.get("통합순위")),
            )
        )
    return rows


def _ability_info_map(
    info_list: Optional[List[Dict[str, Any]]]
) -> Dict[str, Dict[str, Optional[str]]]:
    result = {
        "1": {"grade": None, "value": None},
        "2": {"grade": None, "value": None},
        "3": {"grade": None, "value": None},
    }
    if not info_list:
        return result
    for item in info_list:
        key = str(item.get("ability_no"))
        result[key] = {
            "grade": item.get("ability_grade"),
            "value": item.get("ability_value"),
        }
    return result


def parse_ability_records(data: List[Dict[str, Any]]) -> List[Tuple[Any, ...]]:
    rows = []
    for item in data:
        date = item.get("date")
        ocid = item.get("ocid")
        character_name = item.get("character_name")

        current_info = _ability_info_map(item.get("ability_info"))
        rows.append(
            (
                date,
                ocid,
                character_name,
                "current",
                item.get("ability_grade"),
                current_info.get("1", {}).get("grade"),
                current_info.get("1", {}).get("value"),
                current_info.get("2", {}).get("grade"),
                current_info.get("2", {}).get("value"),
                current_info.get("3", {}).get("grade"),
                current_info.get("3", {}).get("value"),
            )
        )

        for idx in range(1, 4):
            preset = item.get(f"ability_preset_{idx}")
            if not preset:
                continue
            info_map = _ability_info_map(preset.get("ability_info"))
            rows.append(
                (
                    date,
                    ocid,
                    character_name,
                    f"preset{idx}",
                    preset.get("ability_preset_grade"),
                    info_map.get("1", {}).get("grade"),
                    info_map.get("1", {}).get("value"),
                    info_map.get("2", {}).get("grade"),
                    info_map.get("2", {}).get("value"),
                    info_map.get("3", {}).get("grade"),
                    info_map.get("3", {}).get("value"),
                )
            )
    return rows


def parse_hexacore_records(data: List[Dict[str, Any]]) -> List[Tuple[Any, ...]]:
    rows = []
    for item in data:
        date = item.get("date")
        ocid = item.get("ocid")
        character_name = item.get("character_name")
        cores = item.get("character_hexa_core_equipment") or []
        for core in cores:
            rows.append(
                (
                    date,
                    ocid,
                    character_name,
                    core.get("hexa_core_name"),
                    _parse_int(core.get("hexa_core_level")),
                    core.get("hexa_core_type"),
                    _to_json(core.get("linked_skill")),
                )
            )
    return rows


def parse_seteffect_records(data: List[Dict[str, Any]]) -> List[Tuple[Any, ...]]:
    rows = []
    for item in data:
        date = item.get("date")
        ocid = item.get("ocid")
        character_name = item.get("character_name")
        effects = item.get("set_effect") or []
        for effect in effects:
            rows.append(
                (
                    date,
                    ocid,
                    character_name,
                    effect.get("set_name"),
                    _parse_int(effect.get("total_set_count")),
                    _to_json(effect.get("set_effect_info")),
                    _to_json(effect.get("set_option_full")),
                )
            )
    return rows


def _extract_item_total_option(item: Dict[str, Any]) -> Dict[str, Optional[int]]:
    total_option = item.get("item_total_option") or {}
    keys = [
        "str",
        "dex",
        "int",
        "luk",
        "max_hp",
        "max_mp",
        "attack_power",
        "magic_power",
        "armor",
        "speed",
        "jump",
        "boss_damage",
        "ignore_monster_armor",
        "all_stat",
        "damage",
        "equipment_level_decrease",
        "max_hp_rate",
        "max_mp_rate",
    ]
    return {k: _parse_int(total_option.get(k)) for k in keys}


def parse_equipment_records(data: List[Dict[str, Any]]) -> List[Tuple[Any, ...]]:
    rows = []
    for entry in data:
        date = entry.get("date")
        ocid = entry.get("ocid")
        character_name = entry.get("character_name")

        for key, items in entry.items():
            if not key.startswith("item_equipment"):
                continue
            if not isinstance(items, list):
                continue

            equipment_list = key
            for item in items:
                total = _extract_item_total_option(item)
                rows.append(
                    (
                        date,
                        ocid,
                        character_name,
                        equipment_list,
                        item.get("item_equipment_slot"),
                        item.get("item_equipment_part"),
                        item.get("item_name"),
                        item.get("item_icon"),
                        item.get("item_description"),
                        item.get("item_shape_name"),
                        item.get("item_shape_icon"),
                        item.get("item_gender"),
                        _to_json(item.get("item_base_option")),
                        item.get("potential_option_grade"),
                        item.get("additional_potential_option_grade"),
                        _parse_bool(item.get("potential_option_flag")),
                        item.get("potential_option_1"),
                        item.get("potential_option_2"),
                        item.get("potential_option_3"),
                        _parse_bool(item.get("additional_potential_option_flag")),
                        item.get("additional_potential_option_1"),
                        item.get("additional_potential_option_2"),
                        item.get("additional_potential_option_3"),
                        _parse_int(item.get("equipment_level_increase")),
                        _to_json(item.get("item_exceptional_option")),
                        _to_json(item.get("item_add_option")),
                        _parse_int(item.get("growth_exp")),
                        _parse_int(item.get("growth_level")),
                        _parse_int(item.get("scroll_upgrade")),
                        _parse_int(item.get("cuttable_count")),
                        item.get("golden_hammer_flag"),
                        _parse_int(item.get("scroll_resilience_count")),
                        _parse_int(item.get("scroll_upgradeable_count")),
                        item.get("soul_name"),
                        item.get("soul_option"),
                        _to_json(item.get("item_etc_option")),
                        _parse_int(item.get("starforce")),
                        item.get("starforce_scroll_flag"),
                        _to_json(item.get("item_starforce_option")),
                        _parse_int(item.get("special_ring_level")),
                        item.get("date_expire"),
                        item.get("freestyle_flag"),
                        total.get("str"),
                        total.get("dex"),
                        total.get("int"),
                        total.get("luk"),
                        total.get("max_hp"),
                        total.get("max_mp"),
                        total.get("attack_power"),
                        total.get("magic_power"),
                        total.get("armor"),
                        total.get("speed"),
                        total.get("jump"),
                        total.get("boss_damage"),
                        total.get("ignore_monster_armor"),
                        total.get("all_stat"),
                        total.get("damage"),
                        total.get("equipment_level_decrease"),
                        total.get("max_hp_rate"),
                        total.get("max_mp_rate"),
                    )
                )
    return rows


_HYPERSTAT_MAP = {
    "STR": "STR",
    "DEX": "DEX",
    "INT": "INT",
    "LUK": "LUK",
    "HP": "HP",
    "MP": "MP",
    "DF/TF": "DF_TF",
    "크리티컬 확률": "크리티컬_확률",
    "크리티컬 데미지": "크리티컬_데미지",
    "방어율 무시": "방어율_무시",
    "데미지": "데미지",
    "보스 몬스터 공격 시 데미지 증가": "보스_몬스터_공격_시_데미지_증가",
    "상태 이상 내성": "상태_이상_내성",
    "공격력/마력": "공격력_마력",
    "획득 경험치": "획득_경험치",
    "아케인포스": "아케인포스",
    "일반 몬스터 공격 시 데미지 증가": "일반_몬스터_공격_시_데미지_증가",
}


def _empty_hyperstat_columns() -> Dict[str, Optional[Any]]:
    cols = {}
    for base in _HYPERSTAT_MAP.values():
        cols[f"{base}_increase"] = None
        cols[f"{base}_level"] = None
        cols[f"{base}_point"] = None
    return cols


def parse_hyperstat_records(data: List[Dict[str, Any]]) -> List[Tuple[Any, ...]]:
    rows = []
    for item in data:
        date = item.get("date")
        ocid = item.get("ocid")
        character_name = item.get("character_name")
        use_available = _parse_int(item.get("use_available_hyper_stat"))

        for preset_no in (1, 2, 3):
            preset_list = item.get(f"hyper_stat_preset_{preset_no}")
            if preset_list is None:
                continue

            cols = _empty_hyperstat_columns()
            for stat in preset_list:
                base = _HYPERSTAT_MAP.get(stat.get("stat_type"))
                if not base:
                    continue
                cols[f"{base}_increase"] = stat.get("stat_increase")
                cols[f"{base}_level"] = _parse_int(stat.get("stat_level"))
                cols[f"{base}_point"] = _parse_int(stat.get("stat_point"))

            remain_point = _parse_int(item.get(f"hyper_stat_preset_{preset_no}_remain_point"))

            row = (
                date,
                ocid,
                character_name,
                use_available,
                preset_no,
                remain_point,
                cols.get("DEX_increase"),
                cols.get("DEX_level"),
                cols.get("DEX_point"),
                cols.get("DF_TF_increase"),
                cols.get("DF_TF_level"),
                cols.get("DF_TF_point"),
                cols.get("HP_increase"),
                cols.get("HP_level"),
                cols.get("HP_point"),
                cols.get("INT_increase"),
                cols.get("INT_level"),
                cols.get("INT_point"),
                cols.get("LUK_increase"),
                cols.get("LUK_level"),
                cols.get("LUK_point"),
                cols.get("MP_increase"),
                cols.get("MP_level"),
                cols.get("MP_point"),
                cols.get("STR_increase"),
                cols.get("STR_level"),
                cols.get("STR_point"),
                cols.get("공격력_마력_increase"),
                cols.get("공격력_마력_level"),
                cols.get("공격력_마력_point"),
                cols.get("데미지_increase"),
                cols.get("데미지_level"),
                cols.get("데미지_point"),
                cols.get("방어율_무시_increase"),
                cols.get("방어율_무시_level"),
                cols.get("방어율_무시_point"),
                cols.get("보스_몬스터_공격_시_데미지_증가_increase"),
                cols.get("보스_몬스터_공격_시_데미지_증가_level"),
                cols.get("보스_몬스터_공격_시_데미지_증가_point"),
                cols.get("상태_이상_내성_increase"),
                cols.get("상태_이상_내성_level"),
                cols.get("상태_이상_내성_point"),
                cols.get("아케인포스_increase"),
                cols.get("아케인포스_level"),
                cols.get("아케인포스_point"),
                cols.get("일반_몬스터_공격_시_데미지_증가_increase"),
                cols.get("일반_몬스터_공격_시_데미지_증가_level"),
                cols.get("일반_몬스터_공격_시_데미지_증가_point"),
                cols.get("크리티컬_데미지_increase"),
                cols.get("크리티컬_데미지_level"),
                cols.get("크리티컬_데미지_point"),
                cols.get("크리티컬_확률_increase"),
                cols.get("크리티컬_확률_level"),
                cols.get("크리티컬_확률_point"),
                cols.get("획득_경험치_increase"),
                cols.get("획득_경험치_level"),
                cols.get("획득_경험치_point"),
            )
            rows.append(row)
    return rows


def upsert_rank(conn, rows: Sequence[Sequence[Any]]) -> None:
    columns = [
        "date",
        "world",
        "world_rank",
        "floor",
        "record_sec",
        "character_name",
        "ocid",
        "job",
        "sub_job",
        "total_rank",
    ]
    _execute_upsert(conn, "dw.dw_rank", columns, rows, ["date", "world", "world_rank"])


def upsert_ability(conn, rows: Sequence[Sequence[Any]]) -> None:
    columns = [
        "date",
        "ocid",
        "character_name",
        "ability_set",
        "ability_grade",
        "ability_grade1",
        "ability_value1",
        "ability_grade2",
        "ability_value2",
        "ability_grade3",
        "ability_value3",
    ]
    _execute_upsert(conn, "dw.dw_ability", columns, rows, ["date", "ocid", "ability_set"])


def upsert_hexacore(conn, rows: Sequence[Sequence[Any]]) -> None:
    columns = [
        "date",
        "ocid",
        "character_name",
        "hexa_core_name",
        "hexa_core_level",
        "hexa_core_type",
        "linked_skill",
    ]
    _execute_upsert(conn, "dw.dw_hexacore", columns, rows, ["date", "ocid", "hexa_core_name"])


def upsert_seteffect(conn, rows: Sequence[Sequence[Any]]) -> None:
    columns = [
        "date",
        "ocid",
        "character_name",
        "set_name",
        "total_set_count",
        "set_effect_info",
        "set_option_full",
    ]
    _execute_upsert(conn, "dw.dw_seteffect", columns, rows, ["date", "ocid", "set_name"])


def upsert_equipment(conn, rows: Sequence[Sequence[Any]]) -> None:
    columns = [
        "date",
        "ocid",
        "character_name",
        "equipment_list",
        "item_equipment_slot",
        "item_equipment_part",
        "item_name",
        "item_icon",
        "item_description",
        "item_shape_name",
        "item_shape_icon",
        "item_gender",
        "item_base_option",
        "potential_option_grade",
        "additional_potential_option_grade",
        "potential_option_flag",
        "potential_option_1",
        "potential_option_2",
        "potential_option_3",
        "additional_potential_option_flag",
        "additional_potential_option_1",
        "additional_potential_option_2",
        "additional_potential_option_3",
        "equipment_level_increase",
        "item_exceptional_option",
        "item_add_option",
        "growth_exp",
        "growth_level",
        "scroll_upgrade",
        "cuttable_count",
        "golden_hammer_flag",
        "scroll_resilience_count",
        "scroll_upgradeable_count",
        "soul_name",
        "soul_option",
        "item_etc_option",
        "starforce",
        "starforce_scroll_flag",
        "item_starforce_option",
        "special_ring_level",
        "date_expire",
        "freestyle_flag",
        "item_total_option__str",
        "item_total_option__dex",
        "item_total_option__int",
        "item_total_option__luk",
        "item_total_option__max_hp",
        "item_total_option__max_mp",
        "item_total_option__attack_power",
        "item_total_option__magic_power",
        "item_total_option__armor",
        "item_total_option__speed",
        "item_total_option__jump",
        "item_total_option__boss_damage",
        "item_total_option__ignore_monster_armor",
        "item_total_option__all_stat",
        "item_total_option__damage",
        "item_total_option__equipment_level_decrease",
        "item_total_option__max_hp_rate",
        "item_total_option__max_mp_rate",
    ]
    _execute_upsert(
        conn,
        "dw.dw_equipment",
        columns,
        rows,
        ["date", "ocid", "equipment_list", "item_equipment_slot"],
    )


def upsert_hyperstat(conn, rows: Sequence[Sequence[Any]]) -> None:
    columns = [
        "date",
        "ocid",
        "character_name",
        "use_available_hyper_stat",
        "preset_no",
        "remain_point",
        "DEX_increase",
        "DEX_level",
        "DEX_point",
        "DF_TF_increase",
        "DF_TF_level",
        "DF_TF_point",
        "HP_increase",
        "HP_level",
        "HP_point",
        "INT_increase",
        "INT_level",
        "INT_point",
        "LUK_increase",
        "LUK_level",
        "LUK_point",
        "MP_increase",
        "MP_level",
        "MP_point",
        "STR_increase",
        "STR_level",
        "STR_point",
        "공격력_마력_increase",
        "공격력_마력_level",
        "공격력_마력_point",
        "데미지_increase",
        "데미지_level",
        "데미지_point",
        "방어율_무시_increase",
        "방어율_무시_level",
        "방어율_무시_point",
        "보스_몬스터_공격_시_데미지_증가_increase",
        "보스_몬스터_공격_시_데미지_증가_level",
        "보스_몬스터_공격_시_데미지_증가_point",
        "상태_이상_내성_increase",
        "상태_이상_내성_level",
        "상태_이상_내성_point",
        "아케인포스_increase",
        "아케인포스_level",
        "아케인포스_point",
        "일반_몬스터_공격_시_데미지_증가_increase",
        "일반_몬스터_공격_시_데미지_증가_level",
        "일반_몬스터_공격_시_데미지_증가_point",
        "크리티컬_데미지_increase",
        "크리티컬_데미지_level",
        "크리티컬_데미지_point",
        "크리티컬_확률_increase",
        "크리티컬_확률_level",
        "크리티컬_확률_point",
        "획득_경험치_increase",
        "획득_경험치_level",
        "획득_경험치_point",
    ]
    _execute_upsert(conn, "dw.dw_hyperstat", columns, rows, ["date", "ocid", "preset_no"])
