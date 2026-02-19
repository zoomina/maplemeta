import argparse
import os
from typing import Optional

from dw_load_utils import (
    ensure_dw_schema,
    get_dw_connection,
    load_json_file,
    parse_ability_records,
    parse_equipment_records,
    parse_hexacore_records,
    parse_hyperstat_records,
    parse_rank_records,
    parse_seteffect_records,
    upsert_ability,
    upsert_equipment,
    upsert_hexacore,
    upsert_hyperstat,
    upsert_rank,
    upsert_seteffect,
)


def _data_json_dir(explicit_dir: Optional[str] = None) -> str:
    if explicit_dir:
        return explicit_dir
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_dir, "data_json")


def load_dw_for_date(date: str, data_json_dir: Optional[str] = None) -> None:
    data_json_dir = _data_json_dir(data_json_dir)
    conn = get_dw_connection()
    ensure_dw_schema(conn)

    try:
        rank_path = os.path.join(data_json_dir, f"dojang_ranking_{date}.json")
        ability_path = os.path.join(data_json_dir, f"character_ability_{date}.json")
        equipment_path = os.path.join(data_json_dir, f"character_equipment_{date}.json")
        hexacore_path = os.path.join(data_json_dir, f"character_hexamatrix_{date}.json")
        hyperstat_path = os.path.join(data_json_dir, f"character_hyper_stat_{date}.json")
        seteffect_path = os.path.join(data_json_dir, f"character_set_effect_{date}.json")
        loaded_tables = []

        if os.path.exists(rank_path):
            rank_rows = parse_rank_records(load_json_file(rank_path))
            upsert_rank(conn, rank_rows)
            loaded_tables.append(("dw_rank", len(rank_rows)))

        if os.path.exists(ability_path):
            ability_rows = parse_ability_records(load_json_file(ability_path))
            upsert_ability(conn, ability_rows)
            loaded_tables.append(("dw_ability", len(ability_rows)))

        if os.path.exists(hexacore_path):
            hexacore_rows = parse_hexacore_records(load_json_file(hexacore_path))
            upsert_hexacore(conn, hexacore_rows)
            loaded_tables.append(("dw_hexacore", len(hexacore_rows)))

        if os.path.exists(seteffect_path):
            seteffect_rows = parse_seteffect_records(load_json_file(seteffect_path))
            upsert_seteffect(conn, seteffect_rows)
            loaded_tables.append(("dw_seteffect", len(seteffect_rows)))

        if os.path.exists(equipment_path):
            equipment_rows = parse_equipment_records(load_json_file(equipment_path))
            upsert_equipment(conn, equipment_rows)
            loaded_tables.append(("dw_equipment", len(equipment_rows)))

        if os.path.exists(hyperstat_path):
            hyperstat_rows = parse_hyperstat_records(load_json_file(hyperstat_path))
            upsert_hyperstat(conn, hyperstat_rows)
            loaded_tables.append(("dw_hyperstat", len(hyperstat_rows)))

        if not loaded_tables:
            raise FileNotFoundError(
                f"No DW input JSON files found for date={date} in {data_json_dir}."
            )

        summary = ", ".join([f"{table}:{count}" for table, count in loaded_tables])
        print(f"DW load complete for {date} -> {summary}")
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Load DW tables for a specific date.")
    parser.add_argument("--date", required=True, help="Target date (YYYY-MM-DD)")
    parser.add_argument("--data-json-dir", required=False, help="Override data_json directory")
    args = parser.parse_args()

    load_dw_for_date(args.date, args.data_json_dir)


if __name__ == "__main__":
    main()
