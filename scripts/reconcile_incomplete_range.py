import argparse
import glob
import json
import os
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Set, Tuple

from psycopg2.extras import execute_values

from dw_load_utils import (
    ensure_dw_schema,
    get_dw_connection,
    parse_ability_records,
    parse_equipment_records,
    parse_hexacore_records,
    parse_hyperstat_records,
    parse_seteffect_records,
    upsert_ability,
    upsert_api_retry_queue,
    upsert_equipment,
    upsert_hexacore,
    upsert_hyperstat,
    upsert_seteffect,
)


BASE_JSON_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data_json")


def _find_file_recursive(file_name: str) -> str:
    matches = glob.glob(os.path.join(BASE_JSON_DIR, "**", file_name), recursive=True)
    return matches[0] if matches else ""


def _load_json_list(path: str) -> List[dict]:
    if not path or not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, list) else []


def _reingest_json_for_date(conn, date_str: str) -> None:
    endpoint_map = [
        (f"character_ability_{date_str}.json", parse_ability_records, upsert_ability),
        (f"character_equipment_{date_str}.json", parse_equipment_records, upsert_equipment),
        (f"character_hexamatrix_{date_str}.json", parse_hexacore_records, upsert_hexacore),
        (f"character_set_effect_{date_str}.json", parse_seteffect_records, upsert_seteffect),
        (f"character_hyper_stat_{date_str}.json", parse_hyperstat_records, upsert_hyperstat),
    ]
    for file_name, parse_fn, upsert_fn in endpoint_map:
        path = _find_file_recursive(file_name)
        data = _load_json_list(path)
        if not data:
            continue
        for item in data:
            if isinstance(item, dict) and not item.get("date"):
                item["date"] = date_str
        rows = parse_fn(data)
        if rows:
            upsert_fn(conn, rows)


def _get_expected_ocids(date_str: str) -> Dict[str, str]:
    # JSON user_ocid가 우선 기준
    path = _find_file_recursive(f"user_ocid_{date_str}.json")
    users = _load_json_list(path)
    expected = {}
    for u in users:
        if not isinstance(u, dict):
            continue
        ocid = u.get("ocid")
        if not ocid:
            continue
        expected[ocid] = u.get("character_name")
    return expected


def _fetch_endpoint_ocids(cur, table: str, date_str: str) -> Set[str]:
    cur.execute(f"select distinct ocid from {table} where date::date=%s::date and ocid is not null", (date_str,))
    return {r[0] for r in cur.fetchall() if r and r[0]}


def _insert_null_placeholders(conn, missing_items: List[dict]) -> int:
    grouped = defaultdict(list)
    for item in missing_items:
        grouped[item["endpoint"]].append(item)

    inserted = 0
    with conn.cursor() as cur:
        if grouped["ability"]:
            rows = [(i["date"], i["ocid"], i.get("character_name"), "__MISSING__") for i in grouped["ability"]]
            sql = """
                insert into dw.dw_ability (
                    date, ocid, character_name, ability_set
                ) values %s
                on conflict (date, ocid, ability_set)
                do update set character_name = excluded.character_name
            """
            execute_values(cur, sql, rows, page_size=1000)
            inserted += len(rows)

        if grouped["equipment"]:
            rows = [(i["date"], i["ocid"], i.get("character_name"), "__MISSING__", "__MISSING__") for i in grouped["equipment"]]
            sql = """
                insert into dw.dw_equipment (
                    date, ocid, character_name, equipment_list, item_equipment_slot
                ) values %s
                on conflict (date, ocid, equipment_list, item_equipment_slot)
                do update set character_name = excluded.character_name
            """
            execute_values(cur, sql, rows, page_size=1000)
            inserted += len(rows)

        if grouped["hexacore"]:
            rows = [(i["date"], i["ocid"], i.get("character_name"), "__MISSING__") for i in grouped["hexacore"]]
            sql = """
                insert into dw.dw_hexacore (
                    date, ocid, character_name, hexa_core_name
                ) values %s
                on conflict (date, ocid, hexa_core_name)
                do update set character_name = excluded.character_name
            """
            execute_values(cur, sql, rows, page_size=1000)
            inserted += len(rows)

        if grouped["seteffect"]:
            rows = [(i["date"], i["ocid"], i.get("character_name"), "__MISSING__") for i in grouped["seteffect"]]
            sql = """
                insert into dw.dw_seteffect (
                    date, ocid, character_name, set_name
                ) values %s
                on conflict (date, ocid, set_name)
                do update set character_name = excluded.character_name
            """
            execute_values(cur, sql, rows, page_size=1000)
            inserted += len(rows)

        if grouped["hyperstat"]:
            rows = [(i["date"], i["ocid"], i.get("character_name"), 0) for i in grouped["hyperstat"]]
            sql = """
                insert into dw.dw_hyperstat (
                    date, ocid, character_name, preset_no
                ) values %s
                on conflict (date, ocid, preset_no)
                do update set character_name = excluded.character_name
            """
            execute_values(cur, sql, rows, page_size=1000)
            inserted += len(rows)

    conn.commit()
    return inserted


def main():
    parser = argparse.ArgumentParser(description="날짜 범위 미완료 JSON 재보정 + queue/null 처리")
    parser.add_argument("--start", default="2025-06-18")
    parser.add_argument("--end", default="2026-02-19")
    parser.add_argument("--threshold", type=int, default=100)
    args = parser.parse_args()

    conn = get_dw_connection()
    ensure_dw_schema(conn)

    endpoint_table_map = {
        "ability": "dw.dw_ability",
        "equipment": "dw.dw_equipment",
        "hexacore": "dw.dw_hexacore",
        "seteffect": "dw.dw_seteffect",
        "hyperstat": "dw.dw_hyperstat",
    }

    missing_items: List[dict] = []
    dates = []
    cur_date = datetime.strptime(args.start, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end, "%Y-%m-%d").date()
    while cur_date <= end_date:
        dates.append(cur_date.strftime("%Y-%m-%d"))
        cur_date = cur_date.fromordinal(cur_date.toordinal() + 1)

    try:
        with conn.cursor() as cur:
            for d in dates:
                # 랭킹일이 없는 날짜는 스킵
                cur.execute("select 1 from dw.dw_rank where date=%s::date limit 1", (d,))
                if cur.fetchone() is None:
                    continue

                _reingest_json_for_date(conn, d)
                expected = _get_expected_ocids(d)
                if not expected:
                    continue

                for endpoint, table in endpoint_table_map.items():
                    covered = _fetch_endpoint_ocids(cur, table, d)
                    for ocid, character_name in expected.items():
                        if ocid not in covered:
                            missing_items.append(
                                {
                                    "date": d,
                                    "endpoint": endpoint,
                                    "ocid": ocid,
                                    "character_name": character_name,
                                }
                            )

        if not missing_items:
            print("missing_items=0")
            print("action=none")
            return

        print(f"missing_items={len(missing_items)}")
        if len(missing_items) > args.threshold:
            inserted = _insert_null_placeholders(conn, missing_items)
            print(f"action=null_placeholder_inserted rows={inserted}")
        else:
            queue_items = []
            for item in missing_items:
                queue_items.append(
                    {
                        "endpoint": item["endpoint"],
                        "target_date": item["date"],
                        "ocid": item["ocid"],
                        "character_name": item.get("character_name"),
                        "http_status": None,
                        "error_code": "RECONCILE_MISSING",
                        "error_name": "Reconcile Missing",
                        "error_message": "json/db mismatch after reingest",
                        "api_response_body": {"source": "reconcile_incomplete_range"},
                        "retry_count": 0,
                    }
                )
            upsert_api_retry_queue(conn, queue_items, retry_delay_hours=3)
            print(f"action=queued rows={len(queue_items)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
