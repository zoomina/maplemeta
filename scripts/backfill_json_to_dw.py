"""
JSON -> DW 보정 스크립트

- 2025-06-18 이후 JSON을 기준으로 DB 누락/빈값을 보정한다.
- 기본 동작은 dry-run (검사만)이며, --apply 옵션에서만 실제 upsert를 수행한다.
- 적재 실패/스킵 목록을 날짜/테이블/식별자/사유 형태로 리포트한다.
"""
import argparse
import json
import os
import re
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from dw_load_utils import (
    ensure_dw_schema,
    get_dw_connection,
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
    upsert_rank_ocid_by_character,
    upsert_seteffect,
    upsert_stage_user_ocid,
)


DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")
BASE_JSON_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data_json")
REPORT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")


def extract_date_from_filename(path: str) -> Optional[str]:
    m = DATE_RE.search(os.path.basename(path))
    return m.group(1) if m else None


def is_date_in_scope(date_str: str, since: str) -> bool:
    return date_str >= since


def load_json_list(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        return []
    return [x for x in data if isinstance(x, dict)]


def record_fail(
    failures: List[Dict[str, Any]],
    date: Optional[str],
    table: str,
    reason: str,
    identifier: Optional[str] = None,
    raw_sample: Optional[Dict[str, Any]] = None,
) -> None:
    failures.append(
        {
            "date": date,
            "table": table,
            "identifier": identifier,
            "reason": reason,
            "raw_sample": raw_sample,
        }
    )


def dedupe_rows(rows: Sequence[Tuple[Any, ...]], key_indexes: Sequence[int]) -> List[Tuple[Any, ...]]:
    deduped: Dict[Tuple[Any, ...], Tuple[Any, ...]] = {}
    for row in rows:
        key = tuple(row[idx] for idx in key_indexes)
        deduped[key] = row
    return list(deduped.values())


def count_distinct_ocid(cur, table: str, date: str) -> int:
    if table == "dw.dw_rank_ocid":
        cur.execute("select count(distinct ocid) from dw.dw_rank where date=%s::date and ocid is not null", (date,))
        return cur.fetchone()[0] or 0
    if table == "dw.dw_rank":
        cur.execute("select count(*) from dw.dw_rank where date=%s::date", (date,))
        return cur.fetchone()[0] or 0
    if table == "dw.stage_user_ocid":
        cur.execute("select count(distinct ocid) from dw.stage_user_ocid where date=%s::date", (date,))
        return cur.fetchone()[0] or 0
    cur.execute(f"select count(distinct ocid) from {table} where date::date=%s::date", (date,))
    return cur.fetchone()[0] or 0


def validate_required(
    records: Iterable[Dict[str, Any]],
    table: str,
    failures: List[Dict[str, Any]],
    date: Optional[str],
) -> List[Dict[str, Any]]:
    required_by_table = {
        "dw.dw_rank": ("날짜", "월드", "서버내순위"),
        "dw.stage_user_ocid": ("character_name", "ocid"),
        "dw.dw_ability": ("ocid", "character_name"),
        "dw.dw_equipment": ("ocid", "character_name"),
        "dw.dw_hexacore": ("ocid", "character_name"),
        "dw.dw_seteffect": ("ocid", "character_name"),
        "dw.dw_hyperstat": ("ocid", "character_name"),
    }
    required_keys = required_by_table.get(table, tuple())
    valid: List[Dict[str, Any]] = []
    for rec in records:
        missing = [k for k in required_keys if not rec.get(k)]
        if missing:
            identifier = rec.get("ocid") or rec.get("character_name")
            record_fail(
                failures,
                date=date,
                table=table,
                reason=f"required_key_missing:{','.join(missing)}",
                identifier=identifier,
                raw_sample=rec,
            )
            continue
        valid.append(rec)
    return valid


def normalize_rank_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    """
    랭킹 JSON 키 호환:
    - '서버내순위'가 없고 '순위'가 있으면 '서버내순위'로 사용
    """
    if not rec.get("서버내순위") and rec.get("순위"):
        rec["서버내순위"] = rec.get("순위")
    return rec


def save_report(report: Dict[str, Any]) -> str:
    os.makedirs(REPORT_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(REPORT_DIR, f"json_backfill_report_{ts}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description="JSON -> DW 누락/빈값 보정")
    parser.add_argument("--since", default="2025-06-18", help="검사 시작일 (YYYY-MM-DD)")
    parser.add_argument("--apply", action="store_true", help="실제 upsert 수행")
    args = parser.parse_args()

    mode = "apply" if args.apply else "dry-run"
    conn = get_dw_connection()
    ensure_dw_schema(conn)

    failures: List[Dict[str, Any]] = []
    summary: List[Dict[str, Any]] = []
    per_table = defaultdict(int)

    configs = [
        {
            "pattern": "**/dojang_ranking_*.json",
            "db_table": "dw.dw_rank",
            "json_table": "dw.dw_rank",
            "parse_fn": parse_rank_records,
            "upsert_fn": upsert_rank,
            "dedupe_key_indexes": (0, 1, 2),  # date, world, world_rank
        },
        {
            "pattern": "**/user_ocid_*.json",
            "db_table": "dw.stage_user_ocid",
            "json_table": "dw.stage_user_ocid",
            "parse_fn": None,
            "upsert_fn": upsert_stage_user_ocid,
            "dedupe_key_indexes": None,
        },
        {
            "pattern": "**/character_ability_*.json",
            "db_table": "dw.dw_ability",
            "json_table": "dw.dw_ability",
            "parse_fn": parse_ability_records,
            "upsert_fn": upsert_ability,
            "dedupe_key_indexes": (0, 1, 3),  # date, ocid, ability_set
        },
        {
            "pattern": "**/character_equipment_*.json",
            "db_table": "dw.dw_equipment",
            "json_table": "dw.dw_equipment",
            "parse_fn": parse_equipment_records,
            "upsert_fn": upsert_equipment,
            "dedupe_key_indexes": None,  # util 내부 dedupe 사용
        },
        {
            "pattern": "**/character_hexamatrix_*.json",
            "db_table": "dw.dw_hexacore",
            "json_table": "dw.dw_hexacore",
            "parse_fn": parse_hexacore_records,
            "upsert_fn": upsert_hexacore,
            "dedupe_key_indexes": (0, 1, 3),  # date, ocid, hexa_core_name
        },
        {
            "pattern": "**/character_set_effect_*.json",
            "db_table": "dw.dw_seteffect",
            "json_table": "dw.dw_seteffect",
            "parse_fn": parse_seteffect_records,
            "upsert_fn": upsert_seteffect,
            "dedupe_key_indexes": (0, 1, 3),  # date, ocid, set_name
        },
        {
            "pattern": "**/character_hyper_stat_*.json",
            "db_table": "dw.dw_hyperstat",
            "json_table": "dw.dw_hyperstat",
            "parse_fn": parse_hyperstat_records,
            "upsert_fn": upsert_hyperstat,
            "dedupe_key_indexes": (0, 1, 4),  # date, ocid, preset_no
        },
    ]

    try:
        with conn.cursor() as cur:
            for cfg in configs:
                pattern = os.path.join(BASE_JSON_DIR, cfg["pattern"])
                paths = sorted([p for p in __import__("glob").glob(pattern, recursive=True)])
                for path in paths:
                    date = extract_date_from_filename(path)
                    if not date:
                        record_fail(failures, None, cfg["db_table"], "date_not_found_in_filename", path)
                        continue
                    if not is_date_in_scope(date, args.since):
                        continue

                    try:
                        records = load_json_list(path)
                    except Exception as e:
                        record_fail(failures, date, cfg["db_table"], f"json_load_error:{type(e).__name__}", path)
                        continue

                    if not records:
                        record_fail(failures, date, cfg["db_table"], "json_empty_or_not_list", path)
                        continue

                    if cfg["db_table"] == "dw.dw_rank":
                        records = [normalize_rank_record(r) for r in records]

                    valid_records = validate_required(records, cfg["json_table"], failures, date)
                    json_count = len({r.get("ocid") for r in valid_records if r.get("ocid")})
                    before_count = count_distinct_ocid(cur, cfg["db_table"], date)

                    upsert_rows = 0
                    null_fixed_count = max(0, json_count - before_count)

                    if cfg["db_table"] == "dw.stage_user_ocid":
                        dedup_users: Dict[str, Dict[str, Any]] = {}
                        for rec in valid_records:
                            name = rec.get("character_name")
                            if name:
                                dedup_users[name] = rec
                        users = list(dedup_users.values())
                        upsert_rows = len(users)
                        if args.apply and users:
                            cfg["upsert_fn"](conn, date, users)
                            # rank.ocid 보정도 함께 수행
                            upsert_rank_ocid_by_character(conn, date, users)
                    else:
                        # character_* JSON은 date를 filename에서 강제 주입(누락 방지)
                        if cfg["db_table"] != "dw.dw_rank":
                            for rec in valid_records:
                                rec["date"] = date

                        try:
                            rows = cfg["parse_fn"](valid_records) if cfg["parse_fn"] else []
                        except Exception as e:
                            record_fail(
                                failures,
                                date,
                                cfg["db_table"],
                                f"parse_error:{type(e).__name__}",
                                path,
                            )
                            continue

                        if cfg["dedupe_key_indexes"]:
                            rows = dedupe_rows(rows, cfg["dedupe_key_indexes"])

                        upsert_rows = len(rows)
                        if args.apply and rows:
                            try:
                                cfg["upsert_fn"](conn, rows)
                            except Exception as e:
                                record_fail(
                                    failures,
                                    date,
                                    cfg["db_table"],
                                    f"upsert_error:{type(e).__name__}",
                                    path,
                                )
                                continue

                    after_count = count_distinct_ocid(cur, cfg["db_table"], date)
                    summary.append(
                        {
                            "date": date,
                            "table": cfg["db_table"],
                            "mode": mode,
                            "json_count": json_count,
                            "db_count_before": before_count,
                            "upsert_rows": upsert_rows,
                            "db_count_after": after_count,
                            "null_fixed_estimate": null_fixed_count,
                            "file": os.path.relpath(path, BASE_JSON_DIR),
                        }
                    )
                    per_table[cfg["db_table"]] += 1

        report = {
            "mode": mode,
            "since": args.since,
            "generated_at": datetime.now().isoformat(),
            "processed_files": dict(per_table),
            "summary": summary,
            "failed_items": failures,
            "failed_count": len(failures),
        }
        report_path = save_report(report)

        print(f"[{mode}] 완료")
        print(f"report_path={report_path}")
        print(f"processed_files={sum(per_table.values())}")
        print(f"failed_count={len(failures)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
