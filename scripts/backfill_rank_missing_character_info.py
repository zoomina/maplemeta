# -*- coding: utf-8 -*-
"""
dm.version_master 범위 내, dw_rank에 있으나 OCID/character_info가 비어 있는 인원만 수집·적재하는 백필 스크립트.

- 날짜 범위: dm.version_master의 (version, start_date, end_date)에서 산출. end_date null = 오늘까지.
- 비어 있는 인원: dw_rank에는 있으나 stage_user_ocid에 없거나 ocid 없음 → OCID 수집; OCID는 있으나 5개 character_info 테이블에 없음 → character_info 수집.
- 사용 키: config.resolve_api_key("API_KEY")
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for path in (os.path.join(BASE_DIR, "scripts"), BASE_DIR):
    if path not in sys.path:
        sys.path.insert(0, path)

from config import resolve_api_key
from dw_load_utils import (
    ensure_dw_schema,
    fetch_rank_records_for_date,
    fetch_stage_user_ocid,
    get_dw_connection,
    upsert_rank_ocid_by_character,
    upsert_stage_user_ocid,
)
from load_character_info import (
    parse_ability_records,
    parse_equipment_records,
    parse_hexacore_records,
    parse_hyperstat_records,
    parse_seteffect_records,
    process_endpoint_data,
    upsert_ability,
    upsert_equipment,
    upsert_hexacore,
    upsert_hyperstat,
    upsert_seteffect,
)
from load_ocid import get_character_ocid
from dw_load_utils import upsert_api_retry_queue

ENDPOINTS = {
    "equipment": ("item-equipment", parse_equipment_records, upsert_equipment),
    "hexamatrix": ("hexamatrix", parse_hexacore_records, upsert_hexacore),
    "set_effect": ("set-effect", parse_seteffect_records, upsert_seteffect),
    "ability": ("ability", parse_ability_records, upsert_ability),
    "hyper_stat": ("hyper-stat", parse_hyperstat_records, upsert_hyperstat),
}

CHARACTER_INFO_TABLES = (
    "dw.dw_equipment",
    "dw.dw_hexacore",
    "dw.dw_seteffect",
    "dw.dw_ability",
    "dw.dw_hyperstat",
)


def parse_date_arg(s: str) -> date:
    """YYMMDD 또는 YYYY-MM-DD 형식 문자열을 date로 변환."""
    s = s.strip()
    if len(s) == 6 and s.isdigit():
        yy, mm, dd = int(s[:2]), int(s[2:4]), int(s[4:6])
        year = 2000 + yy if yy < 100 else yy
        return date(year, mm, dd)
    return date.fromisoformat(s)


def get_version_master_dates_simple(conn) -> list[date]:
    """dm.version_master에 있는 구간의 모든 날짜를 반환."""
    from datetime import timedelta
    today = date.today()
    with conn.cursor() as cur:
        cur.execute(
            """
            select version, start_date, end_date
            from dm.version_master
            where start_date is not null
            order by start_date
            """
        )
        rows = cur.fetchall()
    result = []
    for row in rows:
        if not row or not row[1]:
            continue
        start_d = row[1]
        end_d = row[2] if row[2] else today
        if end_d < start_d:
            continue
        cur_d = start_d
        while cur_d <= end_d:
            result.append(cur_d)
            cur_d = cur_d + timedelta(days=1)
    return sorted(set(result))


def get_dates_with_rank(conn, candidate_dates: list[date]) -> list[date]:
    """candidate_dates 중 dw.dw_rank에 데이터가 있는 날짜만 반환."""
    if not candidate_dates:
        return []
    with conn.cursor() as cur:
        cur.execute(
            """
            select distinct date::date as dt
            from dw.dw_rank
            where date::date = any(%s)
            order by dt
            """,
            (candidate_dates,),
        )
        return [row[0] for row in cur.fetchall() if row and row[0]]


def get_missing_ocid_for_date(conn, d: date) -> list[dict]:
    """해당 날짜에 dw_rank에는 있으나 stage_user_ocid에 없거나 ocid가 비어 있는 캐릭터 목록 (rank 레코드)."""
    date_str = d.strftime("%Y-%m-%d")
    rank_records = fetch_rank_records_for_date(conn, date_str)
    if not rank_records:
        return []
    with conn.cursor() as cur:
        cur.execute(
            """
            select character_name, ocid
            from dw.stage_user_ocid
            where date = %s::date
            """,
            (date_str,),
        )
        stage = {row[0]: row[1] for row in cur.fetchall() if row and row[0]}
    missing = []
    for r in rank_records:
        name = (r.get("캐릭터명") or "").strip()
        if not name:
            continue
        if name not in stage or not (stage[name] or "").strip():
            missing.append(r)
    return missing


def get_ocids_missing_character_info(conn, d: date) -> set[str]:
    """해당 날짜에 stage_user_ocid에는 있으나 5개 character_info 테이블에 모두 있는 ocid가 아닌 ocid 집합."""
    date_str = d.strftime("%Y-%m-%d")
    with conn.cursor() as cur:
        cur.execute(
            "select ocid from dw.stage_user_ocid where date = %s::date and ocid is not null",
            (date_str,),
        )
        all_stage = {row[0] for row in cur.fetchall() if row and row[0]}
    if not all_stage:
        return set()
    complete = None
    for table in CHARACTER_INFO_TABLES:
        with conn.cursor() as cur:
            cur.execute(
                f"select distinct ocid from {table} where date::date = %s::date",
                (date_str,),
            )
            in_table = {row[0] for row in cur.fetchall() if row and row[0]}
        if complete is None:
            complete = in_table
        else:
            complete &= in_table
    return all_stage - (complete or set())


def backfill_ocid_for_date(conn, d: date, api_key: str, dry_run: bool) -> int:
    """해당 날짜에 OCID 비어 있는 캐릭터만 OCID 조회 후 stage_user_ocid, dw_rank 반영. 처리 건수 반환."""
    missing = get_missing_ocid_for_date(conn, d)
    if not missing:
        return 0
    date_str = d.strftime("%Y-%m-%d")
    users = []
    for r in missing:
        name = (r.get("캐릭터명") or "").strip()
        ocid, api_error = get_character_ocid(name, api_key)
        if ocid:
            users.append({
                "character_name": name,
                "ocid": ocid,
                "sub_job": r.get("세부직업") or r.get("직업군") or "",
                "world": r.get("월드"),
                "level": r.get("레벨"),
                "dojang_floor": r.get("도장층수"),
            })
        if not dry_run:
            time.sleep(0.3)
    if users and not dry_run:
        upsert_stage_user_ocid(conn, date_str, users)
        upsert_rank_ocid_by_character(conn, date_str, users)
        conn.commit()
    return len(users)


def backfill_character_info_for_date(conn, d: date, api_key: str, dry_run: bool) -> int:
    """해당 날짜에 character_info 비어 있는 ocid만 수집·적재. 처리 ocid 수 반환."""
    missing_ocids = get_ocids_missing_character_info(conn, d)
    if not missing_ocids:
        return 0
    date_str = d.strftime("%Y-%m-%d")
    master_data = [u for u in fetch_stage_user_ocid(conn, date_str) if u.get("ocid") in missing_ocids]
    if not master_data:
        return 0
    if dry_run:
        return len(master_data)
    for endpoint_name, (endpoint_url, parse_fn, upsert_fn) in ENDPOINTS.items():
        endpoint_data, retry_items = process_endpoint_data(
            master_data, date_str, endpoint_name, endpoint_url, api_key
        )
        if retry_items:
            upsert_api_retry_queue(conn, retry_items, retry_delay_hours=3)
        if endpoint_data:
            rows = parse_fn(endpoint_data)
            upsert_fn(conn, rows)
        time.sleep(0.2)
    conn.commit()
    return len(master_data)


def main():
    parser = argparse.ArgumentParser(
        description="dm.version_master 범위 내 비어 있는 인원만 OCID·character_info 백필"
    )
    parser.add_argument("--dry-run", action="store_true", help="API/DB 변경 없이 대상만 출력")
    parser.add_argument(
        "--dates",
        nargs="+",
        metavar="DATE",
        help="대상 날짜 (YYMMDD 또는 YYYY-MM-DD). 생략 시 dm.version_master 전체",
    )
    args = parser.parse_args()

    conn = get_dw_connection()
    ensure_dw_schema(conn)
    api_key = resolve_api_key("API_KEY")

    try:
        if args.dates:
            candidate_dates = [parse_date_arg(d) for d in args.dates]
            target_dates = get_dates_with_rank(conn, candidate_dates)
            print(f"지정 날짜: {len(candidate_dates)}개, dw_rank 존재: {len(target_dates)}개")
        else:
            all_dates = get_version_master_dates_simple(conn)
            target_dates = get_dates_with_rank(conn, all_dates)
            print(f"dm.version_master 기반 날짜: {len(all_dates)}개, dw_rank 존재: {len(target_dates)}개")
        if not target_dates:
            print("대상 날짜 없음.")
            return

        total_ocid = 0
        total_char = 0
        for d in target_dates:
            date_str = d.strftime("%Y-%m-%d")
            n_ocid = backfill_ocid_for_date(conn, d, api_key, args.dry_run)
            if n_ocid:
                total_ocid += n_ocid
                print(f"  {date_str} OCID 백필: {n_ocid}명")
            n_char = backfill_character_info_for_date(conn, d, api_key, args.dry_run)
            if n_char:
                total_char += n_char
                print(f"  {date_str} character_info 백필: {n_char}명")
        print(f"총 OCID 백필: {total_ocid}명, character_info 백필: {total_char}명")
        if args.dry_run:
            print("(dry-run: 실제 반영 없음)")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
