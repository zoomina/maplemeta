# -*- coding: utf-8 -*-
"""
DW → DM 전체 백필 스크립트.

DW에 character_info 완료된 모든 날짜를 조회하여 version별로 dm.refresh_dashboard_dm 호출.
"""
from __future__ import annotations

import os
import sys
from datetime import date
from typing import Optional

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPTS_DIR = os.path.join(BASE_DIR, "scripts")
for path in (SCRIPTS_DIR, BASE_DIR):
    if path not in sys.path:
        sys.path.insert(0, path)

from dw_load_utils import ensure_dw_schema, get_dw_connection

# dm.version_master 비어있을 때 fallback: (version, start_date, end_date)
# dm_run_backfill.sql 기준
DM_VERSION_RANGES_FALLBACK = [
    ("12409", date(2025, 12, 10), date(2025, 12, 23)),
    ("12410", date(2025, 12, 24), date(2026, 1, 21)),
    ("12411", date(2026, 1, 22), date(2026, 2, 11)),
    ("12412", date(2026, 2, 12), None),  # 2026-02-12 ~ 현재 (end_date null = open-ended)
]

DM_TABLES_TO_TRUNCATE = (
    "dm.dm_rank",
    "dm.dm_force",
    "dm.dm_hyper",
    "dm.dm_ability",
    "dm.dm_seedring",
    "dm.dm_equipment",
    "dm.dm_hexacore",
    "dm.hyper_master",
    "dm.dm_shift_score",
    "dm.dm_balance_score",
    "dm.equipment_master",
)


def _check_character_info_complete(conn, target_date: date) -> bool:
    """maplemeta_dag.check_data_exists('character_info')와 동일 로직."""
    tables = (
        "dw.dw_equipment",
        "dw.dw_hexacore",
        "dw.dw_seteffect",
        "dw.dw_ability",
        "dw.dw_hyperstat",
    )
    date_str = target_date.strftime("%Y-%m-%d")
    counts = []
    with conn.cursor() as cur:
        for table in tables:
            cur.execute(
                f"select count(distinct ocid) from {table} where date::date = %s::date",
                (date_str,),
            )
            counts.append(cur.fetchone()[0] or 0)
    if not counts or counts[0] == 0:
        return False
    return all(c == counts[0] for c in counts[1:])


def get_dw_completed_dates(conn) -> list[date]:
    """DW에서 character_info 완료된 모든 날짜 조회 (오름차순)."""
    ensure_dw_schema(conn)
    with conn.cursor() as cur:
        cur.execute(
            """
            select distinct date::date as dt
            from dw.dw_rank
            order by dt
            """
        )
        candidates = [row[0] for row in cur.fetchall() if row and row[0]]

    completed = [d for d in candidates if _check_character_info_complete(conn, d)]
    return completed


def get_dw_all_dates(conn) -> list[date]:
    """DW에 있는 모든 날짜 조회 (완료 체크 없음). dw_rank 우선, 비어있으면 5개 character_info 테이블 union."""
    ensure_dw_schema(conn)
    with conn.cursor() as cur:
        cur.execute(
            """
            select distinct date::date as dt
            from dw.dw_rank
            order by dt
            """
        )
        dates = [row[0] for row in cur.fetchall() if row and row[0]]

    if not dates:
        with conn.cursor() as cur:
            cur.execute(
                """
                select distinct dt from (
                    select date::date as dt from dw.dw_equipment
                    union
                    select date::date from dw.dw_hexacore
                    union
                    select date::date from dw.dw_seteffect
                    union
                    select date::date from dw.dw_ability
                    union
                    select date::date from dw.dw_hyperstat
                ) t
                order by dt
                """
            )
            dates = [row[0] for row in cur.fetchall() if row and row[0]]

    return dates


def get_latest_completed_date(conn) -> Optional[date]:
    """DW에서 character_info 완료된 가장 최근 날짜 1개 반환."""
    completed = get_dw_completed_dates(conn)
    return completed[-1] if completed else None


def resolve_version_for_date(conn, target_date: date) -> Optional[str]:
    """
    날짜에 해당하는 version 반환.
    dm.version_master 우선, 없으면 DM_VERSION_RANGES_FALLBACK 사용.
    - end_date null = today (최신 버전은 오늘까지)
    - 각 버전 종료 시점 = 다음 버전 업데이트 노트 올라오기 전 날짜
    """
    today = date.today()
    with conn.cursor() as cur:
        cur.execute(
            """
            select version
            from dm.version_master
            where start_date is not null
              and start_date <= %s::date
              and (
                (end_date is null and %s::date <= current_date)
                or (end_date is not null and end_date >= %s::date)
              )
            order by start_date desc
            limit 1
            """,
            (target_date, target_date, target_date),
        )
        row = cur.fetchone()
        if row:
            return row[0]

    for version, start_d, end_d in DM_VERSION_RANGES_FALLBACK:
        if start_d <= target_date:
            if end_d is None:
                if target_date <= today:
                    return version
            elif end_d >= target_date:
                return version
    return None


def _dates_to_array_sql(dates: list[date]) -> str:
    """date 리스트를 PostgreSQL array literal로 변환."""
    return "array[" + ", ".join(f"date '{d}'" for d in dates) + "]"


def run_refresh_dashboard_dm(
    conn,
    version: str,
    character_dates: list[date],
    agg_dates: list[date],
) -> None:
    """dm.refresh_dashboard_dm 호출."""
    char_arr = _dates_to_array_sql(character_dates)
    agg_arr = _dates_to_array_sql(agg_dates)
    sql = f"""
        select dm.refresh_dashboard_dm(
            p_version => %s,
            p_character_dates => {char_arr},
            p_agg_dates => {agg_arr}
        )
    """
    with conn.cursor() as cur:
        cur.execute(sql, (version,))
    conn.commit()


def run_refresh_shift_balance_score(conn, version: str) -> None:
    """dm.refresh_shift_balance_score 호출 (balance_score + shift_score)."""
    with conn.cursor() as cur:
        cur.execute("select dm.refresh_shift_balance_score(%s)", (version,))
    conn.commit()


def truncate_dm_tables(conn) -> None:
    """DW 기반 DM 테이블 전체 truncate (full-reset용)."""
    with conn.cursor() as cur:
        for table in DM_TABLES_TO_TRUNCATE:
            cur.execute(f"truncate table {table} cascade")
    conn.commit()


def run_full_reset() -> None:
    """DM 테이블 전체 truncate 후 DW 기준으로 재적재 (DW 클렌징 후 사용)."""
    conn = get_dw_connection()
    try:
        print("DM 테이블 truncate 중...")
        truncate_dm_tables(conn)
        print("truncate 완료. DW → DM 전체 백필 시작 (force 모드)")
        run_full_backfill(force=True)
    finally:
        conn.close()


def run_full_backfill(force: bool = False) -> None:
    """전체 백필: DW 날짜 조회 → version별 refresh_dashboard_dm 호출."""
    conn = get_dw_connection()
    try:
        if force:
            completed = get_dw_all_dates(conn)
            if not completed:
                print("DW에 날짜가 없습니다.")
                return
            print(f"DW 날짜 {len(completed)}개 (force 모드, 완료 체크 생략): {[str(d) for d in completed]}")
        else:
            completed = get_dw_completed_dates(conn)
            if not completed:
                print("DW에 character_info 완료된 날짜가 없습니다.")
                return
            print(f"DW 완료 날짜 {len(completed)}개: {[str(d) for d in completed]}")

        # version별로 그룹화
        version_to_dates: dict[str, list[date]] = {}
        for d in completed:
            ver = resolve_version_for_date(conn, d)
            if ver:
                version_to_dates.setdefault(ver, []).append(d)
            else:
                print(f"  경고: {d}에 해당하는 version 없음, 건너뜀")

        for version, dates in sorted(version_to_dates.items()):
            dates_sorted = sorted(dates)
            print(f"version {version}: {[str(d) for d in dates_sorted]}")
            run_refresh_dashboard_dm(conn, version, dates_sorted, dates_sorted)
            print(f"  refresh_dashboard_dm 완료")
            run_refresh_shift_balance_score(conn, version)
            print(f"  refresh_shift_balance_score 완료")

    finally:
        conn.close()


def run_shift_score_backfill() -> None:
    """shift_score 전체 백필: dm.dm_rank에 있는 모든 version에 대해 refresh_shift_balance_score 실행."""
    conn = get_dw_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "select distinct version from dm.dm_rank where version is not null order by version"
            )
            versions = [row[0] for row in cur.fetchall() if row and row[0]]
        if not versions:
            print("dm.dm_rank에 version이 없습니다.")
            return
        print(f"shift_score 백필 대상 version {len(versions)}개: {versions}")
        for version in versions:
            run_refresh_shift_balance_score(conn, version)
            print(f"  {version} 완료")
    finally:
        conn.close()


def run_incremental_for_latest() -> Optional[str]:
    """
    최신 완료일 1개만 DM으로 refresh.
    DAG에서 호출용. 성공 시 version 반환, 실패 시 None.
    """
    conn = get_dw_connection()
    try:
        latest = get_latest_completed_date(conn)
        if not latest:
            print("DW에 character_info 완료된 날짜가 없습니다.")
            return None

        version = resolve_version_for_date(conn, latest)
        if not version:
            print(f"날짜 {latest}에 해당하는 version이 없습니다.")
            return None

        print(f"DM refresh: version={version}, date={latest}")
        run_refresh_dashboard_dm(conn, version, [latest], [latest])
        print("refresh_dashboard_dm 완료")
        return version
    finally:
        conn.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--shift-score-only",
        action="store_true",
        help="shift_score만 전체 백필 (dm.dm_rank 기준 version)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="완료 체크 없이 DW에 있는 모든 날짜를 DM으로 적재 (하위권 등 5테이블 OCID 수 불일치 시 사용)",
    )
    parser.add_argument(
        "--full-reset",
        action="store_true",
        help="DM 테이블 전체 truncate 후 DW 기준으로 재적재 (DW 클렌징 후 사용)",
    )
    args = parser.parse_args()
    if args.shift_score_only:
        run_shift_score_backfill()
    elif args.full_reset:
        run_full_reset()
    elif args.force:
        run_full_backfill(force=True)
    else:
        run_full_backfill()
