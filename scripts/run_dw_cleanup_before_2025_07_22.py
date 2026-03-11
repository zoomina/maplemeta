#!/usr/bin/env python3
"""
DW에서 2025-07-22 이전 날짜 데이터 전부 삭제.
대상: dw_rank, stage_user_ocid, dw_ability, dw_equipment, dw_hexacore, dw_seteffect, dw_hyperstat
"""
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent))
from dw_load_utils import get_dw_connection

CUTOFF = "2025-07-22"


def main():
    conn = get_dw_connection()
    conn.autocommit = False
    cur = conn.cursor()

    # 1) 삭제 전 점검
    cur.execute(
        """
        select table_name, rows_before_cutoff from (
            select 'dw_rank', count(*)::bigint from dw.dw_rank where date < %s::date
            union all select 'stage_user_ocid', count(*) from dw.stage_user_ocid where date < %s::date
            union all select 'dw_ability', count(*) from dw.dw_ability where date::date < %s::date
            union all select 'dw_equipment', count(*) from dw.dw_equipment where date::date < %s::date
            union all select 'dw_hexacore', count(*) from dw.dw_hexacore where date::date < %s::date
            union all select 'dw_seteffect', count(*) from dw.dw_seteffect where date::date < %s::date
            union all select 'dw_hyperstat', count(*) from dw.dw_hyperstat where date::date < %s::date
        ) t(table_name, rows_before_cutoff)
        order by table_name
        """,
        (CUTOFF,) * 7,
    )
    rows = cur.fetchall()
    print("=== 삭제 전 (date < 2025-07-22) ===")
    for t, n in rows:
        print(f"  {t}: {n}")
    total = sum(r[1] for r in rows)
    print(f"  총: {total}")

    if total == 0:
        print("삭제할 행 없음. 종료.")
        cur.close()
        conn.close()
        return

    # 2) 삭제 실행
    cur.execute("delete from dw.dw_hyperstat where date::date < %s::date", (CUTOFF,))
    cur.execute("delete from dw.dw_seteffect where date::date < %s::date", (CUTOFF,))
    cur.execute("delete from dw.dw_hexacore where date::date < %s::date", (CUTOFF,))
    cur.execute("delete from dw.dw_equipment where date::date < %s::date", (CUTOFF,))
    cur.execute("delete from dw.dw_ability where date::date < %s::date", (CUTOFF,))
    cur.execute("delete from dw.stage_user_ocid where date < %s::date", (CUTOFF,))
    cur.execute("delete from dw.dw_rank where date < %s::date", (CUTOFF,))
    conn.commit()
    print("=== 삭제 완료 (commit) ===")

    # 3) 삭제 후 점검
    cur.execute(
        """
        select table_name, remaining from (
            select 'dw_rank', count(*)::bigint from dw.dw_rank where date < %s::date
            union all select 'stage_user_ocid', count(*) from dw.stage_user_ocid where date < %s::date
            union all select 'dw_ability', count(*) from dw.dw_ability where date::date < %s::date
            union all select 'dw_equipment', count(*) from dw.dw_equipment where date::date < %s::date
            union all select 'dw_hexacore', count(*) from dw.dw_hexacore where date::date < %s::date
            union all select 'dw_seteffect', count(*) from dw.dw_seteffect where date::date < %s::date
            union all select 'dw_hyperstat', count(*) from dw.dw_hyperstat where date::date < %s::date
        ) t(table_name, remaining)
        order by table_name
        """,
        (CUTOFF,) * 7,
    )
    rows = cur.fetchall()
    print("=== 삭제 후 잔여 (0이어야 함) ===")
    for t, n in rows:
        print(f"  {t}: {n}")
    cur.close()
    conn.close()
    print("완료.")


if __name__ == "__main__":
    main()
