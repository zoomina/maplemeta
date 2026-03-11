#!/usr/bin/env python3
"""
DW 클렌징: date가 수요일이 아닌 데이터 전부 삭제.
대상: dw_ability, dw_equipment, dw_hexacore, dw_hyperstat, dw_rank, dw_seteffect
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


def main():
    conn = get_dw_connection()
    conn.autocommit = False
    cur = conn.cursor()

    # 1) 삭제 전 점검: 수요일이 아닌 데이터 건수
    cur.execute(
        """
        select table_name, non_wed_rows from (
            select 'dw_rank', count(*)::bigint from dw.dw_rank where extract(dow from date) != 3
            union all select 'dw_ability', count(*) from dw.dw_ability where extract(dow from date::date) != 3
            union all select 'dw_equipment', count(*) from dw.dw_equipment where extract(dow from date::date) != 3
            union all select 'dw_hexacore', count(*) from dw.dw_hexacore where extract(dow from date::date) != 3
            union all select 'dw_hyperstat', count(*) from dw.dw_hyperstat where extract(dow from date::date) != 3
            union all select 'dw_seteffect', count(*) from dw.dw_seteffect where extract(dow from date::date) != 3
        ) t(table_name, non_wed_rows)
        order by table_name
        """
    )
    rows = cur.fetchall()
    print("=== 삭제 전 (수요일이 아닌 데이터) ===")
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
    cur.execute("delete from dw.dw_hyperstat where extract(dow from date::date) != 3")
    cur.execute("delete from dw.dw_seteffect where extract(dow from date::date) != 3")
    cur.execute("delete from dw.dw_hexacore where extract(dow from date::date) != 3")
    cur.execute("delete from dw.dw_equipment where extract(dow from date::date) != 3")
    cur.execute("delete from dw.dw_ability where extract(dow from date::date) != 3")
    cur.execute("delete from dw.dw_rank where extract(dow from date) != 3")
    conn.commit()
    print("=== 삭제 완료 (commit) ===")

    # 3) 삭제 후 점검
    cur.execute(
        """
        select table_name, remaining from (
            select 'dw_rank', count(*)::bigint from dw.dw_rank where extract(dow from date) != 3
            union all select 'dw_ability', count(*) from dw.dw_ability where extract(dow from date::date) != 3
            union all select 'dw_equipment', count(*) from dw.dw_equipment where extract(dow from date::date) != 3
            union all select 'dw_hexacore', count(*) from dw.dw_hexacore where extract(dow from date::date) != 3
            union all select 'dw_hyperstat', count(*) from dw.dw_hyperstat where extract(dow from date::date) != 3
            union all select 'dw_seteffect', count(*) from dw.dw_seteffect where extract(dow from date::date) != 3
        ) t(table_name, remaining)
        order by table_name
        """
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
