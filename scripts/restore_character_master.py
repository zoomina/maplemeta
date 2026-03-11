#!/usr/bin/env python3
"""
dm.character_master 복구 스크립트.

TRUNCATE 등으로 character_master가 비었을 때, CSV + character_color.md 기반으로 복구합니다.
- .cursor/docs/character_master.csv: job, group, type, img
- .cursor/docs/character_color.md: job -> color (hex)

실행: python scripts/restore_character_master.py
"""
from __future__ import annotations

import csv
import os
import re
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR / "scripts"))

from dw_load_utils import get_dw_connection

CSV_PATH = BASE_DIR / ".cursor" / "docs" / "character_master.csv"
COLOR_PATH = BASE_DIR / ".cursor" / "docs" / "character_color.md"

# CSV 직업명 -> character_color.md 직업명 매핑 (형식 차이)
JOB_ALIAS = {
    "아크메이지(썬,콜)": "썬콜",
    "아크메이지(불,독)": "불독",
    "듀얼블레이더": "듀얼블레이드",
}


def load_color_map() -> dict[str, str]:
    """character_color.md에서 직업명 -> hex 색상 매핑 추출."""
    color_map: dict[str, str] = {}
    if not COLOR_PATH.exists():
        return color_map

    # 형식: "직업명 — #HEXCODE" 또는 "직업명 — #HEX"
    pattern = re.compile(r"^([가-힣a-zA-Z0-9]+)\s*[—\-]\s*(#[0-9A-Fa-f]{6})\s*$")
    for line in COLOR_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("(") or "규칙" in line or "Base" in line:
            continue
        m = pattern.match(line)
        if m:
            job, hex_color = m.groups()
            color_map[job] = hex_color
    return color_map


def load_csv_rows() -> list[dict]:
    """character_master.csv 로드."""
    rows = []
    with open(CSV_PATH, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            job = (row.get("직업 이름") or "").strip()
            if not job:
                continue
            rows.append({
                "job": job,
                "group": (row.get("직업군") or "").strip() or None,
                "type": (row.get("계열") or "").strip() or None,
                "img": (row.get("이미지") or "").strip() or None,
            })
    return rows


def build_character_master_rows() -> list[dict]:
    """CSV + color로 dm.character_master용 행 구성."""
    color_map = load_color_map()
    csv_rows = load_csv_rows()

    result = []
    for r in csv_rows:
        job = r["job"]
        color = color_map.get(JOB_ALIAS.get(job, job))
        result.append({
            "job": job,
            "group": r["group"],
            "type": r["type"],
            "img": r["img"],
            "color": color,
            "description": None,
            "link_skill_icon": None,
            "link_skill_name": None,
            "img_full": None,
        })
    return result


def upsert_character_master(conn, rows: list[dict]) -> int:
    """dm.character_master에 upsert."""
    sql = """
        insert into dm.character_master (
            job, "group", type, img, color,
            description, link_skill_icon, link_skill_name, img_full
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        on conflict (job) do update set
            "group" = excluded."group",
            type = excluded.type,
            img = excluded.img,
            color = coalesce(excluded.color, dm.character_master.color),
            description = coalesce(excluded.description, dm.character_master.description),
            link_skill_icon = coalesce(excluded.link_skill_icon, dm.character_master.link_skill_icon),
            link_skill_name = coalesce(excluded.link_skill_name, dm.character_master.link_skill_name),
            img_full = coalesce(excluded.img_full, dm.character_master.img_full)
    """
    count = 0
    with conn.cursor() as cur:
        for r in rows:
            cur.execute(
                sql,
                (
                    r["job"],
                    r["group"],
                    r["type"],
                    r["img"],
                    r["color"],
                    r["description"],
                    r["link_skill_icon"],
                    r["link_skill_name"],
                    r["img_full"],
                ),
            )
            count += 1
    conn.commit()
    return count


def main() -> None:
    if not CSV_PATH.exists():
        print(f"❌ CSV 없음: {CSV_PATH}")
        sys.exit(1)

    rows = build_character_master_rows()
    print(f"📋 CSV에서 {len(rows)}개 직업 로드")

    conn = get_dw_connection()
    try:
        upsert_character_master(conn, rows)
        print(f"✅ dm.character_master 복구 완료: {len(rows)}개 직업 upsert")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
