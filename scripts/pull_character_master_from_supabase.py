#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Supabase에 있는 character_master를 로컬 dm.character_master로 가져옵니다.

실행: python scripts/pull_character_master_from_supabase.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR / "scripts"))

try:
    from dotenv import load_dotenv
    load_dotenv(BASE_DIR / ".env")
except ImportError:
    pass

import requests

from dw_load_utils import get_dw_connection

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = (
    os.getenv("SUPABASE_SERVICE_KEY")
    or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
)
if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError(
        "SUPABASE_URL and SUPABASE_SERVICE_KEY (or SUPABASE_SERVICE_ROLE_KEY) 환경 변수 필요"
    )
_REST_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": "Bearer {}".format(SUPABASE_KEY),
    "Content-Type": "application/json",
}


def fetch_character_master_from_supabase() -> list[dict]:
    """Supabase REST API로 character_master 전체 조회 (dm 스키마 우선, 없으면 public)."""
    for profile in ["dm", None]:
        headers = dict(_REST_HEADERS)
        if profile:
            headers["Accept-Profile"] = profile
        r = requests.get(
            "{}/rest/v1/character_master".format(SUPABASE_URL),
            headers=headers,
            params={"select": "*"},
        )
        if r.status_code == 200:
            return r.json()
    r.raise_for_status()
    return r.json()


def upsert_to_local(conn, rows: list[dict]) -> int:
    """로컬 dm.character_master에 upsert."""
    cols = ["job", '"group"', "type", "img", "color", "description", "link_skill_icon", "link_skill_name", "img_full"]
    sql = """
        insert into dm.character_master ({})
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        on conflict (job) do update set
            "group" = excluded."group",
            type = excluded.type,
            img = excluded.img,
            color = excluded.color,
            description = excluded.description,
            link_skill_icon = excluded.link_skill_icon,
            link_skill_name = excluded.link_skill_name,
            img_full = excluded.img_full
    """.format(", ".join(cols))
    count = 0
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(
                sql,
                (
                    row.get("job"),
                    row.get("group"),
                    row.get("type"),
                    row.get("img"),
                    row.get("color"),
                    row.get("description"),
                    row.get("link_skill_icon"),
                    row.get("link_skill_name"),
                    row.get("img_full"),
                ),
            )
            count += 1
    conn.commit()
    return count


def main() -> None:
    if not SUPABASE_KEY:
        print("SUPABASE_SERVICE_KEY 또는 SUPABASE_SERVICE_ROLE_KEY 환경 변수 필요")
        sys.exit(1)

    print("Supabase에서 character_master 조회 중...")
    rows = fetch_character_master_from_supabase()
    if not rows:
        print("Supabase에 character_master 데이터가 없습니다.")
        sys.exit(1)

    print("   {}개 행 조회됨".format(len(rows)))

    conn = get_dw_connection()
    try:
        upsert_to_local(conn, rows)
        print("dm.character_master 복구 완료: {}개 upsert".format(len(rows)))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
