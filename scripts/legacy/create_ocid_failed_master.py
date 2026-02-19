"""
레거시 이관 스크립트:
기존 ocid_failed_{date}.json 파일들을 통합하여
dw.collect_failed_master 테이블로 적재한다.
"""
import glob
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from dw_load_utils import ensure_dw_schema, get_dw_connection, upsert_failed_master_to_db


def collect_failed_characters(data_json_dir="data_json"):
    failed_set = set()
    pattern = os.path.join(data_json_dir, "ocid_failed_*.json")
    failed_files = glob.glob(pattern)

    print(f"발견된 실패 파일: {len(failed_files)}개")
    for file_path in failed_files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        char_name = item.get("character_name")
                        if char_name:
                            failed_set.add(char_name)
                    elif isinstance(item, str):
                        failed_set.add(item)
            print(f"처리 완료: {os.path.basename(file_path)}")
        except Exception as e:
            print(f"파일 처리 오류 {file_path}: {e}")
    return failed_set


def migrate_failed_master_to_db(data_json_dir="data_json"):
    failed_set = collect_failed_characters(data_json_dir=data_json_dir)
    if not failed_set:
        print("적재할 실패 캐릭터가 없습니다.")
        return set()

    conn = get_dw_connection()
    ensure_dw_schema(conn)
    try:
        upsert_failed_master_to_db(conn, failed_set, reason="legacy_json_migration")
    finally:
        conn.close()

    print(f"DB 적재 완료: {len(failed_set)}개 캐릭터")
    return failed_set


if __name__ == "__main__":
    migrate_failed_master_to_db()
