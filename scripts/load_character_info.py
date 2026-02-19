import requests
import time
import sys
import os

# 상위 디렉토리의 config 모듈 import를 위한 경로 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATE, resolve_api_key
from dw_load_utils import (
    ensure_dw_schema,
    fetch_stage_user_ocid,
    get_dw_connection,
    parse_ability_records,
    parse_equipment_records,
    parse_hexacore_records,
    parse_hyperstat_records,
    parse_seteffect_records,
    upsert_ability,
    upsert_equipment,
    upsert_hexacore,
    upsert_hyperstat,
    upsert_seteffect,
)

def get_character_data(ocid, date, endpoint, api_key=None):
    """
    특정 엔드포인트로 캐릭터 데이터 조회
    """
    if api_key is None:
        api_key = resolve_api_key("API_KEY_2")
    
    headers = {
        "x-nxopen-api-key": api_key
    }
    
    url = f"https://open.api.nexon.com/maplestory/v1/character/{endpoint}?ocid={ocid}&date={date}"
    
    try:
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"{endpoint} 조회 실패 - OCID {ocid}: Status {response.status_code}")
            return None
            
    except Exception as e:
        print(f"{endpoint} 조회 오류 - OCID {ocid}: {e}")
        return None

def process_endpoint_data(master_data, date, endpoint_name, endpoint_url, api_key=None):
    """
    특정 엔드포인트의 데이터를 처리하여 리스트 생성.
    """
    all_data = []
    consecutive_errors = 0
    max_consecutive_errors = 5
    
    for idx, user in enumerate(master_data):
        ocid = user['ocid']
        character_name = user['character_name']
        
        print(f"[{endpoint_name}] 처리 중... ({idx+1}/{len(master_data)}) {character_name}")
        
        data = get_character_data(ocid, date, endpoint_url, api_key)
        
        if data:
            # 기본 정보 추가
            data['date'] = date
            data['ocid'] = ocid
            data['character_name'] = character_name
            all_data.append(data)
            consecutive_errors = 0  # 성공시 연속 에러 카운트 리셋
        else:
            print(f"[{endpoint_name}] 조회 실패: {character_name}")
            consecutive_errors += 1
            
            # 연속 에러가 5건 이상이면 중단
            if consecutive_errors >= max_consecutive_errors:
                print(f"\n[{endpoint_name}] 연속 {max_consecutive_errors}건 에러 발생. 조회를 중단합니다.")
                break
        
        # API 호출 제한 방지 (초당 5건)
        time.sleep(0.3)
    return all_data

def load_character_info_by_endpoint(date=None, api_key=None):
    """
    엔드포인트별로 캐릭터 정보를 수집하여 DW 테이블로 직접 upsert
    """
    if date is None:
        date = DATE

    conn = get_dw_connection()
    ensure_dw_schema(conn)
    try:
        master_data = fetch_stage_user_ocid(conn, date)
        if not master_data:
            print(f"stage_user_ocid에서 날짜 {date} 데이터를 찾지 못했습니다.")
            return None
        print(f"유저 마스터(stage_user_ocid) 로드 완료: {len(master_data)}명")

        endpoints = {
            'equipment': ('item-equipment', parse_equipment_records, upsert_equipment),
            'hexamatrix': ('hexamatrix', parse_hexacore_records, upsert_hexacore),
            'set_effect': ('set-effect', parse_seteffect_records, upsert_seteffect),
            'ability': ('ability', parse_ability_records, upsert_ability),
            'hyper_stat': ('hyper-stat', parse_hyperstat_records, upsert_hyperstat),
        }

        loaded_tables = []

        for endpoint_name, (endpoint_url, parse_fn, upsert_fn) in endpoints.items():
            print(f"\n=== {endpoint_name.upper()} 데이터 수집 시작 ===")
            endpoint_data = process_endpoint_data(master_data, date, endpoint_name, endpoint_url, api_key)
            if not endpoint_data:
                print(f"{endpoint_name} 데이터 없음")
                continue

            rows = parse_fn(endpoint_data)
            upsert_fn(conn, rows)
            loaded_tables.append((endpoint_name, len(rows)))
            print(f"{endpoint_name} upsert 완료: rows={len(rows)}")

        if not loaded_tables:
            print("적재된 엔드포인트 데이터가 없습니다.")
            return None

        print("\n엔드포인트 upsert 완료:")
        for endpoint_name, row_count in loaded_tables:
            print(f"- {endpoint_name}: {row_count}")

        return loaded_tables
    finally:
        conn.close()

if __name__ == "__main__":
    load_character_info_by_endpoint()
