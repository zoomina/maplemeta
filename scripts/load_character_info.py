import requests
import time
import sys
import os
import json

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
    upsert_api_retry_queue,
    upsert_seteffect,
)

PAYLOAD_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data_json", "_airflow_payloads")
API_ERROR_CATALOG = {
    "OPENAPI00001": {"http_status": 500, "response_name": "Internal Server Error", "description": "Internal server error"},
    "OPENAPI00002": {"http_status": 403, "response_name": "Forbidden", "description": "Unauthorized access"},
    "OPENAPI00003": {"http_status": 400, "response_name": "Bad Request", "description": "Invalid identifier"},
    "OPENAPI00004": {"http_status": 400, "response_name": "Bad Request", "description": "Missing or invalid parameter"},
    "OPENAPI00005": {"http_status": 400, "response_name": "Bad Request", "description": "Invalid API key"},
    "OPENAPI00006": {"http_status": 400, "response_name": "Bad Request", "description": "Invalid game or API path"},
    "OPENAPI00007": {"http_status": 429, "response_name": "Too Many Requests", "description": "API call limit exceeded"},
    "OPENAPI00009": {"http_status": 400, "response_name": "Bad Request", "description": "Data being prepared"},
    "OPENAPI00010": {"http_status": 400, "response_name": "Bad Request", "description": "Service under maintenance"},
    "OPENAPI00011": {"http_status": 503, "response_name": "Service Unavailable", "description": "API under maintenance"},
}


def _extract_api_error(response):
    try:
        body = response.json()
    except Exception:
        body = {"raw_text": response.text[:500] if response.text else None}

    error_obj = body.get("error") if isinstance(body, dict) else None
    error_code = None
    error_message = None
    if isinstance(error_obj, dict):
        error_code = error_obj.get("name")
        error_message = error_obj.get("message")
    if not error_code and isinstance(body, dict):
        error_code = body.get("error_code") or body.get("name")
    if not error_message and isinstance(body, dict):
        error_message = body.get("error_message") or body.get("message")

    catalog = API_ERROR_CATALOG.get(error_code, {})
    return {
        "http_status": response.status_code,
        "error_code": error_code,
        "error_name": catalog.get("response_name") or response.reason,
        "error_message": error_message or catalog.get("description") or "unknown_api_error",
        "api_response_body": body,
    }


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
            return response.json(), None
        else:
            api_error = _extract_api_error(response)
            print(
                f"{endpoint} 조회 실패 - OCID {ocid}: "
                f"Status {api_error['http_status']}, code={api_error['error_code']}, msg={api_error['error_message']}"
            )
            return None, api_error
            
    except Exception as e:
        print(f"{endpoint} 조회 오류 - OCID {ocid}: {e}")
        return None, {
            "http_status": None,
            "error_code": "REQUEST_EXCEPTION",
            "error_name": "Request Exception",
            "error_message": str(e),
            "api_response_body": None,
        }

def process_endpoint_data(master_data, date, endpoint_name, endpoint_url, api_key=None):
    """
    특정 엔드포인트의 데이터를 처리하여 리스트 생성.
    """
    all_data = []
    retry_items = []
    consecutive_errors = 0
    max_consecutive_errors = 5
    
    for idx, user in enumerate(master_data):
        ocid = user['ocid']
        character_name = user['character_name']
        
        print(f"[{endpoint_name}] 처리 중... ({idx+1}/{len(master_data)}) {character_name}")
        
        data, api_error = get_character_data(ocid, date, endpoint_url, api_key)
        
        if data:
            # 기본 정보 추가
            data['date'] = date
            data['ocid'] = ocid
            data['character_name'] = character_name
            all_data.append(data)
            consecutive_errors = 0  # 성공시 연속 에러 카운트 리셋
        else:
            print(f"[{endpoint_name}] 조회 실패: {character_name}")
            retry_items.append(
                {
                    "endpoint": endpoint_name,
                    "target_date": date,
                    "ocid": ocid,
                    "character_name": character_name,
                    "http_status": api_error.get("http_status") if api_error else None,
                    "error_code": api_error.get("error_code") if api_error else None,
                    "error_name": api_error.get("error_name") if api_error else None,
                    "error_message": api_error.get("error_message") if api_error else "unknown_error",
                    "api_response_body": api_error.get("api_response_body") if api_error else None,
                }
            )
            consecutive_errors += 1
            
            # 연속 에러가 5건 이상이면 중단
            if consecutive_errors >= max_consecutive_errors:
                print(f"\n[{endpoint_name}] 연속 {max_consecutive_errors}건 에러 발생. 조회를 중단합니다.")
                break
        
        # API 호출 제한 방지 (초당 5건)
        time.sleep(0.3)
    return all_data, retry_items

def _ensure_payload_dir():
    os.makedirs(PAYLOAD_DIR, exist_ok=True)


def _payload_file_path(prefix: str, date: str, run_id: str = None) -> str:
    safe_run_id = (run_id or "manual").replace(":", "_").replace("/", "_")
    _ensure_payload_dir()
    return os.path.join(PAYLOAD_DIR, f"{prefix}_{date}_{safe_run_id}.json")


def collect_character_info_data(date=None, api_key=None):
    """
    character_info 수집 단계.
    DB 적재는 수행하지 않고, 적재 가능한 페이로드만 반환한다.
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
        endpoint_payloads = {}

        for endpoint_name, (endpoint_url, parse_fn, upsert_fn) in endpoints.items():
            print(f"\n=== {endpoint_name.upper()} 데이터 수집 시작 ===")
            endpoint_data, retry_items = process_endpoint_data(master_data, date, endpoint_name, endpoint_url, api_key)
            endpoint_payloads[endpoint_name] = {
                "endpoint_data": endpoint_data,
                "retry_items": retry_items,
            }
            loaded_tables.append((endpoint_name, len(endpoint_data)))
            print(f"{endpoint_name} 수집 완료: records={len(endpoint_data)}, retry={len(retry_items)}")

        return {
            "date": date,
            "endpoint_payloads": endpoint_payloads,
        }
    finally:
        conn.close()


def write_character_info_payload(payload: dict, run_id: str = None) -> str:
    payload_date = payload.get("date") or DATE
    path = _payload_file_path("character_collect", payload_date, run_id)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    return path


def read_character_info_payload(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_character_info_payload(payload: dict, retry_delay_hours: int = 3):
    """
    character_info 적재 단계.
    """
    conn = get_dw_connection()
    ensure_dw_schema(conn)
    try:
        endpoints = {
            'equipment': ('item-equipment', parse_equipment_records, upsert_equipment),
            'hexamatrix': ('hexamatrix', parse_hexacore_records, upsert_hexacore),
            'set_effect': ('set-effect', parse_seteffect_records, upsert_seteffect),
            'ability': ('ability', parse_ability_records, upsert_ability),
            'hyper_stat': ('hyper-stat', parse_hyperstat_records, upsert_hyperstat),
        }
        endpoint_payloads = payload.get("endpoint_payloads") or {}

        loaded_tables = []
        for endpoint_name, (_, parse_fn, upsert_fn) in endpoints.items():
            endpoint_bundle = endpoint_payloads.get(endpoint_name) or {}
            endpoint_data = endpoint_bundle.get("endpoint_data") or []
            retry_items = endpoint_bundle.get("retry_items") or []

            if retry_items:
                upsert_api_retry_queue(conn, retry_items, retry_delay_hours=retry_delay_hours)
                print(f"{endpoint_name} 재시도 큐 적재: {len(retry_items)}건")
            if not endpoint_data:
                print(f"{endpoint_name} 데이터 없음")
                continue

            rows = parse_fn(endpoint_data)
            upsert_fn(conn, rows)
            loaded_tables.append((endpoint_name, len(rows)))
            print(f"{endpoint_name} upsert 완료: rows={len(rows)}")

        if not loaded_tables:
            raise RuntimeError("[CHARACTER LOAD] 적재된 엔드포인트 데이터가 없습니다.")

        print("\n엔드포인트 upsert 완료:")
        for endpoint_name, row_count in loaded_tables:
            print(f"- {endpoint_name}: {row_count}")
        return loaded_tables
    finally:
        conn.close()


def load_character_info_by_endpoint(date=None, api_key=None):
    """
    레거시 호환 함수.
    수집 + 적재를 한 번에 수행한다.
    """
    payload = collect_character_info_data(date=date, api_key=api_key)
    return load_character_info_payload(payload, retry_delay_hours=3)

if __name__ == "__main__":
    load_character_info_by_endpoint()
