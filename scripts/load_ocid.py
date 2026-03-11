import json
import os
import requests
import sys
import time

# 상위 디렉토리의 config 모듈 import를 위한 경로 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATE, resolve_api_key
from dw_load_utils import (
    ensure_dw_schema,
    fetch_rank_records_for_date,
    get_dw_connection,
    load_failed_master_from_db,
    upsert_rank_ocid_by_character,
    upsert_api_retry_queue,
    upsert_failed_master_to_db,
    upsert_stage_user_ocid,
)

def _default_payload_dir():
    return os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data_json", "_airflow_payloads")


PAYLOAD_DIR = os.getenv("AIRFLOW_PAYLOAD_DIR") or os.getenv("PAYLOAD_DIR") or _default_payload_dir()

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

def get_character_ocid(character_name, api_key=None):
    """
    캐릭터 이름으로 OCID 조회
    """
    if api_key is None:
        api_key = resolve_api_key("API_KEY")
    
    headers = {
        "x-nxopen-api-key": api_key
    }
    
    ocid_url = f"https://open.api.nexon.com/maplestory/v1/id?character_name={character_name}"
    
    try:
        response = requests.get(ocid_url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            return data.get('ocid', None), None
        else:
            api_error = _extract_api_error(response)
            print(
                f"OCID 조회 실패 - {character_name}: "
                f"Status {api_error['http_status']}, code={api_error['error_code']}, msg={api_error['error_message']}"
            )
            return None, api_error
            
    except Exception as e:
        print(f"OCID 조회 오류 - {character_name}: {e}")
        return None, {
            "http_status": None,
            "error_code": "REQUEST_EXCEPTION",
            "error_name": "Request Exception",
            "error_message": str(e),
            "api_response_body": None,
        }

def analyze_job_distribution(ranking_data):
    """
    세부직업별 점유율 분석하여 상위 5개 직업 선정
    """
    job_count = {}
    
    for player in ranking_data:
        sub_job = player.get('세부직업', '').strip()
        main_job = player.get('직업군', '').strip()
        
        # 세부직업이 없거나 빈 문자열인 경우 직업군 사용
        if not sub_job or sub_job == '':
            job_name = main_job
        else:
            job_name = sub_job
        
        if job_name:
            job_count[job_name] = job_count.get(job_name, 0) + 1
    
    # 점유율 기준으로 정렬
    sorted_jobs = sorted(job_count.items(), key=lambda x: x[1], reverse=True)
    
    print("\n=== 세부직업별 점유율 ===")
    for i, (job, count) in enumerate(sorted_jobs[:10]):
        percentage = (count / len(ranking_data)) * 100
        print(f"{i+1}. {job}: {count}명 ({percentage:.1f}%)")
    
    # 상위 5개 직업 반환
    top_5_jobs = [job for job, count in sorted_jobs[:5]]
    print(f"\n선정된 상위 5개 직업: {top_5_jobs}")
    
    return top_5_jobs


def get_job_name(player):
    sub_job = player.get('세부직업', '').strip()
    main_job = player.get('직업군', '').strip()
    return sub_job or main_job

def get_top_players_by_job(ranking_data, target_jobs, top_n=30, failed_set=None):
    """
    특정 직업군의 상위 N명 선별 (실패 리스트 제외)
    """
    if failed_set is None:
        failed_set = set()
    
    selected_players = []
    
    for job in target_jobs:
        job_players = []
        
        for player in ranking_data:
            player_job = get_job_name(player)
            
            # 실패 리스트에 있는 캐릭터 제외
            character_name = player.get('캐릭터명', '')
            if character_name in failed_set:
                continue
            
            if player_job == job:
                job_players.append(player)
        
        # 해당 직업의 상위 N명 선택 (이미 통합순위로 정렬되어 있음)
        top_players = job_players[:top_n]
        selected_players.extend(top_players)
        
        print(f"{job}: {len(job_players)}명 중 상위 {len(top_players)}명 선택 (실패 리스트 제외)")
    
    return selected_players

def fill_missing_players(ranking_data, target_jobs, current_count_by_job, failed_set, target_count=30):
    """
    직업군별로 부족한 수만큼 추가 플레이어 선별
    """
    additional_players = []
    
    for job in target_jobs:
        current_count = current_count_by_job.get(job, 0)
        needed = target_count - current_count
        
        if needed <= 0:
            continue
        
        print(f"{job}: 현재 {current_count}명, {needed}명 추가 필요")
        
        job_players = []
        for player in ranking_data:
            player_job = get_job_name(player)
            
            character_name = player.get('캐릭터명', '')
            if character_name in failed_set:
                continue
            
            if player_job == job:
                job_players.append(player)
        
        # 이미 선택된 플레이어는 제외하고 추가 선별
        # current_count 이후부터 needed만큼 가져오기
        additional = job_players[current_count:current_count + needed]
        additional_players.extend(additional)
        print(f"{job}: {len(additional)}명 추가 선별")
    
    return additional_players

def _ensure_payload_dir():
    os.makedirs(PAYLOAD_DIR, exist_ok=True)


def _payload_file_path(prefix: str, date: str, run_id: str = None) -> str:
    safe_run_id = (run_id or "manual").replace(":", "_").replace("/", "_")
    _ensure_payload_dir()
    return os.path.join(PAYLOAD_DIR, f"{prefix}_{date}_{safe_run_id}.json")


def collect_user_ocid_data(date=None, api_key=None):
    """
    OCID 수집 단계.
    DB 적재는 하지 않고, 적재에 필요한 페이로드만 반환한다.
    """
    if date is None:
        date = DATE

    conn = get_dw_connection()
    ensure_dw_schema(conn)
    try:
        failed_set = load_failed_master_from_db(conn)
        print(f"실패 마스터 DB 로드: {len(failed_set)}개 캐릭터 제외")

        ranking_data = fetch_rank_records_for_date(conn, date)
        if not ranking_data:
            print(f"dw.dw_rank에서 날짜 {date} 데이터를 찾지 못했습니다.")
            return None
        print(f"랭킹 데이터 로드 완료(DB): 총 {len(ranking_data)}명")

        selected_players = [p for p in ranking_data if (p.get("캐릭터명") or "").strip() not in failed_set]
        print(f"실패 마스터 제외 후 수집 대상: {len(selected_players)}명")

        user_ocid_list = []
        failed_list = []
        retry_items = []
        consecutive_errors = 0
        max_consecutive_errors = 5

        for idx, player in enumerate(selected_players):
            character_name = (player.get("캐릭터명") or "").strip()
            if not character_name:
                continue
            if (idx + 1) % 100 == 0 or idx == 0:
                print(f"처리 중... ({idx+1}/{len(selected_players)})")
            ocid, api_error = get_character_ocid(character_name, api_key)

            if ocid:
                sub_job = get_job_name(player)
                user_ocid_list.append(
                    {
                        "character_name": character_name,
                        "ocid": ocid,
                        "sub_job": sub_job,
                        "world": player.get("월드"),
                        "level": player.get("레벨"),
                        "dojang_floor": player.get("도장층수"),
                    }
                )
                consecutive_errors = 0
            else:
                print(f"OCID 조회 실패: {character_name}")
                failed_list.append({"character_name": character_name, "data": player})
                retry_items.append(
                    {
                        "endpoint": "ocid_lookup",
                        "target_date": date,
                        "ocid": f"CHAR:{character_name}",
                        "character_name": character_name,
                        "http_status": api_error.get("http_status") if api_error else None,
                        "error_code": api_error.get("error_code") if api_error else None,
                        "error_name": api_error.get("error_name") if api_error else None,
                        "error_message": api_error.get("error_message") if api_error else "unknown_error",
                        "api_response_body": api_error.get("api_response_body") if api_error else None,
                    }
                )
                consecutive_errors += 1
                if consecutive_errors >= max_consecutive_errors:
                    print(f"\n연속 {max_consecutive_errors}건 에러 발생. 조회를 중단합니다.")
                    break

            time.sleep(0.3)

        job_summary = {}
        for user in user_ocid_list:
            job = user.get("sub_job") or ""
            job_summary[job] = job_summary.get(job, 0) + 1

        print(f"\n수집 완료: 성공 {len(user_ocid_list)}명, 실패 {len(failed_list)}명")
        if job_summary:
            print("=== 직업별 수집 현황 ===")
            for job, count in sorted(job_summary.items(), key=lambda x: -x[1])[:15]:
                print(f"  {job}: {count}명")

        failed_chars = sorted({item["character_name"] for item in failed_list})
        return {
            "date": date,
            "user_ocid_list": user_ocid_list,
            "retry_items": retry_items,
            "failed_characters": failed_chars,
            "job_summary": job_summary,
        }
    finally:
        conn.close()


def write_ocid_payload(payload: dict, run_id: str = None) -> str:
    payload_date = payload.get("date") or DATE
    path = _payload_file_path("ocid_collect", payload_date, run_id)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    return path


def read_ocid_payload(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_user_ocid_payload(payload: dict, retry_delay_hours: int = 3):
    """
    OCID 적재 단계.
    - 실패/재시도 큐 반영
    - stage -> dw_rank.ocid 반영
    """
    date = payload.get("date")
    users = payload.get("user_ocid_list") or []
    retry_items = payload.get("retry_items") or []
    failed_characters = payload.get("failed_characters") or []

    conn = get_dw_connection()
    ensure_dw_schema(conn)
    try:
        if failed_characters:
            upsert_failed_master_to_db(conn, failed_characters, reason="ocid_lookup_failed")
            print(f"\n{len(failed_characters)}개 실패 캐릭터를 실패 마스터 DB에 반영")
        if retry_items:
            upsert_api_retry_queue(conn, retry_items, retry_delay_hours=retry_delay_hours)
            print(f"{len(retry_items)}건 재시도 큐 적재")

        if not users:
            raise RuntimeError(f"[OCID LOAD] 적재 가능한 사용자 데이터가 없습니다. date={date}")

        upsert_stage_user_ocid(conn, date, users)
        upsert_rank_ocid_by_character(conn, date, users)
        print(f"\n유저 마스터(stage_user_ocid) upsert 완료: date={date}")
        print(f"dw.dw_rank OCID 동기화 완료: users={len(users)}")
        return {"date": date, "loaded_users": len(users)}
    finally:
        conn.close()


def create_user_ocid_table(date=None, api_key=None):
    """
    레거시 호환 함수.
    수집 + 적재를 한 번에 수행한다.
    """
    payload = collect_user_ocid_data(date=date, api_key=api_key)
    return load_user_ocid_payload(payload, retry_delay_hours=3)


if __name__ == "__main__":
    create_user_ocid_table()
