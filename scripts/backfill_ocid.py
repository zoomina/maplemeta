"""
백필 스크립트: dw.stage_user_ocid에서 직업군별로 30개 미만인 경우 추가 조회
수동 실행용 (한 번만 실행)
"""
import requests
import time
import sys
import os

# 상위 디렉토리의 config 모듈 import를 위한 경로 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import resolve_api_key
from dw_load_utils import (
    ensure_dw_schema,
    fetch_rank_records_for_date,
    fetch_stage_user_ocid,
    get_dw_connection,
    load_failed_master_from_db,
    upsert_failed_master_to_db,
    upsert_stage_user_ocid,
)

def get_character_ocid(character_name, api_key):
    """
    캐릭터 이름으로 OCID 조회
    """
    ocid_url = f"https://open.api.nexon.com/maplestory/v1/id?character_name={character_name}"
    
    try:
        response = requests.get(ocid_url, headers={"x-nxopen-api-key": api_key})
        
        if response.status_code == 200:
            data = response.json()
            return data.get('ocid', None)
        else:
            print(f"OCID 조회 실패 - {character_name}: Status {response.status_code}")
            return None
            
    except Exception as e:
        print(f"OCID 조회 오류 - {character_name}: {e}")
        return None

def get_job_name(player):
    """
    플레이어의 직업명 반환 (세부직업 우선, 없으면 직업군)
    """
    sub_job = player.get('세부직업', '').strip()
    main_job = player.get('직업군', '').strip()
    
    if not sub_job or sub_job == '':
        return main_job
    return sub_job

def backfill_ocid(date):
    """
    특정 날짜의 stage_user_ocid를 백필
    """
    api_key = resolve_api_key("API_KEY_2")
    conn = get_dw_connection()
    ensure_dw_schema(conn)
    try:
        failed_set = load_failed_master_from_db(conn)
        print(f"실패 마스터 DB 로드: {len(failed_set)}개 캐릭터 제외")

        existing_users = fetch_stage_user_ocid(conn, date)
        if not existing_users:
            print(f"dw.stage_user_ocid에 날짜 {date} 데이터가 없습니다.")
            return None
        print(f"기존 stage_user_ocid 로드 완료: {len(existing_users)}명")

        ranking_data = fetch_rank_records_for_date(conn, date)
        if not ranking_data:
            print(f"dw.dw_rank에 날짜 {date} 데이터가 없습니다.")
            return None
        print(f"랭킹 데이터 로드 완료(DB): {len(ranking_data)}명")

        job_count = {}
        existing_char_names = set()
        for user in existing_users:
            job = user.get('sub_job', '')
            job_count[job] = job_count.get(job, 0) + 1
            existing_char_names.add(user.get('character_name', ''))

        print("\n=== 직업군별 현재 개수 ===")
        for job, count in sorted(job_count.items()):
            print(f"{job}: {count}명")

        jobs_to_fill = [job for job, count in job_count.items() if count < 30]
        if not jobs_to_fill:
            print("\n모든 직업군이 30개 이상입니다. 백필 불필요.")
            return existing_users

        print(f"\n백필 필요한 직업군: {jobs_to_fill}")

        additional_users = []
        new_failed_chars = []

        for job in jobs_to_fill:
            current_count = job_count.get(job, 0)
            needed = 30 - current_count

            print(f"\n=== {job} 직업군 백필 시작 (현재 {current_count}명, {needed}명 필요) ===")

            job_players = []
            for player in ranking_data:
                player_job = get_job_name(player)
                character_name = player.get('캐릭터명', '')

                if player_job == job and character_name not in failed_set and character_name not in existing_char_names:
                    job_players.append(player)

            additional_players = job_players[current_count:current_count + needed]
            print(f"{job}: {len(additional_players)}명 추가 조회 시작")

            for idx, player in enumerate(additional_players):
                character_name = player['캐릭터명']
                print(f"처리 중... ({idx+1}/{len(additional_players)}) {character_name}")

                ocid = get_character_ocid(character_name, api_key)
                if ocid:
                    sub_job = player.get('세부직업', '').strip() or player.get('직업군', '').strip()
                    additional_users.append(
                        {
                            'character_name': character_name,
                            'ocid': ocid,
                            'sub_job': sub_job,
                            'world': player['월드'],
                            'level': player.get('레벨'),
                            'dojang_floor': player['도장층수'],
                        }
                    )
                    existing_char_names.add(character_name)
                else:
                    print(f"OCID 조회 실패: {character_name}")
                    new_failed_chars.append(character_name)

                time.sleep(0.3)

            print(f"{job}: 누적 {len(additional_users)}명 추가 완료")

        if additional_users:
            upsert_stage_user_ocid(conn, date, additional_users)
            print(f"\n백필 완료: {len(additional_users)}명 추가 (dw.stage_user_ocid)")
        else:
            print("\n추가된 유저가 없습니다.")

        if new_failed_chars:
            upsert_failed_master_to_db(conn, new_failed_chars, reason="ocid_backfill_failed")
            print(f"{len(new_failed_chars)}개 신규 실패 캐릭터를 실패 마스터 DB에 반영")

        final_users = fetch_stage_user_ocid(conn, date)
        final_job_count = {}
        for user in final_users:
            job = user.get('sub_job', '')
            final_job_count[job] = final_job_count.get(job, 0) + 1

        print("\n=== 최종 직업군별 개수 ===")
        for job, count in sorted(final_job_count.items()):
            print(f"{job}: {count}명")

        return final_users
    finally:
        conn.close()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='OCID 백필 스크립트')
    parser.add_argument('--date', type=str, help='백필할 날짜 (YYYY-MM-DD)', required=True)
    
    args = parser.parse_args()
    
    print(f"백필 시작: {args.date}")
    backfill_ocid(args.date)
