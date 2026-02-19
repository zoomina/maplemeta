import requests
import time
import sys
import os

# 상위 디렉토리의 config 모듈 import를 위한 경로 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATE, resolve_api_key
from dw_load_utils import (
    ensure_dw_schema,
    fetch_rank_records_for_date,
    get_dw_connection,
    load_failed_master_from_db,
    upsert_failed_master_to_db,
    upsert_stage_user_ocid,
)

def get_character_ocid(character_name, api_key=None):
    """
    캐릭터 이름으로 OCID 조회
    """
    if api_key is None:
        api_key = resolve_api_key("API_KEY_2")
    
    headers = {
        "x-nxopen-api-key": api_key
    }
    
    ocid_url = f"https://open.api.nexon.com/maplestory/v1/id?character_name={character_name}"
    
    try:
        response = requests.get(ocid_url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            return data.get('ocid', None)
        else:
            print(f"OCID 조회 실패 - {character_name}: Status {response.status_code}")
            return None
            
    except Exception as e:
        print(f"OCID 조회 오류 - {character_name}: {e}")
        return None

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

def create_user_ocid_table(date=None, api_key=None):
    """
    DW 랭킹 데이터에서 점유율 높은 세부직업 5개의 상위 30명씩 OCID 조회.
    실패 마스터 DB를 제외하고, 직업군별 30개를 보장한 뒤 stage 테이블에 저장.
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

        top_jobs = analyze_job_distribution(ranking_data)
        selected_players = get_top_players_by_job(ranking_data, top_jobs, 30, failed_set)
        print(f"\n총 {len(selected_players)}명의 플레이어 선별 완료")

        user_ocid_list = []
        failed_list = []
        consecutive_errors = 0
        max_consecutive_errors = 5

        for job in top_jobs:
            print(f"\n=== {job} 직업군 처리 시작 ===")
            job_players = [
                p for p in selected_players
                if (p.get('세부직업', '').strip() or p.get('직업군', '').strip()) == job
            ]

            job_success_count = 0
            job_failed_chars = []

            for idx, player in enumerate(job_players):
                character_name = player['캐릭터명']
                sub_job = player.get('세부직업', player.get('직업군', ''))

                print(f"처리 중... ({idx+1}/{len(job_players)}) {character_name} ({sub_job})")
                ocid = get_character_ocid(character_name, api_key)

                if ocid:
                    sub_job = get_job_name(player)
                    user_ocid_list.append(
                        {
                            'character_name': character_name,
                            'ocid': ocid,
                            'sub_job': sub_job,
                            'world': player['월드'],
                            'level': player['레벨'],
                            'dojang_floor': player['도장층수'],
                        }
                    )
                    job_success_count += 1
                    consecutive_errors = 0
                else:
                    print(f"OCID 조회 실패: {character_name}")
                    job_failed_chars.append(character_name)
                    failed_list.append({'index': idx, 'character_name': character_name, 'data': player})
                    consecutive_errors += 1
                    if consecutive_errors >= max_consecutive_errors:
                        print(f"\n연속 {max_consecutive_errors}건 에러 발생. 조회를 중단합니다.")
                        break

                time.sleep(0.3)

            if job_success_count < 30:
                needed = 30 - job_success_count
                print(f"\n{job}: {job_success_count}명만 성공, {needed}명 추가 필요")
                current_failed_set = failed_set | set(job_failed_chars)
                additional_players = fill_missing_players(
                    ranking_data, [job], {job: job_success_count}, current_failed_set, 30
                )

                for player in additional_players:
                    character_name = player['캐릭터명']
                    print(f"추가 조회: {character_name}")
                    ocid = get_character_ocid(character_name, api_key)
                    if ocid:
                        sub_job = get_job_name(player)
                        user_ocid_list.append(
                            {
                                'character_name': character_name,
                                'ocid': ocid,
                                'sub_job': sub_job,
                                'world': player['월드'],
                                'level': player['레벨'],
                                'dojang_floor': player['도장층수'],
                            }
                        )
                        job_success_count += 1
                    else:
                        job_failed_chars.append(character_name)
                        failed_list.append({'character_name': character_name, 'data': player})
                    time.sleep(0.3)

                print(f"{job}: 최종 {job_success_count}명 수집 완료")

        if failed_list:
            new_failed_chars = set(item['character_name'] for item in failed_list)
            upsert_failed_master_to_db(conn, new_failed_chars, reason="ocid_lookup_failed")
            print(f"\n{len(new_failed_chars)}개 실패 캐릭터를 실패 마스터 DB에 반영")

        if not user_ocid_list:
            print("수집된 유저 정보가 없습니다.")
            return None

        upsert_stage_user_ocid(conn, date, user_ocid_list)
        print(f"\n유저 마스터(stage_user_ocid) upsert 완료: date={date}")
        print(f"총 {len(user_ocid_list)}명의 유저 정보 수집")

        job_summary = {}
        for user in user_ocid_list:
            job = user['sub_job']
            job_summary[job] = job_summary.get(job, 0) + 1

        print("\n=== 직업별 수집 현황 ===")
        for job, count in job_summary.items():
            print(f"{job}: {count}명")

        return user_ocid_list
    finally:
        conn.close()

if __name__ == "__main__":
    create_user_ocid_table()
