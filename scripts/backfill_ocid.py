"""
백필 스크립트: 기존 user_ocid_{date}.json 파일에서 직업군별로 30개 미만인 경우 추가 조회
수동 실행용 (한 번만 실행)
"""
import requests
import json
import time
import sys
import os
from datetime import datetime

# 상위 디렉토리의 config 모듈 import를 위한 경로 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import API_KEY

headers = {
    "x-nxopen-api-key": API_KEY
}

def load_failed_master(master_file_path="data_json/ocid_failed_master.json"):
    """
    실패 마스터 파일 로드
    """
    if os.path.exists(master_file_path):
        try:
            with open(master_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    return set(item if isinstance(item, str) else item.get('character_name', '') for item in data)
                elif isinstance(data, dict) and 'failed_characters' in data:
                    return set(data['failed_characters'])
                else:
                    return set()
        except Exception as e:
            print(f"마스터 파일 로드 오류: {e}")
            return set()
    return set()

def get_character_ocid(character_name):
    """
    캐릭터 이름으로 OCID 조회
    """
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
    특정 날짜의 user_ocid 파일을 백필
    """
    # 실패 마스터 파일 로드
    failed_set = load_failed_master()
    print(f"실패 마스터 리스트 로드: {len(failed_set)}개 캐릭터 제외")
    
    # 기존 user_ocid 파일 로드
    user_ocid_file = f"data_json/user_ocid_{date}.json"
    
    if not os.path.exists(user_ocid_file):
        print(f"파일을 찾을 수 없습니다: {user_ocid_file}")
        return None
    
    try:
        with open(user_ocid_file, 'r', encoding='utf-8') as f:
            existing_users = json.load(f)
        print(f"기존 파일 로드 완료: {len(existing_users)}명")
    except Exception as e:
        print(f"파일 로드 오류: {e}")
        return None
    
    # 랭킹 파일 로드
    ranking_file = f"data_json/dojang_ranking_{date}.json"
    
    if not os.path.exists(ranking_file):
        print(f"랭킹 파일을 찾을 수 없습니다: {ranking_file}")
        return None
    
    try:
        with open(ranking_file, 'r', encoding='utf-8') as f:
            ranking_data = json.load(f)
        print(f"랭킹 파일 로드 완료: {len(ranking_data)}명")
    except Exception as e:
        print(f"랭킹 파일 로드 오류: {e}")
        return None
    
    # 직업군별 현재 개수 확인
    job_count = {}
    existing_char_names = set()
    
    for user in existing_users:
        job = user.get('sub_job', '')
        job_count[job] = job_count.get(job, 0) + 1
        existing_char_names.add(user.get('character_name', ''))
    
    print("\n=== 직업군별 현재 개수 ===")
    for job, count in sorted(job_count.items()):
        print(f"{job}: {count}명")
    
    # 직업군별로 30개 미만인 경우 추가 조회
    jobs_to_fill = [job for job, count in job_count.items() if count < 30]
    
    if not jobs_to_fill:
        print("\n모든 직업군이 30개 이상입니다. 백필 불필요.")
        return user_ocid_file
    
    print(f"\n백필 필요한 직업군: {jobs_to_fill}")
    
    # 각 직업군별로 부족한 수만큼 추가 조회
    additional_users = []
    new_failed_chars = []
    
    for job in jobs_to_fill:
        current_count = job_count.get(job, 0)
        needed = 30 - current_count
        
        print(f"\n=== {job} 직업군 백필 시작 (현재 {current_count}명, {needed}명 필요) ===")
        
        # 해당 직업군의 플레이어들 필터링 (실패 리스트 제외, 이미 있는 캐릭터 제외)
        job_players = []
        for player in ranking_data:
            player_job = get_job_name(player)
            character_name = player.get('캐릭터명', '')
            
            if player_job == job and character_name not in failed_set and character_name not in existing_char_names:
                job_players.append(player)
        
        # 현재 개수 이후부터 필요한 수만큼 가져오기
        additional_players = job_players[current_count:current_count + needed]
        
        print(f"{job}: {len(additional_players)}명 추가 조회 시작")
        
        for idx, player in enumerate(additional_players):
            character_name = player['캐릭터명']
            print(f"처리 중... ({idx+1}/{len(additional_players)}) {character_name}")
            
            ocid = get_character_ocid(character_name)
            
            if ocid:
                sub_job = player.get('세부직업', '').strip()
                if not sub_job or sub_job == '':
                    sub_job = player.get('직업군', '').strip()
                
                additional_users.append({
                    'character_name': character_name,
                    'ocid': ocid,
                    'sub_job': sub_job,
                    'world': player['월드'],
                    'level': player['레벨'],
                    'dojang_floor': player['도장층수']
                })
                existing_char_names.add(character_name)
            else:
                print(f"OCID 조회 실패: {character_name}")
                new_failed_chars.append(character_name)
            
            time.sleep(0.3)
        
        print(f"{job}: {len(additional_users)}명 추가 완료")
    
    # 기존 데이터에 추가
    if additional_users:
        existing_users.extend(additional_users)
        
        # 파일 저장
        with open(user_ocid_file, 'w', encoding='utf-8') as f:
            json.dump(existing_users, f, ensure_ascii=False, indent=2)
        
        print(f"\n백필 완료: {len(additional_users)}명 추가")
        print(f"총 {len(existing_users)}명의 유저 정보")
        
        # 최종 직업군별 개수 출력
        final_job_count = {}
        for user in existing_users:
            job = user.get('sub_job', '')
            final_job_count[job] = final_job_count.get(job, 0) + 1
        
        print("\n=== 최종 직업군별 개수 ===")
        for job, count in sorted(final_job_count.items()):
            print(f"{job}: {count}명")
        
        # 새로운 실패 캐릭터를 마스터 파일에 추가
        if new_failed_chars:
            failed_set.update(new_failed_chars)
            master_file = "data_json/ocid_failed_master.json"
            failed_list = sorted(list(failed_set))
            with open(master_file, 'w', encoding='utf-8') as f:
                json.dump(failed_list, f, ensure_ascii=False, indent=2)
            print(f"\n{len(new_failed_chars)}개 새로운 실패 캐릭터를 마스터 파일에 추가")
        
        return user_ocid_file
    else:
        print("\n추가된 유저가 없습니다.")
        return user_ocid_file

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='OCID 백필 스크립트')
    parser.add_argument('--date', type=str, help='백필할 날짜 (YYYY-MM-DD)', required=True)
    
    args = parser.parse_args()
    
    print(f"백필 시작: {args.date}")
    backfill_ocid(args.date)
