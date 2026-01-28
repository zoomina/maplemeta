import requests
import pandas as pd
import json
import time
import sys
import os

# 상위 디렉토리의 config 모듈 import를 위한 경로 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import API_KEY, DATE

def load_failed_master(master_file_path="data_json/ocid_failed_master.json"):
    """
    실패 마스터 파일 로드 (없으면 빈 세트 반환)
    """
    if os.path.exists(master_file_path):
        try:
            with open(master_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # 리스트 형태인지 확인
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

def save_failed_master(failed_set, master_file_path="data_json/ocid_failed_master.json"):
    """
    실패 마스터 파일 저장
    """
    failed_list = sorted(list(failed_set))
    with open(master_file_path, 'w', encoding='utf-8') as f:
        json.dump(failed_list, f, ensure_ascii=False, indent=2)

def get_character_ocid(character_name, api_key=None):
    """
    캐릭터 이름으로 OCID 조회
    """
    if api_key is None:
        from config import API_KEY as api_key
    
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
            sub_job = player.get('세부직업', '').strip()
            main_job = player.get('직업군', '').strip()
            
            # 세부직업이 없거나 빈 문자열인 경우 직업군 사용
            if not sub_job or sub_job == '':
                player_job = main_job
            else:
                player_job = sub_job
            
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
            sub_job = player.get('세부직업', '').strip()
            main_job = player.get('직업군', '').strip()
            
            if not sub_job or sub_job == '':
                player_job = main_job
            else:
                player_job = sub_job
            
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
    도장 랭킹 JSON에서 점유율 높은 세부직업 5개의 상위 30명씩 OCID 조회
    실패 마스터 리스트 제외하고, 직업군별 30개를 보장
    """
    if date is None:
        date = DATE
    
    # 실패 마스터 파일 로드
    failed_set = load_failed_master()
    print(f"실패 마스터 리스트 로드: {len(failed_set)}개 캐릭터 제외")
    
    # 랭킹 JSON 파일 읽기
    ranking_file = f"data_json/dojang_ranking_{date}.json"
    
    try:
        with open(ranking_file, 'r', encoding='utf-8') as f:
            ranking_data = json.load(f)
        
        print(f"랭킹 파일 로드 완료: 총 {len(ranking_data)}명")
    except FileNotFoundError:
        print(f"랭킹 파일을 찾을 수 없습니다: {ranking_file}")
        return None
    
    # 세부직업별 점유율 분석
    top_jobs = analyze_job_distribution(ranking_data)
    
    # 상위 직업별로 상위 30명씩 선별 (실패 리스트 제외)
    selected_players = get_top_players_by_job(ranking_data, top_jobs, 30, failed_set)
    
    print(f"\n총 {len(selected_players)}명의 플레이어 선별 완료")
    
    # 직업별로 그룹화하여 OCID 조회
    user_ocid_list = []
    failed_list = []
    consecutive_errors = 0
    max_consecutive_errors = 5
    
    # 직업별로 처리하여 각 직업군당 30개 보장
    for job in top_jobs:
        print(f"\n=== {job} 직업군 처리 시작 ===")
        job_players = [p for p in selected_players if 
                      (p.get('세부직업', '').strip() or p.get('직업군', '').strip()) == job]
        
        job_success_count = 0
        job_failed_chars = []
        
        for idx, player in enumerate(job_players):
            character_name = player['캐릭터명']
            sub_job = player.get('세부직업', player.get('직업군', ''))
            
            print(f"처리 중... ({idx+1}/{len(job_players)}) {character_name} ({sub_job})")
            
            # OCID 조회
            ocid = get_character_ocid(character_name, api_key)
            
            if ocid:
                # 세부직업이 없으면 직업군 사용
                sub_job = player.get('세부직업', '').strip()
                if not sub_job or sub_job == '':
                    sub_job = player.get('직업군', '').strip()
                    
                user_ocid_list.append({
                    'character_name': character_name,
                    'ocid': ocid,
                    'sub_job': sub_job,
                    'world': player['월드'],
                    'level': player['레벨'],
                    'dojang_floor': player['도장층수']
                })
                job_success_count += 1
                consecutive_errors = 0  # 성공시 연속 에러 카운트 리셋
            else:
                print(f"OCID 조회 실패: {character_name}")
                job_failed_chars.append(character_name)
                failed_list.append({'index': idx, 'character_name': character_name, 'data': player})
                consecutive_errors += 1
                
                # 연속 에러가 5건 이상이면 중단
                if consecutive_errors >= max_consecutive_errors:
                    print(f"\n연속 {max_consecutive_errors}건 에러 발생. 조회를 중단합니다.")
                    break
            
            # API 호출 제한 방지를 위한 딜레이 (초당 5건 제한)
            time.sleep(0.3)
        
        # 직업군별로 30개 미만이면 추가로 채우기
        if job_success_count < 30:
            needed = 30 - job_success_count
            print(f"\n{job}: {job_success_count}명만 성공, {needed}명 추가 필요")
            
            # 추가 플레이어 선별 (실패 리스트 + 이미 실패한 캐릭터 제외)
            current_failed_set = failed_set | set(job_failed_chars)
            additional_players = fill_missing_players(
                ranking_data, [job], {job: job_success_count}, current_failed_set, 30
            )
            
            # 추가 플레이어 OCID 조회
            for player in additional_players:
                character_name = player['캐릭터명']
                print(f"추가 조회: {character_name}")
                
                ocid = get_character_ocid(character_name, api_key)
                if ocid:
                    sub_job = player.get('세부직업', '').strip()
                    if not sub_job or sub_job == '':
                        sub_job = player.get('직업군', '').strip()
                    
                    user_ocid_list.append({
                        'character_name': character_name,
                        'ocid': ocid,
                        'sub_job': sub_job,
                        'world': player['월드'],
                        'level': player['레벨'],
                        'dojang_floor': player['도장층수']
                    })
                    job_success_count += 1
                else:
                    job_failed_chars.append(character_name)
                    failed_list.append({'character_name': character_name, 'data': player})
                
                time.sleep(0.3)
            
            print(f"{job}: 최종 {job_success_count}명 수집 완료")
    
    # 실패한 캐릭터들을 마스터 파일에 추가
    if failed_list:
        new_failed_chars = set(item['character_name'] for item in failed_list)
        failed_set.update(new_failed_chars)
        save_failed_master(failed_set)
        print(f"\n{len(new_failed_chars)}개 새로운 실패 캐릭터를 마스터 파일에 추가")
    
    # JSON으로 저장
    if user_ocid_list:
        output_file = f"data_json/user_ocid_{date}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(user_ocid_list, f, ensure_ascii=False, indent=2)
        print(f"\n유저 마스터 테이블 저장 완료: {output_file}")
        print(f"총 {len(user_ocid_list)}명의 유저 정보 수집")
        
        # 직업별 수집 현황 출력
        job_summary = {}
        for user in user_ocid_list:
            job = user['sub_job']
            job_summary[job] = job_summary.get(job, 0) + 1
        
        print("\n=== 직업별 수집 현황 ===")
        for job, count in job_summary.items():
            print(f"{job}: {count}명")
        
        return output_file
    else:
        print("수집된 유저 정보가 없습니다.")
        return None

if __name__ == "__main__":
    create_user_ocid_table()
