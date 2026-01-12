import requests
import pandas as pd
import json
import time
from config import API_KEY, DATE

headers = {
    "x-nxopen-api-key": API_KEY
}

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

def get_top_players_by_job(ranking_data, target_jobs, top_n=30):
    """
    특정 직업군의 상위 N명 선별
    """
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
            
            if player_job == job:
                job_players.append(player)
        
        # 해당 직업의 상위 N명 선택 (이미 통합순위로 정렬되어 있음)
        top_players = job_players[:top_n]
        selected_players.extend(top_players)
        
        print(f"{job}: {len(job_players)}명 중 상위 {len(top_players)}명 선택")
    
    return selected_players

def create_user_ocid_table(date=DATE):
    """
    도장 랭킹 JSON에서 점유율 높은 세부직업 5개의 상위 30명씩 OCID 조회
    """
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
    
    # 상위 직업별로 상위 30명씩 선별
    selected_players = get_top_players_by_job(ranking_data, top_jobs, 30)
    
    print(f"\n총 {len(selected_players)}명의 플레이어 선별 완료")
    
    user_ocid_list = []
    failed_list = []
    consecutive_errors = 0
    max_consecutive_errors = 5
    
    for idx, player in enumerate(selected_players):
        character_name = player['캐릭터명']
        sub_job = player.get('세부직업', player.get('직업군', ''))
        
        print(f"처리 중... ({idx+1}/{len(selected_players)}) {character_name} ({sub_job})")
        
        # OCID 조회
        ocid = get_character_ocid(character_name)
        
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
            consecutive_errors = 0  # 성공시 연속 에러 카운트 리셋
        else:
            print(f"OCID 조회 실패: {character_name}")
            failed_list.append({'index': idx, 'character_name': character_name, 'data': player})
            consecutive_errors += 1
            
            # 연속 에러가 5건 이상이면 중단
            if consecutive_errors >= max_consecutive_errors:
                print(f"\n연속 {max_consecutive_errors}건 에러 발생. 조회를 중단합니다.")
                # 진행상황 저장
                progress_file = f"data_json/ocid_progress_{date}.json"
                with open(progress_file, 'w', encoding='utf-8') as f:
                    json.dump({
                        'last_processed_index': idx,
                        'total_count': len(selected_players),
                        'success_count': len(user_ocid_list),
                        'failed_count': len(failed_list),
                        'failed_list': failed_list,
                        'selected_jobs': top_jobs
                    }, f, ensure_ascii=False, indent=2)
                print(f"진행상황 저장: {progress_file}")
                break
        
        # API 호출 제한 방지를 위한 딜레이 (초당 5건 제한)
        time.sleep(0.3)
    
    # 실패한 항목들 재시도
    if failed_list and consecutive_errors < max_consecutive_errors:
        print(f"\n실패한 {len(failed_list)}건 재시도 중...")
        retry_failed = []
        
        for failed_item in failed_list:
            character_name = failed_item['character_name']
            print(f"재시도: {character_name}")
            
            ocid = get_character_ocid(character_name)
            if ocid:
                player_data = failed_item['data']
                # 세부직업이 없으면 직업군 사용
                sub_job = player_data.get('세부직업', '').strip()
                if not sub_job or sub_job == '':
                    sub_job = player_data.get('직업군', '').strip()
                    
                user_ocid_list.append({
                    'character_name': character_name,
                    'ocid': ocid,
                    'sub_job': sub_job,
                    'world': player_data['월드'],
                    'level': player_data['레벨'],
                    'dojang_floor': player_data['도장층수']
                })
                print(f"재시도 성공: {character_name}")
            else:
                retry_failed.append(failed_item)
                print(f"재시도 실패: {character_name}")
            
            time.sleep(0.3)
        
        # 최종 실패 목록 업데이트
        failed_list = retry_failed
    
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
        
        # 실패 목록이 있으면 별도 저장
        if failed_list:
            failed_file = f"data_json/ocid_failed_{date}.json"
            with open(failed_file, 'w', encoding='utf-8') as f:
                json.dump(failed_list, f, ensure_ascii=False, indent=2)
            print(f"실패 목록 저장: {failed_file} ({len(failed_list)}건)")
        
        return output_file
    else:
        print("수집된 유저 정보가 없습니다.")
        return None

# 실행
if __name__ == "__main__":
    date = DATE
    create_user_ocid_table(date)