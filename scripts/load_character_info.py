import requests
import pandas as pd
import json
import time
import sys
import os

# 상위 디렉토리의 config 모듈 import를 위한 경로 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import API_KEY, DATE

def get_character_data(ocid, date, endpoint, api_key=None):
    """
    특정 엔드포인트로 캐릭터 데이터 조회
    """
    if api_key is None:
        from config import API_KEY as api_key
    
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

def flatten_json_data(data, prefix=""):
    """
    중첩된 JSON을 평면화
    """
    flattened = {}
    
    if isinstance(data, dict):
        for key, value in data.items():
            new_key = f"{prefix}_{key}" if prefix else key
            
            if isinstance(value, dict):
                flattened.update(flatten_json_data(value, new_key))
            elif isinstance(value, list):
                flattened[new_key] = json.dumps(value, ensure_ascii=False)
            else:
                flattened[new_key] = value
    
    return flattened

def process_endpoint_data(master_data, date, endpoint_name, endpoint_url, api_key=None):
    """
    특정 엔드포인트의 데이터를 처리하여 리스트 생성
    """
    all_data = []
    failed_list = []
    consecutive_errors = 0
    max_consecutive_errors = 5
    
    for idx, user in enumerate(master_data):
        ocid = user['ocid']
        character_name = user['character_name']
        
        print(f"[{endpoint_name}] 처리 중... ({idx+1}/{len(master_data)}) {character_name}")
        
        data = get_character_data(ocid, date, endpoint_url, api_key)
        
        if data:
            # 기본 정보 추가
            data['ocid'] = ocid
            data['character_name'] = character_name
            all_data.append(data)
            consecutive_errors = 0  # 성공시 연속 에러 카운트 리셋
        else:
            print(f"[{endpoint_name}] 조회 실패: {character_name}")
            failed_list.append({'index': idx, 'ocid': ocid, 'character_name': character_name})
            consecutive_errors += 1
            
            # 연속 에러가 5건 이상이면 중단
            if consecutive_errors >= max_consecutive_errors:
                print(f"\n[{endpoint_name}] 연속 {max_consecutive_errors}건 에러 발생. 조회를 중단합니다.")
                # 진행상황 저장
                progress_file = f"data_json/{endpoint_name}_progress_{date}.json"
                with open(progress_file, 'w', encoding='utf-8') as f:
                    json.dump({
                        'endpoint': endpoint_name,
                        'last_processed_index': idx,
                        'total_count': len(master_data),
                        'success_count': len(all_data),
                        'failed_count': len(failed_list),
                        'failed_list': failed_list
                    }, f, ensure_ascii=False, indent=2)
                print(f"[{endpoint_name}] 진행상황 저장: {progress_file}")
                break
        
        # API 호출 제한 방지 (초당 5건)
        time.sleep(0.3)
    
    # 실패한 항목들 재시도
    if failed_list and consecutive_errors < max_consecutive_errors:
        print(f"\n[{endpoint_name}] 실패한 {len(failed_list)}건 재시도 중...")
        retry_failed = []
        
        for failed_item in failed_list:
            ocid = failed_item['ocid']
            character_name = failed_item['character_name']
            print(f"[{endpoint_name}] 재시도: {character_name}")
            
            data = get_character_data(ocid, date, endpoint_url, api_key)
            if data:
                data['ocid'] = ocid
                data['character_name'] = character_name
                all_data.append(data)
                print(f"[{endpoint_name}] 재시도 성공: {character_name}")
            else:
                retry_failed.append(failed_item)
                print(f"[{endpoint_name}] 재시도 실패: {character_name}")
            
            time.sleep(0.3)
        
        # 최종 실패 목록 업데이트
        failed_list = retry_failed
        
        # 최종 실패 목록이 있으면 저장
        if failed_list:
            failed_file = f"data_json/{endpoint_name}_failed_{date}.json"
            with open(failed_file, 'w', encoding='utf-8') as f:
                json.dump(failed_list, f, ensure_ascii=False, indent=2)
            print(f"[{endpoint_name}] 실패 목록 저장: {failed_file} ({len(failed_list)}건)")
    
    return all_data

def load_character_info_by_endpoint(date=None, api_key=None):
    """
    엔드포인트별로 캐릭터 정보를 수집하여 개별 JSON 파일로 저장
    """
    if date is None:
        date = DATE
    
    # 유저 마스터 테이블 읽기
    master_file = f"data_json/user_ocid_{date}.json"
    
    try:
        with open(master_file, 'r', encoding='utf-8') as f:
            master_data = json.load(f)
        print(f"유저 마스터 테이블 로드 완료: {len(master_data)}명")
    except FileNotFoundError:
        print(f"유저 마스터 파일을 찾을 수 없습니다: {master_file}")
        return None
    
    # API 엔드포인트 정의
    endpoints = {
        # 'stat': 'stat',
        'equipment': 'item-equipment', 
        'hexamatrix': 'hexamatrix',
        'set_effect': 'set-effect',
        'ability': 'ability',
        'hyper_stat': 'hyper-stat'
    }
    
    created_files = []
    
    # 각 엔드포인트별로 데이터 수집 및 파일 생성
    for endpoint_name, endpoint_url in endpoints.items():
        print(f"\n=== {endpoint_name.upper()} 데이터 수집 시작 ===")
        
        endpoint_data = process_endpoint_data(master_data, date, endpoint_name, endpoint_url, api_key)
        
        if endpoint_data:
            output_file = f"data_json/character_{endpoint_name}_{date}.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(endpoint_data, f, ensure_ascii=False, indent=2)
            print(f"{endpoint_name} 데이터 저장 완료: {output_file} ({len(endpoint_data)}개)")
            created_files.append(output_file)
        else:
            print(f"{endpoint_name} 데이터 없음")
    
    print(f"\n총 {len(created_files)}개 파일 생성 완료:")
    for file in created_files:
        print(f"- {file}")
    
    return created_files

if __name__ == "__main__":
    load_character_info_by_endpoint()
