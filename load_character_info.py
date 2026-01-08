import requests
import pandas as pd
import json
import time
from config import API_KEY

headers = {
    "x-nxopen-api-key": API_KEY
}

def get_character_data(ocid, date, endpoint):
    """
    특정 엔드포인트로 캐릭터 데이터 조회
    """
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
def process_endpoint_data(df_master, date, endpoint_name, endpoint_url):
    """
    특정 엔드포인트의 데이터를 처리하여 DataFrame 생성
    """
    all_data = []
    
    for idx, row in df_master.iterrows():
        ocid = row['ocid']
        character_name = row['character_name']
        
        print(f"[{endpoint_name}] 처리 중... ({idx+1}/{len(df_master)}) {character_name}")
        
        data = get_character_data(ocid, date, endpoint_url)
        
        if data:
            # 리스트 형태의 데이터가 있는지 확인
            list_fields = []
            base_data = {'ocid': ocid, 'character_name': character_name}
            
            for key, value in data.items():
                if isinstance(value, list) and len(value) > 0:
                    # 리스트의 첫 번째 요소가 dict인 경우 확장 필요
                    if isinstance(value[0], dict):
                        list_fields.append((key, value))
                    else:
                        base_data[key] = json.dumps(value, ensure_ascii=False)
                else:
                    if isinstance(value, dict):
                        flattened = flatten_json_data(value, key)
                        base_data.update(flattened)
                    else:
                        base_data[key] = value
            
            # 리스트 데이터가 있는 경우 각각을 별도 행으로 확장
            if list_fields:
                for field_name, field_data in list_fields:
                    for i, item in enumerate(field_data):
                        row_data = base_data.copy()
                        row_data['list_field'] = field_name
                        row_data['list_index'] = i
                        
                        if isinstance(item, dict):
                            flattened_item = flatten_json_data(item, field_name)
                            row_data.update(flattened_item)
                        else:
                            row_data[f"{field_name}_value"] = item
                        
                        all_data.append(row_data)
            else:
                # 리스트 데이터가 없는 경우 단일 행으로 추가
                all_data.append(base_data)
        
        # API 호출 제한 방지 (초당 5건)
        time.sleep(0.3)
    
    return pd.DataFrame(all_data) if all_data else None

def load_character_info_by_endpoint(date="2025-01-05"):
    """
    엔드포인트별로 캐릭터 정보를 수집하여 개별 CSV 파일로 저장
    """
    # 유저 마스터 테이블 읽기
    master_file = f"user_master_{date.replace('-', '')}.csv"
    
    try:
        df_master = pd.read_csv(master_file, encoding='utf-8-sig')
        print(f"유저 마스터 테이블 로드 완료: {len(df_master)}명")
    except FileNotFoundError:
        print(f"유저 마스터 파일을 찾을 수 없습니다: {master_file}")
        return None
    
    # API 엔드포인트 정의
    endpoints = {
        'stat': 'stat',
        'equipment': 'item-equipment', 
        'vmatrix': 'vmatrix',
        'hexamatrix': 'hexamatrix',
        'symbol': 'symbol-equipment',
        'set_effect': 'set-effect',
        'ability': 'ability',
        'hyper_stat': 'hyper-stat'
    }
    
    created_files = []
    date_str = date.replace('-', '')
    
    # 각 엔드포인트별로 데이터 수집 및 파일 생성
    for endpoint_name, endpoint_url in endpoints.items():
        print(f"\n=== {endpoint_name.upper()} 데이터 수집 시작 ===")
        
        df_endpoint = process_endpoint_data(df_master, date, endpoint_name, endpoint_url)
        
        if df_endpoint is not None and not df_endpoint.empty:
            output_file = f"character_{endpoint_name}_{date_str}.csv"
            df_endpoint.to_csv(output_file, index=False, encoding='utf-8-sig')
            print(f"{endpoint_name} 데이터 저장 완료: {output_file} ({len(df_endpoint)}행)")
            created_files.append(output_file)
        else:
            print(f"{endpoint_name} 데이터 없음")
    
    print(f"\n총 {len(created_files)}개 파일 생성 완료:")
    for file in created_files:
        print(f"- {file}")
    
    return created_files

# 실행
if __name__ == "__main__":
    date = "2025-01-05"
    load_character_info_by_endpoint(date)