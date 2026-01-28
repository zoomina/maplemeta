import requests
import pandas as pd
import json
import time
import sys
import os

# 상위 디렉토리의 config 모듈 import를 위한 경로 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import API_KEY, DATE

def get_dojang_ranking_all_pages(date, world_name, api_key=None):
    """
    특정 서버의 도장 랭킹을 1~5페이지까지 조회하여 통합
    """
    if api_key is None:
        from config import API_KEY as api_key
    
    headers = {
        "x-nxopen-api-key": api_key
    }
    
    all_rankings = []
    
    for page in range(1, 6):  # 1~5페이지
        print(f"[{world_name}] 페이지 {page} 조회 중...")
        
        # 한글 서버명을 URL 인코딩
        world_encoded = world_name.encode('utf-8').hex()
        world_encoded = '%' + '%'.join([world_encoded[i:i+2] for i in range(0, len(world_encoded), 2)])
        
        dojang_url = f"https://open.api.nexon.com/maplestory/v1/ranking/dojang?date={date}&world_name={world_encoded}&difficulty=1&page={page}"
        
        try:
            response = requests.get(dojang_url, headers=headers)
            
            if response.status_code == 200:
                json_data = response.json()
                
                if 'ranking' in json_data and json_data['ranking']:
                    all_rankings.extend(json_data['ranking'])
                    print(f"[{world_name}] 페이지 {page}: {len(json_data['ranking'])}명 조회 완료")
                else:
                    print(f"[{world_name}] 페이지 {page}: 데이터 없음")
                    break
            else:
                print(f"[{world_name}] 페이지 {page} 조회 실패: Status {response.status_code}")
                break
                
        except Exception as e:
            print(f"[{world_name}] 페이지 {page} 조회 오류: {e}")
            break
        
        # API 호출 제한 방지
        time.sleep(0.3)
    
    return all_rankings

def get_multi_world_ranking(date, worlds=['루나', '스카니아'], api_key=None):
    """
    여러 서버의 도장 랭킹을 조회하여 통합
    """
    all_rankings = []
    
    for world in worlds:
        print(f"\n=== {world} 서버 조회 시작 ===")
        world_rankings = get_dojang_ranking_all_pages(date, world, api_key)
        
        if world_rankings:
            all_rankings.extend(world_rankings)
            print(f"{world} 서버 조회 완료: {len(world_rankings)}명")
        else:
            print(f"{world} 서버 조회 실패")
    
    return {'ranking': all_rankings}

def create_dojang_table(json_data, date):
    """
    도장 랭킹 JSON 데이터를 pandas DataFrame으로 변환하고 재정렬
    """
    if 'ranking' not in json_data or not json_data['ranking']:
        print("Error: 'ranking' key not found or empty in JSON data")
        return None
    
    # JSON의 ranking 리스트를 DataFrame으로 변환
    df = pd.DataFrame(json_data['ranking'])
    
    # 층수 기준 내림차순, 동일 층수는 기록시간(초) 오름차순으로 정렬
    df_sorted = df.sort_values(['dojang_floor', 'dojang_time_record'], 
                              ascending=[False, True]).reset_index(drop=True)
    
    # 새로운 통합 랭킹 부여
    df_sorted['unified_ranking'] = range(1, len(df_sorted) + 1)
    
    # 컬럼명을 한글로 변경
    column_mapping = {
        'date': '날짜',
        'unified_ranking': '통합순위',
        'ranking': '서버내순위',
        'dojang_floor': '도장층수',
        'dojang_time_record': '기록시간(초)',
        'character_name': '캐릭터명',
        'world_name': '월드',
        'class_name': '직업군',
        'sub_class_name': '세부직업',
        'character_level': '레벨'
    }
    
    df_korean = df_sorted.rename(columns=column_mapping)
    
    # 시간을 분:초 형식으로 변환
    df_korean['기록시간(분:초)'] = df_korean['기록시간(초)'].apply(
        lambda x: f"{x//60}:{x%60:02d}"
    )

    # JSON으로 저장
    json_data_to_save = df_korean.to_dict('records')
    output_path = f"data_json/dojang_ranking_{date}.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(json_data_to_save, f, ensure_ascii=False, indent=2)
    
    print(f"총 {len(df_korean)}명의 통합 랭킹 데이터 저장 완료: {output_path}")
    return df_korean

def load_ranker(date=None, api_key=None):
    """
    도장 랭킹 데이터를 수집하는 메인 함수
    """
    if date is None:
        date = DATE
    
    print(f"도장 랭킹 조회 시작 (날짜: {date})")
    print("조회 서버: 루나, 스카니아")
    
    # 모든 서버 데이터 수집
    all_ranking_data = get_multi_world_ranking(date, api_key=api_key)
    
    if all_ranking_data['ranking']:
        user_count = len(all_ranking_data['ranking'])
        print(f"\n전체 조회 완료: {user_count}명")
        
        # 유저 수 1000명 미만 체크 (경고만, 실패는 안함)
        if user_count < 1000:
            warning_msg = f"⚠️ 경고: 랭커 수가 1000명 미만입니다. (현재: {user_count}명, 날짜: {date})"
            print(f"\n{'='*60}")
            print(warning_msg)
            print(f"{'='*60}\n")
            # 나중에 알림 연동 시 사용할 수 있도록 로그에 명확히 기록
            # Airflow 로그에서 확인 가능
        
        # 테이블 생성 및 저장
        table = create_dojang_table(all_ranking_data, date)
        return table
    else:
        print("API 호출 실패 또는 데이터 없음")
        return None

if __name__ == "__main__":
    load_ranker()
