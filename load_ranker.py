import requests, datetime
import pandas as pd
import json
import time
from config import API_KEY, DATE

headers = {
"x-nxopen-api-key": API_KEY
}

def get_dojang_ranking_all_pages(date, world_name):
    """
    특정 서버의 도장 랭킹을 1~5페이지까지 조회하여 통합
    """
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

def get_multi_world_ranking(date, worlds=['루나', '스카니아']):
    """
    여러 서버의 도장 랭킹을 조회하여 통합
    """
    all_rankings = []
    
    for world in worlds:
        print(f"\n=== {world} 서버 조회 시작 ===")
        world_rankings = get_dojang_ranking_all_pages(date, world)
        
        if world_rankings:
            all_rankings.extend(world_rankings)
            print(f"{world} 서버 조회 완료: {len(world_rankings)}명")
        else:
            print(f"{world} 서버 조회 실패")
    
    return {'ranking': all_rankings}

def create_dojang_table(json_data):
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
    with open(f"data_json/dojang_ranking_{DATE}.json", 'w', encoding='utf-8') as f:
        json.dump(json_data_to_save, f, ensure_ascii=False, indent=2)
    
    print(f"총 {len(df_korean)}명의 통합 랭킹 데이터 저장 완료")
    return df_korean

def display_table(df):
    """
    DataFrame을 보기 좋게 출력
    """
    if df is not None:
        print("\n=== 메이플스토리 무릉도장 랭킹 ===")
        print(df.to_string(index=False))
        print(f"\n총 {len(df)}명의 랭커")
    else:
        print("테이블 생성 실패")

# API 호출 - 루나, 스카니아 서버 통합 조회
date = DATE
print(f"도장 랭킹 조회 시작 (날짜: {date})")
print("조회 서버: 루나, 스카니아")

# 모든 서버 데이터 수집
all_ranking_data = get_multi_world_ranking(date)

if all_ranking_data['ranking']:
    print(f"\n전체 조회 완료: {len(all_ranking_data['ranking'])}명")
    
    # 테이블 생성 및 출력 (통합 랭킹으로 재정렬)
    table = create_dojang_table(all_ranking_data)
    display_table(table)
else:
    print("API 호출 실패 또는 데이터 없음")

