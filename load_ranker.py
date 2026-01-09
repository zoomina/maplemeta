import requests, datetime
import pandas as pd
from config import API_KEY, DATE

headers = {
"x-nxopen-api-key": API_KEY
}

def create_dojang_table(json_data):
    """
    도장 랭킹 JSON 데이터를 pandas DataFrame으로 변환
    """
    if 'ranking' not in json_data:
        print("Error: 'ranking' key not found in JSON data")
        return None
    
    # JSON의 ranking 리스트를 DataFrame으로 변환
    df = pd.DataFrame(json_data['ranking'])
    
    # 컬럼명을 한글로 변경 (선택사항)
    column_mapping = {
        'date': '날짜',
        'ranking': '순위',
        'dojang_floor': '도장층수',
        'dojang_time_record': '기록시간(초)',
        'character_name': '캐릭터명',
        'world_name': '월드',
        'class_name': '직업군',
        'sub_class_name': '세부직업',
        'character_level': '레벨'
    }
    
    df_korean = df.rename(columns=column_mapping)
    
    # 시간을 분:초 형식으로 변환
    df_korean['기록시간(분:초)'] = df_korean['기록시간(초)'].apply(
        lambda x: f"{x//60}:{x%60:02d}"
    )

    df_korean.to_csv(f"data/dojang_ranking_{DATE}.csv", index=False, encoding='utf-8')
    
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

# API 호출
date = DATE
dojang_url = "https://open.api.nexon.com/maplestory/v1/ranking/dojang?date=" + date + "&world_name=%EB%A3%A8%EB%82%98&difficulty=1"
dojang_rank = requests.get(dojang_url, headers = headers)

print("Status Code:", dojang_rank.status_code)

if dojang_rank.status_code == 200:
    json_data = dojang_rank.json()
    print("Response JSON:", json_data)
    
    # 테이블 생성 및 출력
    table = create_dojang_table(json_data)
    display_table(table)
else:
    print("API 호출 실패")

