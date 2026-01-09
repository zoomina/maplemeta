import requests
import pandas as pd
from config import API_KEY, DATE
import time

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
def create_user_ocid_table(date=DATE):
    """
    도장 랭킹 CSV에서 캐릭터 이름을 읽어와서 유저 마스터 테이블 생성 (상위 100명)
    """
    # 랭킹 CSV 파일 읽기
    ranking_file = f"data/dojang_ranking_{date}.csv"
    
    try:
        df_ranking = pd.read_csv(ranking_file, encoding='utf-8-sig')
        # 상위 100명만 추출
        df_ranking = df_ranking.head(100)
        print(f"랭킹 파일 로드 완료: 상위 {len(df_ranking)}명")
    except FileNotFoundError:
        print(f"랭킹 파일을 찾을 수 없습니다: {ranking_file}")
        return None
    
    user_ocid_list = []
    
    for idx, row in df_ranking.iterrows():
        character_name = row['캐릭터명']
        print(f"처리 중... ({idx+1}/{len(df_ranking)}) {character_name}")
        
        # OCID 조회
        ocid = get_character_ocid(character_name)
        
        if ocid:
            user_ocid_list.append({
                'character_name': character_name,
                'ocid': ocid
            })
        else:
            print(f"OCID 조회 실패: {character_name}")
        
        # API 호출 제한 방지를 위한 딜레이 (초당 5건 제한)
        time.sleep(0.3)
    
    # DataFrame 생성 및 CSV 저장
    if user_ocid_list:
        df_ocid = pd.DataFrame(user_ocid_list)
        output_file = f"data/user_ocid_{date}.csv"
        df_ocid.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"\n유저 마스터 테이블 저장 완료: {output_file}")
        print(f"총 {len(df_ocid)}명의 유저 정보 수집")
        return output_file
    else:
        print("수집된 유저 정보가 없습니다.")
        return None

# 실행
if __name__ == "__main__":
    date = DATE
    create_user_ocid_table(date)