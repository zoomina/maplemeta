import json
import pandas as pd

def merge_dojang_rankings():
    """
    루나와 스카니아 도장 랭킹을 통합하여 새로운 순위로 정렬
    """
    # 루나 데이터 로드
    with open('data_json/dojang_ranking_2026-01-05_루나.json', 'r', encoding='utf-8') as f:
        luna_data = json.load(f)
    
    # 스카니아 데이터 로드
    with open('data_json/dojang_ranking_2026-01-05_스카니아.json', 'r', encoding='utf-8') as f:
        scania_data = json.load(f)
    
    # 두 데이터 합치기
    all_data = luna_data + scania_data
    
    print(f"루나: {len(luna_data)}명, 스카니아: {len(scania_data)}명")
    print(f"총 {len(all_data)}명의 데이터 통합")
    
    # DataFrame으로 변환
    df = pd.DataFrame(all_data)
    
    # 층수 기준 내림차순, 동일 층수는 기록시간(초) 오름차순으로 정렬
    df_sorted = df.sort_values(['도장층수', '기록시간(초)'], 
                              ascending=[False, True]).reset_index(drop=True)
    
    # 새로운 통합 순위 부여
    df_sorted['통합순위'] = range(1, len(df_sorted) + 1)
    
    # 기존 순위를 서버내순위로 변경
    df_sorted = df_sorted.rename(columns={'순위': '서버내순위'})
    
    # 컬럼 순서 재정렬
    columns_order = ['날짜', '통합순위', '서버내순위', '도장층수', '기록시간(초)', 
                    '캐릭터명', '월드', '직업군', '세부직업', '레벨', '기록시간(분:초)']
    df_final = df_sorted[columns_order]
    
    # JSON으로 저장
    result_data = df_final.to_dict('records')
    
    with open('data_json/dojang_ranking_2026-01-05.json', 'w', encoding='utf-8') as f:
        json.dump(result_data, f, ensure_ascii=False, indent=2)
    
    print(f"통합 랭킹 저장 완료: dojang_ranking_2026-01-05.json")
    print(f"총 {len(result_data)}명의 통합 랭킹 생성")
    
    # 상위 10명 출력
    print("\n=== 통합 랭킹 상위 10명 ===")
    for i in range(min(10, len(result_data))):
        data = result_data[i]
        print(f"{data['통합순위']}위: {data['캐릭터명']} ({data['월드']}) - {data['도장층수']}층 {data['기록시간(분:초)']}")
    
    return result_data

if __name__ == "__main__":
    merge_dojang_rankings()