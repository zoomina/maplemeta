"""
OCID 실패 캐릭터 마스터 파일 생성 스크립트
기존 ocid_failed_{date}.json 파일들을 통합하여 ocid_failed_master.json 생성
"""
import json
import os
import glob

def load_failed_master(master_file_path="data_json/ocid_failed_master.json"):
    """
    실패 마스터 파일 로드 (없으면 빈 리스트 반환)
    """
    if os.path.exists(master_file_path):
        try:
            with open(master_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # 리스트 형태인지 확인
                if isinstance(data, list):
                    return set(item.get('character_name', item) if isinstance(item, dict) else item for item in data)
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
    print(f"마스터 파일 저장 완료: {len(failed_list)}개 캐릭터")

def create_master_from_existing_files(data_json_dir="data_json"):
    """
    기존 ocid_failed_{date}.json 파일들을 통합하여 마스터 파일 생성
    """
    failed_set = load_failed_master()
    
    # 기존 실패 파일들 찾기
    pattern = os.path.join(data_json_dir, "ocid_failed_*.json")
    failed_files = glob.glob(pattern)
    
    print(f"발견된 실패 파일: {len(failed_files)}개")
    
    for file_path in failed_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict):
                            char_name = item.get('character_name')
                            if char_name:
                                failed_set.add(char_name)
                        else:
                            failed_set.add(item)
                print(f"처리 완료: {os.path.basename(file_path)}")
        except Exception as e:
            print(f"파일 처리 오류 {file_path}: {e}")
    
    save_failed_master(failed_set)
    return failed_set

if __name__ == "__main__":
    create_master_from_existing_files()
