import os

# 환경 변수에서 API_KEY 읽기, 없으면 기본값 사용
# API_KEY_1: 목요일용 (최근 수요일 데이터 수집)
# API_KEY 또는 API_KEY_2: 평일용 (과거 수요일 데이터 백필)
API_KEY1 = os.getenv("API_KEY_1") or os.getenv("API_KEY1", "test_e19e9e6e44c65995c472cb12a677b8e894475cf5f7c0a5f9b350cc8e423208d2efe8d04e6d233bd35cf2fabdeb93fb0d")
API_KEY = os.getenv("API_KEY_2") or os.getenv("API_KEY", "test_703f78f0882eb117abf99d2892867023586e60eee209b9f499071dbf6b6db9dbefe8d04e6d233bd35cf2fabdeb93fb0d")
DATE = os.getenv("DATE", "2025-08-13")
