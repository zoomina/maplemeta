import os
from typing import Optional


def _clean_env(name: str) -> Optional[str]:
    value = os.getenv(name)
    if value is None:
        return None
    value = value.strip()
    return value or None


# API_KEY_1: 최근 수요일 수집용
# API_KEY_2(API_KEY): 백필/일반 수집용
API_KEY1 = _clean_env("API_KEY_1") or _clean_env("API_KEY1")
API_KEY2 = _clean_env("API_KEY_2") or _clean_env("API_KEY")
API_KEY = API_KEY2  # 기존 코드 호환용 별칭
DATE = _clean_env("DATE") or "2025-08-13"


def resolve_api_key(api_key_name: str) -> str:
    if api_key_name == "API_KEY_1":
        key = API_KEY1
    elif api_key_name in {"API_KEY_2", "API_KEY"}:
        key = API_KEY2
    else:
        raise ValueError(f"알 수 없는 API 키 이름: {api_key_name}")

    if not key:
        raise ValueError(
            f"{api_key_name} 환경 변수가 비어 있습니다. "
            "`.env` 파일에 API_KEY_1/API_KEY_2를 설정하세요."
        )
    return key
