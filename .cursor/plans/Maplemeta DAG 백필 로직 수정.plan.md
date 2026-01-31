---
name: ""
overview: ""
todos: []
isProject: false
---

# Maplemeta DAG 백필 로직 (역순 탐색)

## 개요

- **목적**: “이전주 1개”만 보는 대신, **데이터가 존재하지 않는 주차가 나올 때까지** 과거로 주차 단위 역순 탐색 후, 그 **1개 집계일**만 수집.
- **적용 위치**: `dags/maplemeta_dag.py` — ranker / ocid / character_info 백필.

---

## 핵심 로직

### 1. `get_first_missing_date_backwards(data_type, max_weeks=52, **context)`

- **역할**: 최신 집계일(`get_past_wednesdays` 1개)부터 **과거로 주차 단위**로 탐색해, **데이터가 없는 첫 주**의 집계일 1개만 반환.
- **data_type별 조건**
  - **ranking**: 해당 집계일에 랭킹 파일 없음 → 그날 수집 대상.
  - **ocid**: 랭킹은 있는데 OCID 없음 → 그날 수집 대상.
  - **character_info**: OCID는 있는데 캐릭터 정보 없음 → 그날 수집 대상.
- **탐색 범위**: 최대 52주(`max_weeks=52`).
- **반환**: `[YYYY-MM-DD]` 1개 또는 `[]` (모든 탐색 주에 데이터 있음).

### 2. `backfill_data`에서 날짜 소스

- **변경 전**: `get_reporting_date_with_fallback()` → “이전주 1개”만 사용.
- **변경 후**: `get_first_missing_date_backwards(data_type, max_weeks=52, **context)` 사용.
- ranker / ocid / character_info 모두 **역순 탐색 → 나온 1개 집계일만 백필**.

### 3. Ranker 태스크 로그 (“로직은 ranker에서 찍기”)

- `load_ranker_task_func`에서 역순 탐색 결과를 로그로 출력:
  - 백필 대상 있음: `[API_KEY_1] 백필 대상 집계일(역순 탐색): YYYY-MM-DD`
  - 없음: `[API_KEY_1] 백필 대상 없음 (모든 탐색 주차에 랭킹 데이터 존재)`

---

## 동작 요약


| 단계             | 동작                                                     |
| -------------- | ------------------------------------------------------ |
| Ranker         | 최신 수요일 → 과거로 주차 단위 탐색 → **랭킹이 없는 첫 주** 1개 수집 (없으면 스킵). |
| OCID           | 동일 역순 탐색 → **랭킹은 있는데 OCID 없는 첫 주** 1개 수집.              |
| Character info | 동일 역순 탐색 → **OCID는 있는데 캐릭터정보 없는 첫 주** 1개 수집.           |


- 1회 DAG 실행당 **타입별 1개 집계일**만 처리 → API 토큰/부하 제한 대응.
- DW 적재는 기존대로 `get_reporting_date_with_fallback()`로 1개 날짜 사용.

---

## 수정 파일

- `dags/maplemeta_dag.py`
  - `get_first_missing_date_backwards()` 추가.
  - `backfill_data()` 날짜 소스를 `get_first_missing_date_backwards(data_type, ...)`로 변경.
  - `load_ranker_task_func()`에서 역순 탐색 결과 로그 출력.

