---
name: Airflow 하루치 백필
overview: 집계 요일 전환(2025-06-18 기준)을 반영해 하루치만 처리하도록 수정합니다.
todos:
  - id: limit-backfill-to-one-day
    content: get_past_wednesdays/backfill_data를 1일 처리로 축소
    status: completed
isProject: false
---

# Airflow 하루치 백필로 제한

## 목표

- 집계 요일 전환(2025-06-18 기준)을 반영해 **하루치만** 처리합니다.
- 2025-06-18 **이후(포함)**는 **수요일 기준**, 그 이전은 **해당 주 일요일 기준**을 사용합니다.
- 백필/랭킹/OCID/캐릭터 정보 **모든 작업이 하루치만** 처리되도록 일관되게 제한합니다.

## 변경 포인트

- [dags/maplemeta_dag.py](C:\Users\jmbye\Project\Airflow\dags\maplemeta_dag.py)
  - `get_past_wednesdays()`를 **요일 분기 포함 단일 날짜 반환** 함수로 변경
  - 2025-06-18 기준으로 수요일/일요일 분기 처리
  - `backfill_data()`는 **단일 날짜만 처리**

## 구현 방법

### 1) 기준일/요일 분기 규칙 고정

- 기준일 상수: `2025-06-18` (포함)
- `execution_date >= 2025-06-18` → **가장 최근 수요일 1개**
- `execution_date < 2025-06-18` → **해당 주 일요일 1개**

### 2) 날짜 산출 로직을 단일 날짜로 변경

- `get_past_wednesdays()`를 “최근 수요일 1개 또는 해당 주 일요일 1개”만 반환하도록 변경
- 반환값은 항상 **리스트 길이 1** (`["YYYY-MM-DD"]`)

### 3) 백필 로직을 하루치 전용으로 단순화

- `backfill_data()`는 리스트 1개만 처리
- “총 N개 수요일” 같은 메시지는 **1개 처리** 문구로 수정

### 4) 전체 작업 흐름에 미치는 영향 정리

- 랭킹 작업: 이미 파일이 있으면 **스킵**, 없으면 하루치만 생성
- OCID 작업: 랭킹 파일이 없으면 **스킵**, 있으면 하루치만 생성
- 캐릭터 정보 작업: OCID 파일이 없으면 **스킵**, 있으면 하루치만 생성
- 결과적으로 모든 작업이 **동일 하루치 기준으로 정렬**됨

## 검증

- 2025-06-18 이후 날짜로 수동 실행 시 **수요일 1일**만 처리되는지 확인
- 2025-06-18 이전 날짜로 수동 실행 시 **해당 주 일요일 1일**만 처리되는지 확인
- 랭킹/OCID/캐릭터 정보가 **동일한 하루치만** 생성되는지 확인
- 생성 파일명이 1일치만 생성되는지 확인 (`data_json/*.json`)

## 관련 코드 참고

```61:85:C:\Users\jmbye\Project\Airflow\dags\maplemeta_dag.py
 def get_past_wednesdays(max_weeks=52, **context):
     ...
     for i in range(max_weeks * 7):
         if current_date.weekday() == 2:
             wednesdays.append(current_date.strftime('%Y-%m-%d'))
         ...
```

```109:116:C:\Users\jmbye\Project\Airflow\dags\maplemeta_dag.py
 def backfill_data(api_key, api_key_name, data_type, **context):
     past_wednesdays = get_past_wednesdays(max_weeks=52, **context)
```

