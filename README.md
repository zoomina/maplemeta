# Maplemeta

메이플스토리 메타 분석을 위한 데이터 수집·적재 파이프라인. Nexon Open API와 무릉도장 랭킹 데이터를 기반으로 DW(Data Warehouse) → DM(Data Mart) 적재를 수행한다.

## 주요 기능

- **데이터 수집**: 무릉도장 랭킹 → OCID → 캐릭터 정보(장비, 헥사코어, 세트효과, 어빌리티, 하이퍼스탯)
- **DW 적재**: 수집 데이터를 `dw` 스키마에 저장
- **DM 적재**: DW를 집계하여 대시보드용 `dm` 스키마로 변환
- **Shift Score / 밸런스 점수**: 직업별 메타 변화 추적 지표 (Outcome, Stat, Build)
- **Nexon 공지**: 공지·업데이트·이벤트·캐시샵 수집 및 DM 적재

## 프로젝트 구조

```
maplemeta/
├── dags/                    # Airflow DAG 정의
│   ├── maplemeta_dag.py      # 레거시: 매일 8시, API_KEY_1+2 순차
│   ├── load_character_info_dag.py  # 매주 목요일 8시, API_KEY_1 전용
│   ├── dw_dm_load_dag.py     # DW→DM 적재 (load_character_info 완료 10분 후)
│   └── nexon_notice_dag.py   # Nexon 공지/업데이트/이벤트/캐시샵
├── schemas/                  # DB 스키마 및 ETL 함수
│   ├── dw.sql               # DW 스키마
│   ├── dm_tmp.sql           # DM 스키마 + refresh_dashboard_dm
│   ├── score_tmp.sql        # shift_score, balance_score ETL
│   └── dm_tmp_run_guide.md  # 실행 가이드
├── scripts/                  # 수집·적재 로직
│   ├── load_ranker.py        # 무릉도장 랭킹 수집
│   ├── load_ocid.py          # OCID 수집·적재
│   ├── load_character_info.py # 캐릭터 정보 수집·적재
│   ├── backfill_dw_to_dm.py  # DW→DM 전체 백필
│   ├── backfill_nexon_notice.py # Nexon 공지 백필
│   └── dw_load_utils.py      # DW 연결·스키마 유틸
├── config.py                 # 환경 변수 (API_KEY 등)
├── docker-compose.yml        # Airflow + PostgreSQL
├── requirements.txt
└── .env.example              # 환경 변수 템플릿
```

## 환경 설정

### 1. 환경 변수

`.env.example`을 복사하여 `.env` 생성 후 필요한 환경 변수 설정:

```bash
cp .env.example .env
```

| 변수 | 설명 |
|------|------|
| `API_KEY_1` | 목요일 수집용 Nexon API 키 |
| `API_KEY_2` | 백필용 Nexon API 키 |
| `NEXON_API_KEY` | Nexon 공지 API (없으면 API_KEY_2 사용) |
| `ANTHROPIC_API_KEY` | patch_note LLM 생성용 |
| `DW_DATABASE_URL` | PostgreSQL 연결 문자열 |

### 2. Docker 실행

```bash
docker compose up -d
```

- Airflow UI: http://localhost:8080
- 기본 계정: `airflow` / `airflow`

### 3. 스키마 적용 (최초 1회)

```bash
# DM 스키마
psql "$DW_DATABASE_URL" -v ON_ERROR_STOP=1 -f schemas/dm_tmp.sql

# Shift/Balance 점수 함수
psql "$DW_DATABASE_URL" -v ON_ERROR_STOP=1 -f schemas/score_tmp.sql
```

## DAG 플로우

```mermaid
flowchart LR
    subgraph legacy [maplemeta_data_collection - 레거시]
        R1[load_ranker_1] --> O1 --> L1 --> C1 --> LC1
        LC1 --> R2 --> O2 --> L2 --> C2 --> LC2
    end
    subgraph new [load_character_info - 매주 목요일]
        R[load_ranker] --> O[collect_ocid] --> L[load_ocid] --> C[collect_char] --> LC[load_char]
    end
    subgraph dw_dm [dw_dm_load]
        SENSOR[wait_load_char] --> REFRESH[refresh_dm] --> SHIFT[refresh_shift_score]
    end
    LC -.->|ExternalTaskSensor| SENSOR
```

| DAG | 스케줄 | 설명 |
|-----|--------|------|
| `maplemeta_data_collection` | 매일 8시 | API_KEY_1+2 순차 백필 (레거시) |
| `load_character_info` | 매주 목요일 8시 | API_KEY_1 전용 수집 |
| `dw_dm_load` | 매주 목요일 8시 | load_character_info 완료 10분 후 DM refresh |
| `nexon_notice_backfill` | 매일 9시 | 공지·업데이트·이벤트·캐시샵 |

## 백필

```bash
# DW→DM 전체 백필
python scripts/backfill_dw_to_dm.py

# shift_score만 백필
python scripts/backfill_dw_to_dm.py --shift-score-only
```

## 세그먼트 정의

| segment | 조건 |
|---------|------|
| 50층 | floor 50~69 |
| 상위권 | floor ≥ 90 (해당 date+job에서 90층 이상 < 15명이면 floor ≥ 80) |
| total | 0.7 × 50층 점수 + 0.3 × 상위권 점수 |

---

## 변경 히스토리

> 파일 생성일·커밋 기준. 내용의 날짜(예: 12409, 12/10)는 데이터/버전 기준일.

### 2026-03-03
- **DAG 스케줄 재구성**: `load_character_info` 신규 생성 (매주 목요일 8시, API_KEY_1 전용)
- **dw_dm_load**: 의존성 `maplemeta_data_collection` → `load_character_info`로 변경

### 2026-03-02
- **shift_score / 엔트로피 DB 적재**: `dm_shift_score`, `dm_balance_score`, `score_tmp.sql` ETL
- **dm 검토 및 실행**: character_master 확장, 백필 순서
- **dw-dm 적재 DAG**: backfill_dw_to_dm, refresh_dashboard_dm
- **260225 집계 변경**: date+count, dm_hyper, hyper_master

### 2026-03-01
- **260301 shift/추가적재**: shift_score 집계 공식, dw_dm 추가적재 계획
- **Nexon 적재 방식 변경**: event/cashshop API → 웹 크롤링 전환 계획

### 2026-02-25
- **dm_tmp_run_guide**: 집계 기준 date+count, dm_hyper, 세트효과 제외, 버전별 날짜 고정
- **260224 dw-dm-init-backfill**: DW→DM 초기 백필 계획

### 2026-02-24
- **dw>>dm query**: add dw >> dm query, clean dm sql

### 2026-02-20
- **수집/적재 분리**: OCID, character_info collect/load 분리, 페이로드 파일 저장
- **적재 재시도**: `_run_with_backoff()` 지수 백오프
- **collect-load-split-retry**: 적재 큐 SQL 버그 수정

### 2026-02-19
- **postgresDB DW**: DW 스키마 Postgres 전환
- **data mart query**: dm 스키마 및 refresh_dashboard_dm
- **dw-to-dm-dec2025**: 12월 데이터(12409, 12410) 기준 DW→DM 계획

### 2026-01-30 ~ 2026-01-31
- **DW 스키마**: dw.sql 추가
- **Airflow DAG**: maplemeta_dag, load_ranker→load_ocid→load_character_info

### 2026-01-28
- **Docker Airflow**: docker-compose, airflow DAG 초기 구성
