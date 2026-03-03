# shift_score / 엔트로피 점수 DB 적재 업데이트

**날짜**: 2026-03-01  
**관련 문서**: [260301_shift.md](260301_shift.md), [260301_shift_score_집계.md](260301_shift_score_집계.md)

---

## 1. 개요

shift_score와 엔트로피(밸런스) 점수 계산을 위한 DM 테이블 및 ETL을 추가했다. DM 구조가 확정되기 전까지 `dm_tmp.sql`, `score_tmp.sql`로 수동 실행하며, 확정 후 DAG로 전환 예정이다.

---

## 2. 변경 사항

### 2.1 dm_tmp.sql

| 항목 | 내용 |
|------|------|
| **dm_rank** | `segment` 컬럼 추가, `idx_dm_rank_segment` 인덱스 추가 |
| **dm_shift_score** | 신규 테이블 + outcome_score_100, stat_score_100, build_score_100, total_score_100 (KPI 카드용 0~100 척도) |
| **dm_balance_score** | 신규 테이블 (version, segment, balance_score, top_job, top_share, cr3, top_type, top_type_share) |
| **refresh_dashboard_dm** | dm_rank 적재 시 `segment` 함께 저장 (segment_label 기반) |

### 2.2 score_tmp.sql (신규)

| 함수 | 설명 |
|------|------|
| `dm.refresh_balance_score(p_version)` | 엔트로피 기반 밸런스 점수 적재 (50층, 상위권, total) |
| `dm.refresh_shift_score(p_version)` | share 로그비율 기반 Outcome Shift + 100점 척도(min-max 정규화) 적재 |
| `dm.refresh_shift_balance_score(p_version)` | 위 두 함수 순차 실행 |

### 2.3 백필 실행

- `scripts/backfill_dw_to_dm.py`: 전체 백필 (refresh_dashboard_dm + refresh_shift_balance_score)
- `--shift-score-only`: shift_score만 전체 백필

### 2.4 dm_tmp_run_guide.md

- score_tmp 관련 파일 및 실행 순서 반영

---

## 3. 세그먼트 정의

| segment | 조건 |
|---------|------|
| 50층 | floor 50~69 |
| 상위권 | floor ≥ 90 (해당 date+job에서 floor≥90 인원 < 15명이면 floor ≥ 80) |
| total | 0.7 × 50층 점수 + 0.3 × 상위권 점수 |

---

## 4. 실행 순서

```bash
cd /home/jamin/Workspace/maplemeta

# 1. DM 스키마
psql "$DW_DATABASE_URL" -v ON_ERROR_STOP=1 -f schemas/dm_tmp.sql

# 2. 스코어 스키마/함수
psql "$DW_DATABASE_URL" -v ON_ERROR_STOP=1 -f schemas/score_tmp.sql

# 3. 전체 백필 (refresh_dashboard_dm + refresh_shift_balance_score)
python scripts/backfill_dw_to_dm.py

# shift_score만 백필 시
python scripts/backfill_dw_to_dm.py --shift-score-only
```

---

## 5. DAG 플로우 (확정 후)

1. **DW 적재 플로우**: maplemeta_dag (load_ranker → load_ocid → load_character_info)
2. **DW→DM 적재 플로우**: refresh_dashboard_dm → refresh_shift_balance_score
3. **공지 및 업데이트 플로우**: nexon_notice_dag

---

## 6. 설계 결정

| 항목 | 결정 |
|------|------|
| unique character count | character_name 활용 (고유값) |
| dm_rank segment | 컬럼 추가, refresh_dashboard_dm에서 적재 |
| 파일 관리 | tmp로 수동 실행, 확정 시 dm.sql, score.sql로 정리 후 DAG 반영 |
