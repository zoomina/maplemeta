# 로컬 DW 1회성 백필 런북

## 목적
- 로컬 DB가 없는 상태에서 Docker로 Postgres를 띄우고,
- 현재 `data_json`에 적재된 JSON 전체를 대상으로 DW 백필을 1회 수행한다.
- 마지막으로 검증 쿼리로 적재 결과를 확인한다.

## 사전 조건
- 프로젝트 루트: `/home/jamin/Workspace/maplemeta`
- Docker 실행 가능
- `docker compose` 또는 `docker-compose` 사용 가능

## 1) 로컬 DB 셋업
```bash
bash scripts/setup_local_dw.sh
```

초기화(볼륨 삭제)까지 하고 싶으면:
```bash
bash scripts/setup_local_dw.sh --reset
```

## 2) JSON 전체 1회성 백필
```bash
bash scripts/backfill_dw_once.sh
```

기본값은 `data_json` 하위 폴더까지 재귀 탐색이다.

## 3) 적재 검증
```bash
bash scripts/verify_dw_counts.sh
```

## 참고
- 백필 엔진: `scripts/load_dw_full.py` (재귀 탐색으로 수정됨)
- 검증 쿼리 파일: `schemas/dw_verify_queries.sql`
