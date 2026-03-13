#!/usr/bin/env python3
"""
Supabase dm_balance_score, dm_shift_score 재적재.

전제: Supabase SQL Editor에서 schemas/supabase_migration_score_20260312.sql 실행 후
      로컬 dm에 데이터 존재.

연결: SUPABASE_DB_URL (Direct Postgres, pooler.supabase.com) 우선.
      없으면 SUPABASE_URL + SUPABASE_SERVICE_KEY (REST API).
      supabase.co 차단 환경에서는 SUPABASE_DB_URL 사용.

실행: python scripts/supabase_score_migrate.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR / "scripts"))

def _load_env() -> None:
    env_path = BASE_DIR / ".env"
    if not env_path.exists():
        return
    try:
        from dotenv import dotenv_values
        for k, v in dotenv_values(env_path).items():
            if v is not None:
                os.environ[k] = str(v)
    except ImportError:
        with open(env_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    idx = line.index("=")
                    k = line[:idx].strip()
                    v = line[idx + 1 :].strip().strip('"').strip("'")
                    if k and v:
                        os.environ[k] = v


def main() -> None:
    import subprocess
    _load_env()
    has_db_url = bool(os.getenv("SUPABASE_DB_URL"))
    has_rest = bool(os.getenv("SUPABASE_URL")) and (
        os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    )
    if not has_db_url and not has_rest:
        print(
            "SUPABASE_DB_URL (Direct Postgres) 또는 "
            "SUPABASE_URL + SUPABASE_SERVICE_KEY 환경 변수 필요 (.env 확인)"
        )
        sys.exit(1)
    print("dm_balance_score, dm_shift_score Supabase 재적재 중...")
    result = subprocess.run(
        [sys.executable, str(BASE_DIR / "scripts" / "reset_supabase_dm_tables.py"), "--tables", "dm_balance_score,dm_shift_score"],
        cwd=str(BASE_DIR),
        env={**os.environ, "PGSCHEMA": "dm"},
    )
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
