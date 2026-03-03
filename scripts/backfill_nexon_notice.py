# -*- coding: utf-8 -*-
"""
Nexon Open API 기반 공지/업데이트/이벤트/캐시샵 DW/DM 1회성 백필 스크립트.

- DM 직접 적재: notice, event, cashshop
- DW 적재: dw_update (업데이트)
- DM 적재: version_master (dm_hexacore는 maplemeta 일반 dm refresh 플로우에 포함)
- 인벤 메할일 크롤링 + patch_note LLM 생성

--step 옵션으로 단계별 실행 가능 (DAG 분리용):
  load, dm_direct, detail, mahalil, dw_update, llm, dm, version_master

환경변수 (.env 참조):
- API_KEY_1: 1번 키 (우선 사용). NEXON_API_KEY 있으면 1번으로 사용
- API_KEY_2: 2번 키 (429 시 재시도용)
- ANTHROPIC_API_KEY: Claude API 키 (patch_note 생성 시)
- DW_DATABASE_URL 또는 DATABASE_URL: DB 연결
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import requests
from bs4 import BeautifulSoup

# 상위 디렉토리 import
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config  # .env 로드
from dw_load_utils import ensure_dw_schema, get_dw_connection


def _ensure_dm_schema(conn) -> None:
    """Create new DM tables only (full dm.sql would drop existing data)."""
    ddl = """
    create schema if not exists dm;
    create table if not exists dm.dm_notice (
        notice_id integer primary key,
        title text,
        url text,
        date timestamptz
    );
    create table if not exists dm.dm_event (
        notice_id integer primary key,
        title text,
        url text,
        date timestamptz,
        start_date timestamptz,
        end_date timestamptz,
        thumbnail text
    );
    create table if not exists dm.dm_cashshop (
        notice_id integer primary key,
        title text,
        url text,
        date timestamptz,
        start_date timestamptz,
        end_date timestamptz,
        thumbnail text
    );
    create table if not exists dm.dm_update (
        notice_id integer primary key,
        title text,
        url text,
        date timestamptz
    );
    alter table dm.dm_event add column if not exists thumbnail text;
    alter table dm.dm_cashshop add column if not exists thumbnail text;
    create table if not exists dm.version_master (
        version text primary key,
        start_date date,
        end_date date,
        type text[],
        impacted_job text[],
        content_list text[],
        patch_note text
    );
    alter table dm.version_master add column if not exists content_list text[];
    create table if not exists dm.dm_hexacore (
        version text not null,
        date date not null,
        job text not null,
        segment text not null,
        hexa_core_name text not null,
        hexa_core_type text,
        count bigint not null,
        total_level bigint not null,
        primary key (version, date, job, segment, hexa_core_name)
    );
    create index if not exists idx_dm_hexacore_version_date on dm.dm_hexacore (version, date);
    """
    with conn.cursor() as cur:
        for stmt in ddl.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)
    conn.commit()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

NEXON_BASE = "https://open.api.nexon.com/maplestory/v1"
NEXON_NEWS_BASE = "https://maplestory.nexon.com"
# 스크립트 상위(프로젝트 루트)/static/update. Airflow: /opt/airflow/static/update, 로컬: 프로젝트/static/update
STATIC_UPDATE_DIR = Path(
    os.getenv("STATIC_UPDATE_DIR", str(Path(__file__).resolve().parents[1] / "static" / "update"))
)
# 태스크 간 공유: data_json 볼륨 사용 (/tmp는 태스크별로 비어있을 수 있음)
TMP_JSON_DIR = Path(os.getenv("NEXON_BACKFILL_JSON_DIR", str(Path(__file__).resolve().parents[1] / "data_json" / "nexon_notice_backfill")))
# static 권한 없을 시 폴백 (data_json은 쓰기 가능)
STATIC_UPDATE_FALLBACK_DIR = TMP_JSON_DIR.parent / "nexon_static" / "update"
_effective_static_dir: Optional[Path] = None
VERSION_RE = re.compile(r"(\d+)\.(\d+)\.(\d+)")
DATE_RANGE_RE = re.compile(r"(\d{4})\.(\d{2})\.(\d{2})\s*~\s*(\d{4})\.(\d{2})\.(\d{2})")
NOTICE_ID_RE = re.compile(r"/(\d+)$")

STEPS = ("load", "dm_direct", "detail", "mahalil", "dw_update", "llm", "dm", "version_master")


def _save_json(name: str, data: Any) -> Path:
    path = TMP_JSON_DIR / f"{name}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, default=str), encoding="utf-8")
    return path


def _load_json(name: str) -> Any:
    path = TMP_JSON_DIR / f"{name}.json"
    if not path.exists():
        raise FileNotFoundError(f"step 전제조건: {path} 필요 (이전 step 실행)")
    return json.loads(path.read_text(encoding="utf-8"))


def check_has_updates_for_dag() -> bool:
    """DAG용: update.json에 신규 업데이트 항목이 있으면 True, 없으면 False (ShortCircuitOperator용)."""
    path = TMP_JSON_DIR / "update.json"
    if not path.exists():
        return False
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return bool(data) if isinstance(data, list) else False
    except Exception:
        return False


def _get_nexon_api_keys_ordered() -> list[str]:
    """1번(API_KEY_1) → 2번(API_KEY_2) 순서. NEXON_API_KEY 있으면 1번으로 사용."""
    keys: list[str] = []
    k1 = config.NEXON_API_KEY or config.API_KEY1
    if k1:
        keys.append(k1)
    k2 = config.API_KEY2
    if k2 and (not keys or k2 != keys[0]):
        keys.append(k2)
    return keys


def get_nexon_api_key() -> str:
    """1번 키 반환."""
    keys = _get_nexon_api_keys_ordered()
    if not keys:
        raise ValueError(
            "Nexon API 키가 없습니다. .env에 API_KEY_1 또는 API_KEY_2(또는 NEXON_API_KEY)를 설정하세요."
        )
    return keys[0]


def _fetch_notice_with_key(url: str, api_key: str, params: Optional[dict] = None) -> requests.Response:
    headers = {"x-nxopen-api-key": api_key}
    return requests.get(url, headers=headers, params=params, timeout=30)


def fetch_notice(url: str, params: Optional[dict] = None) -> dict:
    """API 호출. 1번 키 사용, 429 시 2번 키로 재시도."""
    keys = _get_nexon_api_keys_ordered()
    if not keys:
        raise ValueError(".env에 API_KEY_1 또는 API_KEY_2를 설정하세요.")
    last_error: Optional[Exception] = None
    for i, key in enumerate(keys):
        try:
            resp = _fetch_notice_with_key(url, key, params)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            last_error = e
            if e.response is not None and e.response.status_code == 429:
                if i < len(keys) - 1:
                    log.warning("429 Too Many Requests - %d번 키(API_KEY_2)로 재시도", i + 2)
                    continue
            raise
    if last_error:
        raise last_error
    raise RuntimeError("API 호출 실패")


def parse_version_from_title(title: str) -> Optional[str]:
    m = VERSION_RE.search(title)
    if not m:
        return None
    return f"{m.group(1)}{m.group(2)}{m.group(3)}"


def parse_content_from_title(title: str) -> Optional[str]:
    start = title.find("(")
    end = title.rfind(")")
    if start >= 0 and end > start:
        return title[start + 1 : end].strip()


# ---------------------------------------------------------------------------
# 1. API Load
# ---------------------------------------------------------------------------


def load_notice() -> list[dict]:
    url = f"{NEXON_BASE}/notice"
    data = fetch_notice(url)
    items = data.get("notice") or data.get("notices") or []
    return [
        {"notice_id": x.get("notice_id"), "title": x.get("title"), "url": x.get("url"), "date": x.get("date")}
        for x in items
        if isinstance(x, dict) and x.get("notice_id") is not None
    ]


def load_update() -> list[dict]:
    url = f"{NEXON_BASE}/notice-update"
    data = fetch_notice(url)
    items = data.get("update_notice") or data.get("notice") or data.get("notices") or []
    return [
        {"notice_id": x.get("notice_id"), "title": x.get("title"), "url": x.get("url"), "date": x.get("date")}
        for x in items
        if isinstance(x, dict) and x.get("notice_id") is not None
    ]


def _parse_date_range(text: str) -> tuple[Optional[str], Optional[str]]:
    """Parse 'YYYY.MM.DD ~ YYYY.MM.DD' to (start_iso, end_iso)."""
    m = DATE_RANGE_RE.search(text or "")
    if not m:
        return (None, None)
    return (
        f"{m.group(1)}-{m.group(2)}-{m.group(3)}",
        f"{m.group(4)}-{m.group(5)}-{m.group(6)}",
    )


def _normalize_url(href: str) -> str:
    """Convert relative URL to absolute."""
    if not href:
        return ""
    if href.startswith("http"):
        return href
    return f"{NEXON_NEWS_BASE}{href}" if href.startswith("/") else f"{NEXON_NEWS_BASE}/{href}"


def crawl_event() -> list[dict]:
    """웹 크롤링: https://maplestory.nexon.com/News/Event"""
    url = f"{NEXON_NEWS_BASE}/News/Event"
    headers = {"User-Agent": "Mozilla/5.0 (compatible; MapleMeta/1.0)"}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    rows: list[dict] = []
    for wrap in soup.select("div.event_list_wrap"):
        dl = wrap.find("dl")
        if not dl:
            continue
        dt = dl.find("dt")
        dd_data = dl.find("dd", class_="data")
        dd_date = dl.find("dd", class_="date")
        a = dt.find("a") if dt else None
        img = a.find("img") if a else None
        href = (a.get("href") or "").strip() if a else ""
        notice_id_match = NOTICE_ID_RE.search(href)
        notice_id = int(notice_id_match.group(1)) if notice_id_match else None
        if notice_id is None:
            continue
        em = dd_data.find("em", class_="event_listMt") if dd_data else None
        title = (em.get_text(strip=True) if em else "").strip()
        date_text = (dd_date.find("p").get_text(strip=True) if dd_date and dd_date.find("p") else "").strip()
        start_iso, end_iso = _parse_date_range(date_text)
        date_val = start_iso or end_iso
        rows.append({
            "notice_id": notice_id,
            "title": title,
            "url": _normalize_url(href),
            "date": date_val,
            "start_date": start_iso,
            "end_date": end_iso,
            "thumbnail": (img.get("src") or "").strip() if img else None,
        })
    return rows


def crawl_cashshop() -> list[dict]:
    """웹 크롤링: https://maplestory.nexon.com/News/CashShop"""
    url = f"{NEXON_NEWS_BASE}/News/CashShop"
    headers = {"User-Agent": "Mozilla/5.0 (compatible; MapleMeta/1.0)"}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    rows: list[dict] = []
    for wrap in soup.select("div.cash_list_wrap"):
        dl = wrap.find("dl")
        if not dl:
            continue
        dt = dl.find("dt")
        dd_data = dl.find("dd", class_="data")
        dd_date = dl.find("dd", class_="date")
        a = dt.find("a") if dt else None
        img = a.find("img") if a else None
        href = (a.get("href") or "").strip() if a else ""
        notice_id_match = NOTICE_ID_RE.search(href)
        notice_id = int(notice_id_match.group(1)) if notice_id_match else None
        if notice_id is None:
            continue
        a_in_data = dd_data.find("a") if dd_data else None
        span = a_in_data.find("span") if a_in_data else None
        title = (span.get_text(strip=True) if span else "").strip()
        date_text = (dd_date.find("p").get_text(strip=True) if dd_date and dd_date.find("p") else "").strip()
        start_iso, end_iso = _parse_date_range(date_text)
        date_val = start_iso or end_iso
        rows.append({
            "notice_id": notice_id,
            "title": title,
            "url": _normalize_url(href),
            "date": date_val,
            "start_date": start_iso,
            "end_date": end_iso,
            "thumbnail": (img.get("src") or "").strip() if img else None,
        })
    return rows


def load_update_detail(notice_id: int, max_retries: int = 3) -> Optional[dict]:
    """429 시 sleep 후 재시도."""
    url = f"{NEXON_BASE}/notice-update/detail"
    last_error: Optional[Exception] = None
    for attempt in range(max_retries):
        try:
            data = fetch_notice(url, params={"notice_id": notice_id})
            return data
        except requests.exceptions.HTTPError as e:
            last_error = e
            if e.response is not None and e.response.status_code == 429 and attempt < max_retries - 1:
                wait = 2 * (attempt + 1)
                log.warning("429 Too Many Requests notice_id=%s, %ds 후 재시도 (%d/%d)", notice_id, wait, attempt + 1, max_retries)
                time.sleep(wait)
                continue
            log.warning("update detail load failed notice_id=%s: %s", notice_id, e)
            return None
        except Exception as e:
            log.warning("update detail load failed notice_id=%s: %s", notice_id, e)
            return None
    if last_error:
        log.warning("update detail load failed notice_id=%s: %s", notice_id, last_error)
    return None


# ---------------------------------------------------------------------------
# 2. DM 직접 적재 (notice, event, cashshop)
# ---------------------------------------------------------------------------


def dm_load_with_retry(conn, load_fn, max_retries: int = 5):
    for attempt in range(max_retries):
        try:
            load_fn(conn)
            return True
        except Exception as e:
            log.warning("DM load attempt %d failed: %s", attempt + 1, e)
            if attempt < max_retries - 1:
                time.sleep(3)
    return False


def _load_dm_notice(conn, rows: list[dict]) -> None:
    with conn.cursor() as cur:
        cur.execute("delete from dm.dm_notice")
        for r in rows:
            cur.execute(
                """
                insert into dm.dm_notice (notice_id, title, url, date)
                values (%s, %s, %s, %s::timestamptz)
                on conflict (notice_id) do update set title=excluded.title, url=excluded.url, date=excluded.date
                """,
                (r.get("notice_id"), r.get("title"), r.get("url"), r.get("date")),
            )
    conn.commit()


def _load_dm_event(conn, rows: list[dict]) -> None:
    with conn.cursor() as cur:
        cur.execute("delete from dm.dm_event")
        for r in rows:
            cur.execute(
                """
                insert into dm.dm_event (notice_id, title, url, date, start_date, end_date, thumbnail)
                values (%s, %s, %s, %s::timestamptz, %s::timestamptz, %s::timestamptz, %s)
                on conflict (notice_id) do update set title=excluded.title, url=excluded.url, date=excluded.date,
                    start_date=excluded.start_date, end_date=excluded.end_date, thumbnail=excluded.thumbnail
                """,
                (
                    r.get("notice_id"),
                    r.get("title"),
                    r.get("url"),
                    r.get("date"),
                    r.get("start_date"),
                    r.get("end_date"),
                    r.get("thumbnail"),
                ),
            )
    conn.commit()


def _load_dm_update(conn) -> None:
    """dw_update에서 title, url, date를 dm_update로 적재 (dm_notice/dm_event/dm_cashshop처럼)."""
    with conn.cursor() as cur:
        cur.execute("delete from dm.dm_update")
        cur.execute(
            """
            insert into dm.dm_update (notice_id, title, url, date)
            select notice_id, title, url, date from dw.dw_update
            on conflict (notice_id) do update set title=excluded.title, url=excluded.url, date=excluded.date
            """
        )
    conn.commit()


def _load_dm_cashshop(conn, rows: list[dict]) -> None:
    with conn.cursor() as cur:
        cur.execute("delete from dm.dm_cashshop")
        for r in rows:
            cur.execute(
                """
                insert into dm.dm_cashshop (notice_id, title, url, date, start_date, end_date, thumbnail)
                values (%s, %s, %s, %s::timestamptz, %s::timestamptz, %s::timestamptz, %s)
                on conflict (notice_id) do update set title=excluded.title, url=excluded.url, date=excluded.date,
                    start_date=excluded.start_date, end_date=excluded.end_date, thumbnail=excluded.thumbnail
                """,
                (
                    r.get("notice_id"),
                    r.get("title"),
                    r.get("url"),
                    r.get("date"),
                    r.get("start_date"),
                    r.get("end_date"),
                    r.get("thumbnail"),
                ),
            )
    conn.commit()


# ---------------------------------------------------------------------------
# 3. 업데이트 디테일 load + 파일 저장
# ---------------------------------------------------------------------------


def _get_writable_static_dir() -> Path:
    """STATIC_UPDATE_DIR 쓰기 가능 여부 확인, 불가 시 data_json/nexon_static/update 폴백."""
    global _effective_static_dir
    if _effective_static_dir is not None:
        return _effective_static_dir
    for d in (STATIC_UPDATE_DIR, STATIC_UPDATE_FALLBACK_DIR):
        try:
            d.mkdir(parents=True, exist_ok=True)
            test_file = d / ".write_test"
            test_file.write_text("")
            test_file.unlink()
            _effective_static_dir = d
            if d != STATIC_UPDATE_DIR:
                log.warning("STATIC_UPDATE_DIR 권한 없음, 폴백 사용: %s", d)
            return d
        except (PermissionError, OSError):
            continue
    _effective_static_dir = STATIC_UPDATE_DIR
    return _effective_static_dir


def save_detail_html(version: str, notice_id: int, contents: str) -> Optional[Path]:
    static_dir = _get_writable_static_dir()
    path = static_dir / f"{version}_{notice_id}.html"
    try:
        path.write_text(contents or "", encoding="utf-8")
        return path
    except Exception as e:
        log.warning("save detail html failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# 4. 메할일 크롤링
# ---------------------------------------------------------------------------


def crawl_mahalil(version: str) -> Optional[Path]:
    try:
        import urllib.parse

        search_url = f"https://www.inven.co.kr/search/maple/top/{version}/1"
        headers = {"User-Agent": "Mozilla/5.0 (compatible; MapleMeta/1.0)"}
        resp = requests.get(search_url, headers=headers, timeout=15)
        resp.raise_for_status()
        html = resp.text

        # "메할일" 포함 링크 찾기
        from html.parser import HTMLParser

        class LinkCollector(HTMLParser):
            def __init__(self):
                super().__init__()
                self.links: list[str] = []
                self.in_a = False
                self._href = ""

            def handle_starttag(self, tag, attrs):
                if tag == "a":
                    self.in_a = True
                    self._href = ""
                    for k, v in attrs:
                        if k == "href":
                            self._href = v or ""
                            break

            def handle_endtag(self, tag):
                if tag == "a":
                    self.in_a = False

            def handle_data(self, data):
                if self.in_a and "메할일" in data and self._href:
                    self.links.append(self._href)

        parser = LinkCollector()
        parser.feed(html)
        links = parser.links

        if not links:
            log.warning("메할일 링크 없음 version=%s", version)
            return None

        article_url = links[0]
        if not article_url.startswith("http"):
            article_url = "https://www.inven.co.kr" + article_url

        resp2 = requests.get(article_url, headers=headers, timeout=15)
        resp2.raise_for_status()
        article_html = resp2.text

        # articleMain div 추출 (depth 기반으로 매칭되는 </div> 찾기)
        start = article_html.find('<div class="articleMain">')
        if start < 0:
            start = article_html.find('<div class="articleMain"')
        if start < 0:
            log.warning("articleMain 없음 version=%s", version)
            return None

        tag_end = article_html.find(">", start)
        if tag_end < 0:
            log.warning("articleMain 여는 태그 불완전 version=%s", version)
            return None
        pos = tag_end + 1  # articleMain 내용 시작

        depth = 1  # articleMain 내부
        end = -1
        i = pos
        while i < len(article_html):
            if article_html[i : i + 4] == "<div" and (i + 4 >= len(article_html) or article_html[i + 4] in " >\t\n"):
                depth += 1
                i += 4
                continue
            if article_html[i : i + 6] == "</div>":
                depth -= 1
                if depth == 0:
                    end = i + 6
                    break
                i += 6
                continue
            i += 1

        if end < 0:
            log.warning("articleMain 닫는 태그 없음 version=%s", version)
            return None
        outer = article_html[start:end]
        static_dir = _get_writable_static_dir()
        path = static_dir / f"{version}_mahalil.html"
        path.write_text(outer, encoding="utf-8")
        return path
    except Exception as e:
        log.warning("메할일 크롤링 실패 version=%s: %s", version, e)
        return None


# ---------------------------------------------------------------------------
# 5. dw_update 적재
# ---------------------------------------------------------------------------


def load_dw_update(conn, rows: list[dict]) -> None:
    with conn.cursor() as cur:
        for r in rows:
            cur.execute(
                """
                insert into dw.dw_update (notice_id, title, url, date, version, content, detail_path, mahalil_path)
                values (%s, %s, %s, %s::timestamptz, %s, %s, %s, %s)
                on conflict (notice_id) do update set
                    title=excluded.title, url=excluded.url, date=excluded.date,
                    version=excluded.version, content=excluded.content,
                    detail_path=excluded.detail_path, mahalil_path=excluded.mahalil_path
                """,
                (
                    r.get("notice_id"),
                    r.get("title"),
                    r.get("url"),
                    r.get("date"),
                    r.get("version"),
                    r.get("content"),
                    r.get("detail_path"),
                    r.get("mahalil_path"),
                ),
            )
    conn.commit()


def update_mahalil_path(conn, version: str, path: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "update dw.dw_update set mahalil_path = %s where version = %s",
            (path, version),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# 6. patch_note LLM 생성
# ---------------------------------------------------------------------------


def _resolve_static_path(p: Path) -> Optional[Path]:
    """저장된 경로에 파일 없으면 STATIC_UPDATE_DIR에서 동일 파일명으로 재시도 (폴백→정식 경로 마이그레이션)."""
    if not p:
        return None
    if p.exists():
        return p
    alt = STATIC_UPDATE_DIR / p.name
    return alt if alt.exists() else None


def generate_patch_note(version: str, detail_paths: list[Path], mahalil_path: Optional[Path]) -> Optional[Path]:
    api_key = config.ANTHROPIC_API_KEY
    if not api_key or not api_key.strip():
        log.warning("ANTHROPIC_API_KEY 없음 (.env) - patch_note 생성 스킵")
        return None

    try:
        import anthropic

        client = anthropic.Anthropic(api_key=api_key.strip())
        files: list[dict] = []
        for p in detail_paths:
            resolved = _resolve_static_path(p)
            if resolved:
                files.append({"path": str(resolved), "content": resolved.read_text(encoding="utf-8")[:100000]})
        resolved_mahalil = _resolve_static_path(mahalil_path) if mahalil_path else None
        if resolved_mahalil:
            files.append({"path": str(resolved_mahalil), "content": resolved_mahalil.read_text(encoding="utf-8")[:50000]})

        if not files:
            log.warning("patch_note 생성할 파일 없음")
            return None

        prompt = """유첨 파일은 메이플스토리 {version} 버전 패치노트와 인벤 메할일 정리 자료입니다.
(* 메할일 : 메이플스토리에서 유저가 수행해야 할 주요 콘텐츠/이벤트/주간·일일 루틴)

이 자료를 기반으로,
메이플 메타 분석 대시보드의 "패치노트 요약 + 메할일 체크리스트" 페이지에 들어갈 요약 마크다운(.md) 파일을 작성해주세요.

메할일 체크리스트를 최상단에 배치하고, 메타 변화에 주요한 요소 -> 디테일 순서로 작성하세요.""".format(
            version=version
        )

        # Anthropic API: messages with file content
        content_blocks = [{"type": "text", "text": prompt}]
        for f in files:
            content_blocks.append({"type": "text", "text": f"\n\n---\n파일: {f['path']}\n---\n{f['content']}"})

        msg = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=4000,
            messages=[{"role": "user", "content": content_blocks}],
        )
        text = msg.content[0].text if msg.content else ""

        out_path = _get_writable_static_dir() / f"{version}_patch_note.md"
        out_path.write_text(text, encoding="utf-8")
        return out_path
    except ImportError:
        log.warning("anthropic 패키지 없음 - patch_note 생성 스킵")
        return None
    except Exception as e:
        log.warning("patch_note LLM 생성 실패: %s", e)
        return None


# ---------------------------------------------------------------------------
# 6.1 type / impacted_job 추출 (260301_dw_dm_추가적재계획)
# ---------------------------------------------------------------------------

# type 키워드 매핑: (type_label, [keywords])
TYPE_KEYWORDS = [
    ("캐릭터", ["리마스터", "신규 직업"]),
    ("아이템", ["무기", "장비", "아이템"]),
    ("스킬", ["스킬", "코어"]),
    ("이벤트", ["이벤트"]),
    ("시스템", ["개선", "오류"]),
]

# impacted_job 추출 패턴: "ooo,ooo 리마스터" / "신규 직업 ooo"
RE_REMASTER = re.compile(r"([^,]+(?:,[^,]+)*)\s*리마스터")
RE_NEW_JOB = re.compile(r"신규\s*직업\s+([^\s,]+)")


def _extract_types(text: str) -> list[str]:
    """텍스트에서 type 키워드 매칭. 복수 가능."""
    if not text:
        return []
    types: list[str] = []
    for label, keywords in TYPE_KEYWORDS:
        if any(kw in text for kw in keywords):
            types.append(label)
    if not types:
        types.append("기타")
    return types


def _extract_impacted_jobs(text: str) -> list[str]:
    """리마스터/신규 직업 패턴에서 직업명 추출."""
    if not text:
        return []
    jobs: list[str] = []
    for m in RE_REMASTER.finditer(text):
        part = m.group(1).strip()
        for j in (x.strip() for x in part.split(",") if x.strip()):
            if j and j not in jobs:
                jobs.append(j)
    for m in RE_NEW_JOB.finditer(text):
        j = m.group(1).strip()
        if j and j not in jobs:
            jobs.append(j)
    return jobs


def extract_type_and_impacted_job(text: str) -> tuple[list[str], list[str]]:
    """텍스트에서 type, impacted_job 추출."""
    types = _extract_types(text or "")
    jobs = _extract_impacted_jobs(text or "")
    return (types, jobs)


def _get_version_aggregated_text(
    conn, version: str, static_base: Path
) -> str:
    """version별 content + patch_note + detail HTML 텍스트 병합."""
    parts: list[str] = []
    with conn.cursor() as cur:
        cur.execute(
            """
            select content, detail_path from dw.dw_update
            where version = %s
            """,
            (version,),
        )
        rows = cur.fetchall()
    for content, detail_path in rows or []:
        if content:
            parts.append(content)
        if detail_path:
            try:
                p = Path(detail_path)
                if p.exists():
                    html = p.read_text(encoding="utf-8")
                    soup = BeautifulSoup(html, "html.parser")
                    parts.append(soup.get_text(separator=" ", strip=True))
            except Exception as e:
                log.debug("detail_path read skip %s: %s", detail_path, e)
    patch_path = static_base / f"{version}_patch_note.md"
    if patch_path.exists():
        try:
            parts.append(patch_path.read_text(encoding="utf-8"))
        except Exception as e:
            log.debug("patch_note read skip %s: %s", patch_path, e)
    return " ".join(parts)


# ---------------------------------------------------------------------------
# 7. version_master 적재
# ---------------------------------------------------------------------------


def _prepare_dm_version_meta(conn) -> dict[str, dict[str, Any]]:
    """version별 type, impacted_job 계산. dm step에서 호출."""
    static_base = _get_writable_static_dir()
    with conn.cursor() as cur:
        cur.execute(
            "select distinct version from dw.dw_update where version is not null"
        )
        versions = [r[0] for r in cur.fetchall()]
    meta: dict[str, dict[str, Any]] = {}
    for v in versions:
        text = _get_version_aggregated_text(conn, v, static_base)
        types, jobs = extract_type_and_impacted_job(text)
        meta[v] = {"type": types, "impacted_job": jobs}
    return meta


def _get_version_content_list(conn, version: str) -> list[str]:
    """version별 dw_update.content 중 null 제외, 단일 숫자 제외하여 text[] 반환."""
    with conn.cursor() as cur:
        cur.execute(
            """
            select array_agg(content order by date)
            from (
                select content from dw.dw_update
                where version = %s
                  and content is not null
                  and btrim(content) <> ''
                  and content !~ '^\\s*\\d+(\\.\\d+)?\\s*$'
            ) t
            """,
            (version,),
        )
        row = cur.fetchone()
    arr = row[0] if row and row[0] else []
    return list(arr) if arr else []


def load_version_master(conn) -> None:
    """version_master 적재. start_date=업데이트 디테일 업로드일, end_date=다음 업로드일-1."""
    base = str(_get_writable_static_dir())
    static_base = Path(base)

    # 1) version별 start_date, end_date 계산
    with conn.cursor() as cur:
        cur.execute(
            """
            select version, min(date)::date as start_date
            from dw.dw_update
            where version is not null
            group by version
            order by min(date)
            """
        )
        rows = cur.fetchall()
    if not rows:
        return

    versions_ordered = [(r[0], r[1]) for r in rows]
    version_end_dates: dict[str, Optional[str]] = {}
    for i, (v, start_d) in enumerate(versions_ordered):
        if i + 1 < len(versions_ordered):
            # 종료 시점 = 다음 버전 업데이트 노트 올라오기 전 날짜
            next_start = versions_ordered[i + 1][1]
            end_d = str(next_start - timedelta(days=1)) if next_start else None
        else:
            # end_date null → today (최신 버전은 오늘까지)
            end_d = str(date.today())
        version_end_dates[v] = end_d

    # 2) type, impacted_job: dm_version_meta.json 우선, 없으면 인라인 계산
    meta_path = TMP_JSON_DIR / "dm_version_meta.json"
    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception as e:
            log.warning("dm_version_meta.json 로드 실패, 인라인 계산: %s", e)
            meta = _prepare_dm_version_meta(conn)
    else:
        meta = _prepare_dm_version_meta(conn)

    # 3) upsert
    with conn.cursor() as cur:
        for ver, start_date in versions_ordered:
            end_date = version_end_dates.get(ver)
            vm = meta.get(ver, {})
            types = vm.get("type") or []
            jobs = vm.get("impacted_job") or []
            content_list = _get_version_content_list(conn, ver)
            patch_note = f"{base}/{ver}_patch_note.md"
            cur.execute(
                """
                insert into dm.version_master (version, start_date, end_date, type, impacted_job, content_list, patch_note)
                values (%s, %s, %s, %s, %s, %s, %s)
                on conflict (version) do update set
                    start_date = excluded.start_date,
                    end_date = excluded.end_date,
                    type = excluded.type,
                    impacted_job = excluded.impacted_job,
                    content_list = excluded.content_list,
                    patch_note = excluded.patch_note
                """,
                (ver, start_date, end_date, types, jobs, content_list, patch_note),
            )
    conn.commit()


# ---------------------------------------------------------------------------
# Main (step-based)
# ---------------------------------------------------------------------------


def _run_step_load() -> None:
    """DAG1: API Load. notice, update(API), event/cashshop(웹 크롤링)를 JSON으로 저장."""
    TMP_JSON_DIR.mkdir(parents=True, exist_ok=True)
    log.info("1. API Load")
    notice_rows = load_notice()
    time.sleep(0.3)
    update_rows = load_update()
    time.sleep(0.3)
    event_rows = crawl_event()
    time.sleep(0.3)
    cashshop_rows = crawl_cashshop()
    log.info("notice=%d, update=%d, event=%d, cashshop=%d", len(notice_rows), len(update_rows), len(event_rows), len(cashshop_rows))
    _save_json("notice", notice_rows)
    _save_json("update", update_rows)
    _save_json("event", event_rows)
    _save_json("cashshop", cashshop_rows)


def _run_step_dm_direct() -> None:
    """DAG2: DM 직접 적재 (notice, event, cashshop)."""
    notice_rows = _load_json("notice")
    event_rows = _load_json("event")
    cashshop_rows = _load_json("cashshop")
    conn = get_dw_connection()
    ensure_dw_schema(conn)
    _ensure_dm_schema(conn)
    log.info("2. DM 직접 적재 (notice, event, cashshop)")
    for load_fn, rows, name in [
        (_load_dm_notice, notice_rows, "dm_notice"),
        (_load_dm_event, event_rows, "dm_event"),
        (_load_dm_cashshop, cashshop_rows, "dm_cashshop"),
    ]:
        success = dm_load_with_retry(conn, lambda c, fn=load_fn, r=rows: fn(c, r))
        if not success:
            conn.close()
            raise RuntimeError(f"{name} 적재 실패")
    for f in ("notice.json", "event.json", "cashshop.json"):
        (TMP_JSON_DIR / f).unlink(missing_ok=True)
    conn.close()


def _run_step_detail() -> None:
    """DAG3: 업데이트 디테일 load + HTML 저장."""
    update_rows = _load_json("update")
    if not update_rows:
        log.info("업데이트 신규 항목 없음")
        return
    log.info("3. 업데이트 디테일 load")
    for r in update_rows:
        vid = r.get("notice_id")
        if vid is None:
            continue
        v = parse_version_from_title(r.get("title") or "")
        if not v:
            continue
        time.sleep(0.3)  # API rate limit 방지
        detail = load_update_detail(vid)
        if detail and detail.get("contents"):
            p = save_detail_html(v, vid, detail["contents"])
            r["detail_path"] = str(p) if p else None
        else:
            r["detail_path"] = None
        r["version"] = v
        r["content"] = parse_content_from_title(r.get("title") or "")
    _save_json("update", update_rows)


def _run_step_mahalil() -> None:
    """DAG4: 메할일 크롤링. 조회 실패 시 예외 발생 → 당일 재시도 없이 다음날 스케줄에 자동 재실행."""
    update_rows = _load_json("update")
    if not update_rows:
        return
    versions = list({r["version"] for r in update_rows if r.get("version")})
    if not versions:
        return
    conn = get_dw_connection()
    ensure_dw_schema(conn)
    log.info("4. 메할일 크롤링")
    mahalil_paths: dict[str, Optional[Path]] = {}
    failed_versions: list[str] = []
    for v in versions:
        p = crawl_mahalil(v)
        mahalil_paths[v] = p
        if p:
            update_mahalil_path(conn, v, str(p))
            log.info("메할일 저장 version=%s: %s", v, p)
        else:
            failed_versions.append(v)
            log.warning("메할일 조회 실패 version=%s - 다음날 재실행 예약", v)
    if failed_versions:
        conn.close()
        raise RuntimeError(
            f"메할일 조회 실패 version={failed_versions}. 당일 재시도 없이 다음날 스케줄(9시)에 자동 재실행됩니다."
        )
    for r in update_rows:
        v = r.get("version")
        r["mahalil_path"] = str(mahalil_paths[v]) if v and mahalil_paths.get(v) else None
    _save_json("update", update_rows)
    conn.close()


def _run_step_dw_update() -> None:
    """DAG5: dw_update 적재."""
    update_rows = _load_json("update")
    if not update_rows:
        return
    conn = get_dw_connection()
    ensure_dw_schema(conn)
    log.info("5. dw_update 적재")
    load_dw_update(conn, update_rows)
    _load_dm_update(conn)
    conn.close()


def _run_step_llm() -> None:
    """DAG6: Claude LLM patch_note 생성."""
    update_rows = _load_json("update")
    if not update_rows:
        return
    versions = list({r["version"] for r in update_rows if r.get("version")})
    mahalil_paths: dict[str, Optional[Path]] = {}
    for r in update_rows:
        v = r.get("version")
        if v and r.get("mahalil_path"):
            mahalil_paths[v] = Path(r["mahalil_path"])
    log.info("6. patch_note LLM 생성")
    for v in versions:
        detail_paths = [
            Path(r["detail_path"])
            for r in update_rows
            if r.get("version") == v and r.get("notice_id") and r.get("detail_path")
        ]
        p = generate_patch_note(v, detail_paths, mahalil_paths.get(v))
        if p:
            log.info("patch_note 생성 version=%s: %s", v, p)


def _run_step_dm() -> None:
    """DAG7: dm 적재 (version별 type/impacted_job 계산 → dm_version_meta.json 저장)."""
    log.info("7. dm 적재 (type/impacted_job 준비)")
    conn = get_dw_connection()
    ensure_dw_schema(conn)
    try:
        meta = _prepare_dm_version_meta(conn)
        TMP_JSON_DIR.mkdir(parents=True, exist_ok=True)
        _save_json("dm_version_meta", meta)
        log.info("dm_version_meta 저장 완료 version=%d건", len(meta))
    finally:
        conn.close()


def _run_step_version_master() -> None:
    """DAG8: version_master 적재."""
    conn = get_dw_connection()
    ensure_dw_schema(conn)
    _ensure_dm_schema(conn)
    log.info("8. version_master 적재")
    load_version_master(conn)
    conn.close()


def run(args: argparse.Namespace) -> None:
    step = getattr(args, "step", None)
    if step:
        step_handlers = {
            "load": _run_step_load,
            "dm_direct": _run_step_dm_direct,
            "detail": _run_step_detail,
            "mahalil": _run_step_mahalil,
            "dw_update": _run_step_dw_update,
            "llm": _run_step_llm,
            "dm": _run_step_dm,
            "version_master": _run_step_version_master,
        }
        if step not in step_handlers:
            raise ValueError(f"--step {step} (가능: {', '.join(STEPS)})")
        step_handlers[step]()
        log.info("step %s 완료", step)
        return

    # 전체 실행 (기존 동작)
    TMP_JSON_DIR.mkdir(parents=True, exist_ok=True)
    _run_step_load()
    _run_step_dm_direct()
    update_path = TMP_JSON_DIR / "update.json"
    update_rows = json.loads(update_path.read_text(encoding="utf-8")) if update_path.exists() else []
    if not update_rows:
        log.info("업데이트 신규 항목 없음 - stream 종료")
        return
    _run_step_detail()
    _run_step_mahalil()
    _run_step_dw_update()
    _run_step_llm()
    _run_step_dm()
    _run_step_version_master()
    log.info("백필 완료")


def main():
    parser = argparse.ArgumentParser(description="Nexon notice/update/event/cashshop DW/DM 백필")
    parser.add_argument("--step", choices=STEPS, help="단계별 실행 (DAG 분리용)")
    parser.add_argument("--dry-run", action="store_true", help="API만 호출하고 DB 적재 없음")
    args = parser.parse_args()

    if args.dry_run:
        log.info("dry-run: API 호출 (NEXON_API_KEY 필요)")
        try:
            notice_rows = load_notice()
            update_rows = load_update()
            event_rows = crawl_event()
            cashshop_rows = crawl_cashshop()
            log.info("notice=%d, update=%d, event=%d, cashshop=%d", len(notice_rows), len(update_rows), len(event_rows), len(cashshop_rows))
        except ValueError as e:
            log.info("dry-run: API 키 없음 - %s", e)
        return

    run(args)


if __name__ == "__main__":
    main()
