"""
더미 채용공고 생성 기반 모듈

각 직무별 생성기(generator_*.py)에서 공유하는 공통 코드.

흐름:
  Phase 1 → 직무별 특화 웹 검색으로 트렌드 수집 (max_tokens=4000)
  Phase 2 → 트렌드 기반 공고 JSON 생성 (max_tokens=8000, 검색 없음)
"""

from __future__ import annotations

import json
import logging
import logging.handlers
import os
import re
import sys
import argparse
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional, List

import anthropic

# ── 경로 설정 ─────────────────────────────────────────────────
BASE_DIR      = Path(os.environ.get("ANALYSIS_BASE_DIR", "C:/GitHub/new_git/money"))
CACHE_DIR     = BASE_DIR / "cache"
OUTPUT_DIR    = BASE_DIR / "results"
APPLOG_DIR    = BASE_DIR / "applogs"
LOGS_DIR      = BASE_DIR / "logs"

for _d in (CACHE_DIR, OUTPUT_DIR, APPLOG_DIR, LOGS_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# ── 로거 팩토리 ───────────────────────────────────────────────
def setup_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:          # 중복 핸들러 방지
        return logger
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    sh.setFormatter(fmt)

    fh = logging.handlers.TimedRotatingFileHandler(
        APPLOG_DIR / f"{name}.log",
        when="midnight",
        backupCount=7,
        encoding="utf-8",
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    logger.addHandler(sh)
    logger.addHandler(fh)
    return logger


# ── Anthropic 클라이언트 ──────────────────────────────────────
def _get_client() -> anthropic.Anthropic:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError(
            "ANTHROPIC_API_KEY 환경변수가 설정되지 않았습니다.\n"
            "실행 방법: set ANTHROPIC_API_KEY=your-key (Windows) / export ANTHROPIC_API_KEY=... (Linux)"
        )
    return anthropic.Anthropic(api_key=api_key)


TOOLS = [{"type": "web_search_20250305", "name": "web_search"}]


# ── 직무 카테고리 설정 ────────────────────────────────────────
@dataclass
class CategoryConfig:
    """직무 카테고리별 생성기 설정"""
    name:         str         # 파일명 식별자 (영문, e.g. "backend")
    display_name: str         # 표시명 (한글, e.g. "백엔드/서버")
    search_queries: List[str] # Phase 1 웹 검색 쿼리 목록
    job_titles:   List[str]   # Phase 2에서 다양하게 생성할 직무명 힌트
    skills_hint:  List[str]   # 해당 직무의 주요 기술스택 힌트


# ── Phase 1: 직무 특화 트렌드 검색 ───────────────────────────
def _search_trends(client: anthropic.Anthropic, config: CategoryConfig, logger: logging.Logger) -> str:
    """직무 특화 웹 검색 → 트렌드 요약 반환"""
    logger.info("[Phase 1] 트렌드 검색 시작 | 직무: %s", config.display_name)

    queries_text = "\n".join(f"{i+1}. {q}" for i, q in enumerate(config.search_queries))

    messages = [
        {
            "role": "user",
            "content": f"""아래 검색어로 웹 검색 후 결과를 간결하게 요약해주세요.
직무 카테고리: {config.display_name}

검색 목록:
{queries_text}

검색 후 아래 형식으로 요약 (JSON 아님, 자유 텍스트):
- 공고 구조: (주요 필드, 경력/급여 표기 방식)
- 인기 기술스택: ({config.display_name} 관련 언어, 프레임워크, 툴)
- 연봉 트렌드: ({config.display_name} 직군별 연봉 범위)
- 복지 트렌드: (요즘 많이 보이는 복지 항목)
- 인재상: (기업들이 원하는 역량, 소프트스킬)""",
        }
    ]

    step = 0
    try:
        while True:
            step += 1
            response = client.messages.create(
                model="claude-haiku-4-5",
                max_tokens=4000,
                tools=TOOLS,
                messages=messages,
            )
            logger.info("[Phase 1 / Step %d] stop_reason: %s", step, response.stop_reason)

            if response.stop_reason == "tool_use":
                tool_uses = [b for b in response.content if b.type == "tool_use"]
                tool_results = []
                for tu in tool_uses:
                    query = tu.input.get("query", "")
                    logger.info("[검색] %s", query)
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": tu.id,
                        "content": "검색 완료",
                    })
                messages.append({"role": "assistant", "content": response.content})
                messages.append({"role": "user", "content": tool_results})

            elif response.stop_reason in ("end_turn", "max_tokens"):
                text_blocks = [b for b in response.content if b.type == "text"]
                summary = text_blocks[-1].text.strip() if text_blocks else ""
                logger.info("[Phase 1] 트렌드 수집 완료 (%d자)", len(summary))
                return summary

            else:
                logger.warning("[Phase 1] 예상치 못한 stop_reason: %s", response.stop_reason)
                break

    except Exception as e:
        logger.error("[Phase 1] 검색 실패: %s", e)

    return ""


# ── Phase 2: 공고 JSON 생성 ──────────────────────────────────
def _generate_jobs(
    client: anthropic.Anthropic,
    config: CategoryConfig,
    trend_summary: str,
    job_count: int,
    used_companies: List[str],
    logger: logging.Logger,
) -> dict:
    """수집한 트렌드를 바탕으로 공고 JSON 생성 (웹 검색 없음)"""
    import uuid
    session_id = str(uuid.uuid4())[:8]
    used_companies_str = ", ".join(used_companies) if used_companies else "없음"
    today = datetime.now().strftime("%Y-%m-%d")
    job_titles_str = ", ".join(config.job_titles)
    skills_str = ", ".join(config.skills_hint)

    logger.info(
        "[Phase 2] 공고 생성 시작 | 직무=%s | count=%d | session=%s",
        config.display_name, job_count, session_id,
    )

    prompt = f"""아래 채용 트렌드 정보를 바탕으로 일로온 채용공고 {job_count}개를 JSON으로 생성하세요.

## 직무 카테고리
{config.display_name}

## 생성할 직무 예시 (다양하게 활용하되 아래에만 국한되지 않아도 됨)
{job_titles_str}

## 이 직무의 주요 기술스택 힌트
{skills_str}

## 수집된 트렌드 정보
{trend_summary}

## 생성 규칙
- 실제 회사명, 실제 개인정보 절대 사용 금지
- 가상 회사명은 창의적으로, 절대 재사용 금지
- 이미 사용된 회사명 (절대 재사용 금지): {used_companies_str}
- 직무는 {config.display_name} 카테고리 내에서 다양하게
- 모든 공고의 apply.method는 "일로온 입사지원"
- job_id / company.id 에는 실제 UUID v4 형식 사용 (job_001 같은 형식 절대 금지)
- 세션 시드: {session_id}

반드시 순수 JSON만 출력. 마크다운/설명 없이:
{{
  "site": "일로온",
  "category": "{config.display_name}",
  "analyzed_at": "{today}",
  "trend_summary": {{
    "hot_frameworks": [],
    "hot_languages": [],
    "hot_tools": [],
    "talent_keywords": [],
    "salary_trend": "",
    "welfare_trend": ""
  }},
  "jobs": [
    {{
      "job_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
      "status": "active",
      "always_open": false,
      "company": {{
        "id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
        "name": "가상회사명",
        "industry": {{"large": "IT/인터넷", "mid": "세부업종"}},
        "size": "스타트업|중소기업|중견기업|대기업",
        "employee_count": 100
      }},
      "position": {{
        "title": "직무명",
        "job_category": {{"large": "개발", "mid": "{config.display_name}", "small": "세부직종"}},
        "career": {{"type": "신입|경력|신입·경력", "min_year": 0, "max_year": 5}},
        "education": {{"type": "대졸이상", "required": false}},
        "headcount": 1,
        "work_type": "하이브리드"
      }},
      "work_condition": {{
        "location": {{"sido": "서울", "sigungu": "강남구", "address": "상세주소"}},
        "salary": {{"type": "연봉", "min": 40000000, "max": 60000000, "negotiable": true, "unit": "원"}},
        "work_hours": "09:00~18:00"
      }},
      "skills": ["기술스택1", "기술스택2"],
      "detail": {{
        "main_tasks": ["주요업무1", "주요업무2"],
        "requirements": ["자격요건1", "자격요건2"],
        "preferred": ["우대사항1", "우대사항2"],
        "talent": ["인재상1"],
        "benefits": ["복지1", "복지2"]
      }},
      "apply": {{"method": "일로온 입사지원", "document": ["이력서", "자기소개서"]}},
      "recruitment_process": {{
        "total_steps": 3,
        "steps": [
          {{"step": 1, "name": "서류전형", "description": "서류 검토", "duration": "1주일"}},
          {{"step": 2, "name": "기술면접", "description": "기술 역량 확인", "duration": "1주일"}},
          {{"step": 3, "name": "최종면접", "description": "임원 면접", "duration": "1주일"}}
        ],
        "notice": "합격자에 한해 개별 연락"
      }},
      "ai_recommendation": {{"is_recommended": false, "match_score": null}},
      "stats": {{"view_count": 100, "apply_count": 10, "bookmark_count": 20}},
      "dates": {{"posted_at": "{today}", "deadline": null, "updated_at": "{today}"}}
    }}
  ]
}}"""

    try:
        response = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=8000,
            messages=[{"role": "user", "content": prompt}],
        )
        logger.info("[Phase 2] stop_reason: %s", response.stop_reason)

        text_blocks = [b for b in response.content if b.type == "text"]
        if not text_blocks:
            logger.error("[Phase 2] 텍스트 블록 없음")
            return {}

        raw = text_blocks[-1].text.strip()
        logger.info("[Phase 2] 응답 수신 (%d자)", len(raw))

        # 마크다운 코드블록 제거
        if "```" in raw:
            match = re.search(r"```(?:json)?\s*([\s\S]+?)```", raw, re.DOTALL)
            if match:
                raw = match.group(1).strip()

        # JSON 파싱
        try:
            result = json.loads(raw)
            logger.info("[Phase 2] JSON 파싱 성공 | 공고 수: %d", len(result.get("jobs", [])))
            return result
        except json.JSONDecodeError:
            logger.warning("[Phase 2] JSON 불완전 — 보정 시도")
            for closing in ("]}", "\n]}", "\n  ]\n}"):
                try:
                    result = json.loads(raw + closing)
                    logger.info("[Phase 2] 보정 파싱 성공 | 공고 수: %d", len(result.get("jobs", [])))
                    return result
                except json.JSONDecodeError:
                    continue
            logger.error("[Phase 2] JSON 파싱 최종 실패")
            logger.debug("원본 응답:\n%s", raw[:500])
            return {}

    except anthropic.APIConnectionError as e:
        logger.error("[Phase 2] API 연결 실패: %s", e)
        raise
    except anthropic.RateLimitError as e:
        logger.error("[Phase 2] 요청 한도 초과: %s", e)
        raise
    except anthropic.APIStatusError as e:
        logger.error("[Phase 2] API 오류 [status=%d]: %s", e.status_code, e.message)
        raise


# ── UUID 주입 ─────────────────────────────────────────────────
def _inject_uuid(jobs: List[dict]) -> List[dict]:
    """LLM이 job_001 같은 고정 ID를 썼을 경우 UUID로 교체"""
    import uuid as _uuid
    result = []
    for job in jobs:
        job = dict(job)
        job_id = str(job.get("job_id", ""))
        if not job_id or re.match(r"^(job_|UUID|xxx)", job_id) or "-" not in job_id:
            job["job_id"] = str(_uuid.uuid4())
        company = job.get("company", {})
        if isinstance(company, dict):
            c_id = str(company.get("id", ""))
            if not c_id or re.match(r"^(company_|UUID|xxx)", c_id) or "-" not in c_id:
                company["id"] = str(_uuid.uuid4())
            job["company"] = company
        result.append(job)
    return result


# ── 회사명 중복 방지 ──────────────────────────────────────────
USED_COMPANIES_PATH = CACHE_DIR / "used_companies.json"

PG_CONFIG = {
    "host":     os.environ.get("PG_HOST",     "localhost"),
    "port":     int(os.environ.get("PG_PORT", "5432")),
    "dbname":   os.environ.get("PG_JOB_DB",  "iloon_jobs"),
    "user":     os.environ.get("PG_USER",     "airflow"),
    "password": os.environ.get("PG_PASSWORD", "airflow"),
}


def _companies_from_db(logger: logging.Logger) -> List[str]:
    try:
        import psycopg2
        conn = psycopg2.connect(**PG_CONFIG)
        cur  = conn.cursor()
        cur.execute("SELECT DISTINCT company FROM jobs WHERE company IS NOT NULL")
        names = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()
        logger.info("DB에서 기존 회사명 %d개 로드", len(names))
        return names
    except Exception as e:
        logger.warning("DB 회사명 조회 실패 (무시): %s", e)
        return []


def _companies_from_jsonl(logger: logging.Logger) -> List[str]:
    names = []
    for fpath in sorted(LOGS_DIR.glob("dummy-jobs-*.jsonl")):
        try:
            with open(fpath, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    obj = json.loads(line)
                    raws = obj if isinstance(obj, list) else [obj]
                    for raw in raws:
                        company = raw.get("company", {})
                        name = company.get("name", "") if isinstance(company, dict) else str(company)
                        if name:
                            names.append(name)
        except Exception as e:
            logger.warning("JSONL 회사명 스캔 실패 (%s): %s", fpath.name, e)
    logger.info("JSONL에서 기존 회사명 %d개 로드", len(names))
    return names


def _companies_from_cache() -> List[str]:
    if USED_COMPANIES_PATH.exists():
        try:
            with open(USED_COMPANIES_PATH, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return []


def load_used_companies(logger: logging.Logger) -> List[str]:
    """DB + JSONL + 캐시 3곳 모두 스캔해 회사명 합산"""
    all_names = (
        _companies_from_db(logger)
        + _companies_from_jsonl(logger)
        + _companies_from_cache()
    )
    unique = list(dict.fromkeys(n for n in all_names if n))
    logger.info("중복 방지 회사명 총 %d개", len(unique))
    return unique


def save_used_companies(jobs: List[dict], logger: logging.Logger) -> None:
    existing = _companies_from_cache()
    new_names = []
    for j in jobs:
        company = j.get("company", {})
        name = company.get("name", "") if isinstance(company, dict) else str(company)
        if name:
            new_names.append(name)
    merged = list(dict.fromkeys(existing + new_names))
    try:
        with open(USED_COMPANIES_PATH, "w", encoding="utf-8") as f:
            json.dump(merged, f, ensure_ascii=False, indent=2)
        logger.info("회사명 캐시 저장: %d개", len(merged))
    except Exception as e:
        logger.warning("회사명 저장 실패: %s", e)


# ── 캐시 ─────────────────────────────────────────────────────
def _cache_path(category_name: str) -> Path:
    return CACHE_DIR / f"dummy_jobs_{category_name}_{datetime.now().strftime('%Y%m%d')}.json"


def load_cache(category_name: str, logger: logging.Logger) -> Optional[dict]:
    path = _cache_path(category_name)
    if path.exists():
        logger.info("캐시 재사용: %s", path)
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning("캐시 읽기 실패 (재생성): %s", e)
    return None


def save_cache(category_name: str, result: dict, logger: logging.Logger) -> None:
    path = _cache_path(category_name)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        logger.info("캐시 저장: %s", path)
    except Exception as e:
        logger.error("캐시 저장 실패: %s", e)


# ── 결과 저장 ─────────────────────────────────────────────────
def save_results(result: dict, config: CategoryConfig, logger: logging.Logger) -> None:
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    today = datetime.now().strftime("%Y-%m-%d")

    jobs = _inject_uuid(result.get("jobs", []))
    result = {**result, "jobs": jobs}

    # JSON 원본
    json_path = OUTPUT_DIR / f"dummy_jobs_{config.name}_{ts}.json"
    try:
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        logger.info("JSON 저장: %s", json_path)
    except Exception as e:
        logger.error("JSON 저장 실패: %s", e)

    # JSONL (importer가 읽는 파일) — 카테고리별 별도 파일
    jsonl_path = LOGS_DIR / f"dummy-jobs-{config.name}-{today}.jsonl"
    try:
        with open(jsonl_path, "a", encoding="utf-8") as f:
            for job in jobs:
                f.write(json.dumps(job, ensure_ascii=False) + "\n")
        logger.info("JSONL 저장: %s (%d건)", jsonl_path, len(jobs))
    except Exception as e:
        logger.error("JSONL 저장 실패: %s", e)


# ── 메인 실행 함수 ────────────────────────────────────────────
def run_category_generator(config: CategoryConfig, count: int = 5, use_cache: bool = False) -> None:
    """카테고리 설정을 받아 2-Phase 생성 실행"""
    logger = setup_logger(f"generator_{config.name}")
    logger.info("========== [%s] 공고 생성 시작 ==========", config.display_name)
    logger.info("count=%d | use_cache=%s", count, use_cache)

    used_companies = load_used_companies(logger)

    result = load_cache(config.name, logger) if use_cache else None

    if not result:
        try:
            client = _get_client()
            trend_summary = _search_trends(client, config, logger)
            result = _generate_jobs(client, config, trend_summary, count, used_companies, logger)
        except Exception as e:
            logger.critical("[%s] 생성 실패: %s", config.display_name, e)
            sys.exit(1)

        if not result or "jobs" not in result:
            logger.critical("[%s] 유효한 결과 없음", config.display_name)
            sys.exit(1)

        save_cache(config.name, result, logger)

    save_used_companies(result.get("jobs", []), logger)
    save_results(result, config, logger)

    jobs = result.get("jobs", [])
    logger.info("========== [%s] 완료 | 공고 %d개 생성 ==========", config.display_name, len(jobs))

    # 간단 요약 출력
    trend = result.get("trend_summary", {})
    print(f"\n─── [{config.display_name}] 트렌드 요약 ───")
    print(f"  hot_languages  : {trend.get('hot_languages', [])}")
    print(f"  hot_frameworks : {trend.get('hot_frameworks', [])}")
    print(f"  salary_trend   : {trend.get('salary_trend', '')}")
    print(f"  생성된 공고 수 : {len(jobs)}개")


# ── CLI 파서 공통 헬퍼 ────────────────────────────────────────
def parse_args(description: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--count",     default=5, type=int,
                        help="생성할 공고 수 (기본: 5)")
    parser.add_argument("--use-cache", action="store_true",
                        help="당일 캐시 재사용")
    return parser.parse_args()
