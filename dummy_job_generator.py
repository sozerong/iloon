"""
채용공고 구조 + 실시간 IT 트렌드 파악 Agent

흐름:
  1. 사람인 공고 구조 파악 (필드, 형식) ← 참고 사이트
  2. 현재 IT 트렌드 파악 (유행 프레임워크, 기술스택, 인재상)
  3. 두 가지를 합쳐서 현실적인 더미 공고 생성 (일로온 브랜드로)
  4. FastAPI 코드 자동 생성

실행:
  python dummy_job_generator.py
  python dummy_job_generator.py --count 10 --no-cache
"""

import json
import logging
import logging.handlers
import os
import re
import sys
import argparse
from datetime import datetime
from pathlib import Path
from typing import Optional

sys.stdout.reconfigure(encoding="utf-8")

import anthropic

# ── 경로 설정 ─────────────────────────────────────────────────
BASE_DIR    = Path("C:/GitHub/new_git/money")
CACHE_DIR   = BASE_DIR / "cache"
OUTPUT_DIR  = BASE_DIR / "results"
APPLOG_DIR  = BASE_DIR / "applogs"
GENERATED_DIR = BASE_DIR / "generated"

for d in (CACHE_DIR, OUTPUT_DIR, APPLOG_DIR, GENERATED_DIR):
    d.mkdir(parents=True, exist_ok=True)

# ── 로거 ─────────────────────────────────────────────────────
def setup_logger() -> logging.Logger:
    logger = logging.getLogger("dummy_job_generator")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    sh.setFormatter(fmt)

    fh = logging.handlers.TimedRotatingFileHandler(
        APPLOG_DIR / "dummy_job_generator.log",
        when="midnight",
        backupCount=7,
        encoding="utf-8",
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    logger.addHandler(sh)
    logger.addHandler(fh)
    return logger

logger = setup_logger()

# ── Anthropic 클라이언트 ──────────────────────────────────────
def _get_client() -> anthropic.Anthropic:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        logger.critical("ANTHROPIC_API_KEY 환경변수가 설정되지 않았습니다.")
        logger.critical("실행 방법: set ANTHROPIC_API_KEY=your-key (Windows)")
        sys.exit(1)
    return anthropic.Anthropic(api_key=api_key)

TOOLS = [{"type": "web_search_20250305", "name": "web_search"}]


# ── Phase 1: 웹 검색으로 트렌드 수집 ────────────────────────
def _search_trends(client) -> str:
    """사람인/원티드/잡코리아 + IT 트렌드 검색 → 요약 텍스트 반환"""
    logger.info("[Phase 1] 트렌드 검색 시작")

    messages = [
        {
            "role": "user",
            "content": """아래 4가지를 웹 검색하고 결과를 간결하게 요약해주세요.

1. "사람인 IT 개발자 채용공고 2026" → 공고 구조, 필드, 경력/급여 표기 방식
2. "원티드 백엔드 채용공고 2026" → 공고 구조 차이점
3. "2026 IT 개발자 인기 기술스택 채용 트렌드" → 인기 언어/프레임워크/툴
4. "2026 IT 개발자 연봉 복지 트렌드" → 연봉 수준, 복지 트렌드

검색 후 아래 형식으로 요약 (JSON 아님, 자유 텍스트):
- 공고 구조: (주요 필드, 경력 표기 방식, 급여 표기 방식)
- 인기 기술스택: (언어, 프레임워크, 툴 목록)
- 연봉 트렌드: (직군별 연봉 범위)
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


# ── Phase 2: 트렌드 기반 공고 JSON 생성 ──────────────────────
def _generate_jobs(client, trend_summary: str, job_count: int, used_companies: list) -> dict:
    """수집한 트렌드를 바탕으로 공고 JSON만 생성"""
    import uuid
    session_id = str(uuid.uuid4())[:8]
    used_companies_str = ", ".join(used_companies) if used_companies else "없음"
    today = datetime.now().strftime("%Y-%m-%d")

    logger.info("[Phase 2] 공고 생성 시작 | count=%d | session=%s", job_count, session_id)

    prompt = f"""아래 채용 트렌드 정보를 바탕으로 일로온 채용공고 {job_count}개를 JSON으로 생성하세요.

## 수집된 트렌드 정보
{trend_summary}

## 생성 규칙
- 실제 회사명, 실제 개인정보 절대 사용 금지
- 가상 회사명은 창의적으로, 절대 재사용 금지
- 이미 사용된 회사명 (절대 재사용 금지): {used_companies_str}
- 직무 다양하게: 백엔드/프론트/데이터/AI/인프라/보안/게임/iOS/Android 등
- 모든 공고의 apply.method는 "일로온 입사지원"
- 세션 시드: {session_id}

반드시 순수 JSON만 출력. 마크다운/설명 없이:
{{
  "site": "일로온",
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
      "job_id": "여기에_UUID_직접_생성",
      "status": "active",
      "always_open": false,
      "company": {{
        "id": "여기에_UUID_직접_생성",
        "name": "가상회사명",
        "industry": {{"large": "IT/인터넷", "mid": "세부업종"}},
        "size": "스타트업|중소기업|중견기업|대기업",
        "employee_count": 100
      }},
      "position": {{
        "title": "직무명",
        "job_category": {{"large": "개발", "mid": "세부직군", "small": "세부직종"}},
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
    except Exception as e:
        logger.exception("[Phase 2] 예기치 못한 오류: %s", e)
        raise


# ── Agent 실행 (2단계) ────────────────────────────────────────
def run_agent(job_count: int = 5, used_companies: list = []) -> dict:
    client = _get_client()

    # Phase 1: 검색
    trend_summary = _search_trends(client)

    # Phase 2: 생성
    result = _generate_jobs(client, trend_summary, job_count, used_companies)
    return result


# ── FastAPI 코드 자동 생성 ────────────────────────────────────
def build_fastapi(result: dict) -> str:
    if "jobs" not in result:
        logger.warning("jobs 필드 없음 — FastAPI 코드 생성 스킵")
        return ""

    jobs_json   = json.dumps(result["jobs"], ensure_ascii=False, indent=2)
    trend       = result.get("trend_summary", {})
    site        = result.get("site", "채용사이트")
    analyzed_at = result.get("analyzed_at", "")

    return f'''"""
{site} 스타일 채용공고 API
Agent가 실시간 트렌드 분석 후 자동 생성

분석 시점: {analyzed_at}
인기 프레임워크: {trend.get("hot_frameworks", [])}
인기 언어:       {trend.get("hot_languages", [])}
인기 툴:         {trend.get("hot_tools", [])}
연봉 트렌드:     {trend.get("salary_trend", "")}
"""

from fastapi import FastAPI, HTTPException, Query
from typing import Optional
import random
import hashlib

app = FastAPI(
    title="{site} 스타일 채용공고 API",
    description="Agent가 실시간 트렌드 분석 후 자동 생성한 API",
    version="1.0.0",
)

JOBS = {jobs_json}
JOB_MAP = {{j.get("job_id", str(i)): j for i, j in enumerate(JOBS)}}


@app.get("/api/v1/jobs/{{job_id}}")
async def get_job(
    job_id: str,
    user_id: Optional[str] = Query(None, description="유저 ID (AI 매칭 점수 반환)"),
):
    if job_id not in JOB_MAP:
        raise HTTPException(
            status_code=404,
            detail={{
                "result": "error",
                "error_code": "ERR_JOB_NOT_FOUND",
                "message": f"공고를 찾을 수 없습니다: {{job_id}}",
            }},
        )
    job = JOB_MAP[job_id].copy()

    if user_id:
        seed = int(hashlib.md5(f"{{user_id}}{{job_id}}".encode()).hexdigest(), 16)
        rng  = random.Random(seed)
        skills = job.get("skills", [])
        job["ai_recommendation"] = {{
            "is_recommended": True,
            "match_score": round(rng.uniform(0.60, 0.99), 2),
            "match_reasons": rng.sample(skills, k=min(2, len(skills))),
        }}

    return {{"result": "success", "data": {{"job": job}}}}


@app.get("/api/v1/jobs")
async def list_jobs(
    sido: Optional[str]        = Query(None, description="지역 필터"),
    career_type: Optional[str] = Query(None, description="경력 필터 (신입|경력|신입·경력)"),
    skill: Optional[str]       = Query(None, description="기술스택 필터"),
):
    jobs = list(JOB_MAP.values())

    if sido:
        jobs = [j for j in jobs
                if j.get("work_condition", {{}}).get("location", {{}}).get("sido") == sido]
    if career_type:
        jobs = [j for j in jobs
                if j.get("position", {{}}).get("career", {{}}).get("type") == career_type]
    if skill:
        jobs = [j for j in jobs if skill in j.get("skills", [])]

    return {{"result": "success", "data": {{"jobs": jobs, "total": len(jobs)}}}}


@app.get("/api/v1/trends")
async def get_trends():
    """Agent가 분석한 현재 IT 채용 트렌드"""
    return {{
        "result": "success",
        "data": {{
            "analyzed_at": "{analyzed_at}",
            "trends": {json.dumps(trend, ensure_ascii=False)},
        }},
    }}


@app.get("/health")
async def health():
    return {{"status": "ok", "generated_by": "agent", "site": "{site}"}}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
'''


# ── 회사명 중복 방지 ──────────────────────────────────────────
USED_COMPANIES_PATH = CACHE_DIR / "used_companies.json"

PG_CONFIG = {
    "host":     os.environ.get("PG_HOST",     "localhost"),
    "port":     int(os.environ.get("PG_PORT", "5432")),
    "dbname":   os.environ.get("PG_JOB_DB",  "iloon_jobs"),
    "user":     os.environ.get("PG_USER",     "airflow"),
    "password": os.environ.get("PG_PASSWORD", "airflow"),
}


def _companies_from_db() -> list:
    """PostgreSQL jobs 테이블에서 회사명 조회"""
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


def _companies_from_jsonl() -> list:
    """logs/*.jsonl 파일에서 회사명 수집"""
    names = []
    logs_dir = BASE_DIR / "logs"
    for fpath in sorted(logs_dir.glob("dummy-jobs-*.jsonl")):
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
                        if isinstance(company, dict):
                            name = company.get("name", "")
                        else:
                            name = str(company)
                        if name:
                            names.append(name)
        except Exception as e:
            logger.warning("JSONL 회사명 스캔 실패 (%s): %s", fpath.name, e)
    logger.info("JSONL에서 기존 회사명 %d개 로드", len(names))
    return names


def _companies_from_cache() -> list:
    """cache/used_companies.json 로드"""
    if USED_COMPANIES_PATH.exists():
        try:
            with open(USED_COMPANIES_PATH, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return []


def load_used_companies() -> list:
    """DB + JSONL + 캐시 파일 3곳 모두 스캔해서 회사명 합산"""
    all_names = (
        _companies_from_db()
        + _companies_from_jsonl()
        + _companies_from_cache()
    )
    # 중복 제거, 빈값 제거
    unique = list(dict.fromkeys(n for n in all_names if n))
    logger.info("중복 방지 회사명 총 %d개", len(unique))
    return unique


def save_used_companies(jobs: list) -> None:
    """새로 생성된 회사명을 캐시 파일에 누적 저장"""
    existing = _companies_from_cache()
    new_names = []
    for j in jobs:
        company = j.get("company", {})
        if isinstance(company, dict):
            name = company.get("name", "")
        else:
            name = str(company)
        if name:
            new_names.append(name)
    merged = list(dict.fromkeys(existing + new_names))
    try:
        with open(USED_COMPANIES_PATH, "w", encoding="utf-8") as f:
            json.dump(merged, f, ensure_ascii=False, indent=2)
        logger.info("회사명 캐시 저장: %d개", len(merged))
    except Exception as e:
        logger.warning("회사명 저장 실패: %s", e)

def _cache_path(site: str) -> Path:
    return CACHE_DIR / f"dummy_jobs_{site}_{datetime.now().strftime('%Y%m%d')}.json"


def load_cache(site: str) -> Optional[dict]:
    path = _cache_path(site)
    if path.exists():
        logger.info("캐시 재사용: %s", path)
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning("캐시 읽기 실패 (재생성): %s", e)
    return None


def save_cache(site: str, result: dict) -> None:
    path = _cache_path(site)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        logger.info("캐시 저장: %s", path)
    except Exception as e:
        logger.error("캐시 저장 실패: %s", e)


# ── UUID 주입 ─────────────────────────────────────────────────
def _inject_uuid(jobs: list) -> list:
    """LLM이 job_001 같은 고정 ID를 썼을 경우 UUID로 교체"""
    import uuid as _uuid
    result = []
    for job in jobs:
        job = dict(job)
        job_id = str(job.get("job_id", ""))
        # 고정 패턴(job_NNN, company_NNN) 또는 비어있으면 UUID로 교체
        if not job_id or job_id.startswith("job_") or job_id.startswith("UUID"):
            job["job_id"] = str(_uuid.uuid4())
        company = job.get("company", {})
        if isinstance(company, dict):
            c_id = str(company.get("id", ""))
            if not c_id or c_id.startswith("company_") or c_id.startswith("UUID"):
                company["id"] = str(_uuid.uuid4())
            job["company"] = company
        result.append(job)
    return result


# ── 결과 저장 ─────────────────────────────────────────────────
def save_results(result: dict, site: str = "일로온") -> None:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # UUID 주입 (LLM이 고정값 썼을 경우 대비)
    jobs = _inject_uuid(result.get("jobs", []))
    result = {**result, "jobs": jobs}

    # JSON 원본 저장
    json_path = OUTPUT_DIR / f"dummy_jobs_일로온_{ts}.json"
    try:
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        logger.info("결과 저장: %s", json_path)
    except Exception as e:
        logger.error("결과 저장 실패: %s", e)

    # JSONL (Spark/OpenSearch 용)
    jsonl_path = BASE_DIR / "logs" / f"dummy-jobs-{datetime.now().strftime('%Y-%m-%d')}.jsonl"
    try:
        with open(jsonl_path, "a", encoding="utf-8") as f:
            for job in jobs:
                f.write(json.dumps(job, ensure_ascii=False) + "\n")
        logger.info("JSONL 저장: %s (%d건)", jsonl_path, len(jobs))
    except Exception as e:
        logger.error("JSONL 저장 실패: %s", e)

    # FastAPI 코드 저장
    api_code = build_fastapi(result)
    if api_code:
        api_path = GENERATED_DIR / f"job_api_일로온_{ts}.py"
        try:
            with open(api_path, "w", encoding="utf-8") as f:
                f.write(api_code)
            logger.info("FastAPI 코드 생성: %s", api_path)
        except Exception as e:
            logger.error("FastAPI 코드 저장 실패: %s", e)


# ── 메인 ─────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="더미 채용공고 생성 Agent")
    parser.add_argument("--count",     default=5, type=int, help="생성할 공고 수 (기본: 5, 토큰 한계로 5 권장)")
    parser.add_argument("--no-cache",  action="store_true", help="캐시 무시하고 새로 생성 (기본값)")
    parser.add_argument("--use-cache", action="store_true", help="당일 캐시 재사용")
    args = parser.parse_args()

    logger.info("========== 더미 공고 생성 시작 ==========")
    logger.info("count=%d", args.count)

    # 이전 회사명 로드
    used_companies = load_used_companies()
    if used_companies:
        logger.info("이전 사용 회사명 %d개 로드 (중복 방지)", len(used_companies))

    # 캐시 확인 (--use-cache 명시할 때만 재사용)
    result = load_cache("일로온") if args.use_cache else None

    # Agent 실행
    if not result:
        try:
            result = run_agent(job_count=args.count, used_companies=used_companies)
        except Exception as e:
            logger.critical("Agent 실행 실패: %s", e)
            sys.exit(1)

        if not result or "jobs" not in result:
            logger.critical("유효한 결과 없음")
            sys.exit(1)

        save_cache("일로온", result)

    # 회사명 저장
    save_used_companies(result.get("jobs", []))

    # 결과 저장
    save_results(result, "일로온")

    # 트렌드 요약 출력
    trend = result.get("trend_summary", {})
    print("\n─── 실시간 IT 트렌드 분석 결과 ───")
    print(json.dumps(trend, ensure_ascii=False, indent=2))

    # 공고 샘플 출력
    jobs = result.get("jobs", [])
    if jobs:
        print(f"\n─── 생성된 공고 샘플 (1/{len(jobs)}) ───")
        print(json.dumps(jobs[0], ensure_ascii=False, indent=2))

    logger.info("========== 완료 | 공고 %d개 생성 ==========", len(jobs))


if __name__ == "__main__":
    main()
