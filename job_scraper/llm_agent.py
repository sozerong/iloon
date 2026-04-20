"""
Ollama LLM 에이전트
- 채용 공고 페이지 텍스트 → 구조화된 필드 추출
- 채용 목록 페이지 → 공고 링크 목록 추출
"""

import json
import logging
import re
import time
from typing import Optional

import ollama

logger = logging.getLogger("job_scraper.llm_agent")

MODEL = "llama3"          # ollama pull llama3 필요
MAX_TEXT_LEN = 6000       # LLM에 넘길 최대 텍스트 길이 (토큰 절약)
MAX_RETRIES = 3


# ── LLM 호출 공통 ─────────────────────────────────────────────
def _call_llm(prompt: str, expect_json: bool = True) -> str:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = ollama.chat(
                model=MODEL,
                messages=[{"role": "user", "content": prompt}],
                format="json" if expect_json else "",
                options={"temperature": 0.0},  # 결정적 출력
            )
            return resp["message"]["content"]
        except ollama.ResponseError as e:
            logger.warning("[LLM] ResponseError (시도 %d/%d): %s", attempt, MAX_RETRIES, e)
            if attempt == MAX_RETRIES:
                raise
            time.sleep(2 ** attempt)
        except Exception as e:
            logger.error("[LLM] 예기치 못한 오류 (시도 %d/%d): %s", attempt, MAX_RETRIES, e)
            if attempt == MAX_RETRIES:
                raise
            time.sleep(2 ** attempt)


def _parse_json(raw: str) -> Optional[dict]:
    """LLM 응답에서 JSON 파싱. 실패 시 None 반환"""
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        # JSON 블록만 추출 시도
        match = re.search(r"\{.*\}", raw, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass
    logger.warning("[LLM] JSON 파싱 실패: %s...", raw[:200])
    return None


# ── 1. 공고 링크 추출 ─────────────────────────────────────────
LINK_PROMPT = """You are a web scraping assistant.
Below is text extracted from a company's career listing page.
Extract all job posting URLs from the text.

Rules:
- Return ONLY a JSON object with key "links" containing an array of URL strings
- Include only URLs that look like individual job posting detail pages
- If base_url is provided and links are relative paths, prepend the base_url
- Return empty array if no job links found

base_url: {base_url}

Page text (truncated):
{text}

Return format: {{"links": ["url1", "url2", ...]}}"""


def extract_job_links(page_text: str, base_url: str) -> list[str]:
    """채용 목록 페이지에서 개별 공고 링크 추출"""
    text = page_text[:MAX_TEXT_LEN]
    prompt = LINK_PROMPT.format(base_url=base_url, text=text)

    try:
        raw = _call_llm(prompt, expect_json=True)
        result = _parse_json(raw)
        if result and isinstance(result.get("links"), list):
            links = [str(l) for l in result["links"] if l]
            logger.info("[LLM] 링크 추출: %d개", len(links))
            return links
    except Exception as e:
        logger.error("[LLM] 링크 추출 실패: %s", e)

    return []


# ── 2. 공고 상세 필드 추출 ────────────────────────────────────
JOB_EXTRACT_PROMPT = """You are a Korean job posting parser. Extract structured information from the job posting text below.

Return ONLY a JSON object with these exact keys (use null if not found):
{{
  "job_title": "string - job position name",
  "categories": "string - one of: 프론트엔드, 백엔드, 데이터분석, AI/ML, 인프라/DevOps, 기획, 디자인, 보안, 게임개발, 기타",
  "employment_type": "string - one of: 정규직, 비정규직, 계약직, 인턴",
  "employment_deadline": "string - deadline in YYYY-MM-DD format or null",
  "region": "string - work location (city/district)",
  "required_skills": "string - comma-separated tech skills",
  "experience": "string - experience requirement e.g. 신입, 경력 3년 이상, 인턴 6개월",
  "education": "string - one of: 고졸, 대졸, 석사, 박사, 무관",
  "job_description": "string - main duties summary in Korean, max 300 chars",
  "requirements": "string - qualifications summary in Korean, max 300 chars"
}}

Job posting text:
{text}"""


def extract_job_fields(page_text: str, company_name: str) -> Optional[dict]:
    """채용 공고 상세 페이지에서 구조화된 필드 추출"""
    text = page_text[:MAX_TEXT_LEN]
    prompt = JOB_EXTRACT_PROMPT.format(text=text)

    try:
        raw = _call_llm(prompt, expect_json=True)
        result = _parse_json(raw)
        if result:
            logger.info("[LLM][%s] 필드 추출 완료: %s", company_name, result.get("job_title", "?"))
            return result
    except Exception as e:
        logger.error("[LLM][%s] 필드 추출 실패: %s", company_name, e)

    return None


# ── Ollama 연결 확인 ─────────────────────────────────────────
def check_ollama() -> bool:
    try:
        models = ollama.list()
        names = [m.model for m in models.models]
        if not any(MODEL in n for n in names):
            logger.warning("모델 '%s' 없음. 실행: ollama pull %s", MODEL, MODEL)
            logger.warning("설치된 모델: %s", names)
            return False
        logger.info("Ollama 연결 OK | 모델: %s", MODEL)
        return True
    except Exception as e:
        logger.error("Ollama 연결 실패: %s | ollama serve 실행 확인", e)
        return False
