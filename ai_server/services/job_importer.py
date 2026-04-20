"""
JSONL → PostgreSQL + OpenSearch 임포터

지원 소스:
  - logs/dummy-jobs-*.jsonl  (Anthropic 더미 생성기)
  - logs/job-postings.jsonl  (Playwright 스크래퍼)
"""

from __future__ import annotations

import hashlib
import json
import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import JOBS_JSONL_DIR
from ..models.job import Job
from .opensearch_service import ensure_index, bulk_index_jobs, get_model_id

logger = logging.getLogger(__name__)


# ── 유틸 ─────────────────────────────────────────────────────
def _stable_id(source: str, key: str) -> str:
    raw = f"{source}::{key}"
    return str(uuid.UUID(hashlib.md5(raw.encode()).hexdigest()))


def _str(val: Any) -> Optional[str]:
    """어떤 값이든 안전하게 문자열로"""
    if val is None:
        return None
    if isinstance(val, str):
        return val.strip() or None
    if isinstance(val, list):
        return ", ".join(str(v) for v in val if v)
    if isinstance(val, dict):
        return json.dumps(val, ensure_ascii=False)
    return str(val)


def _list_to_str(val: Any, sep: str = "\n- ") -> Optional[str]:
    """list → 줄바꿈 구분 문자열"""
    if val is None:
        return None
    if isinstance(val, list):
        items = [str(v) for v in val if v]
        return ("- " + sep.join(items)) if items else None
    return _str(val)


# ── 더미 공고 정규화 ──────────────────────────────────────────
# 실제 JSONL 구조:
# {
#   job_id, status, always_open,
#   company: {id, name, industry:{large,mid}, size, employee_count},
#   position: {title, job_category:{large,mid,small}, career:{type,min_year,max_year},
#              education:{type,required}, headcount, work_type},
#   work_condition: {location:{sido,sigungu,address}, salary:{type,min,max,negotiable,unit}, work_hours},
#   skills: [...],
#   detail: {main_tasks:[...], requirements:[...], preferred:[...], talent:[...], benefits:[...]},
#   apply: {method, document:[...]},
#   recruitment_process: {total_steps, steps:[{step,name,description,duration}], notice},
#   dates: {posted_at, deadline, updated_at},
#   ai_recommendation: {is_recommended, match_score},
#   stats: {view_count, apply_count, bookmark_count}
# }

def _normalize_dummy(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    # ── 회사 ──────────────────────────────────────────────────
    company_raw  = raw.get("company") or {}
    company_name = _str(company_raw.get("name")) if isinstance(company_raw, dict) else _str(company_raw)

    # ── 포지션 ────────────────────────────────────────────────
    pos = raw.get("position") or {}
    if isinstance(pos, dict):
        title = _str(pos.get("title"))

        jc = pos.get("job_category") or {}
        if isinstance(jc, dict):
            job_type = _str(jc.get("mid") or jc.get("large") or jc.get("small"))
        else:
            job_type = _str(jc)

        career = pos.get("career") or {}
        if isinstance(career, dict):
            career_type = _str(career.get("type"))
        else:
            career_type = _str(career)

        edu = pos.get("education") or {}
        if isinstance(edu, dict):
            education = _str(edu.get("type"))
        else:
            education = _str(edu)
    else:
        title = _str(pos)
        job_type = career_type = education = None

    # ── 근무 조건 ─────────────────────────────────────────────
    wc = raw.get("work_condition") or {}
    if isinstance(wc, dict):
        loc = wc.get("location") or {}
        if isinstance(loc, dict):
            parts = [loc.get("sido"), loc.get("sigungu")]
            location = " ".join(p for p in parts if p) or None
        else:
            location = _str(loc)

        sal = wc.get("salary") or {}
        if isinstance(sal, dict):
            sal_min  = sal.get("min")
            sal_max  = sal.get("max")
            sal_type = sal.get("type", "")
            if sal_min and sal_max:
                salary = f"{sal_type} {sal_min//10000}~{sal_max//10000}만원".strip()
            elif sal_type:
                salary = sal_type
            else:
                salary = None
        else:
            salary = _str(sal)
    else:
        location = salary = None

    # ── 상세 내용 ─────────────────────────────────────────────
    detail = raw.get("detail") or {}
    if isinstance(detail, dict):
        description  = _list_to_str(detail.get("main_tasks"))
        requirements = _list_to_str(detail.get("requirements"))
        preferred    = _list_to_str(detail.get("preferred"))
        benefits     = _list_to_str(detail.get("benefits"))
    else:
        description  = _str(detail)
        requirements = preferred = benefits = None

    # ── 기술스택 → requirements에 추가 ────────────────────────
    skills = raw.get("skills")
    if skills:
        skills_str = "기술스택: " + ", ".join(str(s) for s in skills if s)
        if requirements:
            requirements = requirements + "\n\n" + skills_str
        else:
            requirements = skills_str

    # ── 채용 절차 ─────────────────────────────────────────────
    rec = raw.get("recruitment_process") or {}
    if isinstance(rec, dict):
        steps = rec.get("steps") or []
        if isinstance(steps, list) and steps:
            step_names = []
            for s in steps:
                if isinstance(s, dict):
                    step_names.append(s.get("name", ""))
                else:
                    step_names.append(str(s))
            process = " → ".join(n for n in step_names if n)
        else:
            process = None
    else:
        process = _str(rec)

    # ── 날짜 ──────────────────────────────────────────────────
    dates = raw.get("dates") or {}
    if isinstance(dates, dict):
        deadline = _str(dates.get("deadline") or dates.get("posted_at"))
    else:
        deadline = _str(dates)

    # ── ID ────────────────────────────────────────────────────
    job_id = _str(raw.get("job_id") or raw.get("id")) or \
             _stable_id("dummy", str(title) + str(company_name))

    # ── 조회수 ────────────────────────────────────────────────
    stats = raw.get("stats") or {}
    view_count = int(stats.get("view_count", 0)) if isinstance(stats, dict) else 0

    return {
        "id":           job_id,
        "title":        title,
        "company":      company_name,
        "location":     location,
        "job_type":     job_type,
        "occupation":   job_type,
        "career_type":  career_type,
        "education":    education,
        "salary":       salary,
        "description":  description,
        "requirements": requirements,
        "preferred":    preferred,
        "benefits":     benefits,
        "process":      process,
        "deadline":     deadline,
        "source":       "dummy",
        "url":          raw.get("url"),
        "view_count":   view_count,
        "created_at":   datetime.utcnow(),
        "updated_at":   datetime.utcnow(),
    }


# ── 스크래퍼 공고 정규화 ──────────────────────────────────────
def _normalize_scraper(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    url    = _str(raw.get("company_url") or raw.get("url")) or ""
    job_id = _stable_id(
        "scraper",
        url or str(raw.get("job_title", "")) + str(raw.get("company_name", ""))
    )
    return {
        "id":           job_id,
        "title":        _str(raw.get("job_title") or raw.get("title")),
        "company":      _str(raw.get("company_name") or raw.get("company")),
        "location":     _str(raw.get("region") or raw.get("location")),
        "job_type":     _str(raw.get("categories") or raw.get("job_type")),
        "occupation":   _str(raw.get("categories")),
        "career_type":  _str(raw.get("employment_type") or raw.get("experience") or raw.get("career_type")),
        "education":    _str(raw.get("education")),
        "salary":       _str(raw.get("salary")),
        "description":  _str(raw.get("job_description") or raw.get("description")),
        "requirements": _str(raw.get("requirements") or raw.get("required_skills")),
        "preferred":    _str(raw.get("preferred")),
        "benefits":     _str(raw.get("benefits")),
        "process":      _str(raw.get("process")),
        "deadline":     _str(raw.get("employment_deadline") or raw.get("deadline") or ""),
        "source":       "scraper",
        "url":          url,
        "view_count":   0,
        "created_at":   datetime.utcnow(),
        "updated_at":   datetime.utcnow(),
    }


def _detect_and_normalize(raw: Dict[str, Any], filename: str) -> Optional[Dict[str, Any]]:
    is_scraper = (
        "job-postings" in filename
        or "scraper" in filename
        or "company_url" in raw
        or "scraped_at" in raw
    )
    try:
        return _normalize_scraper(raw) if is_scraper else _normalize_dummy(raw)
    except Exception as e:
        logger.warning("정규화 실패 (%s): %s | raw_keys=%s", filename, e, list(raw.keys()))
        return None


# ── 파일 읽기 ─────────────────────────────────────────────────
def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    records = []
    with open(path, encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, list):
                    records.extend(obj)
                else:
                    records.append(obj)
            except json.JSONDecodeError as e:
                logger.warning("%s line %d JSON 파싱 오류: %s", path.name, lineno, e)
    return records


# ── 메인 임포터 ───────────────────────────────────────────────
async def import_all_jobs(
    job_db: AsyncSession,
    jsonl_dir: Optional[Path] = None,
    index_opensearch: bool = True,
) -> Dict[str, Any]:
    base_dir = Path(jsonl_dir) if jsonl_dir else JOBS_JSONL_DIR
    files    = sorted(base_dir.glob("*.jsonl"))

    if not files:
        logger.warning("JSONL 파일 없음: %s", base_dir)
        return {"files": 0, "parsed": 0, "imported": 0, "skipped": 0}

    all_jobs: List[Dict[str, Any]] = []
    skipped = 0

    for path in files:
        for raw in _load_jsonl(path):
            job = _detect_and_normalize(raw, path.name)
            if job and job.get("title"):
                all_jobs.append(job)
            else:
                skipped += 1

    logger.info("정규화 완료: %d건 성공, %d건 스킵", len(all_jobs), skipped)

    if not all_jobs:
        return {"files": len(files), "parsed": 0, "imported": 0, "skipped": skipped}

    # ── PostgreSQL upsert ─────────────────────────────────────
    stmt = pg_insert(Job).values(all_jobs)
    stmt = stmt.on_conflict_do_update(
        index_elements=["id"],
        set_={
            "title":        stmt.excluded.title,
            "company":      stmt.excluded.company,
            "description":  stmt.excluded.description,
            "requirements": stmt.excluded.requirements,
            "updated_at":   stmt.excluded.updated_at,
        },
    )
    await job_db.execute(stmt)
    await job_db.commit()
    logger.info("PostgreSQL upsert 완료: %d건", len(all_jobs))

    # ── OpenSearch 인덱싱 (더미만) ────────────────────────────
    if index_opensearch:
        dummy_jobs = [j for j in all_jobs if j.get("source") == "dummy"]
        if dummy_jobs:
            try:
                # ML 모델이 준비된 경우 knn_vector 인덱스 보장
                await ensure_index(model_id=get_model_id())
                ok = await bulk_index_jobs(dummy_jobs)
                logger.info("OpenSearch 인덱싱: %d건 (벡터=%s)", ok, bool(get_model_id()))
            except Exception as e:
                logger.error("OpenSearch 인덱싱 실패 (계속 진행): %s", e)

    return {
        "files":    len(files),
        "parsed":   len(all_jobs),
        "imported": len(all_jobs),
        "skipped":  skipped,
    }
