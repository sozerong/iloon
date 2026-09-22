"""공고 추천 서비스

general_recommend : 설문 데이터 → neural_search → 벡터 기반 추천
ai_recommend      : 이력서 분석 → neural_search → LLM 매칭 점수 (상위 공고만)
find_similar_jobs : 공고 ID → 같은 텍스트 기반 유사 공고
"""

from __future__ import annotations

import json
import logging
import math
import uuid
from typing import List, Optional

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from ..models.job import Job
from ..models.user import AIRecommendation, GeneralRecommendation, Document, Survey
from .ollama_client import get_ollama
from .opensearch_service import neural_search, build_search_text, search_jobs

logger = logging.getLogger(__name__)


# ── 설문 → 쿼리 텍스트 ───────────────────────────────────────
def _survey_to_query(survey: Optional[Survey]) -> str:
    """설문 필드를 임베딩 검색용 자연어 텍스트로 변환"""
    if not survey:
        return ""
    parts = [
        survey.job_type    or "",
        survey.occupation  or "",
        survey.region      or "",
        survey.career_type or "",
        survey.education   or "",
    ]
    return " ".join(p for p in parts if p).strip()


# ── 코사인 유사도 (PostgreSQL 임베딩 fallback용) ───────────────
def _cosine(a: List[float], b: List[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na  = math.sqrt(sum(x ** 2 for x in a))
    nb  = math.sqrt(sum(x ** 2 for x in b))
    return dot / (na * nb + 1e-9)


# ── PostgreSQL에서 공고 조회 ──────────────────────────────────
async def _fetch_jobs_by_ids(job_ids: List[str], job_db: AsyncSession) -> List[Job]:
    """OpenSearch 결과 순서를 유지하며 PostgreSQL에서 공고 가져오기"""
    if not job_ids:
        return []
    result = await job_db.execute(select(Job).where(Job.id.in_(job_ids)))
    jobs_map = {j.id: j for j in result.scalars().all()}
    return [jobs_map[jid] for jid in job_ids if jid in jobs_map]


# ── 일반 추천 (설문 기반 Neural Search) ──────────────────────
async def general_recommend(
    user_id: str,
    user_db: AsyncSession,
    job_db:  AsyncSession,
    top_k:   int = 10,
) -> List[GeneralRecommendation]:
    """
    설문 데이터 → neural_search → 유사 공고 추천
    ML 모델 미준비 시 SQL 필터로 fallback
    """
    # 설문 조회
    result = await user_db.execute(select(Survey).where(Survey.user_id == user_id))
    survey = result.scalar_one_or_none()

    query_text = _survey_to_query(survey)
    jobs: List[Job] = []

    if query_text:
        # Neural Search
        try:
            filters: dict = {}
            if survey and survey.region:
                filters["location"] = survey.region
            if survey and survey.career_type:
                filters["career_type"] = survey.career_type

            os_result = await neural_search(
                query_text=query_text,
                filters=filters or None,
                k=top_k,
            )
            job_ids = [item["id"] for item in os_result.get("items", []) if item.get("id")]
            jobs = await _fetch_jobs_by_ids(job_ids, job_db)
            logger.info("Neural 추천: user=%s query='%s' → %d건", user_id, query_text[:30], len(jobs))
        except Exception as e:
            logger.warning("Neural Search 실패 → SQL fallback: %s", e)

    # Fallback: SQL 필터
    if not jobs:
        from sqlalchemy import or_
        query = select(Job).order_by(Job.created_at.desc())
        if survey:
            conds = []
            if survey.job_type:
                conds.append(Job.job_type.ilike(f"%{survey.job_type}%"))
            if survey.region:
                conds.append(Job.location.ilike(f"%{survey.region}%"))
            if survey.career_type:
                conds.append(Job.career_type.ilike(f"%{survey.career_type}%"))
            if conds:
                query = query.where(or_(*conds))
        result2 = await job_db.execute(query.limit(top_k))
        jobs = list(result2.scalars().all())
        logger.info("SQL fallback 추천: user=%s → %d건", user_id, len(jobs))

    # 기존 일반 추천 초기화 후 저장
    await user_db.execute(delete(GeneralRecommendation).where(GeneralRecommendation.user_id == user_id))
    recs = []
    for job in jobs:
        rec = GeneralRecommendation(
            id      = str(uuid.uuid4()),
            user_id = user_id,
            job_id  = job.id,
        )
        user_db.add(rec)
        recs.append(rec)

    await user_db.commit()
    return recs


# ── AI 추천 (이력서 + Neural Search + LLM 점수) ───────────────
AI_REC_SYSTEM = """당신은 채용 AI 전문가입니다.
지원자의 이력서 분석 내용과 공고를 보고 매칭 점수(0~100)와 추천 이유를 JSON으로만 응답하세요.

{"match_score": 85, "reason": "추천 이유 2-3문장"}"""


async def ai_recommend(
    user_id: str,
    user_db: AsyncSession,
    job_db:  AsyncSession,
    top_k:   int = 5,
) -> List[AIRecommendation]:
    """
    이력서 분석 내용 → neural_search로 후보 공고 추출
    → Ollama LLM으로 매칭 점수 계산
    """
    # 최신 이력서 분석
    result = await user_db.execute(
        select(Document)
        .where(Document.user_id == user_id, Document.type == "resume")
        .order_by(Document.created_at.desc())
        .limit(1)
    )
    analysis = result.scalar_one_or_none()
    if not analysis:
        logger.info("이력서 분석 없음: user=%s", user_id)
        return []

    # ai_summary(분석 요약)가 우선, 없으면 원문으로 대체
    resume_ctx = f"{analysis.ai_summary or analysis.original_text or ''}"
    query_text  = resume_ctx[:500]  # 임베딩 쿼리용 (너무 길면 자름)

    # Neural Search로 후보 공고 추출 (top_k * 3개 후보 → LLM으로 top_k 선별)
    candidates: List[Job] = []
    try:
        os_result = await neural_search(query_text=query_text, k=top_k * 3)
        job_ids   = [item["id"] for item in os_result.get("items", []) if item.get("id")]
        candidates = await _fetch_jobs_by_ids(job_ids, job_db)
    except Exception as e:
        logger.warning("AI 추천 Neural Search 실패 → 최신순 fallback: %s", e)

    if not candidates:
        res2 = await job_db.execute(
            select(Job).order_by(Job.created_at.desc()).limit(top_k * 3)
        )
        candidates = list(res2.scalars().all())

    if not candidates:
        return []

    ollama = get_ollama()

    # 전체 후보 스코어링
    scored: List[tuple] = []
    for job in candidates:
        job_summary = (
            f"제목: {job.title}\n"
            f"회사: {job.company}\n"
            f"직무: {job.job_type}\n"
            f"자격요건: {(job.requirements or '')[:400]}"
        )
        messages = [
            {"role": "system",  "content": AI_REC_SYSTEM},
            {"role": "user",    "content": f"[지원자]\n{resume_ctx[:600]}\n\n[공고]\n{job_summary}"},
        ]
        try:
            raw   = await ollama.chat(messages, temperature=0.2, max_tokens=512)
            start = raw.find("{")
            end   = raw.rfind("}") + 1
            data  = json.loads(raw[start:end]) if start != -1 else {}
            match_score = float(data.get("match_score", 0))
            reason      = data.get("reason", "")
        except Exception as e:
            logger.warning("LLM 점수 계산 실패 (job=%s): %s", job.id, e)
            match_score, reason = 0.0, ""
        scored.append((job, match_score, reason))

    # 점수 내림차순 정렬 → top_k 저장
    scored.sort(key=lambda x: x[1], reverse=True)

    await user_db.execute(delete(AIRecommendation).where(AIRecommendation.user_id == user_id))
    recs: List[AIRecommendation] = []
    for job, match_score, reason in scored[:top_k]:
        rec = AIRecommendation(
            id          = str(uuid.uuid4()),
            user_id     = user_id,
            job_id      = job.id,
            match_score = match_score,
            reason      = reason,
        )
        user_db.add(rec)
        recs.append(rec)

    await user_db.commit()
    logger.info("AI 추천 완료: user=%s → %d건 (후보 %d개 스코어링)", user_id, len(recs), len(scored))
    return recs


# ── 유사 공고 검색 ─────────────────────────────────────────────
async def find_similar_jobs(
    job_id: str,
    job_db: AsyncSession,
    top_k:  int = 6,
) -> List[Job]:
    """
    기준 공고의 search_text → neural_search → 유사 공고 반환
    임베딩 없으면 같은 직무 최신순 fallback
    """
    result   = await job_db.execute(select(Job).where(Job.id == job_id))
    base_job = result.scalar_one_or_none()
    if not base_job:
        return []

    # Neural Search
    try:
        query_text = build_search_text({
            "title":       base_job.title       or "",
            "job_type":    base_job.job_type     or "",
            "occupation":  base_job.occupation   or "",
            "career_type": base_job.career_type  or "",
            "location":    base_job.location     or "",
            "description": base_job.description  or "",
            "requirements":base_job.requirements or "",
        })
        os_result = await neural_search(query_text=query_text, k=top_k + 1)
        job_ids   = [
            item["id"] for item in os_result.get("items", [])
            if item.get("id") and item["id"] != job_id
        ][:top_k]
        jobs = await _fetch_jobs_by_ids(job_ids, job_db)
        if jobs:
            return jobs
    except Exception as e:
        logger.warning("유사 공고 Neural Search 실패: %s", e)

    # Fallback: 같은 직무 최신순
    result2 = await job_db.execute(
        select(Job)
        .where(Job.id != job_id, Job.job_type == base_job.job_type)
        .order_by(Job.created_at.desc())
        .limit(top_k)
    )
    return list(result2.scalars().all())
