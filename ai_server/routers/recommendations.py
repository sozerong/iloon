"""추천 공고 라우터"""

from typing import List

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from ..database import get_user_db, get_job_db
from ..models.user import AIRecommendation, GeneralRecommendation
from ..models.job import Job
from ..schemas.user import AIRecommendationOut, GeneralRecommendationOut
from ..schemas.job import JobListItem
from ..services.recommendation_service import general_recommend, ai_recommend

router = APIRouter(prefix="/recommendations", tags=["recommendations"])


@router.post("/{user_id}/general", response_model=List[GeneralRecommendationOut])
async def refresh_general(
    user_id: str,
    top_k: int = 10,
    user_db: AsyncSession = Depends(get_user_db),
    job_db: AsyncSession  = Depends(get_job_db),
):
    """설문 기반 일반 추천 갱신"""
    return await general_recommend(user_id, user_db, job_db, top_k=top_k)


@router.get("/{user_id}/general", response_model=List[JobListItem])
async def get_general(
    user_id: str,
    user_db: AsyncSession = Depends(get_user_db),
    job_db: AsyncSession  = Depends(get_job_db),
):
    """저장된 일반 추천 공고 목록 (공고 상세 포함)"""
    result = await user_db.execute(
        select(GeneralRecommendation)
        .where(GeneralRecommendation.user_id == user_id)
        .order_by(GeneralRecommendation.created_at.desc())
    )
    recs = result.scalars().all()
    job_ids = [r.job_id for r in recs]

    if not job_ids:
        return []

    jobs_result = await job_db.execute(select(Job).where(Job.id.in_(job_ids)))
    jobs_map = {j.id: j for j in jobs_result.scalars().all()}
    return [jobs_map[jid] for jid in job_ids if jid in jobs_map]


@router.post("/{user_id}/ai", response_model=List[AIRecommendationOut])
async def refresh_ai(
    user_id: str,
    top_k: int = 5,
    user_db: AsyncSession = Depends(get_user_db),
    job_db: AsyncSession  = Depends(get_job_db),
):
    """이력서 기반 AI 추천 갱신 (LLM 호출, 느릴 수 있음)"""
    return await ai_recommend(user_id, user_db, job_db, top_k=top_k)


@router.get("/{user_id}/ai", response_model=List[AIRecommendationOut])
async def get_ai(
    user_id: str,
    user_db: AsyncSession = Depends(get_user_db),
):
    """저장된 AI 추천 목록"""
    result = await user_db.execute(
        select(AIRecommendation)
        .where(AIRecommendation.user_id == user_id)
        .order_by(AIRecommendation.match_score.desc())
    )
    return result.scalars().all()
