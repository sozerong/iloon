"""이력서 분석 라우터"""

from typing import List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from ..database import get_user_db
from ..models.user import ResumeAnalysis
from ..schemas.user import ResumeAnalysisCreate, ResumeAnalysisOut
from ..services.resume_service import analyze_resume

router = APIRouter(prefix="/resume", tags=["resume"])


@router.post("/{user_id}/analyze", response_model=ResumeAnalysisOut)
async def analyze(
    user_id: str,
    body: ResumeAnalysisCreate,
    db: AsyncSession = Depends(get_user_db),
):
    """이력서 텍스트 → AI 분석 → 저장"""
    result = await analyze_resume(user_id, body.original_text, db)
    return result


@router.get("/{user_id}/history", response_model=List[ResumeAnalysisOut])
async def history(
    user_id: str,
    db: AsyncSession = Depends(get_user_db),
):
    """이력서 분석 이력 조회 (최신순)"""
    result = await db.execute(
        select(ResumeAnalysis)
        .where(ResumeAnalysis.user_id == user_id)
        .order_by(ResumeAnalysis.created_at.desc())
    )
    return result.scalars().all()


@router.get("/{user_id}/latest", response_model=ResumeAnalysisOut)
async def latest(
    user_id: str,
    db: AsyncSession = Depends(get_user_db),
):
    """가장 최신 이력서 분석 결과"""
    result = await db.execute(
        select(ResumeAnalysis)
        .where(ResumeAnalysis.user_id == user_id)
        .order_by(ResumeAnalysis.created_at.desc())
        .limit(1)
    )
    analysis = result.scalar_one_or_none()
    if not analysis:
        raise HTTPException(status_code=404, detail="이력서 분석 내역이 없습니다.")
    return analysis
