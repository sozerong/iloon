"""문서(이력서/자소서/포트폴리오) 분석 라우터"""

from typing import List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from ..database import get_user_db
from ..models.user import Document, DocumentScore, AIFeedback
from ..schemas.user import DocumentCreate, DocumentOut, DocumentListItem, DocumentWithScores
from ..services.resume_service import analyze_resume

router = APIRouter(prefix="/resume", tags=["resume"])


@router.post("/{user_id}/analyze", response_model=DocumentOut)
async def analyze(
    user_id: str,
    body: DocumentCreate,
    db: AsyncSession = Depends(get_user_db),
):
    """문서 텍스트 → AI 분석 → 저장 (총점 + 세부 점수 + 피드백)"""
    result = await analyze_resume(user_id, body, db)
    return result


@router.get("/{user_id}/history", response_model=List[DocumentListItem])
async def history(
    user_id: str,
    type: str = None,
    db: AsyncSession = Depends(get_user_db),
):
    """문서 목록 조회 (최신순) — type으로 필터 가능: resume | cover_letter | portfolio"""
    q = select(Document).where(Document.user_id == user_id)
    if type:
        q = q.where(Document.type == type)
    q = q.order_by(Document.created_at.desc())

    result = await db.execute(q)
    return result.scalars().all()


@router.get("/{user_id}/latest", response_model=DocumentOut)
async def latest(
    user_id: str,
    type: str = "resume",
    db: AsyncSession = Depends(get_user_db),
):
    """가장 최신 문서 조회"""
    result = await db.execute(
        select(Document)
        .where(Document.user_id == user_id, Document.type == type)
        .order_by(Document.created_at.desc())
        .limit(1)
    )
    doc = result.scalar_one_or_none()
    if not doc:
        raise HTTPException(status_code=404, detail="문서 분석 내역이 없습니다.")
    return doc


@router.get("/{user_id}/detail/{document_id}", response_model=DocumentWithScores)
async def detail(
    user_id: str,
    document_id: str,
    db: AsyncSession = Depends(get_user_db),
):
    """문서 세부 조회 — 총점 + 세부 점수 + AI 피드백 포함"""
    result = await db.execute(
        select(Document)
        .where(Document.id == document_id, Document.user_id == user_id)
        .options(
            selectinload(Document.scores),
            selectinload(Document.feedbacks),
        )
    )
    doc = result.scalar_one_or_none()
    if not doc:
        raise HTTPException(status_code=404, detail="문서를 찾을 수 없습니다.")
    return doc
