"""공고 라우터"""

from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, or_

from ..database import get_job_db
from ..models.job import Job
from ..schemas.job import JobOut, JobListItem, JobListResponse, SimilarJobsResponse
from ..services.recommendation_service import find_similar_jobs

router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.get("", response_model=JobListResponse)
async def list_jobs(
    keyword:     Optional[str] = Query(None, description="검색어"),
    location:    Optional[str] = Query(None),
    job_type:    Optional[str] = Query(None),
    career_type: Optional[str] = Query(None),
    education:   Optional[str] = Query(None),
    page:        int           = Query(1, ge=1),
    size:        int           = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_job_db),
):
    """전체 공고 목록 (필터 + 페이지네이션)"""
    query = select(Job)
    conditions = []

    if keyword:
        kw = f"%{keyword}%"
        conditions.append(
            or_(Job.title.ilike(kw), Job.company.ilike(kw), Job.description.ilike(kw))
        )
    if location:
        conditions.append(Job.location.ilike(f"%{location}%"))
    if job_type:
        conditions.append(Job.job_type.ilike(f"%{job_type}%"))
    if career_type:
        conditions.append(Job.career_type.ilike(f"%{career_type}%"))
    if education:
        conditions.append(Job.education.ilike(f"%{education}%"))

    if conditions:
        query = query.where(*conditions)

    # 전체 건수
    count_result = await db.execute(select(func.count()).select_from(query.subquery()))
    total = count_result.scalar_one()

    # 페이지네이션
    query = query.order_by(Job.created_at.desc()).offset((page - 1) * size).limit(size)
    result = await db.execute(query)
    jobs = result.scalars().all()

    return JobListResponse(total=total, page=page, size=size, items=jobs)


@router.get("/all", response_model=List[JobListItem])
async def list_all_jobs(
    db: AsyncSession = Depends(get_job_db),
):
    """전체 공고 리스트 (페이지네이션 없음)"""
    result = await db.execute(select(Job).order_by(Job.created_at.desc()))
    return result.scalars().all()


@router.get("/{job_id}", response_model=JobOut)
async def get_job(
    job_id: str,
    db: AsyncSession = Depends(get_job_db),
):
    """공고 상세 (조회수 증가)"""
    result = await db.execute(select(Job).where(Job.id == job_id))
    job = result.scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail="공고를 찾을 수 없습니다.")

    job.view_count += 1
    await db.commit()
    await db.refresh(job)
    return job


@router.get("/{job_id}/similar", response_model=SimilarJobsResponse)
async def similar_jobs(
    job_id: str,
    top_k: int = Query(6, ge=1, le=20),
    db: AsyncSession = Depends(get_job_db),
):
    """유사 공고 (임베딩 코사인 유사도 또는 같은 직무)"""
    jobs = await find_similar_jobs(job_id, db, top_k=top_k)
    return SimilarJobsResponse(job_id=job_id, similar_jobs=jobs)
