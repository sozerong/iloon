"""설문 라우터"""

import uuid
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from ..database import get_user_db
from ..models.user import User, Survey
from ..schemas.user import SurveyCreate, SurveyUpdate, SurveyOut

router = APIRouter(prefix="/survey", tags=["survey"])


async def _get_or_create_user(user_id: str, db: AsyncSession) -> User:
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if not user:
        user = User(id=user_id)
        db.add(user)
        await db.commit()
        await db.refresh(user)
    return user


@router.post("/{user_id}", response_model=SurveyOut, status_code=status.HTTP_201_CREATED)
async def create_or_update_survey(
    user_id: str,
    body: SurveyCreate,
    db: AsyncSession = Depends(get_user_db),
):
    """설문 저장 (없으면 생성, 있으면 업데이트)"""
    await _get_or_create_user(user_id, db)

    result = await db.execute(select(Survey).where(Survey.user_id == user_id))
    survey = result.scalar_one_or_none()

    if survey:
        for k, v in body.model_dump(exclude_none=True).items():
            setattr(survey, k, v)
    else:
        survey = Survey(id=str(uuid.uuid4()), user_id=user_id, **body.model_dump())
        db.add(survey)

    await db.commit()
    await db.refresh(survey)
    return survey


@router.get("/{user_id}", response_model=SurveyOut)
async def get_survey(
    user_id: str,
    db: AsyncSession = Depends(get_user_db),
):
    result = await db.execute(select(Survey).where(Survey.user_id == user_id))
    survey = result.scalar_one_or_none()
    if not survey:
        raise HTTPException(status_code=404, detail="설문 데이터가 없습니다.")
    return survey
