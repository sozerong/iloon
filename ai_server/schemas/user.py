"""유저 관련 Pydantic 스키마"""

from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field


# ── Survey ─────────────────────────────────────────────────────
class SurveyCreate(BaseModel):
    job_type:    Optional[str] = Field(None, description="직무 (예: 백엔드 개발)")
    region:      Optional[str] = Field(None, description="근무 희망 지역")
    occupation:  Optional[str] = Field(None, description="직업군 (예: IT/개발)")
    career_type: Optional[str] = Field(None, description="신입 | 경력")
    education:   Optional[str] = Field(None, description="학력")


class SurveyUpdate(SurveyCreate):
    pass


class SurveyOut(SurveyCreate):
    id:         str
    user_id:    str
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


# ── ResumeAnalysis ─────────────────────────────────────────────
class ResumeAnalysisCreate(BaseModel):
    original_text: str = Field(..., description="원본 이력서 텍스트")


class ResumeAnalysisOut(BaseModel):
    id:               str
    user_id:          str
    original_text:    Optional[str]
    analyzed_content: Optional[str]
    score:            Optional[float]
    feedback:         Optional[str]
    created_at:       datetime

    model_config = {"from_attributes": True}


# ── AIRecommendation ───────────────────────────────────────────
class AIRecommendationOut(BaseModel):
    id:          str
    user_id:     str
    job_id:      str
    match_score: Optional[float]
    reason:      Optional[str]
    created_at:  datetime

    model_config = {"from_attributes": True}


# ── GeneralRecommendation ──────────────────────────────────────
class GeneralRecommendationOut(BaseModel):
    id:         str
    user_id:    str
    job_id:     str
    created_at: datetime

    model_config = {"from_attributes": True}


# ── User ───────────────────────────────────────────────────────
class UserOut(BaseModel):
    id:         str
    created_at: datetime

    model_config = {"from_attributes": True}
