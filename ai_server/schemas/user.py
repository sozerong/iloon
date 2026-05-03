"""유저 관련 Pydantic 스키마"""

from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field


# ── Survey ─────────────────────────────────────────────────────────────────────
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


# ── Document ───────────────────────────────────────────────────────────────────
class DocumentCreate(BaseModel):
    type:          str           = Field("resume", description="resume | cover_letter | portfolio")
    title:         Optional[str] = Field(None, description="문서 제목")
    original_text: Optional[str] = Field(None, description="원본 텍스트")
    file_url:      Optional[str] = Field(None, description="포트폴리오 파일 URL")


class DocumentOut(BaseModel):
    id:            str
    user_id:       str
    type:          str
    title:         Optional[str]
    original_text: Optional[str]
    ai_summary:    Optional[str]
    total_score:   Optional[float]
    file_url:      Optional[str]
    created_at:    datetime
    updated_at:    datetime

    model_config = {"from_attributes": True}


class DocumentListItem(BaseModel):
    """목록 조회용 (요약 — 원본 텍스트 제외)"""
    id:          str
    user_id:     str
    type:        str
    title:       Optional[str]
    total_score: Optional[float]
    created_at:  datetime
    updated_at:  datetime

    model_config = {"from_attributes": True}


class DocumentWithScores(DocumentOut):
    """세부 점수 포함 — 이력서/자소서 세부 페이지용"""
    scores:    List["DocumentScoreOut"]  = []
    feedbacks: List["AIFeedbackOut"]     = []

    model_config = {"from_attributes": True}


# ── DocumentScore ──────────────────────────────────────────────────────────────
class DocumentScoreOut(BaseModel):
    id:          str
    document_id: str
    category:    str
    score:       float
    max_score:   float
    created_at:  datetime

    model_config = {"from_attributes": True}


# ── AIFeedback ─────────────────────────────────────────────────────────────────
class AIFeedbackOut(BaseModel):
    id:            str
    document_id:   str
    user_id:       str
    feedback_type: str
    section:       Optional[str]
    content:       str
    model_version: Optional[str]
    created_at:    datetime

    model_config = {"from_attributes": True}


# ── AIRecommendation ───────────────────────────────────────────────────────────
class AIRecommendationOut(BaseModel):
    id:          str
    user_id:     str
    job_id:      str
    match_score: Optional[float]
    reason:      Optional[str]
    created_at:  datetime

    model_config = {"from_attributes": True}


# ── GeneralRecommendation ──────────────────────────────────────────────────────
class GeneralRecommendationOut(BaseModel):
    id:         str
    user_id:    str
    job_id:     str
    created_at: datetime

    model_config = {"from_attributes": True}


# ── User ───────────────────────────────────────────────────────────────────────
class UserOut(BaseModel):
    id:         str
    created_at: datetime

    model_config = {"from_attributes": True}
