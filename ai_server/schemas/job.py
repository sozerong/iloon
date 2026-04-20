"""공고 관련 Pydantic 스키마"""

from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field


class JobOut(BaseModel):
    id:           str
    title:        Optional[str]
    company:      Optional[str]
    location:     Optional[str]
    job_type:     Optional[str]
    occupation:   Optional[str]
    career_type:  Optional[str]
    education:    Optional[str]
    salary:       Optional[str]
    description:  Optional[str]
    requirements: Optional[str]
    preferred:    Optional[str]
    benefits:     Optional[str]
    process:      Optional[str]
    deadline:     Optional[str]
    source:       Optional[str]
    url:          Optional[str]
    view_count:   int
    created_at:   datetime

    model_config = {"from_attributes": True}


class JobListItem(BaseModel):
    """목록 조회용 (요약)"""
    id:          str
    title:       Optional[str]
    company:     Optional[str]
    location:    Optional[str]
    job_type:    Optional[str]
    career_type: Optional[str]
    education:   Optional[str]
    salary:      Optional[str]
    deadline:    Optional[str]
    view_count:  int
    created_at:  datetime

    model_config = {"from_attributes": True}


class JobSearchParams(BaseModel):
    keyword:     Optional[str] = None   # 검색어 (제목, 회사, 설명)
    location:    Optional[str] = None
    job_type:    Optional[str] = None
    career_type: Optional[str] = None
    education:   Optional[str] = None
    page:        int = Field(1, ge=1)
    size:        int = Field(20, ge=1, le=100)


class JobListResponse(BaseModel):
    total: int
    page:  int
    size:  int
    items: List[JobListItem]


class SimilarJobsResponse(BaseModel):
    job_id:       str
    similar_jobs: List[JobListItem]


# ── Chat ───────────────────────────────────────────────────────
class ChatRequest(BaseModel):
    user_id: Optional[str] = None
    message: str = Field(..., min_length=1, max_length=2000)
    history: List[dict] = Field(default_factory=list, description="[{role, content}, ...]")


class ChatResponse(BaseModel):
    answer:  str
    sources: List[str] = Field(default_factory=list, description="참조 공고 ID 목록")
