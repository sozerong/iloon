"""공고 DB 모델"""

from __future__ import annotations

from datetime import datetime
from typing import Optional
from sqlalchemy import String, Float, Text, DateTime, Integer, Index
from sqlalchemy.orm import Mapped, mapped_column
from ..database import JobBase


# ── Job ───────────────────────────────────────────────────────────────────────
class Job(JobBase):
    __tablename__ = "jobs"

    id:           Mapped[str]            = mapped_column(String(50),  primary_key=True)
    title:        Mapped[Optional[str]]  = mapped_column(String(300))
    company:      Mapped[Optional[str]]  = mapped_column(String(200))
    location:     Mapped[Optional[str]]  = mapped_column(String(200))
    job_type:     Mapped[Optional[str]]  = mapped_column(String(100))
    occupation:   Mapped[Optional[str]]  = mapped_column(String(100))
    career_type:  Mapped[Optional[str]]  = mapped_column(String(50))
    education:    Mapped[Optional[str]]  = mapped_column(String(100))
    salary:       Mapped[Optional[str]]  = mapped_column(String(100))
    description:  Mapped[Optional[str]]  = mapped_column(Text)
    requirements: Mapped[Optional[str]]  = mapped_column(Text)
    preferred:    Mapped[Optional[str]]  = mapped_column(Text)
    benefits:     Mapped[Optional[str]]  = mapped_column(Text)
    process:      Mapped[Optional[str]]  = mapped_column(Text)
    deadline:     Mapped[Optional[str]]  = mapped_column(String(100))
    source:       Mapped[Optional[str]]  = mapped_column(String(50))
    url:          Mapped[Optional[str]]  = mapped_column(String(500))
    embedding:    Mapped[Optional[str]]  = mapped_column(Text)           # 임베딩 벡터 (JSON 문자열)
    view_count:   Mapped[int]            = mapped_column(Integer, default=0)
    created_at:   Mapped[datetime]       = mapped_column(DateTime, default=datetime.utcnow)
    updated_at:   Mapped[datetime]       = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


# ── JobRelation ───────────────────────────────────────────────────────────────
class JobRelation(JobBase):
    """공고 간 유사도 관계 — 디테일 페이지 '관련 공고' 용
    job_id 기준으로 related_job_id 목록을 similarity_score 내림차순으로 조회
    """
    __tablename__ = "job_relations"
    __table_args__ = (
        Index("ix_job_relations_job_id", "job_id"),
    )

    id:               Mapped[str]   = mapped_column(String(36), primary_key=True)
    job_id:           Mapped[str]   = mapped_column(String(50))
    related_job_id:   Mapped[str]   = mapped_column(String(50))
    similarity_score: Mapped[float] = mapped_column(Float, default=0.0)
    created_at:       Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
