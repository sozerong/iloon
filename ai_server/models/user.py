"""유저 DB 모델 — 설문, 이력서 분석, 추천 공고"""

from __future__ import annotations

from datetime import datetime
from typing import Optional, List
from sqlalchemy import String, Float, Text, DateTime, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from ..database import UserBase


class User(UserBase):
    __tablename__ = "users"

    id:         Mapped[str]      = mapped_column(String(36), primary_key=True)  # UUID
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    survey:           Mapped[Optional["Survey"]]                  = relationship(back_populates="user", uselist=False)
    resume_analyses:  Mapped[List["ResumeAnalysis"]]              = relationship(back_populates="user")
    ai_recs:          Mapped[List["AIRecommendation"]]            = relationship(back_populates="user")
    general_recs:     Mapped[List["GeneralRecommendation"]]       = relationship(back_populates="user")


class Survey(UserBase):
    __tablename__ = "surveys"

    id:          Mapped[str]           = mapped_column(String(36), primary_key=True)
    user_id:     Mapped[str]           = mapped_column(ForeignKey("users.id"), unique=True)
    job_type:    Mapped[Optional[str]] = mapped_column(String(100))   # 직무
    region:      Mapped[Optional[str]] = mapped_column(String(100))   # 근무지역
    occupation:  Mapped[Optional[str]] = mapped_column(String(100))   # 직업
    career_type: Mapped[Optional[str]] = mapped_column(String(20))    # 신입|경력
    education:   Mapped[Optional[str]] = mapped_column(String(50))    # 학력
    created_at:  Mapped[datetime]      = mapped_column(DateTime, default=datetime.utcnow)
    updated_at:  Mapped[datetime]      = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    user: Mapped["User"] = relationship(back_populates="survey")


class ResumeAnalysis(UserBase):
    __tablename__ = "resume_analyses"

    id:               Mapped[str]            = mapped_column(String(36), primary_key=True)
    user_id:          Mapped[str]            = mapped_column(ForeignKey("users.id"))
    original_text:    Mapped[Optional[str]]  = mapped_column(Text)       # 원본 이력서
    analyzed_content: Mapped[Optional[str]]  = mapped_column(Text)       # AI 분석 내용
    score:            Mapped[Optional[float]]= mapped_column(Float)      # 점수 (0~100)
    feedback:         Mapped[Optional[str]]  = mapped_column(Text)       # 피드백
    created_at:       Mapped[datetime]       = mapped_column(DateTime, default=datetime.utcnow)

    user: Mapped["User"] = relationship(back_populates="resume_analyses")


class AIRecommendation(UserBase):
    __tablename__ = "ai_recommendations"

    id:          Mapped[str]             = mapped_column(String(36), primary_key=True)
    user_id:     Mapped[str]             = mapped_column(ForeignKey("users.id"))
    job_id:      Mapped[str]             = mapped_column(String(50))
    match_score: Mapped[Optional[float]] = mapped_column(Float)
    reason:      Mapped[Optional[str]]   = mapped_column(Text)
    created_at:  Mapped[datetime]        = mapped_column(DateTime, default=datetime.utcnow)

    user: Mapped["User"] = relationship(back_populates="ai_recs")


class GeneralRecommendation(UserBase):
    __tablename__ = "general_recommendations"

    id:         Mapped[str]      = mapped_column(String(36), primary_key=True)
    user_id:    Mapped[str]      = mapped_column(ForeignKey("users.id"))
    job_id:     Mapped[str]      = mapped_column(String(50))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    user: Mapped["User"] = relationship(back_populates="general_recs")
