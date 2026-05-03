"""유저 DB 모델 — 설문, 문서(이력서/자소서/포트폴리오), AI 피드백, 추천 공고"""

from __future__ import annotations

from datetime import datetime
from typing import Optional, List
from sqlalchemy import String, Float, Text, DateTime, ForeignKey, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship
from ..database import UserBase


# ── User ──────────────────────────────────────────────────────────────────────
class User(UserBase):
    __tablename__ = "users"

    id:         Mapped[str]      = mapped_column(String(36), primary_key=True)  # 메인 서버에서 전달받는 UUID
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    survey:       Mapped[Optional["Survey"]]            = relationship(back_populates="user", uselist=False)
    documents:    Mapped[List["Document"]]               = relationship(back_populates="user")
    ai_recs:      Mapped[List["AIRecommendation"]]       = relationship(back_populates="user")
    general_recs: Mapped[List["GeneralRecommendation"]]  = relationship(back_populates="user")


# ── Survey ────────────────────────────────────────────────────────────────────
class Survey(UserBase):
    __tablename__ = "surveys"

    id:          Mapped[str]           = mapped_column(String(36), primary_key=True)
    user_id:     Mapped[str]           = mapped_column(ForeignKey("users.id"), unique=True)
    job_type:    Mapped[Optional[str]] = mapped_column(String(100))   # 직무
    region:      Mapped[Optional[str]] = mapped_column(String(100))   # 근무 희망 지역
    occupation:  Mapped[Optional[str]] = mapped_column(String(100))   # 직업군
    career_type: Mapped[Optional[str]] = mapped_column(String(20))    # 신입 | 경력
    education:   Mapped[Optional[str]] = mapped_column(String(50))    # 학력
    created_at:  Mapped[datetime]      = mapped_column(DateTime, default=datetime.utcnow)
    updated_at:  Mapped[datetime]      = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    user: Mapped["User"] = relationship(back_populates="survey")


# ── Document ──────────────────────────────────────────────────────────────────
class Document(UserBase):
    """이력서 / 자소서 / 포트폴리오 통합 테이블
    type: 'resume' | 'cover_letter' | 'portfolio'
    """
    __tablename__ = "documents"

    id:            Mapped[str]             = mapped_column(String(36), primary_key=True)
    user_id:       Mapped[str]             = mapped_column(ForeignKey("users.id"), index=True)
    type:          Mapped[str]             = mapped_column(String(20), default="resume")   # resume | cover_letter | portfolio
    title:         Mapped[Optional[str]]   = mapped_column(String(200))
    original_text: Mapped[Optional[str]]   = mapped_column(Text)         # 원본 텍스트
    ai_summary:    Mapped[Optional[str]]   = mapped_column(Text)         # AI 분석 요약
    total_score:   Mapped[Optional[float]] = mapped_column(Float)        # 총점 (0~100)
    file_url:      Mapped[Optional[str]]   = mapped_column(String(500))  # 포트폴리오 파일 경로
    created_at:    Mapped[datetime]        = mapped_column(DateTime, default=datetime.utcnow)
    updated_at:    Mapped[datetime]        = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    user:      Mapped["User"]               = relationship(back_populates="documents")
    scores:    Mapped[List["DocumentScore"]] = relationship(back_populates="document", cascade="all, delete-orphan")
    feedbacks: Mapped[List["AIFeedback"]]    = relationship(back_populates="document", cascade="all, delete-orphan")


# ── DocumentScore ─────────────────────────────────────────────────────────────
class DocumentScore(UserBase):
    """문서 세부 점수 (카테고리별)
    category 예시: 맞춤법, 구조, 키워드, 직무적합도, 경험기술, 가독성 ...
    """
    __tablename__ = "document_scores"

    id:          Mapped[str]   = mapped_column(String(36), primary_key=True)
    document_id: Mapped[str]   = mapped_column(ForeignKey("documents.id"), index=True)
    category:    Mapped[str]   = mapped_column(String(50))
    score:       Mapped[float] = mapped_column(Float)
    max_score:   Mapped[float] = mapped_column(Float, default=100.0)
    created_at:  Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    document: Mapped["Document"] = relationship(back_populates="scores")


# ── AIFeedback ────────────────────────────────────────────────────────────────
class AIFeedback(UserBase):
    """AI 피드백
    feedback_type: 'overall' | 'section' | 'keyword'
    section: 자기소개, 경력, 기술스택 등 (section 타입일 때만 사용)
    """
    __tablename__ = "ai_feedbacks"

    id:            Mapped[str]           = mapped_column(String(36), primary_key=True)
    document_id:   Mapped[str]           = mapped_column(ForeignKey("documents.id"), index=True)
    user_id:       Mapped[str]           = mapped_column(String(36), index=True)  # 빠른 조회를 위한 중복 저장
    feedback_type: Mapped[str]           = mapped_column(String(20), default="overall")
    section:       Mapped[Optional[str]] = mapped_column(String(100))
    content:       Mapped[str]           = mapped_column(Text)
    model_version: Mapped[Optional[str]] = mapped_column(String(50))   # 사용된 AI 모델명
    created_at:    Mapped[datetime]      = mapped_column(DateTime, default=datetime.utcnow)

    document: Mapped["Document"] = relationship(back_populates="feedbacks")


# ── AIRecommendation ──────────────────────────────────────────────────────────
class AIRecommendation(UserBase):
    """이력서 기반 AI 추천 공고"""
    __tablename__ = "ai_recommendations"

    id:          Mapped[str]             = mapped_column(String(36), primary_key=True)
    user_id:     Mapped[str]             = mapped_column(ForeignKey("users.id"), index=True)
    job_id:      Mapped[str]             = mapped_column(String(50))
    match_score: Mapped[Optional[float]] = mapped_column(Float)
    reason:      Mapped[Optional[str]]   = mapped_column(Text)
    created_at:  Mapped[datetime]        = mapped_column(DateTime, default=datetime.utcnow)

    user: Mapped["User"] = relationship(back_populates="ai_recs")


# ── GeneralRecommendation ─────────────────────────────────────────────────────
class GeneralRecommendation(UserBase):
    """설문 기반 일반 추천 공고 (벡터 검색)"""
    __tablename__ = "general_recommendations"

    id:         Mapped[str]      = mapped_column(String(36), primary_key=True)
    user_id:    Mapped[str]      = mapped_column(ForeignKey("users.id"), index=True)
    job_id:     Mapped[str]      = mapped_column(String(50))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    user: Mapped["User"] = relationship(back_populates="general_recs")
