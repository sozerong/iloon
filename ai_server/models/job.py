"""공고 DB 모델"""

from __future__ import annotations

from datetime import datetime
from typing import Optional
from sqlalchemy import String, Float, Text, DateTime, Integer
from sqlalchemy.orm import Mapped, mapped_column
from ..database import JobBase


class Job(JobBase):
    __tablename__ = "jobs"

    id:             Mapped[str]            = mapped_column(String(50),  primary_key=True)   # 공고 ID
    title:          Mapped[Optional[str]]  = mapped_column(String(300))                     # 공고 제목
    company:        Mapped[Optional[str]]  = mapped_column(String(200))                     # 회사명
    location:       Mapped[Optional[str]]  = mapped_column(String(200))                     # 근무지
    job_type:       Mapped[Optional[str]]  = mapped_column(String(100))                     # 직무
    occupation:     Mapped[Optional[str]]  = mapped_column(String(100))                     # 직업군
    career_type:    Mapped[Optional[str]]  = mapped_column(String(50))                      # 신입|경력
    education:      Mapped[Optional[str]]  = mapped_column(String(100))                     # 학력
    salary:         Mapped[Optional[str]]  = mapped_column(String(100))                     # 급여
    description:    Mapped[Optional[str]]  = mapped_column(Text)                            # 공고 상세
    requirements:   Mapped[Optional[str]]  = mapped_column(Text)                            # 지원 자격
    preferred:      Mapped[Optional[str]]  = mapped_column(Text)                            # 우대 사항
    benefits:       Mapped[Optional[str]]  = mapped_column(Text)                            # 복리후생
    process:        Mapped[Optional[str]]  = mapped_column(Text)                            # 채용 절차
    deadline:       Mapped[Optional[str]]  = mapped_column(String(100))                     # 마감일
    source:         Mapped[Optional[str]]  = mapped_column(String(50))                      # 출처 (일로온/scraper)
    url:            Mapped[Optional[str]]  = mapped_column(String(500))                     # 원본 URL
    embedding:      Mapped[Optional[str]]  = mapped_column(Text)                            # 임베딩 벡터 (JSON)
    view_count:     Mapped[int]            = mapped_column(Integer, default=0)              # 조회수
    created_at:     Mapped[datetime]       = mapped_column(DateTime, default=datetime.utcnow)
    updated_at:     Mapped[datetime]       = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
