"""로그 DB 모델

user_activity_logs : 유저 행동 로그
  event_type 목록 (분석 스크립트 기준):
    [공고 관련]
    job_detail_view  - 공고 상세 조회        (analyze_ai_vs_normal, analyze_job_popularity, analyze_user_segmentation)
    bookmark         - 공고 북마크            (analyze_ai_vs_normal, analyze_job_popularity, analyze_user_segmentation)
    apply_click      - 외부 지원 링크 클릭    (analyze_ai_vs_normal, analyze_job_popularity, analyze_user_segmentation)
    search_job       - 공고 검색

    [문서/기타]
    analyze_document - 문서 분석 요청
    view_feedback    - AI 피드백 조회
    submit_survey    - 설문 제출/수정

  is_ai_recommended 로 AI 추천 공고 여부 구분
  (기존 view_ai_rec / view_general_rec 역할을 job_detail_view + is_ai_recommended 로 통합)
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional
from sqlalchemy import String, Text, DateTime, Integer, Float, Boolean, Index
from sqlalchemy.orm import Mapped, mapped_column
from ..database import UserBase


class UserActivityLog(UserBase):
    __tablename__ = "user_activity_logs"
    __table_args__ = (
        Index("ix_ual_user_id",    "user_id"),
        Index("ix_ual_event_type", "event_type"),
        Index("ix_ual_created_at", "created_at"),
    )

    id:                Mapped[str]            = mapped_column(String(36), primary_key=True)
    user_id:           Mapped[str]            = mapped_column(String(36))

    # 이벤트 식별
    event_type:        Mapped[str]            = mapped_column(String(50))    # 위 목록 참고

    # 공고 관련 컬럼 (job_detail_view / bookmark / apply_click 시 사용)
    job_id:            Mapped[Optional[str]]  = mapped_column(String(50))    # iloon_jobs.jobs.id 논리적 참조
    is_ai_recommended: Mapped[Optional[bool]] = mapped_column(Boolean)       # AI 추천 공고 여부
    match_score:       Mapped[Optional[float]]= mapped_column(Float)         # AI 매칭 점수
    region_sido:       Mapped[Optional[str]]  = mapped_column(String(50))    # 공고 지역 (지역별 전환율 분석용)
    time_on_page_sec:  Mapped[Optional[int]]  = mapped_column(Integer)       # 체류 시간(초)
    session_id:        Mapped[Optional[str]]  = mapped_column(String(36))    # 세션 ID (분석 4 join key)
    session_duration:  Mapped[Optional[int]]  = mapped_column(Integer)       # 세션 지속 시간(초)

    # 공고 외 이벤트 (analyze_document / view_feedback / submit_survey 시 사용)
    target_type:       Mapped[Optional[str]]  = mapped_column(String(30))    # document | survey
    target_id:         Mapped[Optional[str]]  = mapped_column(String(50))    # 대상 리소스 ID

    # 기타 컨텍스트
    meta:              Mapped[Optional[str]]  = mapped_column(Text)           # 추가 JSON

    created_at:        Mapped[datetime]       = mapped_column(DateTime, default=datetime.utcnow)
