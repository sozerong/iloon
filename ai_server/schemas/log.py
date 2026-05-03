"""로그 이벤트 Pydantic 스키마"""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field


class ActivityLogCreate(BaseModel):
    """프론트엔드에서 전송하는 이벤트 로그"""

    event_type: str = Field(
        ...,
        description="job_detail_view | bookmark | apply_click | search_job | analyze_document | view_feedback | submit_survey",
    )

    # 공고 관련 (job_detail_view / bookmark / apply_click)
    job_id:            Optional[str]   = Field(None, description="공고 ID")
    is_ai_recommended: Optional[bool]  = Field(None, description="AI 추천 공고 여부")
    match_score:       Optional[float] = Field(None, description="AI 매칭 점수")
    region_sido:       Optional[str]   = Field(None, description="공고 지역")
    time_on_page_sec:  Optional[int]   = Field(None, description="체류 시간(초)")
    session_id:        Optional[str]   = Field(None, description="세션 ID")
    session_duration:  Optional[int]   = Field(None, description="세션 지속 시간(초)")

    # 공고 외 이벤트 (analyze_document / view_feedback / submit_survey)
    target_type: Optional[str] = Field(None, description="document | survey")
    target_id:   Optional[str] = Field(None, description="대상 리소스 ID")

    # 기타
    meta: Optional[str] = Field(None, description="추가 컨텍스트 JSON 문자열")


class ActivityLogOut(ActivityLogCreate):
    id:         str
    user_id:    str
    created_at: datetime

    model_config = {"from_attributes": True}
