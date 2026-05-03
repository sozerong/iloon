"""유저 행동 로그 라우터

프론트엔드에서 이벤트 발생 시 POST /logs/{user_id}/event 호출 → user_activity_logs 저장
"""

import uuid
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_user_db
from ..models.log import UserActivityLog
from ..schemas.log import ActivityLogCreate, ActivityLogOut

router = APIRouter(prefix="/logs", tags=["logs"])

# 허용된 event_type 목록
VALID_EVENT_TYPES = {
    "job_detail_view",
    "bookmark",
    "apply_click",
    "search_job",
    "analyze_document",
    "view_feedback",
    "submit_survey",
}


@router.post("/{user_id}/event", response_model=ActivityLogOut, status_code=201)
async def record_event(
    user_id: str,
    body: ActivityLogCreate,
    db: AsyncSession = Depends(get_user_db),
):
    """프론트엔드 이벤트 수신 → user_activity_logs 저장

    프론트에서 보내야 할 최소 필드:
      - event_type (필수)
      - job_detail_view / bookmark / apply_click → job_id, is_ai_recommended, session_id 권장
      - search_job → meta(keyword 등) 권장
      - analyze_document / view_feedback → target_type, target_id 권장
    """
    if body.event_type not in VALID_EVENT_TYPES:
        raise HTTPException(
            status_code=422,
            detail=f"유효하지 않은 event_type: {body.event_type}. 허용값: {sorted(VALID_EVENT_TYPES)}",
        )

    log = UserActivityLog(
        id                = str(uuid.uuid4()),
        user_id           = user_id,
        event_type        = body.event_type,
        job_id            = body.job_id,
        is_ai_recommended = body.is_ai_recommended,
        match_score       = body.match_score,
        region_sido       = body.region_sido,
        time_on_page_sec  = body.time_on_page_sec,
        session_id        = body.session_id,
        session_duration  = body.session_duration,
        target_type       = body.target_type,
        target_id         = body.target_id,
        meta              = body.meta,
    )
    db.add(log)
    await db.commit()
    await db.refresh(log)
    return log
