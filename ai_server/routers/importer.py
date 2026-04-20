"""공고 임포트 라우터"""

from typing import Optional
from fastapi import APIRouter, Depends, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_job_db
from ..services.job_importer import import_all_jobs

router = APIRouter(prefix="/import", tags=["import"])


@router.post("/jobs")
async def import_jobs(
    background_tasks: BackgroundTasks,
    jsonl_dir: Optional[str] = None,
    index_opensearch: bool = True,
    db: AsyncSession = Depends(get_job_db),
):
    """
    JSONL 파일 → PostgreSQL + OpenSearch 임포트

    - `jsonl_dir`: JSONL 디렉토리 경로 (기본: config.JOBS_JSONL_DIR)
    - `index_opensearch`: 더미 공고를 OpenSearch에도 인덱싱 여부
    """
    result = await import_all_jobs(
        job_db           = db,
        jsonl_dir        = jsonl_dir,
        index_opensearch = index_opensearch,
    )
    return {"status": "ok", **result}
