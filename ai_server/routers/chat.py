"""RAG 챗봇 라우터"""

from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_job_db
from ..schemas.job import ChatRequest, ChatResponse
from ..services.rag_service import chat_rag, chat_rag_stream

router = APIRouter(prefix="/chat", tags=["chat"])


@router.post("", response_model=ChatResponse)
async def chat(
    body: ChatRequest,
    db: AsyncSession = Depends(get_job_db),
):
    """RAG 채팅 (동기 응답)"""
    answer, sources = await chat_rag(
        message  = body.message,
        history  = body.history,
        job_db   = db,
        user_id  = body.user_id,
    )
    return ChatResponse(answer=answer, sources=sources)


@router.post("/stream")
async def chat_stream(
    body: ChatRequest,
    db: AsyncSession = Depends(get_job_db),
):
    """RAG 스트리밍 채팅 (SSE text/event-stream)"""
    async def generator():
        async for chunk in chat_rag_stream(body.message, body.history, db):
            yield f"data: {chunk}\n\n"
        yield "data: [DONE]\n\n"

    return StreamingResponse(generator(), media_type="text/event-stream")
