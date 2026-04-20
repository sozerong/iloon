"""RAG 챗봇 서비스 (공고 기반 질의응답)"""

import json
import logging
from typing import AsyncGenerator, List, Tuple
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, or_

from ..models.job import Job
from .ollama_client import get_ollama

logger = logging.getLogger(__name__)

RAG_SYSTEM = """당신은 채용 플랫폼 '일로온'의 AI 어시스턴트입니다.
주어진 채용 공고 정보를 바탕으로 사용자의 질문에 친절하고 정확하게 답변하세요.
공고에 없는 내용은 "해당 공고에 명시된 내용이 없습니다"라고 답하세요.
항상 한국어로 답변하세요."""


async def _retrieve(keyword: str, job_db: AsyncSession, top_k: int = 3) -> List[Job]:
    """키워드 기반 공고 검색 (간단한 keyword search)"""
    words = keyword.split()[:5]
    conditions = []
    for w in words:
        conditions.append(Job.title.ilike(f"%{w}%"))
        conditions.append(Job.description.ilike(f"%{w}%"))
        conditions.append(Job.job_type.ilike(f"%{w}%"))
        conditions.append(Job.company.ilike(f"%{w}%"))

    result = await job_db.execute(
        select(Job).where(or_(*conditions)).limit(top_k)
    )
    return result.scalars().all()


def _build_context(jobs: List[Job]) -> Tuple[str, List[str]]:
    """검색된 공고로 컨텍스트 문자열 구성"""
    parts  = []
    job_ids = []
    for i, job in enumerate(jobs, 1):
        part = (
            f"[공고 {i}]\n"
            f"ID: {job.id}\n"
            f"제목: {job.title}\n"
            f"회사: {job.company}\n"
            f"위치: {job.location}\n"
            f"직무: {job.job_type}\n"
            f"경력: {job.career_type}\n"
            f"학력: {job.education}\n"
            f"급여: {job.salary}\n"
            f"자격요건: {(job.requirements or '')[:600]}\n"
            f"우대사항: {(job.preferred or '')[:300]}\n"
            f"복리후생: {(job.benefits or '')[:300]}\n"
            f"채용절차: {job.process or ''}\n"
            f"마감일: {job.deadline}\n"
        )
        parts.append(part)
        job_ids.append(job.id)
    return "\n---\n".join(parts), job_ids


async def chat_rag(
    message: str,
    history: List[dict],
    job_db: AsyncSession,
    user_id: str = None,
) -> Tuple[str, List[str]]:
    """RAG 기반 채팅 (동기 반환)"""
    jobs = await _retrieve(message, job_db)
    context, source_ids = _build_context(jobs)

    system_content = RAG_SYSTEM
    if context:
        system_content += f"\n\n[참고 공고]\n{context}"

    messages = [{"role": "system", "content": system_content}]
    # 히스토리 (최근 6턴)
    for turn in history[-6:]:
        if turn.get("role") in ("user", "assistant"):
            messages.append({"role": turn["role"], "content": turn["content"]})
    messages.append({"role": "user", "content": message})

    ollama = get_ollama()
    answer = await ollama.chat(messages, temperature=0.4, max_tokens=1024)
    return answer, source_ids


async def chat_rag_stream(
    message: str,
    history: List[dict],
    job_db: AsyncSession,
) -> AsyncGenerator[str, None]:
    """스트리밍 RAG 챗봇"""
    jobs = await _retrieve(message, job_db)
    context, _ = _build_context(jobs)

    system_content = RAG_SYSTEM
    if context:
        system_content += f"\n\n[참고 공고]\n{context}"

    messages = [{"role": "system", "content": system_content}]
    for turn in history[-6:]:
        if turn.get("role") in ("user", "assistant"):
            messages.append({"role": turn["role"], "content": turn["content"]})
    messages.append({"role": "user", "content": message})

    ollama = get_ollama()
    async for chunk in ollama.chat_stream(messages, temperature=0.4, max_tokens=1024):
        yield chunk
