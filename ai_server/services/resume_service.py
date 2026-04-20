"""이력서 분석 서비스"""

import uuid
import json
import logging
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from ..models.user import ResumeAnalysis, Survey
from .ollama_client import get_ollama

logger = logging.getLogger(__name__)

ANALYSIS_SYSTEM = """당신은 전문 채용 컨설턴트입니다. 주어진 이력서를 분석하여 반드시 아래 JSON 형식으로만 응답하세요.

{
  "score": 75,
  "analyzed_content": "이력서 전반적인 분석 요약 (2-3문단)",
  "feedback": "개선 포인트를 구체적으로 bullet point로 작성"
}

score는 0~100 사이 정수, JSON 이외 텍스트는 절대 포함하지 마세요."""


async def analyze_resume(
    user_id: str,
    original_text: str,
    db: AsyncSession,
) -> ResumeAnalysis:
    """이력서 분석 → DB 저장 후 반환"""

    # 유저 설문 컨텍스트 가져오기
    survey_ctx = ""
    result = await db.execute(select(Survey).where(Survey.user_id == user_id))
    survey = result.scalar_one_or_none()
    if survey:
        survey_ctx = (
            f"\n[지원자 정보]\n"
            f"- 직무: {survey.job_type}\n"
            f"- 지역: {survey.region}\n"
            f"- 직업군: {survey.occupation}\n"
            f"- 경력구분: {survey.career_type}\n"
            f"- 학력: {survey.education}\n"
        )

    messages = [
        {"role": "system", "content": ANALYSIS_SYSTEM},
        {
            "role": "user",
            "content": (
                f"{survey_ctx}\n[이력서]\n{original_text[:6000]}"
            ),
        },
    ]

    ollama = get_ollama()
    raw = await ollama.chat(messages, temperature=0.2, max_tokens=2048)

    # JSON 파싱
    score, analyzed_content, feedback = None, raw, ""
    try:
        start = raw.find("{")
        end   = raw.rfind("}") + 1
        if start != -1 and end > start:
            data = json.loads(raw[start:end])
            score            = float(data.get("score", 0))
            analyzed_content = data.get("analyzed_content", "")
            feedback         = data.get("feedback", "")
    except Exception as e:
        logger.warning("Resume JSON parse error: %s | raw=%s", e, raw[:200])

    analysis = ResumeAnalysis(
        id               = str(uuid.uuid4()),
        user_id          = user_id,
        original_text    = original_text,
        analyzed_content = analyzed_content,
        score            = score,
        feedback         = feedback,
    )
    db.add(analysis)
    await db.commit()
    await db.refresh(analysis)
    return analysis
