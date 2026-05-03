"""문서(이력서/자소서/포트폴리오) 분석 서비스"""

import uuid
import json
import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from ..models.user import User, Document, DocumentScore, AIFeedback, Survey
from ..schemas.user import DocumentCreate
from .ollama_client import get_ollama

logger = logging.getLogger(__name__)

ANALYSIS_SYSTEM = """당신은 전문 채용 컨설턴트입니다. 주어진 문서를 분석하여 반드시 아래 JSON 형식으로만 응답하세요.

{
  "total_score": 75,
  "ai_summary": "문서 전반적인 분석 요약 (2-3문단)",
  "scores": [
    {"category": "맞춤법/문법", "score": 80, "max_score": 100},
    {"category": "구조/가독성", "score": 70, "max_score": 100},
    {"category": "직무적합도",  "score": 75, "max_score": 100},
    {"category": "경험/성과기술", "score": 65, "max_score": 100},
    {"category": "키워드",      "score": 85, "max_score": 100}
  ],
  "feedbacks": [
    {"feedback_type": "overall",  "section": null,     "content": "전체적인 피드백"},
    {"feedback_type": "section",  "section": "경력사항", "content": "경력 기술 개선 포인트"},
    {"feedback_type": "keyword",  "section": null,     "content": "추가하면 좋을 키워드"}
  ]
}

total_score는 0~100 사이 정수. JSON 이외 텍스트는 절대 포함하지 마세요."""


async def _get_or_create_user(user_id: str, db: AsyncSession) -> User:
    """users 테이블에 없으면 자동 생성 (FK constraint 방지)"""
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if not user:
        user = User(id=user_id)
        db.add(user)
        await db.commit()
        await db.refresh(user)
    return user


async def analyze_resume(
    user_id: str,
    body: DocumentCreate,
    db: AsyncSession,
) -> Document:
    """문서 분석 → Document + DocumentScore + AIFeedback 저장 후 반환"""

    # users 테이블에 없으면 자동 생성
    await _get_or_create_user(user_id, db)

    # 유저 설문 컨텍스트
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

    doc_type_label = {
        "resume":       "이력서",
        "cover_letter": "자기소개서",
        "portfolio":    "포트폴리오",
    }.get(body.type, "문서")

    messages = [
        {"role": "system", "content": ANALYSIS_SYSTEM},
        {
            "role": "user",
            "content": (
                f"{survey_ctx}\n[{doc_type_label}]\n{(body.original_text or '')[:6000]}"
            ),
        },
    ]

    ollama = get_ollama()
    raw = await ollama.chat(messages, temperature=0.2, max_tokens=2048)

    # JSON 파싱
    total_score, ai_summary, raw_scores, raw_feedbacks = None, raw, [], []
    try:
        start = raw.find("{")
        end   = raw.rfind("}") + 1
        if start != -1 and end > start:
            data           = json.loads(raw[start:end])
            total_score    = float(data.get("total_score", 0))
            ai_summary     = data.get("ai_summary", "")
            raw_scores     = data.get("scores", [])
            raw_feedbacks  = data.get("feedbacks", [])
    except Exception as e:
        logger.warning("Document JSON parse error: %s | raw=%s", e, raw[:200])

    # Document 저장
    doc = Document(
        id            = str(uuid.uuid4()),
        user_id       = user_id,
        type          = body.type,
        title         = body.title,
        original_text = body.original_text,
        ai_summary    = ai_summary,
        total_score   = total_score,
        file_url      = body.file_url,
    )
    db.add(doc)
    await db.flush()  # doc.id 확보

    # DocumentScore 저장
    for s in raw_scores:
        db.add(DocumentScore(
            id          = str(uuid.uuid4()),
            document_id = doc.id,
            category    = s.get("category", ""),
            score       = float(s.get("score", 0)),
            max_score   = float(s.get("max_score", 100)),
        ))

    # AIFeedback 저장
    for f in raw_feedbacks:
        db.add(AIFeedback(
            id            = str(uuid.uuid4()),
            document_id   = doc.id,
            user_id       = user_id,
            feedback_type = f.get("feedback_type", "overall"),
            section       = f.get("section"),
            content       = f.get("content", ""),
            model_version = getattr(ollama, "model", None),
        ))

    await db.commit()
    await db.refresh(doc)
    return doc
