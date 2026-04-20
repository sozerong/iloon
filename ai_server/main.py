"""일로온 AI 서버 — FastAPI 엔트리포인트"""

import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from .database import init_db
from .services.ollama_client import close_ollama
from .services.opensearch_service import ensure_index, ensure_ml_ready
from .routers import (
    survey_router,
    resume_router,
    recommendations_router,
    jobs_router,
    chat_router,
    importer_router,
)

# ── 로깅 설정 ─────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("ai_server")


# ── Lifespan ──────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 일로온 AI 서버 시작")

    # PostgreSQL 테이블 생성
    await init_db()
    logger.info("✅ PostgreSQL 테이블 초기화 완료")

    # OpenSearch ML 모델 등록/배포 (백그라운드 — 최초 실행 시 수 분 소요)
    import asyncio

    async def _setup_opensearch():
        try:
            model_id = await ensure_ml_ready()           # 모델 등록/배포
            await ensure_index(model_id=model_id)        # knn_vector 인덱스 생성
            logger.info("✅ OpenSearch Neural Search 준비 완료 (model=%s)", model_id)
        except Exception as e:
            logger.warning("⚠️  OpenSearch ML 준비 실패 (키워드 검색으로 동작): %s", e)
            try:
                await ensure_index()                     # 벡터 없는 기본 인덱스라도 생성
            except Exception:
                pass

    asyncio.create_task(_setup_opensearch())
    logger.info("🔄 OpenSearch ML 모델 준비 중 (백그라운드)...")

    yield

    await close_ollama()
    logger.info("👋 AI 서버 종료")


# ── App ───────────────────────────────────────────────────────
app = FastAPI(
    title       = "일로온 AI 서버",
    description = "이력서 분석 · 공고 추천 · RAG 챗봇",
    version     = "1.0.0",
    lifespan    = lifespan,
)

# CORS (개발용 전체 허용 — 운영 시 origins 제한)
app.add_middleware(
    CORSMiddleware,
    allow_origins     = ["*"],
    allow_credentials = True,
    allow_methods     = ["*"],
    allow_headers     = ["*"],
)


# ── 미들웨어: 요청 로깅 ───────────────────────────────────────
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start    = time.time()
    response = await call_next(request)
    elapsed  = (time.time() - start) * 1000
    logger.info(
        "%s %s → %d  (%.1fms)",
        request.method, request.url.path, response.status_code, elapsed,
    )
    return response


# ── 전역 예외 핸들러 ──────────────────────────────────────────
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error("Unhandled exception: %s %s — %s", request.method, request.url.path, exc)
    return JSONResponse(status_code=500, content={"detail": "서버 내부 오류가 발생했습니다."})


# ── 라우터 등록 ───────────────────────────────────────────────
app.include_router(survey_router,          prefix="/api/v1")
app.include_router(resume_router,          prefix="/api/v1")
app.include_router(recommendations_router, prefix="/api/v1")
app.include_router(jobs_router,            prefix="/api/v1")
app.include_router(chat_router,            prefix="/api/v1")
app.include_router(importer_router,        prefix="/api/v1")


# ── 헬스체크 ─────────────────────────────────────────────────
@app.get("/health", tags=["health"])
async def health():
    return {"status": "ok", "service": "일로온 AI 서버"}


@app.get("/", tags=["health"])
async def root():
    return {
        "service": "일로온 AI 서버",
        "docs":    "/docs",
        "health":  "/health",
    }
