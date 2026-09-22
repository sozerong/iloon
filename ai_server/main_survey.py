"""일로온 Survey 전용 경량 서버 — FastAPI 엔트리포인트

OpenSearch / Ollama 없이 PostgreSQL + survey 라우터만 동작합니다.
Cloudtype 배포용.
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from .database import init_db
from .routers.survey import router as survey_router

# ── 로깅 설정 ─────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("survey_server")


# ── Lifespan ──────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 일로온 Survey 서버 시작")
    await init_db()
    logger.info("✅ PostgreSQL 테이블 초기화 완료")
    yield
    logger.info("👋 Survey 서버 종료")


# ── App ───────────────────────────────────────────────────────
app = FastAPI(
    title       = "일로온 Survey 서버",
    description = "로그인 후 설문 데이터 수집 API",
    version     = "1.0.0",
    lifespan    = lifespan,
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins     = ["*"],
    allow_credentials = True,
    allow_methods     = ["*"],
    allow_headers     = ["*"],
)


# ── 전역 예외 핸들러 ──────────────────────────────────────────
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error("Unhandled exception: %s %s — %s", request.method, request.url.path, exc)
    return JSONResponse(status_code=500, content={"detail": "서버 내부 오류가 발생했습니다."})


# ── 라우터 등록 ───────────────────────────────────────────────
app.include_router(survey_router, prefix="/api/v1")


# ── 헬스체크 ─────────────────────────────────────────────────
@app.get("/health", tags=["health"])
async def health():
    return {"status": "ok", "service": "일로온 Survey 서버"}


@app.get("/", tags=["health"])
async def root():
    return {
        "service": "일로온 Survey 서버",
        "docs":    "/docs",
        "health":  "/health",
    }
