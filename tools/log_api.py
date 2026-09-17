import asyncio
import json
import logging
import logging.handlers
import os
import sys
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, BackgroundTasks, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, field_validator, ValidationError

# ── 로깅 초기화 ───────────────────────────────────────────────
LOG_DIR = "C:/GitHub/new_git/money/applogs"
LOGS_DIR = "C:/GitHub/new_git/money/logs"
FLUSH_SIZE = 100        # 이벤트 N개 쌓이면 즉시 플러시
FLUSH_INTERVAL = 5.0    # 초 단위 주기적 플러시

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)


def setup_logger() -> logging.Logger:
    logger = logging.getLogger("log_api")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # 콘솔 핸들러
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(fmt)

    # 파일 핸들러 (날짜별 로테이션, 최대 7일 보관)
    file_handler = logging.handlers.TimedRotatingFileHandler(
        filename=os.path.join(LOG_DIR, "log_api.log"),
        when="midnight",
        backupCount=7,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(fmt)

    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
    return logger


logger = setup_logger()


# ── 인메모리 버퍼 ─────────────────────────────────────────────
buffer: list[dict] = []
buffer_lock = asyncio.Lock()
_flush_task: Optional[asyncio.Task] = None


# ── Pydantic 모델 ─────────────────────────────────────────────
class LogEvent(BaseModel):
    event_id: str
    timestamp: str
    session_id: str
    user_id: str
    event_type: str
    model_config = {"extra": "allow"}   # event_type마다 필드가 다르므로 추가 필드 허용

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, v: str) -> str:
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError(f"timestamp 형식이 올바르지 않습니다: '{v}' (ISO 8601 형식 필요)")
        return v


class BulkLogRequest(BaseModel):
    events: list[LogEvent]


# ── JSONL 파일 저장 (날짜별) ──────────────────────────────────
async def flush_to_disk(events: list[dict]) -> int:
    if not events:
        return 0

    by_date: dict[str, list[dict]] = defaultdict(list)
    for ev in events:
        date = ev["timestamp"][:10]
        by_date[date].append(ev)

    total = 0
    for date, rows in by_date.items():
        path = os.path.join(LOGS_DIR, f"user-events-{date}.jsonl")
        try:
            with open(path, "a", encoding="utf-8") as f:
                for row in rows:
                    f.write(json.dumps(row, ensure_ascii=False) + "\n")
            total += len(rows)
            logger.debug("파일 저장: %s (%d건)", path, len(rows))
        except OSError as e:
            logger.error("파일 저장 실패 [%s]: %s", path, e)
            raise

    return total


# ── 주기적 플러시 백그라운드 태스크 ──────────────────────────
async def periodic_flush():
    logger.info("주기적 플러시 태스크 시작 (간격: %.1f초, 임계치: %d건)", FLUSH_INTERVAL, FLUSH_SIZE)
    while True:
        try:
            await asyncio.sleep(FLUSH_INTERVAL)
            async with buffer_lock:
                if not buffer:
                    continue
                to_flush = buffer.copy()
                buffer.clear()

            count = await flush_to_disk(to_flush)
            logger.info("[주기 플러시] %d건 저장 완료", count)

        except asyncio.CancelledError:
            logger.info("주기적 플러시 태스크 종료")
            # 종료 전 버퍼에 남은 데이터 저장
            async with buffer_lock:
                remaining = buffer.copy()
                buffer.clear()
            if remaining:
                try:
                    count = await flush_to_disk(remaining)
                    logger.info("[종료 플러시] 잔여 %d건 저장 완료", count)
                except Exception as e:
                    logger.error("[종료 플러시] 잔여 데이터 저장 실패: %s", e)
            raise

        except Exception as e:
            logger.exception("[주기 플러시] 예기치 못한 오류: %s", e)


# ── 앱 생명주기 ───────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    # startup
    global _flush_task
    logger.info("=== Log Ingest API 시작 ===")
    logger.info("로그 저장 경로: %s", LOGS_DIR)
    logger.info("앱 로그 경로:   %s", LOG_DIR)

    if not os.path.isdir(LOGS_DIR):
        logger.critical("로그 디렉토리가 존재하지 않습니다: %s", LOGS_DIR)
        raise RuntimeError(f"LOGS_DIR 없음: {LOGS_DIR}")

    _flush_task = asyncio.create_task(periodic_flush())

    yield

    # shutdown
    logger.info("=== Log Ingest API 종료 중 ===")
    if _flush_task and not _flush_task.done():
        _flush_task.cancel()
        try:
            await _flush_task
        except asyncio.CancelledError:
            pass
    logger.info("=== Log Ingest API 종료 완료 ===")


app = FastAPI(title="Log Ingest API", lifespan=lifespan)


# ── 전역 예외 핸들러 ──────────────────────────────────────────
@app.exception_handler(ValidationError)
async def validation_exception_handler(request: Request, exc: ValidationError):
    logger.warning("요청 유효성 검사 실패 [%s %s]: %s", request.method, request.url.path, exc)
    return JSONResponse(
        status_code=422,
        content={"detail": exc.errors(), "message": "요청 데이터 형식 오류"},
    )


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    logger.exception("처리되지 않은 예외 [%s %s]: %s", request.method, request.url.path, exc)
    return JSONResponse(
        status_code=500,
        content={"detail": "서버 내부 오류가 발생했습니다."},
    )


# ── 요청 로깅 미들웨어 ────────────────────────────────────────
@app.middleware("http")
async def log_requests(request: Request, call_next):
    logger.debug("→ %s %s", request.method, request.url.path)
    try:
        response = await call_next(request)
        logger.debug("← %s %s %d", request.method, request.url.path, response.status_code)
        return response
    except Exception as e:
        logger.error("미들웨어 오류 [%s %s]: %s", request.method, request.url.path, e)
        raise


# ── 엔드포인트 ────────────────────────────────────────────────
@app.post("/logs/bulk", status_code=202)
async def ingest_bulk(req: BulkLogRequest, background_tasks: BackgroundTasks):
    if not req.events:
        raise HTTPException(status_code=400, detail="events 배열이 비어 있습니다.")

    events = [ev.model_dump() for ev in req.events]
    logger.info("/logs/bulk 수신: %d건", len(events))

    try:
        async with buffer_lock:
            buffer.extend(events)
            current_size = len(buffer)

        if current_size >= FLUSH_SIZE:
            async with buffer_lock:
                to_flush = buffer.copy()
                buffer.clear()
            background_tasks.add_task(flush_to_disk, to_flush)
            logger.info("임계치 도달 → 즉시 플러시 예약: %d건", len(to_flush))
            return {"accepted": len(events), "flushed": len(to_flush), "buffered": 0}

        return {"accepted": len(events), "flushed": 0, "buffered": current_size}

    except Exception as e:
        logger.exception("ingest_bulk 처리 중 오류: %s", e)
        raise HTTPException(status_code=500, detail="로그 적재 중 오류가 발생했습니다.")


@app.post("/logs/flush", status_code=200)
async def manual_flush():
    """버퍼에 남은 이벤트를 즉시 디스크에 씁니다."""
    try:
        async with buffer_lock:
            if not buffer:
                logger.debug("/logs/flush 요청: 버퍼 비어있음")
                return {"flushed": 0}
            to_flush = buffer.copy()
            buffer.clear()

        count = await flush_to_disk(to_flush)
        logger.info("/logs/flush 완료: %d건", count)
        return {"flushed": count}

    except OSError as e:
        logger.error("수동 플러시 파일 저장 실패: %s", e)
        raise HTTPException(status_code=500, detail=f"파일 저장 실패: {e}")
    except Exception as e:
        logger.exception("수동 플러시 중 예기치 못한 오류: %s", e)
        raise HTTPException(status_code=500, detail="플러시 중 오류가 발생했습니다.")


@app.get("/health")
async def health():
    async with buffer_lock:
        buffered = len(buffer)
    log_dir_ok = os.path.isdir(LOGS_DIR)
    status = "ok" if log_dir_ok else "degraded"
    if status == "degraded":
        logger.warning("헬스체크: 로그 디렉토리 접근 불가 [%s]", LOGS_DIR)
    return {
        "status": status,
        "buffered": buffered,
        "logs_dir": LOGS_DIR,
        "logs_dir_ok": log_dir_ok,
    }
