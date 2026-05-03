from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
import asyncpg
import logging

from .config import USER_DB_URL, JOB_DB_URL, PG_HOST, PG_PORT, PG_USER, PG_PASSWORD

logger = logging.getLogger(__name__)


# ── DB 없으면 자동 생성 ───────────────────────────────────────
async def _ensure_databases():
    """iloon_users / iloon_jobs DB가 없으면 생성"""
    # 기본 postgres DB에 연결해서 CREATE DATABASE 실행
    conn = await asyncpg.connect(
        host=PG_HOST,
        port=int(PG_PORT),
        user=PG_USER,
        password=PG_PASSWORD,
        database="postgres",  # 기본 DB로 연결
    )
    try:
        for db_name in ("iloon_users", "iloon_jobs"):
            exists = await conn.fetchval(
                "SELECT 1 FROM pg_database WHERE datname = $1", db_name
            )
            if not exists:
                # CREATE DATABASE는 트랜잭션 밖에서 실행해야 함
                await conn.execute(f'CREATE DATABASE "{db_name}"')
                logger.info("DB 생성: %s", db_name)
            else:
                logger.debug("DB 이미 존재: %s", db_name)
    finally:
        await conn.close()


# ── 엔진 (DB 생성 후 연결) ────────────────────────────────────
user_engine = create_async_engine(USER_DB_URL, echo=False, pool_pre_ping=True, pool_size=5)
UserSession = async_sessionmaker(user_engine, expire_on_commit=False)

job_engine  = create_async_engine(JOB_DB_URL,  echo=False, pool_pre_ping=True, pool_size=5)
JobSession  = async_sessionmaker(job_engine,  expire_on_commit=False)


class UserBase(DeclarativeBase):
    pass

class JobBase(DeclarativeBase):
    pass


# ── 의존성 주입 ───────────────────────────────────────────────
async def get_user_db():
    async with UserSession() as session:
        yield session

async def get_job_db():
    async with JobSession() as session:
        yield session


# ── 테이블 생성 ───────────────────────────────────────────────
async def init_db():
    from .models import user, job, log  # noqa

    # 1. DB 없으면 생성
    await _ensure_databases()

    # 2. 테이블 생성
    async with user_engine.begin() as conn:
        await conn.run_sync(UserBase.metadata.create_all)

    async with job_engine.begin() as conn:
        await conn.run_sync(JobBase.metadata.create_all)
