"""
PostgreSQL 저장 + JSONL 백업
- companies   : 회사 마스터
- job_postings: 채용 공고 (company_id FK)
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path

import psycopg2
import psycopg2.extras

logger = logging.getLogger("job_scraper.storage")

# ── 연결 설정 ─────────────────────────────────────────────────
_DSN = {
    "host":     os.environ.get("PG_HOST",     "localhost"),
    "port":     int(os.environ.get("PG_PORT", "5432")),
    "dbname":   os.environ.get("PG_JOB_DB",   "iloon_jobs"),
    "user":     os.environ.get("PG_USER",     "airflow"),
    "password": os.environ.get("PG_PASSWORD", "airflow"),
}

JSONL_PATH = Path(os.environ.get("JOBS_JSONL_DIR", "logs")) / "job-postings.jsonl"


def _conn():
    return psycopg2.connect(**_DSN)


# ── 초기화 ────────────────────────────────────────────────────
def init_db() -> None:
    with _conn() as conn, conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS companies (
                id            SERIAL PRIMARY KEY,
                company_name  VARCHAR(100) NOT NULL UNIQUE,
                base_url      VARCHAR(500) NOT NULL,
                registered_at TIMESTAMPTZ DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS job_postings (
                id                  SERIAL PRIMARY KEY,
                company_id          INTEGER NOT NULL REFERENCES companies(id),
                company_name        VARCHAR(100),
                company_url         VARCHAR(500),
                job_title           VARCHAR(500),
                categories          VARCHAR(500),
                employment_type     VARCHAR(50),
                employment_deadline TIMESTAMPTZ,
                region              VARCHAR(255),
                required_skills     VARCHAR(500),
                experience          VARCHAR(100),
                education           VARCHAR(100),
                job_description     TEXT,
                requirements        TEXT,
                scraped_at          TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(company_id, company_url)
            );

            CREATE INDEX IF NOT EXISTS idx_jp_company_id ON job_postings(company_id);
            CREATE INDEX IF NOT EXISTS idx_jp_categories ON job_postings(categories);
            CREATE INDEX IF NOT EXISTS idx_jp_scraped_at ON job_postings(scraped_at);
        """)
        conn.commit()
    logger.info("DB 초기화 완료 (host=%s, db=%s)", _DSN["host"], _DSN["dbname"])


# ── companies ─────────────────────────────────────────────────
def upsert_company(company_id: int, name: str, base_url: str) -> None:
    with _conn() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO companies (id, company_name, base_url)
            VALUES (%s, %s, %s)
            ON CONFLICT (company_name) DO UPDATE
                SET base_url = EXCLUDED.base_url
        """, (company_id, name, base_url))
        conn.commit()


# ── job_postings ──────────────────────────────────────────────
def save_job(job: dict) -> bool:
    """저장 성공 True, URL 중복이면 False"""
    try:
        with _conn() as conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO job_postings (
                    company_id, company_name, company_url, job_title,
                    categories, employment_type, employment_deadline,
                    region, required_skills, experience, education,
                    job_description, requirements, scraped_at
                ) VALUES (
                    %(company_id)s, %(company_name)s, %(company_url)s, %(job_title)s,
                    %(categories)s, %(employment_type)s, %(employment_deadline)s,
                    %(region)s, %(required_skills)s, %(experience)s, %(education)s,
                    %(job_description)s, %(requirements)s, %(scraped_at)s
                )
                ON CONFLICT (company_id, company_url) DO NOTHING
            """, {
                "company_id":          job.get("company_id"),
                "company_name":        job.get("company_name"),
                "company_url":         job.get("company_url"),
                "job_title":           job.get("job_title"),
                "categories":          job.get("categories"),
                "employment_type":     job.get("employment_type"),
                "employment_deadline": job.get("employment_deadline"),
                "region":              job.get("region"),
                "required_skills":     job.get("required_skills"),
                "experience":          job.get("experience"),
                "education":           job.get("education"),
                "job_description":     job.get("job_description"),
                "requirements":        job.get("requirements"),
                "scraped_at":          datetime.now().isoformat(),
            })
            inserted = cur.rowcount
            conn.commit()

        if inserted:
            _append_jsonl(job)
            return True

        logger.debug("중복 URL 스킵: %s", job.get("company_url"))
        return False

    except Exception as e:
        logger.error("저장 실패: %s | %s", job.get("company_url"), e)
        raise


def _append_jsonl(job: dict) -> None:
    JSONL_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(JSONL_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(job, ensure_ascii=False, default=str) + "\n")


# ── 조회 ─────────────────────────────────────────────────────
def get_stats() -> dict:
    with _conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT COUNT(*) AS total FROM job_postings")
        total = cur.fetchone()["total"]
        cur.execute("""
            SELECT company_name, COUNT(*) AS cnt
            FROM job_postings
            GROUP BY company_name
            ORDER BY cnt DESC
        """)
        by_company = [dict(r) for r in cur.fetchall()]
    return {"total": total, "by_company": by_company}
