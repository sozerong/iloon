"""
CloudType PostgreSQL 테이블 초기화 스크립트 (최초 1회 실행)

실행:
  python create_tables.py
"""

import os
import sys

sys.stdout.reconfigure(encoding="utf-8")

import psycopg2

PG_HOST     = os.environ.get("PG_HOST",     "svc.sel3.cloudtype.app")
PG_PORT     = int(os.environ.get("PG_PORT", "32200"))
PG_USER     = os.environ.get("PG_USER",     "root")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "root")
PG_DBNAME   = os.environ.get("PG_DBNAME",   "root")


# ── DDL ───────────────────────────────────────────────────────

USERS_DDL = """
CREATE TABLE IF NOT EXISTS users (
    id         VARCHAR(36)  PRIMARY KEY,
    created_at TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS surveys (
    id           VARCHAR(36)  PRIMARY KEY,
    user_id      VARCHAR(36)  NOT NULL UNIQUE REFERENCES users(id),
    job_type     VARCHAR(100),
    region       VARCHAR(100),
    occupation   VARCHAR(100),
    career_type  VARCHAR(20),
    education    VARCHAR(50),
    university   VARCHAR(200),
    major        VARCHAR(100),
    career_years INTEGER,
    company_name VARCHAR(200),
    created_at   TIMESTAMP    NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS documents (
    id            VARCHAR(36)  PRIMARY KEY,
    user_id       VARCHAR(36)  NOT NULL REFERENCES users(id),
    type          VARCHAR(20)  NOT NULL DEFAULT 'resume',
    title         VARCHAR(200),
    original_text TEXT,
    ai_summary    TEXT,
    total_score   FLOAT,
    file_url      VARCHAR(500),
    created_at    TIMESTAMP    NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMP    NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_documents_user_id ON documents(user_id);

CREATE TABLE IF NOT EXISTS document_scores (
    id          VARCHAR(36)  PRIMARY KEY,
    document_id VARCHAR(36)  NOT NULL REFERENCES documents(id),
    category    VARCHAR(50)  NOT NULL,
    score       FLOAT        NOT NULL,
    max_score   FLOAT        NOT NULL DEFAULT 100,
    created_at  TIMESTAMP    NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_document_scores_document_id ON document_scores(document_id);

CREATE TABLE IF NOT EXISTS ai_feedbacks (
    id            VARCHAR(36)  PRIMARY KEY,
    document_id   VARCHAR(36)  NOT NULL REFERENCES documents(id),
    user_id       VARCHAR(36)  NOT NULL,
    feedback_type VARCHAR(20)  NOT NULL DEFAULT 'overall',
    section       VARCHAR(100),
    content       TEXT         NOT NULL,
    model_version VARCHAR(50),
    created_at    TIMESTAMP    NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_ai_feedbacks_document_id ON ai_feedbacks(document_id);
CREATE INDEX IF NOT EXISTS ix_ai_feedbacks_user_id     ON ai_feedbacks(user_id);

CREATE TABLE IF NOT EXISTS ai_recommendations (
    id          VARCHAR(36)  PRIMARY KEY,
    user_id     VARCHAR(36)  NOT NULL REFERENCES users(id),
    job_id      VARCHAR(50)  NOT NULL,
    match_score FLOAT,
    reason      TEXT,
    created_at  TIMESTAMP    NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_ai_recommendations_user_id ON ai_recommendations(user_id);

CREATE TABLE IF NOT EXISTS general_recommendations (
    id         VARCHAR(36)  PRIMARY KEY,
    user_id    VARCHAR(36)  NOT NULL REFERENCES users(id),
    job_id     VARCHAR(50)  NOT NULL,
    created_at TIMESTAMP    NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_general_recommendations_user_id ON general_recommendations(user_id);

CREATE TABLE IF NOT EXISTS user_activity_logs (
    id                VARCHAR(36)  PRIMARY KEY,
    user_id           VARCHAR(36)  NOT NULL,
    event_type        VARCHAR(50)  NOT NULL,
    job_id            VARCHAR(50),
    is_ai_recommended BOOLEAN,
    match_score       FLOAT,
    region_sido       VARCHAR(50),
    time_on_page_sec  INTEGER,
    session_id        VARCHAR(36),
    session_duration  INTEGER,
    target_type       VARCHAR(30),
    target_id         VARCHAR(50),
    meta              TEXT,
    created_at        TIMESTAMP    NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_ual_user_id    ON user_activity_logs(user_id);
CREATE INDEX IF NOT EXISTS ix_ual_event_type ON user_activity_logs(event_type);
CREATE INDEX IF NOT EXISTS ix_ual_created_at ON user_activity_logs(created_at);
"""

JOBS_DDL = """
CREATE TABLE IF NOT EXISTS jobs (
    id           VARCHAR(50)  PRIMARY KEY,
    title        VARCHAR(300),
    company      VARCHAR(200),
    location     VARCHAR(200),
    job_type     VARCHAR(100),
    occupation   VARCHAR(100),
    career_type  VARCHAR(50),
    education    VARCHAR(100),
    salary       VARCHAR(100),
    description  TEXT,
    requirements TEXT,
    preferred    TEXT,
    benefits     TEXT,
    process      TEXT,
    deadline     VARCHAR(100),
    source       VARCHAR(50),
    url          VARCHAR(500),
    embedding    TEXT,
    view_count   INTEGER      NOT NULL DEFAULT 0,
    created_at   TIMESTAMP    NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS job_relations (
    id               VARCHAR(36)  PRIMARY KEY,
    job_id           VARCHAR(50)  NOT NULL REFERENCES jobs(id),
    related_job_id   VARCHAR(50)  NOT NULL,
    similarity_score FLOAT        NOT NULL DEFAULT 0,
    created_at       TIMESTAMP    NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_job_relations_job_id ON job_relations(job_id);
"""


# ── 메인 ──────────────────────────────────────────────────────

def main():
    print("=" * 50)
    print("  CloudType PostgreSQL 테이블 초기화")
    print(f"  {PG_HOST}:{PG_PORT}/{PG_DBNAME}")
    print("=" * 50)

    print(f"\n  DB 연결 중...")
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        dbname=PG_DBNAME, user=PG_USER, password=PG_PASSWORD,
        connect_timeout=10,
    )
    cur = conn.cursor()

    print("\n[1] Users 테이블 생성...")
    cur.execute(USERS_DDL)

    print("[2] Jobs 테이블 생성...")
    cur.execute(JOBS_DDL)

    conn.commit()
    cur.close()
    conn.close()

    print("\n완료! 이제 upload_jobs.py 를 실행하세요.")


if __name__ == "__main__":
    main()
