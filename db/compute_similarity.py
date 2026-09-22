"""
공고 간 유사도 계산 + 유저 추천 계산 스크립트 (일회성 실행)

- jobs 테이블의 공고를 임베딩 → job_relations 테이블에 공고당 유사 공고 top-N 저장
- surveys 테이블의 유저 설문 → ai_recommendations 테이블에 유저당 top-M 저장

실행:
  pip install psycopg2-binary sentence-transformers numpy
  python compute_similarity.py
  python compute_similarity.py --top-similar 5 --top-rec 10
"""

import argparse
import hashlib
import json
import os
import sys
import uuid
from datetime import datetime

sys.stdout.reconfigure(encoding="utf-8")

import numpy as np
import psycopg2
from psycopg2.extras import execute_values
from sentence_transformers import SentenceTransformer

# ── 설정 ──────────────────────────────────────────────────────
PG_HOST     = os.environ.get("PG_HOST",     "svc.sel3.cloudtype.app")
PG_PORT     = int(os.environ.get("PG_PORT", "32200"))
PG_USER     = os.environ.get("PG_USER",     "root")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "root")
PG_DBNAME   = os.environ.get("PG_DBNAME",   "root")

MODEL_NAME  = os.environ.get("EMBED_MODEL", "snunlp/KR-SBERT-V40K-klueNLI-augSTS")  # 한국어 SBERT

TOP_SIMILAR = 5   # 공고당 유사 공고 수
TOP_REC     = 10  # 유저당 추천 공고 수


# ── DB 연결 ───────────────────────────────────────────────────
def connect():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        dbname=PG_DBNAME, user=PG_USER, password=PG_PASSWORD,
        connect_timeout=10,
    )


def _uuid(seed: str) -> str:
    return str(uuid.UUID(hashlib.md5(seed.encode()).hexdigest()))


# ── 공고 로드 ─────────────────────────────────────────────────
def load_jobs(cur):
    cur.execute("""
        SELECT id, title, company, job_type, occupation, career_type,
               education, description, requirements, location
        FROM jobs
    """)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    jobs = [dict(zip(cols, r)) for r in rows]
    print(f"  공고 로드: {len(jobs)}건")
    return jobs


def job_text(job: dict) -> str:
    """임베딩용 텍스트 합성"""
    parts = [
        job.get("title") or "",
        job.get("job_type") or "",
        job.get("occupation") or "",
        job.get("career_type") or "",
        job.get("education") or "",
        job.get("location") or "",
        (job.get("description") or "")[:300],
        (job.get("requirements") or "")[:200],
    ]
    return " ".join(p for p in parts if p).strip()


# ── 임베딩 계산 ───────────────────────────────────────────────
def compute_embeddings(model, jobs: list) -> np.ndarray:
    texts = [job_text(j) for j in jobs]
    print(f"  임베딩 계산 중... ({len(texts)}건)")
    embeddings = model.encode(texts, batch_size=32, show_progress_bar=True, normalize_embeddings=True)
    return embeddings  # shape: (N, dim), L2-normalized → dot product = cosine similarity


# ── job_relations 저장 ────────────────────────────────────────
def save_job_relations(cur, conn, jobs: list, embeddings: np.ndarray, top_k: int):
    print(f"\n  유사 공고 계산 중 (공고당 top-{top_k})...")

    # 코사인 유사도 행렬 (normalize_embeddings=True 이므로 dot product = cosine)
    sim_matrix = embeddings @ embeddings.T  # (N, N)

    rows = []
    now = datetime.utcnow()
    for i, job in enumerate(jobs):
        sims = sim_matrix[i]
        sims[i] = -1  # 자기 자신 제외
        top_indices = np.argsort(sims)[::-1][:top_k]
        for j in top_indices:
            score = float(sims[j])
            if score <= 0:
                continue
            rows.append((
                _uuid(f"rel::{job['id']}::{jobs[j]['id']}"),
                job["id"],
                jobs[j]["id"],
                round(score, 6),
                now,
            ))

    # 기존 데이터 삭제 후 재삽입
    cur.execute("DELETE FROM job_relations")
    execute_values(cur, """
        INSERT INTO job_relations (id, job_id, related_job_id, similarity_score, created_at)
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET similarity_score = EXCLUDED.similarity_score
    """, rows)
    conn.commit()
    print(f"  job_relations 저장 완료: {len(rows)}건")


# ── 유저 추천 저장 ────────────────────────────────────────────
def save_user_recommendations(cur, conn, jobs: list, embeddings: np.ndarray, top_k: int):
    print(f"\n  유저 추천 계산 중 (유저당 top-{top_k})...")

    # 유저 설문 로드
    cur.execute("""
        SELECT user_id, job_type, occupation, career_type, education, region
        FROM surveys
    """)
    surveys = cur.fetchall()
    survey_cols = [d[0] for d in cur.description]
    surveys = [dict(zip(survey_cols, r)) for r in surveys]
    print(f"  설문 유저: {len(surveys)}명")

    if not surveys:
        print("  설문 데이터 없음 → 추천 건너뜀")
        return

    # 유저 프로필 텍스트 → 임베딩
    user_texts = []
    for s in surveys:
        parts = [
            s.get("job_type") or "",
            s.get("occupation") or "",
            s.get("career_type") or "",
            s.get("education") or "",
            s.get("region") or "",
        ]
        user_texts.append(" ".join(p for p in parts if p).strip() or "IT 개발")

    print(f"  유저 임베딩 계산 중...")
    user_embeddings = model_ref.encode(
        user_texts, batch_size=32, show_progress_bar=False, normalize_embeddings=True
    )  # (M, dim)

    # 유저-공고 유사도
    scores_matrix = user_embeddings @ embeddings.T  # (M, N)

    rows = []
    now = datetime.utcnow()
    for idx, survey in enumerate(surveys):
        user_id = survey["user_id"]
        scores = scores_matrix[idx]
        top_indices = np.argsort(scores)[::-1][:top_k]
        for rank, j in enumerate(top_indices):
            rows.append((
                _uuid(f"rec::{user_id}::{jobs[j]['id']}"),
                user_id,
                jobs[j]["id"],
                round(float(scores[j]), 6),
                f"설문 기반 유사도 {round(float(scores[j])*100, 1)}%",
                now,
            ))

    # 기존 추천 삭제 후 재삽입
    cur.execute("DELETE FROM ai_recommendations")
    execute_values(cur, """
        INSERT INTO ai_recommendations (id, user_id, job_id, match_score, reason, created_at)
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET match_score = EXCLUDED.match_score, reason = EXCLUDED.reason
    """, rows)
    conn.commit()
    print(f"  ai_recommendations 저장 완료: {len(rows)}건")


# ── 임베딩 → jobs 테이블 업데이트 ─────────────────────────────
def save_embeddings_to_jobs(cur, conn, jobs: list, embeddings: np.ndarray):
    """jobs.embedding 컬럼에 벡터 저장 (나중에 재사용 가능)"""
    print("\n  임베딩 → jobs 테이블 저장 중...")
    rows = [(json.dumps(embeddings[i].tolist()), job["id"]) for i, job in enumerate(jobs)]
    cur.executemany("UPDATE jobs SET embedding = %s WHERE id = %s", rows)
    conn.commit()
    print(f"  임베딩 저장 완료: {len(rows)}건")


# ── 메인 ──────────────────────────────────────────────────────
model_ref = None  # 유저 임베딩에서 재사용

def main():
    global model_ref

    parser = argparse.ArgumentParser(description="공고 유사도 + 유저 추천 계산")
    parser.add_argument("--top-similar", default=TOP_SIMILAR, type=int, help=f"공고당 유사 공고 수 (기본: {TOP_SIMILAR})")
    parser.add_argument("--top-rec",     default=TOP_REC,     type=int, help=f"유저당 추천 공고 수 (기본: {TOP_REC})")
    parser.add_argument("--skip-embed-save", action="store_true", help="jobs 테이블에 임베딩 저장 생략")
    args = parser.parse_args()

    print("=" * 55)
    print("  공고 유사도 + 유저 추천 계산")
    print(f"  DB: {PG_HOST}:{PG_PORT}/{PG_DBNAME}")
    print(f"  유사 공고: top-{args.top_similar} | 유저 추천: top-{args.top_rec}")
    print("=" * 55)

    # 1. 모델 로드
    print(f"\n[1] 임베딩 모델 로드: {MODEL_NAME}")
    model_ref = SentenceTransformer(MODEL_NAME)

    # 2. DB 연결 + 공고 로드
    print("\n[2] DB 연결 + 공고 로드")
    conn = connect()
    cur  = conn.cursor()
    jobs = load_jobs(cur)

    if not jobs:
        print("[ERROR] 공고 없음 → 먼저 upload_jobs.py 실행하세요")
        sys.exit(1)

    # 3. 임베딩 계산
    print("\n[3] 임베딩 계산")
    embeddings = compute_embeddings(model_ref, jobs)

    # 4. jobs 테이블에 임베딩 저장
    if not args.skip_embed_save:
        print("\n[4] jobs 임베딩 저장")
        save_embeddings_to_jobs(cur, conn, jobs, embeddings)
    else:
        print("\n[4] jobs 임베딩 저장 생략")

    # 5. 유사 공고 계산 → job_relations 저장
    print("\n[5] 유사 공고 계산 → job_relations")
    save_job_relations(cur, conn, jobs, embeddings, top_k=args.top_similar)

    # 6. 유저 추천 계산 → ai_recommendations 저장
    print("\n[6] 유저 추천 계산 → ai_recommendations")
    save_user_recommendations(cur, conn, jobs, embeddings, top_k=args.top_rec)

    cur.close()
    conn.close()

    print("\n" + "=" * 55)
    print("  완료!")
    print("=" * 55)


if __name__ == "__main__":
    main()
