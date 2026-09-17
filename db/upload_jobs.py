"""
로컬 더미 공고 → CloudType PostgreSQL 업로드 스크립트

실행:
  set PG_HOST=svc.sel3.cloudtype.app
  set PG_PORT=32200
  set PG_USER=your_user
  set PG_PASSWORD=your_password
  python upload_jobs.py
"""

import hashlib
import json
import os
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.stdout.reconfigure(encoding="utf-8")

import psycopg2
from psycopg2.extras import execute_values

# ── 설정 ──────────────────────────────────────────────────────
PG_HOST     = os.environ.get("PG_HOST",     "svc.sel3.cloudtype.app")
PG_PORT     = int(os.environ.get("PG_PORT", "32200"))
PG_USER     = os.environ.get("PG_USER",     "root")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "root")
PG_DBNAME   = os.environ.get("PG_JOB_DB",  "root")

BASE_DIR    = Path(__file__).parent.parent
LOGS_DIR    = Path(os.environ.get("LOGS_DIR", BASE_DIR / "logs"))


# ── 유틸 ──────────────────────────────────────────────────────
def _stable_id(source: str, key: str) -> str:
    raw = f"{source}::{key}"
    return str(uuid.UUID(hashlib.md5(raw.encode()).hexdigest()))


def _str(val: Any) -> Optional[str]:
    if val is None:
        return None
    if isinstance(val, str):
        return val.strip() or None
    if isinstance(val, list):
        return ", ".join(str(v) for v in val if v)
    if isinstance(val, dict):
        return json.dumps(val, ensure_ascii=False)
    return str(val)


def _list_to_str(val: Any, sep: str = "\n- ") -> Optional[str]:
    if val is None:
        return None
    if isinstance(val, list):
        items = [str(v) for v in val if v]
        return ("- " + sep.join(items)) if items else None
    return _str(val)


# ── 정규화 ─────────────────────────────────────────────────────
def _normalize(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        company_raw  = raw.get("company") or {}
        company_name = _str(company_raw.get("name")) if isinstance(company_raw, dict) else _str(company_raw)

        pos = raw.get("position") or {}
        if isinstance(pos, dict):
            title       = _str(pos.get("title"))
            jc          = pos.get("job_category") or {}
            job_type    = _str(jc.get("mid") or jc.get("large") or jc.get("small")) if isinstance(jc, dict) else _str(jc)
            career      = pos.get("career") or {}
            career_type = _str(career.get("type")) if isinstance(career, dict) else _str(career)
            edu         = pos.get("education") or {}
            education   = _str(edu.get("type")) if isinstance(edu, dict) else _str(edu)
        else:
            title = _str(pos)
            job_type = career_type = education = None

        wc  = raw.get("work_condition") or {}
        if isinstance(wc, dict):
            loc = wc.get("location") or {}
            if isinstance(loc, dict):
                location = " ".join(p for p in [loc.get("sido"), loc.get("sigungu")] if p) or None
            else:
                location = _str(loc)
            sal = wc.get("salary") or {}
            if isinstance(sal, dict):
                sal_min, sal_max = sal.get("min"), sal.get("max")
                sal_type = sal.get("type", "")
                salary = f"{sal_type} {sal_min//10000}~{sal_max//10000}만원".strip() if sal_min and sal_max else _str(sal_type) or None
            else:
                salary = _str(sal)
        else:
            location = salary = None

        detail       = raw.get("detail") or {}
        description  = _list_to_str(detail.get("main_tasks"))  if isinstance(detail, dict) else _str(detail)
        requirements = _list_to_str(detail.get("requirements")) if isinstance(detail, dict) else None
        preferred    = _list_to_str(detail.get("preferred"))    if isinstance(detail, dict) else None
        benefits     = _list_to_str(detail.get("benefits"))     if isinstance(detail, dict) else None

        skills = raw.get("skills")
        if skills:
            skills_str = "기술스택: " + ", ".join(str(s) for s in skills if s)
            requirements = (requirements + "\n\n" + skills_str) if requirements else skills_str

        rec     = raw.get("recruitment_process") or {}
        steps   = rec.get("steps", []) if isinstance(rec, dict) else []
        process = " → ".join(s.get("name", "") for s in steps if isinstance(s, dict) and s.get("name")) or None

        dates    = raw.get("dates") or {}
        deadline = _str(dates.get("deadline") or dates.get("posted_at")) if isinstance(dates, dict) else None

        job_id = _str(raw.get("job_id") or raw.get("id")) or \
                 _stable_id("dummy", str(title) + str(company_name))

        stats      = raw.get("stats") or {}
        view_count = int(stats.get("view_count", 0)) if isinstance(stats, dict) else 0

        if not title:
            return None

        return {
            "id":           job_id,
            "title":        title,
            "company":      company_name,
            "location":     location,
            "job_type":     job_type,
            "occupation":   job_type,
            "career_type":  career_type,
            "education":    education,
            "salary":       salary,
            "description":  description,
            "requirements": requirements,
            "preferred":    preferred,
            "benefits":     benefits,
            "process":      process,
            "deadline":     deadline,
            "source":       "dummy",
            "url":          _str(raw.get("url")),
            "view_count":   view_count,
            "created_at":   datetime.utcnow(),
            "updated_at":   datetime.utcnow(),
        }
    except Exception as e:
        print(f"  [SKIP] 정규화 실패: {e}")
        return None


# ── JSONL 읽기 ─────────────────────────────────────────────────
def _process_raws(raws: List[Any], jobs: List, skipped_ref: List[int]) -> None:
    for raw in raws:
        job = _normalize(raw)
        if job:
            jobs.append(job)
        else:
            skipped_ref[0] += 1


def load_jobs(file: Optional[str] = None) -> List[Dict[str, Any]]:
    jobs, skipped = [], [0]

    if file:
        json_files = [Path(file)]
        if not json_files[0].exists():
            print(f"[ERROR] 파일 없음: {file}")
            sys.exit(1)
    else:
        # results/dummy_jobs_*_{오늘날짜}*.json → 오늘 날짜 파일 전부
        today = datetime.utcnow().strftime("%Y%m%d")
        results_dir = BASE_DIR / "results"
        json_files = sorted(results_dir.glob(f"dummy_jobs_*{today}*.json")) if results_dir.exists() else []

        if not json_files:
            print(f"[ERROR] 오늘({today}) 생성된 파일 없음: {results_dir}")
            sys.exit(1)

    for path in json_files:
        print(f"  읽는 중: {path.name}")
        try:
            with open(path, encoding="utf-8") as f:
                obj  = json.load(f)
            raws = obj.get("jobs", obj) if isinstance(obj, dict) else obj
            _process_raws(raws, jobs, skipped)
        except Exception as e:
            print(f"  [SKIP] {path.name} 로드 실패: {e}")
            skipped[0] += 1

    if not jobs:
        print(f"[ERROR] 업로드할 공고 없음")
        sys.exit(1)

    print(f"  정규화 완료: {len(jobs)}건 성공, {skipped[0]}건 스킵")
    return jobs


# ── DB 업로드 ──────────────────────────────────────────────────
def upload(jobs: List[Dict[str, Any]]) -> None:
    if not PG_USER or not PG_PASSWORD:
        print("[ERROR] PG_USER, PG_PASSWORD 환경변수를 설정하세요.")
        sys.exit(1)

    print(f"\n  DB 연결 중: {PG_HOST}:{PG_PORT}/{PG_DBNAME}")
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        dbname=PG_DBNAME, user=PG_USER, password=PG_PASSWORD,
        connect_timeout=10,
    )
    cur = conn.cursor()

    cols = [
        "id", "title", "company", "location", "job_type", "occupation",
        "career_type", "education", "salary", "description", "requirements",
        "preferred", "benefits", "process", "deadline", "source", "url",
        "view_count", "created_at", "updated_at",
    ]
    rows = [tuple(j[c] for c in cols) for j in jobs]

    sql = f"""
        INSERT INTO jobs ({", ".join(cols)})
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            title        = EXCLUDED.title,
            company      = EXCLUDED.company,
            description  = EXCLUDED.description,
            requirements = EXCLUDED.requirements,
            updated_at   = EXCLUDED.updated_at
    """

    execute_values(cur, sql, rows)
    conn.commit()

    cur.close()
    conn.close()
    print(f"  업로드 완료: {len(jobs)}건 upsert")


# ── 메인 ──────────────────────────────────────────────────────
def main():
    import argparse
    parser = argparse.ArgumentParser(description="로컬 공고 JSON → CloudType DB 업로드")
    parser.add_argument("--file", default=None,
                        help="업로드할 JSON 파일 경로 (기본: results/dummy_jobs_*오늘날짜*.json)")
    args = parser.parse_args()

    print("=" * 50)
    print("  로컬 공고 → CloudType DB 업로드")
    print(f"  대상: {PG_HOST}:{PG_PORT}/{PG_DBNAME}")
    if args.file:
        print(f"  파일: {args.file}")
    print("=" * 50)

    print("\n[1] 파일 읽기...")
    jobs = load_jobs(file=args.file)

    if not jobs:
        print("[ERROR] 업로드할 공고 없음")
        sys.exit(1)

    print(f"\n[2] DB 업로드 ({len(jobs)}건)...")
    upload(jobs)

    print("\n완료!")


if __name__ == "__main__":
    main()
