"""
추천 시스템 동작 확인 스크립트

실행:
  python test_recommendation.py
  python test_recommendation.py --user-id my-test-user
"""

import argparse
import json
import sys
import requests

# Windows 콘솔 UTF-8 출력
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

BASE = "http://localhost:8000/api/v1"

# ── 예시 설문 데이터 (실제 OpenSearch에 있는 공고 필드에 맞춰 작성) ─
SAMPLE_SURVEYS = {
    "backend": {
        "job_type":    "백엔드 개발",
        "occupation":  "백엔드/서버",
        "region":      "서울",
        "career_type": "경력",
        "education":   "대학교졸업",
    },
    "frontend": {
        "job_type":    "프론트엔드 개발",
        "occupation":  "프론트엔드",
        "region":      "서울",
        "career_type": "신입",
        "education":   "대학교졸업",
    },
    "ai": {
        "job_type":    "AI/ML 엔지니어",
        "occupation":  "AI/ML",
        "region":      "판교",
        "career_type": "경력",
        "education":   "대학원졸업",
    },
    "devops": {
        "job_type":    "DevOps 엔지니어",
        "occupation":  "인프라/DevOps",
        "region":      "서울",
        "career_type": "경력",
        "education":   "대학교졸업",
    },
}


def hline(char="─", n=60):
    print(char * n)


def step(msg):
    print(f"\n{'='*60}")
    print(f"  {msg}")
    print(f"{'='*60}")


def ok(msg):
    print(f"  ✅ {msg}")


def fail(msg):
    print(f"  ❌ {msg}")
    sys.exit(1)


def pretty(data):
    print(json.dumps(data, ensure_ascii=False, indent=2))


# ── 1. 서버 헬스 체크 ──────────────────────────────────────────
def check_health():
    step("서버 상태 확인")
    try:
        r = requests.get(f"{BASE.replace('/api/v1', '')}/health", timeout=5)
        r.raise_for_status()
        ok(f"AI 서버 정상 (status: {r.json().get('status', 'ok')})")
    except Exception as e:
        fail(f"서버 연결 실패: {e}\n  → docker-compose up -d 먼저 실행하세요.")


# ── 2. 설문 저장 ───────────────────────────────────────────────
def save_survey(user_id: str, survey_key: str) -> dict:
    step(f"설문 저장 (user_id={user_id}, 직무={survey_key})")
    body = SAMPLE_SURVEYS[survey_key]
    hline()
    print("  입력 설문:")
    for k, v in body.items():
        print(f"    {k}: {v}")
    hline()

    r = requests.post(f"{BASE}/survey/{user_id}", json=body, timeout=10)
    if r.status_code in (200, 201):
        data = r.json()
        ok(f"설문 저장 완료 (id={data['id']})")
        return data
    else:
        fail(f"설문 저장 실패: {r.status_code} {r.text}")


# ── 3. 일반 추천 생성 (벡터 기반) ─────────────────────────────
def run_general_recommend(user_id: str) -> list:
    step("설문 기반 공고 추천 (Neural Search)")
    print("  ⏳ 벡터 검색 중...")

    r = requests.post(
        f"{BASE}/recommendations/{user_id}/general",
        params={"top_k": 5},
        timeout=30,
    )
    if r.status_code == 200:
        recs = r.json()
        ok(f"추천 생성 완료 ({len(recs)}건)")
        return recs
    else:
        fail(f"추천 생성 실패: {r.status_code} {r.text}")


# ── 4. 추천 공고 상세 조회 ─────────────────────────────────────
def get_recommended_jobs(user_id: str) -> list:
    step("추천된 공고 상세 조회")
    r = requests.get(f"{BASE}/recommendations/{user_id}/general", timeout=10)
    if r.status_code == 200:
        return r.json()
    else:
        fail(f"공고 조회 실패: {r.status_code} {r.text}")


# ── 5. 결과 출력 ───────────────────────────────────────────────
def print_jobs(jobs: list):
    if not jobs:
        print("  ⚠️  추천된 공고가 없습니다.")
        return

    print(f"\n  총 {len(jobs)}개 공고 추천됨\n")
    hline()
    for i, job in enumerate(jobs, 1):
        print(f"  [{i}] {job.get('title', '(제목없음)')}")
        print(f"       회사    : {job.get('company', '-')}")
        print(f"       직무    : {job.get('job_type', '-')}")
        print(f"       지역    : {job.get('location', '-')}")
        print(f"       경력    : {job.get('career_type', '-')}")
        print(f"       급여    : {job.get('salary', '-')}")
        desc = (job.get('description') or '')[:80].replace('\n', ' ')
        print(f"       주요업무 : {desc}...")
        hline()


# ── main ──────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="추천 시스템 동작 확인")
    parser.add_argument("--user-id",    default="test-user-001",
                        help="테스트 유저 ID (기본: test-user-001)")
    parser.add_argument("--survey",     default="backend",
                        choices=list(SAMPLE_SURVEYS.keys()),
                        help="예시 설문 종류 (기본: backend)")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"  일로온 추천 시스템 동작 확인")
    print(f"  user_id : {args.user_id}")
    print(f"  설문 종류: {args.survey}")
    print(f"{'='*60}")

    check_health()
    save_survey(args.user_id, args.survey)
    run_general_recommend(args.user_id)
    jobs = get_recommended_jobs(args.user_id)
    print_jobs(jobs)

    print(f"\n{'='*60}")
    print("  ✅ 동작 확인 완료!")
    print(f"  user_id '{args.user_id}' 설문·추천 데이터가 DB에 저장됨")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
