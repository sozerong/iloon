"""
더미 유저 설문 데이터 생성 및 DB 저장 스크립트

흐름:
  1. 직무별 다양한 설문 프로필 정의
  2. POST /survey/{user_id} 로 설문 저장
  3. POST /recommendations/{user_id}/general 로 추천 생성
  4. 결과 JSONL 로 저장

실행:
  python dummy_survey_generator.py
  python dummy_survey_generator.py --count 20
  python dummy_survey_generator.py --no-recommend   # 추천 생성 생략
"""

import argparse
import json
import logging
import logging.handlers
import random
import sys
import uuid
from datetime import datetime
from pathlib import Path

import requests

sys.stdout.reconfigure(encoding="utf-8")

# ── 경로 설정 ─────────────────────────────────────────────────
BASE_DIR    = Path("C:/GitHub/new_git/money")
APPLOG_DIR  = BASE_DIR / "applogs"
RESULT_DIR  = BASE_DIR / "results"

for d in (APPLOG_DIR, RESULT_DIR):
    d.mkdir(parents=True, exist_ok=True)

BASE_URL = "http://localhost:8000/api/v1"


# ── 로거 ─────────────────────────────────────────────────────
def setup_logger() -> logging.Logger:
    logger = logging.getLogger("dummy_survey_generator")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    sh.setFormatter(fmt)

    fh = logging.handlers.TimedRotatingFileHandler(
        APPLOG_DIR / "dummy_survey_generator.log",
        when="midnight",
        backupCount=7,
        encoding="utf-8",
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    logger.addHandler(sh)
    logger.addHandler(fh)
    return logger


logger = setup_logger()


# ── 설문 프로필 풀 ────────────────────────────────────────────
# 각 항목: job_type(원하는 직무), region(근무지역), occupation(직업), career_type(신입/경력), education(학력)

SURVEY_PROFILES = [
    # ── 백엔드 ──────────────────────────────────────────────
    {
        "job_type":    "백엔드 개발",
        "region":      "천안시 서북구 불당동",
        "occupation":  "백엔드/서버",
        "career_type": "경력",
        "education":   "대학교졸업",
    },
    {
        "job_type":    "백엔드 개발",
        "region":      "천안시 서북구 백석동",
        "occupation":  "백엔드/서버",
        "career_type": "신입",
        "education":   "대학교졸업",
    },
    {
        "job_type":    "Java 백엔드 개발",
        "region":      "천안시 동남구 신방동",
        "occupation":  "백엔드/서버",
        "career_type": "경력",
        "education":   "대학교졸업",
    },
    {
        "job_type":    "Python 백엔드 개발",
        "region":      "아산시 배방읍",
        "occupation":  "백엔드/서버",
        "career_type": "경력",
        "education":   "대학원졸업",
    },

    # ── 프론트엔드 ───────────────────────────────────────────
    {
        "job_type":    "프론트엔드 개발",
        "region":      "천안시 서북구 성성동",
        "occupation":  "프론트엔드",
        "career_type": "신입",
        "education":   "대학교졸업",
    },
    {
        "job_type":    "React 프론트엔드 개발",
        "region":      "천안시 동남구 청수동",
        "occupation":  "프론트엔드",
        "career_type": "경력",
        "education":   "대학교졸업",
    },
    {
        "job_type":    "Vue.js 프론트엔드 개발",
        "region":      "천안시 동남구 봉명동",
        "occupation":  "프론트엔드",
        "career_type": "경력",
        "education":   "전문대졸업",
    },

    # ── AI/ML ────────────────────────────────────────────────
    {
        "job_type":    "AI/ML 엔지니어",
        "region":      "아산시 탕정면",
        "occupation":  "AI/ML",
        "career_type": "경력",
        "education":   "대학원졸업",
    },
    {
        "job_type":    "머신러닝 엔지니어",
        "region":      "아산시 온천동",
        "occupation":  "AI/ML",
        "career_type": "경력",
        "education":   "대학원졸업",
    },
    {
        "job_type":    "데이터 사이언티스트",
        "region":      "천안시 서북구 두정동",
        "occupation":  "데이터분석",
        "career_type": "경력",
        "education":   "대학원졸업",
    },

    # ── 데이터 ───────────────────────────────────────────────
    {
        "job_type":    "데이터 엔지니어",
        "region":      "천안시 서북구 불당동",
        "occupation":  "데이터엔지니어링",
        "career_type": "경력",
        "education":   "대학교졸업",
    },
    {
        "job_type":    "데이터 분석가",
        "region":      "공주시 신관동",
        "occupation":  "데이터분석",
        "career_type": "신입",
        "education":   "대학교졸업",
    },

    # ── DevOps/인프라 ────────────────────────────────────────
    {
        "job_type":    "DevOps 엔지니어",
        "region":      "천안시 동남구 봉명동",
        "occupation":  "인프라/DevOps",
        "career_type": "경력",
        "education":   "대학교졸업",
    },
    {
        "job_type":    "클라우드 엔지니어",
        "region":      "아산시 배방읍",
        "occupation":  "인프라/DevOps",
        "career_type": "경력",
        "education":   "대학교졸업",
    },
    {
        "job_type":    "SRE 엔지니어",
        "region":      "천안시 서북구 백석동",
        "occupation":  "인프라/DevOps",
        "career_type": "경력",
        "education":   "대학원졸업",
    },

    # ── 모바일 ───────────────────────────────────────────────
    {
        "job_type":    "iOS 개발",
        "region":      "당진시 읍내동",
        "occupation":  "모바일",
        "career_type": "경력",
        "education":   "대학교졸업",
    },
    {
        "job_type":    "Android 개발",
        "region":      "논산시 취암동",
        "occupation":  "모바일",
        "career_type": "신입",
        "education":   "대학교졸업",
    },
    {
        "job_type":    "Flutter 개발",
        "region":      "서산시 동문동",
        "occupation":  "모바일",
        "career_type": "경력",
        "education":   "대학교졸업",
    },

    # ── 보안/QA/PM ───────────────────────────────────────────
    {
        "job_type":    "보안 엔지니어",
        "region":      "공주시 중동",
        "occupation":  "보안",
        "career_type": "경력",
        "education":   "대학교졸업",
    },
    {
        "job_type":    "QA 엔지니어",
        "region":      "천안시 동남구 신방동",
        "occupation":  "QA",
        "career_type": "신입",
        "education":   "대학교졸업",
    },
    {
        "job_type":    "프로덕트 매니저",
        "region":      "아산시 온천동",
        "occupation":  "PM/기획",
        "career_type": "경력",
        "education":   "대학교졸업",
    },
    {
        "job_type":    "게임 클라이언트 개발",
        "region":      "천안시 서북구 성성동",
        "occupation":  "게임개발",
        "career_type": "경력",
        "education":   "대학교졸업",
    },
]


# ── API 호출 ─────────────────────────────────────────────────

def save_survey(user_id: str, profile: dict) -> dict | None:
    """POST /survey/{user_id}"""
    try:
        r = requests.post(
            f"{BASE_URL}/survey/{user_id}",
            json=profile,
            timeout=10,
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.error("설문 저장 실패 user_id=%s: %s", user_id, e)
        return None


def run_recommend(user_id: str, top_k: int = 5) -> list:
    """POST /recommendations/{user_id}/general"""
    try:
        r = requests.post(
            f"{BASE_URL}/recommendations/{user_id}/general",
            params={"top_k": top_k},
            timeout=30,
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.error("추천 생성 실패 user_id=%s: %s", user_id, e)
        return []


def check_health() -> bool:
    try:
        r = requests.get(
            f"{BASE_URL.replace('/api/v1', '')}/health",
            timeout=5,
        )
        return r.status_code == 200
    except Exception:
        return False


# ── 메인 ─────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="더미 설문 데이터 생성 및 DB 저장")
    parser.add_argument("--count",        type=int, default=len(SURVEY_PROFILES),
                        help=f"생성할 유저 수 (기본: {len(SURVEY_PROFILES)}, 최대: {len(SURVEY_PROFILES)})")
    parser.add_argument("--no-recommend", action="store_true",
                        help="추천 생성 건너뜀 (설문 저장만)")
    parser.add_argument("--top-k",        type=int, default=5,
                        help="추천 공고 수 (기본: 5)")
    args = parser.parse_args()

    # 서버 상태 확인
    logger.info("서버 상태 확인 중...")
    if not check_health():
        logger.error("AI 서버에 연결할 수 없습니다. docker-compose up -d 를 먼저 실행하세요.")
        sys.exit(1)
    logger.info("서버 정상 확인")

    # 프로필 선택 (count만큼 랜덤 or 순서대로)
    count    = min(args.count, len(SURVEY_PROFILES))
    profiles = random.sample(SURVEY_PROFILES, count)

    logger.info("총 %d명의 더미 유저 설문 생성 시작", count)

    results   = []
    success   = 0
    fail      = 0
    today     = datetime.now().strftime("%Y-%m-%d")

    for i, profile in enumerate(profiles, 1):
        user_id = str(uuid.uuid4())
        logger.info("[%d/%d] user_id=%s | 직무=%s | 지역=%s | 경력=%s",
                    i, count, user_id, profile["job_type"], profile["region"], profile["career_type"])

        # 1. 설문 저장
        survey = save_survey(user_id, profile)
        if not survey:
            fail += 1
            continue

        rec_count = 0

        # 2. 추천 생성 (선택)
        if not args.no_recommend:
            recs = run_recommend(user_id, top_k=args.top_k)
            rec_count = len(recs)
            logger.info("  → 추천 %d건 생성 완료", rec_count)

        results.append({
            "user_id":     user_id,
            "survey_id":   survey.get("id"),
            "profile":     profile,
            "rec_count":   rec_count,
            "created_at":  today,
        })
        success += 1

    # 결과 저장
    ts          = datetime.now().strftime("%Y%m%d_%H%M%S")
    result_path = RESULT_DIR / f"dummy_surveys_{ts}.jsonl"
    with open(result_path, "w", encoding="utf-8") as f:
        for row in results:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    logger.info("=" * 50)
    logger.info("완료: 성공 %d건 / 실패 %d건", success, fail)
    logger.info("결과 저장: %s", result_path)
    logger.info("=" * 50)


if __name__ == "__main__":
    main()
