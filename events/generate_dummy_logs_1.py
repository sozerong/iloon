import json
import uuid
import random
from datetime import datetime, timedelta

# ── 기초 데이터 ───────────────────────────────────────────────
REGIONS = [
    ("충청남도", "천안시 서북구 불당동"),
    ("충청남도", "천안시 서북구 백석동"),
    ("충청남도", "천안시 서북구 성성동"),
    ("충청남도", "천안시 서북구 두정동"),
    ("충청남도", "천안시 동남구 신방동"),
    ("충청남도", "천안시 동남구 청수동"),
    ("충청남도", "천안시 동남구 봉명동"),
    ("충청남도", "아산시 배방읍"),
    ("충청남도", "아산시 탕정면"),
    ("충청남도", "아산시 온천동"),
    ("충청남도", "공주시 신관동"),
    ("충청남도", "공주시 중동"),
    ("충청남도", "논산시 취암동"),
    ("충청남도", "당진시 읍내동"),
    ("충청남도", "서산시 동문동"),
]

DEVICES = ["mobile", "desktop", "tablet"]
DEVICE_WEIGHTS = [0.55, 0.38, 0.07]

UTM_SOURCES = ["google", "naver", "kakao", "direct", "instagram", "jobkorea"]
UTM_WEIGHTS = [0.35, 0.25, 0.15, 0.12, 0.08, 0.05]

JOB_IDS = [f"job_{str(i).zfill(3)}" for i in range(1, 31)]

KEYWORDS = [
    "백엔드 개발자", "프론트엔드 개발자", "데이터 엔지니어",
    "ML 엔지니어", "DevOps", "풀스택 개발자", "파이썬 개발자",
    "Java 개발자", "클라우드 엔지니어", "신입 개발자",
]

SKILLS = ["Python", "Java", "Docker", "Kubernetes", "FastAPI",
          "React", "Spring", "AWS", "Spark", "Elasticsearch"]

MODEL_VERSIONS = ["v1.0", "v1.1", "v1.2"]

CAREER_LEVELS = ["신입", "경력 1-3년", "경력 3-5년", "경력 5년 이상"]


# ── 헬퍼 함수 ─────────────────────────────────────────────────
def random_timestamp(base_date: datetime) -> str:
    offset_sec = random.randint(0, 86399)
    ts = base_date + timedelta(seconds=offset_sec)
    return ts.strftime("%Y-%m-%dT%H:%M:%S.") + f"{random.randint(0,999):03d}Z"


def common_meta(session_id: str, user_id: str, base_date: datetime) -> dict:
    region = random.choice(REGIONS)
    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": random_timestamp(base_date),
        "session_id": session_id,
        "user_id": user_id,
        "device_type": random.choices(DEVICES, DEVICE_WEIGHTS)[0],
        "region_sido": region[0],
        "region_detail": region[1],
        "utm_source": random.choices(UTM_SOURCES, UTM_WEIGHTS)[0],
    }


# ── 이벤트 생성 함수 ──────────────────────────────────────────
def make_search(session_id, user_id, base_date):
    return {
        **common_meta(session_id, user_id, base_date),
        "event_type": "search",
        "keyword": random.choice(KEYWORDS),
        "result_count": random.randint(0, 150),
        "filters": {
            "location": random.choice(["천안", "아산", "공주", "논산", "당진", "서산", None]),
            "career_level": random.choice(CAREER_LEVELS + [None]),
            "salary_min": random.choice([None, 3000, 4000, 5000]),
        },
    }


def make_recommendation_shown(session_id, user_id, base_date):
    job_pool = random.sample(JOB_IDS, k=random.randint(3, 6))
    recommended_jobs = [
        {
            "job_id": job_id,
            "match_score": round(random.uniform(0.60, 0.99), 2),
            "rank": idx + 1,
        }
        for idx, job_id in enumerate(job_pool)
    ]
    return {
        **common_meta(session_id, user_id, base_date),
        "event_type": "recommendation_shown",
        "recommended_jobs": recommended_jobs,
        "model_version": random.choices(MODEL_VERSIONS, [0.1, 0.3, 0.6])[0],
        "recommendation_basis": random.sample(SKILLS, k=random.randint(2, 4)),
    }


def make_job_detail_view(session_id, user_id, base_date, job_id, is_ai, rank, match_score):
    return {
        **common_meta(session_id, user_id, base_date),
        "event_type": "job_detail_view",
        "job_id": job_id,
        "is_ai_recommended": is_ai,
        "rank": rank,
        "match_score": match_score if is_ai else None,
        "time_on_page_sec": random.randint(5, 300),
    }


def make_bookmark(session_id, user_id, base_date, job_id, is_ai):
    return {
        **common_meta(session_id, user_id, base_date),
        "event_type": "bookmark",
        "job_id": job_id,
        "is_ai_recommended": is_ai,
    }


def make_apply_click(session_id, user_id, base_date, job_id, is_ai):
    return {
        **common_meta(session_id, user_id, base_date),
        "event_type": "apply_click",
        "job_id": job_id,
        "is_ai_recommended": is_ai,
        "apply_method": random.choices(["internal", "external"], [0.4, 0.6])[0],
    }


# ── 세션 시뮬레이션 ───────────────────────────────────────────
def simulate_session(base_date: datetime) -> list[dict]:
    """
    실제 유저 행동 패턴을 모사한 세션 하나 생성
    검색 → AI추천 노출 → 클릭 → (저장) → (지원) 흐름
    """
    session_id = f"sess_{uuid.uuid4().hex[:10]}"
    user_id = f"hash_{uuid.uuid4().hex[:8]}"
    events = []

    # 1. 검색 (80% 확률)
    if random.random() < 0.8:
        events.append(make_search(session_id, user_id, base_date))

    # 2. AI 추천 노출
    rec_event = make_recommendation_shown(session_id, user_id, base_date)
    events.append(rec_event)
    recommended = rec_event["recommended_jobs"]

    # 3. 추천 공고 클릭 (AI 추천 CTR: 약 40%)
    clicked_jobs = []
    for rec in recommended:
        # 순위 낮을수록 클릭 확률 감소
        click_prob = max(0.55 - rec["rank"] * 0.08, 0.1)
        if random.random() < click_prob:
            events.append(make_job_detail_view(
                session_id, user_id, base_date,
                rec["job_id"], True, rec["rank"], rec["match_score"]
            ))
            clicked_jobs.append(rec)

    # 4. 일반 공고 클릭 (AI 추천 아닌 것, 30% 확률)
    if random.random() < 0.3:
        normal_job = random.choice(JOB_IDS)
        events.append(make_job_detail_view(
            session_id, user_id, base_date,
            normal_job, False, random.randint(1, 10), None
        ))

    # 5. 저장 (클릭한 공고 중 30% 확률)
    for job in clicked_jobs:
        if random.random() < 0.3:
            events.append(make_bookmark(
                session_id, user_id, base_date, job["job_id"], True
            ))

            # 6. 지원 (저장한 공고 중 40% 확률)
            if random.random() < 0.4:
                events.append(make_apply_click(
                    session_id, user_id, base_date, job["job_id"], True
                ))

    return events


# ── 메인 생성 ─────────────────────────────────────────────────
def generate(days: int = 7, sessions_per_day: int = 200, output_dir: str = "."):
    import os
    os.makedirs(output_dir, exist_ok=True)

    total = 0
    for day_offset in range(days):
        base_date = datetime(2026, 4, 6) + timedelta(days=day_offset)
        date_str = base_date.strftime("%Y-%m-%d")
        filepath = f"{output_dir}/user-events-{date_str}.jsonl"

        all_events = []
        for _ in range(sessions_per_day):
            all_events.extend(simulate_session(base_date))

        # timestamp 기준 정렬
        all_events.sort(key=lambda e: e["timestamp"])

        with open(filepath, "w", encoding="utf-8") as f:
            for event in all_events:
                f.write(json.dumps(event, ensure_ascii=False) + "\n")

        total += len(all_events)
        print(f"  {date_str} — {len(all_events):,}건 → {filepath}")

    print(f"\n총 {total:,}건 생성 완료")


if __name__ == "__main__":
    print("더미 로그 생성 시작...\n")
    generate(days=7, sessions_per_day=200, output_dir="/home/claude/dummy_logs")
