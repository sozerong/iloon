"""
사용자 행동 더미 이벤트 생성기

logs/dummy-jobs-*.jsonl 에서 공고 ID를 읽어
현실적인 사용자 행동 이벤트(조회→북마크→지원)를 생성합니다.

출력: logs/user-events-YYYY-MM-DD.jsonl
      → analyze_ai_vs_normal.py (Spark 분석)의 입력

이벤트 흐름:
  job_detail_view (100%)
    └─ bookmark     (AI 추천 20%, 일반 12%)
         └─ apply_click (북마크 중 AI 35%, 일반 20%)

실행:
  python user_event_generator.py
  python user_event_generator.py --users 500 --ai-ratio 0.4
"""

from __future__ import annotations

import argparse
import json
import logging
import logging.handlers
import os
import random
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

sys.stdout.reconfigure(encoding="utf-8")

# ── 경로 설정 ─────────────────────────────────────────────────
BASE_DIR   = Path(os.environ.get("ANALYSIS_BASE_DIR", "C:/GitHub/new_git/money"))
LOGS_DIR   = BASE_DIR / "logs"
APPLOG_DIR = BASE_DIR / "applogs"

for _d in (LOGS_DIR, APPLOG_DIR):
    _d.mkdir(parents=True, exist_ok=True)


# ── 로거 ─────────────────────────────────────────────────────
def setup_logger() -> logging.Logger:
    logger = logging.getLogger("user_event_generator")
    if logger.handlers:
        return logger
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    sh.setFormatter(fmt)
    fh = logging.handlers.TimedRotatingFileHandler(
        APPLOG_DIR / "user_event_generator.log",
        when="midnight", backupCount=7, encoding="utf-8",
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(sh)
    logger.addHandler(fh)
    return logger


logger = setup_logger()


# ── 공고 로드 ─────────────────────────────────────────────────
def load_jobs() -> List[Dict[str, Any]]:
    """logs/dummy-jobs-*.jsonl 에서 공고 메타 정보 수집"""
    jobs = []
    for fpath in sorted(LOGS_DIR.glob("dummy-jobs-*.jsonl")):
        try:
            with open(fpath, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    raw = json.loads(line)
                    raws = raw if isinstance(raw, list) else [raw]
                    for r in raws:
                        job_id = str(r.get("job_id", ""))
                        if not job_id or "-" not in job_id:
                            continue
                        # 위치 파싱
                        wc  = r.get("work_condition", {})
                        loc = wc.get("location", {}) if isinstance(wc, dict) else {}
                        sido = loc.get("sido", "충청남도") if isinstance(loc, dict) else "충청남도"
                        # 직군 파싱
                        pos = r.get("position", {})
                        jc  = pos.get("job_category", {}) if isinstance(pos, dict) else {}
                        category = jc.get("mid", "기타") if isinstance(jc, dict) else "기타"
                        jobs.append({
                            "job_id":   job_id,
                            "region":   sido or "충청남도",
                            "category": category or "기타",
                        })
        except Exception as e:
            logger.warning("JSONL 읽기 실패 (%s): %s", fpath.name, e)

    logger.info("공고 %d개 로드 완료", len(jobs))
    return jobs


# ── 이벤트 생성 ───────────────────────────────────────────────
def generate_events(
    jobs:      List[Dict[str, Any]],
    user_count: int   = 300,
    ai_ratio:  float  = 0.4,
    days_back: int    = 30,
) -> List[Dict[str, Any]]:
    """
    공고 목록 기반 사용자 행동 이벤트 생성

    전환율 (현실적 수준):
      - 조회 → 북마크: AI 추천 20%, 일반 12%
      - 북마크 → 지원: AI 추천 35%, 일반 20%
    """
    if not jobs:
        return []

    # 가상 사용자 풀
    user_ids = [str(uuid.uuid4()) for _ in range(user_count)]

    # AI 추천 공고 지정
    ai_count = max(1, int(len(jobs) * ai_ratio))
    ai_job_ids = {j["job_id"] for j in random.sample(jobs, k=min(ai_count, len(jobs)))}

    events: List[Dict[str, Any]] = []
    now = datetime.now()

    for job in jobs:
        job_id  = job["job_id"]
        region  = job["region"]
        is_ai   = job_id in ai_job_ids
        match_score = round(random.uniform(0.65, 0.98), 2) if is_ai else None

        # 공고별 조회 수 (AI 추천 공고가 조금 더 많이 조회됨)
        view_count = random.randint(20, 250) if is_ai else random.randint(5, 150)

        for _ in range(view_count):
            user_id = random.choice(user_ids)
            ts = now - timedelta(
                days=random.randint(0, days_back - 1),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59),
            )
            ts_str = ts.strftime("%Y-%m-%dT%H:%M:%S")
            session_sec = random.randint(10, 720)

            # 이 조회 1회를 식별하는 키. 아래 북마크·지원이 같은 값을 물고 나간다.
            # 이게 없으면 "어느 조회가 지원으로 이어졌는가"를 복원할 수 없다 —
            # (user_id, job_id) 로 조인하면 같은 사용자가 같은 공고를 여러 번 볼 때
            # 팬아웃이 생겨 전환율이 부풀려진다.
            view_id = str(uuid.uuid4())

            # ① 조회
            events.append({
                "event_id":        str(uuid.uuid4()),
                "view_id":         view_id,
                "event_type":      "job_detail_view",
                "user_id":         user_id,
                "job_id":          job_id,
                "is_ai_recommended": is_ai,
                "match_score":     match_score,
                "region":          region,
                "category":        job["category"],
                "session_duration": session_sec,
                "timestamp":       ts_str,
            })

            # ② 북마크 (조회 후 일정 확률)
            bookmark_p = 0.20 if is_ai else 0.12
            if random.random() < bookmark_p:
                events.append({
                    "event_id":        str(uuid.uuid4()),
                    "view_id":         view_id,          # 이 북마크를 유발한 조회
                    "event_type":      "bookmark",
                    "user_id":         user_id,
                    "job_id":          job_id,
                    "is_ai_recommended": is_ai,
                    "match_score":     match_score,
                    "region":          region,
                    "category":        job["category"],
                    "session_duration": None,
                    "timestamp":       ts_str,
                })

                # ③ 지원 (북마크 후 일정 확률)
                apply_p = 0.35 if is_ai else 0.20
                if random.random() < apply_p:
                    events.append({
                        "event_id":        str(uuid.uuid4()),
                        "view_id":         view_id,      # 이 지원을 유발한 조회
                        "event_type":      "apply_click",
                        "user_id":         user_id,
                        "job_id":          job_id,
                        "is_ai_recommended": is_ai,
                        "match_score":     match_score,
                        "region":          region,
                        "category":        job["category"],
                        "session_duration": None,
                        "timestamp":       ts_str,
                    })

    random.shuffle(events)
    logger.info(
        "이벤트 생성 완료 | 총 %d건 | AI 추천 공고 %d개 / 일반 공고 %d개",
        len(events), len(ai_job_ids), len(jobs) - len(ai_job_ids),
    )
    return events


# ── 저장 ─────────────────────────────────────────────────────
def save_events(events: List[Dict[str, Any]]) -> Path:
    today    = datetime.now().strftime("%Y-%m-%d")
    out_path = LOGS_DIR / f"user-events-{today}.jsonl"

    # "a" 모드로 누적 저장 (2시간마다 실행 시 하루치 이벤트가 쌓임)
    with open(out_path, "a", encoding="utf-8") as f:
        for ev in events:
            f.write(json.dumps(ev, ensure_ascii=False) + "\n")

    # 현재 파일 총 누적 건수 확인
    total = sum(1 for _ in open(out_path, encoding="utf-8"))
    logger.info("누적 저장: %s | 이번 +%d건 | 오늘 누계 %d건", out_path, len(events), total)
    return out_path


# ── Kafka 발행 ────────────────────────────────────────────────
def publish_to_kafka(events, bootstrap_servers):
    """kafka-python 라이브러리로 Kafka에 이벤트 발행"""
    try:
        from kafka import KafkaProducer
    except ImportError:
        logger.warning("kafka-python 미설치 — pip install kafka-python 실행 필요")
        return 0

    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        )
        for ev in events:
            producer.send("user-events", value=ev)
        producer.flush()
        producer.close()
        logger.info("Kafka 발행 완료: %d건 → user-events", len(events))
        return len(events)
    except Exception as e:
        logger.warning("Kafka 발행 실패 (파일 저장은 정상): %s", e)
        return 0


# ── 메인 ─────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="사용자 행동 더미 이벤트 생성")
    parser.add_argument("--users",    default=300, type=int,
                        help="가상 사용자 수 (기본: 300)")
    parser.add_argument("--ai-ratio", default=0.4, type=float,
                        help="AI 추천 공고 비율 0~1 (기본: 0.4)")
    parser.add_argument("--days",     default=30, type=int,
                        help="이벤트 분산 기간 (기본: 30일)")
    parser.add_argument("--kafka", action="store_true",
                        help="Kafka user-events 토픽에도 발행 (기본: 비활성)")
    parser.add_argument("--kafka-servers", default=None,
                        help="Kafka bootstrap servers (기본: 환경변수 KAFKA_BOOTSTRAP_SERVERS 또는 localhost:9094)")
    args = parser.parse_args()

    logger.info("========== 사용자 이벤트 생성 시작 ==========")
    logger.info("users=%d | ai_ratio=%.2f | days=%d", args.users, args.ai_ratio, args.days)

    jobs = load_jobs()
    if not jobs:
        logger.critical("공고 데이터 없음 — dummy 생성기를 먼저 실행하세요")
        sys.exit(1)

    events = generate_events(jobs, user_count=args.users, ai_ratio=args.ai_ratio, days_back=args.days)
    if not events:
        logger.critical("이벤트 생성 실패")
        sys.exit(1)

    path = save_events(events)

    # Kafka 발행 (--kafka 플래그가 있을 때)
    kafka_sent = 0
    if args.kafka:
        bootstrap_servers = (
            args.kafka_servers
            or os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
            or "localhost:9094"
        )
        kafka_sent = publish_to_kafka(events, bootstrap_servers)

    # 간단 요약
    view_cnt     = sum(1 for e in events if e["event_type"] == "job_detail_view")
    bookmark_cnt = sum(1 for e in events if e["event_type"] == "bookmark")
    apply_cnt    = sum(1 for e in events if e["event_type"] == "apply_click")
    total_in_file = sum(1 for _ in open(path, encoding="utf-8"))
    print(f"\n─── 생성 요약 ───")
    print(f"  공고 수        : {len(jobs)}개")
    print(f"  사용자 수      : {args.users}명")
    print(f"  이번 생성      : {len(events):,}건")
    print(f"    job_detail_view: {view_cnt:,}건")
    print(f"    bookmark       : {bookmark_cnt:,}건 ({bookmark_cnt/view_cnt*100:.1f}%)")
    print(f"    apply_click    : {apply_cnt:,}건 ({apply_cnt/view_cnt*100:.1f}%)")
    print(f"  오늘 누계      : {total_in_file:,}건")
    print(f"  저장 위치      : {path}")
    if args.kafka:
        print(f"  Kafka 발행     : {kafka_sent:,}건")

    logger.info("========== 완료 | 총 %d건 이벤트 생성 ==========", len(events))


if __name__ == "__main__":
    main()
