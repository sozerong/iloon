"""
작업 3 — Airflow DAG 측정용 입력 데이터 생성

analyze_*.py 가 읽는 형식 그대로 만든다:
    logs/dummy-jobs-<카테고리>-<날짜>.jsonl   (한 줄에 원본 공고 dict 하나)

공고 생성기는 Anthropic API를 호출하므로, 작업 1과 같은 방식으로
results.zip 의 실제 생성 결과를 씨앗으로 복제한다. job_id는 전부 고유.

이후 사용자 이벤트는 저장소의 생성기를 그대로 쓴다:
    ANALYSIS_BASE_DIR=. python user_event_generator.py --users 300 --ai-ratio 0.4 --days 30

실행:
    python bench/make_dag_input.py --per-category 200
"""

from __future__ import annotations

import argparse
import copy
import glob
import json
import os
import sys
import uuid
from datetime import datetime
from typing import Any, Dict, List

sys.stdout.reconfigure(encoding="utf-8")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

from bench.make_docs import SEED_RESULT, _ensure_seed_extracted  # noqa: E402

LOGS_DIR = os.path.join(BASE_DIR, "logs")


def category_of(path: str) -> str:
    """dummy_jobs_<카테고리>_<타임스탬프>.json → 카테고리"""
    name = os.path.basename(path)[len("dummy_jobs_"):]
    return name.rsplit("_", 2)[0]


def expand(raw: Dict[str, Any], idx: int) -> Dict[str, Any]:
    """씨앗 공고 복제 — id/제목/회사명을 고유하게."""
    job = copy.deepcopy(raw)
    job["job_id"] = str(uuid.uuid4())
    pos = job.get("position")
    if isinstance(pos, dict) and pos.get("title"):
        pos["title"] = f"{pos['title']} #{idx}"
    comp = job.get("company")
    if isinstance(comp, dict) and comp.get("name"):
        comp["name"] = f"{comp['name']}-{idx % 97}"
        comp["id"] = str(uuid.uuid4())
    stats = job.get("stats")
    if isinstance(stats, dict):
        # 조회수에 변화를 줘야 인기도 예측(GBT)이 학습할 게 생긴다
        stats["view_count"]     = 50 + (idx * 7) % 4000
        stats["apply_count"]    = (idx * 3) % 300
        stats["bookmark_count"] = (idx * 5) % 400
    return job


def main() -> None:
    ap = argparse.ArgumentParser(description="Airflow DAG 입력 JSONL 생성")
    ap.add_argument("--per-category", type=int, default=200)
    ap.add_argument("--date", type=str, default=datetime.now().strftime("%Y-%m-%d"))
    args = ap.parse_args()

    _ensure_seed_extracted()
    os.makedirs(LOGS_DIR, exist_ok=True)

    files = sorted(glob.glob(os.path.join(SEED_RESULT, "*.json")))
    if not files:
        raise RuntimeError(f"씨앗 없음: {SEED_RESULT}")

    total = 0
    idx = 0
    for path in files:
        cat = category_of(path)
        with open(path, encoding="utf-8") as f:
            seeds: List[Dict[str, Any]] = json.load(f).get("jobs", [])
        if not seeds:
            continue

        out = os.path.join(LOGS_DIR, f"dummy-jobs-{cat}-{args.date}.jsonl")
        with open(out, "w", encoding="utf-8") as f:      # 덮어쓰기 — 반복 실행 시 누적 방지
            for i in range(args.per_category):
                f.write(json.dumps(expand(seeds[i % len(seeds)], idx), ensure_ascii=False) + "\n")
                idx += 1
        total += args.per_category
        print(f"  {cat:<10} {args.per_category}건 → {os.path.basename(out)}")

    print(f"\n공고 {total}건 생성 ({len(files)}개 카테고리) → {LOGS_DIR}")
    print("다음: ANALYSIS_BASE_DIR=. python user_event_generator.py --users 300 --ai-ratio 0.4 --days 30")


if __name__ == "__main__":
    main()
