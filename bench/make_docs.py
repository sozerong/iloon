"""
벤치마크용 공고 문서 생성

dummy_job_generator.py / generate_all.py 는 Anthropic API를 호출해 5건씩 만든다.
5,000건을 API로 만드는 건 비용·시간이 과해서, 저장소에 이미 들어있는
results.zip 의 **실제 생성 결과**를 씨앗으로 복제한다.
(문서 크기·필드 구성은 실제 생성물과 동일. 내용은 반복됨 — BENCHMARK.md에 명시)

실행:
    python bench/make_docs.py --count 5000
    → bench/seed/jobs_5000.json
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import sys
import zipfile
from typing import Any, Dict, List

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

SEED_DIR    = os.path.join(BASE_DIR, "bench", "seed")
SEED_RESULT = os.path.join(SEED_DIR, "results")
RESULTS_ZIP = os.path.join(BASE_DIR, "results.zip")

from ai_server.services.job_importer import _normalize_dummy  # noqa: E402


def _ensure_seed_extracted() -> None:
    if os.path.isdir(SEED_RESULT) and glob.glob(os.path.join(SEED_RESULT, "*.json")):
        return
    if not os.path.isfile(RESULTS_ZIP):
        raise FileNotFoundError(f"씨앗 데이터 없음: {RESULTS_ZIP}")
    os.makedirs(SEED_DIR, exist_ok=True)
    with zipfile.ZipFile(RESULTS_ZIP) as z:
        z.extractall(SEED_DIR)


def load_seed_jobs() -> List[Dict[str, Any]]:
    """results.zip 의 생성 결과 → 정규화된 공고 문서 리스트"""
    _ensure_seed_extracted()
    seeds: List[Dict[str, Any]] = []
    for path in sorted(glob.glob(os.path.join(SEED_RESULT, "*.json"))):
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        for raw in data.get("jobs", []):
            job = _normalize_dummy(raw)
            if job and job.get("title"):
                seeds.append(job)
    if not seeds:
        raise RuntimeError("씨앗 공고를 하나도 정규화하지 못했습니다")
    return seeds


def build(count: int) -> List[Dict[str, Any]]:
    """씨앗을 복제해 count 건 생성. id는 전부 고유, 제목/회사에 일련번호 부여."""
    seeds = load_seed_jobs()
    docs: List[Dict[str, Any]] = []
    for i in range(count):
        base = seeds[i % len(seeds)]
        doc = dict(base)
        doc["id"]      = f"bench-{i:06d}"
        doc["title"]   = f"{base['title']} #{i}"
        doc["company"] = f"{base['company']}-{i % 997}"   # 카디널리티 확보용
        docs.append(doc)
    return docs


def main() -> None:
    ap = argparse.ArgumentParser(description="벤치마크용 공고 문서 생성")
    ap.add_argument("--count", type=int, default=5000, help="생성할 문서 수")
    args = ap.parse_args()

    docs = build(args.count)
    os.makedirs(SEED_DIR, exist_ok=True)
    out = os.path.join(SEED_DIR, f"jobs_{args.count}.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(docs, f, ensure_ascii=False, default=str)

    seeds = load_seed_jobs()
    size  = os.path.getsize(out)
    print(f"씨앗 공고      : {len(seeds)}건 (results.zip)")
    print(f"생성 문서      : {len(docs)}건")
    print(f"평균 문서 크기 : {size // len(docs)} bytes")
    print(f"저장           : {out} ({size/1e6:.1f} MB)")


if __name__ == "__main__":
    main()
