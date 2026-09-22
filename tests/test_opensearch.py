"""
OpenSearch 직접 확인 스크립트

실행:
  python test_opensearch.py
"""

import sys
import json
import requests

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

OS = "http://localhost:9200"
INDEX = "iloon_jobs"


def hline():
    print("─" * 60)


def title(msg):
    print(f"\n{'='*60}")
    print(f"  {msg}")
    print(f"{'='*60}")


def post(path, body):
    r = requests.post(f"{OS}{path}", json=body, timeout=15)
    r.raise_for_status()
    return r.json()


def get_model_id():
    data = post("/_plugins/_ml/models/_search", {
        "query": {"term": {"model_state": "DEPLOYED"}},
        "size": 1
    })
    hits = data.get("hits", {}).get("hits", [])
    if hits:
        return hits[0]["_id"]
    return None


def print_hits(data, fields=None):
    hits = data.get("hits", {}).get("hits", [])
    total = data.get("hits", {}).get("total", {}).get("value", 0)
    print(f"  총 {total}건 중 {len(hits)}건 표시\n")
    hline()
    for i, h in enumerate(hits, 1):
        src = h.get("_source", {})
        score = h.get("_score", "-")
        show = fields or ["title", "company", "job_type", "location", "career_type", "salary"]
        print(f"  [{i}] (score={score})")
        for f in show:
            print(f"       {f:12}: {src.get(f, '-')}")
        hline()


# ── 1. 공고 수 ────────────────────────────────────────────────
title("1. 전체 공고 수")
r = requests.get(f"{OS}/{INDEX}/_count", timeout=5)
print(f"  count = {r.json().get('count')}")


# ── 2. 샘플 공고 1건 ──────────────────────────────────────────
title("2. 샘플 공고 1건 (필드 확인)")
data = post(f"/{INDEX}/_search", {"size": 1})
hits = data.get("hits", {}).get("hits", [])
if hits:
    src = hits[0]["_source"]
    print(f"  필드 목록: {list(src.keys())}")
    print(f"  제목: {src.get('title')}")
    print(f"  회사: {src.get('company')}")
    vec = src.get("embedding_vector", [])
    print(f"  벡터: {'있음 (' + str(len(vec)) + '차원)' if vec else '없음'}")


# ── 3. 벡터 생성 여부 ─────────────────────────────────────────
title("3. embedding_vector 생성 확인")
data = post(f"/{INDEX}/_search", {
    "query": {"exists": {"field": "embedding_vector"}},
    "size": 0
})
vec_count = data.get("hits", {}).get("total", {}).get("value", 0)
total_r = requests.get(f"{OS}/{INDEX}/_count", timeout=5).json().get("count", 0)
print(f"  벡터 있는 공고: {vec_count} / {total_r}")


# ── 4. Neural Search ──────────────────────────────────────────
title("4. Neural Search — 백엔드 Python 경력 서울")
model_id = get_model_id()
if not model_id:
    print("  ML 모델 미배포 — Neural Search 불가 (키워드 검색으로 대체)")
else:
    print(f"  model_id: {model_id}")
    data = post(f"/{INDEX}/_search", {
        "query": {
            "neural": {
                "embedding_vector": {
                    "query_text": "Python 백엔드 개발자 서울 경력",
                    "model_id": model_id,
                    "k": 5
                }
            }
        },
        "_source": ["title", "company", "job_type", "location", "career_type", "salary"],
        "size": 5
    })
    print_hits(data)


# ── 5. 키워드 검색 ────────────────────────────────────────────
title("5. 키워드 검색 — '백엔드'")
data = post(f"/{INDEX}/_search", {
    "query": {
        "multi_match": {
            "query": "백엔드",
            "fields": ["title^3", "company^2", "description", "requirements"]
        }
    },
    "_source": ["title", "company", "job_type", "location"],
    "size": 5
})
print_hits(data, ["title", "company", "job_type", "location"])


# ── 6. 필터 검색 ──────────────────────────────────────────────
title("6. 필터 검색 — 경력")
data = post(f"/{INDEX}/_search", {
    "query": {
        "bool": {
            "filter": [{"term": {"career_type": "경력"}}]
        }
    },
    "_source": ["title", "company", "job_type", "career_type", "location"],
    "size": 5
})
print_hits(data, ["title", "company", "job_type", "career_type"])


print("\n  완료!\n")
