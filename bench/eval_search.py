"""
검색 품질 평가 — 키워드 / neural / 폴백 비교

정답 라벨은 사람이 만들 필요가 없다. 공고를 10개 직군으로 나눠 생성했기 때문에
각 공고에 직군 라벨이 이미 붙어 있다 (results.zip 의 파일별 category).
"이 검색어에 대한 정답 = 해당 직군의 공고" 로 두면 Recall/MRR/nDCG 를 바로 잴 수 있다.

⚠️ 코퍼스 한계 — 이 수치를 읽을 때 반드시 같이 봐야 한다
    고유 공고가 **30건뿐**이다 (직군당 3건). 벤치마크에서 쓴 5,000건은 이 30건을
    복제한 것이라 검색 평가에는 쓸 수 없다 (같은 문서가 상위를 도배한다).
    30건 코퍼스에서 Recall@10 은 전체의 1/3 을 긁어오는 것이라 어떤 방법이든
    1.0 에 가깝게 나온다. 그래서 **k 를 1/3/5 로 낮춰서** 잰다.
    직군당 정답이 3건이므로 Recall@3 이 가장 판별력 있다.

실행
    python bench/eval_search.py --mode keyword
    python bench/eval_search.py --mode neural
    python bench/eval_search.py --mode all      # 세 방식 한 번에 비교
"""

from __future__ import annotations

import argparse
import asyncio
import glob
import json
import math
import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

sys.stdout.reconfigure(encoding="utf-8")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

import httpx  # noqa: E402

from bench.make_docs import SEED_RESULT, _ensure_seed_extracted  # noqa: E402
from bench.timing import RESULTS_DIR  # noqa: E402
from ai_server.config import OPENSEARCH_HOST, JOBS_INDEX  # noqa: E402
from ai_server.services import opensearch_service as osvc  # noqa: E402
from ai_server.services.job_importer import _normalize_dummy  # noqa: E402

EVAL_INDEX = "iloon_jobs_eval"     # 벤치마크용 복제 인덱스와 섞이지 않게 분리

# 직군별 검색어. 공고 제목을 그대로 베끼지 않고, 구직자가 칠 법한 말로 적었다.
# (제목을 베끼면 키워드 검색이 유리해져 비교가 무의미해진다)
QUERIES: Dict[str, List[str]] = {
    "백엔드/서버": [
        "서버 개발자", "API 만드는 일", "스프링 백엔드 채용",
        "대용량 트래픽 처리 서버", "데이터베이스 설계하는 개발자",
    ],
    "프론트엔드": [
        "웹 화면 개발", "리액트 개발자", "UI 구현 프론트",
        "브라우저 성능 최적화", "퍼블리싱과 컴포넌트 개발",
    ],
    "AI/ML": [
        "머신러닝 엔지니어", "LLM 다루는 일", "모델 학습 및 배포",
        "인공지능 서비스 개발", "생성형 AI 에이전트 개발",
    ],
    "데이터": [
        "데이터 엔지니어", "ETL 파이프라인 구축", "데이터 분석가 채용",
        "스파크로 대용량 처리", "데이터 웨어하우스 설계",
    ],
    "인프라/DevOps": [
        "인프라 엔지니어", "쿠버네티스 운영", "CI/CD 구축",
        "클라우드 배포 자동화", "서버 모니터링과 장애 대응",
    ],
    "모바일": [
        "앱 개발자", "안드로이드 개발", "iOS 앱 만드는 일",
        "모바일 크로스플랫폼 개발", "스마트폰 앱 성능 개선",
    ],
    "보안": [
        "보안 엔지니어", "취약점 진단", "침해사고 대응",
        "정보보호 관리체계", "모의해킹 하는 일",
    ],
    "게임": [
        "게임 개발자", "언리얼 엔진 개발", "게임 클라이언트 프로그래머",
        "게임 서버 개발", "유니티로 게임 만들기",
    ],
    "QA/테스트": [
        "QA 엔지니어", "테스트 자동화", "품질 보증 담당",
        "버그 검증 업무", "테스트 케이스 설계",
    ],
    "기획/PM": [
        "서비스 기획자", "프로덕트 매니저", "요구사항 정의하는 일",
        "로드맵 수립과 우선순위", "기획서 작성 업무",
    ],
}


# ── 코퍼스 ────────────────────────────────────────────────────
def load_labeled_corpus() -> List[Dict[str, Any]]:
    """씨앗 공고 30건 + 정답 직군 라벨 (파일명이 카테고리를 들고 있다)."""
    _ensure_seed_extracted()
    docs = []
    for path in sorted(glob.glob(os.path.join(SEED_RESULT, "*.json"))):
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        category = data.get("category")
        for raw in data.get("jobs", []):
            job = _normalize_dummy(raw)
            if job and job.get("title"):
                job["_gold_category"] = category
                docs.append(job)
    return docs


async def rebuild_index(docs: List[Dict[str, Any]], with_ml: bool) -> Optional[str]:
    """평가 전용 인덱스를 새로 만들고 색인. JOBS_INDEX 를 건드리지 않는다."""
    model_id = await osvc.ensure_ml_ready() if with_ml else None

    # osvc 는 모듈 상수 JOBS_INDEX 를 쓴다. 평가 인덱스로 잠시 갈아끼운다.
    original = osvc.JOBS_INDEX
    osvc.JOBS_INDEX = EVAL_INDEX
    try:
        async with httpx.AsyncClient(timeout=60.0) as c:
            await c.delete(f"{OPENSEARCH_HOST}/{EVAL_INDEX}")
        await osvc.ensure_index(model_id=model_id)
        ok = await osvc.bulk_index_jobs([{k: v for k, v in d.items()
                                          if not k.startswith("_")} for d in docs])
        async with httpx.AsyncClient(timeout=60.0) as c:
            await c.post(f"{OPENSEARCH_HOST}/{EVAL_INDEX}/_refresh")
        if ok != len(docs):
            print(f"⚠️  색인 {ok}/{len(docs)}건")
    finally:
        osvc.JOBS_INDEX = original
    return model_id


# ── 검색 ─────────────────────────────────────────────────────
async def search_keyword(query: str, k: int) -> List[str]:
    payload = {
        "query": {"multi_match": {
            "query": query,
            "fields": ["title^3", "company^2", "description", "requirements", "search_text"],
        }},
        "size": k,
    }
    async with httpx.AsyncClient(timeout=30.0) as c:
        r = await c.post(f"{OPENSEARCH_HOST}/{EVAL_INDEX}/_search", json=payload)
        r.raise_for_status()
        return [h["_source"]["id"] for h in r.json()["hits"]["hits"]]


async def search_neural(query: str, k: int, model_id: str) -> List[str]:
    payload = {
        "query": {"neural": {"embedding_vector": {
            "query_text": query, "model_id": model_id, "k": k,
        }}},
        "size": k,
    }
    async with httpx.AsyncClient(timeout=60.0) as c:
        r = await c.post(f"{OPENSEARCH_HOST}/{EVAL_INDEX}/_search", json=payload)
        r.raise_for_status()
        return [h["_source"]["id"] for h in r.json()["hits"]["hits"]]


async def search_hybrid(query: str, k: int, model_id: str) -> List[str]:
    """
    운영 코드의 폴백 경로와 같은 형태 — neural 결과가 비면 키워드로 떨어진다.
    실제 서비스가 무엇을 돌려주는지가 이 줄이다.
    """
    ids = await search_neural(query, k, model_id)
    return ids if ids else await search_keyword(query, k)


# ── 지표 ─────────────────────────────────────────────────────
def recall_at_k(ranked: List[str], gold: set, k: int) -> float:
    if not gold:
        return 0.0
    return len(set(ranked[:k]) & gold) / min(len(gold), k)


def mrr(ranked: List[str], gold: set) -> float:
    for i, doc in enumerate(ranked, 1):
        if doc in gold:
            return 1.0 / i
    return 0.0


def ndcg_at_k(ranked: List[str], gold: set, k: int) -> float:
    dcg = sum(1.0 / math.log2(i + 1)
              for i, doc in enumerate(ranked[:k], 1) if doc in gold)
    ideal = sum(1.0 / math.log2(i + 1)
                for i in range(1, min(len(gold), k) + 1))
    return dcg / ideal if ideal else 0.0


async def evaluate(mode: str, docs: List[Dict[str, Any]],
                   model_id: Optional[str], ks: List[int]) -> Dict[str, Any]:
    gold_by_cat: Dict[str, set] = {}
    for d in docs:
        gold_by_cat.setdefault(d["_gold_category"], set()).add(d["id"])

    kmax = max(ks)
    per_query = []
    for category, queries in QUERIES.items():
        gold = gold_by_cat.get(category)
        if not gold:
            continue
        for q in queries:
            if mode == "keyword":
                ranked = await search_keyword(q, kmax)
            elif mode == "neural":
                ranked = await search_neural(q, kmax, model_id) if model_id else []
            else:
                ranked = await search_hybrid(q, kmax, model_id) if model_id else \
                         await search_keyword(q, kmax)
            per_query.append({
                "category": category, "query": q,
                "recall": {k: recall_at_k(ranked, gold, k) for k in ks},
                "mrr": mrr(ranked, gold),
                "ndcg": {k: ndcg_at_k(ranked, gold, k) for k in ks},
                "top": ranked[:3],
            })

    n = len(per_query)
    return {
        "mode": mode,
        "queries": n,
        "recall": {k: sum(p["recall"][k] for p in per_query) / n for k in ks},
        "mrr":    sum(p["mrr"] for p in per_query) / n,
        "ndcg":   {k: sum(p["ndcg"][k] for p in per_query) / n for k in ks},
        "per_query": per_query,
    }


async def run(args: argparse.Namespace) -> None:
    docs = load_labeled_corpus()
    cats = sorted({d["_gold_category"] for d in docs})
    print(f"코퍼스 {len(docs)}건 / 직군 {len(cats)}개 / 쿼리 {sum(len(v) for v in QUERIES.values())}개")
    print(f"⚠️  고유 공고 {len(docs)}건뿐 — 직군당 {len(docs)//len(cats)}건. k를 1/3/5로 낮춰서 잰다.\n")

    modes = ["keyword", "neural", "hybrid"] if args.mode == "all" else [args.mode]
    need_ml = any(m in ("neural", "hybrid") for m in modes)

    print("인덱스 재구성 중" + (" (ML 모델 준비 포함 — 최초 수 분)" if need_ml else "") + "...")
    model_id = await rebuild_index(docs, with_ml=need_ml)
    print(f"model_id = {model_id}\n")
    if need_ml and not model_id:
        print("⚠️  ML 모델 미준비 — neural 은 건너뛴다\n")

    ks = [1, 3, 5]
    results = []
    for m in modes:
        if m == "neural" and not model_id:
            continue
        results.append(await evaluate(m, docs, model_id, ks))

    print("=" * 72)
    print(f"  {'방식':<10} {'Recall@1':>9} {'Recall@3':>9} {'Recall@5':>9} "
          f"{'MRR':>7} {'nDCG@3':>8} {'nDCG@5':>8}")
    print("-" * 72)
    for r in results:
        print(f"  {r['mode']:<10} {r['recall'][1]:>9.3f} {r['recall'][3]:>9.3f} "
              f"{r['recall'][5]:>9.3f} {r['mrr']:>7.3f} "
              f"{r['ndcg'][3]:>8.3f} {r['ndcg'][5]:>8.3f}")
    print("=" * 72)

    # 직군별 Recall@3 — 어디서 갈리는지 봐야 한다
    print("\n  직군별 Recall@3:")
    print(f"    {'직군':<14}" + "".join(f"{r['mode']:>11}" for r in results))
    for cat in cats:
        row = f"    {cat:<14}"
        for r in results:
            vals = [p["recall"][3] for p in r["per_query"] if p["category"] == cat]
            row += f"{(sum(vals)/len(vals) if vals else 0):>11.3f}"
        print(row)

    os.makedirs(RESULTS_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(RESULTS_DIR, f"search_quality_{ts}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"corpus_size": len(docs), "categories": cats,
                   "model_id": model_id, "results": results},
                  f, ensure_ascii=False, indent=2)
    print(f"\n저장: {os.path.relpath(path, BASE_DIR)}")
    print("\n한계: 합성 공고라 실제 채용 공고보다 직군 구분이 뚜렷하다. 수치는 낙관적으로 읽어야 한다.")

    await osvc.close_client()


def selftest() -> None:
    """지표 계산 자체 검증 — 여기가 틀리면 전부 무의미해진다."""
    gold = {"a", "b", "c"}
    assert recall_at_k(["a", "b", "c"], gold, 3) == 1.0
    assert recall_at_k(["x", "y", "z"], gold, 3) == 0.0
    assert abs(recall_at_k(["a", "x", "y"], gold, 3) - 1 / 3) < 1e-9
    # gold 가 k 보다 많으면 분모는 k (Recall@k 상한이 1이 되도록)
    assert recall_at_k(["a"], {"a", "b", "c"}, 1) == 1.0

    assert mrr(["a", "x"], gold) == 1.0
    assert mrr(["x", "a"], gold) == 0.5
    assert mrr(["x", "y"], gold) == 0.0

    assert abs(ndcg_at_k(["a", "b", "c"], gold, 3) - 1.0) < 1e-9
    assert ndcg_at_k(["x", "y", "z"], gold, 3) == 0.0
    # 순서가 나쁠수록 낮아야 한다
    assert ndcg_at_k(["x", "a", "b"], gold, 3) < ndcg_at_k(["a", "b", "x"], gold, 3)
    assert ndcg_at_k([], gold, 3) == 0.0
    print("eval_search 지표 자체 검증 통과")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="검색 품질 평가")
    ap.add_argument("--mode", choices=["keyword", "neural", "hybrid", "all", "selftest"],
                    default="all")
    a = ap.parse_args()
    if a.mode == "selftest":
        selftest()
    else:
        asyncio.run(run(a))
