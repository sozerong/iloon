"""
작업 1 — OpenSearch 색인 벤치마크

매 회차마다 인덱스를 삭제 후 재생성하고, 전체 문서를 색인한다.
구간(문서 준비 / 전송)은 opensearch_service 안의 stage() 계측에서 나온다.

실행:
    python bench/make_docs.py --count 5000
    python bench/bench_index.py --docs 5000 --repeat 3 --label before

    # 구현 후 chunk_size 스윕
    python bench/bench_index.py --docs 5000 --repeat 3 --label after --chunk-size 500

--with-ml 을 주면 ML 모델을 준비하고 ingest pipeline을 태운다 (작업 4용).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from typing import Any, Dict, List

sys.stdout.reconfigure(encoding="utf-8")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

import httpx  # noqa: E402

from bench import timing  # noqa: E402
from bench.timing import repeat, stage  # noqa: E402
from ai_server.config import OPENSEARCH_HOST, JOBS_INDEX  # noqa: E402
from ai_server.services import opensearch_service as osvc  # noqa: E402

SEED_DIR = os.path.join(BASE_DIR, "bench", "seed")


def load_docs(count: int) -> List[Dict[str, Any]]:
    path = os.path.join(SEED_DIR, f"jobs_{count}.json")
    if not os.path.isfile(path):
        raise FileNotFoundError(
            f"{path} 없음 — 먼저 실행: python bench/make_docs.py --count {count}"
        )
    with open(path, encoding="utf-8") as f:
        return json.load(f)


async def reset_index(with_ml: bool) -> None:
    """인덱스를 지우고 새로 만든다 — 매 회차 동일 출발점 보장."""
    async with httpx.AsyncClient(timeout=60.0) as c:
        await c.delete(f"{OPENSEARCH_HOST}/{JOBS_INDEX}")
    await osvc.ensure_index(model_id=osvc.get_model_id() if with_ml else None)


async def count_docs() -> int:
    async with httpx.AsyncClient(timeout=30.0) as c:
        await c.post(f"{OPENSEARCH_HOST}/{JOBS_INDEX}/_refresh")
        r = await c.get(f"{OPENSEARCH_HOST}/{JOBS_INDEX}/_count")
        return r.json().get("count", -1)


async def run_once(docs: List[Dict[str, Any]], with_ml: bool) -> int:
    # 인덱스 초기화는 측정 밖 (색인 자체만 잰다)
    await reset_index(with_ml)
    with stage("색인 전체"):
        ok = await osvc.bulk_index_jobs(docs)
    return ok


def main() -> None:
    ap = argparse.ArgumentParser(description="OpenSearch 색인 벤치마크")
    ap.add_argument("--docs",   type=int, default=5000)
    ap.add_argument("--repeat", type=int, default=3)
    ap.add_argument("--label",  type=str, default="index")
    ap.add_argument("--chunk-size",      type=int, default=None,
                    help="구현 후에만 의미 있음 (bulk 청크 건수)")
    ap.add_argument("--max-chunk-bytes", type=int, default=None,
                    help="구현 후에만 의미 있음 (bulk 청크 바이트)")
    ap.add_argument("--with-ml", action="store_true",
                    help="ML 모델 준비 + ingest pipeline 사용 (작업 4)")
    args = ap.parse_args()

    docs = load_docs(args.docs)
    print(f"문서 {len(docs)}건 로드")

    # 구현 후 튜닝 파라미터는 모듈 전역으로 전달 (구현 전에는 무시됨)
    if args.chunk_size is not None:
        osvc.BULK_CHUNK_SIZE = args.chunk_size
    if args.max_chunk_bytes is not None:
        osvc.BULK_MAX_CHUNK_BYTES = args.max_chunk_bytes

    if args.with_ml:
        print("ML 모델 준비 중 (최초 실행 시 수 분 소요)...")
        mid = asyncio.get_event_loop().run_until_complete(osvc.ensure_ml_ready())
        print(f"model_id = {mid}")
        if not mid:
            print("ML 모델 준비 실패 — pipeline 없이 진행합니다")

    timing.enable(True)

    indexed: List[int] = []

    def one_run() -> None:
        ok = asyncio.get_event_loop().run_until_complete(run_once(docs, args.with_ml))
        indexed.append(ok)

    meta = {
        "docs": len(docs),
        "repeat": args.repeat,
        "chunk_size": args.chunk_size,
        "max_chunk_bytes": args.max_chunk_bytes,
        "with_ml": args.with_ml,
        "opensearch": OPENSEARCH_HOST,
    }
    repeat(args.label, args.repeat, one_run, meta=meta)

    final = asyncio.get_event_loop().run_until_complete(count_docs())
    asyncio.get_event_loop().run_until_complete(osvc.close_client())
    print(f"회차별 색인 성공 건수: {indexed}")
    print(f"마지막 회차 인덱스 문서 수: {final} (기대: {len(docs)})")
    if final != len(docs):
        print("⚠️  문서 수 불일치 — 부분 실패 가능. 결과 해석 시 주의")


if __name__ == "__main__":
    main()
