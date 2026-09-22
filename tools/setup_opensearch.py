"""
OpenSearch 인덱스 생성 스크립트

실행:
  python setup_opensearch.py            # ML 모델 준비 + knn 인덱스
  python setup_opensearch.py --no-ml    # ML 없이 키워드 검색용 인덱스만

매핑을 여기서 따로 정의하지 않는다.
예전에는 이 파일이 자체 MAPPING 을 들고 있었는데, ai_server 의 ensure_index() 가
만드는 매핑(knn_vector + default_pipeline)과 달라서 **어느 쪽으로 먼저 만드느냐에 따라
인덱스가 달라졌다.** 이 스크립트로 먼저 만들면 벡터 필드가 없는 인덱스가 생기고,
ensure_index() 는 ML 모델이 없으면 그 인덱스를 그대로 쓴다 → neural search가 조용히 죽는다.
그래서 매핑의 단일 출처를 opensearch_service 로 두고 여기서는 호출만 한다.
"""

import argparse
import asyncio
import logging
import sys

sys.stdout.reconfigure(encoding="utf-8")

from ai_server.config import OPENSEARCH_HOST, JOBS_INDEX
from ai_server.services.opensearch_service import (
    close_client, ensure_index, ensure_ml_ready,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


async def main(use_ml: bool) -> int:
    import httpx

    # 연결 확인
    try:
        async with httpx.AsyncClient(timeout=5.0) as c:
            r = await c.get(f"{OPENSEARCH_HOST}/_cluster/health")
            r.raise_for_status()
            print(f"✅ OpenSearch 연결 성공 (status: {r.json().get('status')})")
    except Exception as e:
        print(f"❌ OpenSearch 연결 실패: {e}")
        return 1

    model_id = None
    if use_ml:
        print("ML 모델 준비 중 (최초 실행 시 수 분 소요)...")
        model_id = await ensure_ml_ready()
        if model_id:
            print(f"✅ ML 모델 준비 완료: {model_id}")
        else:
            print("⚠️  ML 모델 준비 실패 — 벡터 없는 인덱스로 진행 (키워드 검색은 동작)")

    try:
        await ensure_index(model_id=model_id)
    except Exception as e:
        print(f"❌ 인덱스 생성 실패: {e}")
        return 1
    finally:
        await close_client()

    print(f"✅ 인덱스 '{JOBS_INDEX}' 준비 완료 (knn={bool(model_id)})")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="OpenSearch 인덱스 생성")
    ap.add_argument("--no-ml", action="store_true",
                    help="ML 모델 등록/배포를 건너뛴다 (벡터 없는 인덱스)")
    args = ap.parse_args()

    sys.exit(asyncio.get_event_loop().run_until_complete(main(use_ml=not args.no_ml)))
