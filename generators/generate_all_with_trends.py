"""
전체 카테고리 더미 공고 생성기 + 트렌드/키워드 JSON 저장

generate_all.py 와 동일하게 전 직군 공고를 생성하되,
각 직군의 trend_summary(hot_frameworks, hot_languages, hot_tools,
talent_keywords, salary_trend, welfare_trend)를 모아
results/trend_keywords.json 하나의 파일로 저장합니다.

실행:
  python generate_all_with_trends.py
  python generate_all_with_trends.py --count 3
  python generate_all_with_trends.py --count 5 --parallel
"""

import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR))

from generator_base import (
    CategoryConfig,
    CACHE_DIR,
    OUTPUT_DIR,
    load_used_companies,
    save_used_companies,
    save_results,
    load_cache,
    save_cache,
    setup_logger,
    _get_client,
    _search_trends,
    _generate_jobs,
)

import generator_backend
import generator_frontend
import generator_ai_ml
import generator_data
import generator_devops
import generator_mobile
import generator_security
import generator_game
import generator_qa
import generator_pm

GENERATORS = [
    generator_backend.CONFIG,
    generator_frontend.CONFIG,
    generator_ai_ml.CONFIG,
    generator_data.CONFIG,
    generator_devops.CONFIG,
    generator_mobile.CONFIG,
    generator_security.CONFIG,
    generator_game.CONFIG,
    generator_qa.CONFIG,
    generator_pm.CONFIG,
]

TRENDS_OUTPUT = OUTPUT_DIR / "trend_keywords.json"


def run_one(config: CategoryConfig, count: int, use_cache: bool) -> tuple[str, bool, float, dict]:
    """단일 카테고리 실행 → (display_name, 성공여부, 소요초, trend_summary) 반환"""
    logger = setup_logger(f"generator_{config.name}")
    start = time.time()
    print(f"  ▶ [{config.display_name}] 시작")

    used_companies = load_used_companies(logger)
    result = load_cache(config.name, logger) if use_cache else None

    if not result:
        try:
            client = _get_client()
            trend_summary = _search_trends(client, config, logger)
            result = _generate_jobs(client, config, trend_summary, count, used_companies, logger)
        except Exception as e:
            logger.error("[%s] 생성 실패: %s", config.display_name, e)
            elapsed = time.time() - start
            print(f"  ❌ [{config.display_name}] 실패 ({elapsed:.0f}초)")
            return config.display_name, False, elapsed, {}

        if not result or "jobs" not in result:
            logger.error("[%s] 유효한 결과 없음", config.display_name)
            elapsed = time.time() - start
            print(f"  ❌ [{config.display_name}] 결과 없음 ({elapsed:.0f}초)")
            return config.display_name, False, elapsed, {}

        save_cache(config.name, result, logger)

    save_used_companies(result.get("jobs", []), logger)
    save_results(result, config, logger)

    trend = result.get("trend_summary", {})
    elapsed = time.time() - start
    print(f"  ✅ [{config.display_name}] 완료 ({elapsed:.0f}초)")
    return config.display_name, True, elapsed, trend


def save_trend_keywords(trends: dict) -> None:
    """카테고리별 trend_summary 를 하나의 JSON 파일로 저장"""
    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        with open(TRENDS_OUTPUT, "w", encoding="utf-8") as f:
            json.dump(trends, f, ensure_ascii=False, indent=2)
        print(f"\n📄 트렌드/키워드 저장 완료: {TRENDS_OUTPUT}")
    except Exception as e:
        print(f"\n❌ 트렌드 저장 실패: {e}")


def run_sequential(configs: list, count: int, use_cache: bool) -> tuple[list, dict]:
    results = []
    trends = {}
    for config in configs:
        display, ok, elapsed, trend = run_one(config, count, use_cache)
        results.append((display, ok, elapsed))
        if ok and trend:
            trends[config.name] = {
                "display_name": config.display_name,
                "analyzed_at": datetime.now().strftime("%Y-%m-%d"),
                "trend_summary": trend,
            }
    return results, trends


def run_parallel(configs: list, count: int, use_cache: bool, max_workers: int = 3) -> tuple[list, dict]:
    results = []
    trends = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(run_one, config, count, use_cache): config
            for config in configs
        }
        for future in as_completed(futures):
            config = futures[future]
            display, ok, elapsed, trend = future.result()
            results.append((display, ok, elapsed))
            if ok and trend:
                trends[config.name] = {
                    "display_name": config.display_name,
                    "analyzed_at": datetime.now().strftime("%Y-%m-%d"),
                    "trend_summary": trend,
                }
    return results, trends


def print_summary(results: list, total_elapsed: float) -> None:
    ok_count   = sum(1 for _, ok, _ in results if ok)
    fail_count = len(results) - ok_count

    print(f"\n{'='*50}")
    print(f"  생성 완료 요약")
    print(f"{'─'*50}")
    for display, ok, elapsed in sorted(results, key=lambda x: x[0]):
        status = "✅" if ok else "❌"
        print(f"  {status} {display:<15} ({elapsed:.0f}초)")
    print(f"{'─'*50}")
    print(f"  성공: {ok_count}/{len(results)} | 총 소요: {total_elapsed:.0f}초")
    print(f"{'='*50}")


def main():
    parser = argparse.ArgumentParser(description="전체 카테고리 더미 공고 생성 + 트렌드 저장")
    parser.add_argument("--count",     default=5, type=int,
                        help="카테고리당 생성할 공고 수 (기본: 5)")
    parser.add_argument("--parallel",  action="store_true",
                        help="병렬 실행 (기본: 순차 실행)")
    parser.add_argument("--workers",   default=3, type=int,
                        help="병렬 실행 시 동시 작업 수 (기본: 3)")
    parser.add_argument("--use-cache", action="store_true",
                        help="당일 캐시 재사용")
    args = parser.parse_args()

    total_jobs = args.count * len(GENERATORS)
    mode = "병렬" if args.parallel else "순차"

    print(f"\n{'='*50}")
    print(f"  일로온 더미 공고 전체 생성 (트렌드 저장 포함)")
    print(f"  카테고리: {len(GENERATORS)}개 | 카테고리당: {args.count}개")
    print(f"  예상 총 공고: {total_jobs}개 | 실행 모드: {mode}")
    print(f"{'='*50}\n")

    start = time.time()

    if args.parallel:
        print(f"⚡ 병렬 실행 (동시 {args.workers}개)\n")
        results, trends = run_parallel(GENERATORS, args.count, args.use_cache, args.workers)
    else:
        print("🔄 순차 실행\n")
        results, trends = run_sequential(GENERATORS, args.count, args.use_cache)

    print_summary(results, time.time() - start)

    if trends:
        save_trend_keywords(trends)


if __name__ == "__main__":
    main()
