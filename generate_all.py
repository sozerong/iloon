"""
전체 카테고리 더미 공고 생성기

모든 IT 직무 카테고리를 순차 실행합니다.

실행:
  python generate_all.py
  python generate_all.py --count 3
  python generate_all.py --count 5 --parallel   # 병렬 실행
"""

import argparse
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

BASE_DIR = Path(__file__).parent

GENERATORS = [
    ("generator_backend.py",  "백엔드/서버"),
    ("generator_frontend.py", "프론트엔드"),
    ("generator_ai_ml.py",    "AI/ML"),
    ("generator_data.py",     "데이터"),
    ("generator_devops.py",   "인프라/DevOps"),
    ("generator_mobile.py",   "모바일"),
    ("generator_security.py", "보안"),
    ("generator_game.py",     "게임"),
    ("generator_qa.py",       "QA/테스트"),
    ("generator_pm.py",       "기획/PM"),
]


def run_generator(file: str, display: str, count: int):
    start = time.time()
    print(f"  ▶ [{display}] 시작")
    result = subprocess.run(
        [sys.executable, str(BASE_DIR / file), "--count", str(count)],
        capture_output=False,
        text=True,
    )
    elapsed = time.time() - start
    ok = result.returncode == 0
    status = "✅" if ok else "❌"
    print(f"  {status} [{display}] 완료 ({elapsed:.0f}초)")
    return display, ok, elapsed


def run_sequential(count: int) -> list:
    results = []
    for file, display in GENERATORS:
        display, ok, elapsed = run_generator(file, display, count)
        results.append((display, ok, elapsed))
    return results


def run_parallel(count: int, max_workers: int = 3) -> list:
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(run_generator, file, display, count): display
            for file, display in GENERATORS
        }
        for future in as_completed(futures):
            results.append(future.result())
    return results


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
    parser = argparse.ArgumentParser(description="전체 카테고리 더미 공고 생성")
    parser.add_argument("--count",    default=5, type=int,
                        help="카테고리당 생성할 공고 수 (기본: 5)")
    parser.add_argument("--parallel", action="store_true",
                        help="병렬 실행 (기본: 순차 실행, Anthropic API 동시 요청 주의)")
    parser.add_argument("--workers",  default=3, type=int,
                        help="병렬 실행 시 동시 작업 수 (기본: 3)")
    args = parser.parse_args()

    total_jobs = args.count * len(GENERATORS)
    mode = "병렬" if args.parallel else "순차"

    print(f"\n{'='*50}")
    print(f"  일로온 더미 공고 전체 생성")
    print(f"  카테고리: {len(GENERATORS)}개 | 카테고리당: {args.count}개")
    print(f"  예상 총 공고: {total_jobs}개 | 실행 모드: {mode}")
    print(f"{'='*50}\n")

    start = time.time()

    if args.parallel:
        print(f"⚡ 병렬 실행 (동시 {args.workers}개)\n")
        results = run_parallel(args.count, max_workers=args.workers)
    else:
        print("🔄 순차 실행\n")
        results = run_sequential(args.count)

    print_summary(results, time.time() - start)


if __name__ == "__main__":
    main()
