"""
neural 이 keyword 보다 정말 나은가 — 쌍체 검정

`eval_search.py` 는 모드별 평균만 낸다. Recall@3 이 0.540 → 0.587 이면
"좋아졌다"고 쓰고 싶어지는데, 쿼리 50개에서 0.047 차이는 **문항 2.35개**다.
평균만 보고 판단할 수 없다.

같은 쿼리 50개를 두 모드가 모두 처리했으므로 **쌍체(paired) 비교**가 가능하다.
쿼리별 차이를 직접 보는 쪽이 평균 차이보다 정보가 많다 —
"몇 개가 좋아지고 몇 개가 나빠졌는가"가 드러난다.

두 가지를 낸다.

1. **부호검정 (sign test)**
   좋아진 쿼리 수 vs 나빠진 쿼리 수. 동점은 제외한다.
   귀무가설은 "좋아질 확률 = 나빠질 확률 = 1/2" 이고, 이항분포로 양측 p 를 정확히 계산한다.
   정규근사를 쓰지 않는 이유는 비동점 표본이 작아질 수 있어서다.

2. **쌍체 부트스트랩 신뢰구간**
   쿼리 단위로 복원추출해 평균 차이의 분포를 만든다.
   구간이 0 을 포함하면 "개선됐다"고 말할 수 없다.

둘 다 `scipy` 없이 표준 라이브러리로 계산한다 — 의존성을 늘릴 이유가 없다.

사용
    python bench/search_significance.py                       # 최신 결과 자동 선택
    python bench/search_significance.py --file <path> -k 3
    python bench/search_significance.py --selftest
"""

from __future__ import annotations

import argparse
import glob
import json
import math
import os
import random
import sys
from typing import Dict, List, Tuple

sys.stdout.reconfigure(encoding="utf-8")

BASE_DIR   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS    = os.path.join(BASE_DIR, "bench", "results")
BOOTSTRAP  = 10000
SEED       = 20260922


def latest_result() -> str:
    files = sorted(glob.glob(os.path.join(RESULTS, "search_quality_*.json")))
    if not files:
        raise SystemExit("search_quality_*.json 없음 — 먼저 `python bench/eval_search.py --mode all`")
    return files[-1]


def load_pairs(path: str, k: int, metric: str) -> Tuple[List[float], List[float], List[str]]:
    """(keyword 점수[], neural 점수[], 쿼리[]) — 쿼리 순서가 같은지 검증한다."""
    data = json.load(open(path, encoding="utf-8"))
    by_mode = {r["mode"]: r for r in data["results"]}
    for m in ("keyword", "neural"):
        if m not in by_mode:
            raise SystemExit(f"'{m}' 모드 결과가 없다 — `--mode all` 로 다시 측정할 것")

    def scores(mode: str) -> Tuple[List[float], List[str]]:
        rows = by_mode[mode]["per_query"]
        vals = [r[metric][str(k)] if metric != "mrr" else r["mrr"] for r in rows]
        return vals, [r["query"] for r in rows]

    a, qa = scores("keyword")
    b, qb = scores("neural")
    # 쌍체 검정은 같은 문항끼리 붙어야 성립한다. 순서가 어긋나면 결과가 조용히 틀린다.
    if qa != qb:
        raise SystemExit("두 모드의 쿼리 순서가 다르다 — 쌍체 비교 불가")
    return a, b, qa


def binom_two_sided(k: int, n: int) -> float:
    """p=0.5 이항검정 양측 p-value. 정확 계산 (근사 아님)."""
    if n == 0:
        return 1.0
    pmf = [math.comb(n, i) * 0.5 ** n for i in range(n + 1)]
    # 관측만큼 또는 그보다 더 극단적인(확률이 같거나 작은) 결과를 전부 더한다
    thresh = pmf[k] * (1 + 1e-9)
    return min(1.0, sum(p for p in pmf if p <= thresh))


def bootstrap_ci(a: List[float], b: List[float], reps: int, seed: int) -> Tuple[float, float]:
    """평균 차이(b - a)의 95% 쌍체 부트스트랩 구간."""
    n = len(a)
    diffs = [b[i] - a[i] for i in range(n)]
    rnd = random.Random(seed)
    means = []
    for _ in range(reps):
        s = sum(diffs[rnd.randrange(n)] for _ in range(n))
        means.append(s / n)
    means.sort()
    return means[int(0.025 * reps)], means[int(0.975 * reps)]


def report(path: str, k: int, metric: str) -> dict:
    a, b, queries = load_pairs(path, k, metric)
    n = len(a)
    mean_a, mean_b = sum(a) / n, sum(b) / n

    win  = [i for i in range(n) if b[i] > a[i]]
    lose = [i for i in range(n) if b[i] < a[i]]
    tie  = n - len(win) - len(lose)

    nz = len(win) + len(lose)
    p  = binom_two_sided(len(win), nz)
    lo, hi = bootstrap_ci(a, b, BOOTSTRAP, SEED)

    label = f"{metric}@{k}" if metric != "mrr" else "MRR"
    print("=" * 66)
    print(f"  neural vs keyword — 쌍체 검정 ({label}, 쿼리 {n}개)")
    print("=" * 66)
    print(f"  keyword 평균 {mean_a:.4f}   neural 평균 {mean_b:.4f}   차이 {mean_b - mean_a:+.4f}")
    print()
    print(f"  [부호검정] neural 승 {len(win)} / 패 {len(lose)} / 동점 {tie}")
    print(f"    비동점 {nz}개 기준 양측 p = {p:.4f}"
          + ("   → 유의 (p<0.05)" if p < 0.05 else "   → 구별 불가 (p≥0.05)"))
    print()
    print(f"  [쌍체 부트스트랩 {BOOTSTRAP:,}회] 평균 차이 95% CI  {lo:+.4f} ~ {hi:+.4f}")
    print("    " + ("구간이 0 을 포함한다 → 개선을 주장할 수 없다"
                    if lo <= 0 <= hi else "구간이 0 을 포함하지 않는다 → 차이가 있다"))
    print()

    verdict = "개선" if (p < 0.05 and not (lo <= 0 <= hi)) else "구별 불가"
    print(f"  판정: {verdict}")
    if verdict == "구별 불가":
        print()
        print("  neural 경로는 ML 노드 배포 + ingest pipeline + knn_vector 384d 저장을 요구한다.")
        print("  그 비용을 내고 얻은 것이 측정상 keyword 와 구별되지 않는다.")

    return {
        "file": os.path.basename(path), "metric": label, "queries": n,
        "keyword_mean": mean_a, "neural_mean": mean_b, "diff": mean_b - mean_a,
        "sign_test": {"win": len(win), "lose": len(lose), "tie": tie, "p_two_sided": p},
        "bootstrap_ci95": [lo, hi], "bootstrap_reps": BOOTSTRAP, "seed": SEED,
        "verdict": verdict,
    }


def selftest() -> None:
    """검정 자체가 틀리면 결론이 통째로 뒤집힌다."""
    # 이항검정 — 손으로 검산 가능한 값
    assert abs(binom_two_sided(5, 10) - 1.0) < 1e-9
    assert abs(binom_two_sided(10, 10) - 2 * 0.5 ** 10) < 1e-9     # 양쪽 꼬리
    assert abs(binom_two_sided(0, 10) - 2 * 0.5 ** 10) < 1e-9
    assert binom_two_sided(9, 10) < 0.05 and binom_two_sided(8, 10) > 0.05
    assert binom_two_sided(0, 0) == 1.0

    # 부트스트랩 — 차이가 0 이면 구간도 0
    lo, hi = bootstrap_ci([0.5] * 20, [0.5] * 20, 500, 1)
    assert lo == 0.0 == hi, (lo, hi)

    # 일정한 +0.2 차이면 구간이 0 을 포함하면 안 된다
    lo, hi = bootstrap_ci([0.1] * 30, [0.3] * 30, 500, 1)
    assert lo > 0, (lo, hi)

    # 부호가 섞이면 구간이 0 을 포함해야 한다
    a = [0.5] * 20
    b = [0.9] * 10 + [0.1] * 10
    lo, hi = bootstrap_ci(a, b, 2000, 1)
    assert lo < 0 < hi, (lo, hi)

    print("search_significance 자체 검증 통과")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="neural vs keyword 쌍체 검정")
    ap.add_argument("--file", default=None, help="search_quality_*.json (기본: 최신)")
    ap.add_argument("-k", type=int, default=3, help="Recall@k / nDCG@k 의 k")
    ap.add_argument("--metric", default="recall", choices=["recall", "ndcg", "mrr"])
    ap.add_argument("--selftest", action="store_true")
    args = ap.parse_args()

    if args.selftest:
        selftest()
    else:
        out = report(args.file or latest_result(), args.k, args.metric)
        dest = os.path.join(RESULTS, "search_significance.json")
        json.dump(out, open(dest, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
        print(f"\n  저장: {os.path.relpath(dest, BASE_DIR)}")
