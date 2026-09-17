"""
측정 하니스 — 구간별 소요 시간 측정 / 반복 실행 통계 / JSON 기록

사용:
    from bench.timing import stage, save, reset, repeat, summarize

    with stage("색인"):
        with stage("문서 준비"):
            ...
        with stage("전송"):
            ...
    save("bulk_before", meta={"docs": 5000})

반복 측정:
    stats = repeat("bulk_before", 3, run_once, meta={"docs": 5000})
    # {"n": 3, "mean": ..., "p50": ..., "p95": ..., "min": ..., "max": ...}

Python 3.9 호환 (typing.List/Dict 사용).
"""

from __future__ import annotations

import json
import os
import time
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Callable, Dict, Iterator, List, Optional

RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")

# 완료된 최상위 구간들 / 현재 열려 있는 구간 스택 (중첩 지원)
_root: List[Dict[str, Any]] = []
_stack: List[Dict[str, Any]] = []

# 기본은 꺼짐 — 운영 코드에 stage()가 박혀 있어도 아무것도 쌓이지 않는다.
# 벤치 스크립트가 enable()을 호출할 때만 기록한다.
_enabled = False


def enable(on: bool = True) -> None:
    """측정 기록 on/off. 벤치 스크립트 시작 시 호출."""
    global _enabled
    _enabled = on


def is_enabled() -> bool:
    return _enabled


def reset() -> None:
    """수집된 측정값 초기화 (반복 실행 사이에 호출)."""
    _root.clear()
    _stack.clear()


@contextmanager
def stage(name: str) -> Iterator[Dict[str, Any]]:
    """구간 측정. 중첩 가능. 예외가 나도 소요 시간은 기록된다."""
    if not _enabled:
        yield {}        # 꺼져 있으면 오버헤드 없이 통과
        return

    node: Dict[str, Any] = {"name": name, "seconds": 0.0, "children": []}
    (_stack[-1]["children"] if _stack else _root).append(node)
    _stack.append(node)
    t0 = time.perf_counter()
    try:
        yield node          # node["note"] 등에 추가 정보를 붙일 수 있음
    finally:
        node["seconds"] = time.perf_counter() - t0
        _stack.pop()


def total_seconds() -> float:
    """최상위 구간 소요 시간 합계."""
    return sum(n["seconds"] for n in _root)


def _render(nodes: List[Dict[str, Any]], depth: int = 0) -> List[str]:
    lines = []
    for n in nodes:
        pad = "  " * depth
        extra = f"  {n['note']}" if n.get("note") else ""
        lines.append(f"{pad}{n['name']:<28} {n['seconds']:8.3f}s{extra}")
        lines.extend(_render(n["children"], depth + 1))
    return lines


def report() -> str:
    """사람이 읽을 수 있는 트리 출력 문자열."""
    lines = _render(_root)
    lines.append(f"{'합계':<28} {total_seconds():8.3f}s")
    return "\n".join(lines)


def save(name: str, meta: Optional[Dict[str, Any]] = None) -> str:
    """결과를 bench/results/<name>_<timestamp>.json 으로 저장하고 콘솔에 출력."""
    os.makedirs(RESULTS_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(RESULTS_DIR, f"{name}_{ts}.json")
    payload = {
        "name": name,
        "recorded_at": datetime.now().isoformat(timespec="seconds"),
        "meta": meta or {},
        "total_seconds": total_seconds(),
        "stages": _root,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"\n=== {name} ===")
    print(report())
    print(f"저장: {path}\n")
    return path


def _percentile(values: List[float], pct: float) -> float:
    """nearest-rank 백분위 — 표본이 적어도 실제 관측값을 돌려준다."""
    if not values:
        raise ValueError("빈 리스트")
    s = sorted(values)
    idx = max(1, min(len(s), int(-(-pct * len(s) // 100))))  # ceil(pct/100 * n)
    return s[idx - 1]


def summarize(values: List[float]) -> Dict[str, float]:
    """반복 측정값 → n / mean / p50 / p95 / min / max"""
    if not values:
        raise ValueError("측정값이 없습니다")
    return {
        "n":    float(len(values)),
        "mean": sum(values) / len(values),
        "p50":  _percentile(values, 50),
        "p95":  _percentile(values, 95),
        "min":  min(values),
        "max":  max(values),
    }


def repeat(
    name: str,
    times: int,
    fn: Callable[[], Any],
    meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, float]:
    """
    fn 을 times 회 실행하며 매 회차의 구간 측정을 개별 JSON으로 남기고,
    회차별 총 소요 시간의 통계를 돌려준다.
    """
    totals: List[float] = []
    for i in range(1, times + 1):
        reset()
        fn()
        totals.append(total_seconds())
        save(f"{name}_run{i}", meta=meta)

    stats = summarize(totals)
    print(f"=== {name} — {times}회 반복 ===")
    for k in ("n", "mean", "p50", "p95", "min", "max"):
        print(f"  {k:<5} {stats[k]:8.3f}" + ("" if k == "n" else "s"))
    print()

    os.makedirs(RESULTS_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(RESULTS_DIR, f"{name}_summary_{ts}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(
            {"name": name, "meta": meta or {}, "runs": totals, "stats": stats},
            f, ensure_ascii=False, indent=2,
        )
    print(f"요약 저장: {path}\n")
    return stats


def demo() -> None:
    """자체 검증 — python -m bench.timing 또는 python bench/timing.py"""
    # 꺼져 있으면 아무것도 쌓이지 않는다 (운영 코드 누수 방지)
    enable(False)
    reset()
    with stage("무시됨"):
        pass
    assert _root == [], _root

    enable(True)
    reset()
    with stage("바깥"):
        with stage("안쪽 A"):
            time.sleep(0.02)
        with stage("안쪽 B") as n:
            n["note"] = "(메모)"
            time.sleep(0.01)

    assert len(_root) == 1, _root
    outer = _root[0]
    assert len(outer["children"]) == 2
    assert outer["seconds"] >= sum(c["seconds"] for c in outer["children"])
    assert outer["children"][0]["seconds"] >= 0.02
    assert not _stack, "스택이 비워지지 않음"

    # 예외가 나도 구간은 닫히고 시간이 기록된다
    reset()
    try:
        with stage("실패 구간"):
            raise RuntimeError("boom")
    except RuntimeError:
        pass
    assert not _stack
    assert _root[0]["seconds"] > 0

    # 백분위 (nearest-rank)
    vals = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
    s = summarize(vals)
    assert s["p50"] == 5.0 and s["p95"] == 10.0 and s["mean"] == 5.5, s
    assert summarize([3.0])["p95"] == 3.0

    reset()
    print("timing.py 자체 검증 통과")


if __name__ == "__main__":
    demo()
