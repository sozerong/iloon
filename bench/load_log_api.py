"""
작업 2 — 이벤트 수집 API 부하 스크립트

동시 요청 N개로 총 M건의 이벤트를 보내고, 요청별 응답 시간을 모아
p50 / p95 / 평균과 처리량을 낸다. 서버의 구간별 통계(/stats/timing)도 함께 받아온다.

실행:
    # 서버 (baseline)
    LOG_API_TIMING=1 ANALYSIS_BASE_DIR=... uvicorn bench.log_api_before:app --port 8100
    # 서버 (after)
    LOG_API_TIMING=1 ANALYSIS_BASE_DIR=... uvicorn log_api:app --port 8100

    python bench/load_log_api.py --concurrency 20 --total 20000 --batch 10 --label task2_before
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List

sys.stdout.reconfigure(encoding="utf-8")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

import httpx  # noqa: E402

from bench.timing import summarize, RESULTS_DIR  # noqa: E402

EVENT_TYPES = ["view", "click", "save", "apply", "search"]


def make_batch(n: int, seq: int) -> Dict[str, Any]:
    """LogEvent 스키마에 맞는 이벤트 배치 — timestamp는 ISO 8601."""
    base = datetime(2026, 8, 24, 12, 0, 0)
    events = []
    for i in range(n):
        events.append({
            "event_id":   str(uuid.uuid4()),
            "timestamp":  (base + timedelta(seconds=seq * n + i)).isoformat(),
            "session_id": f"sess-{(seq * n + i) % 500}",
            "user_id":    f"user-{(seq * n + i) % 300}",
            "event_type": EVENT_TYPES[(seq * n + i) % len(EVENT_TYPES)],
            "job_id":     f"job-{(seq * n + i) % 1000}",
            "is_ai_recommended": bool((seq * n + i) % 3),
        })
    return {"events": events}


async def worker(
    client: httpx.AsyncClient,
    url: str,
    queue: "asyncio.Queue[int]",
    batch: int,
    latencies: List[float],
    failures: List[int],
) -> None:
    while True:
        try:
            seq = queue.get_nowait()
        except asyncio.QueueEmpty:
            return
        payload = make_batch(batch, seq)
        t0 = time.perf_counter()
        try:
            r = await client.post(url, json=payload)
            latencies.append((time.perf_counter() - t0) * 1000)
            if r.status_code != 202:
                failures.append(r.status_code)
        except Exception:
            failures.append(-1)
        finally:
            queue.task_done()


async def run(args: argparse.Namespace) -> Dict[str, Any]:
    url      = f"{args.host}/logs/bulk"
    n_batch  = args.total // args.batch
    queue: "asyncio.Queue[int]" = asyncio.Queue()
    for i in range(n_batch):
        queue.put_nowait(i)

    latencies: List[float] = []
    failures:  List[int]   = []

    limits = httpx.Limits(max_connections=args.concurrency + 10)
    async with httpx.AsyncClient(timeout=60.0, limits=limits) as client:
        await client.post(f"{args.host}/stats/reset")

        t0 = time.perf_counter()
        workers = [
            asyncio.create_task(worker(client, url, queue, args.batch, latencies, failures))
            for _ in range(args.concurrency)
        ]
        await asyncio.gather(*workers)
        wall = time.perf_counter() - t0

        # 남은 버퍼를 비워 파일에 실제로 몇 건이 들어갔는지 확인 가능하게
        flushed = await client.post(f"{args.host}/logs/flush")
        timing  = await client.get(f"{args.host}/stats/timing")

    stats = summarize(latencies) if latencies else {}
    return {
        "label":       args.label,
        "host":        args.host,
        "concurrency": args.concurrency,
        "total_events": args.total,
        "batch":       args.batch,
        "requests":    n_batch,
        "wall_seconds": wall,
        "req_per_sec": n_batch / wall if wall else 0,
        "events_per_sec": args.total / wall if wall else 0,
        "failures":    len(failures),
        "latency_ms":  stats,
        "final_flush": flushed.json(),
        "server_stages": timing.json(),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="log_api 부하 측정")
    ap.add_argument("--host",        type=str, default="http://127.0.0.1:8100")
    ap.add_argument("--concurrency", type=int, default=20)
    ap.add_argument("--total",       type=int, default=20000, help="총 이벤트 건수")
    ap.add_argument("--batch",       type=int, default=10,    help="요청당 이벤트 수")
    ap.add_argument("--label",       type=str, default="load")
    args = ap.parse_args()

    result = asyncio.get_event_loop().run_until_complete(run(args))

    lat = result["latency_ms"]
    print(f"\n=== {args.label} ===")
    print(f"  동시 요청     : {args.concurrency}")
    print(f"  총 이벤트     : {args.total} ({result['requests']}요청 × {args.batch}건)")
    print(f"  총 소요       : {result['wall_seconds']:.2f}s")
    print(f"  처리량        : {result['req_per_sec']:.0f} req/s / {result['events_per_sec']:.0f} events/s")
    print(f"  실패          : {result['failures']}")
    print(f"  응답시간 p50  : {lat.get('p50', 0):.2f} ms")
    print(f"  응답시간 p95  : {lat.get('p95', 0):.2f} ms")
    print(f"  응답시간 평균 : {lat.get('mean', 0):.2f} ms")
    print(f"  최종 flush    : {result['final_flush']}")

    stages = result["server_stages"].get("stages")
    if stages:
        print("  서버 구간별 (ms):")
        for name, s in stages.items():
            if s["n"]:
                print(f"    {name:<8} n={s['n']:<6} p50={s['p50']:.3f}  p95={s['p95']:.3f}  mean={s['mean']:.3f}")

    os.makedirs(RESULTS_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(RESULTS_DIR, f"{args.label}_{ts}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"\n저장: {path}\n")


if __name__ == "__main__":
    main()
