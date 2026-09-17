"""
작업 3 — Airflow DagRun 소요 시간 측정

`airflow dags test` 로 DAG를 동기 실행하고 벽시계 시간을 잰다.
(스케줄러에 맡기면 큐 대기가 섞여 들어가 전/후 비교가 흐려진다)

실행:
    python bench/bench_dag.py --dag ai_vs_normal_analysis --repeat 3 --label task3_before

태스크별 소요는 Airflow 메타DB의 task_instance 에서 직접 읽는다.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List

sys.stdout.reconfigure(encoding="utf-8")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

from bench.timing import summarize, RESULTS_DIR  # noqa: E402

CONTAINER = "money-airflow-scheduler"

# `airflow dags test` 는 task_instance 의 start_date/duration 을 채우지 않는다.
# (state 와 end_date 만 남는다) 그래서 태스크별 소요는 여기서 얻을 수 없고,
# 측정 기준은 DagRun 전체 벽시계 시간이다. 태스크별 비용은 bench/bench_dag_tasks.py 로 따로 잰다.
TASK_SQL = """
SELECT ti.task_id, ti.state, '0'
FROM task_instance ti
WHERE ti.dag_id = '{dag}' AND ti.run_id = '{run_id}'
ORDER BY ti.task_id;
"""


def pause(dag: str) -> None:
    """
    측정 내내 DAG를 멈춰 둔다.
    스케줄러가 자기 판단으로 DagRun을 하나 더 띄우면 같은 입력 파일을 두 프로세스가
    동시에 읽어 측정이 오염된다. (`dags delete` 는 pause 상태까지 지워버리므로 쓰지 않는다)
    """
    subprocess.run(["docker", "exec", CONTAINER, "airflow", "dags", "pause", dag],
                   capture_output=True, text=True)


def run_dag(dag: str, logical_date: str) -> "tuple[float, bool]":
    """airflow dags test 실행 → (소요 초, 성공 여부)"""
    t0 = time.perf_counter()
    proc = subprocess.run(
        ["docker", "exec", CONTAINER, "airflow", "dags", "test", dag, logical_date],
        capture_output=True, text=True, encoding="utf-8", errors="replace",
    )
    elapsed = time.perf_counter() - t0
    out = (proc.stdout or "") + (proc.stderr or "")
    # dags test 는 태스크가 실패해도 종료코드 0을 줄 때가 있어 로그로 한 번 더 본다
    failed = ("Marking task as FAILED" in out) or ("state=failed" in out)
    return elapsed, (proc.returncode == 0 and not failed)


def task_durations(dag: str, run_id: str) -> List[Dict[str, Any]]:
    sql = TASK_SQL.format(dag=dag, run_id=run_id).replace("\n", " ")
    proc = subprocess.run(
        ["docker", "exec", "money-postgres", "psql", "-U", "airflow", "-d", "airflow",
         "-t", "-A", "-F", "|", "-c", sql],
        capture_output=True, text=True, encoding="utf-8", errors="replace",
    )
    rows = []
    for line in (proc.stdout or "").strip().splitlines():
        parts = line.split("|")
        if len(parts) == 3 and parts[2]:
            rows.append({"task_id": parts[0], "state": parts[1], "seconds": float(parts[2])})
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description="Airflow DagRun 소요 측정")
    ap.add_argument("--dag",    type=str, default="ai_vs_normal_analysis")
    ap.add_argument("--repeat", type=int, default=3)
    ap.add_argument("--label",  type=str, default="task3")
    ap.add_argument("--date",   type=str, default="2026-08-20")
    args = ap.parse_args()

    runs: List[float] = []
    all_tasks: List[List[Dict[str, Any]]] = []
    oks: List[bool] = []

    pause(args.dag)

    base_day = datetime.strptime(args.date, "%Y-%m-%d")
    for i in range(1, args.repeat + 1):
        # 회차마다 다른 logical_date — 기록을 지우지 않고도 회차가 섞이지 않는다
        day = (base_day - timedelta(days=i - 1)).strftime("%Y-%m-%d")
        run_id = f"manual__{day}T00:00:00+00:00"
        print(f"[{i}/{args.repeat}] {args.dag} 실행 중... (logical_date={day})")
        elapsed, ok = run_dag(args.dag, day)
        tasks = task_durations(args.dag, run_id)
        runs.append(elapsed)
        oks.append(ok)
        all_tasks.append(tasks)
        states = {}
        for t in tasks:
            states[t["state"]] = states.get(t["state"], 0) + 1
        print(f"    {elapsed:.1f}s  성공={ok}  태스크 {len(tasks)}개 {states}")

    stats = summarize(runs)
    print(f"\n=== {args.label} — {args.repeat}회 ===")
    for k in ("n", "mean", "p50", "min", "max"):
        print(f"  {k:<5} {stats[k]:8.1f}" + ("" if k == "n" else "s"))
    print(f"  성공 여부: {oks}")

    os.makedirs(RESULTS_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(RESULTS_DIR, f"{args.label}_{ts}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"label": args.label, "dag": args.dag, "runs": runs,
                   "ok": oks, "stats": stats, "tasks": all_tasks},
                  f, ensure_ascii=False, indent=2)
    print(f"\n저장: {path}\n")


if __name__ == "__main__":
    main()
