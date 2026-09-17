"""
작업 3 — Airflow DagRun 소요 시간 측정 (스케줄러 경로)

`airflow dags test` 는 의존성 그래프와 무관하게 **한 프로세스에서 태스크를 순차 실행**한다.
그래서 병렬화 전/후를 비교하는 데 쓸 수 없다. 여기서는 실제 스케줄러(LocalExecutor)에
DagRun을 맡기고 dag_run.start_date ~ end_date 를 읽는다.

스케줄러 큐 대기가 섞이지만, 전/후 모두 같은 조건이라 비교는 성립한다.

실행:
    python bench/bench_dag_sched.py --repeat 3 --label task3_before_sched
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

SCHED = "money-airflow-scheduler"


def psql(sql: str) -> str:
    proc = subprocess.run(
        ["docker", "exec", "money-postgres", "psql", "-U", "airflow", "-d", "airflow",
         "-t", "-A", "-F", "|", "-c", sql],
        capture_output=True, text=True, encoding="utf-8", errors="replace",
    )
    return (proc.stdout or "").strip()


def airflow(*args: str) -> str:
    proc = subprocess.run(["docker", "exec", SCHED, "airflow", *args],
                          capture_output=True, text=True, encoding="utf-8", errors="replace")
    return (proc.stdout or "") + (proc.stderr or "")


def trigger(dag: str, run_id: str, logical_date: str) -> None:
    # logical_date 를 반드시 지정한다 — ExternalTaskSensor 의 execution_delta 가
    # 이 값을 기준으로 상류 DagRun 을 찾기 때문에, now() 로 두면 상류를 못 찾고 타임아웃 난다.
    airflow("dags", "trigger", dag, "--run-id", run_id, "--exec-date", logical_date)


def wait_done(dag: str, run_id: str, timeout: int = 1800) -> "tuple[str, float]":
    """DagRun 이 끝날 때까지 폴링 → (state, dag_run 소요 초)"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        # DAG가 unpause 된 동안 스케줄러는 자기 스케줄대로 DagRun을 또 만든다.
        # max_active_runs=1 이라 그 런이 슬롯을 물면 벤치 런이 queued 로 멈춘다.
        # 측정 대상이 아니므로 보이는 족족 지운다.
        psql(f"DELETE FROM dag_run WHERE dag_id='{dag}' AND run_id LIKE 'scheduled__%';")

        row = psql(
            "SELECT state, COALESCE(EXTRACT(EPOCH FROM (end_date - start_date)), -1) "
            f"FROM dag_run WHERE dag_id='{dag}' AND run_id='{run_id}';"
        )
        if row:
            parts = row.split("|")
            state = parts[0]
            if state in ("success", "failed"):
                return state, float(parts[1])
        time.sleep(5)
    return "timeout", -1.0


def task_rows(dag: str, run_id: str) -> List[Dict[str, Any]]:
    out = psql(
        "SELECT task_id, state, COALESCE(duration, 0) "
        f"FROM task_instance WHERE dag_id='{dag}' AND run_id='{run_id}' "
        "ORDER BY duration DESC NULLS LAST;"
    )
    rows = []
    for line in out.splitlines():
        p = line.split("|")
        if len(p) == 3:
            rows.append({"task_id": p[0], "state": p[1], "seconds": float(p[2] or 0)})
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description="스케줄러 경로 DagRun 측정")
    ap.add_argument("--dag",    type=str, default="ai_vs_normal_analysis")
    ap.add_argument("--repeat", type=int, default=3)
    ap.add_argument("--label",  type=str, default="task3_sched")
    ap.add_argument("--date",   type=str, default="2026-08-20",
                    help="기준 logical_date (seed_upstream_runs.py 와 맞출 것)")
    args = ap.parse_args()

    # 상류는 계속 pause 로 둔다 (스케줄 타고 자동 실행되면 입력 파일이 바뀐다)
    for up in ("dummy_job_generator", "user_event_generator"):
        airflow("dags", "pause", up)
    # 대상 DAG 는 스케줄러가 집어야 하므로 unpause. 다만 스케줄러가 자기 판단으로 만든
    # scheduled__ 런이 섞이면 측정이 오염되므로, 측정 전에 남은 런을 지운다.
    airflow("dags", "unpause", args.dag)
    psql(f"DELETE FROM dag_run WHERE dag_id='{args.dag}' AND run_id LIKE 'scheduled__%';")

    runs: List[float] = []
    states: List[str] = []
    all_tasks: List[List[Dict[str, Any]]] = []

    base_day = datetime.strptime(args.date, "%Y-%m-%d")
    for i in range(1, args.repeat + 1):
        # seed_upstream_runs.py 가 심어둔 날짜와 맞춰야 센서가 통과한다
        day = (base_day - timedelta(days=i - 1)).strftime("%Y-%m-%d")
        logical = f"{day}T00:00:00+00:00"
        run_id = f"bench_{args.label}_{i}_{int(time.time())}"
        print(f"[{i}/{args.repeat}] trigger {run_id} (logical_date={day})")
        trigger(args.dag, run_id, logical)
        state, seconds = wait_done(args.dag, run_id)
        tasks = task_rows(args.dag, run_id)
        runs.append(seconds)
        states.append(state)
        all_tasks.append(tasks)
        print(f"    {seconds:.1f}s  state={state}  태스크 {len(tasks)}개")
        for t in tasks[:5]:
            print(f"      {t['task_id']:<22} {t['seconds']:7.1f}s  {t['state']}")

    ok_runs = [r for r, s in zip(runs, states) if s == "success"]
    print(f"\n=== {args.label} — {args.repeat}회 ===")
    if ok_runs:
        stats = summarize(ok_runs)
        for k in ("n", "mean", "p50", "min", "max"):
            print(f"  {k:<5} {stats[k]:8.1f}" + ("" if k == "n" else "s"))
    else:
        stats = {}
        print("  성공한 회차가 없다")
    print(f"  상태: {states}")

    airflow("dags", "pause", args.dag)

    os.makedirs(RESULTS_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(RESULTS_DIR, f"{args.label}_{ts}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"label": args.label, "dag": args.dag, "runs": runs,
                   "states": states, "stats": stats, "tasks": all_tasks},
                  f, ensure_ascii=False, indent=2)
    print(f"\n저장: {path}\n")


if __name__ == "__main__":
    main()
