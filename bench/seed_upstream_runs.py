"""
측정용 픽스처 — 상류 DAG의 성공 DagRun을 만들어 둔다.

ExternalTaskSensor 가 기다리는 대상은 "상류 DAG의 특정 logical_date DagRun이 success인가"다.
측정에서 상류를 실제로 돌릴 수는 없다:
  - dummy_job_generator 는 Anthropic API 키가 필요하다
  - user_event_generator 는 logs/user-events-*.jsonl 을 다시 써서 측정 입력이 바뀐다

그래서 상류 DagRun 레코드만 success 로 심는다. 센서가 실제로 이 레코드를 보고
통과하는지 확인하는 용도이기도 하다 (심지 않으면 센서는 타임아웃까지 대기한다).

실행:
    python bench/seed_upstream_runs.py --date 2026-08-20 --days 3
    python bench/seed_upstream_runs.py --date 2026-08-20 --days 3 --clear   # 되돌리기
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime, timedelta
from typing import List, Tuple

sys.stdout.reconfigure(encoding="utf-8")

PG = ["docker", "exec", "money-postgres", "psql", "-U", "airflow", "-d", "airflow", "-t", "-A", "-c"]

# (dag_id, 분석 logical_date 로부터의 오프셋) — DAG의 execution_delta 와 반드시 일치해야 한다
UPSTREAM: List[Tuple[str, timedelta]] = [
    ("dummy_job_generator",  timedelta(hours=1)),   # execution_delta=1h
    ("user_event_generator", timedelta(0)),         # execution_delta=0
]


def psql(sql: str) -> str:
    proc = subprocess.run(PG + [sql], capture_output=True, text=True,
                          encoding="utf-8", errors="replace")
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr)
    return (proc.stdout or "").strip()


def targets(base: str, days: int) -> List[Tuple[str, str]]:
    day0 = datetime.strptime(base, "%Y-%m-%d")
    out = []
    for i in range(days):
        analysis = day0 - timedelta(days=i)
        for dag_id, delta in UPSTREAM:
            out.append((dag_id, (analysis - delta).strftime("%Y-%m-%d %H:%M:%S+00")))
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="상류 DagRun 픽스처")
    ap.add_argument("--date",  type=str, default="2026-08-20", help="분석 DAG 기준 logical_date")
    ap.add_argument("--days",  type=int, default=3, help="회차 수 (하루씩 거슬러 올라감)")
    ap.add_argument("--clear", action="store_true", help="심어둔 픽스처 삭제")
    args = ap.parse_args()

    rows = targets(args.date, args.days)

    if args.clear:
        for dag_id, ts in rows:
            psql(f"DELETE FROM dag_run WHERE dag_id='{dag_id}' "
                 f"AND run_id='fixture__{ts}';")
        print(f"픽스처 {len(rows)}건 삭제")
        return

    for dag_id, ts in rows:
        run_id = f"fixture__{ts}"
        psql(
            "INSERT INTO dag_run "
            "(dag_id, queued_at, execution_date, start_date, end_date, state, run_id, "
            " creating_job_id, external_trigger, run_type, conf, data_interval_start, "
            " data_interval_end, last_scheduling_decision, dag_hash, log_template_id, updated_at) "
            f"VALUES ('{dag_id}', now(), '{ts}', '{ts}', '{ts}', 'success', '{run_id}', "
            "NULL, true, 'manual', '\\x80057d942e', "
            f"'{ts}', '{ts}', now(), NULL, 1, now()) "
            "ON CONFLICT (dag_id, run_id) DO UPDATE SET state='success';"
        )

    print(f"상류 성공 DagRun {len(rows)}건 준비:")
    for dag_id, ts in rows:
        print(f"  {dag_id:<22} {ts}")

    check = psql(
        "SELECT dag_id, execution_date, state FROM dag_run "
        "WHERE run_id LIKE 'fixture__%' ORDER BY dag_id, execution_date;"
    )
    print("\nDB 확인:")
    for line in check.splitlines():
        print(f"  {line}")


if __name__ == "__main__":
    main()
