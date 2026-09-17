"""
사용자 행동 이벤트 생성 DAG

매 2시간마다 실행:
  user_event_generator.py → logs/user-events-YYYY-MM-DD.jsonl
  (ai_analysis_dag 의 Spark 분석 입력)

NOTE: 같은 날짜 파일을 덮어씌우므로 하루 중 최신 이벤트 상태가 유지됨
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/airflow/project"
# 이미지의 기본 인터프리터. requirements.txt가 여기에 설치된다 (Dockerfile 참조).
# 예전 값이던 /opt/airflow/project/venv 는 어디서도 만들어지지 않아 태스크가 전부 실패했다.
PYTHON_BIN  = "python"

BASE_ENV = {
    "ANALYSIS_BASE_DIR":   PROJECT_DIR,
    "ANALYSIS_LOGS_DIR":   f"{PROJECT_DIR}/logs",
    "ANALYSIS_APPLOG_DIR": f"{PROJECT_DIR}/applogs",
    "PYTHONPATH":          PROJECT_DIR,
}

DEFAULT_ARGS = {
    "owner": "data-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=10),
    "email_on_failure": False,
}

with DAG(
    dag_id="user_event_generator",
    description="사용자 행동 더미 이벤트 생성 (Spark 분석용) — 2시간마다",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 */2 * * *",   # 매 2시간 (00:00, 02:00, 04:00 ...)
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    tags=["events", "dummy", "spark"],
) as dag:

    gen_events = BashOperator(
        task_id="gen_user_events",
        bash_command=(
            f"{PYTHON_BIN} {PROJECT_DIR}/user_event_generator.py "
            "--users 300 --ai-ratio 0.4 --days 30"
        ),
        env=BASE_ENV,
        doc_md="공고 기반 사용자 행동 이벤트 생성 → logs/user-events-오늘날짜.jsonl",
    )
