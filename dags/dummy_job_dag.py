"""
더미 채용공고 생성 DAG  (job_scraper_dag 대체)

매일 01:00 실행:
  ① IT 직무별 더미 공고 생성 (10개 카테고리, 병렬)
      백엔드 / 프론트엔드 / AI·ML / 데이터 / DevOps
      모바일 / 보안 / 게임 / QA / 기획·PM
      → 각 카테고리 5개 = 하루 최대 50개 공고
  ② POST /api/v1/import/jobs 호출
      → PostgreSQL upsert + OpenSearch 인덱싱

태스크 구조:
    ┌─ gen_backend  ─┐
    ├─ gen_frontend ─┤
    ├─ gen_ai_ml   ─┤
    ├─ gen_data    ─┤
    ├─ gen_devops  ─┤ → import_jobs
    ├─ gen_mobile  ─┤
    ├─ gen_security─┤
    ├─ gen_game    ─┤
    ├─ gen_qa      ─┤
    └─ gen_pm      ─┘
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# ── 경로 / 환경 ──────────────────────────────────────────────
PROJECT_DIR = "/opt/airflow/project"
PYTHON_BIN  = "/opt/airflow/project/venv/bin/python"
AI_SERVER   = os.environ.get("AI_SERVER_URL", "http://ai-server:8000")

BASE_ENV = {
    "ANALYSIS_BASE_DIR":   PROJECT_DIR,
    "ANALYSIS_LOGS_DIR":   f"{PROJECT_DIR}/logs",
    "ANALYSIS_OUTPUT_DIR": f"{PROJECT_DIR}/results",
    "ANALYSIS_APPLOG_DIR": f"{PROJECT_DIR}/applogs",
    "PYTHONPATH":          PROJECT_DIR,
    # PostgreSQL (회사명 중복 조회)
    "PG_HOST":     os.environ.get("PG_HOST",     "postgres"),
    "PG_PORT":     os.environ.get("PG_PORT",     "5432"),
    "PG_USER":     os.environ.get("PG_USER",     "airflow"),
    "PG_PASSWORD": os.environ.get("PG_PASSWORD", "airflow"),
    "PG_JOB_DB":   os.environ.get("PG_JOB_DB",  "iloon_jobs"),
    # Anthropic
    "ANTHROPIC_API_KEY": os.environ.get("ANTHROPIC_API_KEY", ""),
}

DEFAULT_ARGS = {
    "owner": "data-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=30),
    "email_on_failure": False,
}

# ── 직무 카테고리 목록 ────────────────────────────────────────
CATEGORIES = [
    {"id": "backend",   "display": "백엔드/서버",    "file": "generator_backend.py"},
    {"id": "frontend",  "display": "프론트엔드",      "file": "generator_frontend.py"},
    {"id": "ai_ml",     "display": "AI/ML",          "file": "generator_ai_ml.py"},
    {"id": "data",      "display": "데이터",          "file": "generator_data.py"},
    {"id": "devops",    "display": "인프라/DevOps",   "file": "generator_devops.py"},
    {"id": "mobile",    "display": "모바일",          "file": "generator_mobile.py"},
    {"id": "security",  "display": "보안",            "file": "generator_security.py"},
    {"id": "game",      "display": "게임",            "file": "generator_game.py"},
    {"id": "qa",        "display": "QA/테스트",       "file": "generator_qa.py"},
    {"id": "pm",        "display": "기획/PM",         "file": "generator_pm.py"},
]


with DAG(
    dag_id="dummy_job_generator",
    description="IT 직무별 더미 채용공고 생성 → PostgreSQL + OpenSearch 임포트",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 1 * * *",   # 매일 01:00
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=5,              # 동시 생성 최대 5개 (API 과부하 방지)
    tags=["dummy", "generator", "anthropic"],
) as dag:

    # ── 1. 직무별 공고 생성 태스크 (병렬) ────────────────────
    gen_tasks = []
    for cat in CATEGORIES:
        task = BashOperator(
            task_id=f"gen_{cat['id']}",
            bash_command=(
                f"{PYTHON_BIN} {PROJECT_DIR}/{cat['file']} --count 5"
            ),
            env=BASE_ENV,
            doc_md=f"{cat['display']} 공고 5개 생성",
        )
        gen_tasks.append(task)

    # ── 2. DB + OpenSearch 임포트 ────────────────────────────
    def import_jobs(**context):
        """생성된 JSONL → POST /api/v1/import/jobs"""
        import urllib.request
        import json as _json

        url = f"{AI_SERVER}/api/v1/import/jobs"
        req = urllib.request.Request(
            url,
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                body = resp.read().decode("utf-8")
                data = _json.loads(body)
                print(f"\n{'='*50}")
                print(f"  더미 공고 임포트 완료")
                print(f"  execution_date : {context['execution_date']}")
                print(f"  files parsed   : {data.get('files', 0)}")
                print(f"  imported       : {data.get('imported', 0)}")
                print(f"  skipped        : {data.get('skipped', 0)}")
                print(f"{'='*50}")
                return data
        except Exception as e:
            # 임포트 실패는 경고만 (공고 생성은 이미 완료됨)
            print(f"[WARNING] 임포트 API 호출 실패: {e}")
            print(f"[WARNING] 수동으로 POST {url} 호출 필요")
            # raise 하지 않음 — 생성 결과는 JSONL에 보존됨

    import_task = PythonOperator(
        task_id="import_jobs",
        python_callable=import_jobs,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,   # 일부 생성 실패해도 임포트 시도
        execution_timeout=timedelta(minutes=5),
    )

    # ── 3. 완료 요약 ──────────────────────────────────────────
    def summarize(**context):
        import os as _os
        from pathlib import Path
        from datetime import datetime as _dt

        today = _dt.now().strftime("%Y-%m-%d")
        logs_dir = Path(PROJECT_DIR) / "logs"
        total = 0
        print(f"\n{'='*50}")
        print(f"  더미 공고 생성 완료 요약")
        print(f"  날짜: {today}")
        print(f"{'─'*50}")
        for cat in CATEGORIES:
            jsonl = logs_dir / f"dummy-jobs-{cat['id']}-{today}.jsonl"
            count = 0
            if jsonl.exists():
                with open(jsonl, encoding="utf-8") as f:
                    count = sum(1 for line in f if line.strip())
            total += count
            print(f"  {cat['display']:<15} {count}개")
        print(f"{'─'*50}")
        print(f"  합계: {total}개")
        print(f"{'='*50}")

    summary_task = PythonOperator(
        task_id="summary",
        python_callable=summarize,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ── 의존성 ────────────────────────────────────────────────
    gen_tasks >> import_task >> summary_task
