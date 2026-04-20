"""
[DEPRECATED] Playwright 스크래퍼 DAG — dummy_job_dag.py 로 대체됨
dags/.airflowignore 에 등록되어 Airflow에서 로드되지 않음.

채용 공고 수집 DAG
- 매일 00:00 실행 (분석 DAG보다 2시간 먼저)
- 회사별 병렬 수집 → 완료 알림

태스크 구조:
    check_ollama                     Ollama 연결 + 모델 확인
         ↓
    init_db                          DB 초기화 / 마이그레이션
         ↓
    ┌──────────────────────────────┐
    scrape_1  scrape_2  ... scrape_20  (회사별 독립 태스크)
    └──────────────────────────────┘
         ↓
    scrape_summary                   수집 결과 요약
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PROJECT_DIR = "/opt/airflow/project"
PYTHON_BIN  = "/opt/airflow/project/venv/bin/python"

BASE_ENV = {
    "ANALYSIS_BASE_DIR":   PROJECT_DIR,
    "ANALYSIS_LOGS_DIR":   f"{PROJECT_DIR}/logs",
    "ANALYSIS_OUTPUT_DIR": f"{PROJECT_DIR}/results",
    "ANALYSIS_APPLOG_DIR": f"{PROJECT_DIR}/applogs",
    "JAVA_HOME": "/usr/lib/jvm/java-17-openjdk-amd64",
    "OLLAMA_HOST": "http://host.docker.internal:11434",  # 호스트 Ollama 연결
}

DEFAULT_ARGS = {
    "owner": "data-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
    "email_on_failure": False,
}

# 수집 대상 회사 (companies.py 와 동기화)
COMPANIES = [
    {"id":  1, "name": "네이버"},
    {"id":  2, "name": "카카오"},
    {"id":  3, "name": "라인플러스"},
    {"id":  4, "name": "쿠팡"},
    {"id":  5, "name": "우아한형제들"},
    {"id":  6, "name": "토스"},
    {"id":  7, "name": "당근마켓"},
    {"id":  8, "name": "크래프톤"},
    {"id":  9, "name": "넥슨"},
    {"id": 10, "name": "엔씨소프트"},
    {"id": 11, "name": "삼성SDS"},
    {"id": 12, "name": "LGCNS"},
    {"id": 13, "name": "카카오뱅크"},
    {"id": 14, "name": "카카오페이"},
    {"id": 15, "name": "하이브"},
    {"id": 16, "name": "야놀자"},
    {"id": 17, "name": "무신사"},
    {"id": 18, "name": "컬리"},
    {"id": 19, "name": "직방"},
    {"id": 20, "name": "현대오토에버"},
]

with DAG(
    dag_id="job_scraper",
    description="대기업 채용 공고 수집 파이프라인",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 0 * * *",   # 매일 00:00
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=3,              # 동시 스크래핑 최대 3개 (IP 차단 방지)
    tags=["scraper", "llm", "ollama"],
) as dag:

    # ── 1. Ollama 연결 확인 ───────────────────────────────────
    check_ollama = BashOperator(
        task_id="check_ollama",
        bash_command=(
            f"{PYTHON_BIN} -c \""
            "import sys; sys.path.insert(0, '/opt/airflow/project'); "
            "from job_scraper.llm_agent import check_ollama; "
            "sys.exit(0 if check_ollama() else 1)"
            "\""
        ),
        env=BASE_ENV,
    )

    # ── 2. DB 초기화 ──────────────────────────────────────────
    init_db = BashOperator(
        task_id="init_db",
        bash_command=(
            f"{PYTHON_BIN} -c \""
            "import sys; sys.path.insert(0, '/opt/airflow/project'); "
            "from job_scraper.storage import init_db; init_db()"
            "\""
        ),
        env=BASE_ENV,
    )

    # ── 3. 회사별 스크래핑 태스크 ────────────────────────────
    scrape_tasks = []
    for company in COMPANIES:
        task = BashOperator(
            task_id=f"scrape_{company['id']:02d}_{company['name']}",
            bash_command=(
                f"{PYTHON_BIN} -m job_scraper.main "
                f"--company-id {company['id']}"
            ),
            env={**BASE_ENV, "PYTHONPATH": PROJECT_DIR},
            doc_md=f"{company['name']} 채용 공고 수집",
        )
        scrape_tasks.append(task)

    # ── 4. 수집 결과 요약 ─────────────────────────────────────
    def summarize(**context):
        import sys
        sys.path.insert(0, PROJECT_DIR)
        from job_scraper.storage import get_stats
        stats = get_stats()
        print(f"\n{'='*50}")
        print(f"  채용 공고 수집 완료")
        print(f"  execution_date: {context['execution_date']}")
        print(f"  전체 누적 공고: {stats['total']}건")
        print(f"{'='*50}")
        for row in stats["by_company"]:
            print(f"  {row['company_name']:<15} {row['cnt']}건")

    scrape_summary = PythonOperator(
        task_id="scrape_summary",
        python_callable=summarize,
        provide_context=True,
    )

    # ── 의존성 ────────────────────────────────────────────────
    check_ollama >> init_db >> scrape_tasks >> scrape_summary
