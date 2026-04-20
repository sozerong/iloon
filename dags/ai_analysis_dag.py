"""
AI 분석 DAG — 매일 02:00 실행

태스크 구조:
    validate_input
         ↓
    behavior_1 → behavior_2 → behavior_3 → behavior_4 → behavior_5
                                                          ↓         ↓
                                              segmentation       trend_1
                                             (K-Means)              ↓
                                                                 trend_2
                                                                     ↓
                                                                 trend_3
                                                                     ↓
                                                                 trend_4
                                                                     ↓
                                                                 trend_5
                                                                     ↓
                                                                 trend_6
                                                                     ↓
                                                             job_popularity
                                                            (GBT 인기도 예측)
                                                    ↓                   ↓
                                         [segmentation, job_popularity]
                                                         ↓
                                                  notify_complete
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

BASE_ENV = {
    "ANALYSIS_BASE_DIR":   PROJECT_DIR,
    "ANALYSIS_LOGS_DIR":   f"{PROJECT_DIR}/logs",
    "ANALYSIS_OUTPUT_DIR": f"{PROJECT_DIR}/results",
    "ANALYSIS_APPLOG_DIR": f"{PROJECT_DIR}/applogs",
    "JAVA_HOME": "/usr/lib/jvm/java-17-openjdk-amd64",
    "PATH": "/usr/lib/jvm/java-17-openjdk-amd64/bin:/usr/local/bin:/usr/bin:/bin",
}

DEFAULT_ARGS = {
    "owner": "data-team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=30),
    "email_on_failure": False,
    "email_on_retry": False,
}

BEHAVIOR_SCRIPT       = f"{PROJECT_DIR}/analyze_ai_vs_normal.py"
TREND_SCRIPT          = f"{PROJECT_DIR}/analyze_job_trends.py"
SEGMENTATION_SCRIPT   = f"{PROJECT_DIR}/analyze_user_segmentation.py"
POPULARITY_SCRIPT     = f"{PROJECT_DIR}/analyze_job_popularity.py"

with DAG(
    dag_id="ai_vs_normal_analysis",
    description="사용자 행동 분석 + 공고 트렌드 Spark 파이프라인",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 2 * * *",   # 매일 02:00
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    tags=["spark", "analysis", "ai", "trend"],
) as dag:

    # ── 0. 입력 파일 검증 ─────────────────────────────────────
    validate_input = BashOperator(
        task_id="validate_input",
        bash_command=(
            f"{PYTHON_BIN} {BEHAVIOR_SCRIPT} --step validate && "
            f"{PYTHON_BIN} {TREND_SCRIPT} --step validate"
        ),
        env=BASE_ENV,
        doc_md="사용자 이벤트 + 공고 JSONL 파일 존재 확인",
    )

    # ── 1~5. 사용자 행동 분석 ─────────────────────────────────
    def make_behavior_task(step_no: int, desc: str) -> BashOperator:
        return BashOperator(
            task_id=f"behavior_{step_no}",
            bash_command=f"{PYTHON_BIN} {BEHAVIOR_SCRIPT} --step {step_no}",
            env=BASE_ENV,
            doc_md=desc,
        )

    behavior_1 = make_behavior_task(1, "AI 추천 vs 일반 공고 클릭/저장/지원 전환율 비교")
    behavior_2 = make_behavior_task(2, "지역별 AI 추천 클릭 vs 지원 전환율")
    behavior_3 = make_behavior_task(3, "AI 추천 vs 일반 공고 평균 체류 시간 비교")
    behavior_4 = make_behavior_task(4, "AI 매칭 점수 구간별 지원 전환율")
    behavior_5 = make_behavior_task(5, "일별 AI 추천 vs 일반 지원 전환율 트렌드")

    # ── 1~6. 공고 트렌드 분석 ─────────────────────────────────
    def make_trend_task(step_no: int, desc: str) -> BashOperator:
        return BashOperator(
            task_id=f"trend_{step_no}",
            bash_command=f"{PYTHON_BIN} {TREND_SCRIPT} --step {step_no}",
            env=BASE_ENV,
            doc_md=desc,
        )

    trend_1 = make_trend_task(1, "직군별 공고 수 분포")
    trend_2 = make_trend_task(2, "인기 기술스택 Top 20")
    trend_3 = make_trend_task(3, "직군별 평균 연봉 범위")
    trend_4 = make_trend_task(4, "지역별 공고 분포")
    trend_5 = make_trend_task(5, "경력 유형 × 기업 규모 분포")
    trend_6 = make_trend_task(6, "일별 공고 등록 트렌드")

    # ── 세그멘테이션 (K-Means) ────────────────────────────────
    segmentation = BashOperator(
        task_id="user_segmentation",
        bash_command=f"{PYTHON_BIN} {SEGMENTATION_SCRIPT} --step 1",
        env={**BASE_ENV, "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092"},
        doc_md="K-Means 사용자 세그멘테이션 (4 클러스터)",
    )

    # ── 공고 인기도 예측 (GBTRegressor) ──────────────────────
    job_popularity = BashOperator(
        task_id="job_popularity",
        bash_command=f"{PYTHON_BIN} {POPULARITY_SCRIPT} --step all",
        env=BASE_ENV,
        execution_timeout=timedelta(minutes=45),   # 모델 학습 시간 여유
        doc_md="GBT 모델로 공고 인기도(지원율) 예측 → job_popularity 테이블 저장",
    )

    # ── 완료 알림 ─────────────────────────────────────────────
    def notify(**context):
        import os as _os
        results = _os.path.join(PROJECT_DIR, "results")
        files   = _os.listdir(results) if _os.path.isdir(results) else []
        behavior_files    = [f for f in files if f.startswith("analysis")]
        trend_files       = [f for f in files if f.startswith("trend")]
        segment_files     = [f for f in files if f == "user_segments.json"]
        popularity_files  = [f for f in files if f == "job_popularity_scores.json"]
        print(
            f"\n{'='*55}\n"
            f"  분석 파이프라인 완료\n"
            f"  execution_date      : {context['execution_date']}\n"
            f"  사용자 행동 결과    : {len(behavior_files)}개\n"
            f"  공고 트렌드 결과    : {len(trend_files)}개\n"
            f"  세그멘테이션 결과   : {'user_segments.json' if segment_files else '없음'}\n"
            f"  공고 인기도 결과    : {'job_popularity_scores.json' if popularity_files else '없음'}\n"
            f"{'='*55}"
        )

    notify_complete = PythonOperator(
        task_id="notify_complete",
        python_callable=notify,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ── 의존성 ────────────────────────────────────────────────
    # 사용자 행동 체인
    validate_input >> behavior_1 >> behavior_2 >> behavior_3 >> behavior_4 >> behavior_5

    # 세그멘테이션 (행동 분석 완료 후 병렬 실행)
    behavior_5 >> segmentation

    # 공고 트렌드 체인 (Spark 메모리 절약 위해 행동 분석 후 순차 실행)
    behavior_5 >> trend_1 >> trend_2 >> trend_3 >> trend_4 >> trend_5 >> trend_6

    # 공고 인기도 예측 (트렌드 완료 후 실행 — 트렌드 집계 결과 활용 가능)
    trend_6 >> job_popularity

    # 최종 완료 (세그멘테이션 + 공고 인기도 둘 다 끝나면)
    [segmentation, job_popularity] >> notify_complete
