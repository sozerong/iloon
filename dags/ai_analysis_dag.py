"""
AI 분석 DAG — 매일 02:00 실행

상류 DAG 완료를 **실제로 기다린 뒤**, 입력 건수를 확인하고, 독립 태스크를 병렬로 돌린다.

    wait_for_dummy_jobs ─┐
                         ├→ validate_input ─┬→ behavior_1 .. behavior_5   (서로 독립)
    wait_for_user_events ┘                  ├→ trend_1 .. trend_6         (서로 독립)
                                            ├→ user_segmentation
                                            └→ job_popularity
                                                       ↓
                                                 notify_complete

의존 관계 근거: analyze_*.py 는 전부 logs/*.jsonl 원본만 읽는다.
어떤 스크립트도 다른 스크립트의 산출물을 읽지 않아서, 기존의 긴 체인
(behavior_1 → ... → trend_6 → job_popularity)은 실제 의존이 아니었다.
"""

from __future__ import annotations

import glob
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.trigger_rule import TriggerRule

# ── 경로 / 환경 ──────────────────────────────────────────────
PROJECT_DIR = "/opt/airflow/project"
# 이미지의 기본 인터프리터. airflow_requirements.txt 가 여기에 설치된다 (Dockerfile 참조).
# 예전 값이던 /opt/airflow/project/venv 는 어디서도 만들어지지 않아 태스크가 전부 실패했다.
PYTHON_BIN  = "python"

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

BEHAVIOR_SCRIPT     = f"{PROJECT_DIR}/analyze_ai_vs_normal.py"
TREND_SCRIPT        = f"{PROJECT_DIR}/analyze_job_trends.py"
SEGMENTATION_SCRIPT = f"{PROJECT_DIR}/analyze_user_segmentation.py"
POPULARITY_SCRIPT   = f"{PROJECT_DIR}/analyze_job_popularity.py"

LOGS_DIR = f"{PROJECT_DIR}/logs"


# ── 입력 건수 게이트 ──────────────────────────────────────────
def _count_lines(pattern: str) -> "tuple[int, int]":
    """(파일 수, 총 줄 수). 빈 줄은 세지 않는다."""
    files = glob.glob(os.path.join(LOGS_DIR, pattern))
    rows = 0
    for path in files:
        with open(path, encoding="utf-8") as f:
            rows += sum(1 for line in f if line.strip())
    return len(files), rows


def validate_input(**context) -> dict:
    """
    상류 산출물 건수를 세고 XCom에 남긴다. 0건이면 명시적으로 실패시킨다.

    AirflowFailException 은 재시도 없이 즉시 실패시킨다. 여기서는 그게 맞다 —
    상류 DAG 완료를 센서로 이미 기다렸으므로, 0건이면 몇 번을 다시 세도 0건이다.
    (지시서의 "재시도 대상이 되게" 와는 어긋나지만, 재시도가 상황을 바꾸지 못한다)
    """
    job_files, job_rows = _count_lines("dummy-jobs-*.jsonl")
    ev_files,  ev_rows  = _count_lines("user-events-*.jsonl")

    print(f"공고 JSONL   : 파일 {job_files}개 / {job_rows:,}건")
    print(f"이벤트 JSONL : 파일 {ev_files}개 / {ev_rows:,}건")

    ti = context["ti"]
    ti.xcom_push(key="job_count",   value=job_rows)
    ti.xcom_push(key="event_count", value=ev_rows)
    ti.xcom_push(key="job_files",   value=job_files)
    ti.xcom_push(key="event_files", value=ev_files)

    if job_rows == 0:
        raise AirflowFailException(
            f"공고 입력이 0건이다 ({LOGS_DIR}/dummy-jobs-*.jsonl, 파일 {job_files}개). "
            "상류 dummy_job_generator 산출물을 확인할 것."
        )
    if ev_rows == 0:
        raise AirflowFailException(
            f"이벤트 입력이 0건이다 ({LOGS_DIR}/user-events-*.jsonl, 파일 {ev_files}개). "
            "상류 user_event_generator 산출물을 확인할 것."
        )

    return {"job_count": job_rows, "event_count": ev_rows}


with DAG(
    dag_id="ai_vs_normal_analysis",
    description="사용자 행동 분석 + 공고 트렌드 Spark 파이프라인",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 2 * * *",   # 매일 02:00
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    # 병렬 태스크마다 SparkSession(JVM)이 하나씩 뜬다. 무제한으로 풀면 메모리가 터진다.
    # 워커 메모리에 맞춰 조절할 값.
    max_active_tasks=4,
    tags=["spark", "analysis", "ai", "trend"],
) as dag:

    # ── 상류 DAG 완료 대기 ────────────────────────────────────
    # TriggerDagRunOperator 도 검토했지만 쓰지 않았다. 그쪽은 상류가 하류를 "밀어주는"
    # 구조라 상류 DAG를 고쳐야 하고, 하류를 하나 더 붙일 때마다 상류가 또 바뀐다.
    # 여기서는 분석 DAG 혼자 자기 선행 조건을 선언하는 편이 결합도가 낮다.
    #
    # execution_delta 근거 (둘 다 data_interval_start 기준):
    #   분석 02:00 ← 공고 생성 01:00  → 1시간
    #   분석 02:00 ← 이벤트 생성 02:00 → 0 (2시간 주기라 같은 시각 run이 존재하고,
    #                                      그 run은 분석이 도는 시점엔 이미 끝나 있다)
    wait_for_jobs = ExternalTaskSensor(
        task_id="wait_for_dummy_jobs",
        external_dag_id="dummy_job_generator",
        external_task_id=None,                  # DagRun 전체 완료를 기다린다
        execution_delta=timedelta(hours=1),
        allowed_states=["success"],
        failed_states=["failed"],               # 상류가 죽으면 즉시 실패 (타임아웃까지 안 끌기)
        mode="reschedule",                      # 대기 중 워커 슬롯을 점유하지 않는다
        poke_interval=60,
        timeout=60 * 60,                        # 1시간
    )

    wait_for_events = ExternalTaskSensor(
        task_id="wait_for_user_events",
        external_dag_id="user_event_generator",
        external_task_id=None,
        execution_delta=timedelta(0),
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=60,
        timeout=60 * 60,
    )

    # ── 입력 건수 게이트 ──────────────────────────────────────
    gate = PythonOperator(
        task_id="validate_input",
        python_callable=validate_input,
        retries=0,                              # 0건은 다시 세도 0건이다
        doc_md="상류 산출물 건수 확인 — 0건이면 실패. 건수는 XCom에 남는다.",
    )

    # ── 분석 태스크 ───────────────────────────────────────────
    def bash_task(task_id: str, script: str, step: str, desc: str, **kw) -> BashOperator:
        return BashOperator(
            task_id=task_id,
            bash_command=f"{PYTHON_BIN} {script} --step {step}",
            env=BASE_ENV,
            doc_md=desc,
            **kw,
        )

    behaviors = [
        bash_task("behavior_1", BEHAVIOR_SCRIPT, "1", "AI 추천 vs 일반 공고 클릭/저장/지원 전환율 비교"),
        bash_task("behavior_2", BEHAVIOR_SCRIPT, "2", "지역별 AI 추천 클릭 vs 지원 전환율"),
        bash_task("behavior_3", BEHAVIOR_SCRIPT, "3", "AI 추천 vs 일반 공고 평균 체류 시간 비교"),
        bash_task("behavior_4", BEHAVIOR_SCRIPT, "4", "AI 매칭 점수 구간별 지원 전환율"),
        bash_task("behavior_5", BEHAVIOR_SCRIPT, "5", "일별 AI 추천 vs 일반 지원 전환율 트렌드"),
    ]

    trends = [
        bash_task("trend_1", TREND_SCRIPT, "1", "직군별 공고 수 분포"),
        bash_task("trend_2", TREND_SCRIPT, "2", "인기 기술스택 Top 20"),
        bash_task("trend_3", TREND_SCRIPT, "3", "직군별 평균 연봉 범위"),
        bash_task("trend_4", TREND_SCRIPT, "4", "지역별 공고 분포"),
        bash_task("trend_5", TREND_SCRIPT, "5", "경력 유형 × 기업 규모 분포"),
        bash_task("trend_6", TREND_SCRIPT, "6", "일별 공고 등록 트렌드"),
    ]

    segmentation = BashOperator(
        task_id="user_segmentation",
        bash_command=f"{PYTHON_BIN} {SEGMENTATION_SCRIPT} --step 1",
        env={**BASE_ENV, "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092"},
        doc_md="K-Means 사용자 세그멘테이션 (4 클러스터)",
    )

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
        ti      = context["ti"]
        results = _os.path.join(PROJECT_DIR, "results")
        files   = _os.listdir(results) if _os.path.isdir(results) else []
        jobs    = ti.xcom_pull(task_ids="validate_input", key="job_count") or 0
        events  = ti.xcom_pull(task_ids="validate_input", key="event_count") or 0
        print(
            f"\n{'='*55}\n"
            f"  분석 파이프라인 완료\n"
            f"  execution_date      : {context['execution_date']}\n"
            f"  입력 공고           : {jobs:,}건\n"
            f"  입력 이벤트         : {events:,}건\n"
            f"  사용자 행동 결과    : {len([f for f in files if f.startswith('analysis')])}개\n"
            f"  공고 트렌드 결과    : {len([f for f in files if f.startswith('trend')])}개\n"
            f"  세그멘테이션 결과   : {'있음' if 'user_segments.json' in files else '없음'}\n"
            f"  공고 인기도 결과    : {'있음' if 'job_popularity_scores.json' in files else '없음'}\n"
            f"{'='*55}"
        )

    notify_complete = PythonOperator(
        task_id="notify_complete",
        python_callable=notify,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ── 의존성 ────────────────────────────────────────────────
    analyses = behaviors + trends + [segmentation, job_popularity]

    [wait_for_jobs, wait_for_events] >> gate
    gate >> analyses >> notify_complete
