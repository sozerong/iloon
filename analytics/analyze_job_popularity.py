"""
공고 인기도 예측 모델 — Spark MLlib GBTRegressor

공고 특성(직군, 지역, 연봉, 기술스택 수, 경력)과
사용자 행동 로그(조회수, 북마크수, 지원수)를 결합해
신규 공고의 예상 지원율을 예측합니다.

입력:
  logs/dummy-jobs-*.jsonl   (공고 특성)
  logs/user-events-*.jsonl  (사용자 행동 로그)

출력:
  results/job_popularity_model/     (저장된 모델)
  results/job_popularity_scores.json (공고별 예측 점수)
  PostgreSQL iloon_jobs.job_popularity (예측 결과 저장)

실행:
  python analyze_job_popularity.py --step all
  python analyze_job_popularity.py --step train   # 학습만
  python analyze_job_popularity.py --step predict  # 예측만 (모델 재사용)
  python analyze_job_popularity.py --step validate
"""

from __future__ import annotations

import argparse
import glob as pyglob
import json
import logging
import logging.handlers
import os
import sys
import time
from contextlib import contextmanager
from typing import Any, Dict

sys.stdout.reconfigure(encoding="utf-8")

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, FloatType
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator

# ── 경로 설정 ─────────────────────────────────────────────────
BASE_DIR   = os.environ.get("ANALYSIS_BASE_DIR",   "C:/GitHub/new_git/money")
LOGS_DIR   = os.environ.get("ANALYSIS_LOGS_DIR",   os.path.join(BASE_DIR, "logs"))
OUTPUT_DIR = os.environ.get("ANALYSIS_OUTPUT_DIR", os.path.join(BASE_DIR, "results"))
APPLOG_DIR = os.environ.get("ANALYSIS_APPLOG_DIR", os.path.join(BASE_DIR, "applogs"))
MODEL_DIR  = os.path.join(OUTPUT_DIR, "job_popularity_model")

for _d in (OUTPUT_DIR, APPLOG_DIR):
    os.makedirs(_d, exist_ok=True)

# PostgreSQL 연결
PG_HOST     = os.environ.get("PG_HOST",     "localhost")
PG_PORT     = os.environ.get("PG_PORT",     "5432")
PG_USER     = os.environ.get("PG_USER",     "airflow")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "airflow")
PG_JOB_DB   = os.environ.get("PG_JOB_DB",  "iloon_jobs")


# ── 로거 ─────────────────────────────────────────────────────
def setup_logger() -> logging.Logger:
    logger = logging.getLogger("job_popularity")
    if logger.handlers:
        return logger
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    sh.setFormatter(fmt)
    fh = logging.handlers.TimedRotatingFileHandler(
        os.path.join(APPLOG_DIR, "job_popularity.log"),
        when="midnight", backupCount=7, encoding="utf-8",
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(sh)
    logger.addHandler(fh)
    return logger


logger = setup_logger()


# ── Spark ─────────────────────────────────────────────────────
def create_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("iloon-job-popularity")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    logger.info("SparkSession 초기화 완료 (v%s)", spark.version)
    return spark


@contextmanager
def step(name: str):
    logger.info("[시작] %s", name)
    t = time.perf_counter()
    yield
    logger.info("[완료] %s (%.2f초)", name, time.perf_counter() - t)


def save_json(data: list, filename: str) -> None:
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    logger.info("저장: %s (%d건)", path, len(data))


# ── 데이터 로드 ───────────────────────────────────────────────
def load_job_features(spark: SparkSession) -> DataFrame:
    """공고 JSONL → 특성 DataFrame"""
    files = pyglob.glob(os.path.join(LOGS_DIR, "dummy-jobs-*.jsonl"))
    if not files:
        raise FileNotFoundError(f"공고 JSONL 없음: {LOGS_DIR}/dummy-jobs-*.jsonl")

    df = spark.read.json(files)

    job_df = (
        df
        .withColumn("job_id",     F.col("job_id").cast("string"))
        .withColumn("job_category",
                    F.coalesce(
                        F.col("position.job_category.mid"),
                        F.col("position.job_category.large"),
                    ))
        .withColumn("career_type", F.col("position.career.type"))
        .withColumn("company_size", F.col("company.size"))
        .withColumn("region",       F.col("work_condition.location.sido"))
        .withColumn("salary_mid",
                    ((F.col("work_condition.salary.min").cast(LongType()) +
                      F.col("work_condition.salary.max").cast(LongType())) / 2 / 10000)
                    .cast(FloatType()))
        .withColumn("skill_count",  F.size(F.col("skills")))
        .select("job_id", "job_category", "career_type", "company_size",
                "region", "salary_mid", "skill_count")
        .filter(F.col("job_id").isNotNull())
        .dropDuplicates(["job_id"])
    )
    logger.info("공고 특성 로드: %d건", job_df.count())
    return job_df


def load_event_stats(spark: SparkSession) -> DataFrame:
    """사용자 이벤트 → 공고별 행동 통계 (타깃 변수)"""
    files = pyglob.glob(os.path.join(LOGS_DIR, "user-events-*.jsonl"))
    if not files:
        logger.warning("이벤트 JSONL 없음 — 더미 통계 생성")
        return spark.createDataFrame([], schema="job_id STRING, view_count LONG, bookmark_count LONG, apply_count LONG, apply_rate FLOAT")

    ev = spark.read.json(files)

    stats = (
        ev.groupBy("job_id")
        .agg(
            F.countDistinct(
                F.when(F.col("event_type") == "job_detail_view", F.col("event_id"))
            ).alias("view_count"),
            F.countDistinct(
                F.when(F.col("event_type") == "bookmark", F.col("event_id"))
            ).alias("bookmark_count"),
            F.countDistinct(
                F.when(F.col("event_type") == "apply_click", F.col("event_id"))
            ).alias("apply_count"),
        )
        .withColumn(
            "apply_rate",
            F.when(F.col("view_count") > 0,
                   (F.col("apply_count") / F.col("view_count") * 100).cast(FloatType()))
            .otherwise(F.lit(0.0))
        )
    )
    logger.info("이벤트 통계 생성: %d개 공고", stats.count())
    return stats


# ── 피처 엔지니어링 ───────────────────────────────────────────
def build_features(job_df: DataFrame, stats_df: DataFrame) -> DataFrame:
    """공고 특성 + 행동 통계 조인 → 학습용 DataFrame"""
    merged = (
        job_df.join(stats_df, on="job_id", how="left")
        .fillna({
            "view_count":     0,
            "bookmark_count": 0,
            "apply_count":    0,
            "apply_rate":     0.0,
            "salary_mid":     0.0,
            "skill_count":    0,
        })
        .fillna({
            "job_category": "기타",
            "career_type":  "무관",
            "company_size": "중소기업",
            "region":       "서울",
        })
    )

    total = merged.count()
    logger.info("학습 데이터: %d건", total)
    print(f"\n학습 데이터: {total:,}건")
    return merged


# ── 모델 학습 ─────────────────────────────────────────────────
def train_model(df: DataFrame) -> PipelineModel:
    """GBTRegressor로 apply_rate 예측 모델 학습"""

    # 범주형 변수 인코딩
    cat_cols    = ["job_category", "career_type", "company_size", "region"]
    cat_indexed = [c + "_idx" for c in cat_cols]

    indexers = [
        StringIndexer(inputCol=c, outputCol=c + "_idx", handleInvalid="keep")
        for c in cat_cols
    ]

    # 수치형 + 인코딩된 범주형 조합
    feature_cols = cat_indexed + ["salary_mid", "skill_count", "view_count", "bookmark_count"]

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features",
                                handleInvalid="keep")
    scaler    = StandardScaler(inputCol="raw_features", outputCol="features",
                               withMean=True, withStd=True)
    gbt       = GBTRegressor(
        featuresCol="features",
        labelCol="apply_rate",
        maxIter=30,
        maxDepth=4,
        stepSize=0.1,
        seed=42,
    )

    pipeline = Pipeline(stages=indexers + [assembler, scaler, gbt])

    # 학습 / 검증 분리 (80:20)
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    logger.info("학습 %d건 / 검증 %d건", train_df.count(), test_df.count())

    model = pipeline.fit(train_df)

    # 성능 평가
    preds = model.transform(test_df)
    evaluator = RegressionEvaluator(labelCol="apply_rate", predictionCol="prediction")

    rmse = evaluator.setMetricName("rmse").evaluate(preds)
    r2   = evaluator.setMetricName("r2").evaluate(preds)
    mae  = evaluator.setMetricName("mae").evaluate(preds)

    print(f"\n{'='*50}")
    print(f"  모델 성능 (검증 세트)")
    print(f"{'─'*50}")
    print(f"  RMSE : {rmse:.4f}")
    print(f"  MAE  : {mae:.4f}")
    print(f"  R²   : {r2:.4f}")
    print(f"{'='*50}")
    logger.info("모델 성능 — RMSE=%.4f MAE=%.4f R²=%.4f", rmse, mae, r2)

    # 모델 저장
    model.write().overwrite().save(MODEL_DIR)
    logger.info("모델 저장 완료: %s", MODEL_DIR)

    return model


# ── 예측 + 저장 ───────────────────────────────────────────────
def predict_and_save(model: PipelineModel, df: DataFrame) -> None:
    """전체 공고에 대해 인기도 점수 예측 후 저장"""
    preds = model.transform(df)

    # 인기도 등급 (A~D)
    result = (
        preds
        .withColumn(
            "popularity_score",
            F.round(F.col("prediction"), 2).cast(FloatType()),
        )
        .withColumn(
            "popularity_grade",
            F.when(F.col("popularity_score") >= 15, "A")  # 지원율 15% 이상
             .when(F.col("popularity_score") >= 8,  "B")
             .when(F.col("popularity_score") >= 3,  "C")
             .otherwise("D"),
        )
        .select(
            "job_id", "job_category", "region", "career_type",
            "salary_mid", "skill_count",
            "apply_rate",          # 실제 (학습 데이터)
            "popularity_score",    # 예측
            "popularity_grade",    # A/B/C/D
        )
        .orderBy(F.col("popularity_score").desc())
    )

    # 결과 출력
    print("\n== 인기도 예측 상위 10개 공고 ==")
    result.show(10, truncate=False)

    # 등급별 분포
    grade_dist = (
        result.groupBy("popularity_grade")
        .agg(F.count("*").alias("공고수"),
             F.round(F.avg("popularity_score"), 2).alias("평균점수"))
        .orderBy("popularity_grade")
    )
    print("\n== 등급별 공고 분포 ==")
    grade_dist.show()

    # JSON 저장
    rows = [row.asDict() for row in result.collect()]
    save_json(rows, "job_popularity_scores.json")

    # PostgreSQL 저장
    _save_to_postgres(rows)


def _save_to_postgres(rows: list) -> None:
    """PostgreSQL iloon_jobs.job_popularity 테이블에 저장"""
    try:
        import psycopg2
    except ImportError:
        logger.warning("psycopg2 미설치 — PostgreSQL 저장 생략 (pip install psycopg2-binary)")
        return

    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=int(PG_PORT),
            user=PG_USER, password=PG_PASSWORD,
            dbname=PG_JOB_DB,
        )
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS job_popularity (
                job_id           TEXT NOT NULL,
                job_category     TEXT,
                region           TEXT,
                career_type      TEXT,
                salary_mid       FLOAT,
                skill_count      INT,
                actual_apply_rate FLOAT,
                popularity_score FLOAT,
                popularity_grade TEXT,
                analyzed_at      TIMESTAMP DEFAULT NOW()
            )
        """)

        cur.execute("DELETE FROM job_popularity")

        cur.executemany(
            """INSERT INTO job_popularity
               (job_id, job_category, region, career_type, salary_mid, skill_count,
                actual_apply_rate, popularity_score, popularity_grade)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            [
                (
                    r.get("job_id"), r.get("job_category"), r.get("region"),
                    r.get("career_type"), r.get("salary_mid"), r.get("skill_count"),
                    r.get("apply_rate"), r.get("popularity_score"), r.get("popularity_grade"),
                )
                for r in rows
            ],
        )
        conn.commit()
        logger.info("PostgreSQL 저장 완료: %d건 → job_popularity", len(rows))
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning("PostgreSQL 저장 실패 (계속 진행): %s", e)


# ── 검증 ─────────────────────────────────────────────────────
def validate_input() -> None:
    job_files = pyglob.glob(os.path.join(LOGS_DIR, "dummy-jobs-*.jsonl"))
    ev_files  = pyglob.glob(os.path.join(LOGS_DIR, "user-events-*.jsonl"))
    if not job_files:
        logger.critical("공고 JSONL 없음: %s", LOGS_DIR)
        sys.exit(1)
    logger.info("공고 파일 %d개 / 이벤트 파일 %d개 확인 완료", len(job_files), len(ev_files))


# ── 메인 ─────────────────────────────────────────────────────
def run_train(spark: SparkSession) -> None:
    with step("공고 특성 로드"):
        job_df = load_job_features(spark)

    with step("이벤트 통계 로드"):
        stats_df = load_event_stats(spark)

    with step("피처 조인"):
        df = build_features(job_df, stats_df)

    with step("모델 학습 (GBT)"):
        model = train_model(df)

    with step("인기도 예측 + 저장"):
        predict_and_save(model, df)


def run_predict(spark: SparkSession) -> None:
    """저장된 모델 재사용"""
    if not os.path.exists(MODEL_DIR):
        logger.critical("저장된 모델 없음 — --step train 먼저 실행하세요")
        sys.exit(1)

    with step("공고 특성 로드"):
        job_df = load_job_features(spark)

    with step("이벤트 통계 로드"):
        stats_df = load_event_stats(spark)

    with step("피처 조인"):
        df = build_features(job_df, stats_df)

    with step("모델 로드 + 예측"):
        model = PipelineModel.load(MODEL_DIR)
        predict_and_save(model, df)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="공고 인기도 예측 모델")
    parser.add_argument(
        "--step",
        type=str,
        default="all",
        choices=["all", "train", "predict", "validate"],
        help="all=학습+예측 / train=학습만 / predict=예측만 / validate=파일확인",
    )
    args = parser.parse_args()

    if args.step == "validate":
        validate_input()
        sys.exit(0)

    logger.info("========== 공고 인기도 예측 시작 (step=%s) ==========", args.step)
    t0    = time.perf_counter()
    spark = None
    try:
        spark = create_spark()
        if args.step in ("all", "train"):
            run_train(spark)
        else:
            run_predict(spark)
        logger.info("========== 완료 | %.2f초 ==========", time.perf_counter() - t0)
    except FileNotFoundError as e:
        logger.critical("파일 없음: %s", e)
        sys.exit(1)
    except Exception as e:
        logger.exception("실패: %s", e)
        sys.exit(1)
    finally:
        if spark:
            spark.stop()
