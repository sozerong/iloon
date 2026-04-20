"""
채용공고 트렌드 Spark 분석

입력: logs/dummy-jobs-*.jsonl  (카테고리별 공고 JSONL)
출력: results/job_trend_*.json

분석 항목:
  1. 직군별 공고 수 분포
  2. 인기 기술스택 Top 20
  3. 직군별 평균 연봉 범위
  4. 지역별 공고 분포
  5. 경력 유형 × 기업 규모 분포
  6. 일별 공고 등록 트렌드

실행:
  python analyze_job_trends.py --step all
  python analyze_job_trends.py --step validate
  python analyze_job_trends.py --step 1
"""

from __future__ import annotations

import argparse
import json
import logging
import logging.handlers
import os
import sys
import glob as pyglob
import time
from contextlib import contextmanager
from typing import Any, Dict

sys.stdout.reconfigure(encoding="utf-8")

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType
from pyspark.sql.utils import AnalysisException

# ── 경로 설정 ─────────────────────────────────────────────────
BASE_DIR   = os.environ.get("ANALYSIS_BASE_DIR", "C:/GitHub/new_git/money")
LOGS_DIR   = os.environ.get("ANALYSIS_LOGS_DIR",   os.path.join(BASE_DIR, "logs"))
OUTPUT_DIR = os.environ.get("ANALYSIS_OUTPUT_DIR", os.path.join(BASE_DIR, "results"))
APPLOG_DIR = os.environ.get("ANALYSIS_APPLOG_DIR", os.path.join(BASE_DIR, "applogs"))

for _d in (OUTPUT_DIR, APPLOG_DIR):
    os.makedirs(_d, exist_ok=True)


# ── 로거 ─────────────────────────────────────────────────────
def setup_logger() -> logging.Logger:
    logger = logging.getLogger("job_trend_analysis")
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
        os.path.join(APPLOG_DIR, "job_trend_analysis.log"),
        when="midnight", backupCount=7, encoding="utf-8",
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(sh)
    logger.addHandler(fh)
    return logger


logger = setup_logger()


# ── SparkSession ──────────────────────────────────────────────
def create_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("iloon-job-trend-analysis")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
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


# ── 저장 ─────────────────────────────────────────────────────
def save_json(df: DataFrame, filename: str) -> None:
    path = os.path.join(OUTPUT_DIR, filename)
    rows = [row.asDict() for row in df.collect()]
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2, default=str)
    logger.info("저장: %s (%d행)", path, len(rows))


# ── 데이터 로드 ───────────────────────────────────────────────
def load_data(spark: SparkSession) -> DataFrame:
    """logs/dummy-jobs-*.jsonl 만 읽음 (user-events 제외)"""
    pattern = os.path.join(LOGS_DIR, "dummy-jobs-*.jsonl")
    files   = pyglob.glob(pattern)
    if not files:
        raise FileNotFoundError(f"공고 JSONL 없음: {pattern}")

    logger.info("공고 파일 %d개 로드", len(files))
    df = spark.read.json(files)

    # 주요 컬럼 플래트닝 (중첩 구조 → 단순 컬럼)
    df = (
        df
        .withColumn("job_category_mid",
                    F.col("position.job_category.mid"))
        .withColumn("career_type",
                    F.col("position.career.type"))
        .withColumn("company_size",
                    F.col("company.size"))
        .withColumn("region",
                    F.col("work_condition.location.sido"))
        .withColumn("salary_min",
                    F.col("work_condition.salary.min").cast(LongType()))
        .withColumn("salary_max",
                    F.col("work_condition.salary.max").cast(LongType()))
        .withColumn("posted_at",
                    F.col("dates.posted_at"))
    )

    count = df.count()
    logger.info("전체 공고: %s건", f"{count:,}")
    print(f"\n전체 공고 수: {count:,}건\n")
    return df


# ── 분석 1: 직군별 공고 수 ────────────────────────────────────
def trend_1(df: DataFrame) -> None:
    print("=" * 55)
    print("트렌드 1. 직군별 공고 수 분포")
    print("=" * 55)

    result = (
        df.groupBy("job_category_mid")
        .agg(F.count("*").alias("공고수"))
        .orderBy(F.col("공고수").desc())
        .withColumnRenamed("job_category_mid", "직군")
    )
    result.show(20, truncate=False)
    save_json(result, "trend1_job_category_dist.json")


# ── 분석 2: 인기 기술스택 Top 20 ──────────────────────────────
def trend_2(df: DataFrame) -> None:
    print("=" * 55)
    print("트렌드 2. 인기 기술스택 Top 20")
    print("=" * 55)

    # skills 컬럼이 배열 → explode 후 집계
    result = (
        df.select(F.explode("skills").alias("skill"))
        .filter(F.col("skill").isNotNull() & (F.length(F.col("skill")) > 0))
        .groupBy("skill")
        .agg(F.count("*").alias("공고수"))
        .orderBy(F.col("공고수").desc())
        .limit(20)
    )
    result.show(20, truncate=False)
    save_json(result, "trend2_top_skills.json")


# ── 분석 3: 직군별 평균 연봉 ─────────────────────────────────
def trend_3(df: DataFrame) -> None:
    print("=" * 55)
    print("트렌드 3. 직군별 평균 연봉 범위 (만원)")
    print("=" * 55)

    salary_df = df.filter(
        F.col("salary_min").isNotNull() & F.col("salary_max").isNotNull()
        & (F.col("salary_min") > 0) & (F.col("salary_max") > 0)
    )

    result = (
        salary_df
        .groupBy("job_category_mid")
        .agg(
            F.count("*").alias("공고수"),
            F.round(F.avg("salary_min") / 10000, 0).alias("평균최저연봉_만원"),
            F.round(F.avg("salary_max") / 10000, 0).alias("평균최고연봉_만원"),
            F.round(F.avg((F.col("salary_min") + F.col("salary_max")) / 2) / 10000, 0)
             .alias("평균중간연봉_만원"),
        )
        .orderBy(F.col("평균중간연봉_만원").desc())
        .withColumnRenamed("job_category_mid", "직군")
    )
    result.show(20, truncate=False)
    save_json(result, "trend3_salary_by_category.json")


# ── 분석 4: 지역별 공고 분포 ─────────────────────────────────
def trend_4(df: DataFrame) -> None:
    print("=" * 55)
    print("트렌드 4. 지역별 공고 분포")
    print("=" * 55)

    total = df.count()

    result = (
        df.filter(F.col("region").isNotNull())
        .groupBy("region")
        .agg(F.count("*").alias("공고수"))
        .withColumn("비율(%)", F.round(F.col("공고수") / total * 100, 1))
        .orderBy(F.col("공고수").desc())
        .withColumnRenamed("region", "지역")
    )
    result.show(20, truncate=False)
    save_json(result, "trend4_region_dist.json")


# ── 분석 5: 경력 유형 × 기업 규모 ───────────────────────────
def trend_5(df: DataFrame) -> None:
    print("=" * 55)
    print("트렌드 5. 경력 유형 × 기업 규모 분포")
    print("=" * 55)

    result = (
        df.filter(
            F.col("career_type").isNotNull()
            & F.col("company_size").isNotNull()
        )
        .groupBy("career_type", "company_size")
        .agg(F.count("*").alias("공고수"))
        .orderBy("career_type", F.col("공고수").desc())
        .withColumnRenamed("career_type", "경력유형")
        .withColumnRenamed("company_size", "기업규모")
    )
    result.show(30, truncate=False)
    save_json(result, "trend5_career_company_dist.json")


# ── 분석 6: 일별 공고 등록 트렌드 ───────────────────────────
def trend_6(df: DataFrame) -> None:
    print("=" * 55)
    print("트렌드 6. 일별 공고 등록 트렌드")
    print("=" * 55)

    result = (
        df.filter(F.col("posted_at").isNotNull())
        .withColumn("날짜", F.substring(F.col("posted_at"), 1, 10))
        .groupBy("날짜", "job_category_mid")
        .agg(F.count("*").alias("공고수"))
        .orderBy("날짜", F.col("공고수").desc())
        .withColumnRenamed("job_category_mid", "직군")
    )
    result.show(30, truncate=False)
    save_json(result, "trend6_daily_posting_trend.json")


# ── Step 맵 ───────────────────────────────────────────────────
STEP_MAP = {
    1: ("직군별 공고 수",       trend_1),
    2: ("인기 기술스택 Top 20", trend_2),
    3: ("직군별 평균 연봉",     trend_3),
    4: ("지역별 공고 분포",     trend_4),
    5: ("경력 × 기업 규모",    trend_5),
    6: ("일별 공고 트렌드",     trend_6),
}


def run_step(step_no: int) -> None:
    name, fn = STEP_MAP[step_no]
    spark = None
    try:
        spark = create_spark()
        with step(name):
            df = load_data(spark)
            fn(df)
    except FileNotFoundError as e:
        logger.critical("파일 없음: %s", e)
        sys.exit(1)
    except Exception as e:
        logger.exception("[step %d] 실패: %s", step_no, e)
        sys.exit(1)
    finally:
        if spark:
            spark.stop()


def run_all() -> None:
    logger.info("========== 공고 트렌드 전체 분석 시작 ==========")
    t_total = time.perf_counter()
    spark = None
    failed = []

    try:
        spark = create_spark()
        with step("데이터 로드"):
            df = load_data(spark)

        for no, (name, fn) in STEP_MAP.items():
            try:
                with step(name):
                    fn(df)
            except Exception:
                failed.append(name)
                logger.warning("'%s' 실패 — 나머지 계속", name)

        elapsed = time.perf_counter() - t_total
        if failed:
            logger.warning("========== 완료 (일부 실패) | %.2f초 ==========", elapsed)
        else:
            logger.info("========== 완료 | %.2f초 ==========", elapsed)

    except FileNotFoundError as e:
        logger.critical("공고 파일 없음: %s", e)
        sys.exit(1)
    except Exception as e:
        logger.critical("분석 중단: %s", e)
        sys.exit(1)
    finally:
        if spark:
            spark.stop()


def validate_input() -> None:
    """공고 JSONL 존재 여부 확인 — Spark 없이 실행"""
    pattern = os.path.join(LOGS_DIR, "dummy-jobs-*.jsonl")
    files   = pyglob.glob(pattern)
    if not files:
        logger.critical("공고 JSONL 없음: %s", pattern)
        sys.exit(1)
    logger.info("공고 파일 확인 완료: %d개", len(files))
    for f in files:
        logger.debug("  - %s", os.path.basename(f))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="채용공고 트렌드 Spark 분석")
    parser.add_argument(
        "--step",
        type=str,
        default="all",
        choices=["all", "validate", "1", "2", "3", "4", "5", "6"],
        help="실행할 단계 (all=전체, validate=파일확인, 1~6=개별 분석)",
    )
    args = parser.parse_args()

    if args.step == "all":
        run_all()
    elif args.step == "validate":
        validate_input()
    else:
        run_step(int(args.step))
