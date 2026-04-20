import argparse
import json
import logging
import logging.handlers
import os
import sys
import glob as pyglob
import time
from contextlib import contextmanager

sys.stdout.reconfigure(encoding="utf-8")

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

# ── 경로 설정 (환경변수 우선, 기본값 fallback) ────────────────
BASE_DIR   = os.environ.get("ANALYSIS_BASE_DIR", "C:/GitHub/new_git/money")
LOGS_DIR   = os.environ.get("ANALYSIS_LOGS_DIR",   os.path.join(BASE_DIR, "logs"))
OUTPUT_DIR = os.environ.get("ANALYSIS_OUTPUT_DIR", os.path.join(BASE_DIR, "results"))
APPLOG_DIR = os.environ.get("ANALYSIS_APPLOG_DIR", os.path.join(BASE_DIR, "applogs"))

for d in (OUTPUT_DIR, APPLOG_DIR):
    os.makedirs(d, exist_ok=True)


# ── 로거 초기화 ───────────────────────────────────────────────
def setup_logger() -> logging.Logger:
    logger = logging.getLogger("spark_analysis")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(fmt)

    file_handler = logging.handlers.TimedRotatingFileHandler(
        filename=os.path.join(APPLOG_DIR, "spark_analysis.log"),
        when="midnight",
        backupCount=7,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(fmt)

    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
    return logger


logger = setup_logger()


# ── 유틸 ─────────────────────────────────────────────────────
@contextmanager
def step(name: str):
    """분석 단계별 실행 시간 측정 + 예외 처리"""
    logger.info("▶ 시작: %s", name)
    t0 = time.perf_counter()
    try:
        yield
        elapsed = time.perf_counter() - t0
        logger.info("✔ 완료: %s (%.2f초)", name, elapsed)
    except AnalysisException as e:
        logger.error("✘ Spark 분석 오류 [%s]: %s", name, e)
        raise
    except Exception as e:
        logger.exception("✘ 예기치 못한 오류 [%s]: %s", name, e)
        raise


def save_json(df: DataFrame, filename: str) -> None:
    path = os.path.join(OUTPUT_DIR, filename)
    try:
        data = [row.asDict() for row in df.collect()]
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info("저장 완료: %s (%d건)", path, len(data))
    except OSError as e:
        logger.error("파일 저장 실패 [%s]: %s", path, e)
        raise
    except Exception as e:
        logger.exception("save_json 오류 [%s]: %s", path, e)
        raise


def create_spark() -> SparkSession:
    logger.info("SparkSession 초기화 중...")
    try:
        spark = (
            SparkSession.builder
            .master("local[*]")
            .appName("ai_vs_normal_analysis")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        logger.info("SparkSession 초기화 완료 (version: %s)", spark.version)
        return spark
    except Exception as e:
        logger.critical("SparkSession 초기화 실패: %s", e)
        raise


def load_data(spark: SparkSession) -> DataFrame:
    # user-events-*.jsonl 만 읽음 (dummy-jobs-*.jsonl 은 analyze_job_trends.py 에서 처리)
    log_files = pyglob.glob(os.path.join(LOGS_DIR, "user-events-*.jsonl"))
    if not log_files:
        raise FileNotFoundError(f"사용자 이벤트 JSONL 없음: {LOGS_DIR}/user-events-*.jsonl")

    logger.info("로그 파일 %d개 로드: %s", len(log_files), LOGS_DIR)
    try:
        df = spark.read.json(log_files)
        count = df.count()
        if count == 0:
            raise ValueError("로드된 데이터가 0건입니다.")
        logger.info("전체 이벤트 수: %s건", f"{count:,}")
        print(f"\n전체 이벤트 수: {count:,}건\n")
        return df
    except AnalysisException as e:
        logger.error("데이터 로드 실패 (AnalysisException): %s", e)
        raise
    except Exception as e:
        logger.exception("데이터 로드 중 오류: %s", e)
        raise


# ── 분석 함수 ─────────────────────────────────────────────────
def analysis_1(df: DataFrame) -> None:
    print("=" * 55)
    print("분석 1. AI 추천 vs 일반 공고 전환율 비교")
    print("=" * 55)

    action_df = df.filter(
        F.col("event_type").isin(["job_detail_view", "bookmark", "apply_click"])
    )

    result1 = (
        action_df.groupBy("is_ai_recommended", "event_type")
        .agg(F.count("*").alias("count"))
        .orderBy("is_ai_recommended", "event_type")
    )
    result1.show()

    pivot = (
        action_df.groupBy("is_ai_recommended")
        .pivot("event_type", ["job_detail_view", "bookmark", "apply_click"])
        .count()
        .withColumnRenamed("job_detail_view", "클릭수")
        .withColumnRenamed("bookmark", "저장수")
        .withColumnRenamed("apply_click", "지원수")
        .withColumn("저장률(%)", F.round(F.col("저장수") / F.col("클릭수") * 100, 1))
        .withColumn("지원률(%)", F.round(F.col("지원수") / F.col("클릭수") * 100, 1))
        .withColumn(
            "추천여부",
            F.when(F.col("is_ai_recommended") == True, "AI 추천")
             .when(F.col("is_ai_recommended") == False, "일반 공고")
             .otherwise("미분류"),
        )
        .select("추천여부", "클릭수", "저장수", "저장률(%)", "지원수", "지원률(%)")
    )

    print("\n[ AI 추천 vs 일반 공고 전환율 ]")
    pivot.show()
    save_json(pivot, "analysis1_ai_vs_normal_conversion.json")


def analysis_2(df: DataFrame) -> None:
    print("=" * 55)
    print("분석 2. 지역별 AI 추천 클릭 vs 지원 전환율")
    print("=" * 55)

    region_df = df.filter(
        F.col("event_type").isin(["job_detail_view", "apply_click"])
        & (F.col("is_ai_recommended") == True)
    )

    result2 = (
        region_df.groupBy("region_sido")
        .pivot("event_type", ["job_detail_view", "apply_click"])
        .count()
        .withColumnRenamed("job_detail_view", "AI클릭수")
        .withColumnRenamed("apply_click", "AI지원수")
        .withColumn(
            "AI지원전환율(%)",
            F.round(F.col("AI지원수") / F.col("AI클릭수") * 100, 1),
        )
        .orderBy(F.desc("AI지원전환율(%)"))
    )

    print("\n[ 지역별 AI 추천 지원 전환율 TOP ]")
    result2.show()
    save_json(result2, "analysis2_region_conversion.json")


def analysis_3(df: DataFrame) -> None:
    print("=" * 55)
    print("분석 3. AI 추천 vs 일반 — 평균 체류 시간 비교")
    print("=" * 55)

    dwell_df = df.filter(
        (F.col("event_type") == "job_detail_view")
        & F.col("time_on_page_sec").isNotNull()
    )

    result3 = (
        dwell_df.groupBy("is_ai_recommended")
        .agg(
            F.count("*").alias("클릭수"),
            F.round(F.avg("time_on_page_sec"), 1).alias("평균체류시간(초)"),
            F.round(F.min("time_on_page_sec"), 1).alias("최소(초)"),
            F.round(F.max("time_on_page_sec"), 1).alias("최대(초)"),
        )
        .withColumn(
            "추천여부",
            F.when(F.col("is_ai_recommended") == True, "AI 추천").otherwise("일반 공고"),
        )
        .select("추천여부", "클릭수", "평균체류시간(초)", "최소(초)", "최대(초)")
    )

    print("\n[ AI 추천 vs 일반 공고 체류 시간 ]")
    result3.show()
    save_json(result3, "analysis3_dwell_time.json")


def analysis_4(df: DataFrame) -> None:
    print("=" * 55)
    print("분석 4. AI 매칭 점수 구간별 지원 전환율")
    print("=" * 55)

    click_df = df.filter(
        (F.col("event_type") == "job_detail_view")
        & (F.col("is_ai_recommended") == True)
    ).select("session_id", "job_id", "match_score")

    apply_df = df.filter(
        (F.col("event_type") == "apply_click")
        & (F.col("is_ai_recommended") == True)
    ).select("session_id", "job_id", F.lit(1).alias("applied"))

    joined = (
        click_df.join(apply_df, ["session_id", "job_id"], "left")
        .withColumn("applied", F.coalesce(F.col("applied"), F.lit(0)))
        .withColumn(
            "score_bucket",
            F.concat(
                (F.floor(F.col("match_score") * 10) / 10).cast("string"),
                F.lit("~"),
                ((F.floor(F.col("match_score") * 10) / 10) + 0.1).cast("string"),
            ),
        )
    )

    result4 = (
        joined.groupBy("score_bucket")
        .agg(
            F.count("*").alias("클릭수"),
            F.sum("applied").alias("지원수"),
            F.round(F.avg("applied") * 100, 1).alias("지원전환율(%)"),
        )
        .orderBy("score_bucket")
    )

    print("\n[ 매칭 점수 구간별 지원 전환율 ]")
    result4.show()
    save_json(result4, "analysis4_match_score_conversion.json")


def analysis_5(df: DataFrame) -> None:
    print("=" * 55)
    print("분석 5. 일별 AI 추천 지원 전환율 트렌드")
    print("=" * 55)

    trend_df = df.filter(
        F.col("event_type").isin(["job_detail_view", "apply_click"])
    ).withColumn("date", F.substring("timestamp", 1, 10))

    result5 = (
        trend_df.groupBy("date", "is_ai_recommended")
        .pivot("event_type", ["job_detail_view", "apply_click"])
        .count()
        .withColumn(
            "지원전환율(%)",
            F.round(F.col("apply_click") / F.col("job_detail_view") * 100, 1),
        )
        .withColumn(
            "추천여부",
            F.when(F.col("is_ai_recommended") == True, "AI추천").otherwise("일반"),
        )
        .select(
            "date", "추천여부",
            F.col("job_detail_view").alias("클릭"),
            F.col("apply_click").alias("지원"),
            "지원전환율(%)",
        )
        .orderBy("date", "추천여부")
    )

    print("\n[ 일별 AI vs 일반 지원 전환율 트렌드 ]")
    result5.show(20)
    save_json(result5, "analysis5_daily_trend.json")


# ── 메인 ─────────────────────────────────────────────────────
STEP_MAP = {
    1: ("분석 1. AI vs 일반 전환율", analysis_1),
    2: ("분석 2. 지역별 전환율",     analysis_2),
    3: ("분석 3. 체류 시간 비교",    analysis_3),
    4: ("분석 4. 매칭 점수 전환율",  analysis_4),
    5: ("분석 5. 일별 트렌드",       analysis_5),
}


def run_step(step_no: int) -> None:
    """단일 분석 단계 실행 — Airflow 태스크 단위"""
    if step_no not in STEP_MAP:
        raise ValueError(f"유효하지 않은 step: {step_no} (1~5 사용)")

    name, fn = STEP_MAP[step_no]
    logger.info("========== [step %d] %s 시작 ==========", step_no, name)
    t_total = time.perf_counter()
    spark = None
    try:
        spark = create_spark()
        with step(name):
            df = load_data(spark)
            fn(df)
        logger.info("========== [step %d] 완료 | %.2f초 ==========",
                    step_no, time.perf_counter() - t_total)
    except FileNotFoundError as e:
        logger.critical("[step %d] 로그 파일 없음: %s", step_no, e)
        sys.exit(1)
    except Exception as e:
        logger.critical("[step %d] 실패: %s", step_no, e)
        sys.exit(1)
    finally:
        if spark:
            spark.stop()
            logger.info("[step %d] SparkSession 종료", step_no)


def run_all() -> None:
    """전체 분석 순차 실행 — 로컬 단독 실행용"""
    logger.info("========== 전체 분석 시작 ==========")
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
                logger.warning("'%s' 실패 — 나머지 계속 진행", name)

        elapsed = time.perf_counter() - t_total
        if failed:
            logger.warning("========== 완료 (일부 실패) | %.2f초 ==========", elapsed)
            logger.warning("실패 항목: %s", ", ".join(failed))
        else:
            logger.info("========== 완료 | 총 %.2f초 ==========", elapsed)
        print("\n분석 완료")

    except FileNotFoundError as e:
        logger.critical("로그 파일 없음: %s", e)
        sys.exit(1)
    except Exception as e:
        logger.critical("분석 중단: %s", e)
        sys.exit(1)
    finally:
        if spark:
            spark.stop()
            logger.info("SparkSession 종료")


def validate_input() -> None:
    """사용자 이벤트 파일 존재 여부 확인 — Spark 없이 실행"""
    pattern = os.path.join(LOGS_DIR, "user-events-*.jsonl")
    files   = pyglob.glob(pattern)
    if not files:
        logger.critical("사용자 이벤트 JSONL 없음: %s", pattern)
        sys.exit(1)
    logger.info("이벤트 파일 확인 완료: %d개", len(files))
    for f in files:
        logger.debug("  - %s", f)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AI vs 일반 공고 분석")
    parser.add_argument(
        "--step",
        type=str,
        default="all",
        choices=["all", "validate", "1", "2", "3", "4", "5"],
        help="실행할 단계 (all=전체, validate=파일확인, 1~5=개별 분석)",
    )
    args = parser.parse_args()

    if args.step == "all":
        run_all()
    elif args.step == "validate":
        validate_input()
    else:
        run_step(int(args.step))
