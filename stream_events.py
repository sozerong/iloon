"""
Spark Structured Streaming — Kafka → PostgreSQL 실시간 집계

실행 방법:
    spark-submit \\
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \\
      stream_events.py

옵션:
    --watermark  지연 데이터 허용 시간 (기본: 60s)
"""
import argparse
import logging
import logging.handlers
import os
import sys
import time

sys.stdout.reconfigure(encoding="utf-8")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, BooleanType, IntegerType,
)

# ── 경로 설정 ─────────────────────────────────────────────────
BASE_DIR   = os.environ.get("ANALYSIS_BASE_DIR", "C:/GitHub/new_git/money")
APPLOG_DIR = os.environ.get("ANALYSIS_APPLOG_DIR", os.path.join(BASE_DIR, "applogs"))

os.makedirs(APPLOG_DIR, exist_ok=True)

# ── Kafka 설정 ────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
KAFKA_TOPIC             = "user-events"

# ── PostgreSQL 환경변수 ───────────────────────────────────────
PG_HOST     = os.environ.get("PG_HOST",     "localhost")
PG_PORT     = os.environ.get("PG_PORT",     "5432")
PG_USER     = os.environ.get("PG_USER",     "airflow")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "airflow")
PG_JOB_DB   = os.environ.get("PG_JOB_DB",  "iloon_jobs")


# ── 로거 초기화 ───────────────────────────────────────────────
def setup_logger() -> logging.Logger:
    logger = logging.getLogger("spark_streaming")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(fmt)

    file_handler = logging.handlers.TimedRotatingFileHandler(
        filename=os.path.join(APPLOG_DIR, "spark_streaming.log"),
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


# ── PostgreSQL 테이블 생성 SQL ─────────────────────────────────
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS realtime_event_stats (
    window_start      TIMESTAMP,
    window_end        TIMESTAMP,
    event_type        TEXT,
    is_ai_recommended BOOLEAN,
    event_count       BIGINT,
    created_at        TIMESTAMP DEFAULT NOW()
)
"""

INSERT_SQL = """
INSERT INTO realtime_event_stats (
    window_start, window_end, event_type, is_ai_recommended, event_count
) VALUES (%s, %s, %s, %s, %s)
"""


# ── foreachBatch 핸들러 ───────────────────────────────────────
def write_to_postgres(batch_df, batch_id: int) -> None:
    """각 마이크로배치를 PostgreSQL realtime_event_stats 테이블에 저장."""
    try:
        import psycopg2
    except ImportError:
        logger.warning("psycopg2 미설치 — PostgreSQL 저장 건너뜀 (batch_id=%d)", batch_id)
        return

    rows = batch_df.collect()
    if not rows:
        logger.debug("배치 %d: 데이터 없음", batch_id)
        return

    conn = None
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=int(PG_PORT),
            user=PG_USER,
            password=PG_PASSWORD,
            dbname=PG_JOB_DB,
        )
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
            records = [
                (
                    row["window_start"],
                    row["window_end"],
                    row["event_type"],
                    row["is_ai_recommended"],
                    row["event_count"],
                )
                for row in rows
            ]
            cur.executemany(INSERT_SQL, records)
        conn.commit()
        logger.info(
            "PostgreSQL 저장 완료: batch_id=%d, %d행 → realtime_event_stats",
            batch_id, len(records),
        )
    except Exception as e:
        logger.warning("PostgreSQL 저장 실패 (batch_id=%d, 계속 진행): %s", batch_id, e)
    finally:
        if conn:
            conn.close()


# ── SparkSession ──────────────────────────────────────────────
def create_spark() -> SparkSession:
    logger.info("SparkSession 초기화 중...")
    try:
        spark = (
            SparkSession.builder
            .master("local[*]")
            .appName("stream_events")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        logger.info("SparkSession 초기화 완료 (version: %s)", spark.version)
        return spark
    except Exception as e:
        logger.critical("SparkSession 초기화 실패: %s", e)
        raise


# ── 이벤트 스키마 ─────────────────────────────────────────────
EVENT_SCHEMA = StructType([
    StructField("event_type",        StringType(),  True),
    StructField("user_id",           StringType(),  True),
    StructField("job_id",            StringType(),  True),
    StructField("is_ai_recommended", BooleanType(), True),
    StructField("session_duration",  IntegerType(), True),
    StructField("timestamp",         StringType(),  True),
])


# ── 스트리밍 메인 ─────────────────────────────────────────────
def run_streaming(watermark: str) -> None:
    """
    Kafka → JSON 파싱 → 30초 tumbling window 집계
    → foreachBatch(PostgreSQL) + 콘솔 동시 출력
    """
    spark = create_spark()

    # Kafka 소스
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    # JSON 파싱
    parsed_df = (
        raw_df
        .select(
            F.from_json(
                F.col("value").cast("string"),
                EVENT_SCHEMA,
            ).alias("data")
        )
        .select("data.*")
        .withColumn(
            "event_time",
            F.to_timestamp(F.col("timestamp")),
        )
        .withWatermark("event_time", watermark)
    )

    # 30초 tumbling window 집계
    agg_df = (
        parsed_df
        .groupBy(
            F.window(F.col("event_time"), "30 seconds").alias("window"),
            F.col("event_type"),
            F.col("is_ai_recommended"),
        )
        .agg(F.count("*").alias("event_count"))
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("event_type"),
            F.col("is_ai_recommended"),
            F.col("event_count"),
        )
    )

    # ── 출력 1: PostgreSQL (foreachBatch) ─────────────────────
    pg_query = (
        agg_df.writeStream
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .foreachBatch(write_to_postgres)
        .option("checkpointLocation", os.path.join(BASE_DIR, "checkpoints", "pg_stream"))
        .start()
    )

    # ── 출력 2: 콘솔 동시 출력 ───────────────────────────────
    console_query = (
        agg_df.writeStream
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", os.path.join(BASE_DIR, "checkpoints", "console_stream"))
        .start()
    )

    logger.info(
        "스트리밍 시작 — Kafka: %s, 토픽: %s, watermark: %s",
        KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, watermark,
    )
    logger.info("종료하려면 Ctrl+C 를 누르세요.")

    try:
        # 두 쿼리 모두 awaitTermination
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.info("사용자 중단 요청 — 스트리밍 종료 중...")
    finally:
        for q in spark.streams.active:
            q.stop()
        spark.stop()
        logger.info("SparkSession 종료")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka → PostgreSQL Structured Streaming")
    parser.add_argument(
        "--watermark",
        type=str,
        default="60s",
        help="지연 데이터 허용 watermark (기본: 60s, 예: 30s, 2m)",
    )
    args = parser.parse_args()

    # '60s' → '60 seconds' 변환 (Spark watermark 형식)
    wm = args.watermark.strip()
    if wm.endswith("s") and not wm.endswith("seconds"):
        wm = wm[:-1] + " seconds"
    elif wm.endswith("m") and not wm.endswith("minutes"):
        wm = wm[:-1] + " minutes"

    run_streaming(watermark=wm)
