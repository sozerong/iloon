"""
PySpark MLlib K-Means 사용자 세그멘테이션 분석

실행 예시:
    python analyze_user_segmentation.py --step all
    python analyze_user_segmentation.py --step validate
    python analyze_user_segmentation.py --step 1
    python analyze_user_segmentation.py --step 1 --k 5
"""
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
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans

# ── 경로 설정 (환경변수 우선, 기본값 fallback) ────────────────
BASE_DIR   = os.environ.get("ANALYSIS_BASE_DIR", "C:/GitHub/new_git/money")
LOGS_DIR   = os.environ.get("ANALYSIS_LOGS_DIR",   os.path.join(BASE_DIR, "logs"))
OUTPUT_DIR = os.environ.get("ANALYSIS_OUTPUT_DIR", os.path.join(BASE_DIR, "results"))
APPLOG_DIR = os.environ.get("ANALYSIS_APPLOG_DIR", os.path.join(BASE_DIR, "applogs"))

for d in (OUTPUT_DIR, APPLOG_DIR):
    os.makedirs(d, exist_ok=True)

# ── PostgreSQL 환경변수 ───────────────────────────────────────
PG_HOST     = os.environ.get("PG_HOST",     "localhost")
PG_PORT     = os.environ.get("PG_PORT",     "5432")
PG_USER     = os.environ.get("PG_USER",     "airflow")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "airflow")
PG_JOB_DB   = os.environ.get("PG_JOB_DB",  "iloon_jobs")


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


def create_spark() -> SparkSession:
    logger.info("SparkSession 초기화 중...")
    try:
        spark = (
            SparkSession.builder
            .master("local[*]")
            .appName("user_segmentation")
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


# ── 피처 집계 ─────────────────────────────────────────────────
def build_user_features(df: DataFrame) -> DataFrame:
    """사용자별 피처 집계"""
    # view, bookmark, apply 이벤트 분리 집계
    view_df = (
        df.filter(F.col("event_type") == "job_detail_view")
        .groupBy("user_id")
        .agg(
            F.count("*").alias("view_count"),
            F.avg(F.col("session_duration").cast("double")).alias("avg_session_duration"),
            (
                F.sum(F.when(F.col("is_ai_recommended") == True, 1).otherwise(0))
                / F.count("*")
            ).alias("ai_view_ratio"),
        )
    )

    bookmark_df = (
        df.filter(F.col("event_type") == "bookmark")
        .groupBy("user_id")
        .agg(F.count("*").alias("bookmark_count"))
    )

    apply_df = (
        df.filter(F.col("event_type") == "apply_click")
        .groupBy("user_id")
        .agg(F.count("*").alias("apply_count"))
    )

    # 조인
    user_df = (
        view_df
        .join(bookmark_df, "user_id", "left")
        .join(apply_df,    "user_id", "left")
        .fillna(0, subset=["bookmark_count", "apply_count"])
        .withColumn(
            "apply_rate",
            F.when(F.col("view_count") > 0, F.col("apply_count") / F.col("view_count"))
             .otherwise(0.0),
        )
        .withColumn(
            "bookmark_rate",
            F.when(F.col("view_count") > 0, F.col("bookmark_count") / F.col("view_count"))
             .otherwise(0.0),
        )
        .fillna(0.0, subset=["avg_session_duration", "ai_view_ratio"])
    )

    logger.info("피처 집계 완료. 사용자 수: %d", user_df.count())
    return user_df


# ── K-Means 클러스터링 ────────────────────────────────────────
FEATURE_COLS = [
    "view_count",
    "bookmark_count",
    "apply_count",
    "apply_rate",
    "bookmark_rate",
    "ai_view_ratio",
    "avg_session_duration",
]


def run_kmeans(user_df: DataFrame, k: int) -> DataFrame:
    """VectorAssembler → StandardScaler → KMeans → cluster_id 컬럼 추가"""
    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="raw_features")
    assembled = assembler.transform(user_df)

    scaler = StandardScaler(
        inputCol="raw_features", outputCol="features",
        withMean=True, withStd=True,
    )
    scaler_model = scaler.fit(assembled)
    scaled = scaler_model.transform(assembled)

    kmeans = KMeans(k=k, maxIter=20, seed=42, featuresCol="features", predictionCol="cluster_id")
    km_model = kmeans.fit(scaled)
    result = km_model.transform(scaled)

    logger.info("K-Means 완료 (k=%d)", k)
    return result.select(
        "user_id",
        "cluster_id",
        "view_count",
        "bookmark_count",
        "apply_count",
        "apply_rate",
        "bookmark_rate",
        "ai_view_ratio",
        "avg_session_duration",
    )


# ── 클러스터 이름 자동 결정 ───────────────────────────────────
def assign_segment_names(cluster_stats: list[dict]) -> dict[int, str]:
    """
    cluster_stats: [{"cluster_id": int, "avg_apply_rate": float,
                      "avg_ai_view_ratio": float, "avg_view_count": float}, ...]

    우선순위:
      1. apply_rate 최고  → 적극_지원형
      2. ai_view_ratio 최고 → AI_선호형
      3. view_count 최고 + apply_rate 낮음 → 탐색형
      4. 나머지 → 소극형
    동일 클러스터에 여러 라벨이 붙으면 apply_rate 기준 우선.
    """
    sorted_by_apply  = sorted(cluster_stats, key=lambda x: x["avg_apply_rate"],    reverse=True)
    sorted_by_ai     = sorted(cluster_stats, key=lambda x: x["avg_ai_view_ratio"], reverse=True)
    sorted_by_view   = sorted(cluster_stats, key=lambda x: x["avg_view_count"],    reverse=True)

    # 전체 평균 apply_rate (탐색형 판별 기준)
    avg_apply_global = sum(s["avg_apply_rate"] for s in cluster_stats) / len(cluster_stats)

    # 후보 결정 (cluster_id → 라벨 후보 list)
    candidates: dict[int, list[tuple[int, str]]] = {}  # cluster_id → [(priority, label)]

    def add_candidate(cid: int, priority: int, label: str):
        candidates.setdefault(cid, []).append((priority, label))

    # 1순위: apply_rate 최고
    add_candidate(sorted_by_apply[0]["cluster_id"], 1, "적극_지원형")

    # 2순위: ai_view_ratio 최고
    add_candidate(sorted_by_ai[0]["cluster_id"], 2, "AI_선호형")

    # 3순위: view_count 최고 + apply_rate 낮음
    top_view = sorted_by_view[0]
    if top_view["avg_apply_rate"] < avg_apply_global:
        add_candidate(top_view["cluster_id"], 3, "탐색형")

    # 각 클러스터에 우선순위 높은 라벨 할당
    assigned: dict[int, str] = {}
    for cid, label_list in candidates.items():
        label_list.sort(key=lambda x: x[0])  # 낮을수록 우선
        assigned[cid] = label_list[0][1]

    # 나머지 → 소극형
    for s in cluster_stats:
        if s["cluster_id"] not in assigned:
            assigned[s["cluster_id"]] = "소극형"

    return assigned


# ── PostgreSQL 저장 ───────────────────────────────────────────
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS user_segments (
    user_id              TEXT,
    cluster_id           INT,
    segment_name         TEXT,
    view_count           BIGINT,
    bookmark_count       BIGINT,
    apply_count          BIGINT,
    apply_rate           FLOAT,
    bookmark_rate        FLOAT,
    ai_view_ratio        FLOAT,
    avg_session_duration FLOAT,
    analyzed_at          TIMESTAMP DEFAULT NOW()
)
"""

INSERT_SQL = """
INSERT INTO user_segments (
    user_id, cluster_id, segment_name,
    view_count, bookmark_count, apply_count,
    apply_rate, bookmark_rate, ai_view_ratio,
    avg_session_duration
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


def save_to_postgres(records: list[dict]) -> None:
    """psycopg2로 user_segments 테이블에 저장. 실패 시 WARNING만 남기고 계속."""
    try:
        import psycopg2
    except ImportError:
        logger.warning("psycopg2 미설치 — PostgreSQL 저장 건너뜀")
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
            cur.execute("DELETE FROM user_segments")
            rows = [
                (
                    r["user_id"],
                    r["cluster_id"],
                    r["segment_name"],
                    r["view_count"],
                    r["bookmark_count"],
                    r["apply_count"],
                    r["apply_rate"],
                    r["bookmark_rate"],
                    r["ai_view_ratio"],
                    r["avg_session_duration"],
                )
                for r in records
            ]
            cur.executemany(INSERT_SQL, rows)
        conn.commit()
        logger.info("PostgreSQL 저장 완료: %d건 → iloon_jobs.user_segments", len(rows))
    except Exception as e:
        logger.warning("PostgreSQL 저장 실패 (계속 진행): %s", e)
    finally:
        if conn:
            conn.close()


# ── JSON 저장 ─────────────────────────────────────────────────
def save_json(records: list[dict], filename: str) -> None:
    path = os.path.join(OUTPUT_DIR, filename)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2, default=str)
        logger.info("JSON 저장 완료: %s (%d건)", path, len(records))
    except OSError as e:
        logger.error("파일 저장 실패 [%s]: %s", path, e)
        raise


# ── 메인 분석 단계 ────────────────────────────────────────────
def run_segmentation(k: int) -> None:
    """피처 집계 → K-Means 클러스터링 → 이름 할당 → 저장"""
    spark = None
    try:
        spark = create_spark()

        with step("데이터 로드"):
            df = load_data(spark)

        with step("피처 집계"):
            user_df = build_user_features(df)

        with step(f"K-Means 클러스터링 (k={k})"):
            clustered = run_kmeans(user_df, k)

        with step("클러스터 이름 결정"):
            # 클러스터별 통계 수집
            stats_df = (
                clustered.groupBy("cluster_id")
                .agg(
                    F.count("*").alias("user_count"),
                    F.avg("apply_rate").alias("avg_apply_rate"),
                    F.avg("ai_view_ratio").alias("avg_ai_view_ratio"),
                    F.avg("view_count").alias("avg_view_count"),
                )
                .collect()
            )
            cluster_stats = [
                {
                    "cluster_id":       row["cluster_id"],
                    "user_count":       row["user_count"],
                    "avg_apply_rate":   row["avg_apply_rate"],
                    "avg_ai_view_ratio": row["avg_ai_view_ratio"],
                    "avg_view_count":   row["avg_view_count"],
                }
                for row in stats_df
            ]
            segment_map = assign_segment_names(cluster_stats)
            logger.info("세그먼트 이름 매핑: %s", segment_map)

        with step("결과 수집 및 저장"):
            # segment_name 컬럼 추가 (Python UDF 대신 Spark when/otherwise 사용)
            from functools import reduce
            from pyspark.sql import Column

            seg_col = reduce(
                lambda acc, item: F.when(F.col("cluster_id") == item[0], item[1]).otherwise(acc),
                segment_map.items(),
                F.lit("소극형"),
            )
            final_df = clustered.withColumn("segment_name", seg_col)

            records = [row.asDict() for row in final_df.collect()]
            # Python int/float 변환 (JSON 직렬화 안전)
            for r in records:
                r["cluster_id"]           = int(r["cluster_id"])
                r["view_count"]           = int(r["view_count"])
                r["bookmark_count"]       = int(r["bookmark_count"])
                r["apply_count"]          = int(r["apply_count"])
                r["apply_rate"]           = float(r["apply_rate"])
                r["bookmark_rate"]        = float(r["bookmark_rate"])
                r["ai_view_ratio"]        = float(r["ai_view_ratio"])
                r["avg_session_duration"] = float(r["avg_session_duration"])

            save_json(records, "user_segments.json")
            save_to_postgres(records)

        # ── 결과 요약 출력 ────────────────────────────────────
        print("\n== 사용자 세그멘테이션 결과 ==")
        for s in sorted(cluster_stats, key=lambda x: x["cluster_id"]):
            cid  = s["cluster_id"]
            name = segment_map.get(cid, "소극형")
            n    = s["user_count"]
            ar   = s["avg_apply_rate"] * 100
            ai   = s["avg_ai_view_ratio"] * 100
            print(
                f"클러스터 {cid} ({name}): {n}명 "
                f"| avg_apply_rate={ar:.1f}% "
                f"| avg_ai_ratio={ai:.1f}%"
            )
        print()

    except FileNotFoundError as e:
        logger.critical("로그 파일 없음: %s", e)
        sys.exit(1)
    except Exception as e:
        logger.critical("세그멘테이션 실패: %s", e)
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
    parser = argparse.ArgumentParser(description="K-Means 사용자 세그멘테이션 분석")
    parser.add_argument(
        "--step",
        type=str,
        default="all",
        choices=["all", "validate", "1"],
        help="실행할 단계 (all=전체, validate=파일확인, 1=피처집계+클러스터링+저장)",
    )
    parser.add_argument(
        "--k",
        type=int,
        default=4,
        help="K-Means 클러스터 수 (기본: 4)",
    )
    args = parser.parse_args()

    if args.step in ("all", "1"):
        run_segmentation(k=args.k)
    elif args.step == "validate":
        validate_input()
