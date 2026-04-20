"""데이터 엔지니어/분석 채용공고 더미 생성기"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from generator_base import CategoryConfig, run_category_generator, parse_args

CONFIG = CategoryConfig(
    name="data",
    display_name="데이터",
    search_queries=[
        "사람인 데이터 엔지니어 분석가 채용공고 2026 Spark Kafka",
        "원티드 데이터 엔지니어 채용 2026 Airflow dbt",
        "2026 데이터 엔지니어 분석가 인기 기술스택 트렌드",
        "2026 데이터 직군 연봉 복지 BI 분석",
    ],
    job_titles=[
        "데이터 엔지니어",
        "데이터 분석가",
        "BI 엔지니어",
        "데이터 플랫폼 엔지니어",
        "빅데이터 엔지니어",
        "데이터 웨어하우스 엔지니어",
        "Analytics 엔지니어",
        "데이터 아키텍트",
    ],
    skills_hint=[
        "Python", "SQL", "Apache Spark", "Apache Kafka", "Airflow",
        "dbt", "Hadoop", "Hive", "Presto", "Trino",
        "AWS Redshift", "BigQuery", "Snowflake", "Databricks",
        "Tableau", "Superset", "Metabase", "Pandas", "Spark SQL",
        "ETL", "ELT", "데이터 파이프라인", "데이터 모델링",
    ],
)


if __name__ == "__main__":
    args = parse_args("데이터 더미 채용공고 생성")
    run_category_generator(CONFIG, count=args.count, use_cache=args.use_cache)
