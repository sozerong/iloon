"""백엔드/서버 채용공고 더미 생성기"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from generator_base import CategoryConfig, run_category_generator, parse_args

CONFIG = CategoryConfig(
    name="backend",
    display_name="백엔드/서버",
    search_queries=[
        "사람인 백엔드 개발자 채용공고 2026 Python Java Node.js",
        "원티드 서버 개발자 채용 2026 Spring FastAPI",
        "2026 백엔드 개발자 인기 기술스택 채용 트렌드 Python Java Go",
        "2026 백엔드 개발자 연봉 복지 신입 경력",
    ],
    job_titles=[
        "Python/FastAPI 백엔드 개발자",
        "Java/Spring Boot 서버 개발자",
        "Node.js 백엔드 개발자",
        "Go 언어 서버 개발자",
        "API 서버 개발자",
        "MSA 백엔드 엔지니어",
        "서버 플랫폼 개발자",
    ],
    skills_hint=[
        "Python", "FastAPI", "Django", "Java", "Spring Boot",
        "Node.js", "Go", "Kotlin", "REST API", "GraphQL",
        "PostgreSQL", "MySQL", "Redis", "Kafka", "Docker",
        "AWS", "MSA", "gRPC", "JWT",
    ],
)


if __name__ == "__main__":
    args = parse_args("백엔드/서버 더미 채용공고 생성")
    run_category_generator(CONFIG, count=args.count, use_cache=args.use_cache)
