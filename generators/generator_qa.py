"""QA/테스트 채용공고 더미 생성기"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from generator_base import CategoryConfig, run_category_generator, parse_args

CONFIG = CategoryConfig(
    name="qa",
    display_name="QA/테스트",
    search_queries=[
        "사람인 QA 테스트 엔지니어 채용공고 2026 자동화",
        "원티드 QA 자동화 테스트 엔지니어 채용 2026 Selenium",
        "2026 QA 엔지니어 인기 기술스택 트렌드 자동화 성능테스트",
        "2026 QA 테스트 엔지니어 연봉 복지",
    ],
    job_titles=[
        "QA 엔지니어",
        "SDET (테스트 자동화 엔지니어)",
        "테스트 자동화 개발자",
        "성능 테스트 엔지니어",
        "QA 리드",
        "모바일 QA 엔지니어",
        "API 테스트 엔지니어",
    ],
    skills_hint=[
        "Python", "Java", "Selenium", "Playwright", "Cypress",
        "Appium", "JMeter", "k6", "Locust", "pytest",
        "JUnit", "TestNG", "Postman", "REST Assured",
        "CI/CD", "Jenkins", "GitHub Actions", "Jira",
        "버그 추적", "테스트 계획", "회귀 테스트", "BDD",
    ],
)


if __name__ == "__main__":
    args = parse_args("QA/테스트 더미 채용공고 생성")
    run_category_generator(CONFIG, count=args.count, use_cache=args.use_cache)
