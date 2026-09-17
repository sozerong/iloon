"""기획/PM 채용공고 더미 생성기"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from generator_base import CategoryConfig, run_category_generator, parse_args

CONFIG = CategoryConfig(
    name="pm",
    display_name="기획/PM",
    search_queries=[
        "사람인 서비스 기획자 IT PM 채용공고 2026",
        "원티드 프로덕트 매니저 PO 채용 2026",
        "2026 IT 기획 PM PO 인기 역량 트렌드",
        "2026 서비스 기획자 PM 연봉 복지 애자일",
    ],
    job_titles=[
        "서비스 기획자",
        "IT 프로젝트 매니저 (PM)",
        "프로덕트 오너 (PO)",
        "프로덕트 매니저",
        "앱 서비스 기획자",
        "플랫폼 기획자",
        "B2B 서비스 기획자",
    ],
    skills_hint=[
        "서비스 기획", "애자일", "스크럼", "Jira", "Confluence",
        "Notion", "Figma", "데이터 분석", "SQL",
        "사용자 리서치", "UI/UX 기획", "PRD 작성",
        "로드맵 수립", "KPI 설정", "A/B 테스트",
        "Amplitude", "Mixpanel", "Google Analytics",
    ],
)


if __name__ == "__main__":
    args = parse_args("기획/PM 더미 채용공고 생성")
    run_category_generator(CONFIG, count=args.count, use_cache=args.use_cache)
