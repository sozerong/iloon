"""프론트엔드 채용공고 더미 생성기"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from generator_base import CategoryConfig, run_category_generator, parse_args

CONFIG = CategoryConfig(
    name="frontend",
    display_name="프론트엔드",
    search_queries=[
        "사람인 프론트엔드 개발자 채용공고 2026 React Vue Next.js",
        "원티드 프론트엔드 채용 2026 TypeScript",
        "2026 프론트엔드 인기 기술스택 트렌드 React Next.js TypeScript",
        "2026 프론트엔드 개발자 연봉 복지",
    ],
    job_titles=[
        "React 프론트엔드 개발자",
        "Vue.js 프론트엔드 개발자",
        "Next.js 풀스택 개발자",
        "TypeScript 프론트엔드 엔지니어",
        "UI/UX 프론트엔드 개발자",
        "웹 퍼블리셔/프론트엔드 개발자",
        "Angular 프론트엔드 개발자",
    ],
    skills_hint=[
        "React", "Next.js", "Vue.js", "Nuxt.js", "TypeScript",
        "JavaScript", "HTML5", "CSS3", "Tailwind CSS", "Styled-components",
        "Redux", "Zustand", "Webpack", "Vite", "Jest",
        "Storybook", "Figma", "GraphQL", "REST API",
    ],
)


if __name__ == "__main__":
    args = parse_args("프론트엔드 더미 채용공고 생성")
    run_category_generator(CONFIG, count=args.count, use_cache=args.use_cache)
