"""게임 채용공고 더미 생성기"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from generator_base import CategoryConfig, run_category_generator, parse_args

CONFIG = CategoryConfig(
    name="game",
    display_name="게임",
    search_queries=[
        "사람인 게임 개발자 채용공고 2026 Unity Unreal",
        "원티드 게임 클라이언트 서버 개발자 채용 2026",
        "2026 게임 개발자 인기 기술스택 Unity Unreal C++",
        "2026 게임 개발자 연봉 복지",
    ],
    job_titles=[
        "Unity 게임 클라이언트 개발자",
        "Unreal Engine 게임 개발자",
        "게임 서버 개발자 (C++)",
        "게임 서버 개발자 (Go/Java)",
        "게임플레이 엔지니어",
        "게임 엔진 프로그래머",
        "게임 툴 개발자",
        "라이브 게임 백엔드 개발자",
    ],
    skills_hint=[
        "Unity", "C#", "Unreal Engine", "C++", "Blueprint",
        "Go", "Java", "Node.js", "Python",
        "게임 서버", "멀티플레이어 네트워킹", "물리 엔진",
        "셰이더", "HLSL", "GLSL", "렌더링 파이프라인",
        "Redis", "MongoDB", "AWS", "게임 최적화",
    ],
)


if __name__ == "__main__":
    args = parse_args("게임 더미 채용공고 생성")
    run_category_generator(CONFIG, count=args.count, use_cache=args.use_cache)
