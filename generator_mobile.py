"""모바일 채용공고 더미 생성기"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from generator_base import CategoryConfig, run_category_generator, parse_args

CONFIG = CategoryConfig(
    name="mobile",
    display_name="모바일",
    search_queries=[
        "사람인 iOS Android 모바일 앱 개발자 채용공고 2026",
        "원티드 Flutter React Native 앱 개발자 채용 2026",
        "2026 모바일 앱 개발자 인기 기술스택 iOS Android Flutter",
        "2026 모바일 개발자 연봉 복지",
    ],
    job_titles=[
        "iOS 개발자 (Swift)",
        "Android 개발자 (Kotlin)",
        "Flutter 크로스플랫폼 개발자",
        "React Native 앱 개발자",
        "모바일 앱 개발자 (iOS/Android)",
        "Kotlin Multiplatform 개발자",
        "모바일 플랫폼 엔지니어",
    ],
    skills_hint=[
        "Swift", "SwiftUI", "Objective-C", "Kotlin", "Java",
        "Android Jetpack", "Compose", "Flutter", "Dart",
        "React Native", "Kotlin Multiplatform",
        "Firebase", "REST API", "SQLite", "Room",
        "CoreData", "Xcode", "Android Studio", "CI/CD",
    ],
)


if __name__ == "__main__":
    args = parse_args("모바일 더미 채용공고 생성")
    run_category_generator(CONFIG, count=args.count, use_cache=args.use_cache)
