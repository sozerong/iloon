"""
부산광역시 전용 채용공고 생성기 (전체 직군 + 트렌드 JSON 저장)

generate_all_with_trends.py 와 동일한 방식으로,
부산 전용 직군 설정을 사용해 공고를 생성하고
results/trend_keywords_busan.json 에 트렌드를 저장합니다.

마감일 분배:
  - 6월 마감    : 2026-06-20 ~ 2026-06-30
  - 7월 중순 마감: 2026-07-10 ~ 2026-07-20
  - 8월 마감    : 2026-08-15 ~ 2026-08-31

실행:
  set ANTHROPIC_API_KEY=sk-ant-...
  python generators/generator_busan.py
  python generators/generator_busan.py --count 8
  python generators/generator_busan.py --count 5 --parallel
  python generators/generator_busan.py --categories backend frontend ai_ml
"""

import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR))

from generator_base import (
    CategoryConfig,
    OUTPUT_DIR,
    LOGS_DIR,
    load_used_companies,
    save_used_companies,
    load_cache,
    save_cache,
    setup_logger,
    _get_client,
    _search_trends,
    _generate_jobs,
    _inject_uuid,
)

# ── 공통 설정 ─────────────────────────────────────────────────
_LOCATION = "부산광역시"
_DEADLINE = (
    "아래 3개 구간으로 공고 수를 균등하게 나눠 배분하세요 (deadline 필드에 YYYY-MM-DD 형식):\n"
    "  - 6월 마감: 2026-06-20 ~ 2026-06-30\n"
    "  - 7월 중순 마감: 2026-07-10 ~ 2026-07-20\n"
    "  - 8월 마감: 2026-08-15 ~ 2026-08-31\n"
    "  시군구는 해운대구·부산진구·동래구·남구·수영구·사상구·강서구 중에서 다양하게 선택"
)

# ── 직군 설정 ─────────────────────────────────────────────────
CONFIGS: list[CategoryConfig] = [
    CategoryConfig(
        name="busan_backend",
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
            "API 서버 개발자",
            "MSA 백엔드 엔지니어",
        ],
        skills_hint=[
            "Python", "FastAPI", "Java", "Spring Boot", "Node.js",
            "Go", "REST API", "PostgreSQL", "MySQL", "Redis",
            "Docker", "AWS", "Kafka", "MSA",
        ],
        location_hint=_LOCATION,
        deadline_hint=_DEADLINE,
    ),
    CategoryConfig(
        name="busan_frontend",
        display_name="프론트엔드",
        search_queries=[
            "사람인 원티드 프론트엔드 개발자 채용 2026 React Vue",
            "2026 프론트엔드 인기 기술스택 트렌드 TypeScript Next.js",
            "2026 프론트엔드 개발자 연봉 복지 트렌드",
            "웹 프론트엔드 신입 경력 채용공고 2026",
        ],
        job_titles=[
            "React 프론트엔드 개발자",
            "Vue.js 웹 개발자",
            "Next.js 풀스택 개발자",
            "TypeScript 프론트엔드 엔지니어",
        ],
        skills_hint=[
            "React", "Vue.js", "Next.js", "TypeScript", "JavaScript",
            "Tailwind CSS", "Redux", "Zustand", "Vite", "REST API",
        ],
        location_hint=_LOCATION,
        deadline_hint=_DEADLINE,
    ),
    CategoryConfig(
        name="busan_ai_ml",
        display_name="AI/ML",
        search_queries=[
            "2026 AI 머신러닝 엔지니어 채용공고 트렌드 LLM",
            "원티드 사람인 데이터사이언티스트 ML엔지니어 채용 2026",
            "2026 AI 개발자 인기 기술스택 PyTorch LangChain",
            "2026 AI ML 개발자 연봉 복지 신입 경력",
        ],
        job_titles=[
            "ML 엔지니어",
            "데이터 사이언티스트",
            "AI 서비스 개발자",
            "LLM 파인튜닝 엔지니어",
            "컴퓨터 비전 엔지니어",
        ],
        skills_hint=[
            "Python", "PyTorch", "TensorFlow", "LangChain", "OpenAI API",
            "Hugging Face", "scikit-learn", "Pandas", "FastAPI", "MLflow",
        ],
        location_hint=_LOCATION,
        deadline_hint=_DEADLINE,
    ),
    CategoryConfig(
        name="busan_data",
        display_name="데이터 엔지니어링",
        search_queries=[
            "2026 데이터 엔지니어 채용공고 트렌드 Spark Kafka Airflow",
            "사람인 원티드 데이터 분석가 BI 엔지니어 채용 2026",
            "2026 데이터 엔지니어링 인기 기술스택 dbt BigQuery",
            "2026 데이터 직군 연봉 복지 트렌드",
        ],
        job_titles=[
            "데이터 엔지니어",
            "데이터 분석가",
            "BI 엔지니어",
            "데이터 플랫폼 개발자",
        ],
        skills_hint=[
            "Python", "Spark", "Kafka", "Airflow", "dbt",
            "SQL", "PostgreSQL", "BigQuery", "Tableau", "Superset",
        ],
        location_hint=_LOCATION,
        deadline_hint=_DEADLINE,
    ),
    CategoryConfig(
        name="busan_devops",
        display_name="DevOps/인프라",
        search_queries=[
            "2026 DevOps 엔지니어 채용공고 트렌드 Kubernetes Docker",
            "원티드 사람인 클라우드 인프라 SRE 채용 2026",
            "2026 DevOps 인기 기술스택 Terraform ArgoCD",
            "2026 인프라 엔지니어 연봉 복지 트렌드",
        ],
        job_titles=[
            "DevOps 엔지니어",
            "클라우드 인프라 엔지니어",
            "SRE",
            "CI/CD 파이프라인 엔지니어",
        ],
        skills_hint=[
            "Kubernetes", "Docker", "Terraform", "AWS", "GCP",
            "Jenkins", "GitHub Actions", "ArgoCD", "Prometheus", "Linux",
        ],
        location_hint=_LOCATION,
        deadline_hint=_DEADLINE,
    ),
    CategoryConfig(
        name="busan_mobile",
        display_name="모바일 개발",
        search_queries=[
            "2026 iOS Android 모바일 개발자 채용공고 트렌드",
            "원티드 사람인 Flutter React Native 개발자 채용 2026",
            "2026 모바일 앱 개발자 연봉 복지 기술스택 트렌드",
        ],
        job_titles=[
            "iOS 개발자 (Swift)",
            "Android 개발자 (Kotlin)",
            "Flutter 크로스플랫폼 개발자",
            "React Native 개발자",
        ],
        skills_hint=[
            "Swift", "SwiftUI", "Kotlin", "Jetpack Compose",
            "Flutter", "React Native", "Firebase", "REST API",
        ],
        location_hint=_LOCATION,
        deadline_hint=_DEADLINE,
    ),
    CategoryConfig(
        name="busan_security",
        display_name="보안",
        search_queries=[
            "2026 정보보안 엔지니어 채용공고 트렌드",
            "사람인 원티드 보안 개발자 침해대응 채용 2026",
            "2026 클라우드 보안 취약점 분석 연봉 트렌드",
        ],
        job_titles=[
            "정보보안 엔지니어",
            "보안 개발자",
            "침해사고 대응(IR) 엔지니어",
            "클라우드 보안 엔지니어",
        ],
        skills_hint=[
            "SIEM", "방화벽", "IDS/IPS", "취약점 분석", "Python",
            "네트워크 보안", "클라우드 보안", "ISMS", "모의해킹",
        ],
        location_hint=_LOCATION,
        deadline_hint=_DEADLINE,
    ),
    CategoryConfig(
        name="busan_qa",
        display_name="QA/테스트",
        search_queries=[
            "2026 QA 엔지니어 채용공고 트렌드 자동화 테스트",
            "사람인 원티드 테스트 자동화 SDET 채용 2026",
            "2026 QA 개발자 연봉 기술스택 Playwright Selenium",
        ],
        job_titles=[
            "QA 엔지니어",
            "테스트 자동화 엔지니어",
            "품질 관리 엔지니어",
        ],
        skills_hint=[
            "Selenium", "Playwright", "Appium", "Pytest",
            "Jira", "Postman", "CI/CD", "Python",
        ],
        location_hint=_LOCATION,
        deadline_hint=_DEADLINE,
    ),
    CategoryConfig(
        name="busan_pm",
        display_name="PM/기획",
        search_queries=[
            "2026 프로덕트 매니저 PM 서비스 기획자 채용공고 트렌드",
            "원티드 사람인 IT 기획자 PO PM 채용 2026",
            "2026 서비스 기획자 연봉 복지 필요역량 트렌드",
        ],
        job_titles=[
            "프로덕트 매니저 (PM)",
            "IT 서비스 기획자",
            "앱 서비스 PO",
            "UX 기획자",
        ],
        skills_hint=[
            "Jira", "Confluence", "Figma", "SQL", "Google Analytics",
            "Mixpanel", "A/B 테스트", "OKR", "애자일",
        ],
        location_hint=_LOCATION,
        deadline_hint=_DEADLINE,
    ),
]

TRENDS_OUTPUT = OUTPUT_DIR / "trend_keywords_busan.json"
CATEGORY_KEYS = [c.name.replace("busan_", "") for c in CONFIGS]


# ── 단일 카테고리 실행 ────────────────────────────────────────
def run_one(config: CategoryConfig, count: int, use_cache: bool) -> tuple:
    logger = setup_logger(f"generator_{config.name}")
    start  = time.time()
    print(f"  ▶ [{config.display_name}] 시작")

    used_companies = load_used_companies(logger)
    result = load_cache(config.name, logger) if use_cache else None

    if not result:
        try:
            client        = _get_client()
            trend_summary = _search_trends(client, config, logger)
            result        = _generate_jobs(client, config, trend_summary, count, used_companies, logger)
        except Exception as e:
            logger.error("[%s] 생성 실패: %s", config.display_name, e)
            elapsed = time.time() - start
            print(f"  X [{config.display_name}] 실패 ({elapsed:.0f}초)")
            return config.display_name, False, elapsed, {}

        if not result or "jobs" not in result:
            elapsed = time.time() - start
            print(f"  X [{config.display_name}] 결과 없음 ({elapsed:.0f}초)")
            return config.display_name, False, elapsed, {}

        save_cache(config.name, result, logger)

    jobs = _inject_uuid(result.get("jobs", []))
    save_used_companies(jobs, logger)

    trend   = result.get("trend_summary", {})
    elapsed = time.time() - start
    print(f"  OK [{config.display_name}] 완료 ({elapsed:.0f}초) | 공고 {len(jobs)}개")
    return config.display_name, True, elapsed, jobs, trend


# ── 순차/병렬 실행 ────────────────────────────────────────────
def run_sequential(configs: list, count: int, use_cache: bool) -> tuple:
    results, all_jobs, trends = [], [], {}
    for config in configs:
        display, ok, elapsed, jobs, trend = run_one(config, count, use_cache)
        results.append((display, ok, elapsed))
        if ok:
            all_jobs.extend(jobs)
            if trend:
                trends[config.name] = {
                    "display_name": config.display_name,
                    "analyzed_at":  datetime.now().strftime("%Y-%m-%d"),
                    "trend_summary": trend,
                }
    return results, all_jobs, trends


def run_parallel(configs: list, count: int, use_cache: bool, max_workers: int = 3) -> tuple:
    results, all_jobs, trends = [], [], {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(run_one, config, count, use_cache): config
            for config in configs
        }
        for future in as_completed(futures):
            config = futures[future]
            display, ok, elapsed, jobs, trend = future.result()
            results.append((display, ok, elapsed))
            if ok:
                all_jobs.extend(jobs)
                if trend:
                    trends[config.name] = {
                        "display_name": config.display_name,
                        "analyzed_at":  datetime.now().strftime("%Y-%m-%d"),
                        "trend_summary": trend,
                    }
    return results, all_jobs, trends


def save_all_jobs(jobs: list) -> None:
    """전체 직군 공고를 JSON 1개 + JSONL 1개로 저장"""
    ts    = datetime.now().strftime("%Y%m%d_%H%M%S")
    today = datetime.now().strftime("%Y-%m-%d")

    # 단일 JSON
    json_path = OUTPUT_DIR / f"busan_jobs_{ts}.json"
    payload = {
        "site":         "일로온",
        "region":       "부산광역시",
        "generated_at": today,
        "total":        len(jobs),
        "jobs":         jobs,
    }
    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        print(f"JSON 저장: {json_path} ({len(jobs)}개)")
    except Exception as e:
        print(f"JSON 저장 실패: {e}")

    # 단일 JSONL (importer용)
    jsonl_path = LOGS_DIR / f"dummy-jobs-busan-{today}.jsonl"
    try:
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        with open(jsonl_path, "w", encoding="utf-8") as f:
            for job in jobs:
                f.write(json.dumps(job, ensure_ascii=False) + "\n")
        print(f"JSONL 저장: {jsonl_path} ({len(jobs)}개)")
    except Exception as e:
        print(f"JSONL 저장 실패: {e}")


def save_trend_keywords(trends: dict) -> None:
    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        with open(TRENDS_OUTPUT, "w", encoding="utf-8") as f:
            json.dump(trends, f, ensure_ascii=False, indent=2)
        print(f"\n트렌드/키워드 저장: {TRENDS_OUTPUT}")
    except Exception as e:
        print(f"\n트렌드 저장 실패: {e}")


def print_summary(results: list, total_elapsed: float) -> None:
    ok_count = sum(1 for _, ok, _ in results if ok)
    print(f"\n{'='*50}")
    print(f"  부산 공고 생성 완료")
    print(f"{'─'*50}")
    for display, ok, elapsed in sorted(results, key=lambda x: x[0]):
        status = "OK" if ok else "X"
        print(f"  {status} {display:<15} ({elapsed:.0f}초)")
    print(f"{'─'*50}")
    print(f"  성공: {ok_count}/{len(results)} | 총 소요: {total_elapsed:.0f}초")
    print(f"{'='*50}")


# ── 메인 ─────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="부산광역시 전용 채용공고 생성 (전체 직군 + 트렌드 저장)")
    parser.add_argument("--count",       default=5, type=int,
                        help="카테고리당 공고 수 (기본: 5)")
    parser.add_argument("--parallel",    action="store_true",
                        help="병렬 실행 (기본: 순차)")
    parser.add_argument("--workers",     default=3, type=int,
                        help="병렬 실행 동시 작업 수 (기본: 3)")
    parser.add_argument("--use-cache",   action="store_true",
                        help="당일 캐시 재사용")
    parser.add_argument("--categories",  nargs="+", choices=CATEGORY_KEYS, default=None,
                        metavar="CATEGORY",
                        help=f"실행할 직군 (기본: 전체). 선택: {', '.join(CATEGORY_KEYS)}")
    args = parser.parse_args()

    # 카테고리 필터
    if args.categories:
        selected = [c for c in CONFIGS if c.name.replace("busan_", "") in args.categories]
    else:
        selected = CONFIGS

    total_jobs = args.count * len(selected)
    mode = "병렬" if args.parallel else "순차"

    print(f"\n{'='*50}")
    print(f"  부산광역시 전용 채용공고 생성")
    print(f"  직군: {len(selected)}개 | 직군당: {args.count}개")
    print(f"  예상 총 공고: {total_jobs}개 | 실행 모드: {mode}")
    print(f"  마감일: 6월말 / 7월중순 / 8월말 균등 배분")
    print(f"{'='*50}\n")

    start = time.time()

    if args.parallel:
        results, all_jobs, trends = run_parallel(selected, args.count, args.use_cache, args.workers)
    else:
        results, all_jobs, trends = run_sequential(selected, args.count, args.use_cache)

    print_summary(results, time.time() - start)

    if all_jobs:
        save_all_jobs(all_jobs)
    if trends:
        save_trend_keywords(trends)


if __name__ == "__main__":
    main()
