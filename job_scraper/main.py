"""
채용 공고 스크래핑 실행 진입점

사용법:
  python -m job_scraper.main                   # 전체 20개 회사
  python -m job_scraper.main --company-id 1 2  # 특정 회사만 (네이버, 카카오)
  python -m job_scraper.main --stats           # DB 통계 출력
"""

import argparse
import asyncio
import logging
import logging.handlers
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

# ── 로거 ────────────────────────────────────────────────────
APPLOG_DIR = Path("C:/GitHub/new_git/money/applogs")
APPLOG_DIR.mkdir(parents=True, exist_ok=True)


def setup_logger() -> logging.Logger:
    logger = logging.getLogger("job_scraper")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    sh.setFormatter(fmt)

    fh = logging.handlers.TimedRotatingFileHandler(
        APPLOG_DIR / "job_scraper.log",
        when="midnight",
        backupCount=7,
        encoding="utf-8",
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    logger.addHandler(sh)
    logger.addHandler(fh)
    return logger


logger = setup_logger()

# ── import (로거 설정 후) ────────────────────────────────────
from .companies import COMPANIES, COMPANY_MAP
from .llm_agent import check_ollama
from .scraper   import scrape_all, scrape_company
from .storage   import init_db, upsert_company, save_job, get_stats


# ── 실행 ────────────────────────────────────────────────────
async def run(companies: list[dict]) -> None:
    logger.info("========== 채용 공고 수집 시작 ==========")
    logger.info("대상 회사: %d개", len(companies))

    # Ollama 연결 확인
    if not check_ollama():
        logger.critical("Ollama 미연결. 종료합니다.")
        logger.critical("실행 방법: ollama serve && ollama pull llama3")
        sys.exit(1)

    # DB 초기화 + 회사 마스터 등록
    init_db()
    for c in companies:
        upsert_company(c["id"], c["name"], c["career_url"])

    # 스크래핑
    jobs = await scrape_all(companies)

    # 저장
    saved, skipped = 0, 0
    for job in jobs:
        if save_job(job):
            saved += 1
        else:
            skipped += 1

    # 결과 요약
    stats = get_stats()
    logger.info("========================================")
    logger.info("수집 완료 | 저장: %d건 | 중복 스킵: %d건", saved, skipped)
    logger.info("전체 DB 누적: %d건", stats["total"])
    for row in stats["by_company"]:
        logger.info("  %-15s %d건", row["company_name"], row["cnt"])
    logger.info("========================================")


def main() -> None:
    parser = argparse.ArgumentParser(description="채용 공고 스크래퍼")
    parser.add_argument(
        "--company-id",
        nargs="+",
        type=int,
        metavar="ID",
        help="수집할 회사 ID (미입력 시 전체 20개)",
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="DB 통계만 출력하고 종료",
    )
    args = parser.parse_args()

    # 통계 출력
    if args.stats:
        init_db()
        stats = get_stats()
        print(f"\n전체 수집 공고: {stats['total']}건")
        for row in stats["by_company"]:
            print(f"  {row['company_name']:<15} {row['cnt']}건")
        return

    # 대상 회사 선택
    if args.company_id:
        missing = [i for i in args.company_id if i not in COMPANY_MAP]
        if missing:
            logger.error("존재하지 않는 company_id: %s", missing)
            logger.error("사용 가능: %s", list(COMPANY_MAP.keys()))
            sys.exit(1)
        companies = [COMPANY_MAP[i] for i in args.company_id]
    else:
        companies = COMPANIES

    asyncio.run(run(companies))


if __name__ == "__main__":
    main()
