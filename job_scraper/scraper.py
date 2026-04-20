"""
Playwright 기반 채용 공고 스크래퍼
- JS 렌더링 지원 (React/Vue SPA 대응)
- 회사별 공고 목록 → 상세 페이지 순차 수집
"""

import asyncio
import logging
import random
import time
from typing import Optional
from urllib.parse import urljoin, urlparse

from playwright.async_api import (
    async_playwright,
    Browser,
    Page,
    TimeoutError as PlaywrightTimeout,
)

from .llm_agent import extract_job_links, extract_job_fields

logger = logging.getLogger("job_scraper.scraper")

# ── 설정 ────────────────────────────────────────────────────
PAGE_TIMEOUT   = 30_000          # ms
SCROLL_PAUSE   = 1.5             # 무한스크롤 대기 (초)
SCROLL_TIMES   = 3               # 최대 스크롤 횟수
REQUEST_DELAY  = (2.0, 4.0)      # 요청 간 랜덤 딜레이 (초)
MAX_JOBS_PER_COMPANY = 30        # 회사당 최대 수집 공고 수

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


# ── 브라우저 유틸 ────────────────────────────────────────────
async def _new_page(browser: Browser) -> Page:
    ctx = await browser.new_context(
        user_agent=USER_AGENT,
        locale="ko-KR",
        viewport={"width": 1280, "height": 900},
        extra_http_headers={"Accept-Language": "ko-KR,ko;q=0.9"},
    )
    page = await ctx.new_page()
    # 이미지/폰트 차단 (속도 향상)
    await page.route(
        "**/*.{png,jpg,jpeg,gif,webp,svg,woff,woff2,ttf}",
        lambda r: r.abort(),
    )
    return page


async def _get_text(page: Page, url: str) -> Optional[str]:
    """URL 로드 후 가시 텍스트 추출. 실패 시 None"""
    try:
        await page.goto(url, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
        # 무한스크롤 대응
        for _ in range(SCROLL_TIMES):
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(SCROLL_PAUSE)
        # 스크립트/스타일 제거 후 텍스트
        text = await page.evaluate("""() => {
            const remove = document.querySelectorAll('script,style,nav,footer,header');
            remove.forEach(el => el.remove());
            return document.body.innerText;
        }""")
        return text.strip()
    except PlaywrightTimeout:
        logger.warning("타임아웃: %s", url)
    except Exception as e:
        logger.error("페이지 로드 실패 [%s]: %s", url, e)
    return None


async def _get_links_from_page(page: Page, career_url: str) -> list[str]:
    """
    1차: href 패턴으로 공고 링크 빠르게 추출
    2차: 못 찾으면 LLM에 위임
    """
    try:
        await page.goto(career_url, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
        for _ in range(SCROLL_TIMES):
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(SCROLL_PAUSE)

        # href 패턴 매칭
        all_links: list[str] = await page.evaluate("""() => {
            return Array.from(document.querySelectorAll('a[href]'))
                        .map(a => a.href)
                        .filter(h => h && h.startsWith('http'));
        }""")

        domain = urlparse(career_url).netloc
        job_keywords = [
            "job", "jobs", "recruit", "career", "position",
            "posting", "opening", "공고", "채용", "jd",
        ]
        filtered = [
            l for l in all_links
            if domain in l
            and any(kw in l.lower() for kw in job_keywords)
            and l != career_url
        ]
        filtered = list(dict.fromkeys(filtered))   # 중복 제거

        if filtered:
            logger.info("패턴 매칭 링크: %d개", len(filtered))
            return filtered[:MAX_JOBS_PER_COMPANY]

        # fallback: LLM
        logger.info("패턴 매칭 실패 → LLM으로 링크 추출")
        text = await page.evaluate("() => document.body.innerText")
        return extract_job_links(text or "", career_url)

    except PlaywrightTimeout:
        logger.warning("목록 페이지 타임아웃: %s", career_url)
    except Exception as e:
        logger.error("링크 추출 오류 [%s]: %s", career_url, e)
    return []


# ── 핵심 스크래핑 함수 ────────────────────────────────────────
async def scrape_company(
    company_id: int,
    company_name: str,
    career_url: str,
) -> list[dict]:
    """
    회사 채용 페이지 전체 스크래핑.
    반환: 구조화된 공고 dict 리스트
    """
    results: list[dict] = []
    logger.info("━━ [%s] 스크래핑 시작: %s", company_name, career_url)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        try:
            page = await _new_page(browser)

            # 1) 공고 링크 수집
            job_links = await _get_links_from_page(page, career_url)
            if not job_links:
                logger.warning("[%s] 공고 링크 없음", company_name)
                return results

            logger.info("[%s] 수집할 공고: %d개", company_name, len(job_links))

            # 2) 각 공고 상세 → LLM 추출
            for idx, link in enumerate(job_links[:MAX_JOBS_PER_COMPANY], 1):
                logger.info("[%s] (%d/%d) %s", company_name, idx, len(job_links), link)
                try:
                    text = await _get_text(page, link)
                    if not text:
                        continue

                    fields = extract_job_fields(text, company_name)
                    if not fields:
                        logger.warning("[%s] LLM 추출 실패: %s", company_name, link)
                        continue

                    job = {
                        "company_id":   company_id,
                        "company_name": company_name,
                        "company_url":  link,
                        **fields,
                    }
                    results.append(job)

                except Exception as e:
                    logger.error("[%s] 공고 처리 오류 [%s]: %s", company_name, link, e)

                # 요청 간 딜레이 (서버 부하 방지)
                await asyncio.sleep(random.uniform(*REQUEST_DELAY))

        except Exception as e:
            logger.exception("[%s] 스크래핑 중 예기치 못한 오류: %s", company_name, e)
        finally:
            await browser.close()

    logger.info("━━ [%s] 완료: %d건 수집", company_name, len(results))
    return results


async def scrape_all(companies: list[dict]) -> list[dict]:
    """전체 회사 순차 스크래핑 (병렬 시 IP 차단 위험)"""
    all_results: list[dict] = []
    for company in companies:
        try:
            jobs = await scrape_company(
                company_id=company["id"],
                company_name=company["name"],
                career_url=company["career_url"],
            )
            all_results.extend(jobs)
        except Exception as e:
            logger.error("회사 스크래핑 실패 [%s]: %s", company["name"], e)
        # 회사 간 딜레이
        await asyncio.sleep(random.uniform(3.0, 6.0))
    return all_results
