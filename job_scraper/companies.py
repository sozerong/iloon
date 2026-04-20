"""
스크래핑 대상 대기업 채용 사이트 20개
company_id 는 companies 테이블 PK (job_postings 의 FK)
"""

COMPANIES: list[dict] = [
    {"id":  1, "name": "네이버",       "career_url": "https://recruit.navercorp.com/rcrt/list.do"},
    {"id":  2, "name": "카카오",       "career_url": "https://careers.kakao.com/jobs"},
    {"id":  3, "name": "라인플러스",   "career_url": "https://careers.linecorp.com/ko/jobs"},
    {"id":  4, "name": "쿠팡",         "career_url": "https://www.coupang.jobs/kr/jobs/"},
    {"id":  5, "name": "우아한형제들", "career_url": "https://career.woowahan.com/"},
    {"id":  6, "name": "토스",         "career_url": "https://toss.im/career/jobs"},
    {"id":  7, "name": "당근마켓",     "career_url": "https://about.daangn.com/jobs/"},
    {"id":  8, "name": "크래프톤",     "career_url": "https://krafton.com/jobs/"},
    {"id":  9, "name": "넥슨",         "career_url": "https://career.nexon.com/Home/Recruit"},
    {"id": 10, "name": "엔씨소프트",   "career_url": "https://careers.ncsoft.com/"},
    {"id": 11, "name": "삼성SDS",      "career_url": "https://www.samsungsds.com/kr/recruit/recruit.html"},
    {"id": 12, "name": "LGCNS",        "career_url": "https://www.lgcns.com/careers/"},
    {"id": 13, "name": "카카오뱅크",   "career_url": "https://www.kakaobank.com/jobs"},
    {"id": 14, "name": "카카오페이",   "career_url": "https://careers.kakaopay.com/"},
    {"id": 15, "name": "하이브",       "career_url": "https://hybe.com/careers"},
    {"id": 16, "name": "야놀자",       "career_url": "https://careers.yanolja.co/"},
    {"id": 17, "name": "무신사",       "career_url": "https://about.musinsa.com/careers/"},
    {"id": 18, "name": "컬리",         "career_url": "https://career.kurly.com/"},
    {"id": 19, "name": "직방",         "career_url": "https://zigbang.recruiter.co.kr/app/jobnotice/list"},
    {"id": 20, "name": "현대오토에버", "career_url": "https://www.hyundai-autoever.com/kor/Recruit/RecruitMain.do"},
]

COMPANY_MAP: dict[int, dict] = {c["id"]: c for c in COMPANIES}
