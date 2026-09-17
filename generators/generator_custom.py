"""
비IT 기업 맞춤 채용공고 자동 생성기

비IT 회사가 원하는 업무와 조건을 알려주면
AI가 적절한 IT 직무로 변환하여 채용공고를 자동 생성합니다.

실행:
  python generator_custom.py
  python generator_custom.py --input request.json
  python generator_custom.py --count 3
"""

import argparse
import json
import logging
import logging.handlers
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, str(Path(__file__).parent))

import anthropic

from generator_base import (
    BASE_DIR, OUTPUT_DIR, LOGS_DIR, APPLOG_DIR, CACHE_DIR,
    setup_logger,
    _inject_uuid,
    load_used_companies,
    save_used_companies,
    save_trend_summary,
    TREND_SUMMARY_PATH,
)

# ── 경로 ────────────────────────────────────────────────────────
CUSTOM_OUTPUT_DIR = OUTPUT_DIR / "custom"
CUSTOM_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

logger = setup_logger("generator_custom")

TOOLS = [{"type": "web_search_20250305", "name": "web_search"}]


# ── Anthropic 클라이언트 ─────────────────────────────────────────
def _get_client() -> anthropic.Anthropic:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        logger.critical("ANTHROPIC_API_KEY 환경변수가 설정되지 않았습니다.")
        sys.exit(1)
    return anthropic.Anthropic(api_key=api_key)


# ── 입력 스키마 ──────────────────────────────────────────────────
"""
회사 측에서 제공하는 요청 정보 (request.json 또는 CLI 입력)
{
  "company_name":   "회사명 (실명 사용 가능, 공고에는 가상명으로 대체)",
  "industry":       "업종 (예: 제조업, 유통/물류, 금융, 의료, 교육 등)",
  "company_size":   "회사 규모 (스타트업 / 중소기업 / 중견기업 / 대기업)",
  "location":       "근무지 (예: 서울 강남구, 경기 성남시)",
  "work_to_do":     "하고 싶은 일/해결하고 싶은 문제 (자유 기술)",
  "requirements":   "원하는 조건 (자유 기술, 없으면 빈 문자열)",
  "headcount":      채용 인원 수 (기본 1),
  "deadline":       "지원 마감일 (YYYY-MM-DD, 없으면 null)"
}
"""

SAMPLE_REQUEST = {
    "company_name":  "예시기업",
    "industry":      "제조업",
    "company_size":  "중소기업",
    "location":      "경기 화성시",
    "work_to_do":    "공장 생산 데이터를 실시간으로 수집하고 불량률을 줄이는 시스템을 만들어줄 사람이 필요합니다",
    "requirements":  "현장과 소통할 수 있는 사람이었으면 좋겠고, 경력은 3년 이상이면 좋겠습니다",
    "headcount":     1,
    "deadline":      None,
}


# ── Phase 1: 업무 분석 → IT 직무 매핑 ────────────────────────────
def _analyze_request(client: anthropic.Anthropic, req: dict) -> str:
    """회사 요청을 분석해 적합한 IT 직무와 기술스택을 파악"""
    logger.info("[Phase 1] 요청 분석 시작 | 업종: %s", req.get("industry", ""))

    prompt = f"""비IT 기업의 채용 요청을 분석해서 적합한 IT 직무와 기술스택을 파악해주세요.

## 회사 정보
- 업종: {req.get("industry", "")}
- 규모: {req.get("company_size", "")}
- 위치: {req.get("location", "")}

## 회사가 원하는 것
{req.get("work_to_do", "")}

## 추가 조건
{req.get("requirements", "없음")}

아래 내용을 웹 검색으로 파악해주세요:
1. 이 업무에 가장 적합한 IT 직무명 (구체적으로)
2. 필요한 기술스택과 도구
3. 비슷한 업종에서 실제 채용하는 방식 및 연봉 수준
4. 이 직무에서 요구하는 실무 역량

검색 후 자유 텍스트로 요약해주세요 (JSON 아님)."""

    messages = [{"role": "user", "content": prompt}]
    step = 0

    try:
        while True:
            step += 1
            response = client.messages.create(
                model="claude-haiku-4-5",
                max_tokens=4000,
                tools=TOOLS,
                messages=messages,
            )
            logger.info("[Phase 1 / Step %d] stop_reason: %s", step, response.stop_reason)

            if response.stop_reason == "tool_use":
                tool_uses = [b for b in response.content if b.type == "tool_use"]
                tool_results = []
                for tu in tool_uses:
                    logger.info("[검색] %s", tu.input.get("query", ""))
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": tu.id,
                        "content": "검색 완료",
                    })
                messages.append({"role": "assistant", "content": response.content})
                messages.append({"role": "user", "content": tool_results})

            elif response.stop_reason in ("end_turn", "max_tokens"):
                text_blocks = [b for b in response.content if b.type == "text"]
                summary = text_blocks[-1].text.strip() if text_blocks else ""
                logger.info("[Phase 1] 분석 완료 (%d자)", len(summary))
                return summary

            else:
                logger.warning("[Phase 1] 예상치 못한 stop_reason: %s", response.stop_reason)
                break

    except Exception as e:
        logger.error("[Phase 1] 분석 실패: %s", e)

    return ""


# ── Phase 2: 공고 JSON 생성 ──────────────────────────────────────
def _generate_job(
    client: anthropic.Anthropic,
    req: dict,
    analysis: str,
    used_companies: list,
) -> dict:
    """분석 결과를 바탕으로 채용공고 JSON 1개 생성"""
    import uuid
    session_id = str(uuid.uuid4())[:8]
    today      = datetime.now().strftime("%Y-%m-%d")
    deadline   = req.get("deadline") or "null"
    headcount  = req.get("headcount", 1)
    used_str   = ", ".join(used_companies) if used_companies else "없음"

    logger.info("[Phase 2] 공고 생성 시작 | session=%s", session_id)

    prompt = f"""비IT 기업의 요청과 아래 분석 결과를 바탕으로 일로온 채용공고 JSON 1개를 생성하세요.

## 회사 요청 원문
- 업종: {req.get("industry", "")}
- 규모: {req.get("company_size", "")}
- 위치: {req.get("location", "")}
- 원하는 일: {req.get("work_to_do", "")}
- 추가 조건: {req.get("requirements", "없음")}
- 채용 인원: {headcount}명

## IT 직무 분석 결과
{analysis}

## 생성 규칙
- 회사명은 실제 회사명 대신 창의적인 가상 회사명으로 대체 (업종 분위기 반영)
- 이미 사용된 회사명 (절대 재사용 금지): {used_str}
- apply.method는 반드시 "일로온 입사지원"
- job_id / company.id 는 UUID v4 형식
- 비IT 업종의 특성이 main_tasks와 requirements에 자연스럽게 녹아들게
- 세션 시드: {session_id}

반드시 순수 JSON만 출력. 마크다운/설명 없이:
{{
  "site": "일로온",
  "category": "비IT기업 IT직무",
  "source_industry": "{req.get("industry", "")}",
  "analyzed_at": "{today}",
  "job": {{
    "job_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
    "status": "active",
    "always_open": false,
    "company": {{
      "id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
      "name": "가상회사명",
      "industry": {{"large": "비IT업종", "mid": "{req.get("industry", "")}"}},
      "size": "{req.get("company_size", "중소기업")}",
      "employee_count": 200
    }},
    "position": {{
      "title": "분석된 직무명",
      "job_category": {{"large": "개발", "mid": "분석된 중분류", "small": "분석된 소분류"}},
      "career": {{"type": "경력", "min_year": 0, "max_year": 5}},
      "education": {{"type": "대졸이상", "required": false}},
      "headcount": {headcount},
      "work_type": "상주"
    }},
    "work_condition": {{
      "location": {{"sido": "시도", "sigungu": "시군구", "address": "{req.get("location", "")}"}},
      "salary": {{"type": "연봉", "min": 40000000, "max": 60000000, "negotiable": true, "unit": "원"}},
      "work_hours": "09:00~18:00"
    }},
    "skills": ["기술스택1", "기술스택2"],
    "detail": {{
      "main_tasks": ["주요업무1", "주요업무2"],
      "requirements": ["자격요건1", "자격요건2"],
      "preferred": ["우대사항1"],
      "talent": ["인재상1"],
      "benefits": ["복지1"]
    }},
    "apply": {{"method": "일로온 입사지원", "document": ["이력서", "자기소개서"]}},
    "recruitment_process": {{
      "total_steps": 3,
      "steps": [
        {{"step": 1, "name": "서류전형", "description": "서류 검토", "duration": "1주일"}},
        {{"step": 2, "name": "기술면접", "description": "기술 역량 확인", "duration": "1주일"}},
        {{"step": 3, "name": "최종면접", "description": "현장 담당자 면접", "duration": "1주일"}}
      ],
      "notice": "합격자에 한해 개별 연락"
    }},
    "ai_recommendation": {{"is_recommended": false, "match_score": null}},
    "stats": {{"view_count": 0, "apply_count": 0, "bookmark_count": 0}},
    "dates": {{
      "posted_at": "{today}",
      "deadline": {"null" if deadline == "null" else f'"{deadline}"'},
      "updated_at": "{today}"
    }}
  }}
}}"""

    try:
        response = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=6000,
            messages=[{"role": "user", "content": prompt}],
        )
        logger.info("[Phase 2] stop_reason: %s", response.stop_reason)

        text_blocks = [b for b in response.content if b.type == "text"]
        if not text_blocks:
            logger.error("[Phase 2] 텍스트 블록 없음")
            return {}

        raw = text_blocks[-1].text.strip()
        logger.info("[Phase 2] 응답 수신 (%d자)", len(raw))

        if "```" in raw:
            match = re.search(r"```(?:json)?\s*([\s\S]+?)```", raw, re.DOTALL)
            if match:
                raw = match.group(1).strip()

        try:
            result = json.loads(raw)
            logger.info("[Phase 2] JSON 파싱 성공")
            return result
        except json.JSONDecodeError:
            logger.warning("[Phase 2] JSON 불완전 — 보정 시도")
            for closing in ("}}", "\n}}"):
                try:
                    return json.loads(raw + closing)
                except json.JSONDecodeError:
                    continue
            logger.error("[Phase 2] JSON 파싱 최종 실패")
            return {}

    except Exception as e:
        logger.error("[Phase 2] 생성 실패: %s", e)
        raise


# ── 결과 저장 ────────────────────────────────────────────────────
def _save_results(result: dict, req: dict) -> None:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    industry_safe = re.sub(r"[^\w가-힣]", "_", req.get("industry", "custom"))

    # UUID 주입
    job = result.get("job", {})
    if job:
        jobs_fixed = _inject_uuid([job])
        result = {**result, "job": jobs_fixed[0]}
        job = result["job"]

    # 결과 JSON 저장 (results/custom/)
    json_path = CUSTOM_OUTPUT_DIR / f"custom_job_{industry_safe}_{ts}.json"
    try:
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        logger.info("공고 저장: %s", json_path)
    except Exception as e:
        logger.error("공고 저장 실패: %s", e)

    # JSONL (importer가 읽는 파일)
    today = datetime.now().strftime("%Y-%m-%d")
    jsonl_path = LOGS_DIR / f"dummy-jobs-custom-{today}.jsonl"
    try:
        with open(jsonl_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(job, ensure_ascii=False) + "\n")
        logger.info("JSONL 저장: %s", jsonl_path)
    except Exception as e:
        logger.error("JSONL 저장 실패: %s", e)

    print(f"\n✅ 공고 저장 완료: {json_path}")


# ── 입력 로드 ────────────────────────────────────────────────────
def _load_request(input_path: Optional[str]) -> dict:
    """--input 파일이 있으면 로드, 없으면 대화형 입력"""
    if input_path:
        path = Path(input_path)
        if not path.exists():
            print(f"❌ 파일을 찾을 수 없습니다: {input_path}")
            sys.exit(1)
        with open(path, encoding="utf-8") as f:
            req = json.load(f)
        print(f"📂 요청 파일 로드: {input_path}")
        return req

    # 대화형 입력
    print("\n" + "="*50)
    print("  일로온 맞춤 채용공고 생성기")
    print("  비IT 기업 정보를 입력하면 공고를 자동 생성합니다")
    print("="*50 + "\n")

    def ask(label: str, default: str = "") -> str:
        hint = f" (기본값: {default})" if default else ""
        val = input(f"{label}{hint}: ").strip()
        return val if val else default

    company_name = ask("회사명 (공고에는 가상명으로 대체됩니다)")
    industry     = ask("업종", "제조업")
    company_size = ask("회사 규모 (스타트업/중소기업/중견기업/대기업)", "중소기업")
    location     = ask("근무지", "서울")
    work_to_do   = ask("원하는 일 / 해결하고 싶은 문제 (자유롭게 적어주세요)")
    requirements = ask("추가 조건 (없으면 엔터)", "")
    headcount    = ask("채용 인원", "1")
    deadline     = ask("지원 마감일 YYYY-MM-DD (없으면 엔터)", "")

    return {
        "company_name":  company_name,
        "industry":      industry,
        "company_size":  company_size,
        "location":      location,
        "work_to_do":    work_to_do,
        "requirements":  requirements,
        "headcount":     int(headcount) if headcount.isdigit() else 1,
        "deadline":      deadline if deadline else None,
    }


# ── 샘플 요청 파일 생성 ──────────────────────────────────────────
def _write_sample(output_path: str) -> None:
    path = Path(output_path)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(SAMPLE_REQUEST, f, ensure_ascii=False, indent=2)
    print(f"✅ 샘플 요청 파일 생성: {path}")
    print("   수정 후 python generator_custom.py --input <파일경로> 로 실행하세요")


# ── 메인 ─────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="비IT 기업 맞춤 채용공고 자동 생성")
    parser.add_argument("--input",       metavar="FILE",
                        help="요청 JSON 파일 경로 (없으면 대화형 입력)")
    parser.add_argument("--sample",      metavar="FILE", default="",
                        help="샘플 요청 파일 생성 후 종료 (예: --sample request.json)")
    args = parser.parse_args()

    # 샘플 파일만 생성하고 종료
    if args.sample:
        _write_sample(args.sample)
        return

    # 요청 로드
    req = _load_request(args.input)

    print(f"\n{'='*50}")
    print(f"  생성 시작")
    print(f"  업종: {req.get('industry')} | 위치: {req.get('location')}")
    print(f"{'='*50}\n")

    client         = _get_client()
    used_companies = load_used_companies(logger)

    # Phase 1: 업무 분석 → IT 직무 매핑
    analysis = _analyze_request(client, req)
    if not analysis:
        logger.critical("Phase 1 분석 실패")
        sys.exit(1)

    # Phase 2: 공고 JSON 생성
    result = _generate_job(client, req, analysis, used_companies)
    if not result or "job" not in result:
        logger.critical("공고 생성 실패")
        sys.exit(1)

    # 저장
    save_used_companies([result.get("job", {})], logger)
    _save_results(result, req)

    # 결과 출력
    job = result.get("job", {})
    pos = job.get("position", {})
    print(f"\n─── 생성된 공고 ───")
    print(f"  직무명    : {pos.get('title', '')}")
    print(f"  직무 분류 : {pos.get('job_category', {})}")
    print(f"  기술스택  : {job.get('skills', [])}")
    print(f"  주요업무  : {job.get('detail', {}).get('main_tasks', [])}")
    print(f"  저장 위치 : results/custom/")


if __name__ == "__main__":
    main()
