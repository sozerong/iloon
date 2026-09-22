"""더미 설문 JSON 생성"""

import json
import uuid
from pathlib import Path

SURVEYS = [
    {"job_type": "백엔드 개발",         "region": "천안시 서북구 불당동",  "occupation": "백엔드/서버",      "career_type": "경력", "education": "대학교졸업"},
    {"job_type": "백엔드 개발",         "region": "천안시 서북구 백석동",  "occupation": "백엔드/서버",      "career_type": "신입", "education": "대학교졸업"},
    {"job_type": "Java 백엔드 개발",    "region": "천안시 동남구 신방동",  "occupation": "백엔드/서버",      "career_type": "경력", "education": "대학교졸업"},
    {"job_type": "Python 백엔드 개발",  "region": "아산시 배방읍",         "occupation": "백엔드/서버",      "career_type": "경력", "education": "대학원졸업"},
    {"job_type": "프론트엔드 개발",     "region": "천안시 서북구 성성동",  "occupation": "프론트엔드",       "career_type": "신입", "education": "대학교졸업"},
    {"job_type": "React 프론트엔드",    "region": "천안시 동남구 청수동",  "occupation": "프론트엔드",       "career_type": "경력", "education": "대학교졸업"},
    {"job_type": "AI/ML 엔지니어",      "region": "아산시 탕정면",         "occupation": "AI/ML",           "career_type": "경력", "education": "대학원졸업"},
    {"job_type": "데이터 엔지니어",     "region": "천안시 서북구 두정동",  "occupation": "데이터엔지니어링", "career_type": "경력", "education": "대학교졸업"},
    {"job_type": "데이터 분석가",       "region": "공주시 신관동",         "occupation": "데이터분석",       "career_type": "신입", "education": "대학교졸업"},
    {"job_type": "DevOps 엔지니어",     "region": "천안시 동남구 봉명동",  "occupation": "인프라/DevOps",    "career_type": "경력", "education": "대학교졸업"},
    {"job_type": "클라우드 엔지니어",   "region": "아산시 온천동",         "occupation": "인프라/DevOps",    "career_type": "경력", "education": "대학교졸업"},
    {"job_type": "iOS 개발",            "region": "천안시 서북구 불당동",  "occupation": "모바일",           "career_type": "경력", "education": "대학교졸업"},
    {"job_type": "Android 개발",        "region": "논산시 취암동",         "occupation": "모바일",           "career_type": "신입", "education": "대학교졸업"},
    {"job_type": "보안 엔지니어",       "region": "당진시 읍내동",         "occupation": "보안",             "career_type": "경력", "education": "대학교졸업"},
    {"job_type": "QA 엔지니어",         "region": "서산시 동문동",         "occupation": "QA",               "career_type": "신입", "education": "대학교졸업"},
    {"job_type": "프로덕트 매니저",     "region": "천안시 서북구 백석동",  "occupation": "PM/기획",          "career_type": "경력", "education": "대학교졸업"},
    {"job_type": "게임 클라이언트 개발","region": "공주시 중동",           "occupation": "게임개발",         "career_type": "경력", "education": "대학교졸업"},
    {"job_type": "풀스택 개발",         "region": "아산시 배방읍",         "occupation": "풀스택",           "career_type": "경력", "education": "전문대졸업"},
]

data = [{"user_id": str(uuid.uuid4()), **s} for s in SURVEYS]

out = Path("C:/GitHub/new_git/money/results/dummy_surveys.json")
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

print(f"생성 완료: {len(data)}건 → {out}")
