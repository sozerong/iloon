# 일로온 — 챗봇 & 이력서 분석 API 상세 명세

---

## 1. AI 챗봇

### 화면 구성 요약
- 우측 하단 플로팅 위젯
- 헤더: "일리온 챗봇과 대화하기" + 접기 버튼
- 대화 내용 보관 안내 문구
- 날짜 구분선 표시
- AI 첫 인사 + 자주 묻는 질문 퀵리플라이 버튼 3개
- 하단 메시지 입력창 + 전송 버튼

---

### `POST /chat`
RAG 챗봇 — 동기 응답

**Request Body**
```json
{
  "user_id": "uuid",
  "message": "부산에서 신입 백엔드 개발자 취업하려면 어떻게 해야 해요?",
  "history": [
    { "role": "user",      "content": "이전 질문" },
    { "role": "assistant", "content": "이전 답변" }
  ]
}
```

| 필드 | 타입 | 필수 | 설명 |
|------|------|------|------|
| user_id | string | N | 로그인 유저 ID (비로그인도 사용 가능) |
| message | string | Y | 유저 입력 메시지 (최대 2000자) |
| history | array | N | 이전 대화 맥락 (role: user/assistant) |

**Response**
```json
{
  "answer": "부산 지역 신입 백엔드 취업을 위해서는...",
  "sources": ["job-uuid-1", "job-uuid-2"]
}
```

| 필드 | 타입 | 설명 |
|------|------|------|
| answer | string | AI 생성 답변 텍스트 |
| sources | string[] | 답변 근거로 참조한 공고 ID 목록 |

---

### `POST /chat/stream`
RAG 챗봇 — SSE 스트리밍 응답

> 실시간으로 텍스트가 타이핑되듯 표시될 때 사용

**Request Body** — `/chat`와 동일

**Response**
- Content-Type: `text/event-stream`
- 형식:
```
data: 부산\n\n
data: 지역\n\n
data:  신입\n\n
...
data: [DONE]\n\n
```

---

### `GET /chat/history/{user_id}`
대화 이력 조회

> 화면에 "귀찮은 상담을 위해 대화 내용이 보관됩니다" 문구 — 대화 이력 저장 필요

**Response**
```json
[
  {
    "date": "2026-04-08",
    "messages": [
      { "role": "assistant", "content": "안녕하세요. 일리온 봇입니다.", "created_at": "2026-04-08T13:00:00" },
      { "role": "user",      "content": "부산 취업 어떻게 해요?",       "created_at": "2026-04-08T13:00:10" },
      { "role": "assistant", "content": "부산 지역 취업은...",          "created_at": "2026-04-08T13:00:12" }
    ]
  }
]
```

---

### `GET /chat/quick-replies`
자주 묻는 질문 버튼 목록 조회

> 화면 상 "자주 묻는 질문" 버튼 3개 표시 — 실제 질문 내용은 추후 확정 필요

**Response**
```json
[
  { "id": 1, "label": "자주 묻는 질문 1" },
  { "id": 2, "label": "자주 묻는 질문 2" },
  { "id": 3, "label": "자주 묻는 질문 3" }
]
```

> 버튼 클릭 시 해당 label을 message로 `/chat` 또는 `/chat/stream` 호출  
> 버튼 내용이 고정값이면 프론트 하드코딩으로 처리해도 무방

---

---

## 2. 이력서 분석

### 화면 구성 요약

---

### `POST /resume/{user_id}/analyze`
문서 텍스트 → AI 분석 → 결과 저장

**Request Body**
```json
{
  "type": "resume",
  "title": "김여주의 이력서",
  "original_text": "이력서 전체 텍스트...",
  "file_url": null
}
```

| 필드 | 타입 | 필수 | 설명 |
|------|------|------|------|
| type | string | Y | `resume` / `cover_letter` / `portfolio` |
| title | string | N | 문서 제목 |
| original_text | string | N | 이력서 전문 텍스트 (resume/cover_letter) |
| file_url | string | N | 파일 URL (portfolio일 때) |

**Response**
```json
{
  "id": "doc-uuid",
  "user_id": "user-uuid",
  "type": "resume",
  "title": "김여주의 이력서",
  "ai_summary_strength": "전체적으로 이하 내용 분석해본 결과, **김여주** 님은 경력, **경험/재능/교육** 부분에서 높은 역량을 갖추고 있어요!",
  "ai_summary_weakness": "하지만 기타 **스펙, 자격증, 어학사항** 영역 부분에서는 남들보다 조금 부족해요. 관련된 추천활동을 같이 이어가면 도움이 될 거예요.",
  "total_score": 70.0,
  "created_at": "2026-06-15T00:00:00",
  "updated_at": "2026-06-15T00:00:00"
}
```

---

### `GET /resume/{user_id}/detail/{document_id}`
분석 세부 결과 조회 — 레이더 차트 + 점수 + 피드백 + 추천활동 전체

> 이력서 분석 결과 화면에서 사용하는 핵심 API

**Response**
```json
{
  "id": "doc-uuid",
  "user_id": "user-uuid",
  "type": "resume",
  "title": "김여주의 이력서",
  "ai_summary_strength": "전체적으로 이하 내용 분석해본 결과, **김여주** 님은 경력, **경험/재능/교육** 부분에서 높은 역량을 갖추고 있어요!",
  "ai_summary_weakness": "하지만 기타 **스펙, 자격증, 어학사항** 영역 부분에서는 남들보다 조금 부족해요. 관련된 추천활동을 같이 이어가면 도움이 될 거예요.",
  "total_score": 70.0,
  "average_score": 80.0,

  "scores": [
    { "category": "직무적합도",          "score": 14.0, "max_score": 20.0 },
    { "category": "경험성·구체성",        "score": 15.0, "max_score": 20.0 },
    { "category": "문제해결능력·기술역량", "score": 13.0, "max_score": 20.0 },
    { "category": "분석의완성도",         "score": 14.0, "max_score": 20.0 },
    { "category": "신뢰성·차별성·일관성", "score": 14.0, "max_score": 20.0 }
  ],

  "radar": {
    "직무적합성": 70,
    "스킬":       65,
    "경험":       75,
    "포트폴리오": 60
  },

  "feedbacks": [
    {
      "feedback_type": "overall",
      "section": null,
      "content": "전반적으로 경력, 경험/재능/교육 부분에서 높은 이력서 점수가 있어요! 하지만 기타 스펙, 자격증, 어학사항 영역 부분에서는 남들보다 조금 부족해요. 관련한 수선활동을 같이 수행하면 도움이 될 거에요."
    },
    {
      "feedback_type": "category",
      "section": "직무의 적합도",
      "content": "지원하는 공고의 직무/기술서에서 요구하는 필수 수력과 핵심 역량이 이력서에서 포트폴리오와 선행하게 되어있는 경향이 있어요. 단순히 경험을 나열하는 것이 아닌 그 이상, 회사가 찾고 있는 기술과 원하는 과거 경험에 달하게 입지하되지않은 선행하는 것이 핵심이에요."
    },
    {
      "feedback_type": "category",
      "section": "경험성·구체성",
      "content": "코드적으로 내용이 보인만큼 당당한 약점이 구체적인 항목, 그리고 그로 인해 창출된 전과을 항목화 기술하여 보세요. 특히 결과 부분에서는 막연한 표현인데 수치나 데이터(예에: 속도 20% 개선, 사용자 유입 1.5배 증가 등)를 통해서 설과이 객관성을 확보하세요."
    },
    {
      "feedback_type": "category",
      "section": "문제해결능력·기술역량",
      "content": "IT 식식이이라는 잡은의 사회의 를 기술을 눈이, 금성 가양이니스 스핵을 선생한 다수인 (나가 지식화하여 대!) 구찬 과정에서 마수현 기술의 한계나 프로뻔쩐닝 사해를 높게, 높대를 정제하는 논리적으로 찬제되어 나가는 개발자이(으)로서의 사고와 재능성 보여주세요."
    },
    {
      "feedback_type": "category",
      "section": "분석의 완성도",
      "content": "이(이)이 줄은 내용이이도 사독성이 길찾아진 막연적이 반대되요. 알찍은 말솟잖이 견결은 문장 구조을 유시하며, 발많은수 수식하는 잡아내고 식능 지뿐은 작성했어야 해요. 성겨이라면 구조가 성길어지(이)면 시작적이고 읽기 편하게 구성돼있는지 점검이 필요해요."
    },
    {
      "feedback_type": "category",
      "section": "신뢰성·차별성·일관성",
      "content": "매력마다 자시 소시체, 보, 들을기이이 내용이 이른 줄들기시 잡고 너, 내 일관된 직무 정체성을 실체해 해요. 조사드, 브랜딩 나 선공과된 문구(항목)를 사랑하고, 프린번이 줄은 신유한 결신과 나를들을 넣어 나이 이로 선택하서 더더 신뢰나서 특정성을 높게 세요."
    }
  ],

  "recommended_activities": [
    {
      "category": "언어 자격증 취득",
      "icon": "book",
      "description": "디자인 실무 현장에서 소통 능력을 증명할 수 있는 TOEIC Speaking이나 OPIc 응시 과목을 추천해요."
    },
    {
      "category": "디자인 자격증 취득",
      "icon": "certificate",
      "description": "GTQ 1급이나 컴퓨터그래픽스운용기능사 등 공인된 자격증은 본인이 이를 활용 능력임을 증명해 보세요."
    },
    {
      "category": "디자인 공모전 수상",
      "icon": "trophy",
      "description": "수상 경력은 기업이 소재 영역을 가장 원하게 제가알 수 있는 요소입니다."
    }
  ]
}
```

---

### `GET /resume/{user_id}/history`
분석 이력 목록 조회

- Query: `type` (resume / cover_letter / portfolio)

**Response**
```json
[
  {
    "id": "doc-uuid",
    "type": "resume",
    "title": "김여주의 이력서",
    "total_score": 70.0,
    "created_at": "2026-06-15T00:00:00",
    "updated_at": "2026-06-15T00:00:00"
  }
]
```

---

### `GET /resume/{user_id}/latest`
최신 분석 문서 조회

- Query: `type` (기본값 `resume`)
- Response: `/detail/{document_id}`와 동일한 구조

---

## 응답 필드 정리

### scores 카테고리 목록

| category | max_score | 화면 표시명 |
|----------|-----------|------------|
| 직무적합도 | 20 | 1. 직무의 적합도 |
| 경험성·구체성 | 20 | 2. 경험성 - 구체성 |
| 문제해결능력·기술역량 | 20 | 3. 문제해결능력 - 기술역량 |
| 분석의완성도 | 20 | 4. 분석의 완성도 |
| 신뢰성·차별성·일관성 | 20 | 5. 신뢰성 - 차별성 - 일관성 |

### radar 축 목록 (4축 다이아몬드 형태)
| key | 설명 |
|-----|------|
| 직무적합성 | 직무적합도 점수 기반 |
| 스킬 | 문제해결능력·기술역량 점수 기반 |
| 경험 | 경험성·구체성 점수 기반 |
| 포트폴리오 | 포트폴리오/자격증 보유 여부 반영 |

### recommended_activities icon 값
| icon | 화면 아이콘 |
|------|------------|
| book | 책 아이콘 |
| certificate | 문서/자격증 아이콘 |
| trophy | 트로피 아이콘 |

---

## Spring ↔ FastAPI 연동 흐름

### 이력서 분석 요청 흐름
```
프론트
  → Spring: GET /api/resumes/{resumeId}  (이력서 텍스트 조회)
  → Spring: POST /api/resume/analyze     (Spring이 FastAPI 호출)
      └→ FastAPI: POST /resume/{user_id}/analyze
  → 프론트: 분석 결과 표시
```

> Spring이 이력서 DB에서 텍스트 조합 후 FastAPI에 `original_text`로 전달

### 챗봇 호출 흐름
```
프론트 → FastAPI: POST /chat/stream  (직접 호출 권장 — 스트리밍 특성상 Spring 프록시 비효율)
```
