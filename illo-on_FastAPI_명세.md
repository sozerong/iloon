# 일로온 FastAPI API 명세 (Spring 회의용)

> Base URL: `http://fastapi-server/api/v1`  
> 모든 응답은 JSON, 인증은 Spring이 처리 후 `user_id`를 경로 파라미터로 전달

---

## 회의에서 결정해야 할 것들

| 항목 | 선택지 | 결정 필요 |
|------|--------|-----------|
| FastAPI 호출 주체 | Spring이 프록시 OR 프론트가 직접 호출 | ✅ |
| user_id 전달 방식 | Spring JWT에서 추출 후 전달 OR 프론트에서 직접 | ✅ |
| 공고 DB 접근 | Spring / FastAPI 같은 DB 공유 OR 별도 | ✅ |
| 공고 등록 방식 | 어드민 UI 없음 → 요청 받아 더미 생성기로 직접 넣기 | ✅ |

---

## 1. 설문

### `POST /survey/{user_id}`
설문 저장 (없으면 생성, 있으면 업데이트)

**Request Body**
```json
{
  "job_type": "백엔드 개발",
  "region": "부산 해운대구",
  "occupation": "IT/개발",
  "career_type": "신입",
  "education": "대졸"
}
```

**Response**
```json
{
  "id": "uuid",
  "user_id": "uuid",
  "job_type": "백엔드 개발",
  "region": "부산 해운대구",
  "occupation": "IT/개발",
  "career_type": "신입",
  "education": "대졸",
  "created_at": "2026-06-15T00:00:00",
  "updated_at": "2026-06-15T00:00:00"
}
```

### `GET /survey/{user_id}`
저장된 설문 조회

---

## 2. 공고 추천

### `POST /recommendations/{user_id}/general`
설문 기반 일반 추천 갱신 및 반환

- Query: `top_k` (기본 10)
- 설문 데이터 기준으로 PostgreSQL에서 필터링 후 저장

**Response** — 공고 목록 (JobListItem 배열)
```json
[
  {
    "id": "uuid",
    "job_id": "uuid",
    "user_id": "uuid",
    "created_at": "2026-06-15T00:00:00"
  }
]
```

### `GET /recommendations/{user_id}/general`
저장된 일반 추천 공고 목록 조회 (공고 상세 포함)

### `POST /recommendations/{user_id}/ai`
이력서 분석 기반 AI 추천 갱신 (LLM 호출 — 응답 느릴 수 있음)

- Query: `top_k` (기본 5)
- 이력서 분석 결과 + LLM 기반 매칭 → match_score, reason 포함

**Response**
```json
[
  {
    "id": "uuid",
    "user_id": "uuid",
    "job_id": "uuid",
    "match_score": 87.5,
    "reason": "React 경험과 UI 역량이 공고 요건과 높은 일치도를 보입니다.",
    "created_at": "2026-06-15T00:00:00"
  }
]
```

### `GET /recommendations/{user_id}/ai`
저장된 AI 추천 목록 조회

---

## 3. 이력서 / 문서 분석

### `POST /resume/{user_id}/analyze`
문서 텍스트 → AI 분석 → 점수 + 피드백 저장

**Request Body**
```json
{
  "type": "resume",
  "title": "김여주의 이력서",
  "original_text": "이력서 전문 텍스트...",
  "file_url": null
}
```
- `type`: `resume` | `cover_letter` | `portfolio`
- `file_url`: 포트폴리오 파일 URL (portfolio 타입일 때)

**Response**
```json
{
  "id": "uuid",
  "user_id": "uuid",
  "type": "resume",
  "title": "김여주의 이력서",
  "ai_summary": "AI가 생성한 요약...",
  "total_score": 70.0,
  "created_at": "2026-06-15T00:00:00",
  "updated_at": "2026-06-15T00:00:00"
}
```

### `GET /resume/{user_id}/history`
문서 분석 이력 목록 조회

- Query: `type` (resume | cover_letter | portfolio, 선택)

### `GET /resume/{user_id}/latest`
가장 최근 분석 문서 조회

- Query: `type` (기본 resume)

### `GET /resume/{user_id}/detail/{document_id}`
문서 세부 조회 — 세부 점수 + AI 피드백 포함

**Response**
```json
{
  "id": "uuid",
  "total_score": 70.0,
  "scores": [
    { "category": "직무적합도", "score": 15.0, "max_score": 20.0 },
    { "category": "경험성·구체성", "score": 14.0, "max_score": 20.0 },
    { "category": "문제해결능력·기술역량", "score": 13.0, "max_score": 20.0 },
    { "category": "분석의완성도", "score": 14.0, "max_score": 20.0 },
    { "category": "신뢰성·차별성·일관성", "score": 14.0, "max_score": 20.0 }
  ],
  "feedbacks": [
    {
      "feedback_type": "overall",
      "section": null,
      "content": "전반적으로 경력 기술이 구체적이나 포트폴리오 보완이 필요합니다.",
      "model_version": "llama3"
    }
  ]
}
```

---

## 4. AI 챗봇

### `POST /chat`
RAG 챗봇 (동기 응답)

**Request Body**
```json
{
  "user_id": "uuid",
  "message": "부산에서 신입 백엔드 취업 어떻게 해야 해요?",
  "history": [
    { "role": "user", "content": "이전 질문" },
    { "role": "assistant", "content": "이전 답변" }
  ]
}
```

**Response**
```json
{
  "answer": "부산 지역 IT 채용은...",
  "sources": ["job-uuid-1", "job-uuid-2"]
}
```

### `POST /chat/stream`
RAG 챗봇 스트리밍 (SSE)

- Content-Type: `text/event-stream`
- 응답: `data: {chunk}\n\n` 형식으로 청크 전송, 종료 시 `data: [DONE]`

---

## 5. 공고

### `GET /jobs`
공고 목록 조회 (필터 + 페이지네이션)

- Query: `keyword`, `location`, `job_type`, `career_type`, `education`, `page`(기본 1), `size`(기본 20)

**Response**
```json
{
  "total": 150,
  "page": 1,
  "size": 20,
  "items": [ /* JobListItem 배열 */ ]
}
```

### `GET /jobs/{job_id}`
공고 상세 조회 (조회수 자동 증가)

### `GET /jobs/{job_id}/similar`
유사 공고 추천

- Query: `top_k` (기본 6)

---

## 6. 유저 행동 로그

> Spring 또는 프론트가 직접 호출 — 분석 파이프라인용 데이터 수집

### `POST /logs/{user_id}/event`
유저 이벤트 기록

**Request Body**
```json
{
  "event_type": "job_detail_view",
  "job_id": "uuid",
  "is_ai_recommended": true,
  "match_score": 87.5,
  "region_sido": "부산",
  "time_on_page_sec": 45,
  "session_id": "session-uuid",
  "session_duration": 300
}
```

| event_type | 필수 추가 필드 |
|------------|--------------|
| `job_detail_view` | job_id, is_ai_recommended, session_id |
| `bookmark` | job_id, is_ai_recommended |
| `apply_click` | job_id, is_ai_recommended |
| `search_job` | meta (키워드 등) |
| `analyze_document` | target_type, target_id |
| `view_feedback` | target_type, target_id |
| `submit_survey` | — |

---

## 공고 DB 스키마 (공유 테이블)

Spring과 FastAPI가 공통으로 읽는 `jobs` 테이블 주요 필드:

| 필드 | 타입 | 설명 |
|------|------|------|
| id | UUID | PK |
| title | TEXT | 공고 제목 |
| company | TEXT | 회사명 |
| location | TEXT | 근무지역 |
| job_type | TEXT | 직무 |
| career_type | TEXT | 신입/경력 |
| education | TEXT | 학력 |
| salary | TEXT | 급여 |
| description | TEXT | 상세 내용 |
| requirements | TEXT | 자격조건 |
| preferred | TEXT | 우대조건 |
| benefits | TEXT | 복리후생 |
| process | TEXT | 채용전형 |
| deadline | TEXT | 마감일 |
| view_count | INT | 조회수 |
| created_at | TIMESTAMP | 등록일 |
