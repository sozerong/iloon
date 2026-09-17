# 일로온 (illo-on) 프로젝트 정리

## 프로젝트 개요

| 항목 | 내용 |
|------|------|
| 프로젝트명 | 일로온 (illo-on) |
| 슬로건 | 부산 취업은 일로온! |
| 분류 | 부산 특화 AI 채용 플랫폼 |
| 개발 기간 | 약 2개월 (진행 중) |
| 팀 구성 | Front Dev, Server Dev, AI Dev × 2 |

---

## 프로젝트 목적

일반 채용 플랫폼과 차별화된 AI 기반 채용 서비스 제공:
- 이력서/포트폴리오 AI 분석 및 피드백 (타 사이트 대비 더 상세한 수준)
- 사용자 설문 + 이력서 분석 결과 기반 맞춤 공고 AI 추천
- 부산 지역 특화 AI 챗봇 (기업 정보, 지역 취업 정보 추후 강화 예정)

---

## 전체 아키텍처

```
[프론트엔드]
React → Vercel 배포
  ↕ REST API
[메인 서버]
Spring Boot → AWS 배포 (전체 총괄, BFF 역할)
  ↕ PostgreSQL (Spring DB)
  ↕ 내부 API 호출
[AI 서버]
FastAPI → Cloudtype 배포
  ↕ PostgreSQL (AI DB)
  ↕ OpenSearch (벡터 검색, 추후 적용)

[AI 파이프라인]
LangChain + Spark + pandas → PostgreSQL
Neo4j (Graph RAG, 챗봇 전용)
Airflow (배치 분석, 매일 02:00)
Kafka (실시간 이벤트 스트리밍)
```

---

## 기술 스택

| 영역 | 기술 |
|------|------|
| 프론트엔드 | React, Vercel |
| 메인 서버 | Spring Boot, AWS, PostgreSQL |
| AI 서버 | FastAPI, Cloudtype, PostgreSQL |
| AI / LLM | LangChain, Ollama LLaMA3 |
| 그래프 DB | Neo4j (Graph RAG — 챗봇 전용) |
| 벡터 검색 | OpenSearch (추후 적용) |
| 데이터 파이프라인 | Apache Spark, Apache Airflow, Apache Kafka, pandas |
| 인증 | JWT (Bearer Token), 소셜 로그인 OAuth2 |
| 배포 | AWS (Spring Boot), Vercel (React), Cloudtype (FastAPI) |

---

## 팀 역할 분담

| 역할 | 담당 |
|------|------|
| Front Dev | React UI 개발, Vercel 배포 |
| Server Dev | Spring Boot API 개발, AWS 인프라, DB 설계 |
| AI Dev (본인) | FastAPI AI 서버, 데이터 파이프라인, 더미 데이터 생성 |
| AI Dev | LangChain + Neo4j Graph RAG (챗봇) |

---

## 전체 기능

### 구직자 기능

**회원 관리**
- 이메일 회원가입 (휴대폰 본인인증 포함)
- 소셜 로그인 (네이버 / 카카오 / 애플 / 구글)
- 로그인 / 로그아웃 / 토큰 재발급
- 프로필 조회 및 수정

**온보딩 설문 (5단계)**
- 희망 직무 선택 (태그 선택, 최대 10개)
- 희망 근무 지역 선택 (부산 기준, 최대 10개)
- 현재 직업 선택
- 경력 구분 (신입 / 경력) + 경력 상세 정보
- 최종 학력 입력

**이력서 관리**
- 이력서 작성 / 수정 / 삭제 (다수 이력서 관리)
- 작성 항목: 기본정보, 경력, 학력, 수상, 자격증, 언어, 포트폴리오, 자기소개서
- AI 분석 점수 목록에서 한눈에 확인

**공고 탐색**
- 공고 검색 (키워드)
- 지역별 공고 탐색 (부산 구·군 단위)
- 공고 목록 필터 (지역 / 직무 / 경력 / 학력 / 고용형태)
- 공고 상세 (주요업무 / 자격조건 / 복리후생 / 채용전형 / 마감일 / 근무지역 지도)
- 유사 공고 추천
- 공고 북마크(스크랩)
- 스크랩 현황 요약 (새로운 공고 / 마감 임박 / 전체)

**마이페이지**
- 프로필 조회 / 수정
- 저장된 공고 목록
- 알림

### 기업 기능 *(미설계)*
- 기업 회원가입 / 로그인
- 채용공고 등록 (커스텀 채용전형 설정)
- 채용공고 수정 / 삭제 / 마감
- AI 채용공고 자동 생성 *(추후 개발 예정)*

### AI 기능

**맞춤 공고 추천 (2트랙)**
- 일반 추천: 설문 데이터 기반 PostgreSQL 필터링
- AI 추천: 이력서 분석 결과 + LLM RAG 기반 매칭 (match_score + 추천 이유 제공)
- Neural Search (OpenSearch kNN): 추후 적용 예정

**AI 이력서 / 문서 분석**
- 분석 대상: 이력서 / 자기소개서 / 포트폴리오
- 종합 점수 (0~100점) + 평균 비교
- 4축 레이더 차트 (직무적합성 / 스킬 / 경험 / 포트폴리오)
- 5가지 항목별 상세 피드백:
  1. 직무의 적합도
  2. 경험성 · 구체성
  3. 문제해결능력 · 기술역량
  4. 분석의 완성도
  5. 신뢰성 · 차별성 · 일관성
- 관련 추천활동 카드 (자격증 / 공모전 등)

**AI 챗봇**
- LangChain + Neo4j Graph RAG 기반
- 취업 관련 자유 질문 상담
- 대화 이력 저장
- 자주 묻는 질문 퀵리플라이
- 부산 특화 정보 강화 *(추후: 지역 기업 정보, 취업 지원 정책 등)*

---

## AI Dev (본인) 담당 파트 상세

### FastAPI AI 서버
- **공고 API**: 목록 조회 (필터/페이지네이션), 상세 조회, 유사 공고 추천
- **추천 API**: 설문 기반 일반 추천 / 이력서 기반 AI 추천
- **이력서 분석 API**: 문서 분석 요청, 이력 조회, 세부 결과 조회
- **챗봇 API**: 동기 응답 / SSE 스트리밍
- **설문 API**: 설문 저장 및 조회
- **로그 API**: 유저 행동 이벤트 수집 (공고 클릭, 북마크, 지원 등)
- **공고 임포트 API**: 더미 공고 JSONL → PostgreSQL / OpenSearch 적재

### 데이터 파이프라인
**더미 데이터 생성 (실제 데이터 부재로 직접 구현)**
- 채용공고 생성기: 9개 직군별 JSONL 형식 더미 공고 생성
- 유저 이벤트 생성기: 클릭 / 북마크 / 지원 이벤트 더미 생성
- JSONL → PostgreSQL + OpenSearch 자동 임포트

**배치 분석 (Apache Airflow, 매일 02:00)**
- PySpark 사용자 행동 분석 (AI 추천 vs 일반 공고 전환율 비교)
- K-Means 유저 세그멘테이션 (MLlib, 4개 클러스터)
- GBTRegressor 공고 인기도 예측 (MLlib, A/B/C/D 등급)
- 공고 트렌드 분석

**실시간 집계**
- Kafka 유저 이벤트 스트리밍 (토픽: user-events)
- Spark Structured Streaming 30초 윈도우 집계
- PostgreSQL realtime_event_stats 테이블 적재

---

## Spring ↔ FastAPI 연동 구조

Spring Boot가 BFF(Backend for Frontend) 역할로 FastAPI를 내부 호출:

| Spring API | 실제 처리 |
|------------|-----------|
| GET /api/jobs | FastAPI /jobs 호출 후 반환 |
| GET /api/jobs/{id} | FastAPI /jobs/{id} 호출 (조회수 증가) |
| GET /api/jobs/recommendations | FastAPI /recommendations/{userId}/general 호출 후 스크랩 여부 매핑 |
| GET /api/jobs/keywords | FastAPI /jobs/all 호출 후 키워드 추출, Caffeine 캐시 적용 |
| 이력서 분석 | Spring이 이력서 텍스트 조합 → FastAPI /resume/{userId}/analyze 호출 |

---

## 현재 진행 상황

| 파트 | 상태 |
|------|------|
| Spring Boot API | 인증 / 이력서 / 프로필 / 공고 BFF 구현 완료, AWS 배포 완료 (`illoon.cloud`) |
| React 디자인 | 전체 화면 디자인 완료, 개발 진행 중 |
| FastAPI AI 서버 | 공고 / 추천 / 이력서 분석 / 챗봇 / 설문 / 로그 구현 완료, Cloudtype 배포 |
| 데이터 파이프라인 | 더미 공고 생성기 / 유저 이벤트 생성기 / Spark 분석 / Airflow DAG 구현 완료 |
| Neo4j Graph RAG | 구현 중 |
| OpenSearch 추천 | 추후 적용 예정 |
| 기업 회원 기능 | 미설계 |
| AI 채용공고 자동 생성 | 추후 개발 예정 |
