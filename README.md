# 일로온 (Iloon) — AI 기반 채용 공고 추천 플랫폼

> **PySpark MLlib + OpenSearch Neural Search + Kafka + Airflow**를 활용한  
> 실시간 행동 분석 및 AI 추천 시스템 포트폴리오 프로젝트

---

## 아키텍처

```
┌─────────────────────────────────────────────────────────────┐
│                      Data Pipeline                          │
│                                                             │
│  Job Scraper ──→ PostgreSQL ──→ OpenSearch (Neural Index)   │
│       ↓                                                     │
│  User Event Generator ──→ Kafka ──→ Spark Streaming         │
│       ↓                                                     │
│  Airflow DAG (매일 02:00)                                   │
│    ├── 사용자 행동 분석 (Spark)                              │
│    ├── K-Means 세그멘테이션 (MLlib)                         │
│    ├── 공고 트렌드 분석 (Spark)                              │
│    └── 공고 인기도 예측 GBTRegressor (MLlib)                │
│                  ↓                                          │
│         Superset 대시보드                                   │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                     AI Server (FastAPI)                     │
│                                                             │
│  POST /api/v1/survey/{user_id}                              │
│  POST /api/v1/recommendations/{user_id}/general             │
│    └── OpenSearch Neural Search (MiniLM-L12 384-dim)        │
│  POST /api/v1/recommendations/{user_id}/ai                  │
│    └── Ollama LLaMA3 RAG                                    │
│  POST /api/v1/resume/analyze                                │
│    └── LLM 이력서 분석                                      │
└─────────────────────────────────────────────────────────────┘
```

---

## 기술 스택

| 영역 | 기술 |
|------|------|
| **AI 서버** | FastAPI, Python 3.9, asyncpg, SQLAlchemy 2.0 |
| **벡터 검색** | OpenSearch 2.13.0, ML Commons, Neural Search |
| **임베딩 모델** | `paraphrase-multilingual-MiniLM-L12-v2` (384차원) |
| **LLM** | Ollama LLaMA3 (RAG 기반 추천 설명) |
| **데이터 처리** | PySpark 3.x, MLlib (K-Means, GBTRegressor) |
| **스트리밍** | Kafka (KRaft), Spark Structured Streaming |
| **워크플로우** | Apache Airflow 2.x (LocalExecutor) |
| **데이터베이스** | PostgreSQL 15 |
| **시각화** | Apache Superset 3.1 |
| **컨테이너** | Docker Compose |

---

## 주요 기능

### 1. AI 공고 추천 (Neural Search)
- 사용자 설문(직무/지역/경력) → 쿼리 텍스트 생성
- OpenSearch Neural Search (kNN) 로 의미 기반 공고 매칭
- Ollama LLaMA3 RAG로 추천 이유 자연어 생성

### 2. 사용자 행동 분석 (PySpark)
- AI 추천 공고 vs 일반 공고 클릭·저장·지원 전환율 비교
- 지역별 / 매칭 점수 구간별 지원 전환율
- 일별 AI 추천 효과 트렌드

### 3. K-Means 사용자 세그멘테이션 (MLlib)
- 피처: 조회수, 북마크율, 지원율, AI 추천 조회 비율, 세션 체류 시간
- 4개 클러스터 자동 분류: 적극_지원형 / AI_선호형 / 탐색형 / 소극형
- PostgreSQL `user_segments` 테이블 저장

### 4. GBT 공고 인기도 예측 (MLlib)
- 피처: 직군, 경력, 기업 규모, 지역, 연봉, 기술스택 수, 조회·북마크 수
- GBTRegressor로 지원율(%) 예측
- 인기도 등급 A/B/C/D 분류 → `job_popularity` 테이블 저장

### 5. 실시간 이벤트 집계 (Kafka + Spark Streaming)
- Kafka 토픽 `user-events` 30초 윈도우 집계
- 이벤트 유형별 × AI 추천 여부별 실시간 통계
- PostgreSQL `realtime_event_stats` 테이블 적재

### 6. 분석 결과 시각화 (Superset)
- 세그먼트별 사용자 분포 (파이 차트)
- 직군별 공고 인기도 (막대 차트)
- 인기도 Top 10 공고 테이블

---

## 서비스 포트

| 서비스 | 포트 | 접속 정보 |
|--------|------|-----------|
| AI Server (FastAPI) | `8000` | `/docs` 로 Swagger UI |
| Airflow | `8080` | admin / admin |
| Superset | `8088` | admin / admin |
| Kafka UI | `8090` | — |
| OpenSearch | `9200` | — |
| OpenSearch Dashboards | `5601` | — |
| Ollama | `11434` | — |

---

## 실행 방법

### 전체 서비스 실행

```bash
# 환경 변수 설정 (선택)
export ANTHROPIC_API_KEY=sk-ant-...

# 서비스 기동
docker-compose up -d

# Airflow 초기화 완료 대기 (약 30초)
docker-compose logs -f airflow-init
```

### ML 모델 등록 (OpenSearch Neural Search)

```bash
# OpenSearch 완전 기동 후 실행
python setup_opensearch.py
```

### Superset 대시보드 자동 설정

```bash
# Superset 기동 후 실행 (약 30~60초 대기)
python setup_superset.py
```

### 동작 확인

```bash
# OpenSearch 직접 확인 (공고 수, 벡터, 검색)
python test_opensearch.py

# 추천 시스템 엔드투엔드 테스트
python test_recommendation.py --survey backend --user-id test-user-001
python test_recommendation.py --survey ai      --user-id test-user-002
```

### 분석 스크립트 단독 실행

```bash
# 사용자 행동 분석
python analyze_ai_vs_normal.py --step all

# 공고 트렌드 분석
python analyze_job_trends.py --step all

# K-Means 세그멘테이션
python analyze_user_segmentation.py --k 4

# GBT 공고 인기도 예측
python analyze_job_popularity.py --step all

# Kafka 실시간 스트리밍 (spark-submit 필요)
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  stream_events.py
```

---

## 프로젝트 구조

```
money/
├── ai_server/                    # FastAPI AI 서버
│   ├── main.py
│   ├── routers/
│   │   ├── survey.py             # 설문 저장
│   │   ├── recommendations.py   # 공고 추천 (Neural / RAG)
│   │   ├── resume.py            # 이력서 분석
│   │   └── chat.py              # LLM 채팅
│   └── services/
│       ├── opensearch_service.py # Neural Search + ML 모델 관리
│       ├── recommendation_service.py
│       ├── rag_service.py
│       └── ollama_client.py
│
├── dags/                         # Airflow DAG
│   ├── ai_analysis_dag.py        # 메인 분석 파이프라인
│   ├── job_scraper_dag.py
│   └── user_event_dag.py
│
├── analyze_ai_vs_normal.py       # Spark: 사용자 행동 분석 (5단계)
├── analyze_job_trends.py         # Spark: 공고 트렌드 분석 (6단계)
├── analyze_user_segmentation.py  # Spark MLlib: K-Means 세그멘테이션
├── analyze_job_popularity.py     # Spark MLlib: GBT 인기도 예측
├── stream_events.py              # Spark Streaming: Kafka 실시간 집계
│
├── user_event_generator.py       # 사용자 이벤트 더미 생성기 (Kafka 발행 포함)
├── generator_backend.py          # 직군별 공고 더미 생성기
├── generator_frontend.py
├── generator_ai_ml.py
│   ... (총 9개 직군)
│
├── setup_opensearch.py           # OpenSearch ML 모델 자동 등록
├── setup_superset.py             # Superset 대시보드 자동 설정
├── test_opensearch.py            # OpenSearch 동작 확인
├── test_recommendation.py        # 추천 시스템 E2E 테스트
│
├── docker-compose.yml            # 전체 서비스 정의
├── Dockerfile                    # Airflow 커스텀 이미지
├── requirements.txt
└── init-db.sql                   # PostgreSQL 초기 스키마
```

---

## 데이터 흐름

```
[공고 수집]
job_scraper (Playwright) → PostgreSQL (iloon_jobs)
                        → OpenSearch (iloon_jobs 인덱스 + 임베딩 벡터)

[사용자 이벤트]
user_event_generator.py → JSONL 파일 + Kafka(user-events)
                       ↓
              Spark Structured Streaming
                       ↓
              PostgreSQL (realtime_event_stats)

[배치 분석 — Airflow 매일 02:00]
JSONL 파일 → PySpark
  ├── 행동 분석 결과  → results/analysis*.json
  ├── user_segments  → PostgreSQL
  ├── 트렌드 결과    → results/trend*.json
  └── job_popularity → PostgreSQL + results/job_popularity_scores.json

[AI 추천 요청]
POST /survey/{uid} → 설문 저장
POST /recommendations/{uid}/general
  → OpenSearch Neural kNN → 상위 K개 공고 반환
POST /recommendations/{uid}/ai
  → RAG (공고 컨텍스트 + LLaMA3) → 맞춤 추천 설명
```

---

## 개발 포인트

- **Python 3.9 호환**: `List`, `Tuple`, `Dict` from `typing` 모듈 사용 (PEP 585 미적용)
- **OpenSearch Neural Search**: SHA256 해시 + huggingface/ prefix 두 단계 모델 등록
- **메모리 최적화**: OpenSearch JVM 2g, ml_commons jvm threshold 99%
- **Kafka KRaft 모드**: Zookeeper 없이 단일 노드 브로커 운영
- **GBT 파이프라인**: StringIndexer → VectorAssembler → StandardScaler → GBTRegressor

---

## License

MIT
