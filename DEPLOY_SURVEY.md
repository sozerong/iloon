# Survey 서버 Cloudtype 배포 가이드

## 개요

Survey 전용 경량 서버입니다. OpenSearch / Ollama 없이 **PostgreSQL만** 연결하면 동작합니다.

**제공 API**
- `POST /api/v1/survey/{user_id}` — 설문 저장 (없으면 생성, 있으면 업데이트)
- `GET  /api/v1/survey/{user_id}` — 설문 조회
- `GET  /health` — 헬스체크

---

## Cloudtype 배포 절차

### 1. GitHub 푸시

```bash
git add ai_server/main_survey.py Dockerfile.survey requirements_survey.txt
git commit -m "feat: survey 전용 경량 서버 추가"
git push
```

### 2. Cloudtype 프로젝트 생성

1. [Cloudtype](https://cloudtype.io) 로그인
2. **새 서비스 → GitHub 연결** → 이 레포 선택
3. **Dockerfile 경로** 항목에 `Dockerfile.survey` 입력
4. 포트: `8000`

### 3. 환경변수 설정

| 변수명 | 값 | 설명 |
|---|---|---|
| `PG_HOST` | (Cloudtype PostgreSQL 호스트) | DB 호스트 |
| `PG_PORT` | `5432` | DB 포트 |
| `PG_USER` | (DB 사용자명) | |
| `PG_PASSWORD` | (DB 비밀번호) | |

> **Cloudtype 내부 PostgreSQL 사용 시:** 서비스 연결 탭에서 PostgreSQL을 연결하면  
> 환경변수가 자동 주입됩니다. 변수명이 다를 경우 위 이름으로 매핑해주세요.

### 4. 배포 확인

배포 완료 후 아래 URL로 헬스체크:

```
https://<your-domain>/health
# → {"status": "ok", "service": "일로온 Survey 서버"}
```

API 문서:
```
https://<your-domain>/docs
```

---

## 로컬 테스트 (Docker)

```bash
# 빌드
docker build -f Dockerfile.survey -t iloon-survey .

# 실행 (환경변수는 실제 값으로 교체)
docker run -p 8000:8000 \
  -e PG_HOST=localhost \
  -e PG_PORT=5432 \
  -e PG_USER=airflow \
  -e PG_PASSWORD=airflow \
  iloon-survey

# 헬스체크
curl http://localhost:8000/health
```
