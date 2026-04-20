FROM python:3.9-slim

WORKDIR /app

# 시스템 의존성
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Python 의존성
COPY ai_server_requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# 소스 복사
COPY ai_server/ ./ai_server/

# 데이터 디렉토리
RUN mkdir -p /app/ai_server/data

EXPOSE 8000

CMD ["uvicorn", "ai_server.main:app", "--host", "0.0.0.0", "--port", "8000"]
