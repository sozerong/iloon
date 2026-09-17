"""
OpenSearch 인덱스 생성 스크립트

실행:
  python setup_opensearch.py
"""

import sys
import requests

HOST       = "http://localhost:9200"
INDEX_NAME = "iloon_jobs"

MAPPING = {
    "mappings": {
        "properties": {
            "id":           {"type": "keyword"},
            "title":        {"type": "text"},
            "company":      {"type": "keyword"},
            "location":     {"type": "keyword"},
            "job_type":     {"type": "keyword"},
            "occupation":   {"type": "keyword"},
            "career_type":  {"type": "keyword"},
            "education":    {"type": "keyword"},
            "salary":       {"type": "keyword"},
            "description":  {"type": "text"},
            "requirements": {"type": "text"},
            "preferred":    {"type": "text"},
            "benefits":     {"type": "text"},
            "deadline":     {"type": "keyword"},
            "source":       {"type": "keyword"},
            "url":          {"type": "keyword"},
            "view_count":   {"type": "integer"},
            "created_at":   {"type": "date"},
        }
    }
}

# 연결 확인
try:
    r = requests.get(f"{HOST}/_cluster/health", timeout=5)
    print(f"✅ OpenSearch 연결 성공 (status: {r.json().get('status')})")
except Exception as e:
    print(f"❌ OpenSearch 연결 실패: {e}")
    sys.exit(1)

# 인덱스 이미 있으면 스킵
r = requests.head(f"{HOST}/{INDEX_NAME}", timeout=5)
if r.status_code == 200:
    print(f"ℹ️  인덱스 '{INDEX_NAME}' 이미 존재")
    sys.exit(0)

# 인덱스 생성
r = requests.put(f"{HOST}/{INDEX_NAME}", json=MAPPING, timeout=10)
if r.status_code in (200, 201):
    print(f"✅ 인덱스 '{INDEX_NAME}' 생성 완료")
else:
    print(f"❌ 생성 실패: {r.text}")
    sys.exit(1)
