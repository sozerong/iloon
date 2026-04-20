"""
OpenSearch 서비스 — Neural Search (벡터 기반 시맨틱 검색)

흐름:
  1. 서버 시작 시 ensure_ml_ready() 호출
     → 멀티링구얼 임베딩 모델 등록/배포 (최초 1회, 이후 재사용)
  2. ensure_index() — knn_vector 필드 + ingest pipeline 포함 인덱스 생성
  3. bulk_index_jobs() — 공고 인덱싱 (pipeline이 search_text → embedding_vector 자동 변환)
  4. neural_search() — 쿼리 텍스트 → 벡터 변환 → kNN 검색 (OpenSearch 내부 처리)
  5. search_jobs() — 기존 키워드 검색 (fallback 용)

임베딩 모델:
  sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2
  - 384차원, 한국어 포함 다국어 지원
  - OpenSearch pre-packaged 모델 (별도 외부 API 불필요)
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Dict, List, Optional

import httpx

from ..config import OPENSEARCH_HOST, JOBS_INDEX

logger = logging.getLogger(__name__)

# ── ML 설정 ───────────────────────────────────────────────────
# OpenSearch pre-trained 모델명은 huggingface/ prefix 필요
_ML_MODEL_NAME        = "huggingface/sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
_ML_MODEL_NAME_SHORT  = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
_ML_MODEL_VERSION     = "1.0.1"
_VECTOR_DIM           = 384
_PIPELINE_ID          = "iloon-job-pipeline"
_VECTOR_FIELD         = "embedding_vector"
_TEXT_FIELD           = "search_text"

# URL 방식 등록 시 필요한 SHA256 해시
_ML_MODEL_URL = (
    "https://artifacts.opensearch.org/models/ml-models/huggingface/"
    "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2/"
    "1.0.1/torch_script/"
    "sentence-transformers_paraphrase-multilingual-MiniLM-L12-v2-1.0.1-torch_script.zip"
)
_ML_MODEL_HASH = "a2ae3c4f161bd8e5a99a19ba5589443d33a120bb2bd67aa9da102c8b201f1277"

# 배포된 model_id 캐시 (프로세스 내 재사용)
_model_id: Optional[str] = None


# ── 내부 유틸 ─────────────────────────────────────────────────
async def _get(path: str, **kw) -> Dict:
    async with httpx.AsyncClient(timeout=30.0) as c:
        r = await c.get(f"{OPENSEARCH_HOST}{path}", **kw)
        r.raise_for_status()
        return r.json()


async def _post(path: str, body: Any = None, **kw) -> Dict:
    async with httpx.AsyncClient(timeout=30.0) as c:
        r = await c.post(f"{OPENSEARCH_HOST}{path}", json=body, **kw)
        r.raise_for_status()
        return r.json()


async def _put(path: str, body: Any = None, **kw) -> Dict:
    async with httpx.AsyncClient(timeout=30.0) as c:
        r = await c.put(f"{OPENSEARCH_HOST}{path}", json=body, **kw)
        r.raise_for_status()
        return r.json()


async def _delete(path: str, **kw) -> None:
    async with httpx.AsyncClient(timeout=30.0) as c:
        await c.delete(f"{OPENSEARCH_HOST}{path}", **kw)


async def _poll_task(task_id: str, timeout: int = 300) -> Dict:
    """ML 태스크 완료까지 폴링 (최대 timeout 초)"""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        data = await _get(f"/_plugins/_ml/tasks/{task_id}")
        state = data.get("state", "")
        logger.debug("ML 태스크 %s 상태: %s", task_id, state)
        if state == "COMPLETED":
            return data
        if state in ("FAILED", "STOPPED"):
            raise RuntimeError(f"ML 태스크 실패: {task_id} → {state}\n{data}")
        await asyncio.sleep(5)
    raise TimeoutError(f"ML 태스크 타임아웃: {task_id}")


# ── ML 모델 등록/배포 ─────────────────────────────────────────
async def _find_deployed_model() -> Optional[str]:
    """이미 DEPLOYED 상태인 모델 ID 반환 (모든 유사 이름 포함)"""
    try:
        # short name 과 full name 둘 다 검색
        for name in (_ML_MODEL_NAME, _ML_MODEL_NAME_SHORT):
            data = await _post(
                "/_plugins/_ml/models/_search",
                {"query": {"match": {"name": name}}, "size": 10},
            )
            for hit in data.get("hits", {}).get("hits", []):
                src = hit.get("_source", {})
                if src.get("model_state") == "DEPLOYED":
                    mid = hit["_id"]
                    logger.info("기존 배포 모델 발견: %s (name=%s)", mid, name)
                    return mid
    except Exception as e:
        logger.debug("모델 검색 실패 (무시): %s", e)
    return None


async def _create_model_group() -> str:
    """모델 그룹 생성 (OpenSearch 2.9+ 필수)"""
    try:
        data = await _post(
            "/_plugins/_ml/model_groups/_register",
            {"name": "iloon-nlp", "description": "일로온 NLP 임베딩 모델"},
        )
        return data.get("model_group_id", "")
    except Exception:
        # 이미 존재하면 검색으로 가져오기
        try:
            data = await _post(
                "/_plugins/_ml/model_groups/_search",
                {"query": {"match": {"name": "iloon-nlp"}}},
            )
            hits = data.get("hits", {}).get("hits", [])
            return hits[0]["_id"] if hits else ""
        except Exception:
            return ""


async def _register_model() -> str:
    """
    모델 등록 → model_id 반환.
    1차: pre-trained 방식 (name/version, OpenSearch 자체 다운로드)
    2차: URL + SHA256 해시 방식 (fallback)
    """
    logger.info("ML 모델 등록 시작: %s", _ML_MODEL_NAME)
    group_id = await _create_model_group()

    # ── 1차: pre-trained 방식 ─────────────────────────────────
    try:
        body_pretrained: Dict[str, Any] = {
            "name":         _ML_MODEL_NAME,
            "version":      _ML_MODEL_VERSION,
            "model_format": "TORCH_SCRIPT",
        }
        if group_id:
            body_pretrained["model_group_id"] = group_id

        data    = await _post("/_plugins/_ml/models/_register", body_pretrained)
        task_id = data["task_id"]
        logger.info("pre-trained 등록 태스크: %s", task_id)
        result   = await _poll_task(task_id, timeout=600)
        model_id = result["model_id"]
        logger.info("pre-trained 모델 등록 완료: %s", model_id)
        return model_id
    except Exception as e:
        logger.warning("pre-trained 등록 실패, URL 방식으로 재시도: %s", e)

    # ── 2차: URL + SHA256 해시 방식 ───────────────────────────
    body_url: Dict[str, Any] = {
        "name":                    _ML_MODEL_NAME,
        "version":                 _ML_MODEL_VERSION,
        "model_format":            "TORCH_SCRIPT",
        "model_config": {
            "model_type":          "bert",
            "embedding_dimension": _VECTOR_DIM,
            "framework_type":      "sentence_transformers",
        },
        "url":                     _ML_MODEL_URL,
        "model_content_hash_value": _ML_MODEL_HASH,
    }
    if group_id:
        body_url["model_group_id"] = group_id

    data     = await _post("/_plugins/_ml/models/_register", body_url)
    task_id  = data["task_id"]
    logger.info("URL 방식 등록 태스크: %s (최대 10분 소요)", task_id)
    result   = await _poll_task(task_id, timeout=600)
    model_id = result["model_id"]
    logger.info("URL 방식 모델 등록 완료: %s", model_id)
    return model_id


async def _deploy_model(model_id: str) -> None:
    """모델 배포"""
    logger.info("ML 모델 배포 시작: %s", model_id)
    data    = await _post(f"/_plugins/_ml/models/{model_id}/_deploy")
    task_id = data["task_id"]
    await _poll_task(task_id, timeout=120)
    logger.info("ML 모델 배포 완료: %s", model_id)


async def ensure_ml_ready() -> Optional[str]:
    """
    ML 모델이 배포된 상태인지 확인하고 model_id 반환.
    최초 실행 시 등록 + 배포 (수 분 소요), 이후엔 캐시 반환.
    실패해도 예외 전파하지 않고 None 반환 — 시스템은 계속 동작.
    """
    global _model_id
    if _model_id:
        return _model_id

    try:
        # 이미 배포된 모델이 있으면 재사용
        model_id = await _find_deployed_model()

        if not model_id:
            model_id = await _register_model()
            await _deploy_model(model_id)

        _model_id = model_id
        logger.info("ML 모델 준비 완료: %s", _model_id)
        return _model_id

    except Exception as e:
        logger.error("ML 모델 준비 실패 (키워드 검색으로 fallback): %s", e)
        return None


# ── Ingest Pipeline ───────────────────────────────────────────
async def _ensure_pipeline(model_id: str) -> None:
    """공고 인덱싱 시 자동 임베딩 생성 파이프라인"""
    try:
        await _get(f"/_ingest/pipeline/{_PIPELINE_ID}")
        logger.debug("Pipeline '%s' 이미 존재", _PIPELINE_ID)
        return
    except httpx.HTTPStatusError:
        pass

    await _put(
        f"/_ingest/pipeline/{_PIPELINE_ID}",
        {
            "description": "일로온 공고 임베딩 파이프라인",
            "processors": [
                {
                    "text_embedding": {
                        "model_id":  model_id,
                        "field_map": {_TEXT_FIELD: _VECTOR_FIELD},
                    }
                }
            ],
        },
    )
    logger.info("Pipeline '%s' 생성 완료", _PIPELINE_ID)


# ── 인덱스 매핑 ───────────────────────────────────────────────
def _build_mapping(with_knn: bool = True) -> Dict:
    props: Dict[str, Any] = {
        "id":           {"type": "keyword"},
        _TEXT_FIELD:    {"type": "text"},
        "title":        {"type": "text", "analyzer": "standard"},
        "company":      {"type": "keyword"},
        "location":     {"type": "keyword"},
        "job_type":     {"type": "keyword"},
        "occupation":   {"type": "keyword"},
        "career_type":  {"type": "keyword"},
        "education":    {"type": "keyword"},
        "salary":       {"type": "keyword"},
        "description":  {"type": "text", "analyzer": "standard"},
        "requirements": {"type": "text", "analyzer": "standard"},
        "preferred":    {"type": "text"},
        "benefits":     {"type": "text"},
        "deadline":     {"type": "keyword"},
        "source":       {"type": "keyword"},
        "url":          {"type": "keyword"},
        "view_count":   {"type": "integer"},
        "created_at":   {"type": "date"},
    }

    if with_knn:
        props[_VECTOR_FIELD] = {
            "type":      "knn_vector",
            "dimension": _VECTOR_DIM,
            "method": {
                "name":       "hnsw",
                "space_type": "cosinesimil",
                "engine":     "lucene",
            },
        }

    settings: Dict[str, Any] = {"analysis": {"analyzer": {"korean": {"type": "standard"}}}}
    if with_knn:
        settings["index"] = {
            "knn": True,
            "knn.algo_param.ef_search": 100,
        }
        if _model_id:
            settings["index"]["default_pipeline"] = _PIPELINE_ID

    return {"settings": settings, "mappings": {"properties": props}}


# ── 인덱스 생성/갱신 ──────────────────────────────────────────
async def ensure_index(model_id: Optional[str] = None) -> None:
    """
    인덱스 생성 또는 검증.
    knn_vector 필드가 없는 기존 인덱스는 삭제 후 재생성.
    """
    mid = model_id or _model_id

    async with httpx.AsyncClient(timeout=10.0) as c:
        resp = await c.head(f"{OPENSEARCH_HOST}/{JOBS_INDEX}")
        exists = (resp.status_code == 200)

    if exists:
        try:
            mapping = await _get(f"/{JOBS_INDEX}/_mapping")
            props = (
                mapping.get(JOBS_INDEX, {})
                       .get("mappings", {})
                       .get("properties", {})
            )
            if _VECTOR_FIELD in props:
                logger.debug("인덱스 '%s' kNN 준비 완료", JOBS_INDEX)
                return
            # ML 모델 없으면 기존 인덱스 그대로 사용 (삭제 안 함)
            if not mid:
                logger.debug("인덱스 '%s' 기존 사용 (knn 미적용)", JOBS_INDEX)
                return
            # ML 모델 있는데 knn_vector 없으면 재생성
            logger.info("knn_vector 없음 → 재생성")
            await _delete(f"/{JOBS_INDEX}")
        except Exception as e:
            logger.warning("인덱스 매핑 확인 실패: %s", e)
            return

    # pipeline 먼저 생성
    if mid:
        await _ensure_pipeline(mid)

    mapping = _build_mapping(with_knn=bool(mid))
    await _put(f"/{JOBS_INDEX}", mapping)
    logger.info("OpenSearch 인덱스 '%s' 생성 완료 (knn=%s)", JOBS_INDEX, bool(mid))


# ── 검색 텍스트 생성 ──────────────────────────────────────────
def build_search_text(job: Dict[str, Any]) -> str:
    """공고 필드 → 임베딩용 단일 텍스트"""
    parts = [
        job.get("title",       ""),
        job.get("job_type",    ""),
        job.get("occupation",  ""),
        job.get("career_type", ""),
        job.get("location",    ""),
        job.get("description", "") or "",
        job.get("requirements","") or "",
        job.get("preferred",   "") or "",
    ]
    return " ".join(p for p in parts if p).strip()


# ── 인덱싱 ───────────────────────────────────────────────────
async def bulk_index_jobs(jobs: List[Dict[str, Any]]) -> int:
    """벌크 공고 인덱싱 — pipeline이 자동으로 임베딩 생성"""
    if not jobs:
        return 0

    def _fmt_date(val: Any) -> Any:
        """datetime → OpenSearch ISO 8601 형식 (2026-04-19T07:18:09)"""
        if val is None:
            return None
        s = str(val)
        return s.replace(" ", "T").split(".")[0]  # 공백→T, 마이크로초 제거

    lines = []
    for job in jobs:
        meta = {"index": {"_index": JOBS_INDEX, "_id": job["id"]}}
        doc  = {
            **job,
            _TEXT_FIELD:  build_search_text(job),
            "created_at": _fmt_date(job.get("created_at")),
            "updated_at": _fmt_date(job.get("updated_at")),
        }
        lines.append(json.dumps(meta))
        lines.append(json.dumps(doc, default=str))
    body = "\n".join(lines) + "\n"

    # pipeline 파라미터 추가 (모델 준비된 경우)
    params = {"pipeline": _PIPELINE_ID} if _model_id else {}

    async with httpx.AsyncClient(timeout=120.0) as c:
        resp = await c.post(
            f"{OPENSEARCH_HOST}/_bulk",
            content=body.encode("utf-8"),
            headers={"Content-Type": "application/x-ndjson"},
            params=params,
        )
        resp.raise_for_status()
        result = resp.json()

    errors = [
        item for item in result.get("items", [])
        if item.get("index", {}).get("error")
    ]
    if errors:
        logger.warning("벌크 인덱싱 오류 %d건: %s", len(errors), errors[0])

    ok = len(jobs) - len(errors)
    logger.info("OpenSearch 인덱싱 완료: %d / %d (knn=%s)", ok, len(jobs), bool(_model_id))
    return ok


# ── Neural Search (벡터 기반) ─────────────────────────────────
async def neural_search(
    query_text: str,
    filters:    Optional[Dict[str, str]] = None,
    k:          int = 10,
) -> Dict[str, Any]:
    """
    설문/이력서 텍스트 → OpenSearch가 자동으로 임베딩 → kNN 검색

    filters: {"location": "서울", "career_type": "신입"} 형태
    """
    if not _model_id:
        logger.warning("ML 모델 미준비 — 키워드 검색으로 fallback")
        return await search_jobs(keyword=query_text, size=k)

    neural_clause: Dict[str, Any] = {
        _VECTOR_FIELD: {
            "query_text": query_text,
            "model_id":   _model_id,
            "k":          k,
        }
    }

    query: Dict[str, Any]
    if filters:
        filter_clauses = [
            {"term": {field: value}}
            for field, value in filters.items()
            if value
        ]
        query = {
            "bool": {
                "must":   [{"neural": neural_clause}],
                "filter": filter_clauses,
            }
        }
    else:
        query = {"neural": neural_clause}

    payload = {"query": query, "size": k}

    async with httpx.AsyncClient(timeout=30.0) as c:
        resp = await c.post(
            f"{OPENSEARCH_HOST}/{JOBS_INDEX}/_search",
            json=payload,
        )
        resp.raise_for_status()
        data = resp.json()

    hits  = data.get("hits", {})
    total = hits.get("total", {}).get("value", 0)
    items = [
        {**h["_source"], "_score": h.get("_score")}
        for h in hits.get("hits", [])
    ]
    logger.info("Neural Search '%s...' → %d건 (요청 %d)", query_text[:30], total, k)
    return {"total": total, "items": items}


# ── Keyword Search (fallback) ─────────────────────────────────
async def search_jobs(
    keyword:     Optional[str] = None,
    location:    Optional[str] = None,
    job_type:    Optional[str] = None,
    career_type: Optional[str] = None,
    page:        int = 1,
    size:        int = 20,
) -> Dict[str, Any]:
    """기존 키워드 기반 검색 (필터 + 페이지네이션)"""
    must:    List[Any] = []
    filter_: List[Any] = []

    if keyword:
        must.append({
            "multi_match": {
                "query":  keyword,
                "fields": ["title^3", "company^2", "description", "requirements"],
            }
        })
    if location:
        filter_.append({"term": {"location": location}})
    if job_type:
        filter_.append({"term": {"job_type": job_type}})
    if career_type:
        filter_.append({"term": {"career_type": career_type}})

    if must or filter_:
        query: Any = {"bool": {}}
        if must:
            query["bool"]["must"] = must
        if filter_:
            query["bool"]["filter"] = filter_
    else:
        query = {"match_all": {}}

    payload = {
        "query": query,
        "from":  (page - 1) * size,
        "size":  size,
        "sort":  [{"created_at": "desc"}],
    }

    async with httpx.AsyncClient(timeout=10.0) as c:
        resp = await c.post(f"{OPENSEARCH_HOST}/{JOBS_INDEX}/_search", json=payload)
        resp.raise_for_status()
        data = resp.json()

    hits  = data.get("hits", {})
    total = hits.get("total", {}).get("value", 0)
    items = [h["_source"] for h in hits.get("hits", [])]
    return {"total": total, "items": items}


# ── model_id 공개 접근자 ──────────────────────────────────────
def get_model_id() -> Optional[str]:
    return _model_id
