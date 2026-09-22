# ADR-0005. 임베딩 ingest pipeline 이관 — 측정 결과 기각

- 상태: **Rejected** (구현은 되어 있으나 기본 경로로 쓰지 않음)
- 날짜: 2026-08
- 프로젝트: Illo-on

## 맥락

"애플리케이션이 임베딩을 만들어 벡터까지 전송하는 대신, OpenSearch ingest pipeline 에
맡기자"가 원래 계획이었다. 열어보니 **이미 구현돼 있었다** —
`text_embedding` processor, `field_map: search_text → embedding_vector`,
`knn_vector` 384차원, `default_pipeline`, ML 미배포 시 키워드 폴백까지.

실제 문제는 다른 데 있었다. **`tools/setup_opensearch.py` 가 자기만의 `MAPPING` 을 들고 있었다.**
knn_vector 도 default_pipeline 도 없는 매핑이다. 이걸로 먼저 인덱스를 만들면
`ensure_index()` 가 ML 모델이 없을 때 그 인덱스를 그대로 쓴다 →
**neural search 가 에러 없이 조용히 죽는다.**

## 측정

작업 1(청킹) 완료 상태를 baseline 으로, 같은 세션에서 pipeline 만 껐다 켜고 연속 측정.
문서 5,000건, 3회.

| 조건 | pipeline 없음 | ingest pipeline |
|---|---|---|
| 5,000건 색인 p50 | **0.900s** | **99.7s** |
| 전송(+임베딩) 구간 | ~0.6s | 99.4s |

**111배 느리다.** 임베딩이 전부다 — 문서당 약 20ms, GPU 없이 OpenSearch 내부 CPU 추론.

검증은 전부 통과했다: 5,000/5,000건 벡터 생성(384차원), neural 쿼리 정상(cosine 0.75대),
ML 미배포 시 폴백 정상(2,170건 반환).

## 결정

**기본 색인 경로로 쓰지 않는다.** 코드는 남긴다 (`--with-ml` 로 켤 수 있다).

이건 회귀가 아니라 거래다. 색인 시점에 비용을 한 번 내고 검색 때마다 임베딩 API 를
부르지 않아도 된다. 다만 **"pipeline 으로 옮겨서 빨라졌다"고 말할 수 있는 수치는
나오지 않았다.** 옮겨서 느려졌고 얻은 건 속도가 아니다.

배치 색인 시간이 100초에서 문제되지 않는 규모(공고는 하루 50건 생성)라면 켜도 되지만,
초기 적재나 재색인에서는 이 비용이 그대로 드러난다.

## 같이 고친 것

`tools/setup_opensearch.py` 의 자체 `MAPPING` 을 제거하고 `ensure_ml_ready()` + `ensure_index()`
호출로 교체했다. 매핑의 단일 출처를 `opensearch_service` 로 두지 않으면 다음에 또 갈라진다.

## 남은 일

**neural 검색의 의미적 정확도는 이 ADR 에서 평가하지 않았다.**
별도로 측정했고 결과는 README 의 "검색 품질" 절에 있다 — 요약하면 neural 이
Recall@3/nDCG 에서는 낫고 Recall@1/MRR 에서는 조금 못하다.
ML 전용 노드 분리나 GPU 를 쓰면 위 색인 비용은 완전히 달라진다.
