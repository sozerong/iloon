# 설계 결정 기록 (ADR)

파이프라인 설계에서 내린 결정을 **결정 시점의 근거와 기각한 대안**과 함께 남긴 기록입니다.
기존 문서를 고치는 대신 새 ADR로 대체하며, 대체된 문서는 superseded 상태로 남깁니다.

| 번호 | 결정 | 상태 |
|---|---|---|
| [0001](0001-storage-split-by-query-type.md) | 조회 성격에 따른 저장소 분리 | Accepted |
| [0002](0002-dag-dependency-and-input-gate.md) | DAG 의존성 재정의와 입력 게이트 | Accepted |
| [0003](0003-sync-flush-before-response.md) | 응답 전 동기 flush 전환 | Accepted |
| [0004](0004-bulk-chunk-size-and-bytes.md) | bulk 색인 건수·바이트 이중 제한 | Accepted |
| [0005](0005-embedding-ingest-pipeline.md) | 임베딩 ingest pipeline 이관 | **Rejected** (측정 결과 111배 저하) |
| [0009](0009-why-recommendation-accuracy-not-measured.md) | 추천 정확도 미측정 | Accepted |

번호 0006~0008 은 VCC 저장소의 `docs/adr` 에 있습니다.

기록하지 않은 것: 사후에 정한 SLO, 비용 목표.
프로젝트 진행 당시 제약으로 두지 않았던 항목은 지어내지 않고 비워 두었습니다.
