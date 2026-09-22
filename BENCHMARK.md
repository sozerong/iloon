# 벤치마크 기록

모든 수치는 `bench/timing.py` 하니스로 실제 실행해서 얻은 값이다.
원본 측정 결과는 `bench/results/*.json` 에 남는다.
측정하지 않은 항목은 빈칸으로 두고, 채우지 않은 이유를 적는다.

## 측정 환경

- 하드웨어: AMD Ryzen 7 7800X3D (8C/16T), RAM 63.1 GB
- OS: Windows 11 Home 10.0.26200
- Python: 3.9.7
- Docker 리소스 할당: (측정 시 기록 — OpenSearch `OPENSEARCH_JAVA_OPTS=-Xms2g -Xmx2g`)
- 측정일: (측정 시 기록)

---

## 작업 1 — OpenSearch bulk 색인

**측정 조건**
- 문서: `bench/make_docs.py` 로 생성. `results.zip` 의 실제 생성 공고 30건을 씨앗으로 복제,
  id/제목/회사명은 전부 고유. 정규화 후 평균 1,348 bytes/건, bulk 본문 기준 약 3.2 KB/건.
  (내용이 반복되므로 텍스트 다양성은 실제보다 낮다 — 색인 처리량 측정에는 영향 없음)
- 인덱스: 매 회차 삭제 후 재생성. ML 모델 미배포 = **pipeline/임베딩 없음** (작업 1 효과만 격리)
- 반복: 5,000건은 3회(p50), 대규모 구간은 1회 확인 후 최종값만 3회
- 실행: `python bench/bench_index.py --docs N --repeat R --label ...`
- before = 전체 문서를 `_bulk` 요청 **하나**로 전송 (청킹 없음)
- after  = `helpers.async_bulk`, chunk_size=2000, max_chunk_bytes=10MB, refresh_interval 제어

### 5,000건 (3회, p50)

| 구간 | before | after |
|---|---|---|
| 색인 총 소요 (p50) | **0.693s** | **0.697s** |
| p95 | 0.940s | 0.751s |
| ├ 문서 준비 | 0.067s | 0.015s |
| ├ 전송 | 0.623s | 0.63s |
| └ refresh 복구 | 없음 | 0.13s |
| 색인 성공 | 5,000 / 5,000 | 5,000 / 5,000 |

→ **이 규모에서는 개선 없음.** 차이 0.6%는 회차 편차(min 0.566 / max 0.940) 안이다.

### 규모별 — 여기서 갈린다

| 문서 수 | bulk 본문 크기 | before | after |
|---|---|---|---|
| 5,000 | 15.9 MB | 0.693s | 0.697s |
| 10,000 | 31.8 MB | 1.047s | — |
| 20,000 | 63.6 MB | 2.003s | 2.386s |
| 30,000 | 95.3 MB | 2.963s | — |
| 35,000 | 111 MB | **HTTP 413 — 0건 색인** | 5.444s / 35,000건 |
| 50,000 | 159 MB | **HTTP 413 — 0건 색인** | **5.656s** / 50,000건 |

한계점은 30,000건과 35,000건 사이 — OpenSearch `http.max_content_length` 기본값 100MB.
before는 이 지점을 넘으면 예외로 죽고 **한 건도 색인되지 않는다.**

### chunk_size 스윕 (20,000건, 3회 p50, max_chunk_bytes=10MB 고정)

| chunk_size | p50 |
|---|---|
| 200 | 6.685s |
| 500 | 3.487s |
| 1,000 | 2.534s |
| 2,000 | 2.386s |
| 5,000 | 2.362s |

2,000 이후로 개선이 멈춘다 → 기본값 2,000. 청킹하지 않은 before(2.003s)보다는
여전히 약 19% 느리다. 왕복 횟수를 늘린 대가다.

---

## 작업 2 — 이벤트 수집 API 버퍼링

**측정 조건**
- 동시 요청 20 / 요청당 10건 / 총 20,000건 (2,000 요청), 각 3회
- 서버: uvicorn 단일 프로세스, `LOG_API_TIMING=1`, 로컬 디스크(NVMe)
- 부하 스크립트: `bench/load_log_api.py`
- before = 원본 동작(`bench/log_api_before.py` — 임계치 플러시를 `BackgroundTasks`로 위임).
  계측 코드는 before/after 동일. **flush 경로만 다르다.**
- after = 임계치 플러시를 요청 안에서 `await`

버퍼링 자체(100건 / 5초, `asyncio.Lock`, lifespan 관리)는 **이미 구현되어 있었다.**
따라서 이 표는 "버퍼링 도입 전/후"가 아니라 "플러시를 백그라운드로 미룰 때 / 미루지 않을 때"다.

### 클라이언트 관측 (3회 중 중앙값)

| 지표 | before | after |
|---|---|---|
| p50 응답 시간 | 12.85 ms | 12.77 ms |
| p95 응답 시간 | 121.70 ms | 117.94 ms |
| 평균 응답 시간 | 34.18 ms | 33.28 ms |
| 처리량 | 582 req/s (5,815 events/s) | 598 req/s (5,976 events/s) |
| 실패 | 0 | 0 |

→ **차이 없음.** 회차 편차(before 582~586, after 587~623 req/s) 안이다.

### 서버 내부 구간별 (ms, n=2000)

| 구간 | before p50 | before p95 | after p50 | after p95 |
|---|---|---|---|---|
| 검증 (Pydantic) | 1.510 | 2.667 | 1.496 | 2.719 |
| 직렬화 | 0.014 | 0.021 | 0.014 | 0.020 |
| 저장 | 0.002 | **0.039** | 0.002 | **0.744** |
| 전체 | 1.587 | 2.755 | 1.634 | 3.047 |

**지배 구간은 검증이다 — 서버 시간의 약 93~95%.** 저장은 p50 0.002 ms로 사실상 0이다.
after에서 저장 p95가 0.039 → 0.744 ms로 오른 건 10요청마다 한 번 도는 플러시를
요청 안에서 기다리기 때문이다. 검증이 워낙 커서 end-to-end p95에는 묻힌다.

### 유실 검증 (`tests/test_log_api.py`)

클라이언트가 202를 받는 순간(ASGI `http.response.body` 시점)의 디스크 상태:

| | 응답 시점 디스크 반영 |
|---|---|
| before | **0 / 100건** |
| after | 100 / 100건 |

250건(FLUSH_SIZE 비배수) 전송 후 graceful shutdown → 파일 250건, 중복 0 — before/after 모두 통과.

---

## 작업 3 — Airflow DAG 의존성 + 입력 건수 게이트

**측정 조건**
- 입력(측정 내내 고정): 공고 2,000건(10개 카테고리 JSONL) / 사용자 이벤트 244,839건(84MB)
  - `bench/make_dag_input.py` + 저장소의 `events/user_event_generator.py`
- Airflow 2.9.3 / LocalExecutor / 컨테이너 1개 / PySpark 3.3.4, Java 17
- 측정 대상 DAG 외 상류 DAG는 **pause** — 스케줄 타고 돌면 입력 파일이 바뀐다
- 3회 실행, `dag_run.start_date ~ end_date`

**측정 경로에 대한 주의**
`airflow dags test` 는 의존성 그래프와 무관하게 **한 프로세스에서 태스크를 순차 실행**한다.
그래서 병렬화 비교에는 쓸 수 없다 (실제로 test 경로로는 before 134.8s / after 137.9s 로
차이가 나지 않았다 — 병렬이 실행되지 않았기 때문이다).
아래 수치는 **실제 스케줄러(LocalExecutor)에 DagRun을 맡긴** `bench/bench_dag_sched.py` 기준이다.

| 지표 | before (순차 체인) | after (병렬, max_active_tasks=4) |
|---|---|---|
| DagRun 소요 p50 | **126.3s** | **74.2s** |
| 회차 | 126.4 / 126.3 / 125.2 | 73.9 / 74.5 / 74.2 |
| 태스크 수 | 15 | 17 (센서 2개 포함) |
| 전 회차 성공 | ✅ | ✅ |

**−41%** (126.3 → 74.2s). 회차 편차가 1초 안쪽이라 차이는 편차 밖이다.

개별 태스크는 오히려 느려진다 — CPU를 4개가 나눠 쓰기 때문이다.

| 태스크 | before | after |
|---|---|---|
| job_popularity | 30.4s | 33.4s |
| user_segmentation | 15.8s | 19.0s |
| behavior_4 | 8.8s | 14.7s |
| behavior_1 | 8.3s | 14.1s |

태스크 하나하나는 최대 70% 느려졌는데 전체는 41% 빨라졌다. 직렬 구간이 사라진 효과다.

`airflow dags test` 경로로 잰 값(참고): before 134.8s / after 137.9s — **차이 없음**.
test 경로는 태스크를 한 프로세스에서 순차 실행하므로 병렬화가 실행되지 않는다.
이 수치를 결과로 쓰면 "병렬화 효과 없음"이라는 틀린 결론이 나온다.

### 게이트 동작 확인 (입력 0건)

`logs/user-events-*.jsonl` 을 치우고 `validate_input` 실행:

```
공고 JSONL   : 파일 10개 / 2,000건
이벤트 JSONL : 파일 0개 / 0건
AirflowFailException: 이벤트 입력이 0건이다 (.../user-events-*.jsonl, 파일 0개).
Immediate failure requested. Marking task as FAILED.
```

건수는 XCom(`job_count`, `event_count`)에도 남는다. 0건이면 재시도 없이 즉시 실패한다.

### 재실행 안전성 (멱등성)

`user_segments` / `job_popularity` 둘 다 적재 전에 `DELETE FROM` 을 돌린다 —
**이미 멱등이었다.** 실측으로 확인:

| | 1회 실행 후 | 같은 입력으로 재실행 후 |
|---|---|---|
| `user_segments` | 300행 | **300행** (중복 없음) |
| `job_popularity` | 2,000행 | — |

---

## 작업 4 — ingest pipeline 임베딩

**측정 조건**
- baseline은 **작업 1 완료 후 상태**다 (chunk_size=2000 적용된 코드).
  작업 1 효과와 섞이지 않게, 같은 세션에서 pipeline만 껐다 켜고 연속 측정했다.
- 문서 5,000건 / 3회 / 인덱스 매 회차 삭제 후 재생성
- 모델: `paraphrase-multilingual-MiniLM-L12-v2` (384차원), OpenSearch 내부 CPU 추론
- OpenSearch 단일 노드, heap 2GB

| 조건 | before (pipeline 없음) | after (ingest pipeline) |
|---|---|---|
| 5,000건 색인 p50 | **0.900s** | **99.7s** |
| p95 | 1.093s | 100.5s |
| ├ 문서 준비 | 0.017s | 0.017s |
| ├ 전송 (+임베딩) | ~0.6s | **99.4s** |
| └ refresh 복구 | 0.27s | 0.27s |
| 색인 성공 | 5,000 / 5,000 | 5,000 / 5,000 |

**111배 느리다.** 임베딩 생성 비용이 전부다 — 문서당 약 20ms.
색인 시간이 아니라 **CPU 추론 시간**을 재고 있는 것이다 (GPU 없음).

### 검증

| 항목 | 결과 |
|---|---|
| 인덱스 매핑 `embedding_vector` | `knn_vector`, dimension 384 |
| `index.default_pipeline` | `iloon-job-pipeline` |
| `index.knn` | `true` |
| 벡터가 실제로 채워진 문서 | **5,000 / 5,000건**, 차원 384 |
| neural 쿼리 | 정상 (cosine score 0.75 대) |
| ML 미배포 시 폴백 | 정상 — 예외 없이 키워드 검색으로 대체, 2,170건 반환 |

neural 검색 결과의 **의미적 정확도는 평가하지 않았다.** 문서가 씨앗 30건을 복제한
것이라 내용 다양성이 거의 없다 ("파이썬 백엔드" 질의에 "Unreal Engine 게임 개발자"가
상위로 나온다). 벡터 생성과 kNN 경로가 동작한다는 것까지만 확인한 수치다.
