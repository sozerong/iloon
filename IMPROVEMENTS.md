# 개선 작업 기록 (2026년 8월)

측정 조건과 원본 수치는 `BENCHMARK.md` / `bench/results/*.json` 참조.
추정값은 없다. 실행해서 나온 값만 적었다.

---

## 작업 0 — 선행 버그: ai_server 패키지가 import되지 않음

측정을 시작하려다 발견. 4개 작업과 무관하지만 전부를 막고 있어서 먼저 고쳤다.

① **문제**: `import ai_server.services...` 가 `ImportError`로 죽는다. AI 서버가 뜨지 않는다.

② **확인**:
```
ImportError: cannot import name 'ResumeAnalysisCreate' from 'ai_server.schemas.user'
ImportError: cannot import name 'ResumeAnalysis' from 'ai_server.models.user'
```
`ResumeAnalysis` 모델이 `Document`로, `ResumeAnalysisCreate/Out` 스키마가
`DocumentCreate/Out`으로 이름이 바뀌었는데 두 곳이 옛 이름을 그대로 참조하고 있었다.
실제 사용처(`routers/resume.py`, `services/resume_service.py`)는 이미 새 이름을 쓰고 있어서,
남은 건 재노출(re-export) 구문과 추천 서비스 한 곳뿐이었다.

③ **접근**: 옛 이름으로 별칭을 다는 방법도 있었지만, 실제로 그 이름을 쓰는 코드가
하나도 없어서 죽은 이름을 되살릴 이유가 없다. 참조를 현재 이름으로 맞추는 쪽을 택했다.

④ **해결**:
- `ai_server/schemas/__init__.py` — `ResumeAnalysisCreate, ResumeAnalysisOut`
  → `DocumentCreate, DocumentOut, DocumentListItem, DocumentWithScores`
- `ai_server/services/recommendation_service.py` — `ResumeAnalysis` → `Document`,
  조회에 `Document.type == "resume"` 조건 추가(문서 테이블이 자소서/포트폴리오와 공용이므로),
  없어진 `analysis.analyzed_content` → `ai_summary or original_text`

⑤ **결과**: `ai_server` import 성공. 이후 모든 측정이 가능해졌다.
남은 한계: 런타임 경로(추천 API 실제 호출)까지는 검증하지 않았다. import와 색인 경로만 확인했다.

---

## 작업 1 — OpenSearch 색인 청킹

① **문제**: 지시서는 "건별 `index()` 호출을 bulk로 전환"을 전제했으나,
조사해 보니 **이미 `_bulk`를 쓰고 있었다**. 실제 문제는 다른 데 있었다 —
전체 문서를 **요청 하나에 전부** 담고 있었다. 청킹이 없다.

② **확인**: 문서 수를 올려가며 실측했더니 선형으로 늘다가 특정 지점에서 완전히 죽는다.

| 문서 수 | bulk 본문 | 결과 |
|---|---|---|
| 30,000 | 95.3 MB | 2.963s 성공 |
| 35,000 | 111 MB | `HTTP 413 Request Entity Too Large` — **0건 색인** |

OpenSearch `http.max_content_length` 기본값이 100MB다. 이 선을 넘는 순간
예외가 나고 한 건도 들어가지 않는다. 느려지는 게 아니라 **전량 실패**한다.
부분 실패 처리도 없었다 — 응답 items의 첫 오류만 로그로 찍고 끝이었다.

③ **접근**:
- 직접 리스트를 잘라 여러 번 POST: 가능하지만 바이트 기준 분할·부분 실패 파싱을
  전부 손으로 짜야 한다. 버릴 이유가 없어 보였으나 아래가 더 나았다.
- `opensearch-py`의 `helpers.bulk`: 지시서 권고. 다만 기존 코드가 전부 async라
  동기 `bulk`를 쓰면 이벤트 루프를 막는다.
- **채택: `helpers.async_bulk` + `AsyncOpenSearch`.** 같은 헬퍼의 비동기판이라
  async 경로를 유지하면서 `chunk_size` / `max_chunk_bytes` / `raise_on_error=False`를
  그대로 쓸 수 있다. 기존 httpx 코드는 건드리지 않고 색인 함수만 교체했다.

④ **해결**: `ai_server/services/opensearch_service.py`
- `bulk_index_jobs()` 를 `async_bulk` 기반으로 재작성. `chunk_size`(건수)와
  `max_chunk_bytes`(바이트)를 **둘 다** 설정. 건수만 막으면 문서가 커질 때 같은 사고가 난다.
- 적재 구간 동안 `refresh_interval="-1"`, **`try/finally`로 원래 값 복구**를 보장하고
  복구 후 `_refresh` 1회 명시 호출. 원래 값이 없었으면 `null`로 되돌려 기본값 복원.
  (`_get_refresh_interval` / `_set_refresh_interval`)
- 부분 실패: `raise_on_error=False`로 받아 `_split_failures()`가
  429/502/503/504(일시적)와 나머지(영구)로 나눈다. 원인 타입별 건수를 로그로 남기고,
  일시적 실패분만 1회 재시도한다. HTTP 200이어도 개별 항목은 실패할 수 있다는 게 핵심.
- 커넥션 정리용 `close_client()` 추가 → `main.py` lifespan shutdown에 연결.
- `BULK_CHUNK_SIZE = 2000`, `BULK_MAX_CHUNK_BYTES = 10MB` (근거는 ⑤).

⑤ **결과** (5,000건 3회 p50 / 상세는 `BENCHMARK.md`)

| 구간 | before | after |
|---|---|---|
| 5,000건 색인 | 0.693s | 0.697s |
| 50,000건 색인 | **HTTP 413, 0건** | **5.656s, 50,000건** |

- **5,000건 규모에서는 개선이 없다.** 0.6% 차이는 회차 편차 안이다. 정직하게 적는다.
- 20,000건에서는 오히려 **약 19% 느리다** (2.003s → 2.386s). 왕복 횟수가 늘어난 대가다.
- 개선은 속도가 아니라 **동작 가능 범위**다. 30,000건 근처에서 전량 실패하던 것이
  50,000건까지 정상 동작한다.
- chunk_size 스윕(20,000건): 200→6.685s, 500→3.487s, 1,000→2.534s,
  2,000→2.386s, 5,000→2.362s. **2,000 이후 개선이 멈춰서** 기본값으로 잡았다.

**남은 한계**
- 씨앗 공고 30건을 복제한 문서라 텍스트 다양성이 실제보다 낮다. 색인 처리량에는
  영향이 없지만, 역색인 크기나 분석기 부하까지 그대로라고 말할 수는 없다.
- 단일 노드 OpenSearch(heap 2GB) 기준이다. 샤드가 늘면 최적 chunk_size는 달라진다.
- 재시도는 1회뿐이다. 지속적인 429에는 백오프가 필요한데 넣지 않았다.

---

## 작업 2 — 이벤트 수집 API: 플러시 유실 창 제거

① **문제**: 지시서는 "요청마다 파일을 open/write/close 하는지" 확인 후 버퍼링 도입을
전제했으나, **버퍼링은 이미 전부 구현되어 있었다** — 100건/5초 임계치, `asyncio.Lock`,
`lifespan` 관리, `CancelledError` 잡고 잔여 flush 후 재raise까지.

실제 문제는 다른 곳이었다. 임계치 플러시가 `BackgroundTasks`로 위임되어 있어
**버퍼에서는 이미 비워졌는데 디스크에는 아직 안 쓰인 구간**이 생긴다.
그 사이에 프로세스가 죽으면 그 100건은 어디에도 없다. lifespan shutdown은
이 백그라운드 태스크를 기다려주지 않는다.

② **확인**: 두 가지를 실측했다.

*(a) 어느 구간이 지배적인가* — 지시서가 먼저 확인하라고 한 항목.
서버 내부 구간별 p50(n=2000): 검증 1.510 ms / 직렬화 0.014 ms / **저장 0.002 ms**.
**저장은 병목이 아니다. 검증(Pydantic)이 서버 시간의 93~95%다.**
즉 "저장을 버퍼링해서 빠르게 만든다"는 방향 자체가 이 코드에서는 이미 끝난 얘기였다.

*(b) 유실 창이 실재하는가* — ASGI를 직접 호출해 클라이언트가 202를 받는
바로 그 순간(`http.response.body`)의 디스크 상태를 봤다.

| | 응답 시점 디스크 반영 |
|---|---|
| 원본 | **0 / 100건** |
| 수정 후 | 100 / 100건 |

원본은 "flushed: 100"이라고 응답해 놓고 디스크에는 한 건도 없다.

③ **접근**:
- lifespan shutdown에서 백그라운드 태스크를 추적·대기: 태스크 집합을 들고 다녀야 하고,
  응답 이후 실행이라는 성질은 그대로다. 유실 창이 좁아질 뿐 사라지지 않는다.
- fsync까지 강제: 유실 창과 무관하고 비용만 크다.
- **채택: 요청 안에서 `await flush_to_disk()`.** 저장이 0.002 ms라 미룰 이유가 없다.
  미루는 대가(유실 창)만 있고 얻는 게 없었다. 코드도 줄어든다.

④ **해결**: `tools/log_api.py`
- `ingest_bulk()` — `background_tasks.add_task(flush_to_disk, ...)` → `await flush_to_disk(...)`.
  버퍼 스왑을 락 안에서 한 번에 처리(`to_flush`를 락 구간에서 결정)해 경합 구간을 줄였다.
  `BackgroundTasks` 의존성 제거.
- `OSError`를 따로 잡아 500 + 원인 반환 (기존엔 일반 예외로 뭉뚱그려짐).
- 경로 하드코딩 제거: `C:/GitHub/new_git/money/...` → `ANALYSIS_BASE_DIR` /
  `ANALYSIS_LOGS_DIR` / `ANALYSIS_APPLOG_DIR` 환경변수. `analyze_*.py`와 같은 규약.
  (기존 값은 기본값으로 유지 — 컨테이너에서 동작 불가였던 게 이제 주입 가능)
- `setup_logger()` 핸들러 중복 방지 가드 (`--reload`/테스트에서 로그가 4중으로 찍혔다).
- 구간 계측 추가 — `LOG_API_TIMING=1` 일 때만 수집, `GET /stats/timing`으로 조회.
  기본 off라 운영에는 영향 없다.
- 기존 Pydantic 검증과 timestamp ISO 8601 강제는 그대로 유지.

⑤ **결과** (동시 20 / 총 20,000건 / 3회, 상세는 `BENCHMARK.md`)

| 지표 | before | after |
|---|---|---|
| p50 응답 시간 | 12.85 ms | 12.77 ms |
| p95 응답 시간 | 121.70 ms | 117.94 ms |
| 처리량 | 582 req/s | 598 req/s |
| 응답 시점 유실 | **100건 중 100건 미반영** | 0 |

- **성능 개선은 없다.** 응답 시간·처리량 모두 회차 편차 안이다.
- 서버 내부에서 저장 p95만 0.039 → 0.744 ms로 올랐다. 플러시를 요청 안에서
  기다리는 비용이고, 검증(1.5 ms)에 묻혀 end-to-end에는 드러나지 않는다.
- 얻은 것은 속도가 아니라 **정확성**이다. "저장했다"는 응답이 실제로 저장을 의미하게 됐다.

**검증** (`tests/test_log_api.py`, 4개 통과)
- 250건(임계치 비배수) 전송 → graceful shutdown → 파일 250건, 중복 0
- 응답 시점 디스크 반영 (원본에서는 실패하는 테스트 — 회귀를 실제로 잡는다)
- ISO 8601 위반 422 / 빈 배열 400

**남은 한계**
- 진짜 병목인 검증(1.5 ms)은 손대지 않았다. 줄이려면 `model_dump()` 대신
  원본 dict를 쓰거나 검증을 느슨하게 해야 하는데, 둘 다 데이터 품질을 깎는 거래라 하지 않았다.
- 프로세스가 SIGKILL로 죽으면 버퍼(최대 100건 / 5초)는 여전히 사라진다.
  그건 버퍼링을 쓰는 이상 구조적으로 남는 창이고, 없애려면 WAL이 필요하다.
- 단일 uvicorn 프로세스 기준이다. 워커를 늘리면 프로세스마다 버퍼가 따로 생긴다.

---

## 작업 3 — Airflow: 실제 의존성 + 입력 게이트 + 병렬화

① **문제**: 세 가지가 겹쳐 있었다.
- DAG 간 연결이 **순수 시간 기반**이었다. `dummy_job_generator` 01:00 →
  `ai_vs_normal_analysis` 02:00. 상류가 늦어지거나 실패해도 하류는 그냥 돈다.
  `ExternalTaskSensor` / `TriggerDagRunOperator` 가 하나도 없었다.
- 입력 검증이 **파일 존재만** 확인했다 (`validate_input` → `--step validate`).
  빈 파일이어도 통과한다.
- `behavior_1 → … → behavior_5 → trend_1 → … → trend_6 → job_popularity` 가
  한 줄로 이어져 있었다. 주석엔 "트렌드 집계 결과 활용 가능"이라 적혀 있었다.

② **확인**:
- 각 스크립트가 무엇을 읽는지 전부 확인했다. `analytics/analyze_ai_vs_normal.py`,
  `analyze_job_trends.py`, `analyze_user_segmentation.py`, `analyze_job_popularity.py`
  **모두 `logs/*.jsonl` 원본만 읽는다.** 어느 것도 다른 것의 산출물을 읽지 않는다.
  → 주석의 "트렌드 결과 활용"은 코드에 없다. 체인은 실제 의존이 아니었다.
- 멱등성은 이미 확보돼 있었다. `user_segments` / `job_popularity` 둘 다
  적재 전에 `DELETE FROM` 을 돈다. 실측으로 확인: 300행 → 재실행 후 **300행**.
- 순차 baseline (스케줄러 경로, 3회): **p50 126.3s**.

③ **접근**:
- 상류 대기: `TriggerDagRunOperator` 는 상류가 하류를 밀어주는 구조라 상류 DAG를
  고쳐야 하고, 하류가 늘 때마다 상류가 또 바뀐다. **`ExternalTaskSensor` 채택** —
  분석 DAG가 자기 선행 조건을 스스로 선언하는 쪽이 결합도가 낮다.
- 병렬화 폭: 13개를 전부 동시에 풀면 SparkSession(JVM)이 13개 뜬다.
  `max_active_tasks=4` 로 제한했다.

④ **해결**: `dags/ai_analysis_dag.py`
- `ExternalTaskSensor` 2개 (`dummy_job_generator`, `user_event_generator`).
  `mode="reschedule"` — 대기 중 워커 슬롯을 안 잡는다. `poke_interval=60`,
  `timeout=3600`, `failed_states=["failed"]` (상류가 죽으면 타임아웃까지 안 끌고 즉시 실패).
  `execution_delta` 는 스케줄 간격에 맞췄다: 공고 생성 1시간, 이벤트 생성 0.
- `validate_input` 을 BashOperator → PythonOperator 로 교체. 파일 존재가 아니라
  **줄 수**를 센다. 0건이면 `AirflowFailException`. 건수는 XCom(`job_count`,
  `event_count`)에 남긴다. `retries=0` — 0건은 다시 세도 0건이다.
  (지시서는 "재시도 대상이 되게"라고 했지만 `AirflowFailException` 은 재시도를 막는 API다.
   센서로 상류 완료를 이미 확인한 뒤라 재시도가 상황을 바꾸지 못해 이쪽을 택했다.)
- 체인 해체: `gate >> [behavior_1..5, trend_1..6, segmentation, job_popularity] >> notify`.
- `notify_complete` 가 XCom에서 입력 건수를 읽어 출력에 포함.

**선행 수정 (이거 없이는 DAG가 아예 못 돈다)**
- `PYTHON_BIN = "/opt/airflow/project/venv/bin/python"` → `"python"`.
  그 venv는 Dockerfile에도 compose 볼륨에도 없다. 모든 BashOperator가 실패하고 있었다.
  (`ai_analysis_dag.py`, `dummy_job_dag.py`, `user_event_dag.py`)
- Airflow 이미지가 깨져 있었다. `requirements.txt` 의 `sqlalchemy>=2.0.0` 이
  Airflow 2.9.3 이 고정한 SQLAlchemy 1.4 를 덮어써서 `airflow db migrate` 가
  `MappedAnnotationError` 로 죽고 `airflow users` 서브커맨드조차 사라졌다.
  → `airflow_requirements.txt` 신설 (pyspark / psycopg2-binary / anthropic 만),
  Dockerfile이 이걸 보게 변경. (공식 constraint 파일은 pyspark==3.5.1 을 강제해
  저장소가 고정한 3.3.4 와 충돌해서 쓰지 않았다)
- 분석 스크립트의 컬럼명이 이벤트 스키마와 어긋나 3개 단계가 항상 실패했다
  (`analytics/analyze_ai_vs_normal.py`): `region_sido`→`region`,
  `time_on_page_sec`→`session_duration`, `session_id`→`user_id`.
  전부 `events/user_event_generator.py` 가 내보내는 실제 필드명으로 맞췄다.

⑤ **결과** (스케줄러 경로 3회, 공고 2,000건 / 이벤트 244,839건)

| 지표 | before | after |
|---|---|---|
| DagRun 소요 p50 | **126.3s** | **74.2s** (−41%) |
| 회차 | 126.4 / 126.3 / 125.2 | 73.9 / 74.5 / 74.2 |
| 전 회차 성공 | ✅ | ✅ |

- 개별 태스크는 오히려 느려졌다 (behavior_1 8.3→14.1s, job_popularity 30.4→33.4s).
  4개가 CPU를 나눠 쓰기 때문이다. 그런데도 전체는 41% 줄었다.
- **측정 경로 주의**: `airflow dags test` 로 재면 before 134.8s / after 137.9s 로
  차이가 없다. test 경로는 의존성 그래프와 무관하게 한 프로세스에서 순차 실행하기
  때문이다. 이 수치를 결과로 쓰면 "효과 없음"이라는 틀린 결론이 나온다.
- **게이트 동작 확인**: 이벤트 파일을 치우고 실행 →
  `이벤트 JSONL : 파일 0개 / 0건` 로그 후 `AirflowFailException`,
  `Immediate failure requested. Marking task as FAILED`.
- **멱등성**: 이미 확보돼 있었다. 재실행 후에도 `user_segments` 300행 그대로.

**남은 한계**
- 센서는 상류 DagRun 레코드를 심어서 검증했다 (`bench/seed_upstream_runs.py`).
  상류를 실제로 돌리려면 Anthropic API 키가 필요하고, 이벤트 생성기는 측정 입력을
  덮어써서 실행할 수 없었다. **센서가 통과한다는 것은 확인했지만,
  상류가 실제로 늦어질 때의 대기 동작은 관찰하지 못했다.**
- `max_active_tasks=4` 는 이 머신(16스레드) 기준이다. 워커 메모리에 맞춰 조절할 값이다.
- 컨테이너 하나짜리 LocalExecutor 기준이다. 워커를 늘리면 숫자가 달라진다.

---

## 작업 4 — ingest pipeline 임베딩

① **문제**: 지시서는 "애플리케이션에서 임베딩을 만들어 벡터까지 전송하는지" 확인 후
ingest pipeline으로 이관하라고 했다. 조사해 보니 **이미 전부 구현되어 있었다.**

`ai_server/services/opensearch_service.py` 에:
- `text_embedding` processor + `field_map: search_text → embedding_vector` (`_ensure_pipeline`)
- `knn_vector` 384차원 매핑 + `index.knn` + `index.default_pipeline` (`_build_mapping`)
- 애플리케이션은 원문만 보낸다 (`build_search_text`) — 벡터를 만들어 보내지 않는다
- ML 모델 미배포 시 키워드 검색 폴백 (`neural_search` → `search_jobs`)

실제 문제는 다른 데 있었다. **`tools/setup_opensearch.py` 가 자기만의 `MAPPING` 을 들고 있었다.**
knn_vector도 default_pipeline도 없는 매핑이다. 이 스크립트로 인덱스를 먼저 만들면
벡터 필드가 없는 인덱스가 생기고, `ensure_index()` 는 ML 모델이 없을 때 그 인덱스를
그대로 쓴다(`if not mid: return`). 그러면 **neural search가 조용히 죽는다** —
에러도 없이 계속 키워드 검색으로만 동작한다.

② **확인**: 매핑의 출처가 둘이라는 것 자체가 원인이라, 어느 쪽으로 만들었는지에 따라
인덱스가 달라지는 것을 코드로 확인했다. 배포 후 실측 검증은 ⑤에 있다.

③ **접근**: `tools/setup_opensearch.py` 의 매핑을 `ensure_index()` 와 같게 맞추는 방법도 있지만,
그러면 **같은 매핑을 두 곳에서 관리**하게 되고 다음에 또 갈라진다.
매핑의 단일 출처를 `opensearch_service` 로 두고 스크립트는 호출만 하게 했다.

④ **해결**:
- `tools/setup_opensearch.py` — 자체 `MAPPING` 제거. `ensure_ml_ready()` + `ensure_index()` 호출로 교체.
  `--no-ml` 옵션으로 ML 없이 키워드 인덱스만 만들 수도 있게 남겼다.
- pipeline / 매핑 / 폴백 로직은 **그대로 뒀다.** 이미 지시서 요구대로 되어 있었다.

⑤ **결과** (5,000건 3회, 작업 1 완료 상태를 baseline으로)

| 조건 | before (pipeline 없음) | after (ingest pipeline) |
|---|---|---|
| 5,000건 색인 p50 | **0.900s** | **99.7s** |
| 전송(+임베딩) 구간 | ~0.6s | 99.4s |

- **111배 느려졌다.** 임베딩이 전부다 — 문서당 약 20ms. GPU 없이 OpenSearch 내부
  CPU 추론으로 384차원 벡터를 5,000개 만드는 비용이다.
- 이건 회귀가 아니라 **거래**다. 이 비용을 색인 시점에 한 번 내고, 대신 검색할 때마다
  애플리케이션이 임베딩 API를 호출하지 않아도 된다. 다만 **"pipeline으로 옮겨서
  빨라졌다"고 말할 수 있는 수치는 나오지 않았다.** 옮겨서 느려졌고, 얻은 건 속도가 아니다.

**검증** (전부 실측)

| 항목 | 결과 |
|---|---|
| `embedding_vector` 매핑 | `knn_vector`, dimension 384 |
| `index.default_pipeline` | `iloon-job-pipeline` |
| 벡터가 채워진 문서 | **5,000 / 5,000건** |
| neural 쿼리 | 정상 (cosine score 0.75 대) |
| ML 미배포 시 폴백 | 정상 — 예외 없이 키워드 검색, 2,170건 반환 |

**남은 한계**
- neural 검색의 **의미적 정확도는 평가하지 않았다.** 문서가 씨앗 30건을 복제한 것이라
  내용 다양성이 없어서, "파이썬 백엔드" 질의에 "Unreal Engine 게임 개발자"가 상위로 나온다.
  벡터 생성과 kNN 경로가 동작한다는 것까지만 확인했다.
- 단일 노드 CPU 추론 기준이다. ML 전용 노드를 분리하거나 GPU를 쓰면 완전히 달라진다.
- 20ms/문서라는 값은 이 문서 길이(`search_text` 기준) 에서의 값이다.
  본문이 길어지면 더 늘어난다.

---

## 최종 확인 (지시서 9절)

- [x] 실제로 실행해서 얻은 값인가 — 전부. 원본은 `bench/results/*.json`
- [x] 측정 조건이 함께 적혀 있는가 — `BENCHMARK.md` 각 절 상단
- [x] before/after가 같은 조건에서 측정됐는가 — 같은 입력·같은 회차 수.
      작업 2는 계측 코드를 동일하게 두고 flush 경로만 바꾼 사본(`bench/log_api_before.py`)을 썼다
- [x] 개선이 없었던 항목도 그대로 기록했는가 — 작업 1(5,000건 규모에서 개선 없음,
      20,000건에서 19% 저하), 작업 2(성능 개선 없음), 작업 4(111배 저하) 전부 기록
- [x] 구현하지 않은 것을 구현했다고 적은 곳은 없는가 — 작업 2·4는 "이미 구현되어 있었음"을
      명시. VCC는 "기존 성과 아님, 신규 작업"을 명시

**요약: 4건 중 속도가 개선된 것은 작업 3(−41%) 하나다.**
작업 1은 속도가 아니라 동작 가능 범위(30,000건 → 50,000건 이상)를 넓혔고,
작업 2는 속도가 아니라 유실 창을 없앴고, 작업 4는 느려지는 대신 벡터 검색을 얻었다.
