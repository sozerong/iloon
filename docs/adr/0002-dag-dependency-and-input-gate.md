# ADR-0002. DAG 의존성 재정의와 입력 건수 게이트

- 상태: Accepted
- 날짜: 2026-08
- 프로젝트: Illo-on

## 맥락

세 가지가 겹쳐 있었다.

1. **DAG 간 연결이 순수 시간 기반.** 공고 생성 01:00 → 분석 02:00. 상류가 늦어지거나
   실패해도 하류는 그냥 돈다. `ExternalTaskSensor` 도 `TriggerDagRunOperator` 도 없었다.
2. **입력 검증이 파일 존재만 확인.** 빈 파일이어도 통과한다.
3. **13개 태스크가 한 줄로 직렬 연결.** 주석에는 "트렌드 집계 결과 활용 가능"이라 적혀 있었다.

## 조사

각 분석 스크립트가 무엇을 읽는지 전부 확인했다.
`analytics/analyze_ai_vs_normal.py`, `analyze_job_trends.py`, `analyze_user_segmentation.py`,
`analyze_job_popularity.py` — **전부 `logs/*.jsonl` 원본만 읽는다.**
어느 것도 다른 것의 산출물을 읽지 않는다.

→ 주석의 "트렌드 결과 활용"은 코드에 없다. **직렬 체인은 실제 의존이 아니었다.**

## 검토한 선택지 — 상류 대기

1. **`TriggerDagRunOperator`** — 상류가 하류를 밀어준다.
   기각: 상류 DAG 를 고쳐야 하고, 하류가 하나 늘 때마다 상류가 또 바뀐다.
2. **`ExternalTaskSensor` (채택)** — 하류가 자기 선행 조건을 스스로 선언한다.
   결합도가 낮다. `mode="reschedule"` 로 대기 중 워커 슬롯을 점유하지 않는다.

## 결정

- 센서 2개 (`dummy_job_generator`, `user_event_generator`).
  `execution_delta` 는 스케줄 간격에 맞췄다 — 공고 생성 1시간, 이벤트 생성 0.
  `failed_states=["failed"]` 로 상류가 죽으면 타임아웃까지 끌지 않고 즉시 실패.
- `validate_input` 을 BashOperator(파일 존재) → PythonOperator(**줄 수**)로 교체.
  0건이면 `AirflowFailException`, `retries=0`.
  *0건은 다시 세도 0건이다. 센서로 상류 완료를 이미 확인한 뒤라 재시도가 상황을 바꾸지 못한다.*
  건수는 XCom(`job_count`, `event_count`)에 남긴다.
- 체인 해체: `gate >> [behavior_1..5, trend_1..6, segmentation, job_popularity] >> notify`.
  `max_active_tasks=4` — 태스크마다 SparkSession(JVM)이 뜨므로 무제한은 위험하다.

## 결과 (스케줄러 경로 3회, 공고 2,000건 / 이벤트 244,839건)

| | before | after |
|---|---|---|
| DagRun p50 | 126.3s | **74.2s (−41%)** |

개별 태스크는 오히려 느려졌다 (behavior_1 8.3 → 14.1s). CPU 를 4개가 나눠 쓰기 때문이다.
그런데도 전체는 41% 줄었다 — 직렬 구간이 사라진 효과다.

**측정 경로 주의**: `airflow dags test` 로 재면 134.8s vs 137.9s 로 차이가 없다.
test 경로는 의존성 그래프와 무관하게 한 프로세스에서 순차 실행하므로 병렬화가
실행되지 않는다. 이 수치를 결과로 쓰면 "효과 없음"이라는 틀린 결론이 나온다.

게이트 동작 확인: 이벤트 파일을 치우고 실행 → `이벤트 JSONL : 파일 0개 / 0건` 로그 후
`AirflowFailException`, `Immediate failure requested. Marking task as FAILED`.

## 남은 일

센서는 상류 DagRun 레코드를 심어서 검증했다(`bench/seed_upstream_runs.py`).
상류를 실제로 돌리려면 Anthropic API 키가 필요하고 이벤트 생성기는 측정 입력을 덮어써서
실행할 수 없었다. **센서 통과는 확인했지만 상류 지연 시 대기 동작은 관찰하지 못했다.**
