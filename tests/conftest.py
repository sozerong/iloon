"""
pytest 수집 범위 조정.

`tests/` 에는 성격이 다른 두 종류가 섞여 있다.

- **자동 테스트** — `test_log_api.py`, `test_event_schema.py`.
  외부 서비스 없이 돌고 `def test_*` 를 갖는다.
- **수동 확인 스크립트** — `test_opensearch.py`, `test_recommendation.py`.
  `def test_*` 가 하나도 없고, **import 시점에** OpenSearch·추천 API 에 붙는다.
  이름이 `test_` 로 시작해 pytest 가 수집하려 들고, 그 서비스가 없으면
  수집 단계에서 ConnectionError 가 나 **테스트 전체가 중단된다.**

아래 목록은 후자다. 파일 이름을 바꾸지 않는 이유는 docstring 의 실행 방법
(`python tests/test_opensearch.py`)과 기존 사용 습관을 깨지 않기 위해서다.
자동 테스트로 바꾸려면 서비스 접속을 픽스처로 옮기고 `pytest.skip` 을 달아야 한다.
"""

collect_ignore = [
    "test_opensearch.py",
    "test_recommendation.py",
]
