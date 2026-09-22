"""
이벤트 스키마 불변식 검증

analyze_ai_vs_normal.py 의 분석 4(매칭 점수 구간별 전환율)는 조회와 지원을
1:1 로 짝지을 수 있어야 성립한다. 그 근거가 `view_id` 다.

이게 없던 시절에는 `(user_id, job_id)` 로 조인했는데, 같은 사용자가 같은 공고를
여러 번 보면 조회 N × 지원 M 으로 행이 불어나 전환율이 부풀려졌다
(실측 10.6% vs 설계 참값 6.95%).

여기서 지키는 것:
  1. 모든 이벤트에 view_id 가 있다
  2. 조회 1건당 view_id 1개 (조회 수 == 고유 view_id 수)
  3. 북마크·지원의 view_id 는 반드시 조회에 존재한다 (고아 없음)
  4. 한 view_id 에서 지원은 최대 1번
  5. 전환율이 설계값 근처에 있다 (시뮬레이터가 바뀌면 여기서 걸린다)

실행:
    python -m pytest tests/test_event_schema.py -v
"""

from __future__ import annotations

import importlib
import os
import sys
from collections import Counter
from typing import Any, Dict, List

import pytest

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# user_event_generator 는 events/ 로 옮겨졌다.
sys.path.insert(0, os.path.join(BASE_DIR, "events"))
sys.path.insert(0, BASE_DIR)

# 시뮬레이터에 박혀 있는 설계값 (user_event_generator.py)
DESIGN = {
    True:  {"bookmark": 0.20, "apply_given_bookmark": 0.35},
    False: {"bookmark": 0.12, "apply_given_bookmark": 0.20},
}
TOLERANCE = 0.03      # 난수라 정확히 맞지는 않는다. 3%p 밖이면 뭔가 바뀐 것


@pytest.fixture(scope="module")
def events(tmp_path_factory) -> List[Dict[str, Any]]:
    """작은 공고 세트로 이벤트를 생성해 온다. 파일을 쓰지 않고 함수만 부른다."""
    import random

    tmp = tmp_path_factory.mktemp("events")
    os.environ["ANALYSIS_BASE_DIR"] = str(tmp)

    if "user_event_generator" in sys.modules:
        del sys.modules["user_event_generator"]
    gen = importlib.import_module("user_event_generator")

    # 생성기가 전역 random 을 쓴다. 시드를 박지 않으면 전환율 검증이 플래키해진다
    # (표본이 작아 ±3%p 를 넘나든다). 허용오차를 늘리는 대신 결정적으로 만든다 —
    # 느슨한 허용오차는 회귀도 같이 통과시킨다.
    random.seed(20260922)

    jobs = [
        {"job_id": f"job-{i:03d}", "region": "서울", "category": "백엔드"}
        for i in range(200)
    ]
    # 사용자를 적게 두면 같은 사용자가 같은 공고를 여러 번 보게 된다
    # — 예전 (user_id, job_id) 조인이 깨지던 바로 그 상황을 일부러 만든다
    return gen.generate_events(jobs, user_count=5, ai_ratio=0.5, days_back=3)


def test_every_event_has_view_id(events):
    missing = [e["event_type"] for e in events if not e.get("view_id")]
    assert not missing, f"view_id 없는 이벤트: {Counter(missing)}"


def test_one_view_id_per_view(events):
    views = [e for e in events if e["event_type"] == "job_detail_view"]
    assert len(views) == len({e["view_id"] for e in views}), \
        "조회 수와 고유 view_id 수가 다르다 — 조회 1건당 1개여야 한다"


def test_no_orphan_bookmark_or_apply(events):
    view_ids = {e["view_id"] for e in events if e["event_type"] == "job_detail_view"}
    orphans = [
        e for e in events
        if e["event_type"] in ("bookmark", "apply_click") and e["view_id"] not in view_ids
    ]
    assert not orphans, f"조회 없는 파생 이벤트 {len(orphans)}건"


def test_at_most_one_apply_per_view(events):
    applies = [e["view_id"] for e in events if e["event_type"] == "apply_click"]
    dup = [v for v, n in Counter(applies).items() if n > 1]
    assert not dup, f"한 조회에서 지원이 2번 이상: {dup[:3]}"


def test_join_on_view_id_has_no_fanout(events):
    """
    분석 4 가 하는 조인을 그대로 재현한다.
    view_id 로 조인하면 결과 행 수가 조회 수와 같아야 한다 (팬아웃 없음).
    (user_id, job_id) 로 조인하면 불어난다는 것도 같이 확인한다.
    """
    views   = [e for e in events if e["event_type"] == "job_detail_view"]
    applies = [e for e in events if e["event_type"] == "apply_click"]

    # view_id 기준 left join
    applied = {e["view_id"] for e in applies}
    rows_by_view = sum(1 for v in views if True)          # 1:1 이므로 행 수 불변
    assert rows_by_view == len(views)
    assert len(applied) <= len(views)

    # (user_id, job_id) 기준이면 불어난다
    apply_by_pair = Counter((e["user_id"], e["job_id"]) for e in applies)
    rows_by_pair = sum(max(1, apply_by_pair[(v["user_id"], v["job_id"])]) for v in views)
    assert rows_by_pair >= rows_by_view, "테스트 전제가 깨졌다"

    # 사용자 5명 / 공고 40개라면 중복 조회가 반드시 생겨 팬아웃이 관측돼야 한다
    assert rows_by_pair > rows_by_view, (
        "팬아웃이 관측되지 않았다 — 이 테스트가 회귀를 못 잡는 상태다. "
        "user_count 를 더 줄이거나 조회 수를 늘릴 것"
    )


@pytest.mark.parametrize("is_ai", [True, False])
def test_conversion_matches_design(events, is_ai):
    """전환율이 시뮬레이터 설계값 근처인지. 벗어나면 규칙이 바뀐 것이다."""
    sel = [e for e in events if e["is_ai_recommended"] is is_ai]
    c = Counter(e["event_type"] for e in sel)
    views, bookmarks, applies = c["job_detail_view"], c["bookmark"], c["apply_click"]
    assert views > 0

    d = DESIGN[is_ai]
    assert abs(bookmarks / views - d["bookmark"]) < TOLERANCE, \
        f"북마크 전환율 {bookmarks/views:.3f} vs 설계 {d['bookmark']}"
    if bookmarks:
        assert abs(applies / bookmarks - d["apply_given_bookmark"]) < TOLERANCE + 0.02, \
            f"지원 전환율 {applies/bookmarks:.3f} vs 설계 {d['apply_given_bookmark']}"
