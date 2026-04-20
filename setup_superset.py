"""
Superset 대시보드 자동 설정 스크립트

실행:
  python setup_superset.py

기능:
  1. Superset 로그인 (admin/admin)
  2. PostgreSQL 데이터베이스 연결 등록 (iloon_jobs)
  3. 데이터셋 등록: user_segments, job_popularity, user_events
  4. 차트 생성:
     - 세그먼트별 사용자 수 (파이 차트)
     - 세그먼트별 평균 지원율 (막대 차트)
     - 공고 인기도 등급 분포 (파이 차트)
     - 직군별 평균 인기도 점수 (막대 차트)
     - 공고 인기도 Top 10 (테이블)
  5. 대시보드 생성: "일로온 분석 대시보드"
"""

import sys
import time
import json
import requests

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

SUPERSET = "http://localhost:8088"
ADMIN_USER = "admin"
ADMIN_PASS = "admin"
PG_URI = "postgresql+psycopg2://airflow:airflow@postgres/iloon_jobs"

session = requests.Session()


# ── 유틸 ─────────────────────────────────────────────────────

def step(msg):
    print(f"\n{'='*60}")
    print(f"  {msg}")
    print(f"{'='*60}")


def ok(msg):
    print(f"  OK  {msg}")


def warn(msg):
    print(f"  WARN  {msg}")


def fail(msg):
    print(f"  FAIL  {msg}")
    sys.exit(1)


# ── 1. 로그인 & CSRF ─────────────────────────────────────────

def login():
    step("Superset 로그인")

    # CSRF 토큰 획득
    r = session.get(f"{SUPERSET}/api/v1/security/csrf_token/", timeout=10)
    if r.status_code != 200:
        fail(f"CSRF 토큰 획득 실패: {r.status_code}\n  → Superset이 실행 중인지 확인하세요: docker-compose up -d superset")
    csrf = r.json().get("result")
    session.headers.update({"X-CSRFToken": csrf, "Referer": SUPERSET})

    # 로그인
    r = session.post(f"{SUPERSET}/api/v1/security/login", json={
        "username": ADMIN_USER,
        "password": ADMIN_PASS,
        "provider": "db",
        "refresh": True,
    }, timeout=10)
    if r.status_code != 200:
        fail(f"로그인 실패: {r.status_code} {r.text}")

    token = r.json().get("access_token")
    session.headers.update({"Authorization": f"Bearer {token}"})
    ok(f"로그인 완료 (user: {ADMIN_USER})")
    return token


# ── 2. 데이터베이스 등록 ─────────────────────────────────────

def get_or_create_database():
    step("PostgreSQL 데이터베이스 연결 등록")

    # 기존 DB 목록 확인
    r = session.get(f"{SUPERSET}/api/v1/database/", timeout=10)
    for db in r.json().get("result", []):
        if "iloon" in db.get("database_name", "").lower() or "airflow" in db.get("sqlalchemy_uri", "").lower():
            ok(f"기존 DB 사용: id={db['id']} name={db['database_name']}")
            return db["id"]

    # 새로 등록
    payload = {
        "database_name": "iloon_jobs",
        "sqlalchemy_uri": PG_URI,
        "expose_in_sqllab": True,
        "allow_run_async": True,
        "allow_dml": False,
    }
    r = session.post(f"{SUPERSET}/api/v1/database/", json=payload, timeout=15)
    if r.status_code in (200, 201):
        db_id = r.json()["id"]
        ok(f"데이터베이스 등록 완료 (id={db_id})")
        return db_id
    else:
        fail(f"데이터베이스 등록 실패: {r.status_code} {r.text}")


# ── 3. 데이터셋 등록 ─────────────────────────────────────────

def get_or_create_dataset(db_id: int, table_name: str) -> int:
    # 기존 확인
    r = session.get(
        f"{SUPERSET}/api/v1/dataset/",
        params={"q": json.dumps({"filters": [{"col": "table_name", "opr": "eq", "value": table_name}]})},
        timeout=10,
    )
    results = r.json().get("result", [])
    if results:
        ds_id = results[0]["id"]
        ok(f"기존 데이터셋 사용: {table_name} (id={ds_id})")
        return ds_id

    # 새로 등록
    payload = {
        "database": db_id,
        "table_name": table_name,
        "schema": "public",
    }
    r = session.post(f"{SUPERSET}/api/v1/dataset/", json=payload, timeout=15)
    if r.status_code in (200, 201):
        ds_id = r.json()["id"]
        ok(f"데이터셋 등록: {table_name} (id={ds_id})")
        return ds_id
    else:
        warn(f"데이터셋 등록 실패: {table_name} — {r.status_code} {r.text}")
        return None


# ── 4. 차트 생성 ─────────────────────────────────────────────

def create_chart(name: str, chart_type: str, dataset_id: int, params: dict) -> int:
    """차트 생성 후 chart_id 반환"""
    # 기존 확인
    r = session.get(
        f"{SUPERSET}/api/v1/chart/",
        params={"q": json.dumps({"filters": [{"col": "slice_name", "opr": "eq", "value": name}]})},
        timeout=10,
    )
    results = r.json().get("result", [])
    if results:
        cid = results[0]["id"]
        ok(f"기존 차트 사용: {name} (id={cid})")
        return cid

    payload = {
        "slice_name": name,
        "viz_type": chart_type,
        "datasource_id": dataset_id,
        "datasource_type": "table",
        "params": json.dumps(params),
    }
    r = session.post(f"{SUPERSET}/api/v1/chart/", json=payload, timeout=15)
    if r.status_code in (200, 201):
        cid = r.json()["id"]
        ok(f"차트 생성: {name} (id={cid})")
        return cid
    else:
        warn(f"차트 생성 실패: {name} — {r.status_code} {r.text}")
        return None


def build_charts(ds_segments: int, ds_popularity: int) -> list:
    chart_ids = []

    # ── 차트 1: 세그먼트별 사용자 수 (파이) ──────────────────
    cid = create_chart(
        name="세그먼트별 사용자 수",
        chart_type="pie",
        dataset_id=ds_segments,
        params={
            "groupby": ["segment_label"],
            "metric": {"expressionType": "SIMPLE", "column": {"column_name": "user_id"},
                       "aggregate": "COUNT", "label": "사용자 수"},
            "donut": True,
            "show_legend": True,
            "show_labels": True,
            "label_type": "key_percent",
            "color_scheme": "supersetColors",
        },
    )
    if cid:
        chart_ids.append(cid)

    # ── 차트 2: 세그먼트별 평균 지원율 (막대) ────────────────
    cid = create_chart(
        name="세그먼트별 평균 지원율",
        chart_type="bar",
        dataset_id=ds_segments,
        params={
            "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "apply_rate"},
                         "aggregate": "AVG", "label": "평균 지원율"}],
            "groupby": ["segment_label"],
            "x_axis_label": "세그먼트",
            "y_axis_label": "평균 지원율 (%)",
            "color_scheme": "supersetColors",
            "show_legend": False,
        },
    )
    if cid:
        chart_ids.append(cid)

    # ── 차트 3: 세그먼트별 평균 북마크율 (막대) ──────────────
    cid = create_chart(
        name="세그먼트별 평균 북마크율",
        chart_type="bar",
        dataset_id=ds_segments,
        params={
            "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "bookmark_rate"},
                         "aggregate": "AVG", "label": "평균 북마크율"}],
            "groupby": ["segment_label"],
            "x_axis_label": "세그먼트",
            "y_axis_label": "평균 북마크율 (%)",
            "color_scheme": "supersetColors",
            "show_legend": False,
        },
    )
    if cid:
        chart_ids.append(cid)

    # ── 차트 4: 공고 인기도 등급 분포 (파이) ─────────────────
    if ds_popularity:
        cid = create_chart(
            name="공고 인기도 등급 분포",
            chart_type="pie",
            dataset_id=ds_popularity,
            params={
                "groupby": ["popularity_grade"],
                "metric": {"expressionType": "SIMPLE", "column": {"column_name": "job_id"},
                           "aggregate": "COUNT", "label": "공고 수"},
                "donut": False,
                "show_legend": True,
                "show_labels": True,
                "label_type": "key_percent",
                "color_scheme": "supersetColors",
            },
        )
        if cid:
            chart_ids.append(cid)

        # ── 차트 5: 직군별 평균 인기도 점수 (막대) ───────────
        cid = create_chart(
            name="직군별 평균 인기도 점수",
            chart_type="bar",
            dataset_id=ds_popularity,
            params={
                "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "popularity_score"},
                             "aggregate": "AVG", "label": "평균 인기도 점수"}],
                "groupby": ["job_category"],
                "x_axis_label": "직군",
                "y_axis_label": "평균 인기도 점수",
                "color_scheme": "supersetColors",
                "show_legend": False,
                "order_bars": True,
            },
        )
        if cid:
            chart_ids.append(cid)

        # ── 차트 6: 인기도 Top 10 공고 (테이블) ──────────────
        cid = create_chart(
            name="인기도 Top 10 공고",
            chart_type="table",
            dataset_id=ds_popularity,
            params={
                "metrics": [],
                "groupby": ["job_title", "company", "job_category",
                            "career_type", "popularity_grade", "popularity_score"],
                "order_by_cols": [["popularity_score", False]],
                "page_length": 10,
                "include_search": True,
                "table_timestamp_format": "smart_date",
            },
        )
        if cid:
            chart_ids.append(cid)

    return chart_ids


# ── 5. 대시보드 생성 ─────────────────────────────────────────

def create_dashboard(chart_ids: list) -> int:
    name = "일로온 분석 대시보드"

    # 기존 확인
    r = session.get(
        f"{SUPERSET}/api/v1/dashboard/",
        params={"q": json.dumps({"filters": [{"col": "dashboard_title", "opr": "eq", "value": name}]})},
        timeout=10,
    )
    results = r.json().get("result", [])
    if results:
        did = results[0]["id"]
        ok(f"기존 대시보드 사용: {name} (id={did})")
        return did

    # 레이아웃 — 2열 그리드
    layout = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
    }

    payload = {
        "dashboard_title": name,
        "slug": "iloon-analysis",
        "published": True,
        "position_json": json.dumps(layout),
        "metadata": json.dumps({"color_scheme": "supersetColors"}),
    }
    r = session.post(f"{SUPERSET}/api/v1/dashboard/", json=payload, timeout=15)
    if r.status_code not in (200, 201):
        warn(f"대시보드 생성 실패: {r.status_code} {r.text}")
        return None

    did = r.json()["id"]
    ok(f"대시보드 생성: {name} (id={did})")

    # 차트 연결
    if chart_ids:
        r2 = session.put(
            f"{SUPERSET}/api/v1/dashboard/{did}",
            json={"charts": chart_ids},
            timeout=15,
        )
        if r2.status_code in (200, 201):
            ok(f"차트 {len(chart_ids)}개 연결 완료")
        else:
            warn(f"차트 연결 실패: {r2.status_code} {r2.text}")

    return did


# ── main ─────────────────────────────────────────────────────

def main():
    print(f"\n{'='*60}")
    print("  일로온 Superset 대시보드 자동 설정")
    print(f"{'='*60}")

    # 서버 확인
    step("Superset 서버 상태 확인")
    for attempt in range(1, 4):
        try:
            r = requests.get(f"{SUPERSET}/health", timeout=5)
            if r.status_code == 200:
                ok("Superset 정상")
                break
        except Exception:
            pass
        if attempt < 3:
            print(f"  재시도 {attempt}/3... (5초 대기)")
            time.sleep(5)
    else:
        fail(
            "Superset 연결 실패\n"
            "  → docker-compose up -d superset 후 약 30초 대기하고 재시도하세요."
        )

    login()

    db_id = get_or_create_database()

    step("데이터셋 등록")
    ds_segments   = get_or_create_dataset(db_id, "user_segments")
    ds_popularity = get_or_create_dataset(db_id, "job_popularity")

    step("차트 생성")
    chart_ids = build_charts(ds_segments, ds_popularity)
    print(f"\n  생성된 차트: {len(chart_ids)}개")

    step("대시보드 생성")
    did = create_dashboard(chart_ids)

    print(f"\n{'='*60}")
    print("  완료!")
    if did:
        print(f"  대시보드 URL : {SUPERSET}/superset/dashboard/{did}/")
    print(f"  Superset UI  : {SUPERSET}  (admin / admin)")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
