"""
log_api 버퍼링 검증

핵심: 버퍼에 남아 있던 이벤트가 graceful shutdown 때 전부 디스크로 내려가는가.
FLUSH_SIZE(100)의 배수가 아닌 건수를 보내서 종료 시점에 반드시 잔여분이 남게 한다.

실행:
    python -m pytest tests/test_log_api.py -v
"""

from __future__ import annotations

import importlib
import json
import os
import sys
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List

import pytest
from fastapi.testclient import TestClient

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

EVENT_DATE = "2026-08-24"


def make_events(n: int) -> List[Dict[str, Any]]:
    base = datetime(2026, 8, 24, 12, 0, 0)
    return [
        {
            "event_id":   str(uuid.uuid4()),
            "timestamp":  (base + timedelta(seconds=i)).isoformat(),
            "session_id": f"sess-{i % 10}",
            "user_id":    f"user-{i % 7}",
            "event_type": "click",
            "job_id":     f"job-{i}",
        }
        for i in range(n)
    ]


@pytest.fixture()
def api(tmp_path, monkeypatch):
    """격리된 로그 디렉토리로 log_api를 새로 임포트한다."""
    import asyncio

    # log_api는 모듈 로드 시점에 asyncio.Lock()을 만든다 (3.9에서는 현재 루프를 참조).
    # 앞선 테스트가 루프를 닫았을 수 있으니 새로 깔아준다.
    asyncio.set_event_loop(asyncio.new_event_loop())

    monkeypatch.setenv("ANALYSIS_BASE_DIR", str(tmp_path))
    monkeypatch.setenv("ANALYSIS_LOGS_DIR", str(tmp_path / "logs"))
    monkeypatch.setenv("ANALYSIS_APPLOG_DIR", str(tmp_path / "applogs"))

    if "log_api" in sys.modules:
        del sys.modules["log_api"]
    module = importlib.import_module("log_api")
    importlib.reload(module)
    return module, tmp_path / "logs"


def read_lines(logs_dir) -> List[Dict[str, Any]]:
    path = logs_dir / f"user-events-{EVENT_DATE}.jsonl"
    if not path.exists():
        return []
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def test_graceful_shutdown_loses_nothing(api):
    """250건 전송 → graceful shutdown → 파일에 정확히 250건."""
    module, logs_dir = api
    total = 250                       # FLUSH_SIZE(100)의 배수가 아니어야 잔여분이 생긴다
    assert total % module.FLUSH_SIZE != 0, "잔여 버퍼가 없으면 이 테스트는 의미가 없다"

    events = make_events(total)
    with TestClient(module.app) as client:          # __exit__ 에서 lifespan shutdown
        for i in range(0, total, 25):
            r = client.post("/logs/bulk", json={"events": events[i:i + 25]})
            assert r.status_code == 202, r.text

        # 종료 전에는 일부가 아직 버퍼에 남아 있어야 한다 (버퍼링이 동작한다는 증거)
        assert client.get("/health").json()["buffered"] > 0

    rows = read_lines(logs_dir)
    assert len(rows) == total, f"유실 발생: {len(rows)} / {total}"
    assert len({r["event_id"] for r in rows}) == total, "중복 기록됨"


def test_threshold_flush_is_durable_before_response(api, anyio_backend="asyncio"):
    """
    임계치 플러시는 **응답 바디를 내보내는 시점에 이미 디스크에 있어야** 한다.

    TestClient로는 이걸 검증할 수 없다 — TestClient가 BackgroundTasks까지 기다린 뒤
    응답 객체를 돌려주기 때문이다. 그래서 ASGI를 직접 호출해서, 서버가
    'http.response.body'를 보내는 그 순간의 파일 상태를 본다.
    BackgroundTasks로 미루는 구현에서는 이 시점에 파일이 비어 있다.
    """
    import asyncio

    module, logs_dir = api
    payload = json.dumps({"events": make_events(module.FLUSH_SIZE)}).encode()
    at_response: Dict[str, int] = {}

    async def run() -> None:
        scope = {
            "type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1",
            "method": "POST", "path": "/logs/bulk", "raw_path": b"/logs/bulk",
            "query_string": b"", "root_path": "", "scheme": "http",
            "headers": [
                (b"host", b"testserver"),
                (b"content-type", b"application/json"),
                (b"content-length", str(len(payload)).encode()),
            ],
            "client": ("127.0.0.1", 1234), "server": ("testserver", 80),
        }

        sent_body = False
        never = asyncio.Event()

        async def receive():
            # 바디는 한 번만. 이후 호출은 영원히 대기한다 —
            # 여기서 http.disconnect를 돌려주면 미들웨어가 바디 전송을 중단해버린다.
            nonlocal sent_body
            if sent_body:
                await never.wait()
            sent_body = True
            return {"type": "http.request", "body": payload, "more_body": False}

        async def send(message):
            # 클라이언트가 응답 바디를 받는 바로 그 순간의 디스크 상태
            if message["type"] == "http.response.body" and "lines" not in at_response:
                at_response["lines"] = len(read_lines(logs_dir))

        # lifespan을 직접 열고 닫는다 (플러시 태스크가 떠 있어야 실제 동작과 같다)
        async with module.lifespan(module.app):
            await module.app(scope, receive, send)

    # asyncio.run()은 루프를 닫아버려 이후 테스트의 모듈 재임포트가 깨진다
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(run())
    finally:
        loop.close()
        asyncio.set_event_loop(asyncio.new_event_loop())

    assert at_response.get("lines") == module.FLUSH_SIZE, (
        f"응답 시점에 디스크 반영 {at_response.get('lines')} / {module.FLUSH_SIZE} — "
        "플러시가 응답 이후로 미뤄지고 있다 (유실 창)"
    )


def test_invalid_timestamp_rejected(api):
    """ISO 8601 강제가 유지되는지 — 버퍼링 도입으로 검증이 느슨해지면 안 된다."""
    module, _ = api
    with TestClient(module.app) as client:
        bad = make_events(1)
        bad[0]["timestamp"] = "2026/08/24 12:00:00"
        r = client.post("/logs/bulk", json={"events": bad})
        assert r.status_code == 422


def test_empty_events_rejected(api):
    module, _ = api
    with TestClient(module.app) as client:
        assert client.post("/logs/bulk", json={"events": []}).status_code == 400
