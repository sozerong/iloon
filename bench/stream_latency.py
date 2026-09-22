"""
스트리밍 end-to-end 지연 실측 — Kafka produce → PostgreSQL 적재

무엇을 재는가
    t0 = 프로브 이벤트가 Kafka 에 기록된 시각 (send().get() 이 브로커 ack 를 받고 돌아온 시각)
    t1 = 그 이벤트가 속한 윈도우 행이 realtime_event_stats 에 나타난 시각 (호스트에서 폴링)
    지연 = t1 - t0

    이벤트의 `event_time` 이 아니라 **produce 시각**을 기준으로 잡는다. `event_time` 은
    생성기가 페이로드에 써 넣는 논리 시각이라 과거로도 미래로도 잡을 수 있고,
    그걸 기준으로 재면 파이프라인 지연이 아니라 생성기 설정을 재게 된다.
    produce 시각은 파이프라인 바깥에서 관측된 사실이라 조작할 여지가 없다.
    t0 · t1 을 같은 프로세스(호스트)의 같은 시계로 찍으므로 컨테이너 시계 오차도 섞이지 않는다.

왜 하트비트를 계속 보내는가
    append 모드 윈도우는 watermark 가 window_end 를 넘겨야 방출된다. 그런데 watermark 는
    **관측된 event_time 에서만** 전진한다. 측정 중 트래픽이 끊기면 프로브가 속한 윈도우는
    영원히 닫히지 않는다. 그래서 측정 내내 1초에 1건씩 하트비트를 넣는다.
    즉 여기서 재는 값은 **연속 트래픽 상태의 지연**이고, 그것이 기록할 측정 조건이다.

전제 — 스트리밍 잡이 이미 돌고 있어야 한다 (`startingOffsets=latest` 라 먼저 떠 있어야 한다):
    docker compose up -d kafka postgres
    docker compose run --rm --no-deps \
      -e PYSPARK_SUBMIT_ARGS="--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.4 pyspark-shell" \
      --entrypoint bash airflow-scheduler -lc "cd /opt/airflow/project && python tools/stream_events.py"

사용
    python bench/stream_latency.py --repeat 5
    python bench/stream_latency.py --selftest
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import threading
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bench.timing import summarize  # 반복 측정 통계는 기존 하니스를 그대로 쓴다

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS  = os.path.join(BASE_DIR, "bench", "results")

KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
KAFKA_TOPIC   = "user-events"

PG = dict(
    host=os.environ.get("PG_HOST", "localhost"),
    port=int(os.environ.get("PG_PORT", "5432")),
    user=os.environ.get("PG_USER", "airflow"),
    password=os.environ.get("PG_PASSWORD", "airflow"),
    dbname=os.environ.get("PG_JOB_DB", "iloon_jobs"),
)

# 프로브·하트비트 event_type 접두사. 실제 이벤트 종류와 섞이지 않게 하고,
# 측정이 끝나면 이 접두사로 지운다.
PREFIX = "bench_"


# ── 선언된 상한 ───────────────────────────────────────────────
def declared_bound(window_s: float = 30.0,
                   watermark_s: float = 60.0,
                   trigger_s: float = 30.0) -> Dict[str, float]:
    """
    stream_events.py 에 박혀 있는 세 값만으로 나오는 상한. 내가 고른 숫자가 아니다.

      window(30s)     프로브가 윈도우 시작 직후에 들어오면 window_end 까지 최대 window_s
      watermark(60s)  watermark = max(event_time) - watermark_s 이므로, window_end 를 넘기려면
                      event_time 이 window_end + watermark_s 에 도달해야 한다
      trigger(30s) ×2 그 이벤트를 읽는 배치까지 한 트리거, watermark 는 배치가 끝날 때 갱신되고
                      방출은 그 값을 쓰는 **다음** 배치라 한 트리거 더
    배치 실행·적재 시간은 선언된 값이 아니라 여기 없다. 실측이 상한을 넘으면 그 몫이다.
    """
    return {
        "window": window_s,
        "watermark": watermark_s,
        "trigger_observe": trigger_s,
        "trigger_emit": trigger_s,
        "total": window_s + watermark_s + 2 * trigger_s,
    }


def window_start_of(ts: datetime, window_s: int) -> datetime:
    """
    Spark `F.window` 의 윈도우 시작. epoch 기준으로 자른다.
    30초 윈도우는 UTC 기준으로 잘리지만 KST 오프셋이 분 단위라 로컬 naive 로 계산해도 같다.
    """
    sec = ts.hour * 3600 + ts.minute * 60 + ts.second
    floored = (sec // window_s) * window_s
    return ts.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(seconds=floored)


# ── Kafka ────────────────────────────────────────────────────
def make_event(event_type: str, ts: datetime) -> Dict[str, Any]:
    """stream_events.EVENT_SCHEMA 가 읽는 필드만 채운다. 이름이 어긋나면 from_json 이
    조용히 null 을 내고 행이 영영 안 나타난다 — selftest 가 이 이름들을 잠근다."""
    return {
        "event_type":        event_type,
        "user_id":           str(uuid.uuid4()),
        "job_id":            "bench-0001",
        "is_ai_recommended": False,
        "session_duration":  1,
        "timestamp":         ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3],
    }


class Heartbeat(threading.Thread):
    """측정 내내 watermark 를 전진시키는 배경 트래픽."""

    def __init__(self, producer, interval: float):
        super().__init__(daemon=True)
        self.producer, self.interval = producer, interval
        self.sent = 0
        # 이름이 `_stop` 이면 Thread._stop() 을 가려서 join() 이 터진다.
        self._stopped = threading.Event()

    def run(self) -> None:
        while not self._stopped.is_set():
            self.producer.send(KAFKA_TOPIC, make_event(PREFIX + "heartbeat", datetime.now()))
            self.producer.flush()
            self.sent += 1
            self._stopped.wait(self.interval)

    def stop(self) -> None:
        self._stopped.set()


# ── PostgreSQL 폴링 ───────────────────────────────────────────
def fetch_row(conn, event_type: str) -> Optional[Tuple[datetime, datetime, int]]:
    """해당 event_type 의 윈도우 행. 테이블이 아직 없으면 None (스트림이 첫 적재 전)."""
    import psycopg2

    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT window_start, window_end, event_count "
                "FROM realtime_event_stats WHERE event_type = %s",
                (event_type,),
            )
            return cur.fetchone()
    except psycopg2.errors.UndefinedTable:
        conn.rollback()
        return None


def probe(producer, conn, event_type: str, poll: float, timeout: float) -> Dict[str, Any]:
    """프로브 1건 발행 → 행이 나타날 때까지 폴링."""
    ev = make_event(event_type, datetime.now())
    event_time = datetime.strptime(ev["timestamp"], "%Y-%m-%dT%H:%M:%S.%f")
    producer.send(KAFKA_TOPIC, ev).get(timeout=30)   # 브로커 ack 까지 기다린 뒤 t0 을 찍는다
    t0 = time.time()

    deadline = t0 + timeout
    while time.time() < deadline:
        row = fetch_row(conn, event_type)
        if row:
            t1 = time.time()
            ws, we, cnt = row
            # 시각 해석이 어긋나면(타임존·파싱) 지연값이 통째로 틀린다. 여기서 잡는다.
            expected = window_start_of(event_time, 30)
            if ws != expected:
                raise SystemExit(
                    f"윈도우 불일치: 행 {ws} vs 예상 {expected} (event_time={event_time}) "
                    "— 타임존이나 timestamp 파싱을 확인할 것"
                )
            return {
                "event_type":        event_type,
                "produced_at":       datetime.fromtimestamp(t0).isoformat(timespec="milliseconds"),
                "event_time":        event_time.isoformat(timespec="milliseconds"),
                "window_start":      ws.isoformat(),
                "window_end":        we.isoformat(),
                "event_count":       cnt,
                "latency_seconds":   t1 - t0,
                # 지연이 어디서 왔는지 쪼갠다: produce 가 윈도우 안 어디에 떨어졌는가 / 윈도우가 닫힌 뒤 얼마나 걸렸는가
                "offset_in_window_seconds": (event_time - ws).total_seconds(),
                "after_window_end_seconds": t1 - we.timestamp(),
            }
        time.sleep(poll)
    return {"event_type": event_type, "latency_seconds": None, "timeout_seconds": timeout}


def wait_ready(producer, conn, tag: str, poll: float, every: float, timeout: float) -> float:
    """
    스트림이 실제로 소비 중인지 확인. `startingOffsets=latest` 라 쿼리가 뜨기 전에 보낸 건
    영영 안 잡힌다 — 웜업 프로브를 주기적으로 넣고 하나라도 적재되면 준비된 것으로 본다.
    event_type 에 실행 tag 를 붙인다. 지난 측정이 남긴 행을 보고 "준비됐다"고 오판하면
    스트림이 죽어 있어도 측정이 진행돼 버린다.
    """
    t0 = time.time()
    n = 0
    sent: List[str] = []
    while time.time() - t0 < timeout:
        if (time.time() - t0) >= n * every:
            n += 1
            et = f"{PREFIX}warmup_{tag}_{n}"
            producer.send(KAFKA_TOPIC, make_event(et, datetime.now())).get(timeout=30)
            sent.append(et)
            print(f"  웜업 프로브 {n} 발행")
        for et in sent:
            if fetch_row(conn, et):
                waited = time.time() - t0
                print(f"  스트림 소비 확인 ({waited:.1f}s, 웜업 {n}건)\n")
                return waited
        time.sleep(poll)
    raise SystemExit(
        f"{timeout:.0f}s 안에 웜업 프로브가 적재되지 않았다 — 스트리밍 잡이 떠 있는지 확인할 것"
    )


# ── 측정 ─────────────────────────────────────────────────────
def measure(args) -> Dict[str, Any]:
    import psycopg2
    from kafka import KafkaProducer

    bound = declared_bound(args.window, args.watermark, args.trigger)
    print("=" * 66)
    print("  스트리밍 end-to-end 지연 — Kafka produce → PostgreSQL 적재")
    print("=" * 66)
    print(f"  Kafka {KAFKA_SERVERS} / PostgreSQL {PG['host']}:{PG['port']}/{PG['dbname']}")
    print(f"  선언 상한 {bound['total']:.0f}s "
          f"= window {bound['window']:.0f} + watermark {bound['watermark']:.0f} "
          f"+ trigger {bound['trigger_observe']:.0f}×2")
    print(f"  반복 {args.repeat}회 · 폴링 {args.poll}s · 하트비트 {args.heartbeat}s 간격\n")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )
    conn = psycopg2.connect(connect_timeout=10, **PG)
    conn.autocommit = True

    hb = Heartbeat(producer, args.heartbeat)
    hb.start()

    runs: List[Dict[str, Any]] = []
    try:
        tag = datetime.now().strftime("%H%M%S")
        ready_s = wait_ready(producer, conn, tag, args.poll, every=30.0, timeout=args.timeout)
        for i in range(1, args.repeat + 1):
            r = probe(producer, conn, f"{PREFIX}probe_{tag}_{i}", args.poll, args.timeout)
            runs.append(r)
            if r["latency_seconds"] is None:
                print(f"  [{i}/{args.repeat}] 미적재 — {args.timeout:.0f}s 초과")
            else:
                print(f"  [{i}/{args.repeat}] {r['latency_seconds']:7.2f}s "
                      f"(윈도우 내 위치 {r['offset_in_window_seconds']:5.2f}s, "
                      f"윈도우 종료 후 {r['after_window_end_seconds']:6.2f}s)")
            # 지연의 변동분은 거의 전부 "프로브가 윈도우의 어디에 떨어졌는가"에서 온다.
            # 대기를 고정하면 다음 프로브의 위상이 (지연 + 대기) mod 30 으로 한 점에 고정돼
            # 같은 위상만 반복해서 재게 된다 — 회차마다 대기를 gap_step 씩 늘려 위상을 훑는다.
            if i < args.repeat:
                time.sleep(args.gap + (i - 1) * args.gap_step)
    finally:
        hb.stop()
        hb.join(timeout=5)
        producer.flush()
        producer.close()

    ok = [r["latency_seconds"] for r in runs if r["latency_seconds"] is not None]
    out: Dict[str, Any] = {
        "name": "stream_latency",
        "recorded_at": datetime.now().isoformat(timespec="seconds"),
        "definition": "Kafka produce(ack) → realtime_event_stats 행 관측",
        "meta": {
            "repeat": args.repeat,
            "poll_interval_seconds": args.poll,
            "heartbeat_interval_seconds": args.heartbeat,
            "heartbeat_events": hb.sent,
            "gap_between_probes_seconds": args.gap,
            "gap_step_seconds": args.gap_step,
            "stream_ready_wait_seconds": round(ready_s, 1),
            "kafka": KAFKA_SERVERS,
            "postgres": f"{PG['host']}:{PG['port']}/{PG['dbname']}",
            "window_seconds": args.window,
            "watermark_seconds": args.watermark,
            "trigger_seconds": args.trigger,
        },
        "declared_bound": bound,
        "runs": runs,
    }

    if ok:
        out["stats"] = summarize(ok)
        out["verdict"] = "상한 이내" if out["stats"]["max"] <= bound["total"] else "상한 초과"
        print(f"\n  n={len(ok)}  p50 {out['stats']['p50']:.2f}s  p95 {out['stats']['p95']:.2f}s  "
              f"min {out['stats']['min']:.2f}s  max {out['stats']['max']:.2f}s")
        print(f"  판정: {out['verdict']} (선언 상한 {bound['total']:.0f}s, "
              f"최댓값이 상한의 {out['stats']['max'] / bound['total'] * 100:.0f}%)")
    else:
        out["verdict"] = "측정 실패 — 적재된 프로브 없음"
        print(f"\n  {out['verdict']}")

    if not args.keep:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM realtime_event_stats WHERE event_type LIKE %s", (PREFIX + "%",))
            print(f"\n  측정용 행 {cur.rowcount}건 정리")
    conn.close()

    os.makedirs(RESULTS, exist_ok=True)
    path = os.path.join(RESULTS, f"stream_latency_{datetime.now():%Y%m%d_%H%M%S}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"  저장: {os.path.relpath(path, BASE_DIR)}\n")
    return out


# ── 자체 검증 ─────────────────────────────────────────────────
def selftest() -> None:
    b = declared_bound(30, 60, 30)
    assert b["total"] == 150, b                      # 30 + 60 + 30×2
    assert declared_bound(30, 60, 0)["total"] == 90   # 트리거가 즉시라면 윈도우+워터마크만 남는다
    assert declared_bound(30, 0, 30)["total"] == 90   # watermark 0 이면 윈도우가 닫히자마자 후보

    # 윈도우 경계 — 30초 텀블링
    w = lambda s: window_start_of(datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%f"), 30)
    assert w("2026-09-22T10:00:00.000") == datetime(2026, 9, 22, 10, 0, 0)
    assert w("2026-09-22T10:00:29.999") == datetime(2026, 9, 22, 10, 0, 0)
    assert w("2026-09-22T10:00:30.000") == datetime(2026, 9, 22, 10, 0, 30)
    assert w("2026-09-22T23:59:59.500") == datetime(2026, 9, 22, 23, 59, 30)

    # 페이로드 필드가 stream_events.EVENT_SCHEMA 와 어긋나면 from_json 이 null 을 낸다.
    # 그러면 행이 안 나타나고 "지연 무한"으로 오독된다 — 이름을 소스에서 직접 읽어 잠근다.
    src = open(os.path.join(BASE_DIR, "tools", "stream_events.py"), encoding="utf-8").read()
    fields = set(re.findall(r'StructField\(\s*"(\w+)"', src))
    assert fields and fields <= set(make_event("x", datetime.now())), (fields, make_event("x", datetime.now()).keys())

    # 판정 경계: 상한과 같으면 이내, 넘으면 초과
    assert (150.0 <= b["total"]) and not (150.1 <= b["total"])

    print("stream_latency 자체 검증 통과")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="스트리밍 end-to-end 지연 실측")
    ap.add_argument("--repeat",    type=int,   default=3,   help="프로브 반복 횟수 (기본 3)")
    ap.add_argument("--poll",      type=float, default=0.5, help="PostgreSQL 폴링 간격 초 (기본 0.5)")
    ap.add_argument("--heartbeat", type=float, default=1.0, help="하트비트 간격 초 (기본 1.0)")
    ap.add_argument("--gap",       type=float, default=5.0,  help="프로브 사이 기본 대기 초 (기본 5)")
    ap.add_argument("--gap_step",  type=float, default=6.0,  help="회차마다 대기를 늘리는 폭 초 (기본 6, 윈도우 위상을 훑는다)")
    ap.add_argument("--timeout",   type=float, default=240.0, help="프로브 1건 대기 상한 초 (기본 240)")
    ap.add_argument("--window",    type=float, default=30.0, help="스트림의 윈도우 길이 초")
    ap.add_argument("--watermark", type=float, default=60.0, help="스트림의 watermark 초")
    ap.add_argument("--trigger",   type=float, default=30.0, help="스트림의 트리거 주기 초")
    ap.add_argument("--keep",      action="store_true", help="측정용 행을 지우지 않는다")
    ap.add_argument("--selftest",  action="store_true")
    args = ap.parse_args()

    if args.selftest:
        selftest()
    else:
        measure(args)
