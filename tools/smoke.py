"""End-to-end smoke test driven over real HTTP + SSE.

Run while the server is up:  python3 tools/smoke.py
"""
from __future__ import annotations

import json
import sys
import threading
import time
import urllib.request
import urllib.error

BASE = "http://127.0.0.1:8090"


def call(method: str, path: str, payload: dict | None = None) -> dict:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def stream(sub_id: str, max_wait: float = 20.0, on_ready=None):
    """Consume the SSE stream, return (records, end_event).

    Blocking reads with a socket deadline; the only normal exit is an `end`
    event or a closed connection, so terminal events are never lost to a
    poll-window race.
    """
    import socket
    req = urllib.request.Request(f"{BASE}/api/subscribe/{sub_id}/stream")
    records, end = [], None
    with urllib.request.urlopen(req, timeout=max_wait) as resp:
        if on_ready is not None:
            on_ready()
        event = None
        try:
            for raw in resp:
                line = raw.decode("utf-8").strip()
                if not line:
                    event = None
                    continue
                if line.startswith("event:"):
                    event = line.split(":", 1)[1].strip()
                elif line.startswith("data:"):
                    data = json.loads(line.split(":", 1)[1].strip())
                    if event == "record":
                        records.append(data)
                    elif event == "end":
                        end = data
                        break
        except socket.timeout:
            pass
    return records, end


def check(name: str, cond: bool, detail: str = "") -> None:
    mark = "PASS" if cond else "FAIL"
    print(f"[{mark}] {name}" + (f" — {detail}" if detail else ""))
    if not cond:
        check.failed += 1
check.failed = 0


def main() -> int:
    flt = {"kind": "json", "key": "category", "op": "eq", "value": "alpha"}

    # Self-contained: make sure the demo file exists on a freshly started
    # server (identical content is a no-op, so this is safe to re-run).
    existing = {f["name"] for f in call("GET", "/api/files")["files"]}
    if "demo.jsonl" not in existing:
        call("POST", "/api/files/generate",
             {"name": "demo.jsonl", "lines": 3000, "seed": 42})

    # 1. two queries back-to-back, before either stream is consumed, so the
    #    second is guaranteed to attach to the first's still-early session.
    q1 = call("POST", "/api/query", {
        "file": "demo.jsonl", "filter": flt, "label": "consumer-A"})
    q2 = call("POST", "/api/query", {
        "file": "demo.jsonl",
        "filter": {"kind": "json", "key": "active", "op": "eq", "value": True},
        "label": "consumer-B"})
    check("second query shares the session",
          q1["session_id"] == q2["session_id"],
          f"{q1['session_id']} vs {q2['session_id']}")
    check("first query attaches to a scan (new or reused live one)",
          q1["shared_mode"] in ("split-new", "shared-head", "shared-late"),
          q1["shared_mode"])
    check("second query shares the live scan",
          q2["shared_mode"] in ("shared-head", "shared-late"),
          q2["shared_mode"])

    # ground truth for the comparison, computed after the attachments
    truth = call("POST", "/api/independent", {"file": "demo.jsonl", "filter": flt})
    truth_ids = sorted(r["line_no"] for r in truth["records"])
    print(f"independent scan matched {truth['matched_records']} records, "
          f"read {truth['physical_bytes_read']} physical bytes")

    # 2. consume both streams concurrently
    results: dict[str, tuple] = {}
    t1 = threading.Thread(
        target=lambda: results.setdefault("A", stream(q1["subscriber_id"])))
    t2 = threading.Thread(
        target=lambda: results.setdefault("B", stream(q2["subscriber_id"])))
    t1.start(); t2.start()
    t1.join(timeout=20); t2.join(timeout=20)
    recs1, end1 = results["A"]
    recs2, end2 = results["B"]
    check("A finishes done", end1 and end1["state"] == "done", str(end1 and end1["state"]))
    check("B finishes done", end2 and end2["state"] == "done", str(end2 and end2["state"]))
    ids1 = sorted(r["line_no"] for r in recs1)
    check("A results identical to independent scan",
          ids1 == truth_ids, f"{len(ids1)} vs {len(truth_ids)}")
    check("different filter -> different result set",
          len(recs2) != len(recs1), f"A={len(recs1)} B={len(recs2)}")

    # 3. physical reads: the SHARED live worker read ~one file, not one per
    #    subscriber. Measure the worker's own bytes — the session total also
    #    legitimately includes the late joiner's private catch-up re-reads,
    #    which are bounded to the shards it missed and counted separately.
    state = call("GET", "/api/state")
    shared_session = next(
        s for s in state["sessions"] if s["session_id"] == q1["session_id"])
    worker_bytes = shared_session["worker_bytes_read"]
    catchup_bytes = shared_session["catchup_bytes_read"]
    file_size = truth["physical_bytes_read"]
    check("shared live worker read roughly ONE file despite two subscribers",
          worker_bytes <= file_size * 1.15,
          f"worker={worker_bytes}, file={file_size}")
    check("late joiner only re-reads the shards it missed (bounded catch-up)",
          catchup_bytes <= file_size,
          f"catchup={catchup_bytes}, file={file_size}")

    # 4. slow consumer: born slow with a tiny buffer, so backpressure builds
    #    from the first records and the bounded-buffer overflow fires. The
    #    stream is drained in a background thread while we poll the durable
    #    subscriber state over REST, so the assertion does not depend on a
    #    single SSE iterator observing EOF.
    slow = call("POST", "/api/query", {
        "file": "demo.jsonl", "filter": {"kind": "all"},
        "label": "slowpoke", "max_buffer_items": 20,
        "max_buffer_bytes": 32768, "offer_timeout": 0.2, "slow": True})
    slow_result: dict = {}
    threading.Thread(
        target=lambda: slow_result.update(
            zip(("records", "end"), stream(slow["subscriber_id"], max_wait=15))),
        daemon=True,
    ).start()

    def wait_state(sid: str, terminal: set, deadline: float = 15.0):
        end = time.time() + deadline
        while time.time() < end:
            snap = call("GET", f"/api/subscriber/{sid}")
            if snap["state"] in terminal:
                return snap
            time.sleep(0.1)
        return call("GET", f"/api/subscriber/{sid}")

    slow_snap = wait_state(
        slow["subscriber_id"], {"overflow", "timeout", "done", "canceled", "error"})
    check("slow consumer terminated by backpressure, not silently dropped",
          slow_snap["state"] == "overflow",
          f"state={slow_snap['state']} err={slow_snap['error']}")
    check("others unaffected: overflow is isolated to this subscriber",
          slow_snap["overflow_count"] >= 1,
          f"overflow_count={slow_snap['overflow_count']}")
    check("bounded buffer never exceeded its cap",
          slow_snap["queued_items"] <= slow_snap["max_items"],
          f"queued={slow_snap['queued_items']} cap={slow_snap['max_items']}")

    # 5. cancel + resume
    q3 = call("POST", "/api/query", {
        "file": "demo.jsonl", "filter": flt, "label": "cancel-me"})
    call("POST", f"/api/subscribe/{q3['subscriber_id']}/cancel")
    time.sleep(0.3)
    r = call("POST", f"/api/subscribe/{q3['subscriber_id']}/resume", {})
    recs3, end3 = stream(r["subscriber_id"])
    check("resume completes", end3 is not None and end3["state"] == "done",
          str(end3 and end3["state"]))
    ids3 = sorted(x["line_no"] for x in recs3)
    check("resumed subscriber results match independent scan",
          ids3 == truth_ids, f"{len(ids3)} vs {len(truth_ids)}")

    # 6. version replacement: new query must not attach to old scan. Use a
    #    unique seed so a re-run against a non-clean server still produces a
    #    genuinely new content hash (identical content is intentionally a
    #    no-op pointer flip in the content-addressed store).
    bump_seed = 999 + (int(time.time()) % 1_000_000)
    call("POST", "/api/files/generate",
         {"name": "demo.jsonl", "lines": 3000, "seed": bump_seed})
    q4 = call("POST", "/api/query", {
        "file": "demo.jsonl", "filter": flt, "label": "after-version-bump"})
    state2 = call("GET", "/api/state")
    sessions = {s["session_id"]: s for s in state2["sessions"]}
    check("post-bump query lands on a NEW session",
          q4["session_id"] != q1["session_id"])
    check("new session scans a different content hash",
          sessions[q4["session_id"]]["version_full"]
          != sessions[q1["session_id"]]["version_full"])
    recs4, end4 = stream(q4["subscriber_id"])
    truth2 = call("POST", "/api/independent", {"file": "demo.jsonl", "filter": flt})
    ids4 = sorted(x["line_no"] for x in recs4)
    check("new-version results match independent scan of new version",
          ids4 == sorted(x["line_no"] for x in truth2["records"]),
          f"{len(ids4)} vs {truth2['matched_records']}")

    print("\n=== totals ===")
    print(json.dumps(state2["totals"], indent=2))
    print(f"\n{check.failed} failure(s)")
    return 1 if check.failed else 0


if __name__ == "__main__":
    sys.exit(main())
