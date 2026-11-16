"""Reproduce using the EXACT stream() implementation from smoke.py."""
import json
import socket
import threading
import time
import urllib.request

BASE = "http://127.0.0.1:8090"


def call(method, path, payload=None):
    data = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(BASE + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode())


def stream(sub_id, max_wait=20.0, on_ready=None):
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


flt = {"kind": "json", "key": "category", "op": "eq", "value": "alpha"}
# shared pair
q1 = call("POST", "/api/query", {"file": "demo.jsonl", "filter": flt, "label": "A"})
q2 = call("POST", "/api/query", {"file": "demo.jsonl",
                                  "filter": {"kind": "all"}, "label": "B"})
res = {}
t1 = threading.Thread(target=lambda: res.setdefault("a", stream(q1["subscriber_id"])))
t2 = threading.Thread(target=lambda: res.setdefault("b", stream(q2["subscriber_id"])))
t1.start(); t2.start(); t1.join(); t2.join()
print("shared done:", {k: v[1]["state"] for k, v in res.items()})

# now born-slow
print("creating born-slow...")
slow = call("POST", "/api/query", {
    "file": "demo.jsonl", "filter": {"kind": "all"}, "label": "slowpoke2",
    "max_buffer_items": 20, "max_buffer_bytes": 32768,
    "offer_timeout": 0.5, "slow": True})
print("streaming born-slow", slow["subscriber_id"], "...")
t0 = time.time()
_, e = stream(slow["subscriber_id"], max_wait=15)
print("slow result:", e and e["state"], "in", round(time.time() - t0, 2), "s")
