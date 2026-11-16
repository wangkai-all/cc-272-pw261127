"""Focused born-slow consumer: watch client-side SSE behavior to the end."""
import json
import socket
import time
import urllib.request

BASE = "http://127.0.0.1:8090"


def call(method, path, payload=None):
    data = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(BASE + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read().decode())


q = call("POST", "/api/query", {
    "file": "demo.jsonl", "filter": {"kind": "all"}, "label": "born-slow",
    "max_buffer_items": 20, "max_buffer_bytes": 32768,
    "offer_timeout": 0.5, "slow": True})
sid = q["subscriber_id"]
print("created born-slow", sid)

req = urllib.request.Request(f"{BASE}/api/subscribe/{sid}/stream")
n_rec = 0
end = None
t0 = time.time()
try:
    with urllib.request.urlopen(req, timeout=15) as resp:
        for raw in resp:
            line = raw.decode().strip()
            if line.startswith("event:"):
                ev = line.split(":", 1)[1].strip()
                if ev == "record":
                    n_rec += 1
                elif ev == "end":
                    end = "end-seen"
                    print(f"+{time.time()-t0:.2f}s END event")
                    break
except socket.timeout:
    print(f"CLIENT socket.timeout after {time.time()-t0:.2f}s records={n_rec}")
except Exception as exc:
    print(f"CLIENT {type(exc).__name__} after {time.time()-t0:.2f}s: {exc}")
print(f"records={n_rec} end={end}")
st = call("GET", f"/api/subscriber/{sid}")
print("SERVER:", st["state"], "err=", st["error"], "q=", st["queued_items"])
