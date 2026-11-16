"""Instrumented live slow-consumer test: log every SSE line with timestamps."""
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
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read().decode())


q = call("POST", "/api/query", {
    "file": "demo.jsonl", "filter": {"kind": "all"}, "label": "instr-slow",
    "max_buffer_items": 20, "max_buffer_bytes": 32768, "offer_timeout": 0.5})
sid = q["subscriber_id"]
print("created", sid)
threading.Timer(0.4, lambda: call("POST", f"/api/subscribe/{sid}/slow",
                                  {"slow": True})).start()

req = urllib.request.Request(f"{BASE}/api/subscribe/{sid}/stream")
n_rec = 0
t0 = time.time()
try:
    with urllib.request.urlopen(req, timeout=12) as resp:
        print("response headers:", dict(resp.headers).get("Connection"),
              "proto:", resp.version)
        for raw in resp:
            line = raw.decode().strip()
            if line.startswith("event:"):
                ev = line.split(":", 1)[1].strip()
                if ev != "record":
                    print(f"+{time.time()-t0:5.2f}s EVENT {ev}")
                else:
                    n_rec += 1
                    if n_rec % 5 == 0:
                        print(f"+{time.time()-t0:5.2f}s records={n_rec}")
            elif line.startswith("data:") and n_rec == 0:
                print(f"+{time.time()-t0:5.2f}s {line[:70]}")
except socket.timeout:
    print(f"SOCKET TIMEOUT after {time.time()-t0:.2f}s, records={n_rec}")
print("DONE records=", n_rec)
print("server:", call("GET", f"/api/subscriber/{sid}")["state"])
