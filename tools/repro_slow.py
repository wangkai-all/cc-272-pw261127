"""Minimal urllib reproduction of the slow-consumer SSE flow."""
import json
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
    "file": "demo.jsonl", "filter": {"kind": "all"}, "label": "urllib-slow",
    "max_buffer_items": 20, "max_buffer_bytes": 32768, "offer_timeout": 0.5})
sid = q["subscriber_id"]
print("created", sid, "at", round(time.time(), 2))


def mark_slow():
    time.sleep(0.4)
    call("POST", f"/api/subscribe/{sid}/slow", {"slow": True})
    print("marked slow at", round(time.time(), 2))


threading.Thread(target=mark_slow, daemon=True).start()

req = urllib.request.Request(f"{BASE}/api/subscribe/{sid}/stream")
n_records = 0
end = None
t0 = time.time()
try:
    with urllib.request.urlopen(req, timeout=15) as resp:
        print("connected at", round(time.time(), 2))
        event = None
        for raw in resp:
            line = raw.decode().strip()
            if line.startswith("event:"):
                event = line.split(":", 1)[1].strip()
            elif line.startswith("data:"):
                data = json.loads(line.split(":", 1)[1].strip())
                if event == "record":
                    n_records += 1
                elif event == "end":
                    end = data
                    break
except Exception as exc:
    print("CLIENT EXCEPTION after", round(time.time() - t0, 2), "s:",
          type(exc).__name__, exc)

print("records=", n_records, "end=", end and end["state"],
      "elapsed=", round(time.time() - t0, 2))
state = call("GET", f"/api/subscriber/{sid}")
print("SERVER FINAL:", state["state"], "err=", state["error"],
      "q=", state["queued_items"])
