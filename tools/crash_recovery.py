"""Drive a real crash/recovery cycle against a separately started server."""
import json
import os
import signal
import subprocess
import sys
import time
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PORT = int(os.environ.get("CRASH_PORT", "8095"))
DATA = os.environ.get("CRASH_DATA", os.path.join(ROOT, "state-crash"))
BASE = f"http://127.0.0.1:{PORT}"
PIDFILE = f"/tmp/shared-scan-crash-{PORT}.pid"


def call(method, path, payload=None, timeout=15):
    data = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(BASE + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


def wait_up(deadline=15):
    end = time.time() + deadline
    while time.time() < end:
        try:
            urllib.request.urlopen(BASE + "/api/files", timeout=1).read()
            return True
        except Exception:
            time.sleep(0.2)
    return False


def start():
    env = dict(os.environ, SCAN_PORT=str(PORT), SCAN_DATA_DIR=DATA,
               SCAN_SHARD_PACE="0.25")
    log = open("/tmp/crash-server.log", "ab")
    proc = subprocess.Popen(
        [sys.executable, "-I", os.path.join(ROOT, "server", "main.py")],
        cwd=ROOT, env=env, stdout=log, stderr=log)
    with open(PIDFILE, "w") as fh:
        fh.write(str(proc.pid))
    return proc


def kill_stale():
    """Kill a server recorded by an earlier run of this script."""
    try:
        pid = int(open(PIDFILE).read().strip())
    except (OSError, ValueError):
        return
    try:
        os.kill(pid, signal.SIGKILL)
        time.sleep(0.3)
    except ProcessLookupError:
        pass


def main():
    # Clear any stale server left by an earlier failed run on this port.
    os.makedirs(os.path.dirname(os.path.abspath(PIDFILE)), exist_ok=True)
    kill_stale()
    subprocess.run(["rm", "-rf", DATA], check=True)
    proc = start()
    try:
        assert wait_up(), "server did not start"
        call("POST", "/api/files/generate",
             {"name": "big.jsonl", "lines": 4000, "seed": 3})
        # Start a query and actually open its SSE stream (in a thread) so the
        # worker passes the connection gate and begins journaling shards.
        q = call("POST", "/api/query",
                 {"file": "big.jsonl", "filter": {"kind": "all"},
                  "label": "crash-victim"})

        def drain():
            try:
                req = urllib.request.Request(
                    f"{BASE}/api/subscribe/{q['subscriber_id']}/stream")
                urllib.request.urlopen(req, timeout=30).read()
            except Exception:
                pass

        import threading
        threading.Thread(target=drain, daemon=True).start()
        time.sleep(0.7)  # with 0.25s pacing, several shards are durable now
        journals_before = os.listdir(os.path.join(DATA, "journals"))
        print("journals before crash:", journals_before)
        assert journals_before, "no journal written before crash"
        jpath = os.path.join(DATA, "journals", journals_before[0])
        shard_records = sum(1 for ln in open(jpath) if '"SHARD"' in ln)
        print("durable SHARD records before crash:", shard_records)
        assert shard_records > 0, "no shard journaled before crash"

        # Hard kill — no graceful cleanup; this is the recovery scenario.
        proc.send_signal(signal.SIGKILL)
        proc.wait(timeout=5)
        print("process killed (exit", proc.returncode, ")")

        proc = start()
        assert wait_up(), "server did not restart"
        state = call("GET", "/api/state")
        recovered = [s for s in state["sessions"]
                     if s["session_id"] == q["session_id"]]
        assert recovered, "session was not recovered"
        rs = recovered[0]
        print("recovered session state:", rs["state"],
              "completed shards:", len(rs["completed_shards"]), "/",
              rs["shard_count"])
        assert rs["state"] == "paused", "recovered session must stay paused"
        assert rs["completed_shards"], "no durable shards recovered"
        durable_before = set(rs["completed_shards"])

        # A new query on the SAME version must reuse the recovered session.
        q2 = call("POST", "/api/query",
                  {"file": "big.jsonl", "filter": {"kind": "all"},
                   "label": "after-reboot"})
        print("post-reboot query mode:", q2["shared_mode"],
              "session:", q2["session_id"])
        assert q2["session_id"] == q["session_id"], \
            "did not attach to recovered scan"

        req = urllib.request.Request(
            f"{BASE}/api/subscribe/{q2['subscriber_id']}/stream")
        n = 0
        end = None
        with urllib.request.urlopen(req, timeout=30) as resp:
            event = None
            for raw in resp:
                line = raw.decode().strip()
                if line.startswith("event:"):
                    event = line.split(":", 1)[1].strip()
                elif line.startswith("data:") and event == "end":
                    end = json.loads(line.split(":", 1)[1].strip())
                    break
                elif line.startswith("data:") and event == "record":
                    n += 1
        print("recovered subscriber got", n, "records, end:", end["state"])
        truth = call("POST", "/api/independent",
                     {"file": "big.jsonl", "filter": {"kind": "all"}})
        assert n == truth["matched_records"], (n, truth["matched_records"])
        assert end["state"] == "done"

        # The worker must not have re-read the shards durable before the
        # crash. Its own reads cover only the not-yet-completed suffix, so
        # worker_bytes_read is strictly less than one full file; the catch-up
        # bytes (private re-reads for the late subscriber) are tracked apart.
        state2 = call("GET", "/api/state")
        sess = next(s for s in state2["sessions"]
                    if s["session_id"] == q["session_id"])
        file_size = truth["physical_bytes_read"]
        print("worker reads after recovery:", sess["worker_bytes_read"],
              "catch-up reads:", sess["catchup_bytes_read"],
              "file size:", file_size,
              "durable shards skipped:", len(durable_before))
        assert sess["worker_bytes_read"] < file_size, \
            "worker re-read durable shards after recovery"
        assert sess["worker_bytes_read"] + sess["catchup_bytes_read"] \
            == sess["physical_bytes_read"]
        assert set(sess["completed_shards"]) >= durable_before
        print("CRASH RECOVERY OK")
    finally:
        try:
            proc.terminate()
            proc.wait(timeout=5)
        except Exception:
            pass


if __name__ == "__main__":
    main()
