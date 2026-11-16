"""HTTP + SSE front door for the shared-scan service.

Endpoints (JSON unless noted):

* ``GET  /`` and ``/static/*``            — web UI
* ``GET  /api/files``                     — file registry / versions
* ``POST /api/files``                     — upload or replace a file (new version)
* ``POST /api/files/generate``            — generate a deterministic demo file
* ``POST /api/query``                     — start a query (shared or split)
* ``GET  /api/subscribe/<sub_id>/stream`` — SSE: records + lifecycle events
* ``POST /api/subscribe/<sub_id>/cancel`` — cancel one subscriber
* ``POST /api/subscribe/<sub_id>/resume`` — resume a cancelled/terminated one
* ``POST /api/subscribe/<sub_id>/slow``   — throttle one consumer (demo)
* ``GET  /api/subscriber/<sub_id>``       — subscriber snapshot
* ``POST /api/independent``               — ground-truth full-file scan
* ``GET  /api/state``                     — all sessions, subscribers, totals, events
* ``POST /api/debug/crash``               — hard-kill the process (recovery demo)
* ``POST /api/debug/corrupt``             — poison a journal (recovery demo)
"""
from __future__ import annotations

import json
import os
import signal
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from server.filters import FilterError, build_filter
from server.manager import (
    DEFAULT_MAX_BUFFER_BYTES,
    DEFAULT_MAX_BUFFER_ITEMS,
    DEFAULT_OFFER_TIMEOUT,
    ManagerError,
    ScanManager,
)
from server.versions import validate_name

WEB_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "web")
MAX_BODY = 32 * 1024 * 1024


class App:
    def __init__(self, manager: ScanManager):
        self.manager = manager


# -------------------------------------------------------------- request handler
class Handler(BaseHTTPRequestHandler):
    server_version = "SharedScan/1.0"

    # silence default noisy logging; events feed the UI log panel instead
    def log_message(self, fmt, *args):  # noqa: A003
        pass

    @property
    def app(self) -> App:
        return self.server.app

    # ------------------------------------------------------------ helpers
    def _json(self, status: int, payload) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _error(self, status: int, message: str) -> None:
        self._json(status, {"error": message})

    def _read_body(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        if length <= 0:
            return {}
        if length > MAX_BODY:
            raise ValueError("request body too large")
        raw = self.rfile.read(length)
        return json.loads(raw.decode("utf-8"))

    def _sub_id(self, prefix: str) -> str:
        path = urlparse(self.path).path
        marker = f"/api/{prefix}/"
        rest = path[len(marker):]
        return rest.split("/", 1)[0]

    # --------------------------------------------------------------- GET
    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        try:
            if path == "/" or path == "/index.html":
                return self._serve_static("index.html")
            if path.startswith("/static/"):
                return self._serve_static(path[len("/static/"):])
            if path == "/api/files":
                return self._json(200, {"files": self.app.manager.list_files()})
            if path == "/api/state":
                return self._json(200, self.app.manager.snapshot())
            if path == "/api/events":
                return self._json(200, {"events": self.app.manager.events(200)})
            if path.startswith("/api/subscribe/") and path.endswith("/stream"):
                sub_id = path[len("/api/subscribe/"):-len("/stream")]
                return self._serve_sse(sub_id)
            if path.startswith("/api/subscriber/"):
                sub_id = self._sub_id("subscriber")
                sub = self.app.manager.get_subscriber(sub_id)
                return self._json(200, sub.snapshot())
            self._error(404, "not found")
        except ManagerError as exc:
            self._error(404, str(exc))
        except (ValueError, KeyError) as exc:
            self._error(400, str(exc))

    def _serve_static(self, rel: str) -> None:
        # path traversal guard: only basename files inside WEB_DIR
        rel = rel.replace("\\", "/")
        rel = os.path.normpath("/" + rel).lstrip("/")
        full = os.path.join(WEB_DIR, rel)
        if not os.path.isfile(full):
            return self._error(404, f"no such static file: {rel}")
        ctype = {
            ".html": "text/html; charset=utf-8",
            ".js": "application/javascript; charset=utf-8",
            ".css": "text/css; charset=utf-8",
            ".svg": "image/svg+xml",
        }.get(os.path.splitext(full)[1], "application/octet-stream")
        with open(full, "rb") as fh:
            body = fh.read()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    # -------------------------------------------------------------- POST
    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        try:
            body = self._read_body()
            if path == "/api/files":
                return self._post_files(body)
            if path == "/api/files/generate":
                return self._post_generate(body)
            if path == "/api/query":
                return self._post_query(body)
            if path == "/api/independent":
                return self._post_independent(body)
            if path.startswith("/api/subscribe/") and path.endswith("/cancel"):
                return self._post_cancel(path)
            if path.startswith("/api/subscribe/") and path.endswith("/resume"):
                return self._post_resume(path, body)
            if path.startswith("/api/subscribe/") and path.endswith("/slow"):
                return self._post_slow(path, body)
            if path == "/api/debug/crash":
                return self._post_crash()
            if path == "/api/debug/corrupt":
                return self._post_corrupt(body)
            self._error(404, "not found")
        except FilterError as exc:
            self._error(400, f"invalid filter: {exc}")
        except ManagerError as exc:
            self._error(409, str(exc))
        except (ValueError, KeyError) as exc:
            self._error(400, str(exc))
        except json.JSONDecodeError as exc:
            self._error(400, f"invalid JSON body: {exc}")

    # ------------------------------------------------------- file endpoints
    def _post_files(self, body: dict) -> None:
        name = body.get("name", "")
        validate_name(name)
        content = body.get("content", "")
        if not isinstance(content, str):
            raise ValueError("content must be a string")
        data = content.encode("utf-8")
        entry = self.app.manager.register_file(name, data)
        self._json(200, {"file": entry})

    def _post_generate(self, body: dict) -> None:
        name = body.get("name", "demo.jsonl")
        validate_name(name)
        lines = int(body.get("lines", 5000))
        if not (1 <= lines <= 2_000_000):
            raise ValueError("lines must be between 1 and 2,000,000")
        seed = int(body.get("seed", 42))
        categories = body.get("categories", ["alpha", "beta", "gamma", "delta"])
        payload = self._generate_jsonl(lines, seed, categories)
        entry = self.app.manager.register_file(name, payload)
        self._json(200, {"file": entry, "generated_bytes": len(payload)})

    @staticmethod
    def _generate_jsonl(lines: int, seed: int, categories) -> bytes:
        # Tiny deterministic LCG so regenerating "the same file" yields the
        # same blob and a changed parameter yields a new version.
        state = seed or 1
        out = []
        for i in range(1, lines + 1):
            state = (1103515245 * state + 12345) & 0x7FFFFFFF
            cat = categories[state % len(categories)]
            state = (1103515245 * state + 12345) & 0x7FFFFFFF
            score = state % 1000
            state = (1103515245 * state + 12345) & 0x7FFFFFFF
            active = (state % 2) == 0
            msg = f"record {i:08d} payload block with repeating data " * (
                1 + (state % 4)
            )
            row = {
                "id": i,
                "category": cat,
                "score": score,
                "active": active,
                "msg": msg.strip(),
            }
            out.append(json.dumps(row, ensure_ascii=False, separators=(",", ":")))
        return ("\n".join(out) + "\n").encode("utf-8")

    # ------------------------------------------------------ query endpoints
    def _post_query(self, body: dict) -> None:
        name = body["file"]
        flt = build_filter(body.get("filter"))
        timeout = body.get("timeout_seconds")
        session, sub = self.app.manager.query(
            file_name=name,
            flt=flt,
            label=str(body.get("label", "")),
            version_id=body.get("version_id"),
            timeout_seconds=float(timeout) if timeout is not None else None,
            max_buffer_items=int(
                body.get("max_buffer_items", DEFAULT_MAX_BUFFER_ITEMS)
            ),
            max_buffer_bytes=int(
                body.get("max_buffer_bytes", DEFAULT_MAX_BUFFER_BYTES)
            ),
            offer_timeout=float(
                body.get("offer_timeout", DEFAULT_OFFER_TIMEOUT)
            ),
        )
        # A query can be born "slow" so backpressure builds from the first
        # record instead of racing a later /slow toggle.
        if body.get("slow"):
            sub.slow = True
        self._json(200, {
            "subscriber_id": sub.sub_id,
            "session_id": session.session_id,
            "shared_mode": sub.shared_mode,
            "state": sub.state,
            "slow": sub.slow,
            "stream_url": f"/api/subscribe/{sub.sub_id}/stream",
        })

    def _post_independent(self, body: dict) -> None:
        name = body["file"]
        flt = build_filter(body.get("filter"))
        result = self.app.manager.independent_query(
            file_name=name,
            flt=flt,
            version_id=body.get("version_id"),
            limit=int(body.get("limit", 5000)),
        )
        self._json(200, result)

    # ------------------------------------------------- subscriber lifecycle
    def _post_cancel(self, path: str) -> None:
        sub_id = path[len("/api/subscribe/"):-len("/cancel")]
        sub = self.app.manager.cancel_subscriber(sub_id)
        self._json(200, {"subscriber_id": sub_id, "state": sub.state})

    def _post_resume(self, path: str, body: dict) -> None:
        sub_id = path[len("/api/subscribe/"):-len("/resume")]
        timeout = body.get("timeout_seconds")
        session, sub = self.app.manager.resume_subscriber(
            sub_id=sub_id,
            timeout_seconds=float(timeout) if timeout is not None else None,
        )
        self._json(200, {
            "subscriber_id": sub.sub_id,
            "session_id": session.session_id,
            "shared_mode": sub.shared_mode,
            "state": sub.state,
            "stream_url": f"/api/subscribe/{sub.sub_id}/stream",
        })

    def _post_slow(self, path: str, body: dict) -> None:
        sub_id = path[len("/api/subscribe/"):-len("/slow")]
        slow = bool(body.get("slow", True))
        self.app.manager.set_slow(sub_id, slow)
        self._json(200, {"subscriber_id": sub_id, "slow": slow})

    # ------------------------------------------------------------ debug
    def _post_crash(self) -> None:
        # Flush what we can, then SIGKILL ourselves: supervisor restarts and
        # journal recovery takes over.
        self._json(200, {"crashing": True})
        sys.stdout.flush()
        sys.stderr.flush()
        time.sleep(0.1)
        os.kill(os.getpid(), signal.SIGKILL)

    def _post_corrupt(self, body: dict) -> None:
        session_id = body["session_id"]
        path = self.app.manager.corrupt_journal_for_demo(session_id)
        self._json(200, {"corrupted": path})

    # ---------------------------------------------------------------- SSE
    def _serve_sse(self, sub_id: str) -> None:
        manager = self.app.manager
        sub = manager.get_subscriber(sub_id)
        # The stream has no Content-Length and ends with an `end` event. Force
        # the connection closed when the handler returns so clients (which
        # detect EOF, not a request boundary) terminate promptly and reliably.
        self.close_connection = True
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()

        def send(event: str, data: dict) -> bool:
            try:
                payload = json.dumps(data, ensure_ascii=False)
                self.wfile.write(f"event: {event}\ndata: {payload}\n\n".encode("utf-8"))
                self.wfile.flush()
                return True
            except (BrokenPipeError, ConnectionResetError, OSError):
                return False

        sub.add_consumer()
        try:
            if not send("ready", {"subscriber_id": sub_id, "state": sub.state}):
                return

            idle_loops = 0
            while True:
                # Terminal state takes priority over any records still queued
                # so a backpressure-killed subscriber's stream always ends.
                if sub.state in ("done", "canceled", "overflow", "timeout", "error"):
                    drained = 0
                    while True:
                        tail = sub.buffer.get(timeout=0.05)
                        if tail is None:
                            break
                        if not send("record", tail.to_dict()):
                            return
                        drained += 1
                    send("end", {
                        "subscriber_id": sub_id,
                        "state": sub.state,
                        "error": sub.error,
                        "drained_after_end": drained,
                        "snapshot": sub.snapshot(),
                    })
                    return
                # A "slow" consumer deliberately drains lazily so its bounded
                # buffer fills and the overflow/backpressure path is visible.
                if sub.slow:
                    time.sleep(0.25)
                item = sub.buffer.get(timeout=0.5)
                if item is not None:
                    idle_loops = 0
                    if not send("record", item.to_dict()):
                        return
                    continue
                idle_loops += 1
                if idle_loops % 15 == 0:  # ~7.5s heartbeat keeps proxies happy
                    if not send("heartbeat", {"ts": time.time(), "state": sub.state}):
                        return
        finally:
            sub.remove_consumer()


def build_server(host: str, port: int, data_dir: str, shard_size: int,
                 shard_pace_seconds: float) -> ThreadingHTTPServer:
    manager = ScanManager(
        data_dir=data_dir,
        shard_size=shard_size,
        shard_pace_seconds=shard_pace_seconds,
    )
    httpd = ThreadingHTTPServer((host, port), Handler)
    httpd.allow_reuse_address = True
    httpd.app = App(manager)
    httpd.daemon_threads = True

    def shutdown(*_args):
        manager.shutdown()
        httpd.shutdown()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    return httpd


def main() -> None:
    host = os.environ.get("SCAN_HOST", "127.0.0.1")
    port = int(os.environ.get("SCAN_PORT", "8080"))
    data_dir = os.environ.get("SCAN_DATA_DIR", os.path.join(os.getcwd(), "state"))
    shard_size = int(os.environ.get("SCAN_SHARD_SIZE", str(128 * 1024)))
    shard_pace = float(os.environ.get("SCAN_SHARD_PACE", "0.15"))
    os.makedirs(data_dir, exist_ok=True)
    httpd = build_server(host, port, data_dir, shard_size, shard_pace)
    print(
        f"shared-scan listening on http://{host}:{port} "
        f"(data_dir={data_dir}, shard_size={shard_size}, pace={shard_pace}s)",
        flush=True,
    )
    try:
        httpd.serve_forever()
    finally:
        httpd.app.manager.shutdown()
        httpd.server_close()


if __name__ == "__main__":
    main()
