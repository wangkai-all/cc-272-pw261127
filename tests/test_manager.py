"""In-process integration tests for ScanManager (no HTTP layer).

These exercise the share/split decision, per-subscriber filter independence,
bounded-buffer backpressure isolation, resume exactly-once delivery, and the
hard rule that a new file version can never attach to an old version's scan.
"""
import shutil
import tempfile
import threading
import time
import unittest

from server.engine import (
    SUB_DONE, SUB_OVERFLOW, SESSION_FINISHED,
)
from server.filters import build_filter
from server.manager import ScanManager


def drain_async(sub):
    """Register a consumer and drain in a background thread.

    The engine gates delivery until a consumer connects, so a subscriber that
    is only drained after another one finishes would hit its connect timeout.
    Draining each subscriber on its own thread mirrors the real SSE case.
    """
    box = {"records": [], "state": None}

    def run():
        sub.add_consumer()
        try:
            while True:
                item = sub.buffer.get(timeout=0.2)
                if item is not None:
                    box["records"].append(item)
                    continue
                if sub.state in (
                    "done", "canceled", "overflow", "timeout", "error",
                ):
                    while True:
                        tail = sub.buffer.get(timeout=0.05)
                        if tail is None:
                            break
                        box["records"].append(tail)
                    box["state"] = sub.state
                    return
        finally:
            sub.remove_consumer()

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread, box


def join(thread, box, timeout=15):
    thread.join(timeout=timeout)
    return box["records"], box["state"]


class ManagerIntegration(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.mgr = ScanManager(data_dir=self.dir, shard_size=64,
                               shard_pace_seconds=0.0)
        self._make_file("data.txt", 300, seed=1)

    def tearDown(self):
        self.mgr.shutdown()
        shutil.rmtree(self.dir, ignore_errors=True)

    def _make_file(self, name, lines, seed):
        st = seed or 1
        rows = []
        for i in range(1, lines + 1):
            st = (1103515245 * st + 12345) & 0x7FFFFFFF
            cat = ["alpha", "beta", "gamma"][st % 3]
            rows.append(f'{{"id":{i},"category":"{cat}"}}')
        self.mgr.register_file(name, ("\n".join(rows) + "\n").encode())

    def test_concurrent_queries_share_one_scan(self):
        f1 = build_filter({"kind": "json", "key": "category",
                           "op": "eq", "value": "alpha"})
        f2 = build_filter({"kind": "all"})
        s1, sub1 = self.mgr.query(file_name="data.txt", flt=f1, label="a")
        t1, b1 = drain_async(sub1)
        s2, sub2 = self.mgr.query(file_name="data.txt", flt=f2, label="b")
        t2, b2 = drain_async(sub2)
        # Same in-flight blob -> one physical scan shared by both subscribers.
        self.assertEqual(s1.session_id, s2.session_id)
        r1, st1 = join(t1, b1)
        r2, st2 = join(t2, b2)
        self.assertEqual(st1, SUB_DONE)
        self.assertEqual(st2, SUB_DONE)
        # different filters -> different result sizes from one physical scan
        self.assertLess(len(r1), len(r2))
        # the shared worker read the blob once, not once per subscriber
        self.assertLessEqual(s1.worker_bytes_read,
                             s1.file_size + s1.shard_size)

    def test_results_match_independent_ground_truth(self):
        flt = build_filter({"kind": "json", "key": "category",
                            "op": "eq", "value": "beta"})
        _, sub = self.mgr.query(file_name="data.txt", flt=flt)
        thread, box = drain_async(sub)
        records, state = join(thread, box)
        truth = self.mgr.independent_query(file_name="data.txt", flt=flt)
        got = sorted(r.line_no for r in records)
        want = sorted(r["line_no"] for r in truth["records"])
        self.assertEqual(state, SUB_DONE)
        self.assertEqual(got, want)

    def test_worker_does_not_start_before_first_subscriber(self):
        # Regression: a fresh split session used to start its worker inside
        # _create_session, before any subscriber was attached. The first shard
        # then fanned out to an empty target set and record #1 was silently
        # lost. The worker may start only AFTER a subscriber is attached.
        # Asserted deterministically on the causal ordering (no timing race):
        # wrap _create_session to capture the session the instant it has been
        # created, but before query() attaches the subscriber / starts anything.
        f_all = build_filter({"kind": "all"})
        captured = {}
        original_create = self.mgr._create_session

        def spy_create(file_name, version_id, file_size):
            sess = original_create(file_name, version_id, file_size)
            captured["session"] = sess
            captured["worker_before_attach"] = sess._worker
            captured["subs_before_attach"] = len(sess.subscribers)
            return sess

        self.mgr._create_session = spy_create
        sess, sub = self.mgr.query(file_name="data.txt", flt=f_all)
        self.mgr._create_session = original_create

        self.assertIsNone(
            captured["worker_before_attach"],
            "worker started before any subscriber was attached",
        )
        self.assertEqual(captured["subs_before_attach"], 0)
        # After query() returns the subscriber is published and the worker is
        # started, in that order — never a worker scanning into the void.
        self.assertIn(sub.sub_id, sess.subscribers)
        self.assertIsNotNone(sess._worker)

        thread, box = drain_async(sub)
        records, state = join(thread, box)
        self.assertEqual(state, SUB_DONE)
        line_nos = [r.line_no for r in records]
        # The first physical lines were never dropped into an empty fan-out.
        self.assertEqual(line_nos[:3], [1, 2, 3])
        self.assertEqual(len(line_nos), 300)
        self.assertEqual(min(line_nos), 1)

    def test_slow_consumer_overflow_is_isolated(self):
        # Two subscribers on one scan. The slow one connects but never drains:
        # its bounded buffer fills and the offer timeout kills ONLY it. The
        # healthy subscriber keeps draining and must get every record.
        f_all = build_filter({"kind": "all"})
        sess, healthy = self.mgr.query(
            file_name="data.txt", flt=f_all,
            max_buffer_items=5, max_buffer_bytes=4096, offer_timeout=0.1)
        # Connect the healthy consumer BEFORE adding the slow peer so the first
        # fanned-out records find an attached consumer (mirrors real SSE).
        ht, hb = drain_async(healthy)
        _, slow = self.mgr.query(
            file_name="data.txt", flt=f_all,
            max_buffer_items=5, max_buffer_bytes=4096, offer_timeout=0.1)
        # The "slow" consumer: connected gate, zero draining.
        slow.add_consumer()

        deadline = time.time() + 10
        while time.time() < deadline and slow.state != SUB_OVERFLOW:
            time.sleep(0.05)
        self.assertEqual(slow.state, SUB_OVERFLOW)
        self.assertEqual(slow.overflow_count, 1)
        # its buffer never exceeded the item cap — memory stays bounded
        self.assertLessEqual(slow.buffer.snapshot()["queued_items"], 5)

        h_records, h_state = join(ht, hb)
        self.assertEqual(h_state, SUB_DONE)
        # healthy subscriber got every line despite the neighbour's death
        self.assertEqual(len(h_records), 300)
        self.assertEqual(sess.state, SESSION_FINISHED)

    def test_cancel_then_resume_is_exactly_once(self):
        flt = build_filter({"kind": "json", "key": "category",
                            "op": "eq", "value": "gamma"})
        _, sub = self.mgr.query(file_name="data.txt", flt=flt)
        # A real client always opens the SSE stream (registering a consumer)
        # before/while records flow; mirror that so the connect gate behaves as
        # it does in production rather than stalling on an abandoned query.
        t0, b0 = drain_async(sub)
        # cancel almost immediately, while the scan is still in flight
        time.sleep(0.02)
        self.mgr.cancel_subscriber(sub.sub_id)
        t0.join(timeout=5)
        _, sub2 = self.mgr.resume_subscriber(sub_id=sub.sub_id)
        thread, box = drain_async(sub2)
        records, state = join(thread, box)
        truth = self.mgr.independent_query(file_name="data.txt", flt=flt)
        line_nos = [r.line_no for r in records]
        self.assertEqual(state, SUB_DONE)
        # no duplicates, no missing rows
        self.assertEqual(len(line_nos), len(set(line_nos)))
        self.assertEqual(sorted(line_nos),
                         sorted(r["line_no"] for r in truth["records"]))

    def test_new_version_never_attaches_to_old_scan(self):
        flt = build_filter({"kind": "all"})
        old_sess, old_sub = self.mgr.query(file_name="data.txt", flt=flt)
        thread, box = drain_async(old_sub)
        join(thread, box)
        old_version = old_sess.version_id
        # replace the file with different content -> new immutable version
        self._make_file("data.txt", 150, seed=999)
        new_info = self.mgr.registry.resolve("data.txt")
        self.assertNotEqual(new_info.version_id, old_version)
        new_sess, new_sub = self.mgr.query(file_name="data.txt", flt=flt)
        thread, box = drain_async(new_sub)
        records, state = join(thread, box)
        self.assertNotEqual(new_sess.session_id, old_sess.session_id)
        self.assertEqual(new_sess.version_id, new_info.version_id)
        # new version has only 150 lines; an old-scan leak would give 300
        self.assertEqual(state, SUB_DONE)
        self.assertEqual(len(records), 150)
        self.assertLessEqual(max(r.line_no for r in records), 150)

    def test_explicit_old_version_query_stays_pinned(self):
        flt = build_filter({"kind": "all"})
        old_sess, old_sub = self.mgr.query(file_name="data.txt", flt=flt)
        thread, box = drain_async(old_sub)
        join(thread, box)
        old_version = old_sess.version_id
        blob_size = len(open(
            self.mgr.registry.blob_path(old_version), "rb").read())
        self._make_file("data.txt", 150, seed=999)
        # explicit pin to the historical content hash
        info = self.mgr.registry.resolve("data.txt", old_version)
        sess, sub = self.mgr.query(
            file_name="data.txt", flt=flt, version_id=old_version)
        thread, box = drain_async(sub)
        records, state = join(thread, box)
        self.assertEqual(sess.version_id, old_version)
        self.assertEqual(info.size, blob_size)
        self.assertEqual(len(records), 300)
        self.assertEqual(state, SUB_DONE)


if __name__ == "__main__":
    unittest.main(verbosity=2)
