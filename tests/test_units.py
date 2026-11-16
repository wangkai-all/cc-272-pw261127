"""Unit tests for the building blocks: buffers, filters, versions, journal."""
import json
import os
import shutil
import tempfile
import time
import unittest

from server.engine import BoundedBuffer, Record
from server.filters import FilterError, build_filter
from server.journal import (
    JournalError, ScanJournal, plan_shards, recover_journals,
)
from server.versions import FileRegistry, validate_name


class TestBoundedBuffer(unittest.TestCase):
    def test_item_cap(self):
        buf = BoundedBuffer(max_items=3, max_bytes=10_000)
        for i in range(3):
            self.assertTrue(buf.put(i, 10, timeout=0.05))
        self.assertFalse(buf.put("x", 10, timeout=0.1))

    def test_byte_cap(self):
        buf = BoundedBuffer(max_items=100, max_bytes=100)
        self.assertTrue(buf.put("a", 60, timeout=0.05))
        # 60 + 60 > 100 -> blocked
        self.assertFalse(buf.put("b", 60, timeout=0.1))
        self.assertEqual(buf.get(), "a")
        # space freed
        self.assertTrue(buf.put("b", 60, timeout=0.2))

    def test_fifo_order_and_close(self):
        buf = BoundedBuffer(max_items=10, max_bytes=10_000)
        for i in range(5):
            buf.put(i, 1, timeout=1)
        buf.close()
        out = []
        while True:
            v = buf.get(timeout=0.1)
            if v is None:
                break
            out.append(v)
        self.assertEqual(out, [0, 1, 2, 3, 4])

    def test_snapshot_counters(self):
        buf = BoundedBuffer(max_items=10, max_bytes=100)
        buf.put("x", 40, timeout=1)
        snap = buf.snapshot()
        self.assertEqual(snap["queued_items"], 1)
        self.assertEqual(snap["queued_bytes"], 40)
        self.assertFalse(snap["closed"])


class TestFilters(unittest.TestCase):
    def test_all(self):
        f = build_filter(None)
        self.assertTrue(f.matches("anything"))

    def test_contains(self):
        f = build_filter({"kind": "contains", "pattern": "abc"})
        self.assertTrue(f.matches("xx abc yy"))
        self.assertFalse(f.matches("xx yy"))

    def test_regex_and_bad_pattern(self):
        f = build_filter({"kind": "regex", "pattern": "a[0-9]+z"})
        self.assertTrue(f.matches("a42z"))
        self.assertFalse(f.matches("az"))
        with self.assertRaises(FilterError):
            build_filter({"kind": "regex", "pattern": "("})

    def test_json_ops(self):
        line = json.dumps({"cat": "x", "n": 5, "active": True})
        self.assertTrue(
            build_filter({"kind": "json", "key": "cat", "op": "eq",
                          "value": "x"}).matches(line))
        self.assertTrue(
            build_filter({"kind": "json", "key": "n", "op": "gt",
                          "value": 4}).matches(line))
        self.assertFalse(
            build_filter({"kind": "json", "key": "n", "op": "lt",
                          "value": 5}).matches(line))
        self.assertTrue(
            build_filter({"kind": "json", "key": "active", "op": "eq",
                          "value": True}).matches(line))
        # non-json line never matches a json filter
        self.assertFalse(
            build_filter({"kind": "json", "key": "cat"}).matches("not json"))

    def test_unknown_kind_rejected(self):
        with self.assertRaises(FilterError):
            build_filter({"kind": "bogus"})


class TestPlanShards(unittest.TestCase):
    def test_empty_file(self):
        self.assertEqual(plan_shards(0, 100), [])

    def test_exact_multiple(self):
        self.assertEqual(plan_shards(300, 100),
                         [(0, 100), (100, 200), (200, 300)])

    def test_partial_last(self):
        self.assertEqual(plan_shards(250, 100),
                         [(0, 100), (100, 200), (200, 250)])

    def test_deterministic(self):
        self.assertEqual(plan_shards(12345, 777), plan_shards(12345, 777))


class TestVersionRegistry(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def test_content_addressing_and_version_chain(self):
        reg = FileRegistry(self.dir)
        e1, v1, new1 = reg.register("f.jsonl", b"version-one\n")
        self.assertTrue(new1)
        self.assertEqual(v1.seq, 1)
        # identical content is idempotent (no new seq)
        _, v1b, new1b = reg.register("f.jsonl", b"version-one\n")
        self.assertFalse(new1b)
        self.assertEqual(v1b.seq, 1)
        # changed content -> new immutable version, pointer flips
        e2, v2, new2 = reg.register("f.jsonl", b"version-two-longer\n")
        self.assertTrue(new2)
        self.assertEqual(v2.seq, 2)
        self.assertNotEqual(v1.version_id, v2.version_id)
        self.assertEqual(reg.get("f.jsonl").current, v2.version_id)
        # old blob still exists and is resolvable (old scans keep working)
        old = reg.resolve("f.jsonl", v1.version_id)
        self.assertEqual(old.version_id, v1.version_id)
        with open(reg.blob_path(v1.version_id), "rb") as fh:
            self.assertEqual(fh.read(), b"version-one\n")

    def test_two_versions_are_distinct_blobs(self):
        reg = FileRegistry(self.dir)
        _, va, _ = reg.register("f", b"aaaa\n")
        _, vb, _ = reg.register("f", b"bbbb\n")
        self.assertNotEqual(va.version_id, vb.version_id)
        self.assertTrue(os.path.exists(reg.blob_path(va.version_id)))
        self.assertTrue(os.path.exists(reg.blob_path(vb.version_id)))

    def test_reload_persists(self):
        reg = FileRegistry(self.dir)
        reg.register("f", b"hello\n")
        reg2 = FileRegistry(self.dir)
        self.assertEqual(len(reg2.list_files()), 1)
        self.assertEqual(reg2.get("f").versions[0].size, 6)

    def test_name_validation(self):
        validate_name("ok-file_1.txt")
        for bad in ["", "a/b", "x" * 200, "a b", "a:b", "a*b", "../etc"]:
            with self.assertRaises(ValueError):
                validate_name(bad)


class TestJournalRecovery(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.jdir = os.path.join(self.dir, "journals")
        self.qdir = os.path.join(self.dir, "quarantine")
        self.blobs = set()

        def blob_exists(vid):
            return vid in self.blobs
        self.blob_exists = blob_exists

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def _write_journal(self, session_id, vid, size, shard_size, shards,
                       ended=False):
        j = ScanJournal(self.jdir, session_id)
        j.open_append()
        j.write_session(file_name="f", version_id=vid, file_size=size,
                        shard_size=shard_size,
                        shard_count=len(plan_shards(size, shard_size)))
        for idx in shards:
            start = idx * shard_size
            end = min(start + shard_size, size)
            j.write_shard(idx, start, end, end - start)
        if ended:
            j.write_end("finished")
        j.close()

    def test_clean_recovery_replays_completed_shards(self):
        vid = "a" * 64
        self.blobs.add(vid)
        self._write_journal("scan-1", vid, size=1000, shard_size=100,
                            shards=[0, 1, 2])
        rec = recover_journals(self.jdir, self.qdir,
                               blob_exists=self.blob_exists)
        self.assertEqual(len(rec), 1)
        self.assertEqual(rec[0]["completed"], [0, 1, 2])
        self.assertEqual(rec[0]["version_id"], vid)
        self.assertIsNone(rec[0]["ended"])

    def test_ended_journal_is_removed_not_replayed(self):
        vid = "b" * 64
        self.blobs.add(vid)
        self._write_journal("scan-done", vid, 500, 100, [0, 1], ended=True)
        rec = recover_journals(self.jdir, self.qdir,
                               blob_exists=self.blob_exists)
        self.assertEqual(rec, [])
        self.assertFalse(
            os.path.exists(os.path.join(self.jdir, "scan-done.log")))

    def test_missing_blob_quarantined(self):
        self._write_journal("scan-orphan", "c" * 64, 500, 100, [0])
        rec = recover_journals(self.jdir, self.qdir,
                               blob_exists=self.blob_exists)
        self.assertEqual(rec, [])
        quarantined = os.listdir(self.qdir)
        self.assertTrue(any("scan-orphan" in q for q in quarantined))

    def test_out_of_plan_shard_quarantined(self):
        vid = "d" * 64
        self.blobs.add(vid)
        self._write_journal("scan-bad", vid, 500, 100, [0])
        # inject a shard index way outside the plan
        with open(os.path.join(self.jdir, "scan-bad.log"), "a") as fh:
            fh.write('{"type":"SHARD","ts":0,"index":999,'
                     '"start":0,"end":1}\n')
        rec = recover_journals(self.jdir, self.qdir,
                               blob_exists=self.blob_exists)
        self.assertEqual(rec, [])
        self.assertTrue(
            any("scan-bad" in q for q in os.listdir(self.qdir)))

    def test_two_versions_never_merge(self):
        # Two journals pinned to two different content hashes recover as two
        # independent descriptors — there is no code path that can join them.
        v1, v2 = "e" * 64, "f" * 64
        self.blobs.update({v1, v2})
        self._write_journal("scan-v1", v1, 500, 100, [0])
        self._write_journal("scan-v2", v2, 800, 100, [0, 1])
        rec = recover_journals(self.jdir, self.qdir,
                               blob_exists=self.blob_exists)
        versions = sorted(r["version_id"] for r in rec)
        self.assertEqual(versions, sorted([v1, v2]))
        for r in rec:
            # every descriptor is internally consistent with its own plan
            plan = plan_shards(r["file_size"], r["shard_size"])
            self.assertEqual(len(plan), r["shard_count"])
            for idx in r["completed"]:
                self.assertLess(idx, r["shard_count"])

    def test_malformed_line_quarantined(self):
        vid = "g" * 64
        self.blobs.add(vid)
        self._write_journal("scan-mal", vid, 300, 100, [0])
        with open(os.path.join(self.jdir, "scan-mal.log"), "a") as fh:
            fh.write("{not valid json\n")
        rec = recover_journals(self.jdir, self.qdir,
                               blob_exists=self.blob_exists)
        self.assertEqual(rec, [])
        self.assertTrue(
            any("scan-mal" in q for q in os.listdir(self.qdir)))


if __name__ == "__main__":
    unittest.main(verbosity=2)
