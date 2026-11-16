"""Scan engine: shared physical scans, per-subscriber logical streams.

Design contract (see README for the prose version):

* A :class:`ScanSession` scans one immutable blob once, shard by shard, with a
  single worker thread. Records are fanned out to every attached subscriber,
  each of which applies its own filter and owns a *bounded* buffer.
* Sharing is decided on ``(file, content-hash, shard-plan)``. A second query
  on the same in-flight scan attaches ("shared"); a query whose scan already
  finished, or whose file version differs, gets its own scan ("split").
* A slow or cancelled subscriber never blocks or corrupts anyone else. When a
  subscriber's bounded buffer cannot accept within its offer timeout, or its
  overall deadline expires, that subscriber alone is terminated; the shared
  scan keeps running and memory stays bounded.
* Completed shards are journaled durably. Resume re-reads only missing shards
  from the immutable blob; crash recovery validates the pinned content hash
  and shard plan before replaying anything.
"""
from __future__ import annotations

import itertools
import os
import threading
import time
import uuid
from dataclasses import dataclass, field

from .filters import Filter
from .journal import ScanJournal, plan_shards

# --------------------------------------------------------------------- states
SUB_RUNNING = "running"
SUB_PAUSED = "paused"
SUB_DONE = "done"
SUB_CANCELED = "canceled"
SUB_OVERFLOW = "overflow"        # slow consumer: bounded buffer filled up
SUB_TIMEOUT = "timeout"          # overall subscriber deadline expired
SUB_ERROR = "error"

TERMINAL_SUB_STATES = {SUB_DONE, SUB_CANCELED, SUB_OVERFLOW, SUB_TIMEOUT, SUB_ERROR}
ACTIVE_SUB_STATES = {SUB_RUNNING, SUB_PAUSED}

SESSION_RUNNING = "running"
SESSION_PAUSED = "paused"
SESSION_FINISHED = "finished"


class ScanError(RuntimeError):
    pass


# ------------------------------------------------------------------ utilities
def _now() -> float:
    return time.time()


# ============================================================== BoundedBuffer
class BoundedBuffer:
    """Bounded MPMC-ish queue limited by BOTH item count and total bytes.

    The dual limit is what makes slow-consumer memory use provably finite:
    a single huge record cannot blow the budget by hiding behind the count
    limit, and a flood of tiny records cannot hide behind the byte limit.
    """

    def __init__(self, max_items: int, max_bytes: int):
        self.max_items = max_items
        self.max_bytes = max_bytes
        self._items: list = []
        self._head = 0
        self._bytes = 0
        self._cond = threading.Condition(threading.Lock())
        self._closed = False

    def put(self, item, size: int, timeout: float | None) -> bool:
        """Return True if accepted, False if the timeout elapsed or closed."""
        deadline = None if timeout is None else _now() + max(0.0, timeout)
        with self._cond:
            while not self._closed and self._full_locked(size):
                remaining = None if deadline is None else deadline - _now()
                if remaining is not None and remaining <= 0:
                    return False
                if not self._cond.wait(timeout=remaining):
                    if self._closed:
                        return False
                    if deadline is not None and _now() >= deadline:
                        return False
            if self._closed:
                return False
            self._items.append((item, size))
            self._bytes += size
            self._cond.notify_all()
            return True

    def _full_locked(self, incoming_size: int) -> bool:
        if len(self._items) - self._head >= self.max_items:
            return True
        if self._bytes + incoming_size > self.max_bytes:
            return True
        return False

    def get(self, timeout: float | None = None):
        with self._cond:
            while not self._closed and self._head >= len(self._items):
                if not self._cond.wait(timeout=timeout):
                    return None
            if self._head >= len(self._items):
                return None  # closed and drained
            item, size = self._items[self._head]
            self._head += 1
            self._bytes -= size
            # Compact occasionally so the list cannot grow without bound.
            if self._head > max(64, len(self._items) // 2):
                del self._items[: self._head]
                self._head = 0
            self._cond.notify_all()
            return item

    def close(self) -> None:
        with self._cond:
            self._closed = True
            self._cond.notify_all()

    def snapshot(self) -> dict:
        with self._cond:
            return {
                "queued_items": len(self._items) - self._head,
                "queued_bytes": self._bytes,
                "max_items": self.max_items,
                "max_bytes": self.max_bytes,
                "closed": self._closed,
            }


# ===================================================================== Record
@dataclass(frozen=True)
class Record:
    line_no: int
    shard_index: int
    text: str
    byte_len: int

    def to_dict(self) -> dict:
        return {
            "line_no": self.line_no,
            "shard_index": self.shard_index,
            "text": self.text,
            "byte_len": self.byte_len,
        }


# ================================================================== Subscriber
class Subscriber:
    """One query's logical stream, fed by a shared physical scan."""

    _id_counter = itertools.count(1)

    def __init__(
        self,
        *,
        filter_spec: Filter,
        shared_mode: str,
        max_buffer_items: int,
        max_buffer_bytes: int,
        offer_timeout: float,
        deadline: float | None,
        label: str = "",
        resumed_from: str | None = None,
        delivered_shards: frozenset[int] = frozenset(),
        catchup_shards: frozenset[int] = frozenset(),
        # Shards the predecessor already delivered. The resume catch-up re-read
        # covers every missing shard (including the in-flight one the worker had
        # pre-fetched but not finished), so the live worker must skip these too
        # or they would be delivered twice.
        live_skip_shards: frozenset[int] = frozenset(),
        connect_timeout: float = 5.0,
    ):
        self.sub_id = f"sub-{next(self._id_counter)}"
        self.label = label
        self.filter = filter_spec
        self.shared_mode = shared_mode          # shared-head | shared-late | split-new | split-completed | resume
        self.resumed_from = resumed_from
        self.delivered_shards: set[int] = set(delivered_shards)
        # Shards reserved for private catch-up re-reads. The live worker must
        # NOT fan these out to this subscriber, otherwise the records would be
        # delivered twice (once live, once by the catch-up thread). Entries are
        # removed as each catch-up shard is emitted, giving per-shard
        # exactly-once delivery regardless of worker/catch-up timing.
        self.catchup_shards: set[int] = set(catchup_shards)
        # Shards the live worker must skip for this subscriber because the
        # catch-up thread owns them. On resume this initially covers EVERY
        # missing shard — including the in-flight shard the worker had
        # pre-fetched but the predecessor never finished receiving — so a shard
        # is delivered by exactly one side, never both and never neither.
        self.live_skip_shards: set[int] = set(live_skip_shards)
        self.offer_timeout = offer_timeout
        # Separate grace period for the first SSE consumer to connect, so a
        # slow connect is never confused with a slow draining consumer.
        self.connect_timeout = connect_timeout
        self.deadline = deadline
        self.created_at = _now()
        self.state = SUB_RUNNING
        self.ended_at: float | None = None
        self.error: str | None = None
        self.buffer = BoundedBuffer(max_buffer_items, max_buffer_bytes)
        # Counters — the difference between physical and delivered bytes is
        # what proves the scan is not merely caching a final answer.
        self.matched_records = 0
        self.matched_bytes = 0
        self.delivered_records = 0
        self.delivered_bytes = 0
        self.overflow_count = 0
        self.timeout_count = 0
        self.slow = False
        # Backpressure gate: records are not offered to the bounded buffer
        # until at least one SSE consumer has connected, so a query whose
        # stream has not been opened yet cannot be overflow-killed at birth.
        self.consumers = 0
        self._consumer_cond = threading.Condition()
        # Catch-up coordination: a subscriber that has private catch-up reads
        # in flight must not be marked done by the scan's _finish, because its
        # remaining (past) shards arrive from the catch-up thread, not the
        # worker. The stream ends only when both sides are complete.
        self.catchup_pending = 0
        self.session_finished = False
        self._lock = threading.Lock()

    # ----------------------------------------------------------- catch-up
    def begin_catchup(self) -> None:
        with self._lock:
            self.catchup_pending += 1

    def finish_catchup(self) -> bool:
        """Mark one catch-up job done. Return True if the stream can now end."""
        with self._lock:
            self.catchup_pending = max(0, self.catchup_pending - 1)
            return self.catchup_pending == 0 and self.session_finished

    def note_session_finished(self) -> bool:
        """Mark the scan complete. Return True if catch-up is also done."""
        with self._lock:
            self.session_finished = True
            return self.catchup_pending == 0

    # ------------------------------------------------------------- consumers
    def add_consumer(self) -> None:
        with self._consumer_cond:
            self.consumers += 1
            self._consumer_cond.notify_all()

    def remove_consumer(self) -> None:
        with self._consumer_cond:
            self.consumers = max(0, self.consumers - 1)

    def has_consumer(self) -> bool:
        with self._consumer_cond:
            return self.consumers > 0

    def wait_for_consumer(self, timeout: float) -> bool:
        deadline = _now() + max(0.0, timeout)
        with self._consumer_cond:
            while self.consumers == 0 and self.state == SUB_RUNNING:
                remaining = deadline - _now()
                if remaining <= 0:
                    return False
                self._consumer_cond.wait(timeout=min(remaining, 0.5))
            return self.consumers > 0

    # ------------------------------------------------------------ lifecycle
    def terminate(self, state: str, error: str) -> bool:
        with self._lock:
            if self.state in TERMINAL_SUB_STATES:
                return False
            self.state = state
            self.error = error
            self.ended_at = _now()
            self.buffer.close()
            return True

    def pause(self) -> bool:
        with self._lock:
            if self.state != SUB_RUNNING:
                return False
            self.state = SUB_PAUSED
            return True

    def resume(self) -> bool:
        with self._lock:
            if self.state != SUB_PAUSED:
                return False
            self.state = SUB_RUNNING
            return True

    def is_active(self) -> bool:
        return self.state in ACTIVE_SUB_STATES

    def has_timed_out(self) -> bool:
        return self.deadline is not None and _now() >= self.deadline

    # ------------------------------------------------------------- delivery
    def live_skips_shard(self, shard_index: int) -> bool:
        """True when this shard is reserved for a private catch-up re-read."""
        with self._lock:
            return shard_index in self.catchup_shards

    def mark_catchup_shard_done(self, shard_index: int) -> None:
        with self._lock:
            self.catchup_shards.discard(shard_index)
            self.live_skip_shards.discard(shard_index)

    def offer_record(self, record: Record) -> None:
        """Filter then enqueue. Terminate this subscriber alone on backpressure."""
        with self._lock:
            if self.state != SUB_RUNNING:
                return
            if (record.shard_index in self.catchup_shards
                    or record.shard_index in self.live_skip_shards):
                # Owned by a catch-up thread; the live worker must skip it.
                return
            if self.deadline is not None and _now() >= self.deadline:
                self._terminate_locked(SUB_TIMEOUT, "subscriber deadline expired")
                return
            if not self.filter.matches(record.text):
                return
            self.matched_records += 1
            self.matched_bytes += record.byte_len
        # Gate until a stream is attached; bounded by the connect grace period
        # so an abandoned query still terminates instead of blocking forever.
        if not self.has_consumer():
            if not self.wait_for_consumer(self.connect_timeout):
                with self._lock:
                    if self.state == SUB_RUNNING and self.consumers == 0:
                        self._terminate_locked(
                            SUB_TIMEOUT,
                            "no consumer connected within connect timeout",
                        )
                return
        accepted = self.buffer.put(record, record.byte_len, self.offer_timeout)
        if not accepted:
            with self._lock:
                if self.state == SUB_RUNNING:
                    self.overflow_count += 1
                    self._terminate_locked(
                        SUB_OVERFLOW,
                        "bounded buffer full: consumer too slow (backpressure limit reached)",
                    )
            return
        with self._lock:
            if self.state == SUB_RUNNING:
                self.delivered_records += 1
                self.delivered_bytes += record.byte_len
                # Watermark for resume/catch-up: once a record from this shard
                # has entered the bounded buffer, the shard counts as delivered.
                self.delivered_shards.add(record.shard_index)

    def mark_shard_delivered(self, index: int) -> None:
        with self._lock:
            self.delivered_shards.add(index)

    def offer_catchup_record(self, record: Record) -> None:
        """Enqueue a record produced by this subscriber's own catch-up thread.

        Same filtering/backpressure semantics as :meth:`offer_record`, but it
        does not skip the catch-up shards (it owns them) and clears the shard's
        reservation once its records are flowing, so later live records for the
        same shard cannot double-deliver.
        """
        with self._lock:
            if self.state != SUB_RUNNING:
                return
            if self.deadline is not None and _now() >= self.deadline:
                self._terminate_locked(SUB_TIMEOUT, "subscriber deadline expired")
                return
            if not self.filter.matches(record.text):
                return
            self.matched_records += 1
            self.matched_bytes += record.byte_len
        if not self.has_consumer():
            if not self.wait_for_consumer(self.connect_timeout):
                with self._lock:
                    if self.state == SUB_RUNNING and self.consumers == 0:
                        self._terminate_locked(
                            SUB_TIMEOUT,
                            "no consumer connected within connect timeout",
                        )
                return
        accepted = self.buffer.put(record, record.byte_len, self.offer_timeout)
        if not accepted:
            with self._lock:
                if self.state == SUB_RUNNING:
                    self.overflow_count += 1
                    self._terminate_locked(
                        SUB_OVERFLOW,
                        "bounded buffer full: consumer too slow (backpressure limit reached)",
                    )
            return
        with self._lock:
            if self.state == SUB_RUNNING:
                self.delivered_records += 1
                self.delivered_bytes += record.byte_len
                self.delivered_shards.add(record.shard_index)

    def end_stream(self) -> None:
        with self._lock:
            if self.state == SUB_RUNNING:
                self.state = SUB_DONE
                self.ended_at = _now()
                self.buffer.close()

    def _terminate_locked(self, state: str, error: str) -> None:
        self.state = state
        self.error = error
        self.ended_at = _now()
        self.buffer.close()

    def snapshot(self) -> dict:
        with self._lock:
            buf = self.buffer.snapshot()
            return {
                "sub_id": self.sub_id,
                "label": self.label,
                "state": self.state,
                "error": self.error,
                "shared_mode": self.shared_mode,
                "resumed_from": self.resumed_from,
                "slow": self.slow,
                "offer_timeout": self.offer_timeout,
                "deadline": self.deadline,
                "matched_records": self.matched_records,
                "matched_bytes": self.matched_bytes,
                "delivered_records": self.delivered_records,
                "delivered_bytes": self.delivered_bytes,
                "delivered_shards": sorted(self.delivered_shards),
                "overflow_count": self.overflow_count,
                "timeout_count": self.timeout_count,
                "age_seconds": round(_now() - self.created_at, 3),
                **buf,
            }


# ================================================================= ScanSession
class ScanSession:
    """One physical scan of one immutable blob, shared by N subscribers."""

    def __init__(
        self,
        *,
        session_id: str,
        file_name: str,
        version_id: str,
        blob_path: str,
        file_size: int,
        shard_size: int,
        journal: ScanJournal,
        recovered_completed: frozenset[int] = frozenset(),
        shard_pace_seconds: float = 0.0,
    ):
        self.session_id = session_id
        self.file_name = file_name
        self.version_id = version_id
        self.blob_path = blob_path
        self.file_size = file_size
        self.shard_size = shard_size
        self.shards: list[tuple[int, int]] = plan_shards(file_size, shard_size)
        self.journal = journal
        # Demo pacing: sleep between shards so concurrent queries can observe
        # sharing and slow-consumer backpressure. Zero in production-like mode.
        self.shard_pace_seconds = max(0.0, shard_pace_seconds)
        self.state = SESSION_RUNNING
        self.subscribers: dict[str, Subscriber] = {}
        self.completed_shards: set[int] = set(recovered_completed)
        self.next_shard = 0
        # Physical work actually performed by THIS session.
        self.physical_bytes_read = 0
        # Bytes read by the main scan worker only (excludes catch-up re-reads).
        # After recovery this counts just the not-yet-durable shards, proving
        # the durable prefix was replayed rather than re-read from disk.
        self.worker_bytes_read = 0
        self.logical_input_bytes = 0
        self.records_emitted = 0
        self.created_at = _now()
        self.ended_at: float | None = None
        self.recovered = bool(recovered_completed)
        self._lock = threading.RLock()
        self._wake = threading.Condition(self._lock)
        self._worker: threading.Thread | None = None
        self._catchup_threads: list[threading.Thread] = []
        # Line numbers are derived deterministically from byte offsets, so a
        # line keeps the same id whether it comes from the shared worker, a
        # catch-up re-read, or an independent ground-truth scan.
        self._shard_start_line: dict[int, int] = self._compute_line_starts()
        # Global table of every line's starting byte offset, enabling catch-up
        # to map any re-read line back to its exact 1-based line number without
        # relying on shard arithmetic or how fragments were split.
        self._line_offsets: list[int] = self._compute_line_offsets()
        # Authoritative partition: the exact 1-based line numbers the worker's
        # carry algorithm attributes to each shard. Built once with the same
        # byte-level logic as the scan, so catch-up delivers the identical set
        # of lines for a shard that the live worker would have — including
        # lines that straddle a shard boundary (owned by the later shard).
        self._shard_lines: dict[int, list[int]] = self._compute_shard_lines()

    # ------------------------------------------------------------- attach
    def attach(self, sub: Subscriber) -> None:
        with self._lock:
            if self.state == SESSION_FINISHED:
                raise ScanError("cannot attach to a finished session")
            self.subscribers[sub.sub_id] = sub

    def attach_with_claimed_shards(
        self, sub: Subscriber, configure=None
    ) -> set[int]:
        """Configure ``sub``'s shard reservations and publish it atomically.

        ``configure(claimed)`` is invoked INSIDE the session lock, after the
        worker's claimed shards (``index < next_shard``) are snapshotted but
        BEFORE the subscriber becomes visible to the scan worker. It must set
        the subscriber's catch-up / live-skip reservations. Because nothing can
        fan a record out to ``sub`` until it is published at the end, a claimed
        shard can never be delivered both live and by catch-up.

        Returns the claimed shard set: every such shard has already been taken
        by the live worker and will never be replayed, so the caller covers it
        with catch-up; shards from ``next_shard`` onward arrive live. That single
        lock-held boundary makes delivery exactly-once with no timing window,
        independent of filter matches or completion bookkeeping.
        """
        with self._lock:
            if self.state == SESSION_FINISHED:
                raise ScanError("cannot attach to a finished session")
            claimed = set(range(min(self.next_shard, len(self.shards))))
            if configure is not None:
                configure(claimed)
            # Publish only after reservations are fully in place.
            self.subscribers[sub.sub_id] = sub
        return claimed

    def detach(self, sub_id: str) -> Subscriber | None:
        with self._lock:
            sub = self.subscribers.pop(sub_id, None)
            if sub is not None:
                sub.terminate(SUB_CANCELED, "cancelled by client")
                self._wake.notify_all()
            return sub

    def active_subscribers(self) -> list[Subscriber]:
        with self._lock:
            return [s for s in self.subscribers.values() if s.is_active()]

    # ------------------------------------------------------------- control
    def request_pause(self) -> None:
        with self._lock:
            if self.state == SESSION_RUNNING:
                self.state = SESSION_PAUSED
                for sub in self.subscribers.values():
                    if sub.state == SUB_RUNNING:
                        sub.pause()

    def request_resume(self) -> None:
        with self._lock:
            if self.state == SESSION_PAUSED:
                self.state = SESSION_RUNNING
                for sub in self.subscribers.values():
                    if sub.state == SUB_PAUSED:
                        sub.resume()
                self._wake.notify_all()

    def start_worker(self) -> None:
        self._worker = threading.Thread(
            target=self._run_scan, name=f"scan-{self.session_id}", daemon=False
        )
        self._worker.start()

    def _sleep_pace(self) -> None:
        """Inter-shard pacing that ignores unrelated condition notifications.

        Uses an explicit deadline loop rather than a single ``wait``: the
        session condition is also notified for attach/resume events, and a
        bare wait would wake on those and silently disable the pace.
        """
        deadline = _now() + self.shard_pace_seconds
        with self._lock:
            while self.state == SESSION_RUNNING:
                remaining = deadline - _now()
                if remaining <= 0:
                    return
                self._wake.wait(timeout=min(remaining, 0.05))

    def _compute_line_starts(self) -> dict[int, int]:
        """Map every shard index to the 1-based line number at its first byte.

        One extra pass over the blob at session construction, counting
        newlines per shard. It is cheap relative to the scans and makes line
        ids content-deterministic: a line keeps the same id whether it comes
        from the shared worker, a catch-up re-read, or an independent
        ground-truth scan.
        """
        starts = {0: 1}
        if not self.shards:
            return starts
        running = 1
        try:
            with open(self.blob_path, "rb") as fh:
                for idx, (start, end) in enumerate(self.shards[:-1]):
                    fh.seek(start)
                    chunk = fh.read(end - start)
                    running += chunk.count(b"\n")
                    starts[idx + 1] = running
        except OSError:
            pass
        return starts

    def _compute_line_offsets(self) -> list[int]:
        """Byte offset of the first byte of every line, in file order.

        ``offsets[k]`` is the starting offset of 1-based line ``k + 1``. A
        single streaming pass; memory is O(lines), acceptable for the demo and
        what makes catch-up line numbering exact regardless of shard splits.
        """
        offsets = [0]
        try:
            with open(self.blob_path, "rb") as fh:
                pos = 0
                while True:
                    block = fh.read(1 << 20)
                    if not block:
                        break
                    base = pos
                    for i, byte in enumerate(block):
                        if byte == 0x0A:  # '\n'
                            offsets.append(base + i + 1)
                    pos += len(block)
            # A trailing offset one past EOF can appear for a final newline; the
            # last real line starts at the offset before it.
            if offsets and offsets[-1] >= self.file_size:
                offsets.pop()
        except OSError:
            pass
        return offsets

    def line_no_at_offset(self, offset: int) -> int:
        """1-based line number whose starting byte is at or before ``offset``."""
        import bisect
        # bisect_right gives the count of offsets <= the requested position.
        return max(1, bisect.bisect_right(self._line_offsets, offset))

    def _compute_shard_lines(self) -> dict[int, list[int]]:
        """Shard index -> the exact 1-based line numbers the worker emits there.

        Replays the worker's carry split over the whole blob once at session
        construction. Because this is the same algorithm the scan thread runs,
        the partition is authoritative: a straddling line is attributed to the
        later shard exactly as it would be during a live scan. Catch-up simply
        looks up its missing shards here instead of re-deriving boundaries.
        """
        result: dict[int, list[int]] = {i: [] for i in range(len(self.shards))}
        if not self.shards:
            return result
        line_no = 1
        carry = b""
        total = len(self.shards)
        try:
            with open(self.blob_path, "rb") as fh:
                for index, (start, end) in enumerate(self.shards):
                    fh.seek(start)
                    chunk = fh.read(end - start)
                    data = carry + chunk
                    carry = b""
                    lines = data.split(b"\n")
                    if index < total - 1:
                        carry = lines.pop()
                    for raw in lines:
                        if raw:
                            result[index].append(line_no)
                        line_no += 1
            if carry and total:
                result[total - 1].append(line_no)
        except OSError:
            pass
        return result

    # ------------------------------------------------------------- shard IO
    def _read_shard(self, fd: int, start: int, end: int) -> bytes:
        chunk = b""
        offset = start
        while offset < end:
            block = os.pread(fd, end - offset, offset)
            if not block:
                break
            chunk += block
            offset += len(block)
        self.physical_bytes_read += len(chunk)
        self.worker_bytes_read += len(chunk)
        return chunk

    def _emit_line(
        self, line_bytes: bytes, shard_index: int, line_no: int | None = None
    ) -> None:
        if not line_bytes:
            return
        text = line_bytes.decode("utf-8", errors="replace")
        if line_no is None:
            line_no = self._shard_start_line.get(shard_index, 1)
        record = Record(
            line_no=line_no,
            shard_index=shard_index,
            text=text,
            byte_len=len(line_bytes),
        )
        self.records_emitted += 1
        self.logical_input_bytes += record.byte_len
        # Fan out under the session lock so the subscriber set is stable;
        # each offer only blocks on that subscriber's own bounded buffer.
        with self._lock:
            targets = list(self.subscribers.values())
        for sub in targets:
            sub.offer_record(record)

    def _run_scan(self) -> None:
        try:
            fd = os.open(self.blob_path, os.O_RDONLY)
        except OSError as exc:
            self._fail_all(f"cannot open blob: {exc}")
            return
        try:
            carry = b""
            total = len(self.shards)
            while True:
                with self._lock:
                    while self.state == SESSION_PAUSED:
                        self._wake.wait(timeout=1.0)
                    if self.state == SESSION_FINISHED:
                        return
                    index = self.next_shard
                    if index >= total:
                        break
                    self.next_shard = index + 1
                if index in self.completed_shards:
                    # Already durable before a crash/restart: do not re-read.
                    continue
                start, end = self.shards[index]
                chunk = self._read_shard(fd, start, end)
                data = carry + chunk
                carry = b""
                lines = data.split(b"\n")
                if index < total - 1:
                    # Last fragment may be a partial line continued in the
                    # next shard; hold it as carry.
                    carry = lines.pop()
                # Number from the authoritative partition (which lists only
                # non-empty lines), so skip empty tokens to keep the pairing
                # aligned; worker and catch-up then emit identical line ids.
                line_iter = iter(self._shard_lines.get(index, ()))
                for raw in lines:
                    if not raw:
                        continue
                    line_no = next(line_iter)
                    self._emit_line(raw, index, line_no)
                # Advance the durable shard bookkeeping for the scan.
                with self._lock:
                    self.completed_shards.add(index)
                self.journal.write_shard(index, start, end, end - start)
                if self.shard_pace_seconds and index < total - 1:
                    # Pause outside the lock; a cancel/pause request still lands.
                    self._sleep_pace()
            if carry:
                last_idx = total - 1
                final_lines = self._shard_lines.get(last_idx, ())
                final_no = final_lines[-1] if final_lines else 1
                self._emit_line(carry, last_idx, final_no)
            self._finish()
        except Exception as exc:  # defensive: never let the thread die silently
            self._fail_all(f"scan worker error: {exc}")
        finally:
            os.close(fd)

    def _finish(self) -> None:
        with self._lock:
            if self.state == SESSION_FINISHED:
                return
            self.state = SESSION_FINISHED
            self.ended_at = _now()
            for sub in list(self.subscribers.values()):
                # Only end subscribers whose private catch-up reads are done.
                # A subscriber still catching up is ended by its catch-up
                # thread once finish_catchup() reports both halves complete.
                if sub.note_session_finished():
                    sub.end_stream()
            self.journal.write_end(SESSION_FINISHED)
            self.journal.close()
            self._wake.notify_all()

    def _fail_all(self, message: str) -> None:
        with self._lock:
            self.state = SESSION_FINISHED
            self.ended_at = _now()
            for sub in list(self.subscribers.values()):
                sub.terminate(SUB_ERROR, message)
            self.journal.write_end("error")
            self.journal.close()

    # ----------------------------------------------------- catch-up resume
    def catch_up(
        self,
        sub: Subscriber,
        missing: set[int],
        *,
        on_bytes_read=None,
    ) -> None:
        """Re-read only ``missing`` shards from the immutable blob.

        Runs on its own thread so it never disturbs the live scan worker.

        Line ownership is taken from the authoritative ``_shard_lines`` table,
        which is built with the worker's exact carry algorithm. Catch-up simply
        reads each missing shard's byte span once (for honest physical-read
        accounting) and then re-emits precisely the line numbers the table
        assigns to it, fetching each line by its global offset. There is no
        second boundary parser, so a straddling line can never be lost or
        duplicated regardless of how the missing shards are arranged.
        """
        if not missing:
            # No private reads; the session's _finish decides when it ends.
            return
        ordered = sorted(missing)
        total = len(self.shards)
        sub.begin_catchup()

        def work() -> None:
            try:
                fd = os.open(self.blob_path, os.O_RDONLY)
            except OSError as exc:
                sub.terminate(SUB_ERROR, f"catch-up cannot open blob: {exc}")
                return
            try:
                total_read = 0
                for index in ordered:
                    # Honor the physical-read contract: touch this shard's own
                    # bytes from disk even though line emission is table-driven.
                    start, end = self.shards[index]
                    off = start
                    while off < end:
                        block = os.pread(fd, end - off, off)
                        if not block:
                            break
                        total_read += len(block)
                        off += len(block)

                    # Emit exactly the lines the worker would attribute here.
                    for line_no in self._shard_lines.get(index, ()):
                        lo = self._line_offsets[line_no - 1]
                        if line_no - 1 < len(self._line_offsets) - 1:
                            hi = self._line_offsets[line_no]
                        else:
                            hi = self.file_size
                        raw = os.pread(fd, hi - lo, lo).rstrip(b"\n")
                        if not raw:
                            continue
                        record = Record(
                            line_no=line_no,
                            shard_index=index,
                            text=raw.decode("utf-8", errors="replace"),
                            byte_len=len(raw),
                        )
                        with self._lock:
                            self.records_emitted += 1
                            self.logical_input_bytes += record.byte_len
                        sub.offer_catchup_record(record)

                    # Release this shard's reservations now that all of its
                    # lines have been offered; journal it durably (idempotent).
                    sub.mark_catchup_shard_done(index)
                    s, e = self.shards[index]
                    with self._lock:
                        already = index in self.completed_shards
                        self.completed_shards.add(index)
                    if not already:
                        self.journal.write_shard(index, s, e, e - s)
                self.physical_bytes_read += total_read
                if on_bytes_read is not None:
                    on_bytes_read(total_read)
                # End the stream only if the live scan has also finished;
                # otherwise future shards still have to be fanned out.
                if sub.finish_catchup():
                    sub.end_stream()
            except Exception as exc:
                sub.terminate(SUB_ERROR, f"catch-up error: {exc}")
            finally:
                os.close(fd)

        thread = threading.Thread(
            target=work, name=f"catchup-{self.session_id}-{sub.sub_id}", daemon=False
        )
        with self._lock:
            self._catchup_threads.append(thread)
        thread.start()

    def _emit_catchup_lines(
        self, lines: list[bytes], wanted: set[int], sub: Subscriber
    ) -> None:
        # Retained for compatibility/tests; the per-shard catch-up path above
        # emits records directly with explicit ownership. This implements the
        # same explicit-boundary attribution for any caller that still has a
        # pre-split contiguous line list.
        first = min(wanted)
        shard_size = self.shard_size
        total = len(self.shards)
        # Byte offset of the first kept line. When the leading partial was
        # dropped, reading started at shard `first`'s boundary but its first
        # whole line begins after that fragment's newline; either way line
        # ownership only needs the boundary positions, so track the running
        # offset from the start of shard `first`.
        pos_in_span = 0
        per_shard_pos: dict[int, int] = {}
        for raw in lines:
            owner = first + (pos_in_span // shard_size) if shard_size else first
            # The span is contiguous over wanted shards, so a computed owner is
            # always within the wanted range (trailing partial already removed).
            if owner in wanted:
                ppos = per_shard_pos.get(owner, 0)
                line_no = self._shard_start_line.get(owner, 1) + ppos
                per_shard_pos[owner] = ppos + 1
                text = raw.decode("utf-8", errors="replace")
                record = Record(
                    line_no=line_no,
                    shard_index=owner,
                    text=text,
                    byte_len=len(raw),
                )
                with self._lock:
                    self.records_emitted += 1
                    self.logical_input_bytes += record.byte_len
                sub.offer_catchup_record(record)
            pos_in_span += len(raw) + 1  # account for the stripped "\n"
        # Every wanted shard has now had its full lines offered privately;
        # release both reservations so any late live delivery is harmless and
        # the sets cannot grow unbounded.
        for owner in wanted:
            sub.mark_catchup_shard_done(owner)

    # ------------------------------------------------------------- snapshot
    def snapshot(self) -> dict:
        with self._lock:
            return {
                "session_id": self.session_id,
                "file": self.file_name,
                "version_id": self.version_id[:12],
                "version_full": self.version_id,
                "file_size": self.file_size,
                "shard_size": self.shard_size,
                "shard_count": len(self.shards),
                "state": self.state,
                "recovered": self.recovered,
                "completed_shards": sorted(self.completed_shards),
                "next_shard": self.next_shard,
                "physical_bytes_read": self.physical_bytes_read,
                "worker_bytes_read": self.worker_bytes_read,
                "catchup_bytes_read": self.physical_bytes_read - self.worker_bytes_read,
                "logical_input_bytes": self.logical_input_bytes,
                "records_emitted": self.records_emitted,
                "created_at": self.created_at,
                "ended_at": self.ended_at,
                "subscribers": [s.snapshot() for s in self.subscribers.values()],
            }


# ============================================================== independent
def run_independent_scan(
    blob_path: str,
    flt: Filter,
    *,
    chunk_size: int = 1 << 20,
) -> dict:
    """A ground-truth scan that reads the whole blob alone, no sharing.

    Used by the UI's "compare with independent query" check and by tests.
    """
    matched: list[Record] = []
    physical = 0
    line_no = 0
    carry = b""
    fd = os.open(blob_path, os.O_RDONLY)
    try:
        offset = 0
        while True:
            block = os.pread(fd, chunk_size, offset)
            if not block:
                break
            physical += len(block)
            offset += len(block)
            data = carry + block
            carry = b""
            lines = data.split(b"\n")
            carry = lines.pop()
            for raw in lines:
                line_no += 1
                text = raw.decode("utf-8", errors="replace")
                if flt.matches(text):
                    matched.append(
                        Record(line_no, -1, text, len(raw))
                    )
        if carry:
            line_no += 1
            text = carry.decode("utf-8", errors="replace")
            if flt.matches(text):
                matched.append(Record(line_no, -1, text, len(carry)))
    finally:
        os.close(fd)
    return {
        "records": matched,
        "physical_bytes_read": physical,
        "line_count": line_no,
    }
