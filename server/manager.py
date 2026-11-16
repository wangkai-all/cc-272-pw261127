"""ScanManager: session registry, share/split decisions, recovery, watchdog."""
from __future__ import annotations

import collections
import os
import threading
import time
import uuid

from .engine import (
    ACTIVE_SUB_STATES,
    SESSION_FINISHED,
    SESSION_PAUSED,
    SESSION_RUNNING,
    SUB_TIMEOUT,
    TERMINAL_SUB_STATES,
    ScanSession,
    Subscriber,
    run_independent_scan,
)
from .filters import Filter
from .journal import ScanJournal, plan_shards, recover_journals
from .versions import FileRegistry

# Bounded-buffer defaults. Small on purpose: the demo needs a slow consumer to
# reach the backpressure limit quickly and visibly.
DEFAULT_MAX_BUFFER_ITEMS = 200
DEFAULT_MAX_BUFFER_BYTES = 256 * 1024
DEFAULT_OFFER_TIMEOUT = 2.0
MAX_EVENTS = 200


class ManagerError(RuntimeError):
    pass


class ScanManager:
    def __init__(
        self,
        *,
        data_dir: str,
        shard_size: int = 128 * 1024,
        shard_pace_seconds: float = 0.15,
    ):
        self.data_dir = data_dir
        self.journal_dir = os.path.join(data_dir, "journals")
        self.quarantine_dir = os.path.join(data_dir, "quarantine")
        self.registry = FileRegistry(os.path.join(data_dir, "registry"))
        self.shard_size = shard_size
        self.shard_pace_seconds = shard_pace_seconds
        self._lock = threading.RLock()
        self.sessions: dict[str, ScanSession] = {}
        self._sub_index: dict[str, str] = {}          # sub_id -> session_id
        self._detached: dict[str, Subscriber] = {}     # cancelled subs kept for resume
        self._events: collections.deque = collections.deque(maxlen=MAX_EVENTS)
        # Physical-read counters. Catch-up and independent reads are tracked
        # separately so the UI can show why total bytes differ from a full scan.
        self.independent_physical_bytes = 0
        self.independent_scan_count = 0
        self.catchup_physical_bytes = 0
        self._stop = threading.Event()
        self._watchdog: threading.Thread | None = None
        self._recover()
        self._watchdog = threading.Thread(
            target=self._watchdog_loop, name="manager-watchdog", daemon=True
        )
        self._watchdog.start()

    # ------------------------------------------------------------------ events
    def log_event(self, kind: str, message: str, **fields) -> None:
        entry = {"ts": time.time(), "kind": kind, "message": message, **fields}
        with self._lock:
            self._events.append(entry)

    def events(self, limit: int = 100) -> list[dict]:
        with self._lock:
            return list(self._events)[-limit:]

    # ---------------------------------------------------------------- recovery
    def _recover(self) -> None:
        descriptors = recover_journals(
            self.journal_dir,
            self.quarantine_dir,
            blob_exists=lambda vid: os.path.exists(self.registry.blob_path(vid)),
        )
        for desc in descriptors:
            try:
                info = self.registry.resolve(desc["file"], desc["version_id"])
            except KeyError:
                self._quarantine(desc["session_id"], "orphan")
                continue
            if info.size != desc["file_size"] or desc["shard_size"] != self.shard_size:
                # Plan/environment changed: never mix an old plan into a new scan.
                self._quarantine(desc["session_id"], "planmismatch")
                continue
            journal = ScanJournal(self.journal_dir, desc["session_id"])
            journal.open_append()
            session = ScanSession(
                session_id=desc["session_id"],
                file_name=desc["file"],
                version_id=desc["version_id"],
                blob_path=self.registry.blob_path(desc["version_id"]),
                file_size=desc["file_size"],
                shard_size=desc["shard_size"],
                journal=journal,
                recovered_completed=frozenset(desc["completed"]),
                shard_pace_seconds=self.shard_pace_seconds,
            )
            # Restored sessions wait PAUSED; no worker is started until a
            # subscriber attaches, so recovery never burns reads on its own.
            session.state = SESSION_PAUSED
            session.next_shard = max(desc["completed"], default=-1) + 1
            self.sessions[session.session_id] = session
            self.log_event(
                "recover",
                f"recovered {session.session_id} for {desc['file']} "
                f"@ {desc['version_id'][:10]}: {len(desc['completed'])}/"
                f"{desc['shard_count']} shards durable, paused awaiting resume",
                session_id=session.session_id,
                version_id=desc["version_id"],
                completed=desc["completed"],
            )

    def _quarantine(self, session_id: str, reason: str) -> None:
        src = os.path.join(self.journal_dir, f"{session_id}.log")
        if os.path.exists(src):
            os.replace(
                src,
                os.path.join(self.quarantine_dir, f"{reason}-{session_id}.log"),
            )

    # ------------------------------------------------------------- file API
    def register_file(self, name: str, content: bytes) -> dict:
        entry, info, is_new = self.registry.register(name, content)
        self.log_event(
            "version",
            f"file {name} now at v{info.seq} ({info.version_id[:10]})"
            + ("" if is_new else " — identical content, pointer unchanged"),
            file=name,
            version_id=info.version_id,
            seq=info.seq,
            new_version=is_new,
        )
        return entry.to_dict()

    def list_files(self) -> list[dict]:
        return self.registry.list_files()

    # ------------------------------------------------------------ query API
    def query(
        self,
        *,
        file_name: str,
        flt: Filter,
        label: str = "",
        version_id: str | None = None,
        timeout_seconds: float | None = None,
        max_buffer_items: int = DEFAULT_MAX_BUFFER_ITEMS,
        max_buffer_bytes: int = DEFAULT_MAX_BUFFER_BYTES,
        offer_timeout: float = DEFAULT_OFFER_TIMEOUT,
    ) -> tuple[ScanSession, Subscriber]:
        info = self.registry.resolve(file_name, version_id)
        deadline = None if timeout_seconds is None else time.time() + timeout_seconds
        with self._lock:
            session, mode = self._find_or_create_session(
                file_name, info.version_id, info.size
            )
            sub = Subscriber(
                filter_spec=flt,
                shared_mode=mode,
                max_buffer_items=max_buffer_items,
                max_buffer_bytes=max_buffer_bytes,
                offer_timeout=offer_timeout,
                deadline=deadline,
                label=label or f"query-{len(session.subscribers) + 1}",
            )
            # Atomically snapshot the worker's claimed shards, configure the
            # reservations, and publish the subscriber in one lock-held step so
            # the worker cannot fan out a catch-up-owned shard in between.
            def configure_query(claimed):
                catchup = claimed if mode == "shared-late" else set()
                sub.catchup_shards = set(catchup)
                sub.live_skip_shards = set(catchup)

            claimed = session.attach_with_claimed_shards(
                sub, configure=configure_query
            )
            missing = claimed if mode == "shared-late" else set()
            self._sub_index[sub.sub_id] = session.session_id

            # Start (or wake) the scan only once this subscriber is attached.
            # A fresh split session has no worker until now, so its first shard
            # is guaranteed a fan-out target; a paused session is woken/revived.
            if session.state == SESSION_PAUSED:
                session.request_resume()
            if session._worker is None:
                session.start_worker()

            if mode == "shared-head":
                self.log_event(
                    "share",
                    f"{sub.sub_id} attached to live scan {session.session_id} "
                    f"at shard 0 (full share, zero extra reads)",
                    session_id=session.session_id,
                    sub_id=sub.sub_id,
                )
            elif mode == "shared-late":
                self.log_event(
                    "share",
                    f"{sub.sub_id} attached late to {session.session_id}: "
                    f"{len(missing)} shard(s) need private catch-up reads",
                    session_id=session.session_id,
                    sub_id=sub.sub_id,
                    catchup_shards=sorted(missing),
                )
            else:
                self.log_event(
                    "split",
                    f"{sub.sub_id} starts separate scan {session.session_id} ({mode})",
                    session_id=session.session_id,
                    sub_id=sub.sub_id,
                )

            if missing:
                session.catch_up(
                    sub, missing,
                    on_bytes_read=lambda n: self._add_catchup_bytes(n),
                )
            return session, sub

    def _add_catchup_bytes(self, n: int) -> None:
        with self._lock:
            self.catchup_physical_bytes += n

    def _find_or_create_session(
        self, file_name: str, version_id: str, file_size: int
    ) -> tuple[ScanSession, str]:
        """The share/split decision, in one place.

        SHARE only when an in-flight (running/paused) session exists for the
        exact same immutable blob. A query arriving at shard 0 gets a full
        share; later arrivals share future shards and catch up privately.
        Everything else is a split — a finished scan is never reused as a
        result cache.
        """
        candidates = [
            s for s in self.sessions.values()
            if s.file_name == file_name
            and s.version_id == version_id
            and s.state != SESSION_FINISHED
        ]
        if candidates:
            session = max(candidates, key=lambda s: s.next_shard)
            if session.next_shard == 0 and not session.completed_shards:
                return session, "shared-head"
            return session, "shared-late"
        return self._create_session(file_name, version_id, file_size), "split-new"

    def _create_session(
        self, file_name: str, version_id: str, file_size: int
    ) -> ScanSession:
        session_id = f"scan-{uuid.uuid4().hex[:12]}"
        journal = ScanJournal(self.journal_dir, session_id)
        journal.open_append()
        journal.write_session(
            file_name=file_name,
            version_id=version_id,
            file_size=file_size,
            shard_size=self.shard_size,
            shard_count=len(plan_shards(file_size, self.shard_size)),
        )
        session = ScanSession(
            session_id=session_id,
            file_name=file_name,
            version_id=version_id,
            blob_path=self.registry.blob_path(version_id),
            file_size=file_size,
            shard_size=self.shard_size,
            journal=journal,
            shard_pace_seconds=self.shard_pace_seconds,
        )
        self.sessions[session_id] = session
        # Do NOT start the worker yet: with no subscriber attached, the first
        # shard's records would fan out to an empty target set and be silently
        # lost. The worker starts only after the first subscriber is attached
        # (see query()/resume_subscriber()), so record #1 always has a target.
        return session

    # ------------------------------------------------------- cancel / resume
    def cancel_subscriber(self, sub_id: str) -> Subscriber:
        with self._lock:
            session_id = self._sub_index.get(sub_id)
            if session_id is None:
                raise ManagerError(f"unknown subscriber {sub_id}")
            session = self.sessions[session_id]
            sub = session.detach(sub_id)
            if sub is None:
                raise ManagerError(f"subscriber {sub_id} already gone")
            # Keep the terminated subscriber around so resume can reconstruct
            # its filter, buffer policy and delivered-shard watermark.
            self._detached[sub_id] = sub
            self.log_event(
                "cancel",
                f"{sub_id} cancelled; other subscribers of {session_id} keep running",
                session_id=session_id,
                sub_id=sub_id,
            )
            self._maybe_idle_pause(session)
            return sub

    def resume_subscriber(
        self,
        *,
        sub_id: str,
        timeout_seconds: float | None = None,
    ) -> tuple[ScanSession, Subscriber]:
        """Resume a cancelled/terminated subscriber.

        A new subscriber is created and attached to the same session if it is
        still in flight; shards the old subscriber had already received are
        skipped and only missing ones are re-read privately.
        """
        with self._lock:
            old_session_id = self._sub_index.get(sub_id)
            if old_session_id is None:
                raise ManagerError(f"unknown subscriber {sub_id}")
            old_session = self.sessions[old_session_id]
            base_sub = old_session.subscribers.get(sub_id) or self._detached.get(sub_id)
            if base_sub is None:
                raise ManagerError(f"no retained state for subscriber {sub_id}")
            if base_sub.state not in TERMINAL_SUB_STATES:
                raise ManagerError(f"subscriber {sub_id} is still {base_sub.state}")

            info = self.registry.resolve(
                old_session.file_name, old_session.version_id
            )
            if old_session.state == SESSION_FINISHED:
                # The shared scan is over: never serve cached results, split.
                session = self._create_session(
                    old_session.file_name, old_session.version_id, info.size
                )
                mode = "split-completed"
                new_sub = Subscriber(
                    filter_spec=base_sub.filter,
                    shared_mode=mode,
                    max_buffer_items=base_sub.buffer.max_items,
                    max_buffer_bytes=base_sub.buffer.max_bytes,
                    offer_timeout=base_sub.offer_timeout,
                    deadline=(
                        None if timeout_seconds is None
                        else time.time() + timeout_seconds
                    ),
                    label=f"{base_sub.label}-resumed",
                    resumed_from=sub_id,
                )
                # A freshly created session has claimed no shards yet, so every
                # shard is delivered live; there is no private catch-up read.
                claimed = session.attach_with_claimed_shards(new_sub)
                missing = set()
            else:
                session = old_session
                mode = "resume"
                new_sub = Subscriber(
                    filter_spec=base_sub.filter,
                    shared_mode=mode,
                    max_buffer_items=base_sub.buffer.max_items,
                    max_buffer_bytes=base_sub.buffer.max_bytes,
                    offer_timeout=base_sub.offer_timeout,
                    deadline=(
                        None if timeout_seconds is None
                        else time.time() + timeout_seconds
                    ),
                    label=f"{base_sub.label}-resumed",
                    resumed_from=sub_id,
                )

                def configure_resume(claimed):
                    # All claimed shards (including the in-flight one pre-fetched
                    # when the cancel landed and zero-match shards) are re-read
                    # privately; shards from next_shard onward arrive live.
                    new_sub.catchup_shards = set(claimed)
                    new_sub.live_skip_shards = set(claimed)

                claimed = session.attach_with_claimed_shards(
                    new_sub, configure=configure_resume
                )
                missing = set(claimed)
            self._sub_index[new_sub.sub_id] = session.session_id
            if session.state == SESSION_PAUSED:
                session.request_resume()
            # A split-completed session is brand new and has never had a worker;
            # start it only now that the resuming subscriber is attached.
            if session._worker is None:
                session.start_worker()
            self.log_event(
                "resume",
                f"{new_sub.sub_id} resumes {sub_id} on {session.session_id} "
                f"({mode}); {len(missing)} shard(s) to catch up",
                session_id=session.session_id,
                sub_id=new_sub.sub_id,
                resumed_from=sub_id,
                missing=sorted(missing),
            )
            if missing:
                session.catch_up(
                    new_sub, missing,
                    on_bytes_read=lambda n: self._add_catchup_bytes(n),
                )
            else:
                new_sub.end_stream()
            return session, new_sub

    def set_slow(self, sub_id: str, slow: bool) -> None:
        sub = self.get_subscriber(sub_id)
        sub.slow = slow
        self.log_event(
            "slow",
            f"{sub_id} marked {'SLOW' if slow else 'normal'}",
            sub_id=sub_id,
            slow=slow,
        )

    def _session_of(self, sub_id: str) -> ScanSession:
        session_id = self._sub_index.get(sub_id)
        if session_id is None:
            raise ManagerError(f"unknown subscriber {sub_id}")
        return self.sessions[session_id]

    def get_subscriber(self, sub_id: str) -> Subscriber:
        session = self._session_of(sub_id)
        sub = session.subscribers.get(sub_id) or self._detached.get(sub_id)
        if sub is None:
            raise ManagerError(f"unknown subscriber {sub_id}")
        return sub

    # ---------------------------------------------------------- independent
    def independent_query(
        self,
        *,
        file_name: str,
        flt: Filter,
        version_id: str | None = None,
        limit: int = 5000,
    ) -> dict:
        info = self.registry.resolve(file_name, version_id)
        result = run_independent_scan(
            self.registry.blob_path(info.version_id), flt
        )
        with self._lock:
            self.independent_physical_bytes += result["physical_bytes_read"]
            self.independent_scan_count += 1
        records = result["records"][:limit]
        return {
            "file": file_name,
            "version_id": info.version_id,
            "line_count": result["line_count"],
            "physical_bytes_read": result["physical_bytes_read"],
            "matched_records": len(result["records"]),
            "truncated": len(result["records"]) > limit,
            "records": [r.to_dict() for r in records],
        }

    # ------------------------------------------------------------- watchdog
    def _watchdog_loop(self) -> None:
        while not self._stop.wait(timeout=0.5):
            self._watchdog_tick()

    def _watchdog_tick(self) -> None:
        with self._lock:
            now = time.time()
            for session in list(self.sessions.values()):
                for sub in list(session.subscribers.values()):
                    if sub.state != "running":
                        continue
                    if sub.deadline is not None and now >= sub.deadline:
                        if sub.terminate(SUB_TIMEOUT, "subscriber deadline expired"):
                            sub.timeout_count += 1
                            self.log_event(
                                "timeout",
                                f"{sub.sub_id} timed out; scan and peers unaffected",
                                session_id=session.session_id,
                                sub_id=sub.sub_id,
                            )
                self._maybe_idle_pause(session)

    def _maybe_idle_pause(self, session: ScanSession) -> None:
        if session.state != SESSION_RUNNING:
            return
        if not session.active_subscribers():
            session.request_pause()
            self.log_event(
                "pause",
                f"session {session.session_id} paused: no active subscribers "
                f"(progress retained at shard {session.next_shard})",
                session_id=session.session_id,
            )

    # --------------------------------------------------------------- crash
    def corrupt_journal_for_demo(self, session_id: str) -> str:
        """Inject an out-of-plan shard record to prove recovery validation."""
        path = os.path.join(self.journal_dir, f"{session_id}.log")
        if not os.path.exists(path):
            raise ManagerError(f"no journal for session {session_id}")
        with open(path, "a", encoding="utf-8") as fh:
            fh.write('{"type":"SHARD","ts":0,"index":999999,"start":0,"end":1}\n')
            fh.flush()
            os.fsync(fh.fileno())
        self.log_event("corrupt", f"injected out-of-plan shard into {session_id}")
        return path

    # ------------------------------------------------------------- snapshot
    def totals(self) -> dict:
        with self._lock:
            sessions = list(self.sessions.values())
            shared_physical = sum(s.physical_bytes_read for s in sessions)
            return {
                "sessions": len(sessions),
                "running_sessions": sum(
                    1 for s in sessions if s.state == SESSION_RUNNING
                ),
                "paused_sessions": sum(
                    1 for s in sessions if s.state == SESSION_PAUSED
                ),
                "finished_sessions": sum(
                    1 for s in sessions if s.state == SESSION_FINISHED
                ),
                "active_subscribers": sum(
                    1 for s in sessions
                    for x in s.subscribers.values()
                    if x.state in ACTIVE_SUB_STATES
                ),
                "shared_physical_bytes_read": shared_physical,
                "catchup_physical_bytes_read": self.catchup_physical_bytes,
                "independent_physical_bytes_read": self.independent_physical_bytes,
                "independent_scan_count": self.independent_scan_count,
                "total_physical_bytes_read": (
                    shared_physical
                    + self.catchup_physical_bytes
                    + self.independent_physical_bytes
                ),
            }

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "totals": self.totals(),
                "sessions": [s.snapshot() for s in self.sessions.values()],
                "events": self.events(),
            }

    def shutdown(self) -> None:
        self._stop.set()
        if self._watchdog:
            self._watchdog.join(timeout=2)
