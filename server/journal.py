"""Shard-level write-ahead journal for crash recovery.

A scan appends one durable record per shard it completes. If the process dies,
restart rebuilds each session's completed-shard set from its journal and only
ever resumes *the same (file, content-hash, shard-plan)* scan.

Version mixing is prevented by construction:

* ``SESSION`` pins the immutable blob id (== sha256 of the content), the file
  name and the exact shard plan.
* On recovery every shard record is checked against that pinned plan; any
  journal whose blob is gone, whose plan changed, or whose records do not
  belong to the plan is quarantined instead of replayed.
* Two different versions have different blob ids and different session keys,
  so their journals can never be merged.
"""
from __future__ import annotations

import json
import os
import threading
import time


class JournalError(RuntimeError):
    pass


class ScanJournal:
    def __init__(self, journal_dir: str, session_id: str):
        os.makedirs(journal_dir, exist_ok=True)
        self.path = os.path.join(journal_dir, f"{session_id}.log")
        self.session_id = session_id
        self._lock = threading.Lock()
        self._fh: object | None = None

    def open_append(self) -> None:
        # line buffering + explicit flush/fsync; "a" never overwrites history
        self._fh = open(self.path, "a", encoding="utf-8", buffering=1)

    def _write(self, record: dict) -> None:
        if self._fh is None:
            raise JournalError("journal is not open")
        line = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
        with self._lock:
            self._fh.write(line + "\n")
            self._fh.flush()
            os.fsync(self._fh.fileno())

    def write_session(
        self,
        *,
        file_name: str,
        version_id: str,
        file_size: int,
        shard_size: int,
        shard_count: int,
    ) -> None:
        self._write(
            {
                "type": "SESSION",
                "ts": time.time(),
                "session_id": self.session_id,
                "file": file_name,
                "version_id": version_id,
                "file_size": file_size,
                "shard_size": shard_size,
                "shard_count": shard_count,
            }
        )

    def write_shard(self, index: int, start: int, end: int, physical_bytes: int) -> None:
        self._write(
            {
                "type": "SHARD",
                "ts": time.time(),
                "index": index,
                "start": start,
                "end": end,
                "physical_bytes": physical_bytes,
            }
        )

    def write_end(self, state: str) -> None:
        self._write({"type": "END", "ts": time.time(), "state": state})

    def close(self) -> None:
        with self._lock:
            if self._fh is not None:
                self._fh.close()
                self._fh = None

    def remove(self) -> None:
        self.close()
        try:
            os.remove(self.path)
        except FileNotFoundError:
            pass


def plan_shards(size: int, shard_size: int) -> list[tuple[int, int]]:
    """Deterministic [start, end) byte ranges; identical input -> identical plan."""
    if shard_size <= 0:
        raise ValueError("shard_size must be positive")
    if size == 0:
        return []
    return [
        (start, min(start + shard_size, size))
        for start in range(0, size, shard_size)
    ]


def recover_journals(
    journal_dir: str,
    quarantine_dir: str,
    *,
    blob_exists,
) -> list[dict]:
    """Read every journal and validate it against its pinned shard plan.

    Returns a list of recovery descriptors::

        {"session_id", "file", "version_id", "shard_size", "shard_count",
         "completed": [int, ...], "ended": str | None, "journal_path"}

    Anything inconsistent (missing blob, malformed records, foreign shards,
    mixed versions) is moved to ``quarantine_dir`` and omitted.
    """
    os.makedirs(journal_dir, exist_ok=True)
    os.makedirs(quarantine_dir, exist_ok=True)
    recovered: list[dict] = []

    for fname in sorted(os.listdir(journal_dir)):
        if not fname.endswith(".log"):
            continue
        path = os.path.join(journal_dir, fname)
        session_id = fname[:-4]
        try:
            descriptor = _recover_one(path, session_id, blob_exists)
        except JournalError:
            _quarantine(path, quarantine_dir)
            continue
        if descriptor is None:
            # Cleanly ended session: its journal has no recovery value.
            try:
                os.remove(path)
            except OSError:
                pass
            continue
        recovered.append(descriptor)
    return recovered


def _recover_one(path: str, session_id: str, blob_exists) -> dict | None:
    header: dict | None = None
    completed: list[int] = []
    ended: str | None = None
    with open(path, "r", encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except ValueError as exc:
                raise JournalError(f"malformed json on line {line_no}") from exc
            rtype = rec.get("type")
            if rtype == "SESSION":
                if header is not None:
                    raise JournalError("duplicate SESSION record")
                if rec.get("session_id") != session_id:
                    raise JournalError("session id mismatch")
                header = rec
            elif rtype == "SHARD":
                if header is None:
                    raise JournalError("SHARD before SESSION")
                completed.append(_validate_shard(rec, header, line_no))
            elif rtype == "END":
                if header is None:
                    raise JournalError("END before SESSION")
                ended = str(rec.get("state", "unknown"))
            else:
                raise JournalError(f"unknown record type {rtype!r}")

    if header is None:
        raise JournalError("empty journal / no SESSION record")
    if not blob_exists(header["version_id"]):
        # The immutable content this scan was pinned to no longer exists.
        raise JournalError("blob for journaled version is missing")

    try:
        file_size = int(header["file_size"])
        shard_size = int(header["shard_size"])
        shard_count = int(header["shard_count"])
    except (KeyError, TypeError, ValueError) as exc:
        raise JournalError("malformed SESSION header") from exc
    plan = plan_shards(file_size, shard_size)
    if len(plan) != shard_count:
        raise JournalError("shard plan no longer matches journal header")
    if len(set(completed)) != len(completed):
        raise JournalError("duplicate shard records")

    if ended is not None:
        return None
    return {
        "session_id": session_id,
        "file": header["file"],
        "version_id": header["version_id"],
        "file_size": file_size,
        "shard_size": shard_size,
        "shard_count": shard_count,
        "completed": sorted(completed),
        "ended": ended,
        "journal_path": path,
    }


def _validate_shard(rec: dict, header: dict, line_no: int) -> int:
    try:
        index = int(rec["index"])
        start = int(rec["start"])
        end = int(rec["end"])
    except (KeyError, TypeError, ValueError) as exc:
        raise JournalError(f"bad SHARD record on line {line_no}") from exc
    shard_size = int(header["shard_size"])
    if not (0 <= index < int(header["shard_count"])):
        raise JournalError(f"shard index {index} outside plan")
    if start != index * shard_size:
        raise JournalError(f"shard {index} start does not match plan")
    expected_end = min((index + 1) * shard_size, int(header["file_size"]))
    if end != expected_end:
        raise JournalError(f"shard {index} end does not match plan")
    return index


def _quarantine(path: str, quarantine_dir: str) -> None:
    os.makedirs(quarantine_dir, exist_ok=True)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    base = os.path.basename(path)
    dest = os.path.join(quarantine_dir, f"{stamp}-{base}")
    try:
        os.replace(path, dest)
    except OSError:
        pass
