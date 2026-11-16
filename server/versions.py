"""File registry and content-addressed, immutable blob store.

Correctness foundation for version isolation:

* Every upload is hashed and stored once under ``blobs/<sha256>``. A blob is
  never mutated or deleted while scans may reference it.
* "Replacing" a file only flips the registry's current-version pointer. A scan
  that started against the old version keeps reading the old blob, so a new
  query can physically never be wired onto an old scan and an old scan can
  never see new bytes.
* The registry is persisted with tmp-file + ``os.replace`` + fsync so a crash
  never leaves a half-written index.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import threading
import time
from dataclasses import dataclass, field

_NAME_RE = re.compile(r"^[A-Za-z0-9._-]{1,128}$")


def validate_name(name: str) -> None:
    if not isinstance(name, str) or not _NAME_RE.match(name):
        raise ValueError(
            "invalid file name: 1-128 chars, letters/digits/dot/underscore/dash only"
        )


def fsync_dir(path: str) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


@dataclass
class VersionInfo:
    version_id: str          # content hash (sha256 hex), also the blob name
    seq: int                 # per-file monotonic version sequence, 1-based
    size: int
    created_at: float

    def to_dict(self) -> dict:
        return {
            "version_id": self.version_id,
            "seq": self.seq,
            "size": self.size,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "VersionInfo":
        return cls(
            version_id=d["version_id"],
            seq=d["seq"],
            size=d["size"],
            created_at=d["created_at"],
        )


@dataclass
class FileEntry:
    name: str
    current: str                       # version_id currently served to new queries
    versions: list[VersionInfo] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "current": self.current,
            "versions": [v.to_dict() for v in self.versions],
        }

    @classmethod
    def from_dict(cls, d: dict) -> "FileEntry":
        return cls(
            name=d["name"],
            current=d["current"],
            versions=[VersionInfo.from_dict(v) for v in d["versions"]],
        )

    def version(self, version_id: str) -> VersionInfo:
        for v in self.versions:
            if v.version_id == version_id:
                return v
        raise KeyError(version_id)


class FileRegistry:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.blob_dir = os.path.join(data_dir, "blobs")
        self.registry_path = os.path.join(data_dir, "registry.json")
        os.makedirs(self.blob_dir, exist_ok=True)
        self._lock = threading.RLock()
        self.files: dict[str, FileEntry] = {}
        self._load()

    # ---------------------------------------------------------------- persist
    def _load(self) -> None:
        if not os.path.exists(self.registry_path):
            return
        with open(self.registry_path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
        for name, data in raw.get("files", {}).items():
            self.files[name] = FileEntry.from_dict(data)

    def _persist_locked(self) -> None:
        payload = {
            "files": {name: entry.to_dict() for name, entry in self.files.items()}
        }
        tmp = self.registry_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self.registry_path)
        fsync_dir(self.data_dir)

    # ------------------------------------------------------------------ blobs
    def blob_path(self, version_id: str) -> str:
        # version_id is a sha256 hex string, so this cannot escape blob_dir
        if not re.fullmatch(r"[0-9a-f]{64}", version_id):
            raise ValueError("malformed version id")
        return os.path.join(self.blob_dir, version_id)

    def _store_blob(self, content: bytes) -> tuple[str, int]:
        digest = hashlib.sha256(content).hexdigest()
        path = self.blob_path(digest)
        if not os.path.exists(path):
            tmp = os.path.join(self.blob_dir, f".tmp-{digest}-{os.getpid()}")
            with open(tmp, "wb") as fh:
                fh.write(content)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, path)
            fsync_dir(self.blob_dir)
        return digest, len(content)

    # --------------------------------------------------------------- public API
    def register(self, name: str, content: bytes) -> tuple[FileEntry, VersionInfo, bool]:
        """Create a file or replace its contents.

        Returns ``(entry, version_info, is_new_version)``. Identical re-upload
        is idempotent and does not allocate a new sequence number.
        """
        validate_name(name)
        digest, size = self._store_blob(content)
        with self._lock:
            entry = self.files.get(name)
            now = time.time()
            if entry is not None:
                for v in entry.versions:
                    if v.version_id == digest:
                        entry.current = digest
                        self._persist_locked()
                        return entry, v, False
                seq = max(v.seq for v in entry.versions) + 1
                info = VersionInfo(version_id=digest, seq=seq, size=size, created_at=now)
                entry.versions.append(info)
                entry.current = digest
                self._persist_locked()
                return entry, info, True
            info = VersionInfo(version_id=digest, seq=1, size=size, created_at=now)
            entry = FileEntry(name=name, current=digest, versions=[info])
            self.files[name] = entry
            self._persist_locked()
            return entry, info, True

    def resolve(self, name: str, version_id: str | None = None) -> VersionInfo:
        """Resolve a (name, optional explicit version) to an immutable version."""
        with self._lock:
            entry = self.files.get(name)
            if entry is None:
                raise KeyError(f"unknown file: {name}")
            if version_id is None:
                version_id = entry.current
            return entry.version(version_id)

    def get(self, name: str) -> FileEntry:
        with self._lock:
            entry = self.files.get(name)
            if entry is None:
                raise KeyError(f"unknown file: {name}")
            return entry

    def list_files(self) -> list[dict]:
        with self._lock:
            entries = sorted(self.files.values(), key=lambda e: e.name.lower())
            return [entry.to_dict() for entry in entries]
