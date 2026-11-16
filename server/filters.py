"""Per-subscriber filters.

A filter is evaluated independently for every subscriber against every record
produced by a shared scan. The scan itself never applies a filter — that is the
whole point: one physical scan, many different logical result streams.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass


class FilterError(ValueError):
    pass


@dataclass(frozen=True)
class Filter:
    kind: str  # "all" | "contains" | "regex" | "json"
    pattern: str = ""
    key: str = ""
    op: str = ""
    value: object = None
    _compiled: object = None

    def to_dict(self) -> dict:
        return {
            "kind": self.kind,
            "pattern": self.pattern,
            "key": self.key,
            "op": self.op,
            # value may be any JSON scalar; json.dumps keeps it representable
            "value": self.value,
        }

    def matches(self, line: str) -> bool:
        if self.kind == "all":
            return True
        if self.kind == "contains":
            return self.pattern in line
        if self.kind == "regex":
            return self._compiled.search(line) is not None
        if self.kind == "json":
            try:
                obj = json.loads(line)
            except (ValueError, TypeError):
                return False
            if not isinstance(obj, dict) or self.key not in obj:
                return False
            actual = obj[self.key]
            op = self.op
            if op == "eq":
                return actual == self.value
            if op == "ne":
                return actual != self.value
            if op == "contains":
                return isinstance(actual, str) and str(self.value) in actual
            if op == "gt":
                try:
                    return actual > self.value
                except TypeError:
                    return False
            if op == "lt":
                try:
                    return actual < self.value
                except TypeError:
                    return False
            raise FilterError(f"unknown json op: {op}")
        raise FilterError(f"unknown filter kind: {self.kind}")


def build_filter(spec: dict | None) -> Filter:
    """Build a Filter from an API request body, validating everything up front."""
    if not spec or spec.get("kind", "all") == "all":
        return Filter(kind="all")
    kind = spec.get("kind")
    if kind == "contains":
        pattern = spec.get("pattern", "")
        if not isinstance(pattern, str):
            raise FilterError("contains pattern must be a string")
        return Filter(kind="contains", pattern=pattern)
    if kind == "regex":
        pattern = spec.get("pattern", "")
        if not isinstance(pattern, str):
            raise FilterError("regex pattern must be a string")
        try:
            compiled = re.compile(pattern)
        except re.error as exc:
            raise FilterError(f"invalid regex: {exc}") from exc
        return Filter(kind="regex", pattern=pattern, _compiled=compiled)
    if kind == "json":
        key = spec.get("key", "")
        op = spec.get("op", "eq")
        if not isinstance(key, str) or not key:
            raise FilterError("json filter requires a non-empty key")
        if op not in ("eq", "ne", "contains", "gt", "lt"):
            raise FilterError(f"unsupported json op: {op}")
        value = spec.get("value")
        return Filter(kind="json", key=key, op=op, value=value)
    raise FilterError(f"unsupported filter kind: {kind}")
