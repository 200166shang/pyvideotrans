"""Privacy-safe, crash-resilient performance tracing for podcast runs."""

from __future__ import annotations

import json
import math
import os
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

TRACE_FILE_NAME = "performance-trace.jsonl"
TRACE_SCHEMA_VERSION = 1

_STAGE_IDS = frozenset(("prepare", "asr", "translate", "tts", "finalize"))
_EVENTS = frozenset(("queue", "start", "completion", "commit"))
_EVENT_KEYS = frozenset(
    (
        "schema_version",
        "process_segment",
        "monotonic_ns",
        "event",
        "stage_id",
        "chunk_id",
        "dependency_range",
        "limits",
        "duration_ns",
    )
)
_REQUIRED_EVENT_KEYS = frozenset(
    ("schema_version", "process_segment", "monotonic_ns", "event")
)
_LIMIT_KEYS = frozenset(("concurrency", "requests_per_minute"))


class TraceError(ValueError):
    """Raised when a performance trace cannot be safely read or written."""

    code = "trace_invalid"


@dataclass(frozen=True)
class PerformanceTraceRead:
    """Validated trace events and their report-ready direct durations."""

    events: tuple[dict[str, Any], ...]
    completed_process_segment_durations_ns: tuple[tuple[int, int], ...]
    active_process_duration_ns: int
    single_segment_wall_clock_ns: int | None


class PerformanceTrace:
    """Append validated events for one new process segment in a run directory."""

    def __init__(
        self,
        run_directory: str | os.PathLike[str],
        *,
        monotonic_ns: Callable[[], int] | None = None,
    ) -> None:
        self.path = Path(run_directory) / TRACE_FILE_NAME
        existing = recover_performance_trace(self.path)
        self.process_segment = (
            existing.events[-1]["process_segment"] + 1 if existing.events else 1
        )
        self._clock = monotonic_ns or time.monotonic_ns
        self._origin_ns = self._read_clock()
        self._last_relative_ns = -1
        self._completed = False

    def record(
        self,
        event: str,
        *,
        stage_id: str | None = None,
        chunk_id: int | None = None,
        dependency_range: tuple[int, int] | list[int] | None = None,
        limits: Mapping[str, int | float] | None = None,
        duration_ns: int | None = None,
    ) -> dict[str, Any]:
        """Append one content-free event and return its canonical representation."""

        if self._completed:
            raise TraceError("a completed process segment cannot accept more events")
        relative_ns = self._read_clock() - self._origin_ns
        if relative_ns < self._last_relative_ns:
            raise TraceError("monotonic clock moved backwards")
        event_data: dict[str, Any] = {
            "schema_version": TRACE_SCHEMA_VERSION,
            "process_segment": self.process_segment,
            "monotonic_ns": relative_ns,
            "event": event,
        }
        if stage_id is not None:
            event_data["stage_id"] = stage_id
        if chunk_id is not None:
            event_data["chunk_id"] = chunk_id
        if dependency_range is not None:
            event_data["dependency_range"] = list(dependency_range)
        if limits is not None:
            event_data["limits"] = dict(limits)
        if duration_ns is not None:
            event_data["duration_ns"] = duration_ns
        validate_trace_event(event_data)
        _append_event(self.path, event_data)
        self._last_relative_ns = relative_ns
        return event_data

    def complete_segment(self) -> dict[str, Any]:
        """Durably record this process segment's direct elapsed duration."""

        relative_ns = self._read_clock() - self._origin_ns
        event = {
            "schema_version": TRACE_SCHEMA_VERSION,
            "process_segment": self.process_segment,
            "monotonic_ns": relative_ns,
            "event": "completion",
            "duration_ns": relative_ns,
        }
        if self._completed:
            raise TraceError("process segment is already complete")
        if relative_ns < self._last_relative_ns:
            raise TraceError("monotonic clock moved backwards")
        validate_trace_event(event)
        _append_event(self.path, event)
        self._last_relative_ns = relative_ns
        self._completed = True
        return event

    def _read_clock(self) -> int:
        value = self._clock()
        _non_negative_int(value, "monotonic clock")
        return value


def validate_trace_event(event: Mapping[str, Any]) -> None:
    """Reject fields or values outside the content-free trace schema."""

    if not isinstance(event, Mapping):
        raise TraceError("trace event must be an object")
    keys = set(event)
    if not _REQUIRED_EVENT_KEYS <= keys or not keys <= _EVENT_KEYS:
        raise TraceError("trace event contains unknown or missing fields")
    if event["schema_version"] != TRACE_SCHEMA_VERSION:
        raise TraceError("unsupported trace schema version")
    _positive_int(event["process_segment"], "process_segment")
    _non_negative_int(event["monotonic_ns"], "monotonic_ns")
    if event["event"] not in _EVENTS:
        raise TraceError("trace event type is invalid")

    stage_id = event.get("stage_id")
    if stage_id is not None and stage_id not in _STAGE_IDS:
        raise TraceError("stage_id is invalid")
    if "chunk_id" in event:
        _non_negative_int(event["chunk_id"], "chunk_id")
        if stage_id not in {"translate", "tts"}:
            raise TraceError("chunk_id requires a chunked stage_id")
    if "dependency_range" in event:
        dependency = event["dependency_range"]
        if not isinstance(dependency, (list, tuple)) or len(dependency) != 2:
            raise TraceError("dependency_range must contain two numeric indices")
        _non_negative_int(dependency[0], "dependency_range start")
        _non_negative_int(dependency[1], "dependency_range end")
        if dependency[0] > dependency[1]:
            raise TraceError("dependency_range must be inclusive and ordered")
    if "limits" in event:
        limits = event["limits"]
        if not isinstance(limits, Mapping) or set(limits) != _LIMIT_KEYS:
            raise TraceError("limits fields are invalid")
        _positive_int(limits["concurrency"], "limits.concurrency")
        _positive_number(limits["requests_per_minute"], "limits.requests_per_minute")
    if "duration_ns" in event:
        _non_negative_int(event["duration_ns"], "duration_ns")
        if (
            event["event"] != "completion"
            or keys != _REQUIRED_EVENT_KEYS | {"duration_ns"}
            or event["duration_ns"] != event["monotonic_ns"]
        ):
            raise TraceError("duration_ns is only valid for direct process completion")


def read_performance_trace(
    path: str | os.PathLike[str],
) -> PerformanceTraceRead:
    """Read and validate complete trace lines without repairing malformed data."""

    target = Path(path)
    if not target.exists():
        return PerformanceTraceRead((), (), 0, None)
    raw = target.read_bytes()
    if raw and not raw.endswith(b"\n"):
        raise TraceError("trace has an incomplete trailing line")

    return _read_complete_trace(raw)


def recover_performance_trace(
    path: str | os.PathLike[str],
) -> PerformanceTraceRead:
    """Validate complete records and durably discard one partial tail, if present."""

    target = Path(path)
    if not target.exists():
        return PerformanceTraceRead((), (), 0, None)
    raw = target.read_bytes()
    if not raw or raw.endswith(b"\n"):
        result = _read_complete_trace(raw)
        _protect_owner_only(target)
        return result

    final_newline = raw.rfind(b"\n")
    complete_length = final_newline + 1
    complete = raw[:complete_length]
    result = _read_complete_trace(complete)
    descriptor = os.open(target, os.O_WRONLY)
    try:
        os.ftruncate(descriptor, complete_length)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    _protect_owner_only(target)
    return result


def _read_complete_trace(raw: bytes) -> PerformanceTraceRead:
    """Decode a byte string known to contain only newline-terminated records."""

    events: list[dict[str, Any]] = []
    previous_segment = 0
    previous_timestamp = -1
    durations: dict[int, int] = {}
    for raw_line in raw.splitlines():
        try:
            event = json.loads(raw_line)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise TraceError("trace contains invalid JSON") from error
        validate_trace_event(event)
        segment = event["process_segment"]
        timestamp = event["monotonic_ns"]
        if segment < previous_segment or segment > previous_segment + 1:
            raise TraceError("process_segment sequence is invalid")
        if segment == previous_segment and timestamp < previous_timestamp:
            raise TraceError("monotonic_ns moved backwards within a process segment")
        if segment != previous_segment:
            previous_timestamp = -1
        if segment in durations:
            raise TraceError("a completed process segment has later events")
        if "duration_ns" in event:
            durations[segment] = event["duration_ns"]
        events.append(dict(event))
        previous_segment = segment
        previous_timestamp = timestamp

    active_duration = sum(durations.values())
    segments = {event["process_segment"] for event in events}
    wall_clock = durations.get(1) if segments == {1} else None
    return PerformanceTraceRead(
        tuple(events), tuple(durations.items()), active_duration, wall_clock
    )


def _append_event(path: Path, event: Mapping[str, Any]) -> None:
    encoded = (
        json.dumps(
            event,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    created = not path.exists()
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        _protect_owner_only(path)
        written = os.write(descriptor, encoded)
        if written != len(encoded):
            raise OSError("performance trace append was incomplete")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    if created:
        _fsync_directory(path.parent)


def _protect_owner_only(path: Path) -> None:
    try:
        os.chmod(path, 0o600)
    except (NotImplementedError, OSError):
        # Permission bits are unavailable on some supported platforms/filesystems.
        pass


def _fsync_directory(directory: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        descriptor = os.open(directory, flags)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        # Some platforms/filesystems do not support syncing directory handles.
        pass
    finally:
        os.close(descriptor)


def _non_negative_int(value: Any, label: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise TraceError(f"{label} must be a non-negative integer")


def _positive_int(value: Any, label: str) -> None:
    _non_negative_int(value, label)
    if value == 0:
        raise TraceError(f"{label} must be positive")


def _positive_number(value: Any, label: str) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value <= 0
    ):
        raise TraceError(f"{label} must be a positive finite number")


__all__ = [
    "TRACE_FILE_NAME",
    "TRACE_SCHEMA_VERSION",
    "PerformanceTrace",
    "PerformanceTraceRead",
    "TraceError",
    "recover_performance_trace",
    "read_performance_trace",
    "validate_trace_event",
]
