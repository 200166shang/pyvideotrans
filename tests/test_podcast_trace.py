import json
import stat

import pytest

from videotrans.podcast.orchestrator import _complete_trace_segment_safely
from videotrans.podcast.trace import (
    TRACE_FILE_NAME,
    TRACE_SCHEMA_VERSION,
    PerformanceTrace,
    TraceError,
    read_performance_trace,
    validate_trace_event,
)


class FakeClock:
    def __init__(self, *values: int) -> None:
        self._values = iter(values)

    def __call__(self) -> int:
        return next(self._values)


def test_writes_schema_versioned_relative_events_and_direct_wall_clock(tmp_path):
    trace = PerformanceTrace(tmp_path, monotonic_ns=FakeClock(1_000, 1_100, 1_450))

    queued = trace.record(
        "queue",
        stage_id="tts",
        chunk_id=3,
        dependency_range=(7, 9),
        limits={"concurrency": 4, "requests_per_minute": 150},
    )
    completed = trace.complete_segment()

    assert queued == {
        "schema_version": TRACE_SCHEMA_VERSION,
        "process_segment": 1,
        "monotonic_ns": 100,
        "event": "queue",
        "stage_id": "tts",
        "chunk_id": 3,
        "dependency_range": [7, 9],
        "limits": {"concurrency": 4, "requests_per_minute": 150},
    }
    assert completed == {
        "schema_version": TRACE_SCHEMA_VERSION,
        "process_segment": 1,
        "monotonic_ns": 450,
        "event": "completion",
        "duration_ns": 450,
    }

    path = tmp_path / TRACE_FILE_NAME
    persisted = [
        json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()
    ]
    assert persisted == [queued, completed]

    result = read_performance_trace(path)
    assert result.events == (queued, completed)
    assert result.active_process_duration_ns == 450
    assert result.single_segment_wall_clock_ns == 450


def test_resume_discards_only_a_trailing_partial_line_and_increments_segment(tmp_path):
    first = PerformanceTrace(tmp_path, monotonic_ns=FakeClock(10_000, 10_025))
    first.record("start", stage_id="prepare")
    path = tmp_path / TRACE_FILE_NAME
    valid_prefix = path.read_bytes()
    with path.open("ab") as stream:
        stream.write(b'{"schema_version":1,"process_segment":1')

    resumed = PerformanceTrace(tmp_path, monotonic_ns=FakeClock(20_000, 20_075))
    resumed_event = resumed.record("start", stage_id="asr")

    assert resumed.process_segment == 2
    assert path.read_bytes().startswith(valid_prefix)
    assert b'"process_segment":1\n' not in path.read_bytes()[len(valid_prefix) :]
    assert resumed_event["process_segment"] == 2
    assert resumed_event["monotonic_ns"] == 75
    assert read_performance_trace(path).events[-1] == resumed_event


def test_resume_rejects_a_complete_invalid_line_without_truncating_anything(tmp_path):
    trace = PerformanceTrace(tmp_path, monotonic_ns=FakeClock(1_000, 1_010))
    trace.record("start", stage_id="prepare")
    path = tmp_path / TRACE_FILE_NAME
    with path.open("ab") as stream:
        stream.write(b'{"error":"private exception text"}\n{"partial":')
    corrupted = path.read_bytes()

    with pytest.raises(TraceError, match="unknown or missing") as captured:
        PerformanceTrace(tmp_path, monotonic_ns=FakeClock(2_000))

    assert captured.value.code == "trace_invalid"
    assert path.read_bytes() == corrupted


def _valid_event(**changes):
    event = {
        "schema_version": TRACE_SCHEMA_VERSION,
        "process_segment": 1,
        "monotonic_ns": 25,
        "event": "start",
        "stage_id": "translate",
        "chunk_id": 2,
        "dependency_range": [2, 2],
        "limits": {"concurrency": 4, "requests_per_minute": 60},
    }
    event.update(changes)
    return event


@pytest.mark.parametrize(
    "forbidden",
    [
        {"text": "private words"},
        {"path": "/private/source.m4a"},
        {"credentials": "secret"},
        {"headers": {"Authorization": "Bearer secret"}},
        {"body": "provider response"},
        {"error": "arbitrary exception text"},
    ],
)
def test_exact_whitelist_rejects_every_content_bearing_field(forbidden):
    with pytest.raises(TraceError, match="unknown"):
        validate_trace_event(_valid_event(**forbidden))


@pytest.mark.parametrize(
    "changes",
    [
        {"stage_id": "customer supplied stage"},
        {"chunk_id": "sha256:" + "a" * 64},
        {"dependency_range": [0, "translated text"]},
        {"limits": {"concurrency": 4, "requests_per_minute": 60, "token": "x"}},
    ],
)
def test_identifiers_dependencies_and_limits_cannot_carry_text(changes):
    with pytest.raises(TraceError):
        validate_trace_event(_valid_event(**changes))


def test_direct_duration_is_only_a_bare_process_completion():
    event = {
        "schema_version": TRACE_SCHEMA_VERSION,
        "process_segment": 1,
        "monotonic_ns": 25,
        "event": "completion",
        "duration_ns": 25,
        "dependency_range": [0, 0],
    }

    with pytest.raises(TraceError, match="direct process completion"):
        validate_trace_event(event)


def test_trace_is_owner_only_and_fsyncs_file_and_parent_directory(
    tmp_path, monkeypatch
):
    import videotrans.podcast.trace as trace_module

    fsynced = []
    real_fsync = trace_module.os.fsync

    def recording_fsync(descriptor):
        fsynced.append(descriptor)
        return real_fsync(descriptor)

    monkeypatch.setattr(trace_module.os, "fsync", recording_fsync)
    trace = PerformanceTrace(tmp_path, monotonic_ns=FakeClock(1_000, 1_001))
    trace.record("start", stage_id="prepare")

    assert stat.S_IMODE((tmp_path / TRACE_FILE_NAME).stat().st_mode) == 0o600
    assert len(fsynced) >= 2


def test_partial_append_is_discarded_on_the_next_process_start(tmp_path, monkeypatch):
    import videotrans.podcast.trace as trace_module

    trace = PerformanceTrace(tmp_path, monotonic_ns=FakeClock(1_000, 1_010))
    real_write = trace_module.os.write

    def partial_write(descriptor, data):
        return real_write(descriptor, data[:-1])

    monkeypatch.setattr(trace_module.os, "write", partial_write)
    with pytest.raises(OSError, match="append was incomplete"):
        trace.record("start", stage_id="prepare")

    monkeypatch.setattr(trace_module.os, "write", real_write)
    resumed = PerformanceTrace(tmp_path, monotonic_ns=FakeClock(2_000, 2_020))
    resumed.record("start", stage_id="prepare")

    result = read_performance_trace(tmp_path / TRACE_FILE_NAME)
    assert len(result.events) == 1
    assert result.events[0]["process_segment"] == 1


def test_resumed_trace_sums_only_direct_segment_durations_and_has_no_wall_clock(
    tmp_path,
):
    first = PerformanceTrace(tmp_path, monotonic_ns=FakeClock(1_000, 1_100))
    first.complete_segment()
    second = PerformanceTrace(tmp_path, monotonic_ns=FakeClock(50_000, 50_250))
    second.complete_segment()

    result = read_performance_trace(tmp_path / TRACE_FILE_NAME)

    assert result.completed_process_segment_durations_ns == ((1, 100), (2, 250))
    assert result.active_process_duration_ns == 350
    assert result.single_segment_wall_clock_ns is None


def test_safe_completion_does_not_duplicate_a_record_written_before_fsync_error(
    tmp_path, monkeypatch
) -> None:
    trace = PerformanceTrace(tmp_path, monotonic_ns=FakeClock(1_000, 1_100))
    complete = trace.complete_segment

    def write_then_fail():
        complete()
        raise OSError("fsync failed after write")

    monkeypatch.setattr(trace, "complete_segment", write_then_fail)
    with pytest.raises(OSError, match="fsync failed"):
        _complete_trace_segment_safely(trace)

    recovered = _complete_trace_segment_safely(trace)
    events = read_performance_trace(trace.path).events

    assert recovered["event"] == "completion"
    assert [event["event"] for event in events] == ["completion"]
