"""Offline TTS concurrency replay from a complete performance trace."""

from __future__ import annotations

import argparse
import asyncio
import json
import math
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from videotrans.podcast.replay import VirtualClock
from videotrans.podcast.scheduler import AsyncScheduler
from videotrans.podcast.trace import TraceError, read_performance_trace

MEASURED_BASELINE_SECONDS = 52.009
IMPROVEMENT_TARGET_FRACTION = 0.10
CALIBRATION_TOLERANCE_SECONDS = 0.5
TTS_REQUESTS_PER_MINUTE = 150.0
BASELINE_CONCURRENCY = 4
DEFAULT_CONCURRENCIES = (4, 6, 8)
DEFAULT_LATENCY_MULTIPLIERS = (1.0, 1.1, 1.2)


@dataclass(frozen=True)
class _Observation:
    chunk_id: int
    release_seconds: float
    observed_start_seconds: float
    observed_commit_seconds: float
    dependency_range: tuple[int, int]

    @property
    def service_seconds(self) -> float:
        return self.observed_commit_seconds - self.observed_start_seconds


@dataclass(frozen=True)
class _ReplayChunk:
    chunk_id: int
    release_seconds: float
    start_seconds: float
    commit_seconds: float


@dataclass(frozen=True)
class _TraceInput:
    observations: tuple[_Observation, ...]
    trace_total_seconds: float
    finalization_tail_seconds: float


def replay_tts_concurrency(
    trace_path: str | Path,
    *,
    concurrencies: Sequence[int] = DEFAULT_CONCURRENCIES,
    latency_multipliers: Sequence[float] = DEFAULT_LATENCY_MULTIPLIERS,
    requests_per_minute: float = TTS_REQUESTS_PER_MINUTE,
    measured_baseline_seconds: float = MEASURED_BASELINE_SECONDS,
    calibration_tolerance_seconds: float = CALIBRATION_TOLERANCE_SECONDS,
) -> dict[str, Any]:
    """Compare TTS concurrency using releases and service times from ``trace_path``.

    Translation behavior is frozen by replaying every observed TTS queue time at
    its original absolute offset. Each TTS operation keeps its observed
    start-to-commit duration, optionally scaled for sensitivity analysis. The
    real production scheduler supplies both the concurrency gate and smooth
    request-start pacing.
    """

    candidate_concurrencies = _positive_unique_ints(concurrencies, "concurrency")
    if BASELINE_CONCURRENCY not in candidate_concurrencies:
        raise ValueError("concurrencies must include the baseline concurrency 4")
    multipliers = _positive_floats(latency_multipliers, "latency multiplier")
    if 1.0 not in multipliers:
        raise ValueError("latency_multipliers must include 1.0")
    if not math.isfinite(requests_per_minute) or requests_per_minute <= 0:
        raise ValueError("requests_per_minute must be greater than zero")
    if not math.isfinite(measured_baseline_seconds) or measured_baseline_seconds <= 0:
        raise ValueError("measured_baseline_seconds must be greater than zero")
    if (
        not math.isfinite(calibration_tolerance_seconds)
        or calibration_tolerance_seconds < 0
    ):
        raise ValueError("calibration_tolerance_seconds cannot be negative")

    trace_input = _read_trace_input(Path(trace_path), requests_per_minute)
    schedules = asyncio.run(
        _replay_all(
            trace_input.observations,
            candidate_concurrencies,
            multipliers,
            requests_per_minute,
        )
    )

    baseline_schedule = schedules[(BASELINE_CONCURRENCY, 1.0)]
    baseline_predicted = _predicted_total(
        baseline_schedule, trace_input.finalization_tail_seconds
    )
    residual = abs(baseline_predicted - measured_baseline_seconds)
    if residual > calibration_tolerance_seconds:
        raise TraceError(
            "baseline replay residual exceeds calibration tolerance: "
            f"{residual:.6f}s > {calibration_tolerance_seconds:.6f}s"
        )

    target_seconds = measured_baseline_seconds * (1 - IMPROVEMENT_TARGET_FRACTION)
    candidates = [
        _candidate_result(
            concurrency,
            schedules[(concurrency, 1.0)],
            trace_input.finalization_tail_seconds,
            measured_baseline_seconds,
            target_seconds,
        )
        for concurrency in candidate_concurrencies
    ]
    sensitivity = [
        {
            "service_latency_multiplier": multiplier,
            "candidates": [
                _candidate_summary(
                    concurrency,
                    schedules[(concurrency, multiplier)],
                    trace_input.finalization_tail_seconds,
                    measured_baseline_seconds,
                    target_seconds,
                )
                for concurrency in candidate_concurrencies
            ],
        }
        for multiplier in multipliers
    ]

    return {
        "schema_version": 1,
        "assumptions": {
            "translation_release_times": "frozen_observed_tts_queue_times",
            "tts_service_latency": "observed_start_to_commit_per_chunk",
            "stage_offset": "original_absolute_trace_time",
            "finalization_tail": "observed_last_tts_commit_to_process_completion",
            "predictions_are_measurements": False,
        },
        "observations": {
            "chunk_count": len(trace_input.observations),
            "trace_total_seconds": _rounded(trace_input.trace_total_seconds),
            "measured_baseline_seconds": measured_baseline_seconds,
            "original_tts_concurrency": BASELINE_CONCURRENCY,
            "tts_requests_per_minute": float(requests_per_minute),
            "first_release_seconds": _rounded(
                trace_input.observations[0].release_seconds
            ),
            "finalization_tail_seconds": _rounded(
                trace_input.finalization_tail_seconds
            ),
        },
        "calibration": {
            "concurrency": BASELINE_CONCURRENCY,
            "predicted_total_seconds": _rounded(baseline_predicted),
            "absolute_residual_seconds": _rounded(residual),
            "tolerance_seconds": calibration_tolerance_seconds,
            "passed": True,
        },
        "target": {
            "relative_improvement_fraction": IMPROVEMENT_TARGET_FRACTION,
            "maximum_total_seconds": _rounded(target_seconds),
        },
        "candidates": candidates,
        "sensitivity": sensitivity,
    }


def _read_trace_input(path: Path, expected_rpm: float) -> _TraceInput:
    trace = read_performance_trace(path)
    events = trace.events
    if not events:
        raise TraceError("trace is empty")
    if {event["process_segment"] for event in events} != {1}:
        raise TraceError("resumed traces cannot be replayed")

    completions = [
        event
        for event in events
        if event["event"] == "completion" and "stage_id" not in event
    ]
    if len(completions) != 1 or events[-1] is not completions[0]:
        raise TraceError("trace must end with one process completion")
    trace_total_seconds = completions[0]["duration_ns"] / 1_000_000_000

    stage_pairs: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
    for stage_id in ("prepare", "asr", "finalize"):
        stage_events = [event for event in events if event.get("stage_id") == stage_id]
        if [event["event"] for event in stage_events] != ["start", "commit"]:
            raise TraceError(f"trace requires one complete {stage_id} stage")
        stage_pairs[stage_id] = (stage_events[0], stage_events[1])

    translation_by_event = _chunk_stage_events(events, "translate")
    translation_ids = set(translation_by_event["queue"])
    if (
        not translation_ids
        or any(set(items) != translation_ids for items in translation_by_event.values())
        or translation_ids != set(range(len(translation_ids)))
    ):
        raise TraceError(
            "translation queue, start, and commit observations must be contiguous"
        )
    for chunk_id in translation_ids:
        queued = translation_by_event["queue"][chunk_id]["monotonic_ns"]
        started = translation_by_event["start"][chunk_id]["monotonic_ns"]
        committed = translation_by_event["commit"][chunk_id]["monotonic_ns"]
        if not queued <= started <= committed:
            raise TraceError("translation observations are out of order")

    by_event = _chunk_stage_events(events, "tts")
    queue_order: list[int] = []
    for event in events:
        if event.get("stage_id") != "tts":
            continue
        event_name = event["event"]
        chunk_id = event.get("chunk_id")
        dependency = event.get("dependency_range")
        if not isinstance(dependency, list) or len(dependency) != 2:
            raise TraceError("every TTS observation requires a dependency_range")
        if event_name in {"queue", "start"}:
            limits = event.get("limits")
            if not isinstance(limits, dict):
                raise TraceError("TTS queue and start observations require limits")
            if limits.get("concurrency") != BASELINE_CONCURRENCY:
                raise TraceError("trace is not a concurrency-4 baseline")
            if float(limits.get("requests_per_minute", -1)) != float(expected_rpm):
                raise TraceError("trace TTS rate does not match the replay rate")
        if event_name == "queue":
            queue_order.append(chunk_id)

    chunk_ids = set(by_event["queue"])
    if not chunk_ids or any(set(items) != chunk_ids for items in by_event.values()):
        raise TraceError("TTS queue, start, and commit chunk observations must match")
    expected_chunk_ids = set(range(len(chunk_ids)))
    if chunk_ids != expected_chunk_ids or queue_order != list(range(len(chunk_ids))):
        raise TraceError(
            "TTS chunk observations must be contiguous and released in order"
        )

    observations: list[_Observation] = []
    for chunk_id in range(len(chunk_ids)):
        queued = by_event["queue"][chunk_id]
        started = by_event["start"][chunk_id]
        committed = by_event["commit"][chunk_id]
        dependencies = {
            tuple(event["dependency_range"]) for event in (queued, started, committed)
        }
        if len(dependencies) != 1:
            raise TraceError("TTS dependency observations disagree")
        dependency = next(iter(dependencies))
        if dependency[0] not in translation_ids or dependency[1] not in translation_ids:
            raise TraceError("TTS dependency refers to a missing translation chunk")
        release_ns = queued["monotonic_ns"]
        prefix_committed_ns = max(
            translation_by_event["commit"][index]["monotonic_ns"]
            for index in range(dependency[1] + 1)
        )
        if release_ns < prefix_committed_ns:
            raise TraceError("TTS release precedes its committed translation prefix")
        start_ns = started["monotonic_ns"]
        commit_ns = committed["monotonic_ns"]
        if not release_ns <= start_ns <= commit_ns:
            raise TraceError("TTS queue, start, and commit times are out of order")
        observations.append(
            _Observation(
                chunk_id=chunk_id,
                release_seconds=release_ns / 1_000_000_000,
                observed_start_seconds=start_ns / 1_000_000_000,
                observed_commit_seconds=commit_ns / 1_000_000_000,
                dependency_range=dependency,
            )
        )

    last_tts_commit_ns = max(
        by_event["commit"][chunk_id]["monotonic_ns"] for chunk_id in chunk_ids
    )
    last_tts_commit = last_tts_commit_ns / 1_000_000_000
    finalization_tail = trace_total_seconds - last_tts_commit
    if finalization_tail < 0:
        raise TraceError("process completion precedes the final TTS commit")
    finalize_start, finalize_commit = stage_pairs["finalize"]
    if not (
        last_tts_commit_ns
        <= finalize_start["monotonic_ns"]
        <= finalize_commit["monotonic_ns"]
        <= completions[0]["monotonic_ns"]
    ):
        raise TraceError("finalization observations are out of order")
    return _TraceInput(tuple(observations), trace_total_seconds, finalization_tail)


def _chunk_stage_events(
    events: Sequence[dict[str, Any]], stage_id: str
) -> dict[str, dict[int, dict[str, Any]]]:
    by_event: dict[str, dict[int, dict[str, Any]]] = {
        name: {} for name in ("queue", "start", "commit")
    }
    for event in events:
        if event.get("stage_id") != stage_id:
            continue
        event_name = event["event"]
        if event_name not in by_event:
            raise TraceError(f"trace contains an unsupported {stage_id} event")
        chunk_id = event.get("chunk_id")
        if not isinstance(chunk_id, int):
            raise TraceError(f"every {stage_id} observation requires a chunk_id")
        if chunk_id in by_event[event_name]:
            raise TraceError(
                f"retry or duplicate {stage_id} observations cannot be replayed"
            )
        by_event[event_name][chunk_id] = event
    return by_event


async def _replay_all(
    observations: Sequence[_Observation],
    concurrencies: Sequence[int],
    multipliers: Sequence[float],
    requests_per_minute: float,
) -> dict[tuple[int, float], tuple[_ReplayChunk, ...]]:
    result: dict[tuple[int, float], tuple[_ReplayChunk, ...]] = {}
    for multiplier in multipliers:
        for concurrency in concurrencies:
            result[(concurrency, multiplier)] = await _replay(
                observations,
                concurrency=concurrency,
                requests_per_minute=requests_per_minute,
                latency_multiplier=multiplier,
            )
    return result


async def _replay(
    observations: Sequence[_Observation],
    *,
    concurrency: int,
    requests_per_minute: float,
    latency_multiplier: float,
) -> tuple[_ReplayChunk, ...]:
    clock = VirtualClock()
    scheduler: AsyncScheduler[int, int] = AsyncScheduler(
        concurrency=concurrency,
        requests_per_minute=requests_per_minute,
        initial_tokens=0,
        max_retries=0,
        clock=clock.monotonic,
        sleeper=clock.sleep,
        jitter=lambda: 0.0,
    )
    starts: dict[int, float] = {}
    commits: dict[int, float] = {}

    async def submit(observation: _Observation) -> int:
        await clock.sleep(observation.release_seconds)

        async def operation(chunk_id: int) -> int:
            starts[chunk_id] = clock.monotonic()
            await clock.sleep(observation.service_seconds * latency_multiplier)
            commits[chunk_id] = clock.monotonic()
            return chunk_id

        return await scheduler.run_one(observation.chunk_id, operation)

    async def submit_all() -> list[int]:
        return list(
            await asyncio.gather(*(submit(observation) for observation in observations))
        )

    results = await clock.drive(submit_all())
    expected_order = [observation.chunk_id for observation in observations]
    if list(results) != expected_order:
        raise RuntimeError("production scheduler replay changed input order")
    return tuple(
        _ReplayChunk(
            chunk_id=observation.chunk_id,
            release_seconds=observation.release_seconds,
            start_seconds=starts[observation.chunk_id],
            commit_seconds=commits[observation.chunk_id],
        )
        for observation in observations
    )


def _candidate_result(
    concurrency: int,
    schedule: Sequence[_ReplayChunk],
    finalization_tail_seconds: float,
    measured_baseline_seconds: float,
    target_seconds: float,
) -> dict[str, Any]:
    result = _candidate_summary(
        concurrency,
        schedule,
        finalization_tail_seconds,
        measured_baseline_seconds,
        target_seconds,
    )
    result["schedule"] = [
        {
            "chunk_id": chunk.chunk_id,
            "release_seconds": _rounded(chunk.release_seconds),
            "start_seconds": _rounded(chunk.start_seconds),
            "commit_seconds": _rounded(chunk.commit_seconds),
        }
        for chunk in schedule
    ]
    return result


def _candidate_summary(
    concurrency: int,
    schedule: Sequence[_ReplayChunk],
    finalization_tail_seconds: float,
    measured_baseline_seconds: float,
    target_seconds: float,
) -> dict[str, Any]:
    predicted = _predicted_total(schedule, finalization_tail_seconds)
    improvement = measured_baseline_seconds - predicted
    return {
        "concurrency": concurrency,
        "predicted_total_seconds": _rounded(predicted),
        "improvement_seconds": _rounded(improvement),
        "relative_improvement_fraction": _rounded(
            improvement / measured_baseline_seconds
        ),
        "meets_ten_percent_gate": predicted <= target_seconds,
    }


def _predicted_total(
    schedule: Sequence[_ReplayChunk], finalization_tail_seconds: float
) -> float:
    return max(chunk.commit_seconds for chunk in schedule) + finalization_tail_seconds


def _positive_unique_ints(values: Sequence[int], name: str) -> tuple[int, ...]:
    normalized = tuple(values)
    if not normalized or any(
        isinstance(value, bool) or value <= 0 for value in normalized
    ):
        raise ValueError(f"{name} values must be positive integers")
    if any(not isinstance(value, int) for value in normalized):
        raise ValueError(f"{name} values must be positive integers")
    if len(set(normalized)) != len(normalized):
        raise ValueError(f"{name} values must be unique")
    return normalized


def _positive_floats(values: Sequence[float], name: str) -> tuple[float, ...]:
    normalized = tuple(float(value) for value in values)
    if not normalized or any(
        not math.isfinite(value) or value <= 0 for value in normalized
    ):
        raise ValueError(f"{name} values must be positive")
    if len(set(normalized)) != len(normalized):
        raise ValueError(f"{name} values must be unique")
    return normalized


def _rounded(value: float) -> float:
    return round(value, 6)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trace", required=True, type=Path)
    args = parser.parse_args(argv)
    print(json.dumps(replay_tts_concurrency(args.trace), sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised through the module CLI
    raise SystemExit(main())


__all__ = ["replay_tts_concurrency"]
