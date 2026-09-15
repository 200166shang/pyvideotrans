"""Deterministic, content-free performance replay for the podcast pipeline."""

from __future__ import annotations

import asyncio
import heapq
import io
import tempfile
import wave
from collections.abc import Awaitable, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TypeVar

from videotrans.podcast import scheduler as production_scheduler
from videotrans.podcast.alibaba_text import SpeechResult, TranslationResult
from videotrans.podcast.manifest import (
    ManifestStore,
    PodcastManifest,
    atomic_write_json,
)
from videotrans.podcast.orchestrator import PodcastCoordinator, PodcastRuntime
from videotrans.podcast.profiles import ALIBABA_PODCAST_V2
from videotrans.podcast.trace import PerformanceTrace, read_performance_trace

ResultT = TypeVar("ResultT")


@dataclass(frozen=True)
class _StageConfiguration:
    chunk_count: int
    concurrency: int
    requests_per_minute: float
    initial_tokens: float


@dataclass(frozen=True)
class _ScheduledChunk:
    ready_seconds: float
    start_seconds: float
    finish_seconds: float


class VirtualClock:
    """Shared offline replay clock, advancing only to awaited deadlines.

    Used by both the coordinator replay and trace-based throughput comparison.
    It performs no wall-clock sleeping or provider work.
    """

    def __init__(self) -> None:
        self._now = 0.0
        self._sequence = 0
        self._revision = 0
        self._sleepers: list[tuple[float, int, asyncio.Future[None]]] = []

    def monotonic(self) -> float:
        return self._now

    async def sleep(self, delay_seconds: float) -> None:
        if delay_seconds < 0:
            raise ValueError("delay_seconds cannot be negative")
        if delay_seconds == 0:
            await asyncio.sleep(0)
            return

        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()
        self._sequence += 1
        heapq.heappush(
            self._sleepers,
            (self._now + delay_seconds, self._sequence, future),
        )
        self._revision += 1
        await future

    def _advance_next(self) -> bool:
        while self._sleepers and self._sleepers[0][2].done():
            heapq.heappop(self._sleepers)
        if not self._sleepers:
            return False

        self._now = max(self._now, self._sleepers[0][0])
        while self._sleepers and self._sleepers[0][0] <= self._now:
            _, _, future = heapq.heappop(self._sleepers)
            if not future.done():
                future.set_result(None)
        self._revision += 1
        return True

    async def drive(self, awaitable: Awaitable[ResultT]) -> ResultT:
        """Run asyncio work, advancing time only after runnable work settles."""

        task = asyncio.create_task(awaitable)
        stable_turns = 0
        observed_revision = self._revision
        while not task.done():
            await asyncio.sleep(0)
            if task.done():
                break
            if self._revision != observed_revision:
                observed_revision = self._revision
                stable_turns = 0
                continue

            stable_turns += 1
            # Semaphore hand-offs and producer/consumer wake-ups can span
            # several event-loop turns. Let them settle before jumping time.
            if stable_turns < 8:
                continue
            if not self._advance_next():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                raise RuntimeError("virtual replay deadlocked without a timer")
            observed_revision = self._revision
            stable_turns = 0

        return await task


_CHUNK_COUNT = 12
_PREPARE_SECONDS = 2.24
_ASR_SECONDS = 13.20
_TRANSLATION_STANDALONE_SECONDS = 11.68
_TTS_STANDALONE_SECONDS = 26.06
_OVERLAP_TARGET_SECONDS = 45.0

_TRANSLATION = _StageConfiguration(
    chunk_count=_CHUNK_COUNT,
    concurrency=4,
    requests_per_minute=60,
    initial_tokens=1,
)
_TTS = _StageConfiguration(
    chunk_count=_CHUNK_COUNT,
    concurrency=4,
    requests_per_minute=150,
    initial_tokens=0,
)


def _new_scheduler(
    configuration: _StageConfiguration,
    clock: VirtualClock,
) -> production_scheduler.AsyncScheduler[int, int]:
    return production_scheduler.AsyncScheduler(
        concurrency=configuration.concurrency,
        requests_per_minute=configuration.requests_per_minute,
        initial_tokens=configuration.initial_tokens,
        max_retries=0,
        clock=clock.monotonic,
        sleeper=clock.sleep,
        jitter=lambda: 0.0,
    )


async def _replay_stage(
    configuration: _StageConfiguration,
    service_latency_seconds: float,
) -> tuple[_ScheduledChunk, ...]:
    if service_latency_seconds < 0:
        raise ValueError("service_latency_seconds cannot be negative")

    clock = VirtualClock()
    scheduler = _new_scheduler(configuration, clock)
    starts = [0.0] * configuration.chunk_count
    finishes = [0.0] * configuration.chunk_count

    async def operation(index: int) -> int:
        starts[index] = clock.monotonic()
        await clock.sleep(service_latency_seconds)
        finishes[index] = clock.monotonic()
        return index

    await clock.drive(scheduler.run(range(configuration.chunk_count), operation))
    return tuple(
        _ScheduledChunk(
            ready_seconds=0.0,
            start_seconds=starts[index],
            finish_seconds=finishes[index],
        )
        for index in range(configuration.chunk_count)
    )


def _duration(schedule: Sequence[_ScheduledChunk]) -> float:
    return max((chunk.finish_seconds for chunk in schedule), default=0.0)


async def _calibrate_service_latency(
    configuration: _StageConfiguration,
    observed_duration_seconds: float,
) -> tuple[float, tuple[_ScheduledChunk, ...]]:
    """Use the production scheduler to fit a uniform service latency."""

    if observed_duration_seconds < 0:
        raise ValueError("observed_duration_seconds cannot be negative")

    lower = 0.0
    upper = max(1.0, observed_duration_seconds)
    upper_schedule = await _replay_stage(configuration, upper)
    while _duration(upper_schedule) < observed_duration_seconds:
        upper *= 2.0
        upper_schedule = await _replay_stage(configuration, upper)

    for _ in range(60):
        midpoint = (lower + upper) / 2.0
        replayed = await _replay_stage(configuration, midpoint)
        if _duration(replayed) < observed_duration_seconds:
            lower = midpoint
        else:
            upper = midpoint

    latency = (lower + upper) / 2.0
    return latency, await _replay_stage(configuration, latency)


async def _replay_overlap(
    translation_latency_seconds: float,
    tts_latency_seconds: float,
) -> tuple[tuple[_ScheduledChunk, ...], tuple[_ScheduledChunk, ...]]:
    """Drive the actual production overlap coordinator with virtual providers."""

    clock = VirtualClock()

    class ReplayCoordinator(PodcastCoordinator):
        async def _call_translation(self, text: str) -> TranslationResult:
            await clock.sleep(translation_latency_seconds)
            return TranslationResult(text="中" * 500, usage=0, request_id=None)

        async def _call_tts(self, text: str) -> SpeechResult:
            await clock.sleep(tts_latency_seconds)
            return SpeechResult(audio=_silent_wav(), usage=0, request_id=None)

    with tempfile.TemporaryDirectory(prefix="podcast-replay-") as temporary:
        run_directory = Path(temporary)
        private_dir = run_directory / "private"
        private_dir.mkdir()
        profile = ALIBABA_PODCAST_V2
        manifest = PodcastManifest.create(source={"replay": 1}, profile=asdict(profile))
        store = ManifestStore(run_directory / "manifest.json")
        store.save(manifest)
        state: dict[str, object] = {
            "stage_elapsed_ms": {},
            "chunk_elapsed_ms": {"translate": {}, "tts": {}},
        }
        atomic_write_json(run_directory / "run.private.json", state)
        runtime = PodcastRuntime(
            asr=None,  # type: ignore[arg-type]
            translator=None,  # type: ignore[arg-type]
            tts=None,  # type: ignore[arg-type]
            upload_audio=lambda _: "",
            finalizer=None,  # type: ignore[arg-type]
        )
        coordinator = ReplayCoordinator(
            runtime,
            profile,
            clock=clock.monotonic,
            monotonic_ns=lambda: round(clock.monotonic() * 1_000_000_000),
            scheduler_clock=clock.monotonic,
            scheduler_sleeper=clock.sleep,
        )
        segments = [
            {
                "sequence_id": f"segment-{index + 1:06d}",
                "text": "a",
                "speaker_id": f"speaker-{index + 1:06d}",
            }
            for index in range(_CHUNK_COUNT)
        ]
        trace = PerformanceTrace(
            run_directory,
            monotonic_ns=lambda: round(clock.monotonic() * 1_000_000_000),
        )
        await clock.drive(
            coordinator._translate_and_tts_async(
                segments,
                private_dir,
                manifest,
                state,
                store,
                run_directory,
                trace,
            )
        )
        events = read_performance_trace(trace.path).events

    def event_times(stage: str, event: str) -> list[float]:
        selected = {
            int(item["chunk_id"]): float(item["monotonic_ns"]) / 1_000_000_000
            for item in events
            if item.get("stage_id") == stage and item["event"] == event
        }
        return [selected[index] for index in range(_CHUNK_COUNT)]

    translation_starts = event_times("translate", "start")
    translation_finishes = event_times("translate", "commit")
    tts_ready = event_times("tts", "queue")
    tts_starts = event_times("tts", "start")
    tts_finishes = event_times("tts", "commit")
    return (
        tuple(
            _ScheduledChunk(0.0, translation_starts[index], translation_finishes[index])
            for index in range(_CHUNK_COUNT)
        ),
        tuple(
            _ScheduledChunk(tts_ready[index], tts_starts[index], tts_finishes[index])
            for index in range(_CHUNK_COUNT)
        ),
    )


def _silent_wav() -> bytes:
    output = io.BytesIO()
    with wave.open(output, "wb") as audio:
        audio.setnchannels(1)
        audio.setsampwidth(2)
        audio.setframerate(16_000)
        audio.writeframes(b"\0\0" * 160)
    return output.getvalue()


def _timing_rows(schedule: Sequence[_ScheduledChunk]) -> list[list[float | int]]:
    return [
        [
            index,
            round(chunk.ready_seconds, 6),
            round(chunk.start_seconds, 6),
            round(chunk.finish_seconds, 6),
        ]
        for index, chunk in enumerate(schedule)
    ]


async def _build_replay() -> dict[str, object]:
    translation_latency, translation_standalone = await _calibrate_service_latency(
        _TRANSLATION,
        _TRANSLATION_STANDALONE_SECONDS,
    )
    tts_latency, tts_standalone = await _calibrate_service_latency(
        _TTS,
        _TTS_STANDALONE_SECONDS,
    )
    translation_overlap, tts_overlap = await _replay_overlap(
        translation_latency,
        tts_latency,
    )

    overlap_phase_seconds = max(
        _duration(translation_overlap),
        _duration(tts_overlap),
    )
    predicted_wall_clock_seconds = (
        _PREPARE_SECONDS + _ASR_SECONDS + overlap_phase_seconds
    )
    translation_replayed = _duration(translation_standalone)
    tts_replayed = _duration(tts_standalone)

    return {
        "schema_version": 1,
        "sample": {
            "source_duration_seconds": 300,
            "chunk_count": _CHUNK_COUNT,
            "dependency_map": [[index, index] for index in range(_CHUNK_COUNT)],
        },
        "observations": {
            "prepare_seconds": _PREPARE_SECONDS,
            "asr_seconds": _ASR_SECONDS,
            "translation_standalone_seconds": _TRANSLATION_STANDALONE_SECONDS,
            "tts_standalone_seconds": _TTS_STANDALONE_SECONDS,
        },
        "configuration": {
            "translation": {
                "concurrency": _TRANSLATION.concurrency,
                "requests_per_minute": _TRANSLATION.requests_per_minute,
                "initial_tokens": _TRANSLATION.initial_tokens,
            },
            "tts": {
                "concurrency": _TTS.concurrency,
                "requests_per_minute": _TTS.requests_per_minute,
                "initial_tokens": _TTS.initial_tokens,
            },
        },
        "calibration": {
            "translation": {
                "service_latency_seconds": round(translation_latency, 6),
                "replayed_standalone_seconds": round(translation_replayed, 6),
                "absolute_error_seconds": round(
                    abs(translation_replayed - _TRANSLATION_STANDALONE_SECONDS),
                    6,
                ),
            },
            "tts": {
                "service_latency_seconds": round(tts_latency, 6),
                "replayed_standalone_seconds": round(tts_replayed, 6),
                "absolute_error_seconds": round(
                    abs(tts_replayed - _TTS_STANDALONE_SECONDS),
                    6,
                ),
            },
        },
        "prediction": {
            "overlap_phase_seconds": round(overlap_phase_seconds, 6),
            "wall_clock_seconds": round(predicted_wall_clock_seconds, 6),
            "target_seconds": _OVERLAP_TARGET_SECONDS,
            "meets_target": predicted_wall_clock_seconds <= _OVERLAP_TARGET_SECONDS,
        },
        "schedule": {
            "translation_standalone": _timing_rows(translation_standalone),
            "tts_standalone": _timing_rows(tts_standalone),
            "translation_overlap": _timing_rows(translation_overlap),
            "tts_overlap": _timing_rows(tts_overlap),
        },
    }


def replay_second_round_performance() -> dict[str, object]:
    """Calibrate and replay the accepted sample with production schedulers.

    The returned JSON-compatible structure contains only numeric timings,
    configuration values, dependency indices, and booleans. It never carries
    source, translation, or synthesis text.
    """

    return asyncio.run(_build_replay())
