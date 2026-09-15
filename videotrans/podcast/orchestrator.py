"""Ordered, resumable coordinator for English-to-Mandarin podcast runs."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol

from .alibaba_asr import AlibabaWholeFileAsrClient, AsrPollResult
from .alibaba_text import AlibabaTranslationAdapter, AlibabaTTSAdapter
from .chunking import (
    IncrementalTTSChunker,
    PodcastTextRow,
    TTSChunk,
    chunk_translation_rows,
)
from .finalize import FinalizedAudio, Mp3Finalizer
from .manifest import (
    ManifestStore,
    PodcastManifest,
    atomic_write_json,
    chunk_identity,
)
from .profiles import PodcastProfile
from .report import (
    build_report,
    fingerprint_file,
    make_chunk,
    make_stage,
    validate_report,
    write_report,
)
from .scheduler import AsyncScheduler
from .trace import TRACE_FILE_NAME, PerformanceTrace, recover_performance_trace

PRIVATE_STATE_NAME = "run.private.json"
MANIFEST_NAME = "manifest.json"
DEFAULT_REPORT_NAME = "production-report.json"
DEFAULT_OUTPUT_NAME = "podcast.zh-CN.mp3"
ASR_CNY_PER_SECOND = 0.00022
TRANSLATION_CONSERVATIVE_CNY_PER_MILLION_TOKENS = 1.95
TTS_CNY_PER_TEN_THOUSAND_CHARACTERS = 0.8
MAX_PROVIDER_ATTEMPTS = 6

ASR_TIMING_DEFAULTS = {
    "upload_elapsed_ms": 0,
    "upload_count": 0,
    "submit_elapsed_ms": 0,
    "submit_count": 0,
    "poll_inclusive_elapsed_ms": 0,
    "poll_inclusive_count": 0,
    "status_query_elapsed_ms": 0,
    "status_query_count": 0,
    "result_download_elapsed_ms": 0,
    "result_download_count": 0,
    "result_parse_elapsed_ms": 0,
    "result_parse_count": 0,
    "poll_sleep_elapsed_ms": 0,
    "poll_sleep_count": 0,
    "poll_retry_sleep_elapsed_ms": 0,
    "poll_retry_sleep_count": 0,
    "provider_queue_elapsed_ms": None,
    "provider_task_elapsed_ms": None,
}

ASR_POLL_DURATION_FIELDS = (
    "status_query_elapsed_ms",
    "status_query_count",
    "result_download_elapsed_ms",
    "result_download_count",
    "result_parse_elapsed_ms",
    "result_parse_count",
)


class UploadAudio(Protocol):
    def __call__(self, source: Path) -> str: ...


@dataclass(frozen=True)
class PodcastRuntime:
    asr: AlibabaWholeFileAsrClient
    translator: AlibabaTranslationAdapter
    tts: AlibabaTTSAdapter
    upload_audio: UploadAudio
    finalizer: Mp3Finalizer


@dataclass(frozen=True)
class PodcastRunResult:
    run_directory: Path
    output_path: Path
    report_path: Path
    status: str


class PodcastPipelineError(RuntimeError):
    """A stable, credential-free error suitable for CLI output and reports."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.safe_message = message
        super().__init__(message)


class _PipelineStopped(RuntimeError):
    """Internal signal used to prevent provider starts after another failure."""


class PodcastCoordinator:
    """Own all durable transitions for the five-stage production pipeline."""

    def __init__(
        self,
        runtime: PodcastRuntime,
        profile: PodcastProfile,
        *,
        clock: Callable[[], float] = time.perf_counter,
        sleeper: Callable[[float], None] = time.sleep,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
        scheduler_clock: Callable[[], float] = time.monotonic,
        scheduler_sleeper: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.runtime = runtime
        self.profile = profile
        self._clock = clock
        self._sleeper = sleeper
        self._monotonic_ns = monotonic_ns
        self._scheduler_clock = scheduler_clock
        self._scheduler_sleeper = scheduler_sleeper

    def create(
        self,
        source: Path,
        run_directory: Path,
        *,
        report_path: Path | None = None,
        cache_mode: str = "cold",
    ) -> PodcastRunResult:
        source = Path(source).expanduser().resolve()
        if not source.is_file():
            raise PodcastPipelineError("input_not_found", "Input file does not exist")
        run_directory = Path(run_directory).expanduser().resolve()
        run_directory.mkdir(parents=True, exist_ok=True)
        store = ManifestStore(run_directory / MANIFEST_NAME, lock_timeout=0)
        existing = list(run_directory.iterdir())
        if existing:
            if store.path in existing:
                raise PodcastPipelineError(
                    "run_already_exists",
                    "Run directory already contains a manifest; use --resume",
                )
            raise PodcastPipelineError(
                "run_directory_not_empty",
                "Run directory is not empty and has no resumable manifest",
            )
        normalized_report_path = (
            _validate_new_report_destination(source, run_directory, report_path)
            if report_path
            else None
        )

        with store.run_lock():
            if store.path.exists():
                raise PodcastPipelineError(
                    "run_already_exists",
                    "Run directory already contains a manifest; use --resume",
                )
            unexpected = [
                path for path in run_directory.iterdir() if path != store.lock_path
            ]
            if unexpected:
                raise PodcastPipelineError(
                    "run_directory_not_empty",
                    "Run directory is not empty and has no resumable manifest",
                )
            duration_ms = probe_duration_ms(source)
            input_fingerprint = fingerprint_file(source)
            manifest = PodcastManifest.create(
                source={"fingerprint": input_fingerprint},
                profile=asdict(self.profile),
            )
            state = {
                "schema_version": 1,
                "source_path": str(source),
                "profile_id": self.profile.id,
                "profile_fingerprint": self.profile.fingerprint,
                "cache_mode": cache_mode,
                "report_path": str(normalized_report_path)
                if normalized_report_path
                else None,
                "input_fingerprint": input_fingerprint,
                "input_duration_ms": duration_ms,
                "asr_submission_state": "not_started",
                "asr_timing": dict(ASR_TIMING_DEFAULTS),
                "stage_elapsed_ms": {},
                "chunk_elapsed_ms": {"translate": {}, "tts": {}},
            }
            atomic_write_json(run_directory / PRIVATE_STATE_NAME, state)
            store.save(manifest)
            return self._execute(
                run_directory, manifest, state, store, normalized_report_path
            )

    def resume(
        self,
        run_directory: Path,
        *,
        report_path: Path | None = None,
    ) -> PodcastRunResult:
        run_directory = Path(run_directory).expanduser().resolve()
        store = ManifestStore(run_directory / MANIFEST_NAME, lock_timeout=0)
        state_path = run_directory / PRIVATE_STATE_NAME
        if not store.path.is_file() or not state_path.is_file():
            raise PodcastPipelineError(
                "run_state_missing", "Run directory is missing resumable state"
            )
        with store.run_lock():
            state = _read_json(state_path)
            if (
                state.get("profile_id") != self.profile.id
                or state.get("profile_fingerprint") != self.profile.fingerprint
            ):
                raise PodcastPipelineError(
                    "profile_mismatch", "Saved run uses a different production profile"
                )
            destination = _resolve_report_destination(run_directory, state, report_path)
            manifest = store.load()
            self._validate_committed_artifacts(run_directory, manifest)
            recover_performance_trace(run_directory / TRACE_FILE_NAME)
            manifest.normalize_for_resume()
            store.save(manifest)
            if manifest.run["status"] in {"accepted", "rejected"}:
                return self._terminal_result(
                    run_directory, manifest, state, report_path=destination
                )
            return self._execute(run_directory, manifest, state, store, destination)

    @staticmethod
    def _validate_committed_artifacts(
        run_directory: Path, manifest: PodcastManifest
    ) -> None:
        """Validate every reusable artifact before mutating resume state."""

        private_dir = run_directory / "private"
        for stage in manifest.stages.values():
            artifact = stage["artifact"]
            if artifact is not None and artifact.get("path") is not None:
                _validate_artifact(
                    run_directory / artifact["path"], artifact, run_directory
                )
        for item in manifest.stage("translate")["chunks"]:
            if item["status"] == "committed":
                path = _validate_artifact_package(
                    private_dir / "translate", item, artifact_name="artifact.json"
                )
                _validate_artifact(path, item["artifact"], run_directory)
        for item in manifest.stage("tts")["chunks"]:
            if item["status"] == "committed":
                _validate_tts_artifact(item, private_dir, run_directory)

    def _terminal_result(
        self,
        run_directory: Path,
        manifest: PodcastManifest,
        state: Mapping[str, Any],
        *,
        report_path: Path | None,
    ) -> PodcastRunResult:
        destination = _resolve_report_destination(run_directory, state, report_path)
        report = _read_json(destination)
        validate_report(report)
        if report["run"]["status"] != manifest.run["status"]:
            raise PodcastPipelineError(
                "review_state_mismatch",
                "Manifest and production report review states do not match",
            )
        output = run_directory / DEFAULT_OUTPUT_NAME
        _validate_artifact(
            output, manifest.stage("finalize")["artifact"], run_directory
        )
        return PodcastRunResult(
            run_directory=run_directory,
            output_path=output,
            report_path=destination,
            status=manifest.run["status"],
        )

    def _execute(
        self,
        run_directory: Path,
        manifest: PodcastManifest,
        state: dict[str, Any],
        store: ManifestStore,
        report_path: Path | None,
    ) -> PodcastRunResult:
        private_dir = run_directory / "private"
        private_dir.mkdir(parents=True, exist_ok=True)
        source = Path(state["source_path"])
        trace = PerformanceTrace(run_directory, monotonic_ns=self._monotonic_ns)
        segment_completed = False
        try:
            trace.record("start", stage_id="prepare")
            prepared_audio = self._prepare(
                source, private_dir, manifest, state, store, run_directory
            )
            trace.record("commit", stage_id="prepare")
            trace.record("start", stage_id="asr")
            segments = self._asr(
                prepared_audio, private_dir, manifest, state, store, run_directory
            )
            trace.record("commit", stage_id="asr")
            audio_chunks = self._translate_and_tts(
                segments,
                private_dir,
                manifest,
                state,
                store,
                run_directory,
                trace,
            )
            trace.record("start", stage_id="finalize")
            finalized = self._finalize(
                audio_chunks, run_directory, manifest, state, store
            )
            trace.record("commit", stage_id="finalize")
            completion = _complete_trace_segment_safely(trace)
            segment_completed = True
            segment_ms = round(completion["duration_ns"] / 1_000_000)
            active_process_ms = round(
                recover_performance_trace(trace.path).active_process_duration_ns
                / 1_000_000
            )
            state["active_process_ms"] = active_process_ms
            if manifest.run["resume_count"] == 0:
                state["wall_clock_ms"] = segment_ms
                state["timing_basis"] = "wall-clock"
            else:
                state["wall_clock_ms"] = active_process_ms
                state["timing_basis"] = "active-process"
            atomic_write_json(run_directory / PRIVATE_STATE_NAME, state)
        except BaseException:
            if not segment_completed:
                _complete_trace_segment_safely(trace)
                state["active_process_ms"] = round(
                    recover_performance_trace(trace.path).active_process_duration_ns
                    / 1_000_000
                )
                atomic_write_json(run_directory / PRIVATE_STATE_NAME, state)
            raise
        destination = self._write_report(
            run_directory,
            manifest,
            state,
            finalized,
            report_path=report_path,
        )
        return PodcastRunResult(
            run_directory=run_directory,
            output_path=finalized.path,
            report_path=destination,
            status=manifest.run["status"],
        )

    def _prepare(
        self,
        source: Path,
        private_dir: Path,
        manifest: PodcastManifest,
        state: dict[str, Any],
        store: ManifestStore,
        run_directory: Path,
    ) -> Path:
        prepared_audio = private_dir / "input.asr.m4a"
        if manifest.stage("prepare")["status"] == "completed":
            _validate_artifact(
                prepared_audio,
                manifest.stage("prepare")["artifact"],
                run_directory,
            )
            return prepared_audio
        if not source.is_file():
            raise PodcastPipelineError(
                "input_not_found", "Original input is required to resume this stage"
            )
        started = self._clock()
        manifest.start_stage("prepare")
        store.save(manifest)
        try:
            prepare_asr_audio(source, prepared_audio)
            atomic_write_json(
                private_dir / "prepare.json",
                {
                    "input_fingerprint": state["input_fingerprint"],
                    "duration_ms": state["input_duration_ms"],
                    "sample_rate_hz": 16000,
                    "channels": 1,
                    "codec": "aac",
                },
            )
            manifest.commit_stage(
                "prepare",
                artifact_identity=fingerprint_file(prepared_audio),
                artifact_path=prepared_audio.relative_to(run_directory).as_posix(),
                size_bytes=prepared_audio.stat().st_size,
            )
            store.save(manifest)
        except Exception:
            manifest.fail_stage("prepare")
            store.save(manifest)
            raise
        finally:
            self._add_stage_elapsed("prepare", started, state, run_directory)
        return prepared_audio

    def _asr(
        self,
        source: Path,
        private_dir: Path,
        manifest: PodcastManifest,
        state: dict[str, Any],
        store: ManifestStore,
        run_directory: Path,
    ) -> list[dict[str, Any]]:
        artifact_path = private_dir / "asr.json"
        if manifest.stage("asr")["status"] == "completed":
            _validate_artifact(
                artifact_path, manifest.stage("asr")["artifact"], run_directory
            )
            return list(_read_json(artifact_path)["segments"])
        if not source.is_file():
            raise PodcastPipelineError(
                "input_not_found", "Original input is required to resume ASR"
            )

        started = self._clock()
        manifest.start_stage("asr")
        store.save(manifest)
        try:
            task_id = manifest.stage("asr")["task_id"]
            if task_id is None:
                if state.get("asr_submission_state") == "in_flight":
                    raise PodcastPipelineError(
                        "asr_submission_uncertain",
                        "ASR submission may have been accepted before interruption; "
                        "verify it manually instead of submitting again",
                    )
                audio_url = self._time_asr_operation(
                    state, "upload", lambda: self.runtime.upload_audio(source)
                )
                state["asr_submission_state"] = "in_flight"
                atomic_write_json(run_directory / PRIVATE_STATE_NAME, state)
                submitted = self._time_asr_operation(
                    state,
                    "submit",
                    lambda: self.runtime.asr.submit(
                        audio_url, language=self.profile.source_language
                    ),
                )
                manifest.set_asr_task_id(submitted.task_id)
                store.save(manifest)
                state["asr_submission_state"] = "task_recorded"
                atomic_write_json(run_directory / PRIVATE_STATE_NAME, state)
                task_id = submitted.task_id
            result = self._wait_for_asr(task_id, state, run_directory)
            payload = {
                "usage_duration_seconds": result.usage.duration_seconds,
                "segments": [
                    {
                        "sequence_id": f"segment-{index:06d}",
                        "begin_ms": segment.begin_ms,
                        "end_ms": segment.end_ms,
                        "text": segment.text,
                        "speaker_id": str(segment.speaker_id)
                        if segment.speaker_id is not None
                        else None,
                    }
                    for index, segment in enumerate(result.segments, start=1)
                ],
            }
            atomic_write_json(artifact_path, payload)
            manifest.commit_stage(
                "asr",
                artifact_identity=fingerprint_file(artifact_path),
                artifact_path=artifact_path.relative_to(run_directory).as_posix(),
                size_bytes=artifact_path.stat().st_size,
            )
            store.save(manifest)
            return list(payload["segments"])
        except Exception:
            if manifest.stage("asr")["status"] != "completed":
                manifest.fail_stage("asr")
                store.save(manifest)
            raise
        finally:
            self._add_stage_elapsed("asr", started, state, run_directory)

    def _wait_for_asr(
        self, task_id: str, state: dict[str, Any], run_directory: Path
    ) -> AsrPollResult:
        transient_failures = 0
        poll_attempt = 0
        while True:
            try:
                try:
                    result = self._time_asr_operation(
                        state, "poll_inclusive", lambda: self.runtime.asr.poll(task_id)
                    )
                finally:
                    self._merge_asr_poll_diagnostics(
                        state,
                        getattr(self.runtime.asr, "last_poll_diagnostics", None),
                    )
            except Exception as error:
                if not getattr(error, "retryable", False) or transient_failures >= 5:
                    raise
                delay = min(30.0, 2.0**transient_failures)
                transient_failures += 1
                state["asr_poll_retries"] = int(state.get("asr_poll_retries", 0)) + 1
                atomic_write_json(run_directory / PRIVATE_STATE_NAME, state)
                self._time_asr_operation(
                    state, "poll_retry_sleep", lambda delay=delay: self._sleeper(delay)
                )
                continue
            if result.is_complete:
                return result
            delay = self.runtime.asr.polling_policy.delay_for_attempt(poll_attempt)
            self._time_asr_operation(
                state, "poll_sleep", lambda delay=delay: self._sleeper(delay)
            )
            poll_attempt += 1

    def _translate_and_tts(
        self,
        segments: Sequence[Mapping[str, Any]],
        private_dir: Path,
        manifest: PodcastManifest,
        state: dict[str, Any],
        store: ManifestStore,
        run_directory: Path,
        trace: PerformanceTrace,
    ) -> list[Path]:
        """Overlap committed translations with ordered incremental synthesis."""

        self._adopt_orphan_packages(private_dir, manifest, store, run_directory)
        if manifest.uncertain_chunks():
            raise PodcastPipelineError(
                "submission_uncertain",
                "A provider request may already have been billed; inspect the run "
                "instead of submitting it again",
            )
        return asyncio.run(
            self._translate_and_tts_async(
                segments,
                private_dir,
                manifest,
                state,
                store,
                run_directory,
                trace,
            )
        )

    async def _translate_and_tts_async(
        self,
        segments: Sequence[Mapping[str, Any]],
        private_dir: Path,
        manifest: PodcastManifest,
        state: dict[str, Any],
        store: ManifestStore,
        run_directory: Path,
        trace: PerformanceTrace,
    ) -> list[Path]:
        translation_chunks = chunk_translation_rows(
            segments,
            min_token_units=self.profile.translation_chunk_min,
            max_token_units=self.profile.translation_chunk_max,
        )
        translation_sources = [
            {
                "sequence_id": chunk.sequence_id,
                "row_sequence_ids": chunk.row_sequence_ids,
                "text_sha256": _text_sha256(chunk.text),
            }
            for chunk in translation_chunks
        ]
        manifest.define_chunks("translate", translation_sources)
        store.save(manifest)

        committed: dict[int, dict[str, Any]] = {}
        pending: list[tuple[int, Any]] = []
        for item in manifest.stage("translate")["chunks"]:
            path = private_dir / "translate" / f"{item['index']:06d}" / "artifact.json"
            if item["status"] == "committed":
                _validate_artifact_package(
                    private_dir / "translate", item, artifact_name="artifact.json"
                )
                _validate_artifact(path, item["artifact"], run_directory)
                committed[item["index"]] = _read_json(path)
            else:
                pending.append((item["index"], translation_chunks[item["index"]]))

        translation_events: asyncio.Queue[tuple[int, dict[str, Any]] | None] = (
            asyncio.Queue()
        )
        tts_queue: asyncio.Queue[TTSChunk | None] = asyncio.Queue()
        stop = asyncio.Event()
        first_error: list[BaseException] = []

        def remember_error(error: BaseException) -> None:
            if not first_error:
                first_error.append(error)
            stop.set()

        translation_scheduler = AsyncScheduler(
            concurrency=self.profile.translation_concurrency,
            requests_per_minute=self.profile.translation_rpm,
            initial_tokens=1,
            transient_predicate=_is_safe_no_success_error,
            drain_on_failure=True,
            clock=self._scheduler_clock,
            sleeper=self._scheduler_sleeper,
        )
        tts_scheduler = AsyncScheduler(
            concurrency=self.profile.tts_concurrency,
            requests_per_minute=self.profile.tts_rpm,
            initial_tokens=0,
            transient_predicate=_is_safe_no_success_error,
            clock=self._scheduler_clock,
            sleeper=self._scheduler_sleeper,
        )

        translation_started = self._clock() if pending else None
        if pending:
            manifest.start_stage("translate")
            store.save(manifest)
            for index, _ in pending:
                trace.record(
                    "queue",
                    stage_id="translate",
                    chunk_id=index,
                    limits={
                        "concurrency": self.profile.translation_concurrency,
                        "requests_per_minute": self.profile.translation_rpm,
                    },
                )

        async def translate_operation(indexed_chunk: tuple[int, Any]) -> dict[str, Any]:
            index, chunk = indexed_chunk
            if stop.is_set():
                raise _PipelineStopped()
            trace.record(
                "start",
                stage_id="translate",
                chunk_id=index,
                limits={
                    "concurrency": self.profile.translation_concurrency,
                    "requests_per_minute": self.profile.translation_rpm,
                },
            )
            try:
                payload = await self._translate_one(
                    chunk,
                    private_dir,
                    manifest,
                    state,
                    store,
                    run_directory,
                )
            except BaseException as error:
                current = manifest.stage("translate")["chunks"][index]
                if (
                    not _is_safe_no_success_error(error)
                    or current["status"] == "failed"
                ):
                    remember_error(error)
                raise
            trace.record("commit", stage_id="translate", chunk_id=index)
            await translation_events.put((index, payload))
            return payload

        async def produce_translations() -> None:
            try:
                if pending:
                    await translation_scheduler.run(pending, translate_operation)
            except BaseException as error:
                remember_error(error)
            finally:
                if translation_started is not None:
                    self._add_stage_elapsed(
                        "translate", translation_started, state, run_directory
                    )
                await translation_events.put(None)

        tts_started: list[float] = []

        async def tts_worker() -> None:
            while True:
                chunk = await tts_queue.get()
                if chunk is None:
                    return
                if stop.is_set():
                    continue
                index = int(chunk.sequence_id.rsplit("-", 1)[1]) - 1
                dependency = _translation_dependency_range(chunk)

                async def tts_operation(work: TTSChunk) -> Path:
                    if stop.is_set():
                        raise _PipelineStopped()
                    trace.record(
                        "start",
                        stage_id="tts",
                        chunk_id=index,
                        dependency_range=dependency,
                        limits={
                            "concurrency": self.profile.tts_concurrency,
                            "requests_per_minute": self.profile.tts_rpm,
                        },
                    )
                    return await self._tts_one(
                        work,
                        private_dir,
                        manifest,
                        state,
                        store,
                        run_directory,
                    )

                try:
                    await tts_scheduler.run_one(chunk, tts_operation)
                    trace.record(
                        "commit",
                        stage_id="tts",
                        chunk_id=index,
                        dependency_range=dependency,
                    )
                except BaseException as error:
                    remember_error(error)

        workers = [
            asyncio.create_task(tts_worker())
            for _ in range(self.profile.tts_concurrency)
        ]

        chunker = IncrementalTTSChunker(max_characters=self.profile.tts_soft_char_limit)
        next_translation = 0
        next_tts = 0

        async def register_tts(chunk: TTSChunk) -> None:
            nonlocal next_tts
            source = _tts_chunk_source(chunk, self.profile.voice)
            stage = manifest.stage("tts")
            if next_tts < len(stage["chunks"]):
                item = stage["chunks"][next_tts]
                expected = chunk_identity(stage["identity"], next_tts, source)
                if item["identity"] != expected:
                    raise PodcastPipelineError(
                        "artifact_invalid",
                        "Saved synthesis plan does not match translations",
                    )
            else:
                item = manifest.append_chunk("tts", source)
                store.save(manifest)
            trace.record(
                "queue",
                stage_id="tts",
                chunk_id=next_tts,
                dependency_range=_translation_dependency_range(chunk),
                limits={
                    "concurrency": self.profile.tts_concurrency,
                    "requests_per_minute": self.profile.tts_rpm,
                },
            )
            if item["status"] == "committed":
                _validate_tts_artifact(item, private_dir, run_directory)
            elif not stop.is_set():
                if not tts_started:
                    tts_started.append(self._clock())
                    manifest.start_stage("tts")
                    store.save(manifest)
                await tts_queue.put(chunk)
            next_tts += 1

        async def release_contiguous() -> None:
            nonlocal next_translation
            while next_translation in committed:
                payload = committed.pop(next_translation)
                row = PodcastTextRow(
                    sequence_id=str(payload["sequence_id"]),
                    text=str(payload["translated_text"]),
                    speaker_id=payload.get("speaker_id"),
                )
                for sealed in chunker.add(row):
                    await register_tts(sealed)
                next_translation += 1

        producer = asyncio.create_task(produce_translations())
        try:
            await release_contiguous()
            while True:
                event = await translation_events.get()
                if event is None:
                    break
                index, payload = event
                committed[index] = payload
                await release_contiguous()
            await producer

            if not first_error and next_translation == len(translation_chunks):
                for sealed in chunker.finish():
                    await register_tts(sealed)
                manifest.seal_chunks("tts")
                store.save(manifest)
                if next_tts != len(manifest.stage("tts")["chunks"]):
                    raise PodcastPipelineError(
                        "artifact_invalid",
                        "Saved synthesis plan has chunks beyond the canonical plan",
                    )

                aggregate_path = private_dir / "translate.json"
                rows = self._translated_rows(private_dir, len(translation_chunks))
                atomic_write_json(aggregate_path, {"rows": rows})
                manifest.commit_stage(
                    "translate",
                    artifact_identity=fingerprint_file(aggregate_path),
                    artifact_path=aggregate_path.relative_to(run_directory).as_posix(),
                    size_bytes=aggregate_path.stat().st_size,
                )
                store.save(manifest)
        except BaseException as error:
            remember_error(error)
        finally:
            for _ in workers:
                await tts_queue.put(None)
            await asyncio.gather(*workers)

        if tts_started:
            self._add_stage_elapsed("tts", tts_started[0], state, run_directory)
        if first_error:
            raise first_error[0]
        if next_tts == 0:
            raise PodcastPipelineError(
                "no_speech", "The transcription did not produce speech to synthesize"
            )

        tts_paths = self._tts_paths(private_dir, next_tts)
        if manifest.stage("tts")["artifact"] is None:
            manifest.commit_stage("tts", artifact_identity=_paths_identity(tts_paths))
            store.save(manifest)
        return tts_paths

    async def _translate_one(
        self,
        chunk: Any,
        private_dir: Path,
        manifest: PodcastManifest,
        state: dict[str, Any],
        store: ManifestStore,
        run_directory: Path,
    ) -> dict[str, Any]:
        item = manifest.stage("translate")["chunks"][
            int(chunk.sequence_id.rsplit("-", 1)[1]) - 1
        ]
        started = self._clock()
        attempt_identity = manifest.start_chunk("translate", item["identity"])
        store.save(manifest)
        try:
            try:
                result = await self._call_translation(chunk.text)
            except Exception as error:
                self._record_provider_failure("translate", item, error, manifest, store)
                raise

            speakers = chunk.speaker_ids
            payload = {
                "sequence_id": chunk.sequence_id,
                "translated_text": result.text,
                "speaker_id": speakers[0] if len(speakers) == 1 else None,
                "row_sequence_ids": list(chunk.row_sequence_ids),
                "usage": result.usage,
            }
            path = _write_json_artifact_package(
                private_dir / "translate",
                item["index"],
                logical_identity=item["identity"],
                attempt_identity=attempt_identity,
                payload=payload,
            )
            manifest.commit_chunk(
                "translate",
                item["identity"],
                artifact_identity=fingerprint_file(path),
                artifact_path=path.relative_to(run_directory).as_posix(),
                size_bytes=path.stat().st_size,
            )
            store.save(manifest)
            return payload
        finally:
            self._add_chunk_elapsed(
                "translate", item["identity"], started, state, run_directory
            )

    def _translated_rows(self, private_dir: Path, count: int) -> list[dict[str, Any]]:
        return [
            _read_json(private_dir / "translate" / f"{index:06d}" / "artifact.json")
            for index in range(count)
        ]

    async def _tts_one(
        self,
        chunk: Any,
        private_dir: Path,
        manifest: PodcastManifest,
        state: dict[str, Any],
        store: ManifestStore,
        run_directory: Path,
    ) -> Path:
        item = manifest.stage("tts")["chunks"][
            int(chunk.sequence_id.rsplit("-", 1)[1]) - 1
        ]
        started = self._clock()
        attempt_identity = manifest.start_chunk("tts", item["identity"])
        store.save(manifest)
        try:
            try:
                result = await self._call_tts(chunk.text)
            except Exception as error:
                self._record_provider_failure("tts", item, error, manifest, store)
                raise

            path = _write_bytes_artifact_package(
                private_dir / "tts",
                item["index"],
                logical_identity=item["identity"],
                attempt_identity=attempt_identity,
                data=result.audio,
                metadata={"usage": result.usage, "voice": self.profile.voice},
            )
            manifest.commit_chunk(
                "tts",
                item["identity"],
                artifact_identity=fingerprint_file(path),
                artifact_path=path.relative_to(run_directory).as_posix(),
                size_bytes=path.stat().st_size,
            )
            store.save(manifest)
            return path
        finally:
            self._add_chunk_elapsed(
                "tts", item["identity"], started, state, run_directory
            )

    @staticmethod
    def _record_provider_failure(
        stage_name: str,
        item: Mapping[str, Any],
        error: Exception,
        manifest: PodcastManifest,
        store: ManifestStore,
    ) -> None:
        """Classify only errors raised by the provider adapter call itself."""

        if _is_safe_no_success_error(error):
            if item["attempts"] >= MAX_PROVIDER_ATTEMPTS:
                manifest.fail_chunk(stage_name, item["identity"])
            else:
                manifest.retry_chunk(stage_name, item["identity"])
        elif _is_definite_no_success_error(error):
            manifest.fail_chunk(stage_name, item["identity"])
        else:
            manifest.fail_stage(stage_name)
        store.save(manifest)

    async def _call_translation(self, text: str) -> Any:
        return await asyncio.to_thread(self.runtime.translator.translate, text)

    async def _call_tts(self, text: str) -> Any:
        return await asyncio.to_thread(self.runtime.tts.synthesize, text)

    @staticmethod
    def _tts_paths(private_dir: Path, count: int) -> list[Path]:
        return [
            private_dir / "tts" / f"{index:06d}" / "artifact.audio"
            for index in range(count)
        ]

    @staticmethod
    def _adopt_orphan_packages(
        private_dir: Path,
        manifest: PodcastManifest,
        store: ManifestStore,
        run_directory: Path,
    ) -> None:
        """Commit durable provider results left behind before manifest commit."""

        adopted = False
        artifact_names = {"translate": "artifact.json", "tts": "artifact.audio"}
        for stage_name in artifact_names:
            _discard_incomplete_artifact_packages(private_dir / stage_name)
        for stage_name, item in manifest.uncertain_chunks():
            try:
                path = _validate_artifact_package(
                    private_dir / stage_name,
                    item,
                    artifact_name=artifact_names[stage_name],
                )
            except PodcastPipelineError:
                continue
            if stage_name == "tts":
                try:
                    _validate_tts_audio(path)
                except PodcastPipelineError:
                    continue
            manifest.commit_chunk(
                stage_name,
                item["identity"],
                artifact_identity=fingerprint_file(path),
                artifact_path=path.relative_to(run_directory).as_posix(),
                size_bytes=path.stat().st_size,
            )
            adopted = True
        if adopted:
            store.save(manifest)

    def _finalize(
        self,
        audio_chunks: Sequence[Path],
        run_directory: Path,
        manifest: PodcastManifest,
        state: dict[str, Any],
        store: ManifestStore,
    ) -> FinalizedAudio:
        output = run_directory / DEFAULT_OUTPUT_NAME
        if manifest.stage("finalize")["status"] == "completed":
            _validate_artifact(
                output, manifest.stage("finalize")["artifact"], run_directory
            )
            return _probe_finalized(output)
        started = self._clock()
        manifest.start_stage("finalize")
        store.save(manifest)
        try:
            finalized = self.runtime.finalizer.finalize(audio_chunks, output)
            manifest.commit_stage(
                "finalize",
                artifact_identity=f"sha256:{finalized.fingerprint}",
                artifact_path=output.relative_to(run_directory).as_posix(),
                size_bytes=output.stat().st_size,
            )
            store.save(manifest)
            return finalized
        except Exception:
            manifest.fail_stage("finalize")
            store.save(manifest)
            raise
        finally:
            self._add_stage_elapsed("finalize", started, state, run_directory)

    def _write_report(
        self,
        run_directory: Path,
        manifest: PodcastManifest,
        state: Mapping[str, Any],
        finalized: FinalizedAudio,
        *,
        report_path: Path | None,
    ) -> Path:
        destination = (
            Path(report_path).expanduser().resolve()
            if report_path
            else Path(state["report_path"])
            if state.get("report_path")
            else run_directory / DEFAULT_REPORT_NAME
        )
        quality = {"status": "pending", "decided_at": None, "note": None}
        if manifest.run["status"] in {"accepted", "rejected"} and destination.is_file():
            existing = _read_json(destination)
            existing_quality = existing.get("listening_quality_gate")
            if isinstance(existing_quality, dict):
                quality = existing_quality
        report = build_report(
            run_id=manifest.run["identity"],
            run_status=manifest.run["status"],
            cache_mode=str(state["cache_mode"]),
            input_fingerprint=str(state["input_fingerprint"]),
            input_duration_ms=int(state["input_duration_ms"]),
            input_language=self.profile.source_language,
            profile={
                "id": self.profile.id,
                "fingerprint": self.profile.fingerprint,
                "target_language": self.profile.target_language,
                "region": self.profile.region,
                "providers": {
                    "asr": self.profile.asr_model,
                    "translation": self.profile.translation_model,
                    "tts": self.profile.tts_model,
                    "voice": self.profile.voice,
                },
            },
            stages=[
                self._report_stage(name, manifest, state, run_directory)
                for name in ("prepare", "asr", "translate", "tts", "finalize")
            ],
            output={
                "format": "mp3",
                "bitrate_kbps": finalized.bitrate_kbps,
                "channels": finalized.channels,
                "duration_ms": finalized.duration_ms,
                "fingerprint": f"sha256:{finalized.fingerprint}",
            },
            listening_quality_gate=quality,
            wall_clock_ms=int(state.get("wall_clock_ms", 0)) or None,
            timing_basis=str(state.get("timing_basis", "wall-clock")),
        )
        return write_report(report, destination)

    def _report_stage(
        self,
        name: str,
        manifest: PodcastManifest,
        state: Mapping[str, Any],
        run_directory: Path,
    ) -> dict[str, Any]:
        stage = manifest.stage(name)
        provider = {
            "prepare": "local",
            "asr": "alibaba",
            "translate": "alibaba",
            "tts": "alibaba",
            "finalize": "ffmpeg",
        }[name]
        chunks = [
            make_chunk(
                item["identity"],
                status="succeeded" if item["status"] == "committed" else "pending",
                attempts=item["attempts"],
                retries=min(item["retries"], max(item["attempts"] - 1, 0)),
                elapsed_ms=int(
                    state.get("chunk_elapsed_ms", {})
                    .get(name, {})
                    .get(item["identity"], 0)
                ),
                artifact_fingerprint=item["artifact"]["identity"]
                if item["artifact"]
                else None,
            )
            for item in stage["chunks"]
        ]
        retries = stage["retries"] + sum(item["retries"] for item in stage["chunks"])
        if name == "asr":
            retries += int(state.get("asr_poll_retries", 0))
        attempts = max(stage["attempts"], retries + 1 if retries else stage["attempts"])
        return make_stage(
            name,
            provider=provider,
            status="succeeded" if stage["status"] == "completed" else "pending",
            attempts=attempts,
            retries=retries,
            elapsed_ms=int(state.get("stage_elapsed_ms", {}).get(name, 0)),
            cost_unconfirmed_cny=_unconfirmed_stage_cost(name, run_directory),
            chunks=chunks,
            artifact_fingerprint=stage["artifact"]["identity"]
            if stage["artifact"]
            else None,
        )

    def _add_stage_elapsed(
        self, name: str, started: float, state: dict[str, Any], run_directory: Path
    ) -> None:
        elapsed = max(0, round((self._clock() - started) * 1000))
        timings = state.setdefault("stage_elapsed_ms", {})
        timings[name] = int(timings.get(name, 0)) + elapsed
        atomic_write_json(run_directory / PRIVATE_STATE_NAME, state)

    def _time_asr_operation(
        self, state: dict[str, Any], name: str, operation: Callable[[], Any]
    ) -> Any:
        """Measure one ASR operation, including failed provider calls."""

        started = self._clock()
        try:
            return operation()
        finally:
            elapsed = max(0, round((self._clock() - started) * 1000))
            timings = state.setdefault("asr_timing", dict(ASR_TIMING_DEFAULTS))
            for field, default in ASR_TIMING_DEFAULTS.items():
                timings.setdefault(field, default)
            timings[f"{name}_elapsed_ms"] = int(
                timings[f"{name}_elapsed_ms"]
            ) + elapsed
            timings[f"{name}_count"] = int(timings[f"{name}_count"]) + 1

    @staticmethod
    def _merge_asr_poll_diagnostics(state: dict[str, Any], diagnostics: Any) -> None:
        """Accumulate client subphases nested inside the inclusive poll measurement."""

        if diagnostics is None:
            return
        if isinstance(diagnostics, Mapping):
            values = diagnostics
        else:
            try:
                values = asdict(diagnostics)
            except TypeError:
                return
        timings = state.setdefault("asr_timing", dict(ASR_TIMING_DEFAULTS))
        for field, default in ASR_TIMING_DEFAULTS.items():
            timings.setdefault(field, default)
        for field in ASR_POLL_DURATION_FIELDS:
            value = values.get(field)
            if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                timings[field] = int(timings[field]) + value
        for field in ("provider_queue_elapsed_ms", "provider_task_elapsed_ms"):
            value = values.get(field)
            if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                timings[field] = value

    def _add_chunk_elapsed(
        self,
        stage: str,
        identity: str,
        started: float,
        state: dict[str, Any],
        run_directory: Path,
    ) -> None:
        elapsed = max(0, round((self._clock() - started) * 1000))
        stage_timings = state.setdefault("chunk_elapsed_ms", {}).setdefault(stage, {})
        stage_timings[identity] = int(stage_timings.get(identity, 0)) + elapsed
        atomic_write_json(run_directory / PRIVATE_STATE_NAME, state)


def probe_duration_ms(path: Path) -> int:
    """Read media duration with ffprobe without exposing the path in errors."""

    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                str(path),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        return max(0, round(float(result.stdout.strip()) * 1000))
    except (OSError, subprocess.SubprocessError, ValueError) as error:
        raise PodcastPipelineError(
            "input_probe_failed", "Could not read the input media duration"
        ) from error


def prepare_asr_audio(source: Path, destination: Path) -> Path:
    """Create the mono 16 kHz AAC input required for diarized whole-file ASR."""

    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.stem}.{os.getpid()}.partial.m4a")
    try:
        subprocess.run(
            [
                "ffmpeg",
                "-v",
                "error",
                "-y",
                "-i",
                str(source),
                "-vn",
                "-ac",
                "1",
                "-ar",
                "16000",
                "-c:a",
                "aac",
                "-b:a",
                "48k",
                str(temporary),
            ],
            check=True,
            capture_output=True,
        )
        if not temporary.is_file() or temporary.stat().st_size == 0:
            raise OSError("empty prepared audio")
        with temporary.open("rb") as handle:
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
        return destination
    except (OSError, subprocess.SubprocessError) as error:
        raise PodcastPipelineError(
            "input_prepare_failed", "Could not prepare mono audio for recognition"
        ) from error
    finally:
        temporary.unlink(missing_ok=True)


def _probe_finalized(path: Path) -> FinalizedAudio:
    if not path.is_file():
        raise PodcastPipelineError("output_missing", "Committed output file is missing")
    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-select_streams",
                "a:0",
                "-show_entries",
                "stream=sample_rate,channels,bit_rate:format=duration,bit_rate",
                "-of",
                "json",
                str(path),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        data = json.loads(result.stdout)
        return FinalizedAudio(
            path=path,
            fingerprint=fingerprint_file(path).removeprefix("sha256:"),
            duration_ms=round(float(data["format"]["duration"]) * 1000),
            bitrate_kbps=round(
                int(data["streams"][0].get("bit_rate") or data["format"]["bit_rate"])
                / 1000
            ),
            channels=int(data["streams"][0]["channels"]),
            sample_rate_hz=int(data["streams"][0]["sample_rate"]),
        )
    except (
        OSError,
        subprocess.SubprocessError,
        ValueError,
        KeyError,
        IndexError,
    ) as error:
        raise PodcastPipelineError(
            "output_probe_failed", "Could not validate the committed MP3"
        ) from error


def _read_json(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError) as error:
        raise PodcastPipelineError(
            "artifact_invalid", "A required run artifact is missing or invalid"
        ) from error
    if not isinstance(data, dict):
        raise PodcastPipelineError(
            "artifact_invalid", "A run artifact is not an object"
        )
    return data


def _complete_trace_segment_safely(trace: PerformanceTrace) -> dict[str, Any]:
    """Recover a partial completion and never append a duplicate completion."""

    recovered = recover_performance_trace(trace.path)
    for event in reversed(recovered.events):
        if event["process_segment"] < trace.process_segment:
            break
        if (
            event["process_segment"] == trace.process_segment
            and event["event"] == "completion"
        ):
            return event
    return trace.complete_segment()


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    if not data:
        raise PodcastPipelineError("empty_audio", "TTS returned an empty audio chunk")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _write_json_artifact_package(
    root: Path,
    index: int,
    *,
    logical_identity: str,
    attempt_identity: str,
    payload: Mapping[str, Any],
) -> Path:
    return _write_artifact_package(
        root,
        index,
        logical_identity=logical_identity,
        attempt_identity=attempt_identity,
        artifact_name="artifact.json",
        write=lambda path: atomic_write_json(path, payload),
    )


def _write_bytes_artifact_package(
    root: Path,
    index: int,
    *,
    logical_identity: str,
    attempt_identity: str,
    data: bytes,
    metadata: Mapping[str, Any],
) -> Path:
    return _write_artifact_package(
        root,
        index,
        logical_identity=logical_identity,
        attempt_identity=attempt_identity,
        artifact_name="artifact.audio",
        write=lambda path: _atomic_write_bytes(path, data),
        metadata=metadata,
        validate=_validate_tts_audio,
    )


def _write_artifact_package(
    root: Path,
    index: int,
    *,
    logical_identity: str,
    attempt_identity: str,
    artifact_name: str,
    write: Callable[[Path], None],
    metadata: Mapping[str, Any] | None = None,
    validate: Callable[[Path], None] | None = None,
) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    stable = root / f"{index:06d}"
    if stable.exists():
        raise PodcastPipelineError(
            "artifact_invalid", "A provider artifact package already exists"
        )
    temporary = Path(tempfile.mkdtemp(prefix=f".{index:06d}.", suffix=".tmp", dir=root))
    try:
        artifact = temporary / artifact_name
        write(artifact)
        if validate is not None:
            validate(artifact)
        identity = fingerprint_file(artifact)
        size_bytes = artifact.stat().st_size
        receipt = {
            "schema_version": 1,
            "logical_identity": logical_identity,
            "attempt_identity": attempt_identity,
            "artifact_name": artifact_name,
            "artifact_identity": identity,
            "artifact_size_bytes": size_bytes,
        }
        if metadata:
            receipt.update(dict(metadata))
        atomic_write_json(temporary / "receipt.json", receipt)
        _fsync_directory(temporary)
        os.replace(temporary, stable)
        _fsync_directory(root)
        return stable / artifact_name
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)


def _validate_artifact_package(
    root: Path,
    item: Mapping[str, Any],
    *,
    artifact_name: str,
) -> Path:
    package = root / f"{int(item['index']):06d}"
    receipt = _read_json(package / "receipt.json")
    artifact = package / artifact_name
    if (
        receipt.get("schema_version") != 1
        or receipt.get("logical_identity") != item.get("identity")
        or receipt.get("attempt_identity") != item.get("attempt_identity")
        or receipt.get("artifact_name") != artifact_name
        or not artifact.is_file()
        or receipt.get("artifact_size_bytes") != artifact.stat().st_size
        or receipt.get("artifact_identity") != fingerprint_file(artifact)
    ):
        raise PodcastPipelineError(
            "artifact_invalid", "A provider artifact package failed validation"
        )
    return artifact


def _validate_tts_audio(path: Path) -> None:
    """Reject a provider success whose final audio is not decodable."""

    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-select_streams",
                "a:0",
                "-show_entries",
                "stream=codec_type:format=duration",
                "-of",
                "json",
                str(path),
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
        probe = json.loads(result.stdout)
        streams = probe.get("streams", [])
        duration = float(probe.get("format", {}).get("duration", 0))
        if not streams or streams[0].get("codec_type") != "audio" or duration <= 0:
            raise ValueError("audio stream has no positive duration")
        subprocess.run(
            [
                "ffmpeg",
                "-nostdin",
                "-v",
                "error",
                "-i",
                str(path),
                "-map",
                "0:a:0",
                "-f",
                "null",
                "-",
            ],
            check=True,
            capture_output=True,
            timeout=30,
        )
    except (
        OSError,
        subprocess.SubprocessError,
        json.JSONDecodeError,
        TypeError,
        ValueError,
    ) as error:
        raise PodcastPipelineError(
            "invalid_audio", "TTS returned an invalid final audio artifact"
        ) from error


def _discard_incomplete_artifact_packages(root: Path) -> None:
    """Remove only private temporary package directories left by an interruption."""

    if not root.is_dir():
        return
    for candidate in root.glob(".*.tmp"):
        if (
            candidate.parent == root
            and candidate.name.startswith(".")
            and candidate.name.endswith(".tmp")
            and candidate.is_dir()
            and not candidate.is_symlink()
        ):
            shutil.rmtree(candidate)


def _fsync_directory(directory: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        descriptor = os.open(directory, flags)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)


def _text_sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _tts_chunk_source(chunk: TTSChunk, voice: str) -> dict[str, Any]:
    return {
        "sequence_id": chunk.sequence_id,
        "row_sequence_ids": chunk.row_sequence_ids,
        "text_sha256": _text_sha256(chunk.text),
        "voice": voice,
    }


def _translation_dependency_range(chunk: TTSChunk) -> tuple[int, int]:
    try:
        indices = [
            int(sequence_id.rsplit("-", 1)[1]) - 1
            for sequence_id in chunk.row_sequence_ids
        ]
    except (IndexError, ValueError) as error:
        raise PodcastPipelineError(
            "artifact_invalid", "Synthesis chunk has an invalid translation dependency"
        ) from error
    if not indices or min(indices) < 0:
        raise PodcastPipelineError(
            "artifact_invalid", "Synthesis chunk has no translation dependency"
        )
    return min(indices), max(indices)


def _is_safe_no_success_error(error: BaseException) -> bool:
    if bool(getattr(error, "submission_may_have_succeeded", False)):
        return False
    if not bool(getattr(error, "retryable", False)):
        return False
    status = getattr(error, "status_code", None)
    if isinstance(status, int):
        return status in (408, 429) or 500 <= status <= 599
    code = str(getattr(error, "code", "")).lower()
    return any(marker in code for marker in ("throttl", "rate_limit", "ratelimit"))


def _is_definite_no_success_error(error: BaseException) -> bool:
    if bool(getattr(error, "submission_may_have_succeeded", False)):
        return False
    return isinstance(error, ValueError) or (
        hasattr(error, "retryable") and not bool(getattr(error, "retryable"))
    )


def _paths_identity(paths: Sequence[Path]) -> str:
    digest = hashlib.sha256()
    for path in paths:
        digest.update(fingerprint_file(path).encode("ascii"))
    return f"sha256:{digest.hexdigest()}"


def _validate_artifact(
    path: Path, artifact: Mapping[str, Any] | None, run_directory: Path
) -> None:
    if artifact is None or not path.is_file():
        raise PodcastPipelineError(
            "artifact_invalid", "A committed run artifact is missing"
        )
    try:
        expected_path = artifact.get("path")
        if (
            expected_path is not None
            and path.relative_to(run_directory).as_posix() != expected_path
        ):
            raise ValueError
        expected_size = artifact.get("size_bytes")
        if expected_size is not None and path.stat().st_size != expected_size:
            raise ValueError
        if fingerprint_file(path) != artifact.get("identity"):
            raise ValueError
    except (OSError, ValueError) as error:
        raise PodcastPipelineError(
            "artifact_invalid", "A committed run artifact failed validation"
        ) from error


def _validate_tts_artifact(
    item: Mapping[str, Any], private_dir: Path, run_directory: Path
) -> None:
    audio_path = _validate_artifact_package(
        private_dir / "tts", item, artifact_name="artifact.audio"
    )
    _validate_tts_audio(audio_path)
    _validate_artifact(audio_path, item.get("artifact"), run_directory)


def _unconfirmed_stage_cost(name: str, run_directory: Path) -> float:
    """Estimate Beijing list-price cost from persisted numeric usage only."""

    private_dir = run_directory / "private"
    if name == "asr":
        usage = _read_json(private_dir / "asr.json").get("usage_duration_seconds")
        return round(float(usage or 0) * ASR_CNY_PER_SECOND, 8)
    if name == "translate":
        units = sum(
            float(_read_json(path).get("usage") or 0)
            for path in sorted((private_dir / "translate").glob("*/artifact.json"))
        )
        # The adapter currently records total tokens. Charging every token at
        # the higher output rate intentionally makes this a conservative estimate.
        return round(
            units * TRANSLATION_CONSERVATIVE_CNY_PER_MILLION_TOKENS / 1_000_000,
            8,
        )
    if name == "tts":
        characters = sum(
            float(_read_json(path).get("usage") or 0)
            for path in sorted((private_dir / "tts").glob("*/receipt.json"))
        )
        return round(
            characters * TTS_CNY_PER_TEN_THOUSAND_CHARACTERS / 10_000,
            8,
        )
    return 0.0


def _validate_new_report_destination(
    source: Path, run_directory: Path, report_path: Path
) -> Path:
    destination = report_path.expanduser().resolve()
    private_directory = run_directory / "private"
    reserved = {
        source,
        run_directory / MANIFEST_NAME,
        run_directory / f"{MANIFEST_NAME}.lock",
        run_directory / PRIVATE_STATE_NAME,
        run_directory / DEFAULT_OUTPUT_NAME,
        run_directory / "benchmark-summary.json",
    }
    try:
        inside_private = destination.is_relative_to(private_directory)
    except AttributeError:  # pragma: no cover - Python < 3.9 compatibility
        inside_private = (
            private_directory == destination or private_directory in destination.parents
        )
    if destination.exists() or destination in reserved or inside_private:
        raise PodcastPipelineError(
            "report_path_unsafe",
            "New report path must be unused and outside private/reserved artifacts",
        )
    return destination


def _resolve_report_destination(
    run_directory: Path,
    state: Mapping[str, Any],
    requested: Path | None,
) -> Path:
    saved = (
        Path(str(state["report_path"])).expanduser().resolve()
        if state.get("report_path")
        else run_directory / DEFAULT_REPORT_NAME
    )
    if requested is not None and requested.expanduser().resolve() != saved:
        raise PodcastPipelineError(
            "report_path_mismatch",
            "Resume and review must use the report path saved by the run",
        )
    return saved


def load_terminal_run(
    run_directory: Path,
    profile: PodcastProfile,
    *,
    report_path: Path | None = None,
) -> PodcastRunResult | None:
    """Load an accepted/rejected run without constructing cloud providers."""

    run_directory = run_directory.expanduser().resolve()
    state = _read_json(run_directory / PRIVATE_STATE_NAME)
    if (
        state.get("profile_id") != profile.id
        or state.get("profile_fingerprint") != profile.fingerprint
    ):
        raise PodcastPipelineError(
            "profile_mismatch", "Saved run uses a different production profile"
        )
    store = ManifestStore(run_directory / MANIFEST_NAME, lock_timeout=0)
    with store.run_lock():
        manifest = store.load()
        if manifest.run["status"] not in {"accepted", "rejected"}:
            return None
        destination = _resolve_report_destination(run_directory, state, report_path)
        report = _read_json(destination)
        validate_report(report)
        if report["run"]["status"] != manifest.run["status"]:
            raise PodcastPipelineError(
                "review_state_mismatch",
                "Manifest and production report review states do not match",
            )
        output = run_directory / DEFAULT_OUTPUT_NAME
        _validate_artifact(
            output, manifest.stage("finalize")["artifact"], run_directory
        )
        return PodcastRunResult(
            run_directory=run_directory,
            output_path=output,
            report_path=destination,
            status=manifest.run["status"],
        )


__all__ = [
    "DEFAULT_OUTPUT_NAME",
    "DEFAULT_REPORT_NAME",
    "PodcastCoordinator",
    "PodcastPipelineError",
    "PodcastRunResult",
    "PodcastRuntime",
    "load_terminal_run",
    "prepare_asr_audio",
    "probe_duration_ms",
]
