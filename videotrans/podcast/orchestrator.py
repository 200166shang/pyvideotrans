"""Ordered, resumable coordinator for English-to-Mandarin podcast runs."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import subprocess
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, Sequence

from .alibaba_asr import AlibabaWholeFileAsrClient, AsrPollResult
from .alibaba_text import AlibabaTTSAdapter, AlibabaTranslationAdapter
from .chunking import PodcastTextRow, chunk_translation_rows, chunk_tts_rows
from .finalize import FinalizedAudio, Mp3Finalizer
from .manifest import ManifestStore, PodcastManifest, atomic_write_json
from .profiles import PodcastProfile
from .report import build_report, fingerprint_file, make_chunk, make_stage, write_report
from .scheduler import AsyncScheduler


PRIVATE_STATE_NAME = "run.private.json"
MANIFEST_NAME = "manifest.json"
DEFAULT_REPORT_NAME = "production-report.json"
DEFAULT_OUTPUT_NAME = "podcast.zh-CN.mp3"


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


class PodcastCoordinator:
    """Own all durable transitions for the five-stage production pipeline."""

    def __init__(
        self,
        runtime: PodcastRuntime,
        profile: PodcastProfile,
        *,
        clock: Callable[[], float] = time.perf_counter,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        self.runtime = runtime
        self.profile = profile
        self._clock = clock
        self._sleeper = sleeper

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

        with store.run_lock():
            if store.path.exists():
                raise PodcastPipelineError(
                    "run_already_exists",
                    "Run directory already contains a manifest; use --resume",
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
                "cache_mode": cache_mode,
                "report_path": str(Path(report_path).expanduser().resolve())
                if report_path
                else None,
                "input_fingerprint": input_fingerprint,
                "input_duration_ms": duration_ms,
                "stage_elapsed_ms": {},
                "chunk_elapsed_ms": {"translate": {}, "tts": {}},
            }
            atomic_write_json(run_directory / PRIVATE_STATE_NAME, state)
            store.save(manifest)
            return self._execute(run_directory, manifest, state, store, report_path)

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
            if state.get("profile_id") != self.profile.id:
                raise PodcastPipelineError(
                    "profile_mismatch", "Saved run uses a different production profile"
                )
            manifest = store.load(resume=True)
            return self._execute(run_directory, manifest, state, store, report_path)

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

        self._prepare(source, private_dir, manifest, state, store, run_directory)
        segments = self._asr(source, private_dir, manifest, state, store, run_directory)
        translated = self._translate(
            segments, private_dir, manifest, state, store, run_directory
        )
        audio_chunks = self._tts(
            translated, private_dir, manifest, state, store, run_directory
        )
        finalized = self._finalize(
            audio_chunks, run_directory, manifest, state, store
        )
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
    ) -> None:
        if manifest.stage("prepare")["status"] == "completed":
            return
        if not source.is_file():
            raise PodcastPipelineError(
                "input_not_found", "Original input is required to resume this stage"
            )
        self._timed_stage(
            "prepare",
            manifest,
            state,
            store,
            run_directory,
            lambda: self._commit_json_stage(
                "prepare",
                private_dir / "prepare.json",
                {
                    "input_fingerprint": state["input_fingerprint"],
                    "duration_ms": state["input_duration_ms"],
                },
                manifest,
                store,
                run_directory,
            ),
        )

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
                audio_url = self.runtime.upload_audio(source)
                submitted = self.runtime.asr.submit(
                    audio_url, language=self.profile.source_language
                )
                manifest.set_asr_task_id(submitted.task_id)
                store.save(manifest)
                task_id = submitted.task_id
            result = self._wait_for_asr(task_id)
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

    def _wait_for_asr(self, task_id: str) -> AsrPollResult:
        transient_failures = 0
        poll_attempt = 0
        while True:
            try:
                result = self.runtime.asr.poll(task_id)
            except Exception as error:
                if not getattr(error, "retryable", False) or transient_failures >= 5:
                    raise
                delay = min(30.0, 2.0**transient_failures)
                transient_failures += 1
                self._sleeper(delay)
                continue
            if result.is_complete:
                return result
            self._sleeper(self.runtime.asr.polling_policy.delay_for_attempt(poll_attempt))
            poll_attempt += 1

    def _translate(
        self,
        segments: Sequence[Mapping[str, Any]],
        private_dir: Path,
        manifest: PodcastManifest,
        state: dict[str, Any],
        store: ManifestStore,
        run_directory: Path,
    ) -> list[dict[str, Any]]:
        aggregate_path = private_dir / "translate.json"
        chunks = chunk_translation_rows(
            segments,
            min_token_units=self.profile.translation_chunk_min,
            max_token_units=self.profile.translation_chunk_max,
        )
        sources = [
            {
                "sequence_id": chunk.sequence_id,
                "row_sequence_ids": chunk.row_sequence_ids,
                "text_sha256": _text_sha256(chunk.text),
            }
            for chunk in chunks
        ]
        manifest.define_chunks("translate", sources)
        store.save(manifest)
        pending = [
            chunks[item["index"]]
            for item in manifest.stage("translate")["chunks"]
            if item["status"] != "committed"
        ]
        if pending:
            started = self._clock()
            manifest.start_stage("translate")
            store.save(manifest)
            scheduler = AsyncScheduler(
                concurrency=self.profile.translation_concurrency,
                requests_per_minute=60_000,
                initial_tokens=1,
            )

            async def operation(chunk: Any) -> dict[str, Any]:
                return await self._translate_one(
                    chunk, private_dir, manifest, state, store, run_directory
                )

            try:
                asyncio.run(scheduler.run(pending, operation))
            finally:
                self._add_stage_elapsed("translate", started, state, run_directory)
        if manifest.stage("translate")["artifact"] is None:
            rows = self._translated_rows(private_dir, len(chunks))
            atomic_write_json(aggregate_path, {"rows": rows})
            manifest.commit_stage(
                "translate",
                artifact_identity=fingerprint_file(aggregate_path),
                artifact_path=aggregate_path.relative_to(run_directory).as_posix(),
                size_bytes=aggregate_path.stat().st_size,
            )
            store.save(manifest)
        return list(_read_json(aggregate_path)["rows"])

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
        manifest.start_chunk("translate", item["identity"])
        store.save(manifest)
        try:
            result = await asyncio.to_thread(self.runtime.translator.translate, chunk.text)
            speakers = chunk.speaker_ids
            payload = {
                "sequence_id": chunk.sequence_id,
                "translated_text": result.text,
                "speaker_id": speakers[0] if len(speakers) == 1 else None,
                "row_sequence_ids": list(chunk.row_sequence_ids),
                "usage": result.usage,
            }
            path = private_dir / "translate" / f"{item['index']:06d}.json"
            atomic_write_json(path, payload)
            manifest.commit_chunk(
                "translate",
                item["identity"],
                artifact_identity=fingerprint_file(path),
                artifact_path=path.relative_to(run_directory).as_posix(),
                size_bytes=path.stat().st_size,
            )
            store.save(manifest)
            return payload
        except Exception:
            manifest.fail_chunk("translate", item["identity"])
            store.save(manifest)
            raise
        finally:
            self._add_chunk_elapsed(
                "translate", item["identity"], started, state, run_directory
            )

    def _translated_rows(self, private_dir: Path, count: int) -> list[dict[str, Any]]:
        return [
            _read_json(private_dir / "translate" / f"{index:06d}.json")
            for index in range(count)
        ]

    def _tts(
        self,
        translated: Sequence[Mapping[str, Any]],
        private_dir: Path,
        manifest: PodcastManifest,
        state: dict[str, Any],
        store: ManifestStore,
        run_directory: Path,
    ) -> list[Path]:
        rows = [
            PodcastTextRow(
                sequence_id=str(row["sequence_id"]),
                text=str(row["translated_text"]),
                speaker_id=row.get("speaker_id"),
            )
            for row in translated
        ]
        chunks = chunk_tts_rows(rows, max_characters=self.profile.tts_soft_char_limit)
        sources = [
            {
                "sequence_id": chunk.sequence_id,
                "row_sequence_ids": chunk.row_sequence_ids,
                "text_sha256": _text_sha256(chunk.text),
                "voice": self.profile.voice,
            }
            for chunk in chunks
        ]
        manifest.define_chunks("tts", sources)
        store.save(manifest)
        if not chunks:
            raise PodcastPipelineError(
                "no_speech", "The transcription did not produce speech to synthesize"
            )
        pending = [
            chunks[item["index"]]
            for item in manifest.stage("tts")["chunks"]
            if item["status"] != "committed"
        ]
        if pending:
            started = self._clock()
            manifest.start_stage("tts")
            store.save(manifest)
            scheduler = AsyncScheduler(
                concurrency=self.profile.tts_concurrency,
                requests_per_minute=self.profile.tts_rpm,
                initial_tokens=0,
            )

            async def operation(chunk: Any) -> Path:
                return await self._tts_one(
                    chunk, private_dir, manifest, state, store, run_directory
                )

            try:
                asyncio.run(scheduler.run(pending, operation))
            finally:
                self._add_stage_elapsed("tts", started, state, run_directory)
        if manifest.stage("tts")["artifact"] is None:
            manifest.commit_stage(
                "tts",
                artifact_identity=_paths_identity(self._tts_paths(private_dir, len(chunks))),
            )
            store.save(manifest)
        return self._tts_paths(private_dir, len(chunks))

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
        manifest.start_chunk("tts", item["identity"])
        store.save(manifest)
        try:
            result = await asyncio.to_thread(self.runtime.tts.synthesize, chunk.text)
            path = private_dir / "tts" / f"{item['index']:06d}.audio"
            _atomic_write_bytes(path, result.audio)
            atomic_write_json(
                path.with_suffix(".json"),
                {"usage": result.usage, "voice": self.profile.voice},
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
        except Exception:
            manifest.fail_chunk("tts", item["identity"])
            store.save(manifest)
            raise
        finally:
            self._add_chunk_elapsed("tts", item["identity"], started, state, run_directory)

    @staticmethod
    def _tts_paths(private_dir: Path, count: int) -> list[Path]:
        return [private_dir / "tts" / f"{index:06d}.audio" for index in range(count)]

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
        report = build_report(
            run_id=manifest.run["identity"],
            run_status="awaiting_review",
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
                self._report_stage(name, manifest, state)
                for name in ("prepare", "asr", "translate", "tts", "finalize")
            ],
            output={
                "format": "mp3",
                "bitrate_kbps": finalized.bitrate_kbps,
                "channels": finalized.channels,
                "duration_ms": finalized.duration_ms,
                "fingerprint": f"sha256:{finalized.fingerprint}",
            },
        )
        return write_report(report, destination)

    def _report_stage(
        self, name: str, manifest: PodcastManifest, state: Mapping[str, Any]
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
                    state.get("chunk_elapsed_ms", {}).get(name, {}).get(item["identity"], 0)
                ),
                artifact_fingerprint=item["artifact"]["identity"]
                if item["artifact"]
                else None,
            )
            for item in stage["chunks"]
        ]
        return make_stage(
            name,
            provider=provider,
            status="succeeded" if stage["status"] == "completed" else "pending",
            attempts=stage["attempts"],
            retries=min(stage["retries"], max(stage["attempts"] - 1, 0)),
            elapsed_ms=int(state.get("stage_elapsed_ms", {}).get(name, 0)),
            chunks=chunks,
            artifact_fingerprint=stage["artifact"]["identity"]
            if stage["artifact"]
            else None,
        )

    def _timed_stage(
        self,
        name: str,
        manifest: PodcastManifest,
        state: dict[str, Any],
        store: ManifestStore,
        run_directory: Path,
        operation: Callable[[], None],
    ) -> None:
        started = self._clock()
        manifest.start_stage(name)
        store.save(manifest)
        try:
            operation()
        except Exception:
            manifest.fail_stage(name)
            store.save(manifest)
            raise
        finally:
            self._add_stage_elapsed(name, started, state, run_directory)

    @staticmethod
    def _commit_json_stage(
        name: str,
        path: Path,
        payload: Mapping[str, Any],
        manifest: PodcastManifest,
        store: ManifestStore,
        run_directory: Path,
    ) -> None:
        atomic_write_json(path, payload)
        manifest.commit_stage(
            name,
            artifact_identity=fingerprint_file(path),
            artifact_path=path.relative_to(run_directory).as_posix(),
            size_bytes=path.stat().st_size,
        )
        store.save(manifest)

    def _add_stage_elapsed(
        self, name: str, started: float, state: dict[str, Any], run_directory: Path
    ) -> None:
        elapsed = max(0, round((self._clock() - started) * 1000))
        timings = state.setdefault("stage_elapsed_ms", {})
        timings[name] = int(timings.get(name, 0)) + elapsed
        atomic_write_json(run_directory / PRIVATE_STATE_NAME, state)

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
                "stream=sample_rate,channels:format=duration,bit_rate",
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
            bitrate_kbps=round(int(data["format"]["bit_rate"]) / 1000),
            channels=int(data["streams"][0]["channels"]),
            sample_rate_hz=int(data["streams"][0]["sample_rate"]),
        )
    except (OSError, subprocess.SubprocessError, ValueError, KeyError, IndexError) as error:
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
        raise PodcastPipelineError("artifact_invalid", "A run artifact is not an object")
    return data


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


def _text_sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _paths_identity(paths: Sequence[Path]) -> str:
    digest = hashlib.sha256()
    for path in paths:
        digest.update(fingerprint_file(path).encode("ascii"))
    return f"sha256:{digest.hexdigest()}"


__all__ = [
    "DEFAULT_OUTPUT_NAME",
    "DEFAULT_REPORT_NAME",
    "PodcastCoordinator",
    "PodcastPipelineError",
    "PodcastRunResult",
    "PodcastRuntime",
    "probe_duration_ms",
]
