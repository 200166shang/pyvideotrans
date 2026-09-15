from __future__ import annotations

import hashlib
import io
import json
import shutil
import struct
import subprocess
import threading
import wave
from dataclasses import replace
from pathlib import Path

import pytest

import videotrans.podcast.orchestrator as podcast_orchestrator
from videotrans.podcast.alibaba_asr import (
    AsrPollResult,
    AsrUsage,
    PollingPolicy,
    SubmittedAsrTask,
    TranscriptSegment,
)
from videotrans.podcast.alibaba_text import (
    SpeechResult,
    TransientAlibabaError,
    TranslationResult,
)
from videotrans.podcast.cli_runner import _record_review
from videotrans.podcast.finalize import FinalizedAudio
from videotrans.podcast.manifest import ManifestStore
from videotrans.podcast.orchestrator import (
    PodcastCoordinator,
    PodcastPipelineError,
    PodcastRuntime,
    prepare_asr_audio,
)
from videotrans.podcast.profiles import ALIBABA_PODCAST_V2


class FakeAsr:
    def __init__(self, text: str) -> None:
        self.text = text
        self.submit_calls = 0
        self.poll_calls = 0
        self.submit_failure: BaseException | None = None
        self.polling_policy = PollingPolicy()

    def submit(self, audio_url: str, *, language: str) -> SubmittedAsrTask:
        assert audio_url == "oss://temporary/input.m4a"
        assert language == "en"
        self.submit_calls += 1
        if self.submit_failure is not None:
            raise self.submit_failure
        return SubmittedAsrTask("task-1", "PENDING")

    def poll(self, task_id: str) -> AsrPollResult:
        assert task_id == "task-1"
        self.poll_calls += 1
        return AsrPollResult(
            task_id,
            "SUCCEEDED",
            segments=(TranscriptSegment(0, 1000, self.text, "speaker-1", 0),),
            usage=AsrUsage(duration_seconds=1.0),
        )


class FakeTranslator:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def translate(self, text: str) -> TranslationResult:
        self.calls.append(text)
        return TranslationResult(text=text, usage=len(text), request_id=None)


class FakeTts:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.fail_on_call: int | None = None
        self.failure: Exception = ValueError("offline permanent failure")

    def synthesize(self, text: str) -> SpeechResult:
        self.calls.append(text)
        if self.fail_on_call == len(self.calls):
            raise self.failure
        return SpeechResult(audio=_fake_wav(), usage=len(text), request_id=None)


def _fake_wav(*, frames: int = 160, streaming_header: bool = False) -> bytes:
    output = io.BytesIO()
    with wave.open(output, "wb") as audio:
        audio.setnchannels(1)
        audio.setsampwidth(2)
        audio.setframerate(16_000)
        audio.writeframes(b"\0\0" * frames)
    result = bytearray(output.getvalue())
    if streaming_header:
        struct.pack_into("<I", result, 4, 0x7FFFFFBF)
        struct.pack_into("<I", result, 40, 0x7FFFFFDB)
    return bytes(result)


class FakeFinalizer:
    def __init__(self) -> None:
        self.calls = 0

    def finalize(self, chunks: list[Path], output: Path) -> FinalizedAudio:
        self.calls += 1
        output.write_bytes(b"ID3" + b"".join(path.read_bytes() for path in chunks))
        return FinalizedAudio(
            path=output,
            fingerprint=hashlib.sha256(output.read_bytes()).hexdigest(),
            duration_ms=1234,
            bitrate_kbps=64,
            channels=1,
            sample_rate_hz=48000,
        )


def make_coordinator(
    text: str,
) -> tuple[PodcastCoordinator, FakeAsr, FakeTranslator, FakeTts, FakeFinalizer]:
    asr = FakeAsr(text)
    translator = FakeTranslator()
    tts = FakeTts()
    finalizer = FakeFinalizer()
    runtime = PodcastRuntime(
        asr=asr,
        translator=translator,
        tts=tts,
        upload_audio=lambda _: "oss://temporary/input.m4a",
        finalizer=finalizer,
    )
    profile = replace(
        ALIBABA_PODCAST_V2,
        translation_concurrency=1,
        translation_rpm=60_000_000,
        tts_concurrency=1,
        tts_rpm=60_000_000,
    )
    return PodcastCoordinator(runtime, profile), asr, translator, tts, finalizer


@pytest.fixture(autouse=True)
def fake_prepare_audio(monkeypatch):
    def prepare(source: Path, destination: Path) -> Path:
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(source.read_bytes())
        return destination

    monkeypatch.setattr("videotrans.podcast.orchestrator.prepare_asr_audio", prepare)


def test_full_run_writes_mp3_manifest_and_privacy_safe_report(
    tmp_path, monkeypatch
) -> None:
    source = tmp_path / "source.m4a"
    source.write_bytes(b"authorized input")
    run_dir = tmp_path / "run"
    monkeypatch.setattr(
        "videotrans.podcast.orchestrator.probe_duration_ms", lambda _: 1000
    )
    coordinator, asr, translator, tts, finalizer = make_coordinator("A short episode.")

    result = coordinator.create(source, run_dir)

    assert result.status == "awaiting_review"
    assert result.output_path.read_bytes().startswith(b"ID3")
    assert asr.submit_calls == 1
    assert translator.calls == ["A short episode."]
    assert tts.calls == ["A short episode."]
    assert finalizer.calls == 1
    manifest = ManifestStore(run_dir / "manifest.json").load()
    assert manifest.run["status"] == "awaiting_review"
    report = json.loads(result.report_path.read_text(encoding="utf-8"))
    assert report["profile"]["providers"]["voice"] == "Andre"
    assert report["totals"]["cost_unconfirmed_cny"] > 0
    assert report["privacy"] == {
        "contains_credentials": False,
        "contains_transcript": False,
        "contains_absolute_source_path": False,
    }
    serialized = result.report_path.read_text(encoding="utf-8")
    assert str(source) not in serialized
    assert "A short episode" not in serialized


def test_tts_starts_before_later_translation_finishes(tmp_path, monkeypatch) -> None:
    second_translation_started = threading.Event()
    release_second_translation = threading.Event()
    tts_started = threading.Event()

    class BlockingTranslator(FakeTranslator):
        def __init__(self) -> None:
            super().__init__()
            self._lock = threading.Lock()

        def translate(self, text: str) -> TranslationResult:
            with self._lock:
                self.calls.append(text)
                call_number = len(self.calls)
            if call_number == 2:
                second_translation_started.set()
                assert release_second_translation.wait(5)
            return TranslationResult(text=text, usage=len(text), request_id=None)

    class SignallingTts(FakeTts):
        def synthesize(self, text: str) -> SpeechResult:
            tts_started.set()
            return super().synthesize(text)

    source = tmp_path / "source.m4a"
    source.write_bytes(b"authorized input")
    run_dir = tmp_path / "run"
    monkeypatch.setattr(
        "videotrans.podcast.orchestrator.probe_duration_ms", lambda _: 1000
    )
    asr = FakeAsr("A" * 5000)
    translator = BlockingTranslator()
    tts = SignallingTts()
    finalizer = FakeFinalizer()
    profile = replace(
        ALIBABA_PODCAST_V2,
        translation_rpm=60_000_000,
        tts_rpm=60_000_000,
    )
    coordinator = PodcastCoordinator(
        PodcastRuntime(
            asr=asr,
            translator=translator,
            tts=tts,
            upload_audio=lambda _: "oss://temporary/input.m4a",
            finalizer=finalizer,
        ),
        profile,
    )
    outcome: list[object] = []

    def run() -> None:
        try:
            outcome.append(coordinator.create(source, run_dir))
        except BaseException as error:
            outcome.append(error)

    thread = threading.Thread(target=run)
    thread.start()
    assert second_translation_started.wait(5)
    assert tts_started.wait(5), "TTS did not overlap the blocked translation"
    release_second_translation.set()
    thread.join(10)

    assert not thread.is_alive()
    assert len(outcome) == 1
    assert not isinstance(outcome[0], BaseException)


def test_translation_failure_stops_new_tts_launches_while_in_flight_tts_drains(
    tmp_path, monkeypatch
) -> None:
    tts_started = threading.Event()
    translation_failed = threading.Event()
    release_tts = threading.Event()

    class FailingTranslator(FakeTranslator):
        def translate(self, text: str) -> TranslationResult:
            self.calls.append(text)
            if len(self.calls) == 2:
                assert tts_started.wait(5)
                translation_failed.set()
                raise ValueError("permanent translation failure")
            return TranslationResult(text="中" * 500, usage=len(text), request_id=None)

    class DrainingTts(FakeTts):
        def synthesize(self, text: str) -> SpeechResult:
            self.calls.append(text)
            tts_started.set()
            assert release_tts.wait(5)
            return SpeechResult(audio=_fake_wav(), usage=len(text), request_id=None)

    source = tmp_path / "source.m4a"
    source.write_bytes(b"authorized input")
    run_dir = tmp_path / "run"
    monkeypatch.setattr(
        "videotrans.podcast.orchestrator.probe_duration_ms", lambda _: 1000
    )
    asr = FakeAsr("A" * 9_000)
    translator = FailingTranslator()
    tts = DrainingTts()
    finalizer = FakeFinalizer()
    profile = replace(
        ALIBABA_PODCAST_V2,
        translation_concurrency=1,
        translation_rpm=60_000_000,
        tts_concurrency=1,
        tts_rpm=60_000_000,
    )
    coordinator = PodcastCoordinator(
        PodcastRuntime(
            asr=asr,
            translator=translator,
            tts=tts,
            upload_audio=lambda _: "oss://temporary/input.m4a",
            finalizer=finalizer,
        ),
        profile,
    )
    outcome: list[BaseException] = []

    def run() -> None:
        try:
            coordinator.create(source, run_dir)
        except BaseException as error:
            outcome.append(error)

    thread = threading.Thread(target=run)
    thread.start()
    assert translation_failed.wait(5)
    release_tts.set()
    thread.join(10)

    assert not thread.is_alive()
    assert len(outcome) == 1
    assert isinstance(outcome[0], ValueError)
    assert len(tts.calls) == 1


def test_create_rejects_nonempty_directory_without_manifest(tmp_path) -> None:
    source = tmp_path / "source.m4a"
    source.write_bytes(b"authorized input")
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    existing = run_dir / "keep.txt"
    existing.write_text("user data", encoding="utf-8")
    coordinator, _, _, _, _ = make_coordinator("A short episode.")

    with pytest.raises(PodcastPipelineError, match="not empty"):
        coordinator.create(source, run_dir)

    assert existing.read_text(encoding="utf-8") == "user data"
    assert not (run_dir / "manifest.json").exists()


def test_create_rejects_existing_external_report(tmp_path) -> None:
    source = tmp_path / "source.m4a"
    source.write_bytes(b"authorized input")
    run_dir = tmp_path / "run"
    report = tmp_path / "existing-report.json"
    report.write_text("keep", encoding="utf-8")
    coordinator, _, _, _, _ = make_coordinator("A short episode.")

    with pytest.raises(PodcastPipelineError, match="must be unused"):
        coordinator.create(source, run_dir, report_path=report)

    assert report.read_text(encoding="utf-8") == "keep"


@pytest.mark.parametrize(
    "reserved_name", ["manifest.json.lock", "benchmark-summary.json"]
)
def test_create_rejects_reserved_report_path(tmp_path, reserved_name) -> None:
    source = tmp_path / "source.m4a"
    source.write_bytes(b"authorized input")
    run_dir = tmp_path / "run"
    coordinator, _, _, _, _ = make_coordinator("A short episode.")

    with pytest.raises(PodcastPipelineError, match="outside private/reserved"):
        coordinator.create(source, run_dir, report_path=run_dir / reserved_name)


def test_resume_rejects_report_rebinding(tmp_path, monkeypatch) -> None:
    source = tmp_path / "source.m4a"
    source.write_bytes(b"authorized input")
    run_dir = tmp_path / "run"
    report = tmp_path / "report.json"
    monkeypatch.setattr(
        "videotrans.podcast.orchestrator.probe_duration_ms", lambda _: 1000
    )
    coordinator, _, _, _, _ = make_coordinator("A short episode.")
    coordinator.create(source, run_dir, report_path=report)

    with pytest.raises(PodcastPipelineError, match="report path saved by the run"):
        coordinator.resume(run_dir, report_path=tmp_path / "different.json")


def test_resume_keeps_committed_tts_chunks_and_reuses_asr_task(
    tmp_path, monkeypatch
) -> None:
    source = tmp_path / "source.m4a"
    source.write_bytes(b"authorized input")
    run_dir = tmp_path / "run"
    monkeypatch.setattr(
        "videotrans.podcast.orchestrator.probe_duration_ms", lambda _: 1000
    )
    coordinator, asr, _, tts, finalizer = make_coordinator(
        "甲" * 500 + "乙" * 500 + "丙" * 100
    )
    tts.fail_on_call = 2

    with pytest.raises(ValueError, match="offline permanent failure"):
        coordinator.create(source, run_dir)

    manifest = ManifestStore(run_dir / "manifest.json").load()
    assert manifest.stage("tts")["chunks"][0]["status"] == "committed"
    first_text = tts.calls[0]
    tts.fail_on_call = None

    result = coordinator.resume(run_dir)

    assert result.status == "awaiting_review"
    state = json.loads((run_dir / "run.private.json").read_text(encoding="utf-8"))
    assert state["timing_basis"] == "active-process"
    assert asr.submit_calls == 1
    assert tts.calls.count(first_text) == 1
    assert finalizer.calls == 1


def test_resume_never_repeats_uncertain_asr_submission(tmp_path, monkeypatch) -> None:
    class SubmissionCrash(BaseException):
        pass

    source = tmp_path / "source.m4a"
    source.write_bytes(b"authorized input")
    run_dir = tmp_path / "run"
    monkeypatch.setattr(
        "videotrans.podcast.orchestrator.probe_duration_ms", lambda _: 1000
    )
    coordinator, asr, _, _, _ = make_coordinator("A short episode.")
    asr.submit_failure = SubmissionCrash()

    with pytest.raises(SubmissionCrash):
        coordinator.create(source, run_dir)

    asr.submit_failure = None
    with pytest.raises(PodcastPipelineError, match="may have been accepted"):
        coordinator.resume(run_dir)

    assert asr.submit_calls == 1


def test_resume_rejects_corrupted_committed_artifact(tmp_path, monkeypatch) -> None:
    source = tmp_path / "source.m4a"
    source.write_bytes(b"authorized input")
    run_dir = tmp_path / "run"
    monkeypatch.setattr(
        "videotrans.podcast.orchestrator.probe_duration_ms", lambda _: 1000
    )
    coordinator, _, _, _, _ = make_coordinator("A short episode.")
    coordinator.create(source, run_dir)
    (run_dir / "private" / "asr.json").write_text("{}", encoding="utf-8")
    manifest_before = (run_dir / "manifest.json").read_bytes()

    with pytest.raises(PodcastPipelineError, match="failed validation"):
        coordinator.resume(run_dir)

    assert (run_dir / "manifest.json").read_bytes() == manifest_before


def test_resume_rejects_changed_profile_with_same_id(tmp_path, monkeypatch) -> None:
    source = tmp_path / "source.m4a"
    source.write_bytes(b"authorized input")
    run_dir = tmp_path / "run"
    monkeypatch.setattr(
        "videotrans.podcast.orchestrator.probe_duration_ms", lambda _: 1000
    )
    coordinator, asr, translator, tts, finalizer = make_coordinator("A short episode.")
    coordinator.create(source, run_dir)
    changed = replace(coordinator.profile, voice="DifferentVoice")
    changed_coordinator = PodcastCoordinator(
        PodcastRuntime(
            asr, translator, tts, lambda _: "oss://temporary/input.m4a", finalizer
        ),
        changed,
    )

    with pytest.raises(PodcastPipelineError, match="different production profile"):
        changed_coordinator.resume(run_dir)


def test_listener_review_updates_manifest_and_report_without_cloud(
    tmp_path, monkeypatch
) -> None:
    source = tmp_path / "source.m4a"
    source.write_bytes(b"authorized input")
    run_dir = tmp_path / "run"
    monkeypatch.setattr(
        "videotrans.podcast.orchestrator.probe_duration_ms", lambda _: 1000
    )
    coordinator, _, _, _, _ = make_coordinator("A short episode.")
    result = coordinator.create(source, run_dir)

    _record_review(
        run_dir,
        "accepted",
        profile=coordinator.profile,
        note="steady voice",
        report_path=None,
    )
    coordinator.resume(run_dir)

    manifest = ManifestStore(run_dir / "manifest.json").load()
    report = json.loads(result.report_path.read_text(encoding="utf-8"))
    assert manifest.run["status"] == "accepted"
    assert report["run"]["status"] == "accepted"
    assert report["listening_quality_gate"]["status"] == "accepted"
    assert report["listening_quality_gate"]["note"] == "steady voice"


def test_listener_review_rejects_changed_profile_before_mutation(
    tmp_path, monkeypatch
) -> None:
    source = tmp_path / "source.m4a"
    source.write_bytes(b"authorized input")
    run_dir = tmp_path / "run"
    monkeypatch.setattr(
        "videotrans.podcast.orchestrator.probe_duration_ms", lambda _: 1000
    )
    coordinator, _, _, _, _ = make_coordinator("A short episode.")
    coordinator.create(source, run_dir)
    changed = replace(coordinator.profile, voice="DifferentVoice")

    with pytest.raises(PodcastPipelineError, match="different production profile"):
        _record_review(
            run_dir,
            "accepted",
            profile=changed,
            note=None,
            report_path=None,
        )

    manifest = ManifestStore(run_dir / "manifest.json").load()
    assert manifest.run["status"] == "awaiting_review"


def test_report_counts_provider_retries(tmp_path, monkeypatch) -> None:
    source = tmp_path / "source.m4a"
    source.write_bytes(b"authorized input")
    run_dir = tmp_path / "run"
    monkeypatch.setattr(
        "videotrans.podcast.orchestrator.probe_duration_ms", lambda _: 1000
    )
    coordinator, _, _, tts, _ = make_coordinator("A short episode.")
    tts.fail_on_call = 1
    tts.failure = TransientAlibabaError(code="throttled")

    result = coordinator.create(source, run_dir)

    report = json.loads(result.report_path.read_text(encoding="utf-8"))
    assert len(tts.calls) == 2
    assert report["totals"]["retries"] == 1
    assert report["stages"][3]["retries"] == 1


def test_exhausted_safe_retries_leave_chunk_failed(tmp_path, monkeypatch) -> None:
    class AlwaysThrottledTts(FakeTts):
        def synthesize(self, text: str) -> SpeechResult:
            self.calls.append(text)
            raise TransientAlibabaError(code="throttled")

    original_scheduler = podcast_orchestrator.AsyncScheduler

    class FastRetryScheduler(original_scheduler):
        def __init__(self, *args, **kwargs):
            kwargs.update(
                retry_base_seconds=0,
                retry_max_seconds=0,
                jitter=lambda: 0,
            )
            super().__init__(*args, **kwargs)

    source = tmp_path / "source.m4a"
    source.write_bytes(b"authorized input")
    run_dir = tmp_path / "run"
    monkeypatch.setattr(
        "videotrans.podcast.orchestrator.probe_duration_ms", lambda _: 1000
    )
    base, asr, translator, _, finalizer = make_coordinator("A short episode.")
    tts = AlwaysThrottledTts()
    coordinator = PodcastCoordinator(
        PodcastRuntime(
            asr=asr,
            translator=translator,
            tts=tts,
            upload_audio=lambda _: "oss://temporary/input.m4a",
            finalizer=finalizer,
        ),
        base.profile,
    )
    monkeypatch.setattr(podcast_orchestrator, "AsyncScheduler", FastRetryScheduler)

    with pytest.raises(TransientAlibabaError):
        coordinator.create(source, run_dir)

    item = ManifestStore(run_dir / "manifest.json").load().stage("tts")["chunks"][0]
    assert len(tts.calls) == 6
    assert item["attempts"] == 6
    assert item["retries"] == 5
    assert item["status"] == "failed"


@pytest.mark.parametrize(
    "failure",
    [
        TransientAlibabaError(code="timeout"),
        TransientAlibabaError(
            code="audio_download_failed",
            status_code=503,
            submission_may_have_succeeded=True,
        ),
    ],
)
def test_ambiguous_tts_failure_is_not_retried_or_resubmitted_on_resume(
    tmp_path, monkeypatch, failure
) -> None:
    source = tmp_path / "source.m4a"
    source.write_bytes(b"authorized input")
    run_dir = tmp_path / "run"
    monkeypatch.setattr(
        "videotrans.podcast.orchestrator.probe_duration_ms", lambda _: 1000
    )
    coordinator, _, _, tts, _ = make_coordinator("A short episode.")
    tts.fail_on_call = 1
    tts.failure = failure

    with pytest.raises(TransientAlibabaError):
        coordinator.create(source, run_dir)
    with pytest.raises(PodcastPipelineError, match="may already have been billed"):
        coordinator.resume(run_dir)

    assert len(tts.calls) == 1


def test_resume_adopts_translation_package_committed_before_manifest(
    tmp_path, monkeypatch
) -> None:
    source = tmp_path / "source.m4a"
    source.write_bytes(b"authorized input")
    run_dir = tmp_path / "run"
    monkeypatch.setattr(
        "videotrans.podcast.orchestrator.probe_duration_ms", lambda _: 1000
    )
    coordinator, _, translator, tts, _ = make_coordinator("A short episode.")
    original = podcast_orchestrator._write_json_artifact_package

    def crash_after_package(*args, **kwargs):
        original(*args, **kwargs)
        raise ValueError("local manifest window")

    monkeypatch.setattr(
        podcast_orchestrator, "_write_json_artifact_package", crash_after_package
    )
    with pytest.raises(ValueError, match="local manifest window"):
        coordinator.create(source, run_dir)

    manifest = ManifestStore(run_dir / "manifest.json").load()
    assert manifest.stage("translate")["chunks"][0]["status"] == "submission_uncertain"
    assert (run_dir / "private" / "translate" / "000000" / "receipt.json").is_file()
    incomplete = run_dir / "private" / "tts" / ".000000.crash.tmp"
    incomplete.mkdir(parents=True)
    (incomplete / "partial").write_bytes(b"partial")

    monkeypatch.setattr(podcast_orchestrator, "_write_json_artifact_package", original)
    result = coordinator.resume(run_dir)

    assert result.status == "awaiting_review"
    assert not incomplete.exists()
    assert len(translator.calls) == 1
    assert len(tts.calls) == 1


def test_resume_adopts_tts_package_committed_before_manifest(
    tmp_path, monkeypatch
) -> None:
    class CommitWindowCrash(BaseException):
        pass

    source = tmp_path / "source.m4a"
    source.write_bytes(b"authorized input")
    run_dir = tmp_path / "run"
    monkeypatch.setattr(
        "videotrans.podcast.orchestrator.probe_duration_ms", lambda _: 1000
    )
    coordinator, _, _, tts, _ = make_coordinator("A short episode.")
    original = podcast_orchestrator._write_bytes_artifact_package

    def crash_after_package(*args, **kwargs):
        original(*args, **kwargs)
        raise CommitWindowCrash()

    monkeypatch.setattr(
        podcast_orchestrator, "_write_bytes_artifact_package", crash_after_package
    )
    with pytest.raises(CommitWindowCrash):
        coordinator.create(source, run_dir)

    manifest = ManifestStore(run_dir / "manifest.json").load()
    assert manifest.stage("tts")["chunks"][0]["status"] == "submission_uncertain"
    assert (run_dir / "private" / "tts" / "000000" / "receipt.json").is_file()

    monkeypatch.setattr(podcast_orchestrator, "_write_bytes_artifact_package", original)
    result = coordinator.resume(run_dir)

    assert result.status == "awaiting_review"
    assert len(tts.calls) == 1


def test_resume_does_not_adopt_or_resubmit_corrupt_uncertain_package(
    tmp_path, monkeypatch
) -> None:
    class CommitWindowCrash(BaseException):
        pass

    source = tmp_path / "source.m4a"
    source.write_bytes(b"authorized input")
    run_dir = tmp_path / "run"
    monkeypatch.setattr(
        "videotrans.podcast.orchestrator.probe_duration_ms", lambda _: 1000
    )
    coordinator, _, _, tts, _ = make_coordinator("A short episode.")
    original = podcast_orchestrator._write_bytes_artifact_package

    def crash_after_package(*args, **kwargs):
        original(*args, **kwargs)
        raise CommitWindowCrash()

    monkeypatch.setattr(
        podcast_orchestrator, "_write_bytes_artifact_package", crash_after_package
    )
    with pytest.raises(CommitWindowCrash):
        coordinator.create(source, run_dir)

    artifact = run_dir / "private" / "tts" / "000000" / "artifact.audio"
    artifact.write_bytes(b"corrupt")
    monkeypatch.setattr(podcast_orchestrator, "_write_bytes_artifact_package", original)

    with pytest.raises(PodcastPipelineError, match="may already have been billed"):
        coordinator.resume(run_dir)

    assert len(tts.calls) == 1


def test_invalid_provider_audio_stays_uncertain_and_is_not_resubmitted(
    tmp_path, monkeypatch
) -> None:
    class InvalidAudioTts(FakeTts):
        def synthesize(self, text: str) -> SpeechResult:
            self.calls.append(text)
            return SpeechResult(
                audio=_fake_wav(frames=0), usage=len(text), request_id=None
            )

    source = tmp_path / "source.m4a"
    source.write_bytes(b"authorized input")
    run_dir = tmp_path / "run"
    monkeypatch.setattr(
        "videotrans.podcast.orchestrator.probe_duration_ms", lambda _: 1000
    )
    base, asr, translator, _, finalizer = make_coordinator("A short episode.")
    tts = InvalidAudioTts()
    coordinator = PodcastCoordinator(
        PodcastRuntime(
            asr=asr,
            translator=translator,
            tts=tts,
            upload_audio=lambda _: "oss://temporary/input.m4a",
            finalizer=finalizer,
        ),
        base.profile,
    )

    with pytest.raises(PodcastPipelineError, match="invalid final audio"):
        coordinator.create(source, run_dir)

    item = ManifestStore(run_dir / "manifest.json").load().stage("tts")["chunks"][0]
    assert item["status"] == "submission_uncertain"
    with pytest.raises(PodcastPipelineError, match="may already have been billed"):
        coordinator.resume(run_dir)
    assert len(tts.calls) == 1


def test_tts_validation_accepts_streaming_wav_size_sentinels(tmp_path) -> None:
    artifact = tmp_path / "streaming.wav"
    artifact.write_bytes(_fake_wav(streaming_header=True))

    podcast_orchestrator._validate_tts_audio(artifact)


def test_resume_rejects_missing_tts_usage_descriptor(tmp_path, monkeypatch) -> None:
    source = tmp_path / "source.m4a"
    source.write_bytes(b"authorized input")
    run_dir = tmp_path / "run"
    monkeypatch.setattr(
        "videotrans.podcast.orchestrator.probe_duration_ms", lambda _: 1000
    )
    coordinator, _, _, _, _ = make_coordinator("A short episode.")
    coordinator.create(source, run_dir)
    (run_dir / "private" / "tts" / "000000" / "receipt.json").unlink()

    with pytest.raises(PodcastPipelineError, match="missing"):
        coordinator.resume(run_dir)


@pytest.mark.skipif(
    shutil.which("ffmpeg") is None or shutil.which("ffprobe") is None,
    reason="ffmpeg and ffprobe are required",
)
def test_prepare_asr_audio_makes_mono_16khz_aac(tmp_path) -> None:
    source = tmp_path / "stereo.wav"
    destination = tmp_path / "prepared.m4a"
    with wave.open(str(source), "wb") as output:
        output.setnchannels(2)
        output.setsampwidth(2)
        output.setframerate(48000)
        frame = struct.pack("<hh", 0, 0)
        output.writeframes(frame * 4800)

    prepare_asr_audio(source, destination)
    probe = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-select_streams",
            "a:0",
            "-show_entries",
            "stream=codec_name,sample_rate,channels",
            "-of",
            "json",
            str(destination),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    stream = json.loads(probe.stdout)["streams"][0]
    assert stream == {"codec_name": "aac", "sample_rate": "16000", "channels": 1}
