from __future__ import annotations

import hashlib
import json
import shutil
import struct
import subprocess
import wave
from dataclasses import replace
from pathlib import Path

import pytest

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
from videotrans.podcast.profiles import ALIBABA_PODCAST_V1


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
        return SpeechResult(audio=("audio:" + text).encode(), usage=len(text), request_id=None)


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


def make_coordinator(text: str) -> tuple[PodcastCoordinator, FakeAsr, FakeTranslator, FakeTts, FakeFinalizer]:
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
        ALIBABA_PODCAST_V1,
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


def test_full_run_writes_mp3_manifest_and_privacy_safe_report(tmp_path, monkeypatch) -> None:
    source = tmp_path / "source.m4a"
    source.write_bytes(b"authorized input")
    run_dir = tmp_path / "run"
    monkeypatch.setattr("videotrans.podcast.orchestrator.probe_duration_ms", lambda _: 1000)
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


@pytest.mark.parametrize("reserved_name", ["manifest.json.lock", "benchmark-summary.json"])
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
    monkeypatch.setattr("videotrans.podcast.orchestrator.probe_duration_ms", lambda _: 1000)
    coordinator, _, _, _, _ = make_coordinator("A short episode.")
    coordinator.create(source, run_dir, report_path=report)

    with pytest.raises(PodcastPipelineError, match="report path saved by the run"):
        coordinator.resume(run_dir, report_path=tmp_path / "different.json")


def test_resume_keeps_committed_tts_chunks_and_reuses_asr_task(tmp_path, monkeypatch) -> None:
    source = tmp_path / "source.m4a"
    source.write_bytes(b"authorized input")
    run_dir = tmp_path / "run"
    monkeypatch.setattr("videotrans.podcast.orchestrator.probe_duration_ms", lambda _: 1000)
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
    assert asr.submit_calls == 1
    assert tts.calls.count(first_text) == 1
    assert finalizer.calls == 1


def test_resume_never_repeats_uncertain_asr_submission(tmp_path, monkeypatch) -> None:
    class SubmissionCrash(BaseException):
        pass

    source = tmp_path / "source.m4a"
    source.write_bytes(b"authorized input")
    run_dir = tmp_path / "run"
    monkeypatch.setattr("videotrans.podcast.orchestrator.probe_duration_ms", lambda _: 1000)
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
    monkeypatch.setattr("videotrans.podcast.orchestrator.probe_duration_ms", lambda _: 1000)
    coordinator, _, _, _, _ = make_coordinator("A short episode.")
    coordinator.create(source, run_dir)
    (run_dir / "private" / "asr.json").write_text("{}", encoding="utf-8")

    with pytest.raises(PodcastPipelineError, match="failed validation"):
        coordinator.resume(run_dir)


def test_resume_rejects_changed_profile_with_same_id(tmp_path, monkeypatch) -> None:
    source = tmp_path / "source.m4a"
    source.write_bytes(b"authorized input")
    run_dir = tmp_path / "run"
    monkeypatch.setattr("videotrans.podcast.orchestrator.probe_duration_ms", lambda _: 1000)
    coordinator, asr, translator, tts, finalizer = make_coordinator("A short episode.")
    coordinator.create(source, run_dir)
    changed = replace(coordinator.profile, voice="DifferentVoice")
    changed_coordinator = PodcastCoordinator(
        PodcastRuntime(asr, translator, tts, lambda _: "oss://temporary/input.m4a", finalizer),
        changed,
    )

    with pytest.raises(PodcastPipelineError, match="different production profile"):
        changed_coordinator.resume(run_dir)


def test_listener_review_updates_manifest_and_report_without_cloud(tmp_path, monkeypatch) -> None:
    source = tmp_path / "source.m4a"
    source.write_bytes(b"authorized input")
    run_dir = tmp_path / "run"
    monkeypatch.setattr("videotrans.podcast.orchestrator.probe_duration_ms", lambda _: 1000)
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
    monkeypatch.setattr("videotrans.podcast.orchestrator.probe_duration_ms", lambda _: 1000)
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
    monkeypatch.setattr("videotrans.podcast.orchestrator.probe_duration_ms", lambda _: 1000)
    coordinator, _, _, tts, _ = make_coordinator("A short episode.")
    tts.fail_on_call = 1
    tts.failure = TransientAlibabaError(code="timeout")

    result = coordinator.create(source, run_dir)

    report = json.loads(result.report_path.read_text(encoding="utf-8"))
    assert len(tts.calls) == 2
    assert report["totals"]["retries"] == 1
    assert report["stages"][3]["retries"] == 1


def test_resume_rejects_missing_tts_usage_descriptor(tmp_path, monkeypatch) -> None:
    source = tmp_path / "source.m4a"
    source.write_bytes(b"authorized input")
    run_dir = tmp_path / "run"
    monkeypatch.setattr("videotrans.podcast.orchestrator.probe_duration_ms", lambda _: 1000)
    coordinator, _, _, _, _ = make_coordinator("A short episode.")
    coordinator.create(source, run_dir)
    (run_dir / "private" / "tts" / "000000.json").unlink()

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
