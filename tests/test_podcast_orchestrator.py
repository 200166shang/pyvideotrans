from __future__ import annotations

import json
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
from videotrans.podcast.alibaba_text import SpeechResult, TranslationResult
from videotrans.podcast.finalize import FinalizedAudio
from videotrans.podcast.manifest import ManifestStore
from videotrans.podcast.orchestrator import (
    PodcastCoordinator,
    PodcastRuntime,
)
from videotrans.podcast.profiles import ALIBABA_PODCAST_V1


class FakeAsr:
    def __init__(self, text: str) -> None:
        self.text = text
        self.submit_calls = 0
        self.poll_calls = 0
        self.polling_policy = PollingPolicy()

    def submit(self, audio_url: str, *, language: str) -> SubmittedAsrTask:
        assert audio_url == "oss://temporary/input.m4a"
        assert language == "en"
        self.submit_calls += 1
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

    def synthesize(self, text: str) -> SpeechResult:
        self.calls.append(text)
        if self.fail_on_call == len(self.calls):
            raise ValueError("offline permanent failure")
        return SpeechResult(audio=("audio:" + text).encode(), usage=len(text), request_id=None)


class FakeFinalizer:
    def __init__(self) -> None:
        self.calls = 0

    def finalize(self, chunks: list[Path], output: Path) -> FinalizedAudio:
        self.calls += 1
        output.write_bytes(b"ID3" + b"".join(path.read_bytes() for path in chunks))
        return FinalizedAudio(
            path=output,
            fingerprint="a" * 64,
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
        tts_concurrency=1,
        tts_rpm=60_000_000,
    )
    return PodcastCoordinator(runtime, profile), asr, translator, tts, finalizer


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
    assert report["privacy"] == {
        "contains_credentials": False,
        "contains_transcript": False,
        "contains_absolute_source_path": False,
    }
    serialized = result.report_path.read_text(encoding="utf-8")
    assert str(source) not in serialized
    assert "A short episode" not in serialized


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
