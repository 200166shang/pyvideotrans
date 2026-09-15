import pytest

from videotrans.podcast.profiles import ALIBABA_PODCAST_V1, get_profile


def test_default_profile_is_pinned_and_uses_andre() -> None:
    profile = get_profile("alibaba-podcast-v1")

    assert profile is ALIBABA_PODCAST_V1
    assert profile.asr_model == "qwen-audio-3.0-asr-flash-filetrans"
    assert profile.translation_model == "qwen-mt-flash"
    assert profile.tts_model == "qwen3-tts-flash-2025-11-27"
    assert profile.voice == "Andre"
    assert profile.translation_rpm == 60
    assert profile.tts_concurrency == 4
    assert profile.tts_rpm == 150
    assert len(profile.fingerprint) == 64


def test_unknown_profile_is_rejected() -> None:
    with pytest.raises(ValueError, match="unknown podcast profile"):
        get_profile("other")
