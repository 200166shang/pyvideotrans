from dataclasses import asdict

import pytest

from videotrans.podcast.profiles import ALIBABA_PODCAST_V2, get_profile


def test_default_profile_is_pinned_and_uses_andre() -> None:
    profile = get_profile("alibaba-podcast-v2")

    assert profile is ALIBABA_PODCAST_V2
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


def test_throughput_profile_is_isolated_and_preserves_provider_contract() -> None:
    candidate = get_profile("alibaba-podcast-tts-throughput")
    original = asdict(ALIBABA_PODCAST_V2)
    changed = {
        key: value for key, value in asdict(candidate).items() if value != original[key]
    }
    assert changed == {
        "id": "alibaba-podcast-tts-throughput",
        "tts_concurrency": 8,
        "tts_soft_char_limit": 250,
    }
    assert candidate.fingerprint != ALIBABA_PODCAST_V2.fingerprint
    assert get_profile("alibaba-podcast-v2") is ALIBABA_PODCAST_V2
