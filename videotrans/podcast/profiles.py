from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class PodcastProfile:
    id: str
    region: str
    source_language: str
    target_language: str
    asr_model: str
    translation_model: str
    tts_model: str
    voice: str
    translation_concurrency: int
    translation_rpm: int
    tts_concurrency: int
    tts_rpm: int
    translation_chunk_min: int
    translation_chunk_max: int
    tts_soft_char_limit: int
    output_sample_rate_hz: int
    output_channels: int
    output_bitrate_kbps: int

    @property
    def fingerprint(self) -> str:
        payload = json.dumps(asdict(self), sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


ALIBABA_PODCAST_V1 = PodcastProfile(
    id="alibaba-podcast-v1",
    region="cn-beijing",
    source_language="en",
    target_language="zh-cn",
    asr_model="qwen-audio-3.0-asr-flash-filetrans",
    translation_model="qwen-mt-flash",
    tts_model="qwen3-tts-flash-2025-11-27",
    voice="Andre",
    translation_concurrency=4,
    translation_rpm=60,
    tts_concurrency=4,
    tts_rpm=150,
    translation_chunk_min=600,
    translation_chunk_max=1000,
    tts_soft_char_limit=500,
    output_sample_rate_hz=48000,
    output_channels=1,
    output_bitrate_kbps=64,
)


_PROFILES = {ALIBABA_PODCAST_V1.id: ALIBABA_PODCAST_V1}


def get_profile(profile_id: str) -> PodcastProfile:
    try:
        return _PROFILES[profile_id]
    except KeyError as exc:
        raise ValueError(f"unknown podcast profile: {profile_id}") from exc
