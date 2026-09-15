import base64
from dataclasses import dataclass

import pytest

from videotrans.podcast.alibaba_text import (
    BEIJING_ENDPOINT,
    BEIJING_REGION,
    QWEN_MT_MODEL,
    QWEN_TTS_LANGUAGE,
    QWEN_TTS_MODEL,
    QWEN_TTS_VOICE,
    AlibabaTranslationAdapter,
    AlibabaTTSAdapter,
    PermanentAlibabaError,
    TransientAlibabaError,
)


class FakeTransport:
    def __init__(self, response=None, error=None):
        self.response = response
        self.error = error
        self.calls = []

    def call(self, **kwargs):
        self.calls.append(kwargs)
        if self.error:
            raise self.error
        return self.response


def translation_response(text="中文", usage=None, request_id="mt-request"):
    return {
        "status_code": 200,
        "output": {"choices": [{"message": {"content": text}}]},
        "usage": usage or {"input_tokens": 8, "output_tokens": 3},
        "request_id": request_id,
    }


def test_translation_is_pinned_and_passes_options_without_configuration_reads():
    transport = FakeTransport(translation_response())
    adapter = AlibabaTranslationAdapter(transport, api_key="test-key")

    result = adapter.translate(
        "English source",
        glossary=[{"source": "agent", "target": "智能体"}],
        context="software podcast",
    )

    assert result.text == "中文"
    assert result.usage == 11
    assert result.request_id == "mt-request"
    assert adapter.model == QWEN_MT_MODEL == "qwen-mt-flash"
    assert adapter.region == BEIJING_REGION == "cn-beijing"
    assert adapter.endpoint == BEIJING_ENDPOINT
    assert transport.calls == [
        {
            "api_key": "test-key",
            "model": "qwen-mt-flash",
            "messages": [{"role": "user", "content": "English source"}],
            "result_format": "message",
            "translation_options": {
                "source_lang": "en",
                "target_lang": "zh",
                "terms": [{"source": "agent", "target": "智能体"}],
                "domains": "software podcast",
            },
        }
    ]


def test_translation_accepts_object_shaped_dashscope_response():
    @dataclass
    class Message:
        content: str

    @dataclass
    class Choice:
        message: Message

    @dataclass
    class Output:
        choices: list

    @dataclass
    class Usage:
        total_tokens: int

    @dataclass
    class Response:
        output: Output
        usage: Usage
        request_id: str
        code: str = ""
        status_code: int = 200

    response = Response(Output([Choice(Message("对象响应"))]), Usage(17), "object-id")
    result = AlibabaTranslationAdapter(FakeTransport(response)).translate("source")
    assert result.text == "对象响应"
    assert result.usage == 17
    assert result.request_id == "object-id"


@pytest.mark.parametrize(
    ("response", "error_type", "retryable"),
    [
        ({"status_code": 429, "code": "Throttling", "message": "secret prose"}, TransientAlibabaError, True),
        ({"status_code": 503, "code": "ServiceUnavailable", "message": "secret prose"}, TransientAlibabaError, True),
        ({"status_code": 401, "code": "InvalidApiKey", "message": "secret prose"}, PermanentAlibabaError, False),
        ({"status_code": 400, "code": "BadRequest", "message": "secret prose"}, PermanentAlibabaError, False),
    ],
)
def test_translation_classifies_errors_without_response_text(response, error_type, retryable):
    response["request_id"] = "safe-id"
    with pytest.raises(error_type) as captured:
        AlibabaTranslationAdapter(FakeTransport(response)).translate("do not disclose")
    assert captured.value.retryable is retryable
    assert captured.value.request_id == "safe-id"
    assert "secret prose" not in str(captured.value)
    assert "do not disclose" not in str(captured.value)


def test_transport_timeout_is_transient_and_sanitized():
    with pytest.raises(TransientAlibabaError) as captured:
        AlibabaTranslationAdapter(
            FakeTransport(error=TimeoutError("response included private text"))
        ).translate("private source")
    assert "private" not in str(captured.value)
    assert captured.value.retryable


def test_empty_translation_is_a_retryable_malformed_response():
    with pytest.raises(TransientAlibabaError) as captured:
        AlibabaTranslationAdapter(FakeTransport(translation_response(text=""))).translate("source")
    assert captured.value.code == "invalid_translation_response"


def tts_event(data=None, url=None, usage=None, request_id="tts-request"):
    audio = {}
    if data is not None:
        audio["data"] = base64.b64encode(data).decode("ascii")
    if url is not None:
        audio["url"] = url
    return {
        "status_code": 200,
        "output": {"audio": audio},
        "usage": usage or {"characters": 6},
        "request_id": request_id,
    }


def test_tts_uses_pinned_andre_stream_and_downloads_complete_wav():
    transport = FakeTransport(
        iter(
            [
                tts_event(b"raw-pcm", request_id="first"),
                tts_event(url="https://example.invalid/final.wav", request_id="last"),
            ]
        )
    )
    downloader_calls = []
    adapter = AlibabaTTSAdapter(
        transport,
        downloader=lambda url: downloader_calls.append(url) or b"complete-wav",
        api_key="test-key",
    )

    result = adapter.synthesize("沉稳男声")

    assert result.audio == b"complete-wav"
    assert result.usage == 6
    assert result.request_id == "last"
    assert downloader_calls == ["https://example.invalid/final.wav"]
    assert adapter.model == QWEN_TTS_MODEL == "qwen3-tts-flash-2025-11-27"
    assert adapter.voice == QWEN_TTS_VOICE == "Andre"
    assert adapter.language == QWEN_TTS_LANGUAGE == "Chinese"
    assert transport.calls == [
        {
            "api_key": "test-key",
            "model": "qwen3-tts-flash-2025-11-27",
            "text": "沉稳男声",
            "voice": "Andre",
            "language_type": "Chinese",
            "stream": True,
        }
    ]


def test_tts_downloads_final_url_when_stream_has_no_audio_data():
    urls = []

    def download(url):
        urls.append(url)
        return b"downloaded-audio"

    result = AlibabaTTSAdapter(
        FakeTransport(iter([tts_event(url="https://example.invalid/audio.wav")])),
        downloader=download,
    ).synthesize("中文")
    assert result.audio == b"downloaded-audio"
    assert urls == ["https://example.invalid/audio.wav"]


def test_tts_prefers_complete_final_url_over_raw_streamed_chunks():
    downloaded = []
    events = iter([tts_event(b"chunk"), tts_event(url="https://example.invalid/final.wav")])
    result = AlibabaTTSAdapter(
        FakeTransport(events),
        downloader=lambda url: downloaded.append(url) or b"complete-wav",
    ).synthesize("中文")
    assert result.audio == b"complete-wav"
    assert downloaded == ["https://example.invalid/final.wav"]


def test_tts_exposes_soft_limit_but_only_rejects_over_hard_limit():
    adapter = AlibabaTTSAdapter(FakeTransport(iter([tts_event(b"audio")])))
    assert adapter.soft_text_limit == 500
    assert adapter.hard_text_limit == 600
    with pytest.raises(TransientAlibabaError, match="missing_complete_audio_url"):
        adapter.synthesize("中" * 501)

    over_limit_transport = FakeTransport(iter([tts_event(b"never")]))
    with pytest.raises(ValueError, match="600"):
        AlibabaTTSAdapter(over_limit_transport).synthesize("中" * 601)
    assert over_limit_transport.calls == []


def test_tts_classifies_stream_error_without_embedding_response_text():
    event = {
        "status_code": 429,
        "code": "Throttling",
        "message": "the synthesized content was private",
        "request_id": "failed-id",
    }
    with pytest.raises(TransientAlibabaError) as captured:
        AlibabaTTSAdapter(FakeTransport(iter([event]))).synthesize("private input")
    assert captured.value.request_id == "failed-id"
    assert "private" not in str(captured.value)


def test_tts_requires_injected_downloader_for_url_response():
    with pytest.raises(PermanentAlibabaError) as captured:
        AlibabaTTSAdapter(
            FakeTransport(iter([tts_event(url="https://example.invalid/audio.wav")]))
        ).synthesize("中文")
    assert captured.value.code == "downloader_required"


def test_invalid_base64_audio_is_transient():
    event = {
        "status_code": 200,
        "output": {"audio": {"data": "not base64"}},
        "request_id": "bad-audio",
    }
    with pytest.raises(TransientAlibabaError) as captured:
        AlibabaTTSAdapter(FakeTransport(iter([event]))).synthesize("中文")
    assert captured.value.code == "invalid_audio_data"
    assert captured.value.request_id == "bad-audio"
