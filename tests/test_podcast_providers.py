from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from videotrans.podcast.alibaba_asr import AlibabaAsrHttpTransport
from videotrans.podcast.alibaba_text import TransientAlibabaError
from videotrans.podcast.providers import (
    AlibabaAudioUploadError,
    AlibabaRuntimeConfigurationError,
    create_alibaba_podcast_providers,
    resolve_alibaba_endpoint,
)


class FakeCall:
    def __init__(self):
        self.calls = []

    def __call__(self, **kwargs):
        self.calls.append(kwargs)
        return {}


class FakeDashScope:
    def __init__(self, upload_result="oss://bucket/audio.m4a"):
        self.generation_call = FakeCall()
        self.multimodal_call = FakeCall()
        self.upload_calls = []

        def upload_file(model, path, api_key):
            self.upload_calls.append((model, path, api_key))
            return upload_result

        self.Generation = SimpleNamespace(call=self.generation_call)
        self.MultiModalConversation = SimpleNamespace(call=self.multimodal_call)
        self.utils = SimpleNamespace(
            oss_utils=SimpleNamespace(upload_file=upload_file)
        )
        self.base_http_api_url = "before"


class FakeRequests:
    def __init__(self):
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return SimpleNamespace(content=b"audio", raise_for_status=lambda: None)


def make_runtime(*, environment=None, params=None, upload_result="oss://bucket/audio.m4a"):
    dashscope = FakeDashScope(upload_result)
    requests = FakeRequests()
    runtime = create_alibaba_podcast_providers(
        environ=environment or {},
        params=params or {},
        dashscope_module=dashscope,
        requests_module=requests,
    )
    return runtime, dashscope, requests


def test_environment_key_has_priority_for_all_sdk_calls():
    runtime, dashscope, _ = make_runtime(
        environment={"DASHSCOPE_API_KEY": "env-secret"},
        params={"qwenmt_key": "mt-secret", "qwentts_key": "tts-secret"},
    )

    with pytest.raises(TransientAlibabaError):
        runtime.translation.translate("Hello")
    assert dashscope.generation_call.calls[0]["api_key"] == "env-secret"
    with pytest.raises(TransientAlibabaError):
        runtime.tts.synthesize("你好")
    assert dashscope.multimodal_call.calls[0]["api_key"] == "env-secret"


def test_service_specific_config_keys_are_used_without_environment_key():
    runtime, dashscope, _ = make_runtime(
        params={"qwenmt_key": "mt-secret", "qwentts_key": "tts-secret"}
    )

    with pytest.raises(TransientAlibabaError):
        runtime.translation.translate("Hello")
    with pytest.raises(TransientAlibabaError):
        runtime.tts.synthesize("你好")

    assert dashscope.generation_call.calls[0]["api_key"] == "mt-secret"
    assert dashscope.multimodal_call.calls[0]["api_key"] == "tts-secret"


def test_one_existing_config_key_can_supply_the_whole_personal_runtime():
    runtime, dashscope, _ = make_runtime(params={"qwenmt_key": "shared-secret"})

    with pytest.raises(TransientAlibabaError):
        runtime.tts.synthesize("你好")

    assert dashscope.multimodal_call.calls[0]["api_key"] == "shared-secret"


def test_missing_key_error_is_actionable_and_sanitized():
    with pytest.raises(AlibabaRuntimeConfigurationError) as caught:
        make_runtime(environment={}, params={})

    message = str(caught.value)
    assert "DASHSCOPE_API_KEY" in message
    assert "qwenmt_key/qwentts_key" in message


@pytest.mark.parametrize(
    ("environment", "params", "workspace_id", "base_url"),
    [
        ({}, {}, None, "https://dashscope.aliyuncs.com/api/v1"),
        (
            {"DASHSCOPE_WORKSPACE_ID": "env-space"},
            {"qwenmt_spaceid": "config-space"},
            "env-space",
            "https://env-space.cn-beijing.maas.aliyuncs.com/api/v1",
        ),
        (
            {},
            {"qwenmt_spaceid": "mt-space", "qwentts_spaceid": "tts-space"},
            "mt-space",
            "https://mt-space.cn-beijing.maas.aliyuncs.com/api/v1",
        ),
        (
            {},
            {"qwentts_spaceid": "https://custom.example/api/v1/"},
            None,
            "https://custom.example/api/v1",
        ),
    ],
)
def test_endpoint_resolution(environment, params, workspace_id, base_url):
    endpoint = resolve_alibaba_endpoint(environment, params)

    assert endpoint.workspace_id == workspace_id
    assert endpoint.base_url == base_url


def test_factory_applies_resolved_base_url_to_sdk():
    runtime, dashscope, _ = make_runtime(
        environment={
            "DASHSCOPE_API_KEY": "secret",
            "DASHSCOPE_WORKSPACE_ID": "workspace",
        }
    )

    assert runtime.endpoint.base_url == (
        "https://workspace.cn-beijing.maas.aliyuncs.com/api/v1"
    )
    assert dashscope.base_http_api_url == runtime.endpoint.base_url


@pytest.mark.parametrize(
    "result",
    [
        "oss://bucket/audio.m4a",
        {"file_url": "oss://bucket/audio.m4a"},
        {"output": {"uploaded_files": [{"url": "oss://bucket/audio.m4a"}]}},
        SimpleNamespace(output=SimpleNamespace(oss_url="oss://bucket/audio.m4a")),
        ("oss://bucket/audio.m4a", {"expires_in": 172800}),
    ],
)
def test_audio_upload_normalizes_sdk_result_shapes(tmp_path: Path, result):
    audio = tmp_path / "source.m4a"
    audio.write_bytes(b"audio")
    runtime, dashscope, _ = make_runtime(
        environment={"DASHSCOPE_API_KEY": "secret"}, upload_result=result
    )

    assert runtime.upload_audio(audio) == "oss://bucket/audio.m4a"
    model, upload_path, api_key = dashscope.upload_calls[0]
    assert model == "qwen-audio-3.0-asr-flash-filetrans"
    assert upload_path == audio.resolve().as_uri()
    assert api_key == "secret"


def test_audio_upload_failure_does_not_disclose_key_or_path(tmp_path: Path):
    audio = tmp_path / "private-name.m4a"
    audio.write_bytes(b"audio")
    dashscope = FakeDashScope()

    def fail(*args):
        raise RuntimeError("secret private-name.m4a")

    dashscope.utils.oss_utils.upload_file = fail
    runtime = create_alibaba_podcast_providers(
        environ={"DASHSCOPE_API_KEY": "secret"},
        params={},
        dashscope_module=dashscope,
        requests_module=FakeRequests(),
    )

    with pytest.raises(AlibabaAudioUploadError) as caught:
        runtime.upload_audio(audio)
    assert "secret" not in str(caught.value)
    assert "private-name" not in str(caught.value)


class FakeHttpResponse:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def read(self):
        return json.dumps(self.payload).encode()


def test_asr_public_endpoint_and_oss_resolve_header():
    captured = []

    def opener(request, timeout):
        captured.append((request, timeout))
        return FakeHttpResponse(
            {"output": {"task_id": "task", "task_status": "PENDING"}}
        )

    transport = AlibabaAsrHttpTransport(None, lambda: "secret", opener=opener)
    transport.submit(
        {"input": {"file_urls": ["oss://bucket/source.m4a"]}}
    )

    request, _ = captured[0]
    assert request.full_url.startswith("https://dashscope.aliyuncs.com/api/v1/")
    assert request.headers["X-dashscope-ossresourceresolve"] == "enable"
    assert request.headers["X-dashscope-async"] == "enable"


def test_asr_http_url_does_not_enable_oss_resolution():
    captured = []

    def opener(request, timeout):
        captured.append(request)
        return FakeHttpResponse({})

    transport = AlibabaAsrHttpTransport(
        "workspace", lambda: "secret", opener=opener
    )
    transport.submit({"input": {"file_urls": ["https://example/audio.m4a"]}})

    assert "X-dashscope-ossresourceresolve" not in captured[0].headers


def test_asr_result_download_omits_content_type_from_signed_get():
    captured = []

    def opener(request, timeout):
        captured.append(request)
        return FakeHttpResponse({"transcripts": []})

    transport = AlibabaAsrHttpTransport(None, lambda: "secret", opener=opener)

    transport.download_result("https://example/result.json?signature=redacted")

    assert "Content-type" not in captured[0].headers


def test_tts_url_download_uses_requests_timeout():
    dashscope = FakeDashScope()
    dashscope.MultiModalConversation.call = lambda **kwargs: {
        "output": {"audio": {"url": "https://example/audio.wav"}}
    }
    requests = FakeRequests()
    runtime = create_alibaba_podcast_providers(
        environ={"DASHSCOPE_API_KEY": "secret"},
        params={},
        dashscope_module=dashscope,
        requests_module=requests,
    )

    result = runtime.tts.synthesize("你好")

    assert result.audio == b"audio"
    assert requests.calls == [("https://example/audio.wav", {"timeout": 60})]
