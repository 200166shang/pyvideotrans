"""Production Alibaba provider wiring for the podcast pipeline.

The adapters remain dependency-injected and offline-testable.  This module is
the only place that imports the DashScope SDK, requests, or the application's
persisted provider settings at runtime.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.request import urlopen

from videotrans.podcast.alibaba_asr import (
    MODEL as ASR_MODEL,
)
from videotrans.podcast.alibaba_asr import (
    AlibabaAsrHttpTransport,
    AlibabaWholeFileAsrClient,
)
from videotrans.podcast.alibaba_text import (
    BEIJING_ENDPOINT,
    AlibabaTranslationAdapter,
    AlibabaTTSAdapter,
)
from videotrans.podcast.profiles import ALIBABA_PODCAST_V1, PodcastProfile


class AlibabaRuntimeConfigurationError(RuntimeError):
    """Actionable configuration failure that never includes credential values."""


class AlibabaAudioUploadError(RuntimeError):
    """Sanitized local-audio upload failure."""


@dataclass(frozen=True)
class AlibabaEndpoint:
    workspace_id: str | None
    base_url: str


@dataclass(frozen=True)
class AlibabaPodcastProviders:
    """Ready-to-run provider clients and the temporary-audio uploader."""

    asr: AlibabaWholeFileAsrClient
    translation: AlibabaTranslationAdapter
    tts: AlibabaTTSAdapter
    upload_audio: Callable[[str | os.PathLike[str]], str]
    endpoint: AlibabaEndpoint


def create_alibaba_podcast_providers(
    *,
    profile: PodcastProfile = ALIBABA_PODCAST_V1,
    environ: Mapping[str, str] | None = None,
    params: Mapping[str, Any] | Any | None = None,
    dashscope_module: Any | None = None,
    requests_module: Any | None = None,
    asr_opener: Callable[..., Any] = urlopen,
) -> AlibabaPodcastProviders:
    """Build real Beijing-region Alibaba clients without making network calls.

    Tests should inject ``params``, ``dashscope_module`` and ``requests_module``.
    Production callers may omit them to lazily load the application's existing
    settings and installed provider libraries.
    """

    environment = os.environ if environ is None else environ
    app_params = _load_app_params() if params is None else params
    dashscope = _load_dashscope() if dashscope_module is None else dashscope_module
    requests = _load_requests() if requests_module is None else requests_module

    environment_key = _clean(environment.get("DASHSCOPE_API_KEY"))
    qwenmt_key = _param(app_params, "qwenmt_key")
    qwentts_key = _param(app_params, "qwentts_key")
    text_key = environment_key or qwenmt_key or qwentts_key
    tts_key = environment_key or qwentts_key or qwenmt_key
    if not text_key or not tts_key:
        raise AlibabaRuntimeConfigurationError(
            "Alibaba API key is missing; set DASHSCOPE_API_KEY or configure "
            "qwenmt_key/qwentts_key in the application settings."
        )

    endpoint = resolve_alibaba_endpoint(environment, app_params)
    # DashScope's Python SDK uses this module-level endpoint for both call
    # classes.  Set it once while constructing a runtime whose providers share
    # the resolved Beijing endpoint.
    dashscope.base_http_api_url = endpoint.base_url

    generation_call = dashscope.Generation.call
    multimodal_call = dashscope.MultiModalConversation.call
    upload_file = dashscope.utils.oss_utils.upload_file

    translation = AlibabaTranslationAdapter(generation_call, api_key=text_key)
    tts = AlibabaTTSAdapter(
        multimodal_call,
        downloader=lambda url: requests.get(url, timeout=60),
        api_key=tts_key,
    )
    asr_transport = AlibabaAsrHttpTransport(
        endpoint.workspace_id,
        lambda: text_key,
        base_url=endpoint.base_url,
        opener=asr_opener,
    )
    uploader = _AlibabaAudioUploader(
        upload_file=upload_file,
        model=profile.asr_model or ASR_MODEL,
        api_key=text_key,
    )
    return AlibabaPodcastProviders(
        asr=AlibabaWholeFileAsrClient(asr_transport),
        translation=translation,
        tts=tts,
        upload_audio=uploader,
        endpoint=endpoint,
    )


def resolve_alibaba_endpoint(
    environ: Mapping[str, str], params: Mapping[str, Any] | Any
) -> AlibabaEndpoint:
    """Resolve one Beijing endpoint, preferring the explicit environment value."""

    configured = (
        _clean(environ.get("DASHSCOPE_WORKSPACE_ID"))
        or _param(params, "qwenmt_spaceid")
        or _param(params, "qwentts_spaceid")
    )
    if not configured:
        return AlibabaEndpoint(workspace_id=None, base_url=BEIJING_ENDPOINT)
    if configured.startswith(("https://", "http://")):
        return AlibabaEndpoint(workspace_id=None, base_url=configured.rstrip("/"))
    return AlibabaEndpoint(
        workspace_id=configured,
        base_url=f"https://{configured}.cn-beijing.maas.aliyuncs.com/api/v1",
    )


class _AlibabaAudioUploader:
    def __init__(
        self,
        *,
        upload_file: Callable[..., Any],
        model: str,
        api_key: str,
    ) -> None:
        self._upload_file = upload_file
        self._model = model
        self._api_key = api_key

    def __call__(self, path: str | os.PathLike[str]) -> str:
        audio_path = Path(path).expanduser()
        if not audio_path.is_file():
            raise AlibabaAudioUploadError("Audio upload input is not a readable file.")
        try:
            # Current DashScope releases require a file URI; older/newer return
            # shapes are normalized below so the coordinator only sees oss://.
            result = self._upload_file(
                self._model,
                audio_path.resolve().as_uri(),
                self._api_key,
            )
        except Exception:  # noqa: BLE001 - never leak SDK errors containing credentials
            raise AlibabaAudioUploadError(
                "Alibaba temporary audio upload failed; verify the API key, "
                "workspace, region, and network connection."
            ) from None
        url = _find_oss_url(result)
        if url is None:
            raise AlibabaAudioUploadError(
                "Alibaba temporary audio upload returned no oss:// URL."
            )
        return url

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(model={self._model!r})"


def _find_oss_url(value: Any, *, _depth: int = 0) -> str | None:
    if _depth > 5:
        return None
    if isinstance(value, str):
        return value if value.startswith("oss://") else None
    if isinstance(value, Mapping):
        for key in ("file_url", "fileUrl", "oss_url", "ossUrl", "url"):
            found = _find_oss_url(value.get(key), _depth=_depth + 1)
            if found:
                return found
        for key in ("output", "data", "result", "uploaded_files", "files"):
            found = _find_oss_url(value.get(key), _depth=_depth + 1)
            if found:
                return found
        return None
    if isinstance(value, (list, tuple)):
        for item in value:
            found = _find_oss_url(item, _depth=_depth + 1)
            if found:
                return found
        return None
    for key in (
        "file_url",
        "fileUrl",
        "oss_url",
        "ossUrl",
        "url",
        "output",
        "data",
        "result",
        "uploaded_files",
        "files",
    ):
        found = _find_oss_url(getattr(value, key, None), _depth=_depth + 1)
        if found:
            return found
    return None


def _param(params: Mapping[str, Any] | Any, name: str) -> str | None:
    getter = getattr(params, "get", None)
    value = getter(name, "") if callable(getter) else getattr(params, name, "")
    return _clean(value)


def _clean(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _load_app_params() -> Any:
    from videotrans.configure.config import params

    return params


def _load_dashscope() -> Any:
    try:
        import dashscope
    except ImportError:
        raise AlibabaRuntimeConfigurationError(
            "DashScope SDK is unavailable; install project dependencies."
        ) from None
    return dashscope


def _load_requests() -> Any:
    try:
        import requests
    except ImportError:
        raise AlibabaRuntimeConfigurationError(
            "requests is unavailable; install project dependencies."
        ) from None
    return requests
