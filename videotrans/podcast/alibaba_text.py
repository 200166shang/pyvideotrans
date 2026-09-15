"""Offline-testable Alibaba text translation and speech synthesis adapters.

The adapters in this module deliberately do not import DashScope or application
configuration.  Callers inject a small DashScope-like transport (an object with
``call(**kwargs)``) and, for TTS URL responses, a downloader.
"""

from __future__ import annotations

import base64
import binascii
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from numbers import Real
from typing import Any, Protocol

BEIJING_ENDPOINT = "https://dashscope.aliyuncs.com/api/v1"
BEIJING_REGION = "cn-beijing"
QWEN_MT_MODEL = "qwen-mt-flash"
QWEN_TTS_MODEL = "qwen3-tts-flash-2025-11-27"
QWEN_TTS_VOICE = "Andre"
QWEN_TTS_LANGUAGE = "Chinese"
TTS_SOFT_TEXT_LIMIT = 500
TTS_HARD_TEXT_LIMIT = 600


class DashScopeTransport(Protocol):
    """The only provider operation required by these adapters."""

    def call(self, **kwargs: Any) -> Any:
        ...


@dataclass(frozen=True)
class TranslationResult:
    text: str
    usage: int | float
    request_id: str | None


@dataclass(frozen=True)
class SpeechResult:
    audio: bytes
    usage: int | float
    request_id: str | None


class AlibabaProviderError(RuntimeError):
    """Sanitized provider failure safe to place in logs and reports."""

    retryable = False
    classification = "permanent"

    def __init__(
        self,
        *,
        code: str = "provider_error",
        status_code: int | None = None,
        request_id: str | None = None,
    ) -> None:
        self.code = code
        self.status_code = status_code
        self.request_id = request_id
        fields = [self.classification, f"code={code}"]
        if status_code is not None:
            fields.append(f"status={status_code}")
        if request_id:
            fields.append(f"request_id={request_id}")
        super().__init__("Alibaba provider error (" + ", ".join(fields) + ")")


class TransientAlibabaError(AlibabaProviderError):
    retryable = True
    classification = "transient"


class PermanentAlibabaError(AlibabaProviderError):
    retryable = False
    classification = "permanent"


# Short aliases make the retry contract convenient for orchestration code.
TransientProviderError = TransientAlibabaError
PermanentProviderError = PermanentAlibabaError
ProviderError = AlibabaProviderError


def _value(obj: Any, key: str, default: Any = None) -> Any:
    if isinstance(obj, Mapping):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _path(obj: Any, *keys: str) -> Any:
    current = obj
    for key in keys:
        current = _value(current, key)
        if current is None:
            return None
    return current


def _number(value: Any) -> int | float | None:
    if isinstance(value, bool) or not isinstance(value, Real):
        return None
    return value


def _usage(response: Any) -> int | float:
    usage = _value(response, "usage")
    direct = _number(usage)
    if direct is not None:
        return direct

    for key in (
        "total_tokens",
        "total_token",
        "characters",
        "input_characters",
        "total_characters",
    ):
        amount = _number(_value(usage, key))
        if amount is not None:
            return amount

    token_parts = [
        _number(_value(usage, "input_tokens")),
        _number(_value(usage, "output_tokens")),
    ]
    if any(part is not None for part in token_parts):
        return sum(part or 0 for part in token_parts)
    return 0


def _request_id(response: Any) -> str | None:
    request_id = _value(response, "request_id")
    if request_id is None:
        request_id = _value(response, "requestId")
    return str(request_id) if request_id is not None else None


_TRANSIENT_CODES = (
    "throttl",
    "rate_limit",
    "ratelimit",
    "timeout",
    "timed_out",
    "internal",
    "unavailable",
    "service_error",
    "network",
    "connection",
)


def _status_code(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _is_success(response: Any) -> bool:
    status = _status_code(_value(response, "status_code"))
    code = _value(response, "code")
    status_ok = status is None or 200 <= status < 300
    code_ok = code in (None, "", 0, "0", "OK", "Success", "200", 200)
    return status_ok and code_ok


def _safe_code(value: Any, fallback: str) -> str:
    if value in (None, ""):
        return fallback
    code = str(value)
    # Provider codes are identifiers. Refuse arbitrary response prose here.
    if len(code) > 80 or any(char.isspace() for char in code):
        return fallback
    return code


def _provider_error(response: Any, fallback: str = "provider_error") -> AlibabaProviderError:
    status = _status_code(_value(response, "status_code"))
    code = _safe_code(_value(response, "code"), fallback)
    normalized = code.lower()
    transient = (
        status in (408, 409, 425, 429)
        or (status is not None and status >= 500)
        or any(marker in normalized for marker in _TRANSIENT_CODES)
    )
    error_type = TransientAlibabaError if transient else PermanentAlibabaError
    return error_type(
        code=code,
        status_code=status,
        request_id=_request_id(response),
    )


def _transport_failure(exc: Exception) -> AlibabaProviderError:
    if isinstance(exc, AlibabaProviderError):
        return exc
    status = _status_code(getattr(exc, "status_code", None))
    code = _safe_code(getattr(exc, "code", None), type(exc).__name__)
    if isinstance(exc, (TimeoutError, ConnectionError)):
        return TransientAlibabaError(code=code, status_code=status)
    synthetic = {"code": code, "status_code": status}
    return _provider_error(synthetic, fallback="transport_error")


def _call(transport: DashScopeTransport | Callable[..., Any], **kwargs: Any) -> Any:
    try:
        method = getattr(transport, "call", None)
        if method is not None:
            return method(**kwargs)
        if callable(transport):
            return transport(**kwargs)
        raise TypeError("transport must be callable or expose call")
    except Exception as exc:
        raise _transport_failure(exc) from exc


class QwenTranslationAdapter:
    """Pinned English-to-Chinese Qwen MT adapter for the podcast pipeline."""

    model = QWEN_MT_MODEL
    region = BEIJING_REGION
    endpoint = BEIJING_ENDPOINT
    source_language = "en"
    target_language = "zh"

    def __init__(self, transport: DashScopeTransport | Callable[..., Any], api_key: str | None = None) -> None:
        self._transport = transport
        self._api_key = api_key

    def translate(
        self,
        text: str,
        *,
        glossary: Any = None,
        context: Any = None,
    ) -> TranslationResult:
        if not isinstance(text, str) or not text.strip():
            raise ValueError("translation text must be non-empty")

        options: dict[str, Any] = {
            "source_lang": self.source_language,
            "target_lang": self.target_language,
        }
        if glossary is not None:
            options["terms"] = glossary
        if context is not None:
            options["domains"] = context

        kwargs: dict[str, Any] = {
            "model": self.model,
            "messages": [{"role": "user", "content": text}],
            "result_format": "message",
            "translation_options": options,
        }
        if self._api_key is not None:
            kwargs["api_key"] = self._api_key

        response = _call(self._transport, **kwargs)
        if not _is_success(response):
            raise _provider_error(response)

        translated = _path(response, "output", "choices")
        if translated:
            translated = _path(translated[0], "message", "content")
        if translated is None:
            translated = _path(response, "output", "text")
        if not isinstance(translated, str) or not translated:
            raise TransientAlibabaError(
                code="invalid_translation_response",
                request_id=_request_id(response),
            )
        return TranslationResult(
            text=translated,
            usage=_usage(response),
            request_id=_request_id(response),
        )


class QwenTTSAdapter:
    """Pinned Mandarin Andre voice adapter using DashScope streaming output."""

    model = QWEN_TTS_MODEL
    region = BEIJING_REGION
    endpoint = BEIJING_ENDPOINT
    voice = QWEN_TTS_VOICE
    language = QWEN_TTS_LANGUAGE
    soft_text_limit = TTS_SOFT_TEXT_LIMIT
    hard_text_limit = TTS_HARD_TEXT_LIMIT

    def __init__(
        self,
        transport: DashScopeTransport | Callable[..., Any],
        downloader: Callable[[str], Any] | None = None,
        api_key: str | None = None,
    ) -> None:
        self._transport = transport
        self._downloader = downloader
        self._api_key = api_key

    def synthesize(self, text: str) -> SpeechResult:
        if not isinstance(text, str) or not text:
            raise ValueError("TTS text must be non-empty")
        if len(text) > self.hard_text_limit:
            raise ValueError(f"TTS text exceeds hard limit of {self.hard_text_limit} characters")

        kwargs: dict[str, Any] = {
            "model": self.model,
            "text": text,
            "voice": self.voice,
            "language_type": self.language,
            "stream": True,
        }
        if self._api_key is not None:
            kwargs["api_key"] = self._api_key

        response = _call(self._transport, **kwargs)
        events: Iterable[Any]
        if isinstance(response, (Mapping, str, bytes, bytearray)):
            events = (response,)
        else:
            try:
                events = iter(response)
            except TypeError:
                events = (response,)

        final_url: str | None = None
        usage: int | float = 0
        request_id: str | None = None

        try:
            for event in events:
                if not _is_success(event):
                    raise _provider_error(event)
                request_id = _request_id(event) or request_id
                event_usage = _usage(event)
                if event_usage:
                    usage = event_usage

                encoded = _path(event, "output", "audio", "data")
                if encoded:
                    if isinstance(encoded, str) and "," in encoded and encoded.startswith("data:"):
                        encoded = encoded.split(",", 1)[1]
                    try:
                        # Validate intermediate chunks but do not persist them:
                        # Qwen3-TTS streaming data is raw playback audio, while
                        # the final event URL is the complete WAV artifact.
                        base64.b64decode(encoded, validate=True)
                    except (binascii.Error, TypeError, ValueError) as exc:
                        raise TransientAlibabaError(
                            code="invalid_audio_data",
                            request_id=request_id,
                        ) from exc

                url = _path(event, "output", "audio", "url")
                if isinstance(url, str) and url:
                    final_url = url
        except AlibabaProviderError:
            raise
        except Exception as exc:
            raise _transport_failure(exc) from exc

        if final_url:
            audio = self._download(final_url, request_id)
        else:
            raise TransientAlibabaError(
                code="missing_complete_audio_url", request_id=request_id
            )

        return SpeechResult(audio=audio, usage=usage, request_id=request_id)

    def _download(self, url: str, request_id: str | None) -> bytes:
        if self._downloader is None:
            raise PermanentAlibabaError(
                code="downloader_required",
                request_id=request_id,
            )
        try:
            downloaded = self._downloader(url)
            raise_for_status = getattr(downloaded, "raise_for_status", None)
            if raise_for_status is not None:
                raise_for_status()
            content = getattr(downloaded, "content", downloaded)
            if not isinstance(content, (bytes, bytearray)) or not content:
                raise ValueError("empty download")
            return bytes(content)
        except AlibabaProviderError:
            raise
        except Exception as exc:
            status = _status_code(getattr(getattr(exc, "response", None), "status_code", None))
            error_type = PermanentAlibabaError if status is not None and 400 <= status < 500 and status != 429 else TransientAlibabaError
            raise error_type(
                code="audio_download_failed",
                status_code=status,
                request_id=request_id,
            ) from exc


# Descriptive aliases for callers that do not need to mention the model family.
AlibabaTranslationAdapter = QwenTranslationAdapter
AlibabaTTSAdapter = QwenTTSAdapter
TTSResult = SpeechResult
