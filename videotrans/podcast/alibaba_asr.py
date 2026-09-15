"""Alibaba Model Studio whole-file asynchronous ASR client.

The client owns the submit/poll protocol but delegates I/O to a transport.  This
keeps orchestration tests offline and lets production code choose either HTTP or
the DashScope SDK.  Credentials belong to the transport and are never copied
into task or transcript results.
"""

from __future__ import annotations

import json
import re
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

MODEL = "qwen-audio-3.0-asr-flash-filetrans"
RUNNING_STATUSES = frozenset({"PENDING", "RUNNING"})


class AlibabaAsrTransport(Protocol):
    """Transport boundary implemented by HTTP, an SDK, or an offline fake."""

    def submit(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        """Submit one whole-file transcription task."""

    def poll(self, task_id: str) -> Mapping[str, Any]:
        """Fetch the current state of an existing task without resubmitting."""

    def download_result(self, url: str) -> Mapping[str, Any]:
        """Download the short-lived JSON result referenced by a successful task."""


@dataclass(frozen=True)
class PollingPolicy:
    """Bounded polling cadence recommended for a single personal pipeline."""

    initial_seconds: float = 2.0
    maximum_seconds: float = 5.0
    multiplier: float = 1.5

    def __post_init__(self) -> None:
        if not 2.0 <= self.initial_seconds <= 5.0:
            raise ValueError("initial_seconds must be between 2 and 5")
        if not self.initial_seconds <= self.maximum_seconds <= 5.0:
            raise ValueError("maximum_seconds must be between initial_seconds and 5")
        if self.multiplier < 1.0:
            raise ValueError("multiplier must be at least 1")

    def delay_for_attempt(self, attempt: int) -> float:
        """Return the delay after a non-terminal poll, starting at attempt zero."""

        if attempt < 0:
            raise ValueError("attempt must not be negative")
        return min(
            self.maximum_seconds,
            self.initial_seconds * (self.multiplier**attempt),
        )


@dataclass(frozen=True)
class SubmittedAsrTask:
    """Small credential-free record suitable for persisting as a checkpoint."""

    task_id: str
    status: str
    request_id: str | None = None

    def checkpoint(self) -> dict[str, str]:
        return {"task_id": self.task_id}


@dataclass(frozen=True)
class TranscriptSegment:
    begin_ms: int
    end_ms: int
    text: str
    speaker_id: int | str | None = None
    channel_id: int | None = None


@dataclass(frozen=True)
class AsrUsage:
    """Normalized numeric metering returned by Model Studio."""

    duration_seconds: float | None = None


@dataclass(frozen=True)
class AsrPollResult:
    task_id: str
    status: str
    segments: tuple[TranscriptSegment, ...] = ()
    usage: AsrUsage = AsrUsage()
    request_id: str | None = None

    @property
    def is_complete(self) -> bool:
        return self.status == "SUCCEEDED"


class AlibabaAsrError(RuntimeError):
    """Base class containing only redacted, persistable error metadata."""

    retryable = False

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        code: str | None = None,
    ) -> None:
        self.status_code = status_code
        self.code = code
        super().__init__(_redact_sensitive(message))


class TransientAlibabaAsrError(AlibabaAsrError):
    retryable = True


class PermanentAlibabaAsrError(AlibabaAsrError):
    retryable = False


class AlibabaAsrTransportError(RuntimeError):
    """Neutral transport failure that the client classifies before exposing."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        code: str | None = None,
    ) -> None:
        self.status_code = status_code
        self.code = code
        super().__init__(_redact_sensitive(message))


_TRANSIENT_STATUS_CODES = frozenset({408, 409, 425, 429, 500, 502, 503, 504})
_TRANSIENT_CODE_MARKERS = (
    "THROTTL",
    "RATE_LIMIT",
    "TIMEOUT",
    "INTERNAL",
    "SERVICE_UNAVAILABLE",
    "SYSTEM_ERROR",
    "TEMPORAR",
    "NETWORK",
    "TOO_MANY",
)
_PERMANENT_CODE_MARKERS = (
    "INVALID",
    "AUTH",
    "ACCESS_DENIED",
    "PERMISSION",
    "NOT_FOUND",
    "UNSUPPORTED",
    "BAD_REQUEST",
    "FILE_DOWNLOAD_FAILED",
)


def classify_error(error: Exception) -> AlibabaAsrError:
    """Convert transport/provider failures into retryable or permanent errors."""

    if isinstance(error, AlibabaAsrError):
        return error

    status_code = getattr(error, "status_code", None)
    code = getattr(error, "code", None)
    normalized_code = str(code or "").upper()
    message = str(error) or error.__class__.__name__
    details = {"status_code": status_code, "code": str(code) if code else None}

    if isinstance(error, (TimeoutError, ConnectionError, URLError)):
        return TransientAlibabaAsrError(message, **details)
    if status_code in _TRANSIENT_STATUS_CODES or any(
        marker in normalized_code for marker in _TRANSIENT_CODE_MARKERS
    ):
        return TransientAlibabaAsrError(message, **details)
    if (
        isinstance(status_code, int)
        and 400 <= status_code < 500
        or any(marker in normalized_code for marker in _PERMANENT_CODE_MARKERS)
    ):
        return PermanentAlibabaAsrError(message, **details)
    return PermanentAlibabaAsrError(message, **details)


class AlibabaWholeFileAsrClient:
    """Submit one audio URL and resume it later using only its saved task ID."""

    def __init__(
        self,
        transport: AlibabaAsrTransport,
        *,
        polling_policy: PollingPolicy | None = None,
    ) -> None:
        self._transport = transport
        self.polling_policy = polling_policy or PollingPolicy()

    def submit(
        self,
        audio_url: str,
        *,
        language: str = "en",
        speaker_count: int | None = None,
    ) -> SubmittedAsrTask:
        """Submit exactly one request and return the task ID for immediate storage."""

        if not audio_url:
            raise ValueError("audio_url is required")
        if speaker_count is not None and not 2 <= speaker_count <= 100:
            raise ValueError("speaker_count must be between 2 and 100")

        parameters: dict[str, Any] = {
            "language_hints": [language],
            "channel_id": [0],
            "diarization_enabled": True,
        }
        if speaker_count is not None:
            parameters["speaker_count"] = speaker_count

        payload = {
            "model": MODEL,
            "input": {"file_urls": [audio_url]},
            "parameters": parameters,
        }
        response = self._call(self._transport.submit, payload)
        output = _mapping(response.get("output"), "submit output")
        task_id = _required_string(output.get("task_id"), "task_id")
        status = _required_string(output.get("task_status"), "task_status").upper()
        if status not in RUNNING_STATUSES:
            raise PermanentAlibabaAsrError(
                f"unexpected submit status: {status}", code="MALFORMED_RESPONSE"
            )
        return SubmittedAsrTask(
            task_id=task_id,
            status=status,
            request_id=_optional_string(response.get("request_id")),
        )

    def poll(self, task_id: str) -> AsrPollResult:
        """Poll an existing task. This method never calls submit."""

        if not task_id:
            raise ValueError("task_id is required")
        response = self._call(self._transport.poll, task_id)
        output = _mapping(response.get("output"), "poll output")
        returned_task_id = _required_string(output.get("task_id"), "task_id")
        if returned_task_id != task_id:
            raise PermanentAlibabaAsrError(
                "poll response task_id did not match the requested task",
                code="MALFORMED_RESPONSE",
            )

        status = _required_string(output.get("task_status"), "task_status").upper()
        request_id = _optional_string(response.get("request_id"))
        usage = _parse_usage(response.get("usage"))
        if status in RUNNING_STATUSES:
            return AsrPollResult(
                task_id=task_id,
                status=status,
                usage=usage,
                request_id=request_id,
            )
        if status != "SUCCEEDED":
            raise _provider_error(output, fallback=f"ASR task ended as {status}")

        result_documents = self._download_successful_results(output)
        segments = tuple(
            segment
            for document in result_documents
            for segment in _parse_segments(document)
        )
        return AsrPollResult(
            task_id=task_id,
            status=status,
            segments=segments,
            usage=usage,
            request_id=request_id,
        )

    def wait_for_completion(
        self,
        task_id: str,
        *,
        sleeper: Callable[[float], None] = time.sleep,
        max_polls: int | None = None,
    ) -> AsrPollResult:
        """Poll to completion using 2–5 second backoff; inject a no-op in tests."""

        attempt = 0
        while True:
            result = self.poll(task_id)
            if result.is_complete:
                return result
            attempt += 1
            if max_polls is not None and attempt >= max_polls:
                raise TransientAlibabaAsrError(
                    "ASR task did not finish within the polling budget",
                    code="POLL_BUDGET_EXHAUSTED",
                )
            sleeper(self.polling_policy.delay_for_attempt(attempt - 1))

    def _download_successful_results(
        self, output: Mapping[str, Any]
    ) -> list[Mapping[str, Any]]:
        raw_results = output.get("results")
        if not isinstance(raw_results, Sequence) or isinstance(raw_results, (str, bytes)):
            raise PermanentAlibabaAsrError(
                "successful task had no results", code="MALFORMED_RESPONSE"
            )

        documents: list[Mapping[str, Any]] = []
        failures: list[AlibabaAsrError] = []
        for raw_result in raw_results:
            result = _mapping(raw_result, "subtask result")
            subtask_status = _required_string(
                result.get("subtask_status"), "subtask_status"
            ).upper()
            if subtask_status != "SUCCEEDED":
                failures.append(_provider_error(result, fallback="ASR subtask failed"))
                continue
            result_url = _required_string(
                result.get("transcription_url"), "transcription_url"
            )
            documents.append(self._call(self._transport.download_result, result_url))

        if not documents:
            if failures:
                raise failures[0]
            raise PermanentAlibabaAsrError(
                "successful task contained no successful subtasks",
                code="MALFORMED_RESPONSE",
            )
        return documents

    @staticmethod
    def _call(function: Callable[..., Mapping[str, Any]], *args: Any) -> Mapping[str, Any]:
        try:
            response = function(*args)
        except Exception as error:
            classified = classify_error(error)
            raise classified from error
        return _mapping(response, "transport response")


class AlibabaAsrHttpTransport:
    """Minimal Beijing HTTP transport with a non-retained API-key provider."""

    def __init__(
        self,
        workspace_id: str | None,
        api_key_provider: Callable[[], str] | None,
        *,
        base_url: str | None = None,
        timeout_seconds: float = 30.0,
        opener: Callable[..., Any] = urlopen,
    ) -> None:
        if api_key_provider is None:
            raise ValueError("api_key_provider is required")
        if base_url:
            self._base_url = base_url.rstrip("/")
        elif workspace_id:
            self._base_url = (
                f"https://{workspace_id}.cn-beijing.maas.aliyuncs.com/api/v1"
            )
        else:
            self._base_url = "https://dashscope.aliyuncs.com/api/v1"
        self._api_key_provider = api_key_provider
        self._timeout_seconds = timeout_seconds
        self._opener = opener

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(region='cn-beijing')"

    def submit(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        file_urls = payload.get("input", {}).get("file_urls", [])
        resolve_oss = any(
            isinstance(url, str) and url.startswith("oss://") for url in file_urls
        )
        return self._request(
            "POST",
            f"{self._base_url}/services/audio/asr/transcription",
            payload=payload,
            asynchronous=True,
            resolve_oss=resolve_oss,
        )

    def poll(self, task_id: str) -> Mapping[str, Any]:
        return self._request("GET", f"{self._base_url}/tasks/{task_id}")

    def download_result(self, url: str) -> Mapping[str, Any]:
        return self._request("GET", url, authenticated=False)

    def _request(
        self,
        method: str,
        url: str,
        *,
        payload: Mapping[str, Any] | None = None,
        asynchronous: bool = False,
        resolve_oss: bool = False,
        authenticated: bool = True,
    ) -> Mapping[str, Any]:
        headers = {"Content-Type": "application/json"}
        if authenticated:
            api_key = self._api_key_provider()
            if not api_key:
                raise PermanentAlibabaAsrError(
                    "API key provider returned no credential", code="MISSING_API_KEY"
                )
            headers["Authorization"] = f"Bearer {api_key}"
        if asynchronous:
            headers["X-DashScope-Async"] = "enable"
        if resolve_oss:
            headers["X-DashScope-OssResourceResolve"] = "enable"
        body = json.dumps(payload).encode("utf-8") if payload is not None else None
        request = Request(url, data=body, headers=headers, method=method)
        try:
            with self._opener(request, timeout=self._timeout_seconds) as response:
                return _decode_json(response.read())
        except HTTPError as error:
            error_payload = _safe_error_payload(error)
            raise AlibabaAsrTransportError(
                error_payload.get("message") or f"HTTP {error.code}",
                status_code=error.code,
                code=_optional_string(error_payload.get("code")),
            ) from error
        except URLError as error:
            raise AlibabaAsrTransportError(
                str(error.reason), code="NETWORK_ERROR"
            ) from error


def _parse_segments(document: Mapping[str, Any]) -> list[TranscriptSegment]:
    transcripts = document.get("transcripts")
    if not isinstance(transcripts, Sequence) or isinstance(transcripts, (str, bytes)):
        raise PermanentAlibabaAsrError(
            "transcription result had no transcripts", code="MALFORMED_RESPONSE"
        )

    segments: list[TranscriptSegment] = []
    for raw_transcript in transcripts:
        transcript = _mapping(raw_transcript, "transcript")
        channel_id = _optional_int(transcript.get("channel_id"), "channel_id")
        sentences = transcript.get("sentences")
        if not isinstance(sentences, Sequence) or isinstance(sentences, (str, bytes)):
            raise PermanentAlibabaAsrError(
                "transcript had no sentences", code="MALFORMED_RESPONSE"
            )
        for raw_sentence in sentences:
            sentence = _mapping(raw_sentence, "sentence")
            begin_ms = _required_int(sentence.get("begin_time"), "begin_time")
            end_ms = _required_int(sentence.get("end_time"), "end_time")
            if begin_ms < 0 or end_ms < begin_ms:
                raise PermanentAlibabaAsrError(
                    "sentence timestamps were invalid", code="MALFORMED_RESPONSE"
                )
            text = _required_string(sentence.get("text"), "sentence text").strip()
            if not text:
                continue
            speaker_id = sentence.get("speaker_id")
            if speaker_id is not None and not isinstance(speaker_id, (int, str)):
                raise PermanentAlibabaAsrError(
                    "speaker_id was not numeric or textual",
                    code="MALFORMED_RESPONSE",
                )
            segments.append(
                TranscriptSegment(
                    begin_ms=begin_ms,
                    end_ms=end_ms,
                    text=text,
                    speaker_id=speaker_id,
                    channel_id=channel_id,
                )
            )
    return segments


def _parse_usage(value: Any) -> AsrUsage:
    if value is None:
        return AsrUsage()
    usage = _mapping(value, "usage")
    duration = usage.get("duration")
    if duration is None:
        return AsrUsage()
    if isinstance(duration, bool):
        raise PermanentAlibabaAsrError(
            "usage duration was not numeric", code="MALFORMED_RESPONSE"
        )
    try:
        numeric_duration = float(duration)
    except (TypeError, ValueError) as error:
        raise PermanentAlibabaAsrError(
            "usage duration was not numeric", code="MALFORMED_RESPONSE"
        ) from error
    if numeric_duration < 0:
        raise PermanentAlibabaAsrError(
            "usage duration was negative", code="MALFORMED_RESPONSE"
        )
    return AsrUsage(duration_seconds=numeric_duration)


def _provider_error(
    payload: Mapping[str, Any], *, fallback: str
) -> AlibabaAsrError:
    code = _optional_string(payload.get("code"))
    message = _optional_string(payload.get("message")) or fallback
    return classify_error(AlibabaAsrTransportError(message, code=code))


def _mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise PermanentAlibabaAsrError(
            f"{field} was not an object", code="MALFORMED_RESPONSE"
        )
    return value


def _required_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PermanentAlibabaAsrError(
            f"{field} was missing", code="MALFORMED_RESPONSE"
        )
    return value.strip()


def _optional_string(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _required_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PermanentAlibabaAsrError(
            f"{field} was not numeric", code="MALFORMED_RESPONSE"
        )
    return int(value)


def _optional_int(value: Any, field: str) -> int | None:
    if value is None:
        return None
    return _required_int(value, field)


def _decode_json(raw: bytes) -> Mapping[str, Any]:
    try:
        return _mapping(json.loads(raw.decode("utf-8")), "HTTP response")
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise AlibabaAsrTransportError(
            "response was not valid JSON", code="MALFORMED_RESPONSE"
        ) from error


def _safe_error_payload(error: HTTPError) -> Mapping[str, Any]:
    try:
        return _decode_json(error.read())
    except Exception:  # noqa: BLE001 - provider bodies are optional and untrusted
        return {}


def _redact_sensitive(message: str) -> str:
    redacted = re.sub(r"(?i)Bearer\s+[^\s,;]+", "Bearer [REDACTED]", message)
    redacted = re.sub(
        r"(?i)(accesskeyid|signature|api[_-]?key|token)=([^&\s]+)",
        r"\1=[REDACTED]",
        redacted,
    )
    redacted = re.sub(
        r'(?i)(["\']?(?:api[_-]?key|token|signature)["\']?\s*:\s*)["\']?[^"\'\s,}]+',
        r"\1[REDACTED]",
        redacted,
    )
    redacted = re.sub(r"\bsk-[A-Za-z0-9_-]+", "sk-[REDACTED]", redacted)
    return redacted
