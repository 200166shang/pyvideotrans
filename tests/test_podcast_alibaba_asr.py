from __future__ import annotations

from dataclasses import asdict
from typing import Any, Mapping

import pytest

from videotrans.podcast.alibaba_asr import (
    MODEL,
    AlibabaAsrTransportError,
    AlibabaWholeFileAsrClient,
    PermanentAlibabaAsrError,
    TransientAlibabaAsrError,
    classify_error,
)


class FakeTransport:
    def __init__(
        self,
        *,
        submit_response: Mapping[str, Any] | None = None,
        poll_response: Mapping[str, Any] | None = None,
        result_response: Mapping[str, Any] | None = None,
    ) -> None:
        self.submit_response = submit_response or {}
        self.poll_response = poll_response or {}
        self.result_response = result_response or {}
        self.submissions: list[Mapping[str, Any]] = []
        self.polled_task_ids: list[str] = []
        self.downloaded_urls: list[str] = []

    def submit(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        self.submissions.append(payload)
        return self.submit_response

    def poll(self, task_id: str) -> Mapping[str, Any]:
        self.polled_task_ids.append(task_id)
        return self.poll_response

    def download_result(self, url: str) -> Mapping[str, Any]:
        self.downloaded_urls.append(url)
        return self.result_response


def test_submit_once_returns_persistable_task_id() -> None:
    transport = FakeTransport(
        submit_response={
            "output": {"task_id": "task-123", "task_status": "PENDING"},
            "request_id": "request-123",
        }
    )
    client = AlibabaWholeFileAsrClient(transport)

    task = client.submit("oss://podcasts/episode.m4a", speaker_count=2)

    assert task.task_id == "task-123"
    assert task.checkpoint() == {"task_id": "task-123"}
    assert len(transport.submissions) == 1
    assert transport.submissions[0] == {
        "model": MODEL,
        "input": {"file_urls": ["oss://podcasts/episode.m4a"]},
        "parameters": {
            "language_hints": ["en"],
            "channel_id": [0],
            "diarization_enabled": True,
            "speaker_count": 2,
        },
    }
    assert "credential" not in asdict(task)
    assert "api_key" not in asdict(task)


def test_poll_running_uses_existing_task_without_submit_or_sleep() -> None:
    transport = FakeTransport(
        poll_response={
            "output": {"task_id": "task-123", "task_status": "RUNNING"},
            "request_id": "request-poll",
        }
    )
    client = AlibabaWholeFileAsrClient(transport)

    result = client.poll("task-123")

    assert result.status == "RUNNING"
    assert not result.is_complete
    assert result.segments == ()
    assert transport.polled_task_ids == ["task-123"]
    assert transport.submissions == []
    assert transport.downloaded_urls == []
    assert client.polling_policy.delay_for_attempt(0) == 2.0
    assert client.polling_policy.delay_for_attempt(10) == 5.0


def test_success_parses_timestamped_segments_and_numeric_usage() -> None:
    signed_result_url = (
        "https://result.example/transcript.json?"
        "OSSAccessKeyId=temporary&Signature=temporary-signature"
    )
    transport = FakeTransport(
        poll_response={
            "output": {
                "task_id": "task-123",
                "task_status": "SUCCEEDED",
                "results": [
                    {
                        "subtask_status": "SUCCEEDED",
                        "file_url": "oss://podcasts/episode.m4a",
                        "transcription_url": signed_result_url,
                    }
                ],
            },
            "usage": {"duration": "42.5"},
            "request_id": "request-done",
        },
        result_response={
            "file_url": "oss://podcasts/episode.m4a",
            "transcripts": [
                {
                    "channel_id": 0,
                    "text": "Welcome. Today we discuss testing.",
                    "sentences": [
                        {
                            "begin_time": 120,
                            "end_time": 980,
                            "text": "Welcome.",
                            "speaker_id": 0,
                        },
                        {
                            "begin_time": 1100,
                            "end_time": 3100,
                            "text": "Today we discuss testing.",
                            "speaker_id": 1,
                        },
                    ],
                }
            ],
        },
    )
    client = AlibabaWholeFileAsrClient(transport)

    result = client.poll("task-123")

    assert result.is_complete
    assert result.usage.duration_seconds == 42.5
    assert [asdict(segment) for segment in result.segments] == [
        {
            "begin_ms": 120,
            "end_ms": 980,
            "text": "Welcome.",
            "speaker_id": 0,
            "channel_id": 0,
        },
        {
            "begin_ms": 1100,
            "end_ms": 3100,
            "text": "Today we discuss testing.",
            "speaker_id": 1,
            "channel_id": 0,
        },
    ]
    assert transport.downloaded_urls == [signed_result_url]
    assert "OSSAccessKeyId" not in repr(result)
    assert "Signature" not in repr(result)


@pytest.mark.parametrize(
    ("error", "expected_type"),
    [
        (
            AlibabaAsrTransportError(
                "rate limited", status_code=429, code="Throttling"
            ),
            TransientAlibabaAsrError,
        ),
        (TimeoutError("timed out"), TransientAlibabaAsrError),
        (
            AlibabaAsrTransportError(
                "bad key", status_code=401, code="InvalidApiKey"
            ),
            PermanentAlibabaAsrError,
        ),
        (
            AlibabaAsrTransportError(
                "source unavailable", code="FILE_DOWNLOAD_FAILED"
            ),
            PermanentAlibabaAsrError,
        ),
    ],
)
def test_error_classification(
    error: Exception, expected_type: type[Exception]
) -> None:
    classified = classify_error(error)

    assert isinstance(classified, expected_type)
    assert classified.retryable is issubclass(
        expected_type, TransientAlibabaAsrError
    )
