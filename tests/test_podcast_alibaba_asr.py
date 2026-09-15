from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict
from typing import Any

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


def test_poll_exposes_content_free_phase_and_provider_durations() -> None:
    transport = FakeTransport(
        poll_response={
            "output": {
                "task_id": "task-123",
                "task_status": "SUCCEEDED",
                "submit_time": "2026-09-15T10:00:00+00:00",
                "scheduled_time": "2026-09-15T10:00:01+00:00",
                "end_time": "2026-09-15T10:00:03.500000+00:00",
                "results": [
                    {
                        "subtask_status": "SUCCEEDED",
                        "transcription_url": "https://result.example/temporary.json",
                    }
                ],
            }
        },
        result_response={
            "transcripts": [
                {
                    "sentences": [
                        {
                            "begin_time": 0,
                            "end_time": 1,
                            "text": "one",
                        }
                    ]
                }
            ]
        },
    )
    clock_values = iter((0.0, 0.1, 0.1, 0.3, 0.3, 0.35))
    client = AlibabaWholeFileAsrClient(transport, clock=lambda: next(clock_values))

    result = client.poll("task-123")

    assert result.diagnostics.status_query_count == 1
    assert result.diagnostics.status_query_elapsed_ms == 100
    assert result.diagnostics.result_download_count == 1
    assert result.diagnostics.result_download_elapsed_ms == 200
    assert result.diagnostics.result_parse_count == 1
    assert result.diagnostics.result_parse_elapsed_ms == 50
    assert result.diagnostics.provider_queue_elapsed_ms == 1000
    assert result.diagnostics.provider_task_elapsed_ms == 2500
    assert "temporary.json" not in repr(result.diagnostics)


@pytest.mark.parametrize(
    "timestamps",
    [
        {},
        {"submit_time": "2026-09-15", "scheduled_time": "2026-09-15", "end_time": "2026-09-15"},
        {"submit_time": "bad", "scheduled_time": "bad", "end_time": "bad"},
        {
            "submit_time": "2026-09-15T10:00:03+00:00",
            "scheduled_time": "2026-09-15T10:00:01+00:00",
            "end_time": "2026-09-15T10:00:02+00:00",
        },
        {
            "submit_time": "2026-09-15T10:00:00",
            "scheduled_time": "2026-09-15T10:00:01+00:00",
            "end_time": "2026-09-15T10:00:02+00:00",
        },
    ],
)
def test_poll_ignores_unusable_provider_timestamps(timestamps: Mapping[str, str]) -> None:
    transport = FakeTransport(
        poll_response={
            "output": {
                "task_id": "task-123",
                "task_status": "RUNNING",
                **timestamps,
            }
        }
    )

    result = AlibabaWholeFileAsrClient(transport).poll("task-123")

    assert result.diagnostics.provider_queue_elapsed_ms is None
    assert result.diagnostics.provider_task_elapsed_ms is None


def test_failed_poll_retains_its_content_free_query_measurement() -> None:
    client = AlibabaWholeFileAsrClient(FakeTransport())

    with pytest.raises(PermanentAlibabaAsrError):
        client.poll("task-123")

    assert client.last_poll_diagnostics.status_query_count == 1
    assert client.last_poll_diagnostics.result_download_count == 0
    assert client.last_poll_diagnostics.result_parse_count == 0


@pytest.mark.parametrize("failure_phase", ["query", "download", "parse"])
def test_failed_poll_measures_attempted_subphases_without_stale_values(failure_phase):
    class Transport(FakeTransport):
        def poll(self, task_id):
            if failure_phase == "query":
                raise AlibabaAsrTransportError("offline failure", status_code=503)
            return {"output": {"task_id": task_id, "task_status": "SUCCEEDED",
                               "results": [{"subtask_status": "SUCCEEDED",
                                            "transcription_url": "https://private.invalid"}]}}

        def download_result(self, url):
            if failure_phase == "download":
                raise AlibabaAsrTransportError("offline failure", status_code=503)
            return {"transcripts": "invalid"}

    # Two polls ensure the latest record does not contain cumulative old phases.
    times = iter(i / 10 for i in range(20))
    client = AlibabaWholeFileAsrClient(Transport(), clock=lambda: next(times))
    for _ in range(2):
        with pytest.raises((TransientAlibabaAsrError, PermanentAlibabaAsrError)):
            client.poll("task-123")
        diagnostic = client.last_poll_diagnostics
        assert diagnostic.status_query_elapsed_ms == 100
        assert diagnostic.status_query_count == 1
        assert diagnostic.result_download_count == (0 if failure_phase == "query" else 1)
        assert diagnostic.result_download_elapsed_ms == (0 if failure_phase == "query" else 100)
        assert diagnostic.result_parse_count == (1 if failure_phase == "parse" else 0)
        assert diagnostic.result_parse_elapsed_ms == (100 if failure_phase == "parse" else 0)


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
