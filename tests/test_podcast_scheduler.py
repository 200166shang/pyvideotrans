import asyncio

import pytest

from videotrans.podcast.scheduler import (
    AsyncScheduler,
    SmoothTokenBucket,
    TransientError,
    is_transient_error,
    schedule_ordered,
)


def test_retryable_attribute_is_honored() -> None:
    class ProviderFailure(Exception):
        retryable = True

    assert is_transient_error(ProviderFailure())


class FakeTime:
    def __init__(self):
        self.value = 0.0
        self.sleeps = []

    def monotonic(self):
        return self.value

    async def sleep(self, delay):
        self.sleeps.append(delay)
        self.value += delay
        await asyncio.sleep(0)


def test_token_bucket_starts_empty_and_smooths_requests():
    fake = FakeTime()
    bucket = SmoothTokenBucket(
        requests_per_minute=150,
        clock=fake.monotonic,
        sleeper=fake.sleep,
    )

    async def scenario():
        await bucket.acquire()
        await bucket.acquire()
        await bucket.acquire()

    asyncio.run(scenario())

    assert fake.sleeps == pytest.approx([0.4, 0.4, 0.4])


def test_scheduler_bounds_concurrency_and_keeps_input_order():
    active = 0
    peak = 0

    async def operation(value):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep((8 - value) * 0.001)
        active -= 1
        return value * 10

    async def no_wait(_delay):
        await asyncio.sleep(0)

    result = asyncio.run(
        schedule_ordered(
            range(8),
            operation,
            concurrency=4,
            requests_per_minute=60_000_000,
            sleeper=no_wait,
        )
    )

    assert peak == 4
    assert result == [value * 10 for value in range(8)]


def test_every_transient_retry_reacquires_a_rate_token():
    fake = FakeTime()
    attempts = 0

    async def operation(_value):
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise TransientError("try again")
        return "ok"

    scheduler = AsyncScheduler(
        requests_per_minute=60,
        clock=fake.monotonic,
        sleeper=fake.sleep,
        jitter=lambda: 0,
    )

    assert asyncio.run(scheduler.run([1], operation)) == ["ok"]
    assert attempts == 3
    assert fake.sleeps == pytest.approx([1.0, 1.0, 1.0])


def test_retry_uses_exponential_full_jitter():
    fake = FakeTime()
    attempts = 0

    async def operation(_value):
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ConnectionError("temporary")
        return "ok"

    scheduler = AsyncScheduler(
        requests_per_minute=60_000_000,
        retry_base_seconds=1,
        retry_max_seconds=10,
        clock=fake.monotonic,
        sleeper=fake.sleep,
        jitter=lambda: 0.5,
    )

    assert asyncio.run(scheduler.run([1], operation)) == ["ok"]
    retry_sleeps = [delay for delay in fake.sleeps if delay > 0.01]
    assert retry_sleeps == pytest.approx([0.5, 1.0])


def test_default_allows_five_retries_after_the_first_attempt():
    attempts = 0

    async def operation(_value):
        nonlocal attempts
        attempts += 1
        raise TransientError("still unavailable")

    async def no_wait(_delay):
        await asyncio.sleep(0)

    scheduler = AsyncScheduler(
        requests_per_minute=60_000_000,
        sleeper=no_wait,
        jitter=lambda: 0,
    )

    with pytest.raises(TransientError):
        asyncio.run(scheduler.run([1], operation))
    assert attempts == 6


def test_permanent_failures_are_not_retried():
    attempts = 0

    async def operation(_value):
        nonlocal attempts
        attempts += 1
        raise ValueError("invalid request")

    async def no_wait(_delay):
        await asyncio.sleep(0)

    scheduler = AsyncScheduler(
        requests_per_minute=60_000_000,
        sleeper=no_wait,
    )

    with pytest.raises(ValueError, match="invalid request"):
        asyncio.run(scheduler.run([1], operation))
    assert attempts == 1


class HttpError(Exception):
    def __init__(self, status_code):
        self.status_code = status_code


def test_transient_classifier_handles_expected_http_statuses():
    assert is_transient_error(HttpError(408))
    assert is_transient_error(HttpError(429))
    assert is_transient_error(HttpError(503))
    assert not is_transient_error(HttpError(400))
