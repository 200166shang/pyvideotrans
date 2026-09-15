import asyncio

import pytest

from videotrans.podcast.scheduler import (
    AsyncScheduler,
    ScheduledSuccess,
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


@pytest.mark.parametrize("drain_on_failure", [False, True])
def test_scheduler_bounds_concurrency_and_keeps_input_order(drain_on_failure):
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
            drain_on_failure=drain_on_failure,
        )
    )

    assert peak == 4
    assert result == [value * 10 for value in range(8)]


@pytest.mark.parametrize("drain_on_failure", [False, True])
def test_every_transient_retry_reacquires_a_rate_token(drain_on_failure):
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
        drain_on_failure=drain_on_failure,
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


def test_default_failure_behavior_still_cancels_in_flight_operations():
    second_started = asyncio.Event()
    second_cancelled = asyncio.Event()
    failure = ValueError("invalid request")

    async def operation(value):
        if value == 0:
            await second_started.wait()
            raise failure
        second_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            second_cancelled.set()
            raise

    async def no_wait(_delay):
        await asyncio.sleep(0)

    scheduler = AsyncScheduler(
        concurrency=2,
        requests_per_minute=60_000_000,
        initial_tokens=1,
        sleeper=no_wait,
    )

    with pytest.raises(ValueError) as captured:
        asyncio.run(scheduler.run([0, 1], operation))

    assert captured.value is failure
    assert second_cancelled.is_set()


def test_drain_on_failure_stops_launches_and_drains_successes_in_input_order():
    failure = ValueError("invalid request")
    later_failure = RuntimeError("another invalid request")
    started = []
    completed = []

    async def scenario():
        initial_attempts_started = asyncio.Event()
        release_later_failure = asyncio.Event()
        release_first_success = asyncio.Event()
        release_second_success = asyncio.Event()
        second_success_completed = asyncio.Event()

        async def operation(value):
            started.append(value)
            if len(started) == 4:
                initial_attempts_started.set()
            await initial_attempts_started.wait()
            if value == 0:
                raise failure
            if value == 1:
                await release_later_failure.wait()
                raise later_failure
            if value == 2:
                await release_first_success.wait()
            if value == 3:
                await release_second_success.wait()
            completed.append(value)
            if value == 3:
                second_success_completed.set()
            return value * 10

        async def no_wait(_delay):
            await asyncio.sleep(0)

        scheduler = AsyncScheduler(
            concurrency=4,
            requests_per_minute=60_000_000,
            initial_tokens=1,
            sleeper=no_wait,
            drain_on_failure=True,
        )
        run_task = asyncio.create_task(scheduler.run(range(7), operation))

        await initial_attempts_started.wait()
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert started == [0, 1, 2, 3]
        assert not run_task.done()

        release_second_success.set()
        await second_success_completed.wait()
        assert not run_task.done()
        release_later_failure.set()
        release_first_success.set()

        with pytest.raises(ValueError) as captured:
            await run_task
        return scheduler, captured.value

    scheduler, raised = asyncio.run(scenario())

    assert raised is failure
    assert started == [0, 1, 2, 3]
    assert completed == [3, 2]
    assert scheduler.successful_results == (
        ScheduledSuccess(index=2, item=2, result=20),
        ScheduledSuccess(index=3, item=3, result=30),
    )


def test_drain_on_failure_abandons_another_items_pending_retry():
    failure = ValueError("invalid request")
    retry_attempts = 0
    retry_sleep_cancelled = False

    async def scenario():
        retry_sleep_started = asyncio.Event()
        never_release_retry = asyncio.Event()

        async def sleeper(delay):
            nonlocal retry_sleep_cancelled
            if delay < 0.01:
                await asyncio.sleep(0)
                return
            retry_sleep_started.set()
            try:
                await never_release_retry.wait()
            except asyncio.CancelledError:
                retry_sleep_cancelled = True
                raise

        async def operation(value):
            nonlocal retry_attempts
            if value == 0:
                await retry_sleep_started.wait()
                raise failure
            retry_attempts += 1
            raise TransientError("try again")

        scheduler = AsyncScheduler(
            concurrency=2,
            requests_per_minute=60_000_000,
            initial_tokens=1,
            retry_base_seconds=1,
            sleeper=sleeper,
            jitter=lambda: 1,
            drain_on_failure=True,
        )
        with pytest.raises(ValueError) as captured:
            await scheduler.run([0, 1], operation)
        return captured.value

    raised = asyncio.run(scenario())

    assert raised is failure
    assert retry_attempts == 1
    assert retry_sleep_cancelled


class HttpError(Exception):
    def __init__(self, status_code):
        self.status_code = status_code


def test_transient_classifier_handles_expected_http_statuses():
    assert is_transient_error(HttpError(408))
    assert is_transient_error(HttpError(429))
    assert is_transient_error(HttpError(503))
    assert not is_transient_error(HttpError(400))
