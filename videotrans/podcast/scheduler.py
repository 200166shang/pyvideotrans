"""Bounded asynchronous scheduling primitives for podcast cloud stages."""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Generic, TypeVar

ItemT = TypeVar("ItemT")
ResultT = TypeVar("ResultT")

Clock = Callable[[], float]
Sleeper = Callable[[float], Awaitable[None]]
TransientPredicate = Callable[[BaseException], bool]
JitterSource = Callable[[], float]


class TransientError(Exception):
    """Explicitly marks an operation failure as safe to retry."""


@dataclass(frozen=True)
class ScheduledSuccess(Generic[ItemT, ResultT]):
    """A successful operation result identified by its input position."""

    index: int
    item: ItemT
    result: ResultT


class _DrainState:
    def __init__(self) -> None:
        self.stopped = asyncio.Event()
        self.first_failure: BaseException | None = None

    def fail(self, error: BaseException) -> None:
        if self.first_failure is None:
            self.first_failure = error
            self.stopped.set()


class SmoothTokenBucket:
    """A single-token-capacity bucket that spaces request starts evenly.

    The default zero-token startup means the first request waits one complete
    interval.  After an idle period one token can accumulate, but the bucket
    never permits a burst.
    """

    def __init__(
        self,
        requests_per_minute: float = 150,
        *,
        initial_tokens: float = 0,
        clock: Clock = time.monotonic,
        sleeper: Sleeper = asyncio.sleep,
    ) -> None:
        if requests_per_minute <= 0:
            raise ValueError("requests_per_minute must be greater than zero")
        if not 0 <= initial_tokens <= 1:
            raise ValueError("initial_tokens must be between zero and one")
        self.requests_per_minute = float(requests_per_minute)
        self.initial_tokens = float(initial_tokens)
        self._interval = 60.0 / self.requests_per_minute
        self._clock = clock
        self._sleeper = sleeper
        self._lock = asyncio.Lock()
        self._next_start: float | None = None

    async def acquire(self) -> None:
        """Wait until the next evenly spaced request-start slot."""

        async with self._lock:
            now = self._clock()
            if self._next_start is None:
                self._next_start = now + self._interval * (1 - self.initial_tokens)
            start_at = max(now, self._next_start)
            self._next_start = start_at + self._interval
            delay = max(0.0, start_at - now)
        if delay:
            await self._sleeper(delay)


def is_transient_error(error: BaseException) -> bool:
    """Classify common network and HTTP failures without provider imports."""

    if isinstance(error, (TransientError, TimeoutError, ConnectionError)):
        return True
    explicit = getattr(error, "transient", None)
    if isinstance(explicit, bool):
        return explicit
    retryable = getattr(error, "retryable", None)
    if isinstance(retryable, bool):
        return retryable

    status = _http_status(error)
    return status in (408, 429) or (status is not None and 500 <= status <= 599)


class AsyncScheduler(Generic[ItemT, ResultT]):
    """Run async operations with bounded concurrency, pacing, and retries.

    ``drain_on_failure`` keeps attempts already inside ``operation`` alive after
    the first permanent failure.  Their ordered successes remain available in
    ``successful_results`` after that original failure is raised.
    """

    def __init__(
        self,
        *,
        concurrency: int = 4,
        requests_per_minute: float = 150,
        initial_tokens: float = 0,
        max_retries: int = 5,
        retry_base_seconds: float = 1,
        retry_max_seconds: float = 60,
        transient_predicate: TransientPredicate = is_transient_error,
        clock: Clock = time.monotonic,
        sleeper: Sleeper = asyncio.sleep,
        jitter: JitterSource = random.random,
        drain_on_failure: bool = False,
    ) -> None:
        if concurrency <= 0:
            raise ValueError("concurrency must be greater than zero")
        if max_retries < 0:
            raise ValueError("max_retries cannot be negative")
        if retry_base_seconds < 0:
            raise ValueError("retry_base_seconds cannot be negative")
        if retry_max_seconds < retry_base_seconds:
            raise ValueError("retry_max_seconds cannot be less than retry_base_seconds")

        self.concurrency = concurrency
        self.max_retries = max_retries
        self.retry_base_seconds = float(retry_base_seconds)
        self.retry_max_seconds = float(retry_max_seconds)
        self._transient_predicate = transient_predicate
        self._sleeper = sleeper
        self._jitter = jitter
        self.drain_on_failure = drain_on_failure
        self.successful_results: tuple[ScheduledSuccess[ItemT, ResultT], ...] = ()
        self._semaphore = asyncio.Semaphore(concurrency)
        self._limiter = SmoothTokenBucket(
            requests_per_minute,
            initial_tokens=initial_tokens,
            clock=clock,
            sleeper=sleeper,
        )

    async def run(
        self,
        items: Iterable[ItemT],
        operation: Callable[[ItemT], Awaitable[ResultT]],
    ) -> list[ResultT]:
        """Return results in input order, regardless of completion order."""

        indexed_items = list(enumerate(items))
        self.successful_results = ()
        if self.drain_on_failure:
            return await self._run_draining(indexed_items, operation)

        tasks = [
            asyncio.create_task(self._run_one(item, operation))
            for _, item in indexed_items
        ]
        if not tasks:
            return []
        try:
            # asyncio.gather preserves the order of its awaitables.
            results = list(await asyncio.gather(*tasks))
        except BaseException:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
        self.successful_results = tuple(
            ScheduledSuccess(index=index, item=item, result=result)
            for (index, item), result in zip(indexed_items, results)
        )
        return results

    async def run_one(
        self,
        item: ItemT,
        operation: Callable[[ItemT], Awaitable[ResultT]],
    ) -> ResultT:
        """Run one dynamically discovered item through shared limits and retries."""

        return await self._run_one(item, operation)

    async def _run_draining(
        self,
        indexed_items: list[tuple[int, ItemT]],
        operation: Callable[[ItemT], Awaitable[ResultT]],
    ) -> list[ResultT]:
        """Stop new attempts after failure while draining started operations."""

        if not indexed_items:
            return []
        state = _DrainState()
        successes: list[ScheduledSuccess[ItemT, ResultT] | None] = [None] * len(
            indexed_items
        )
        tasks = [
            asyncio.create_task(
                self._run_one_draining(index, item, operation, state, successes)
            )
            for index, item in indexed_items
        ]
        try:
            await asyncio.gather(*tasks)
        except BaseException:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

        self.successful_results = tuple(
            success for success in successes if success is not None
        )
        if state.first_failure is not None:
            raise state.first_failure
        return [success.result for success in self.successful_results]

    async def _run_one(
        self,
        item: ItemT,
        operation: Callable[[ItemT], Awaitable[ResultT]],
    ) -> ResultT:
        retries = 0
        while True:
            # Keeping the limiter inside the semaphore paces actual operation
            # starts; queued token holders cannot burst when a worker slot opens.
            async with self._semaphore:
                await self._limiter.acquire()
                try:
                    return await operation(item)
                except BaseException as error:
                    if (
                        isinstance(error, asyncio.CancelledError)
                        or not self._transient_predicate(error)
                        or retries >= self.max_retries
                    ):
                        raise

            delay_ceiling = min(
                self.retry_max_seconds,
                self.retry_base_seconds * (2**retries),
            )
            jitter_fraction = min(1.0, max(0.0, float(self._jitter())))
            retry_delay = delay_ceiling * jitter_fraction
            retries += 1
            if retry_delay:
                await self._sleeper(retry_delay)

    async def _run_one_draining(
        self,
        index: int,
        item: ItemT,
        operation: Callable[[ItemT], Awaitable[ResultT]],
        state: _DrainState,
        successes: list[ScheduledSuccess[ItemT, ResultT] | None],
    ) -> None:
        retries = 0
        while not state.stopped.is_set():
            async with self._semaphore:
                if state.stopped.is_set():
                    return
                acquired = await _await_unless_stopped(
                    self._limiter.acquire(), state.stopped
                )
                if not acquired or state.stopped.is_set():
                    return
                try:
                    result = await operation(item)
                except asyncio.CancelledError:
                    raise
                except BaseException as error:
                    if state.stopped.is_set():
                        return
                    if (
                        not self._transient_predicate(error)
                        or retries >= self.max_retries
                    ):
                        state.fail(error)
                        return
                else:
                    successes[index] = ScheduledSuccess(
                        index=index, item=item, result=result
                    )
                    return

            if state.stopped.is_set():
                return
            delay_ceiling = min(
                self.retry_max_seconds,
                self.retry_base_seconds * (2**retries),
            )
            jitter_fraction = min(1.0, max(0.0, float(self._jitter())))
            retry_delay = delay_ceiling * jitter_fraction
            retries += 1
            if retry_delay and not await _await_unless_stopped(
                self._sleeper(retry_delay), state.stopped
            ):
                return


async def schedule_ordered(
    items: Iterable[ItemT],
    operation: Callable[[ItemT], Awaitable[ResultT]],
    **scheduler_options: object,
) -> list[ResultT]:
    """Convenience wrapper around :class:`AsyncScheduler`."""

    scheduler: AsyncScheduler[ItemT, ResultT] = AsyncScheduler(
        **scheduler_options  # type: ignore[arg-type]
    )
    return await scheduler.run(items, operation)


def _http_status(error: BaseException) -> int | None:
    for attribute in ("status_code", "status", "code"):
        value = getattr(error, attribute, None)
        if isinstance(value, int):
            return value
    response = getattr(error, "response", None)
    value = getattr(response, "status_code", None)
    return value if isinstance(value, int) else None


async def _await_unless_stopped(
    awaitable: Awaitable[None], stopped: asyncio.Event
) -> bool:
    """Await pacing/backoff unless a permanent failure stops new attempts."""

    waiting = asyncio.ensure_future(awaitable)
    stopping = asyncio.create_task(stopped.wait())
    try:
        await asyncio.wait((waiting, stopping), return_when=asyncio.FIRST_COMPLETED)
        if stopped.is_set():
            return False
        await waiting
        return True
    finally:
        if not waiting.done():
            waiting.cancel()
        if not stopping.done():
            stopping.cancel()
        await asyncio.gather(waiting, stopping, return_exceptions=True)


__all__ = [
    "AsyncScheduler",
    "ScheduledSuccess",
    "SmoothTokenBucket",
    "TransientError",
    "is_transient_error",
    "schedule_ordered",
]
