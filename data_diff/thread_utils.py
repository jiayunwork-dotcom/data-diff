import itertools
import threading
from collections import deque
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures.thread import _WorkItem
from queue import PriorityQueue
from time import monotonic, sleep
from typing import Any, Optional

import attrs


class AutoPriorityQueue(PriorityQueue):
    """Overrides PriorityQueue to automatically get the priority from _WorkItem.kwargs

    We also assign a unique id for each item, to avoid making comparisons on _WorkItem.
    As a side effect, items with the same priority are returned FIFO.
    """

    _counter = itertools.count().__next__

    def put(self, item: Optional[_WorkItem], block=True, timeout=None) -> None:
        priority = item.kwargs.pop("priority") if item is not None else 0
        super().put((-priority, self._counter(), item), block, timeout)

    def get(self, block=True, timeout=None) -> Optional[_WorkItem]:
        _p, _c, work_item = super().get(block, timeout)
        return work_item


class PriorityThreadPoolExecutor(ThreadPoolExecutor):
    """Overrides ThreadPoolExecutor to use AutoPriorityQueue

    XXX WARNING: Might break in future versions of Python
    """

    def __init__(self, *args) -> None:
        super().__init__(*args)
        self._work_queue = AutoPriorityQueue()


class YielderStopped(BaseException):
    """Raised inside ``ThreadedYielder.__iter__`` when the yielder was stopped.

    Derives from ``BaseException`` so that the worker wrappers, which catch
    ``Exception`` to record query failures, do not mistake a deliberate stop
    for a database error.
    """


@attrs.define(frozen=False, init=False)
class ThreadedYielder(Iterable):
    """Yields results from multiple threads into a single iterator, ordered by priority.

    To add a source iterator, call ``submit()`` with a function that returns an iterator.
    Priority for the iterator can be provided via the keyword argument 'priority'. (higher runs first)

    ``stop()`` ends this yielder early: no new tasks are dispatched, queued tasks
    are cancelled, and readers are unblocked with :class:`YielderStopped`.  It is
    safe to call more than once.
    """

    _pool: ThreadPoolExecutor
    _futures: deque
    _yield: deque = attrs.field(alias="_yield")  # Python keyword!
    _exception: Optional[BaseException]
    yield_list: bool
    _stopped: threading.Event = attrs.field(factory=threading.Event, init=False)
    _stop_lock: threading.Lock = attrs.field(factory=threading.Lock, init=False)
    _pool_shutdown: bool = attrs.field(default=False, init=False)

    def __init__(self, max_workers: Optional[int] = None, yield_list: bool = False) -> None:
        super().__init__()
        self._pool = PriorityThreadPoolExecutor(max_workers)
        self._futures = deque()
        self._yield = deque()
        self._exception = None
        self.yield_list = yield_list
        self._stopped = threading.Event()
        self._stop_lock = threading.Lock()
        self._pool_shutdown = False

    def _worker(self, fn, *args, **kwargs) -> None:
        try:
            res = fn(*args, **kwargs)
            if res is not None:
                if self.yield_list:
                    self._yield.append(res)
                else:
                    self._yield += res
        except Exception as e:
            self._exception = e

    def submit(self, fn: Callable, *args, priority: int = 0, **kwargs) -> None:
        if self._stopped.is_set():
            return
        try:
            future = self._pool.submit(self._worker, fn, *args, priority=priority, **kwargs)
        except RuntimeError:
            # The pool was shut down concurrently with this submit.
            if self._stopped.is_set():
                return
            raise
        self._futures.append(future)

    def _shutdown_pool(self) -> None:
        with self._stop_lock:
            if self._pool_shutdown:
                return
            try:
                # Python >= 3.9: also cancel tasks that have not started yet.
                self._pool.shutdown(wait=False, cancel_futures=True)
            except TypeError:  # pragma: no cover - Python 3.8
                self._pool.shutdown(wait=False)
            self._pool_shutdown = True

    def stop(self) -> None:
        """Stop dispatching and cancel queued (not yet started) tasks.

        Running tasks are left to finish on their own; closing the underlying
        database views is expected to bound them.  This method does not block.
        """
        with self._stop_lock:
            if self._stopped.is_set():
                return
            self._stopped.set()
        self._shutdown_pool()

    def wait_closed(self, timeout: Optional[float] = None) -> bool:
        """Wait (bounded) for every task that is still running to finish."""
        self._shutdown_pool()
        threads = list(self._pool._threads)
        if timeout is None:
            for thread in threads:
                thread.join()
            return True
        end = monotonic() + timeout
        for thread in threads:
            remaining = max(0.0, end - monotonic())
            thread.join(remaining)
        return all(not thread.is_alive() for thread in threads)

    @property
    def stopped(self) -> bool:
        return self._stopped.is_set()

    def __iter__(self) -> Iterator[Any]:
        while True:
            if self._stopped.is_set():
                raise YielderStopped()
            if self._exception:
                raise self._exception

            while self._yield:
                yield self._yield.popleft()

            if not self._futures:
                # No more tasks
                return

            if self._futures[0].done():
                self._futures.popleft()
            else:
                sleep(0.001)
