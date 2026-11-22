"""Provides classes for performing a table diff
"""
import threading
import time
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from enum import Enum
from operator import methodcaller
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple, Union

import attrs

from data_diff.abcs.database_types import IKey
from data_diff.errors import (
    DataDiffConsistencyError,
    DataDiffIncompleteResultError,
    DataDiffMismatchingKeyTypesError,
    DataDiffUnsupportedConsistencyError,
)
from data_diff.info_tree import InfoTree, SegmentInfo  # noqa: F401  (SegmentInfo re-exported for compatibility)
from data_diff.table_segment import TableSegment, create_mesh_from_points  # noqa: F401
from data_diff.thread_utils import ThreadedYielder, YielderStopped
from data_diff.tracking import create_end_event_json, create_start_event_json, is_tracking_enabled, send_event_json
from data_diff.utils import Vector, dbt_diff_string_template, getLogger, run_as_daemon, safezip, truncate_error

logger = getLogger(__name__)


class Algorithm(Enum):
    AUTO = "auto"
    JOINDIFF = "joindiff"
    HASHDIFF = "hashdiff"


class Consistency(str, Enum):
    """How consistently each side of the diff is read.

    LIVE (default): each query sees whatever is committed when it runs.
    SNAPSHOT: every query for a given side runs in one read-only snapshot that
              is fixed when the result is first consumed.  Each side has its
              own snapshot; this does not prove the two databases were equal at
              one wall-clock instant.
    """

    LIVE = "live"
    SNAPSHOT = "snapshot"


DiffResult = Iterator[Tuple[str, tuple]]  # Iterator[Tuple[Literal["+", "-"], tuple]]
DiffResultList = Iterator[List[Tuple[str, tuple]]]


@attrs.define(frozen=False)
class ThreadBase:
    "Provides utility methods for optional threading"

    threaded: bool = True
    max_threadpool_size: Optional[int] = 1

    def _thread_map(self, func, iterable):
        if not self.threaded:
            return map(func, iterable)

        with ThreadPoolExecutor(max_workers=self.max_threadpool_size) as task_pool:
            return task_pool.map(func, iterable)

    def _threaded_call(self, func, iterable):
        "Calls a method for each object in iterable."
        return list(self._thread_map(methodcaller(func), iterable))

    def _thread_as_completed(self, func, iterable):
        if not self.threaded:
            yield from map(func, iterable)
            return

        with ThreadPoolExecutor(max_workers=self.max_threadpool_size) as task_pool:
            futures = [task_pool.submit(func, item) for item in iterable]
            for future in as_completed(futures):
                yield future.result()

    def _threaded_call_as_completed(self, func, iterable):
        "Calls a method for each object in iterable. Returned in order of completion."
        return self._thread_as_completed(methodcaller(func), iterable)

    @contextmanager
    def _run_in_background(self, *funcs):
        with ThreadPoolExecutor(max_workers=self.max_threadpool_size) as task_pool:
            futures = [task_pool.submit(f) for f in funcs if f is not None]
            yield futures
            for f in futures:
                f.result()


@attrs.define(frozen=True)
class DiffStats:
    diff_by_sign: Dict[str, int]
    table1_count: int
    table2_count: int
    unchanged: int
    diff_percent: float
    extra_column_diffs: Optional[Dict[str, int]]


class _DiffResultState:
    """Mutable bookkeeping shared by a DiffResultWrapper and its iterator."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.consumed = False
        self.exhausted = False
        self.closed = False
        self.error: Optional[BaseException] = None


class _DiffIterator:
    """The producer side of a diff.

    Wraps the generator returned by ``_diff_tables_wrapper`` and keeps the
    current dispatcher (:class:`ThreadedYielder`) reachable so that ``close()``
    can stop dispatching and wait for in-flight segment queries.
    """

    def __init__(self, generator, holder=None) -> None:
        self._generator = generator
        self._yielder: Optional[ThreadedYielder] = None
        self._closed = False
        self._lock = threading.Lock()
        if holder is not None:
            # Registered before the first `next()`, so the producer can look it
            # up when it creates its dispatcher.
            holder["iterator"] = self

    def attach_yielder(self, yielder: ThreadedYielder) -> None:
        with self._lock:
            if self._closed:
                # The consumer closed us before the dispatcher was created.
                yielder.stop()
                return
            self._yielder = yielder

    def detach_yielder(self, yielder: ThreadedYielder) -> None:
        with self._lock:
            if self._yielder is yielder:
                self._yielder = None

    def __iter__(self) -> "_DiffIterator":
        return self

    def __next__(self):
        return next(self._generator)

    def close(self, release_views=None, pump_lock=None, timeout: float = 2.0) -> None:
        with self._lock:
            self._closed = True
            yielder = self._yielder
        # Stop dispatch first: no queued task may start after this, so the views
        # are never closed under an unscheduled query.  Then release the views,
        # which cancels any query already running on the server and unblocks the
        # producer thread.
        if yielder is not None:
            yielder.stop()
        if release_views is not None:
            release_views()
        try:
            self._generator.close()
        except (GeneratorExit, StopIteration, ValueError):  # pragma: no cover - defensive
            # ValueError: the producer is executing on another thread; the
            # stopped yielder plus closed views let it unwind by itself.
            pass
        except Exception:  # pragma: no cover - defensive
            logger.debug("Error while closing the diff generator", exc_info=True)
        if yielder is not None:
            yielder.wait_closed(timeout=timeout)
        if pump_lock is not None:
            # Bound the wait for a consumer thread that is still inside next().
            if pump_lock.acquire(timeout=timeout):
                pump_lock.release()


class _SnapshotSession:
    """Owns the two read views of a snapshot diff run.

    Attached to the producer only once both views are ready; ``close()`` is
    idempotent and may be called by the result wrapper, by the producer's
    ``finally`` block, or both.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._views = None
        self._closed = False

    def attach(self, views) -> None:
        with self._lock:
            if self._closed:
                # A concurrent close won the race; release what we just opened.
                for view in views:
                    view.close()
                return
            self._views = views

    def close(self, timeout: float = 4.0) -> None:
        from data_diff.consistency import TOTAL_CLOSE_SECONDS

        timeout = min(timeout, TOTAL_CLOSE_SECONDS)
        with self._lock:
            if self._closed:
                return
            self._closed = True
            views, self._views = self._views, None
        views = list(views or ())
        share = (timeout / len(views)) if views else 0.0
        for view in views:
            try:
                view.close(timeout=share)
            except Exception:  # pragma: no cover - cleanup must not mask failures
                logger.debug("Error while closing a snapshot view", exc_info=True)

    @property
    def closed(self) -> bool:
        return self._closed


def _snapshot_worker_count(database, fallback=None) -> int:
    """How many worker connections one side's read view may use.

    The limit is the caller's ``thread_count`` for that side (the pool that
    runs the diff), never more.  The anchor connection is extra, so the total
    per side is ``thread_count + 1``.
    """
    count = getattr(database, "thread_count", 1)
    if count is None:
        count = fallback if fallback is not None else 4
    return max(1, int(count))


@attrs.define(frozen=False, init=False)
class DiffResultWrapper:
    """The object returned by every diff.

    Supports iteration, ``close()`` and the context-manager protocol.  Results
    are cached as they are produced, so a fully consumed result can be iterated
    again and asked for statistics after ``close()``.

    ``close()`` is idempotent.  Closing a result that was never consumed does
    not touch the database.  Closing a partially consumed result stops
    dispatching new segments, waits (bounded) for in-flight work, and releases
    this run's separate snapshot connections.  After that, further iteration or
    statistics raise :class:`~data_diff.errors.DataDiffIncompleteResultError`
    until/unless the result was exhausted first.
    """

    diff: Any
    info_tree: InfoTree
    stats: dict
    result_list: list = attrs.field(factory=list)
    consistency: Consistency = Consistency.LIVE
    _state: _DiffResultState = attrs.field(factory=_DiffResultState)
    _closer: Optional[Any] = None
    _pump_lock: Any = attrs.field(factory=threading.Lock, init=False, repr=False)
    _pump_thread: Optional[int] = attrs.field(default=None, init=False, repr=False)

    def __init__(self, diff, info_tree, stats, consistency=Consistency.LIVE, closer=None) -> None:
        self.diff = diff
        self.info_tree = info_tree
        self.stats = stats
        self.result_list = []
        self.consistency = Consistency(consistency)
        self._state = _DiffResultState()
        self._closer = closer
        self._pump_lock = threading.Lock()
        self._pump_thread: Optional[int] = None

    # -- iteration -------------------------------------------------------------
    def __iter__(self) -> Iterator[Any]:
        """Yield rows from the shared cache, producing more as needed.

        All iterators of one result share the same producer and cache, so
        consuming the same result from two threads never re-runs it and never
        mixes two runs' snapshots.
        """
        state = self._state
        index = 0
        while True:
            with state.lock:
                if state.exhausted:
                    # A fully consumed run may always be re-read from the cache,
                    # even after close(): it describes the state it scanned.
                    if index < len(self.result_list):
                        item = self.result_list[index]
                        index += 1
                        should_yield = True
                    else:
                        return
                elif state.error is not None:
                    raise state.error
                elif state.closed:
                    # Never let a cached prefix pass for a complete comparison.
                    raise DataDiffIncompleteResultError(
                        "This diff result was closed before it was fully consumed; its cached prefix is not a "
                        "complete comparison. Re-run the diff to get a full result."
                    )
                elif index < len(self.result_list):
                    item = self.result_list[index]
                    index += 1
                    state.consumed = True
                    should_yield = True
                else:
                    should_yield = False
            if should_yield:
                try:
                    yield item
                except KeyboardInterrupt:
                    # Bounded cleanup even when the caller never calls close().
                    self.close()
                    raise
                continue
            self._pump_one()

    def _pump_one(self) -> None:
        """Advance the producer by (at least) one row.

        Only one thread pumps at a time; others wait for a new cached row, the
        end of the result, or an error.
        """
        if self._pump_thread == threading.get_ident():
            # Re-entrant request from inside the producer itself (should not
            # happen); avoid deadlocking.
            return
        with self._pump_lock:
            state = self._state
            with state.lock:
                if state.exhausted or state.error is not None or state.closed:
                    return
                before = len(self.result_list)
            self._pump_thread = threading.get_ident()
            try:
                try:
                    item = next(self.diff)
                except StopIteration:
                    with state.lock:
                        state.exhausted = True
                except YielderStopped:
                    with state.lock:
                        state.closed = True
                except BaseException as e:
                    with state.lock:
                        # Errors caused purely by closing (closed-view, closed
                        # connection) must not mask the "incomplete" fact, and
                        # must never turn a failed run into a success summary.
                        if not state.closed and state.error is None:
                            state.error = e
                else:
                    with state.lock:
                        if len(self.result_list) == before:
                            self.result_list.append(item)
            finally:
                self._pump_thread = None
            if state.closed and not state.exhausted:
                raise DataDiffIncompleteResultError(
                    "This diff result was closed before it was fully consumed; its cached prefix is not a "
                    "complete comparison."
                )

    def _require_complete(self) -> None:
        state = self._state
        with state.lock:
            done = state.exhausted
            closed = state.closed
            error = state.error
        if not done and not closed and error is None:
            # Consume the rest, so a fresh result is materialised into cache.
            list(self)
            with state.lock:
                done = state.exhausted
                error = state.error
        if done:
            return
        if error is not None:
            raise error
        raise DataDiffIncompleteResultError(
            "This diff result was not fully consumed, so its statistics do not describe a complete "
            f"comparison (cached {len(self.result_list)} row(s))."
        )

    def _get_stats(self, is_dbt: bool = False) -> DiffStats:
        self._require_complete()

        key_columns = self.info_tree.info.tables[0].key_columns
        len_key_columns = len(key_columns)
        diff_by_key = {}
        extra_column_diffs = None
        if is_dbt:
            extra_column_values_store = {}
            extra_columns = self.info_tree.info.tables[0].extra_columns
            extra_column_diffs = {k: 0 for k in extra_columns}

        for sign, values in self.result_list:
            k = values[:len_key_columns]
            if is_dbt:
                extra_column_values = values[len_key_columns:]
            if k in diff_by_key:
                assert sign != diff_by_key[k]
                diff_by_key[k] = "!"
                if is_dbt:
                    for i in range(len(extra_columns)):
                        if extra_column_values[i] != extra_column_values_store[k][i]:
                            extra_column_diffs[extra_columns[i]] += 1
            else:
                diff_by_key[k] = sign
                if is_dbt:
                    extra_column_values_store[k] = extra_column_values

        diff_by_sign = {k: 0 for k in "+-!"}
        for sign in diff_by_key.values():
            diff_by_sign[sign] += 1

        table1_count = self.info_tree.info.rowcounts[1]
        table2_count = self.info_tree.info.rowcounts[2]
        unchanged = table1_count - diff_by_sign["-"] - diff_by_sign["!"]
        diff_percent = 1 - unchanged / max(table1_count, table2_count)

        return DiffStats(diff_by_sign, table1_count, table2_count, unchanged, diff_percent, extra_column_diffs)

    def get_stats_string(self, is_dbt: bool = False):
        diff_stats = self._get_stats(is_dbt)

        total_rows_diff = diff_stats.table2_count - diff_stats.table1_count

        if is_dbt:
            string_output = dbt_diff_string_template(
                total_rows_table1=diff_stats.table1_count,
                total_rows_table2=diff_stats.table2_count,
                total_rows_diff=total_rows_diff,
                rows_added=diff_stats.diff_by_sign["+"],
                rows_removed=diff_stats.diff_by_sign["-"],
                rows_updated=diff_stats.diff_by_sign["!"],
                rows_unchanged=diff_stats.unchanged,
                extra_info_dict=diff_stats.extra_column_diffs,
                extra_info_str="[u]Values Changed[/u]",
            )

        else:
            string_output = ""
            string_output += f"{diff_stats.table1_count} rows in table A\n"
            string_output += f"{diff_stats.table2_count} rows in table B\n"
            string_output += f"{diff_stats.diff_by_sign['-']} rows exclusive to table A (not present in B)\n"
            string_output += f"{diff_stats.diff_by_sign['+']} rows exclusive to table B (not present in A)\n"
            string_output += f"{diff_stats.diff_by_sign['!']} rows updated\n"
            string_output += f"{diff_stats.unchanged} rows unchanged\n"
            string_output += f"{100*diff_stats.diff_percent:.2f}% difference score\n"

            if self.stats:
                string_output += "\nExtra-Info:\n"
                for k, v in sorted(self.stats.items()):
                    string_output += f"  {k} = {v}\n"

            if self.consistency is Consistency.SNAPSHOT:
                string_output += (
                    "Read consistency: snapshot (per-side). Each side was read from one committed state; "
                    "this does not prove both sides were equal at the same moment.\n"
                )

        return string_output

    def close(self) -> None:
        """Stop this diff and release its snapshot resources.

        Safe to call repeatedly and to call on an untouched result (in which
        case no database is contacted).  Bounded in time; in-flight work is
        waited for briefly, then its server-side statements are cancelled.
        """
        state = self._state
        with state.lock:
            if state.closed or state.exhausted:
                state.closed = True
                already_done = True
            else:
                state.closed = True
                already_done = False

        closer = self._closer
        if closer is not None:
            self._closer = None
        if not already_done:
            self._stop_producer(closer)
        elif closer is not None:
            # Exhausted run: the producer already released its views; this is
            # a harmless no-op (idempotent).
            try:
                closer()
            except Exception:  # pragma: no cover - cleanup must not mask failures
                logger.debug("Error while closing snapshot views", exc_info=True)

    def _stop_producer(self, release_views=None) -> None:
        diff = self.diff
        close = getattr(diff, "close", None)
        if close is not None:
            try:
                close(release_views=release_views, pump_lock=self._pump_lock, timeout=2.0)
            except TypeError:
                try:
                    close()
                except Exception:  # pragma: no cover - defensive
                    logger.debug("Error while closing the diff iterator", exc_info=True)
            except Exception:  # pragma: no cover - defensive
                logger.debug("Error while closing the diff iterator", exc_info=True)
        elif release_views is not None:
            release_views()

    def __enter__(self) -> "DiffResultWrapper":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def get_stats_dict(self, is_dbt: bool = False):
        diff_stats = self._get_stats(is_dbt)
        json_output = {
            "rows_A": diff_stats.table1_count,
            "rows_B": diff_stats.table2_count,
            "exclusive_A": diff_stats.diff_by_sign["-"],
            "exclusive_B": diff_stats.diff_by_sign["+"],
            "updated": diff_stats.diff_by_sign["!"],
            "unchanged": diff_stats.unchanged,
            "total": sum(diff_stats.diff_by_sign.values()),
            "stats": self.stats,
        }
        json_output["values"] = diff_stats.extra_column_diffs or {}
        return json_output


@attrs.define(frozen=False)
class TableDiffer(ThreadBase, ABC):
    INFO_TREE_CLASS = InfoTree

    bisection_factor = 32
    stats: dict = {}
    # Subclasses that can honour consistency="snapshot" set this to True.
    SNAPSHOT_CONSISTENCY_SUPPORTED = False

    ignored_columns1: Set[str] = attrs.field(factory=set)
    ignored_columns2: Set[str] = attrs.field(factory=set)
    _ignored_columns_lock: threading.Lock = attrs.field(factory=threading.Lock, init=False)
    yield_list: bool = False
    # Per-thread pointer to the diff currently being produced on this thread.
    # Close() never consults it: it uses the result object's own iterator, so
    # two concurrent/interleaved diffs cannot stop or release each other.
    _diff_iterator_local: threading.local = attrs.field(factory=threading.local, init=False, repr=False)

    def diff_tables(
        self,
        table1: TableSegment,
        table2: TableSegment,
        info_tree: InfoTree = None,
        consistency: Union[str, Consistency] = Consistency.LIVE,
    ) -> DiffResultWrapper:
        """Diff the given tables.

        Parameters:
            table1 (TableSegment): The "before" table to compare. Or: source table
            table2 (TableSegment): The "after" table to compare. Or: target table
            consistency: "live" (default) reads whatever is committed per query;
                         "snapshot" fixes one committed state per side before any
                         data is read.  Snapshot views are opened lazily on first
                         consumption, and only the hash-diff path supports them.

        Returns:
            A :class:`DiffResultWrapper` that yields pair-tuples, representing the diff. Items can be either -
            ('-', row) for items in table1 but not in table2.
            ('+', row) for items in table2 but not in table1.
            Where `row` is a tuple of values, corresponding to the diffed columns.
        """
        try:
            consistency = Consistency(consistency)
        except ValueError:
            raise DataDiffConsistencyError(
                f"Unknown consistency {consistency!r}; expected one of "
                f"{[c.value for c in Consistency]}."
            ) from None

        if consistency is Consistency.SNAPSHOT:
            self._validate_snapshot_request(table1, table2)

        if info_tree is None:
            segment_info = self.INFO_TREE_CLASS.SEGMENT_INFO_CLASS([table1, table2])
            info_tree = self.INFO_TREE_CLASS(segment_info)

        session = _SnapshotSession() if consistency is Consistency.SNAPSHOT else None
        holder = {}
        diff = _DiffIterator(
            self._diff_tables_wrapper(
                table1, table2, info_tree, consistency=consistency, session=session, diff_iter_owner=holder
            ),
            holder,
        )
        return DiffResultWrapper(
            diff,
            info_tree,
            self.stats,
            consistency=consistency,
            closer=session.close if session is not None else None,
        )

    def _validate_snapshot_request(self, table1: TableSegment, table2: TableSegment) -> None:
        if not getattr(self, "SNAPSHOT_CONSISTENCY_SUPPORTED", False):
            raise DataDiffUnsupportedConsistencyError(
                f"consistency='snapshot' is only supported by HashDiffer, not {type(self).__name__}."
            )
        for table in (table1, table2):
            db = table.database
            if not db.supports_snapshot_consistency():
                raise DataDiffUnsupportedConsistencyError(
                    f"consistency='snapshot' is not supported for database {db.name} "
                    "(only PostgreSQL 16 is supported)."
                )
            # Reject transaction settings that exported snapshots cannot carry
            # over; failing now is much clearer than failing on first use.
            if getattr(db, "is_autocommit", False):
                raise DataDiffUnsupportedConsistencyError(
                    f"consistency='snapshot' requires a transaction-capable connection on {db.name}."
                )

    def _open_snapshot_views(self, table1: TableSegment, table2: TableSegment):
        from data_diff.consistency import open_snapshot_views

        worker_counts = [
            _snapshot_worker_count(t.database, self.max_threadpool_size) for t in (table1, table2)
        ]
        return open_snapshot_views(
            [table1.database, table2.database],
            worker_counts,
            [[table1.table_path], [table2.table_path]],
        )

    def _diff_tables_wrapper(
        self,
        table1: TableSegment,
        table2: TableSegment,
        info_tree: InfoTree,
        consistency: Consistency = Consistency.LIVE,
        session: "_SnapshotSession" = None,
        diff_iter_owner: "_DiffIterator" = None,
    ) -> DiffResult:
        if is_tracking_enabled():
            options = attrs.asdict(self, recurse=False)
            # not useful event attributes (one is a lock, the other per-thread state)
            options.pop("_ignored_columns_lock", None)
            options.pop("_diff_iterator_local", None)
            options["differ_name"] = type(self).__name__
            options["consistency"] = consistency.value
            event_json = create_start_event_json(options)
            run_as_daemon(send_event_json, event_json)

        start = time.monotonic()
        error = None
        stopped = False
        if diff_iter_owner is not None:
            self._diff_iterator_local.value = diff_iter_owner.get("iterator")
        try:
            if consistency is Consistency.SNAPSHOT:
                # Views are opened here — i.e. on first consumption, never when
                # the lazy result object was created.  Both sides must be ready
                # before a single diff row is yielded.
                views = self._open_snapshot_views(table1, table2)
                session.attach(views)
                table1 = table1.new(database=views[0].database)
                table2 = table2.new(database=views[1].database)

            if table1.database.dialect.PREVENT_OVERFLOW_WHEN_CONCAT or table2.database.dialect.PREVENT_OVERFLOW_WHEN_CONCAT:
                table1.database.dialect.enable_preventing_type_overflow()
                table2.database.dialect.enable_preventing_type_overflow()

            # Query and validate schema
            table1, table2 = self._threaded_call("with_schema", [table1, table2])
            self._validate_and_adjust_columns(table1, table2)

            yield from self._diff_tables_root(table1, table2, info_tree)

        except YielderStopped:
            # Deliberate early stop during close(); not a query failure, but the
            # run is incomplete and the consumer must be told so.
            stopped = True
            error = None
        except BaseException as e:  # Catch KeyboardInterrupt too
            error = e
        finally:
            info_tree.aggregate_info()

            if consistency is Consistency.SNAPSHOT and session is not None:
                session.close()
            self._diff_iterator_local.value = None

            if is_tracking_enabled():
                runtime = time.monotonic() - start
                rowcounts = info_tree.info.rowcounts
                table1_count = rowcounts[1] if rowcounts else None
                table2_count = rowcounts[2] if rowcounts else None
                diff_count = info_tree.info.diff_count
                err_message = truncate_error(repr(error))
                event_json = create_end_event_json(
                    error is None,
                    runtime,
                    table1.database.name,
                    table2.database.name,
                    table1_count,
                    table2_count,
                    diff_count,
                    err_message,
                )
                send_event_json(event_json)

            if error:
                raise error
            if stopped:
                raise YielderStopped()

    def _validate_and_adjust_columns(self, table1: TableSegment, table2: TableSegment) -> None:
        pass

    def _diff_tables_root(
        self, table1: TableSegment, table2: TableSegment, info_tree: InfoTree
    ) -> Union[DiffResult, DiffResultList]:
        return self._bisect_and_diff_tables(table1, table2, info_tree)

    @abstractmethod
    def _diff_segments(
        self,
        ti: ThreadedYielder,
        table1: TableSegment,
        table2: TableSegment,
        info_tree: InfoTree,
        max_rows: int,
        level=0,
        segment_index=None,
        segment_count=None,
    ):
        ...

    def _bisect_and_diff_tables(self, table1: TableSegment, table2: TableSegment, info_tree):
        original_tables = (table1, table2)
        if len(table1.key_columns) != len(table2.key_columns):
            raise ValueError("Tables should have an equivalent number of key columns!")

        key_types1 = [table1._schema[i] for i in table1.key_columns]
        key_types2 = [table2._schema[i] for i in table2.key_columns]

        for kt in key_types1 + key_types2:
            if not isinstance(kt, IKey):
                raise NotImplementedError(f"Cannot use a column of type {kt} as a key")

        for i, (kt1, kt2) in enumerate(safezip(key_types1, key_types2)):
            if kt1.python_type is not kt2.python_type:
                k1 = table1.key_columns[i]
                k2 = table2.key_columns[i]
                raise DataDiffMismatchingKeyTypesError(
                    f"Key columns {k1} and {k2} can't be compared due to different types."
                )

        # Query min/max values
        key_ranges = self._threaded_call_as_completed("query_key_range", [table1, table2])

        # Start with the first completed value, so we don't waste time waiting
        min_key1, max_key1 = self._parse_key_range_result(key_types1, next(key_ranges))

        btable1 = table1.new_key_bounds(min_key=min_key1, max_key=max_key1, key_types=key_types1)
        btable2 = table2.new_key_bounds(min_key=min_key1, max_key=max_key1, key_types=key_types2)

        logger.info(
            f"Diffing segments at key-range: {btable1.min_key}..{btable2.max_key}. "
            f"size: table1 <= {btable1.approximate_size()}, table2 <= {btable2.approximate_size()}"
        )

        ti = ThreadedYielder(self.max_threadpool_size, self.yield_list)
        diff_iter = getattr(self._diff_iterator_local, "value", None)
        if diff_iter is not None:
            diff_iter.attach_yielder(ti)
        try:
            # Bisect (split) the table into segments, and diff them recursively.
            ti.submit(self._bisect_and_diff_segments, ti, btable1, btable2, info_tree, priority=999)

            # Now we check for the second min-max, to diff the portions we "missed".
            # This is achieved by subtracting the table ranges, and dividing the resulting space into aligned boxes.
            # For example, given tables A & B, and a 2D compound key, where A was queried first for key-range,
            # the regions of B we need to diff in this second pass are marked by B1..8:
            # ┌──┬──────┬──┐
            # │B1│  B2  │B3│
            # ├──┼──────┼──┤
            # │B4│  A   │B5│
            # ├──┼──────┼──┤
            # │B6│  B7  │B8│
            # └──┴──────┴──┘
            # Overall, the max number of new regions in this 2nd pass is 3^|k| - 1

            # Note: python types can be the same, but the rendering parameters (e.g. casing) can differ.
            min_key2, max_key2 = self._parse_key_range_result(key_types2, next(key_ranges))

            points = [sorted(p) for p in safezip(min_key1, min_key2, max_key1, max_key2)]
            box_mesh = create_mesh_from_points(*points)

            new_regions = [(p1, p2) for p1, p2 in box_mesh if p1 < p2 and not (p1 >= min_key1 and p2 <= max_key1)]

            for p1, p2 in new_regions:
                extra_table1 = original_tables[0].new_key_bounds(min_key=p1, max_key=p2, key_types=key_types1)
                extra_table2 = original_tables[1].new_key_bounds(min_key=p1, max_key=p2, key_types=key_types2)
                ti.submit(self._bisect_and_diff_segments, ti, extra_table1, extra_table2, info_tree, priority=999)
        except BaseException:
            if diff_iter is not None:
                diff_iter.detach_yielder(ti)
            raise
        return ti

    def _parse_key_range_result(self, key_types, key_range) -> Tuple[Vector, Vector]:
        min_key_values, max_key_values = key_range

        # We add 1 because our ranges are exclusive of the end (like in Python)
        try:
            min_key = Vector(key_type.make_value(mn) for key_type, mn in safezip(key_types, min_key_values))
            max_key = Vector(key_type.make_value(mx) + 1 for key_type, mx in safezip(key_types, max_key_values))
        except (TypeError, ValueError) as e:
            raise type(e)(f"Cannot apply {key_types} to '{min_key_values}', '{max_key_values}'.") from e

        return min_key, max_key

    def _bisect_and_diff_segments(
        self,
        ti: ThreadedYielder,
        table1: TableSegment,
        table2: TableSegment,
        info_tree: InfoTree,
        level=0,
        max_rows=None,
    ):
        assert table1.is_bounded and table2.is_bounded

        # Choose evenly spaced checkpoints (according to min_key and max_key)
        biggest_table = max(table1, table2, key=methodcaller("approximate_size"))
        checkpoints = biggest_table.choose_checkpoints(self.bisection_factor - 1)

        # Get it thread-safe, to avoid segment misalignment because of bad timing.
        with self._ignored_columns_lock:
            table1 = attrs.evolve(table1, ignored_columns=frozenset(self.ignored_columns1))
            table2 = attrs.evolve(table2, ignored_columns=frozenset(self.ignored_columns2))

        # Create new instances of TableSegment between each checkpoint
        segmented1 = table1.segment_by_checkpoints(checkpoints)
        segmented2 = table2.segment_by_checkpoints(checkpoints)

        # Recursively compare each pair of corresponding segments between table1 and table2
        for i, (t1, t2) in enumerate(safezip(segmented1, segmented2)):
            info_node = info_tree.add_node(t1, t2, max_rows=max_rows)
            ti.submit(
                self._diff_segments, ti, t1, t2, info_node, max_rows, level + 1, i + 1, len(segmented1), priority=level
            )

    def ignore_column(self, column_name1: str, column_name2: str) -> None:
        """
        Ignore the column (by name on sides A & B) in md5s & diffs from now on.

        This affects 2 places:

        - The columns are not checksumed for new(!) segments.
        - The columns are ignored in in-memory diffing for running segments.

        The columns are never ignored in the fetched values, whether they are
        the same or different — for data consistency.

        Use this feature to collect relatively well-represented differences
        across all columns if one of them is highly different in the beginning
        of a table (as per the order of segmentation/bisection). Otherwise,
        that one column might easily hit the limit and stop the whole diff.
        """
        with self._ignored_columns_lock:
            self.ignored_columns1.add(column_name1)
            self.ignored_columns2.add(column_name2)
