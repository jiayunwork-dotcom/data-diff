"""Per-side snapshot consistency for the hash-diff path.

The public entry point is :meth:`TableDiffer.diff_tables` with
``consistency="snapshot"``.  A *read view* is opened lazily, on the first
consumption of the returned result object, and consists of:

* one *anchor* connection that starts a ``REPEATABLE READ, READ ONLY``
  transaction and exports its snapshot with ``pg_export_snapshot()``.  The
  anchor transaction stays open for the whole diff; that is what keeps the
  exported snapshot valid.
* a small pool of *worker* connections (at most the caller's ``thread_count``
  for that side).  Every worker starts a ``REPEATABLE READ, READ ONLY``
  transaction and imports the anchor snapshot with
  ``SET TRANSACTION SNAPSHOT``.  All queries issued for that side of the diff
  are executed on one of these connections, inside that snapshot transaction.

Each side gets an independent snapshot; nothing here tries to align the two
databases to a common wall-clock instant.

The implementation deliberately does not touch the caller's
:class:`~data_diff.databases.base.Database` object, its connection pool or its
transactions: a view owns brand-new connections and closes only those.
"""

from __future__ import annotations

import logging
import queue
import threading
import time
from typing import Any, Optional, Sequence

from data_diff.databases.base import Database, QueryResult

logger = logging.getLogger("consistency")

# How long cleanup may wait for an in-flight query before cancelling it, and
# how long it then gives the server to acknowledge the cancellation.  These are
# internal safety valves; healthy runs return far earlier.
CLOSE_WAIT_SECONDS = 1.5
CANCEL_WAIT_SECONDS = 0.3

# Total cleanup budget for one diff run (both sides).
TOTAL_CLOSE_SECONDS = 4.0

# A query that cannot obtain a worker connection within this window fails
# rather than waiting forever (only reachable during pathological shutdown).
LATE_ACQUIRE_TIMEOUT = 30.0


class SnapshotViewClosedError(RuntimeError):
    "Raised when a query is attempted on a read view that has been closed."


class SnapshotViewError(RuntimeError):
    "Raised when a read view cannot be established."


class SnapshotDatabase(Database):
    """A ``Database`` façade whose queries run inside one side's read view.

    Instances behave like the database they wrap (same name, dialect, schema
    helpers), but ``query()`` is routed through the view's snapshot
    transaction instead of the caller's connection pool.  Closing the façade
    closes the view; it never closes the underlying database object.
    """

    CONNECT_URI_HELP = "snapshot read view"
    CONNECT_URI_PARAMS: list[str] = []

    def __init__(self, view: SnapshotView) -> None:
        Database.__init__(self, default_schema=view.base.default_schema)
        self.view = view

    @property
    def name(self) -> str:
        return self.view.base.name

    @property
    def dialect(self):
        return self.view.base.dialect

    @property
    def is_autocommit(self) -> bool:
        return False

    def _query(self, sql_code: Any) -> QueryResult:
        return self.view.execute(sql_code)

    def select_table_schema(self, path):
        return self.view.base.select_table_schema(path)

    def select_table_unique_columns(self, path):
        return self.view.base.select_table_unique_columns(path)

    def _normalize_table_path(self, path):
        return self.view.base._normalize_table_path(path)

    def close(self) -> None:
        self.is_closed = True
        self.view.close()

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"<SnapshotDatabase {self.name} view={self.view!r}>"


class SnapshotView:
    """One side's read view.

    Subclasses implement :meth:`_start`, :meth:`_execute_one` and
    :meth:`_close_connections`.
    """

    base: Database
    worker_count: int

    def __init__(self, base: Database, worker_count: int, tables: Sequence[Any] = ()) -> None:
        if worker_count < 1:
            raise ValueError("Snapshot views need at least one worker connection")
        self.base = base
        self.worker_count = worker_count
        self.tables = list(tables)
        self.database = SnapshotDatabase(self)

        self._available: queue.Queue[Any] = queue.Queue()
        self._connections: list[Any] = []
        self._leased: set = set()
        self._anchor: Any = None
        self._lock = threading.Lock()
        self._idle = threading.Condition(self._lock)
        self._active = 0
        self._closing = False
        self._closed = False
        self._started = False

    # -- lifecycle -------------------------------------------------------------
    def start(self) -> None:
        try:
            self._start()
        except BaseException:
            # Roll back whatever was already prepared (anchor and/or workers).
            self._force_close()
            raise
        self._started = True

    def _start(self) -> None:
        raise NotImplementedError

    def _execute_one(self, connection: Any, sql_code: Any) -> QueryResult:
        raise NotImplementedError

    def _close_connections(self) -> None:
        # Hook for subclasses that keep extra per-connection state.
        pass

    def execute(self, sql_code: Any) -> QueryResult:
        connection = self._acquire()
        try:
            try:
                return self._execute_one(connection, sql_code)
            except BaseException:
                self._discard(connection)
                raise
        finally:
            self._release(connection)

    def _acquire(self):
        deadline = time.monotonic() + LATE_ACQUIRE_TIMEOUT
        while True:
            with self._lock:
                if self._closing or self._closed:
                    raise SnapshotViewClosedError("The snapshot read view is closed; no more queries are allowed.")
            try:
                connection = self._available.get(timeout=0.05)
            except queue.Empty:
                # If the pool never returns a connection there is no point
                # waiting forever; fail with a clear error instead.
                if time.monotonic() > deadline:
                    raise SnapshotViewClosedError(
                        "Timed out waiting for a snapshot connection; the view may be shutting down."
                    )
                continue
            with self._lock:
                if self._closing or self._closed:
                    # A concurrent close() may already have drained the queue;
                    # do not put this one back (that would leak it), close it.
                    try:
                        connection.close()
                    except Exception:  # pragma: no cover - best effort
                        logger.debug("Ignoring error closing a late snapshot connection", exc_info=True)
                    raise SnapshotViewClosedError("The snapshot read view is closing; no more queries are allowed.")
                self._active += 1
                self._leased.add(connection)
            return connection

    def _release(self, connection) -> None:
        with self._lock:
            self._active -= 1
            self._leased.discard(connection)
            closed = self._closed
            self._idle.notify_all()
        if closed:
            # close() already drained the queue; nobody else will close this
            # connection, so do it here to avoid leaking a session.
            try:
                connection.close()
            except Exception:  # pragma: no cover - best effort
                logger.debug("Ignoring error closing a returned snapshot connection", exc_info=True)
            return
        self._available.put(connection)

    def _discard(self, connection) -> None:
        """Abort the failed worker's transaction so it is clean on reuse.

        The connection itself is returned to the pool by ``execute``'s
        ``finally`` block.
        """
        self._rollback(connection)

    def _rollback(self, connection) -> None:
        try:
            connection.rollback()
        except Exception:  # pragma: no cover - best effort
            logger.debug("Ignoring rollback error during snapshot cleanup", exc_info=True)

    @property
    def is_closed(self) -> bool:
        return self._closed

    def close(self, timeout: float = CLOSE_WAIT_SECONDS) -> None:
        timeout = max(0.0, min(timeout, CLOSE_WAIT_SECONDS))
        with self._lock:
            if self._closed or self._closing:
                return
            self._closing = True

        # Let queries that were already dispatched finish.  New queries are
        # refused by _acquire from now on.
        deadline = time.monotonic() + timeout
        with self._lock:
            while self._active and time.monotonic() < deadline:
                self._idle.wait(timeout=min(0.05, max(0.0, deadline - time.monotonic())))
            still_active = self._active

        if still_active:
            # Only happens when the server is stalled.  Ask PostgreSQL to
            # cancel the running statements, give it a short grace period, then
            # close regardless so we never hang past the caller's bound.
            with self._lock:
                leased = list(self._leased)
            for connection in leased:
                try:
                    connection.cancel()
                except Exception:  # pragma: no cover - best effort
                    logger.debug("Ignoring cancel error during stalled snapshot close", exc_info=True)
            cancel_deadline = time.monotonic() + CANCEL_WAIT_SECONDS
            with self._lock:
                while self._active and time.monotonic() < cancel_deadline:
                    self._idle.wait(timeout=min(0.05, max(0.0, cancel_deadline - time.monotonic())))

        self._force_close()

    def _force_close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._closing = True
        connections, self._connections = self._connections, []
        leased, self._leased = self._leased, set()
        anchor, self._anchor = self._anchor, None
        # Drain the availability queue; close every connection exactly once.
        while True:
            try:
                connection = self._available.get_nowait()
            except queue.Empty:
                break
            if connection not in connections:
                connections.append(connection)
        if anchor is not None and anchor not in connections:
            connections.append(anchor)
        # If a query is still executing on a leased connection (only possible
        # after close() already exceeded its wait budget), cancel it before
        # closing: this aborts the server-side statement instead of yanking the
        # socket out from under it.
        for connection in leased:
            try:
                connection.cancel()
            except Exception:  # pragma: no cover - best effort
                logger.debug("Ignoring cancel error during snapshot teardown", exc_info=True)
        for connection in connections:
            self._rollback(connection)
        self._close_connections()
        for connection in connections:
            try:
                connection.close()
            except Exception:  # pragma: no cover - best effort
                logger.debug("Ignoring error closing snapshot connection", exc_info=True)
        with self._lock:
            self._idle.notify_all()

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"<{type(self).__name__} base={self.base.name} workers={self.worker_count}>"


class PostgresSnapshotView(SnapshotView):
    """Read view for PostgreSQL implemented with exported snapshots."""

    def __init__(self, base, worker_count: int, tables: Sequence[Any] = ()) -> None:
        super().__init__(base, worker_count, tables)
        self._snapshot_id: Optional[str] = None
        self._armed: set = set()

    def _start(self) -> None:
        if not hasattr(self.base, "_new_snapshot_connection"):
            raise SnapshotViewError(f"{self.base.name} does not provide snapshot connections")

        anchor = self.base._new_snapshot_connection("anchor")
        self._anchor = anchor
        anchor.autocommit = True
        self._apply_settings(anchor)
        self._check_server_version(anchor)
        self._check_tables(anchor)
        # End the (implicit) autocommit-free transaction opened by the checks
        # before switching to an explicit snapshot transaction.
        anchor.rollback()
        anchor.autocommit = False

        anchor.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
        cursor = anchor.cursor()
        try:
            cursor.execute("SELECT pg_export_snapshot()")
            row = cursor.fetchone()
        finally:
            cursor.close()
        if not row or not row[0]:
            raise SnapshotViewError("PostgreSQL did not return a snapshot identifier")
        self._snapshot_id = row[0]

        for _ in range(self.worker_count):
            connection = self.base._new_snapshot_connection("worker")
            self._connections.append(connection)
            connection.autocommit = True
            self._apply_settings(connection)
            connection.rollback()
            connection.autocommit = False
            self._arm(connection)
            self._available.put(connection)

        logger.debug(
            "[%s] Opened snapshot view %s with %d worker connection(s)",
            self.base.name,
            self._snapshot_id,
            self.worker_count,
        )

    def _apply_settings(self, connection) -> None:
        """Apply the same session defaults data-diff gives the caller.

        ``connect()`` runs ``set_timezone_to_utc()`` on the caller's connections,
        so new snapshot connections must do the same to compare values
        identically.  This is a session default only (``SET``, not a commit),
        and it never touches the caller's pool.

        Any *extra* GUCs a caller set manually on its own pooled connection are
        necessarily per-connection and cannot be mirrored without touching that
        pool; pass them as connection options in the URI/dict instead.
        """
        try:
            sql = self.base.dialect.set_timezone_to_utc()
        except NotImplementedError:
            return
        cursor = connection.cursor()
        try:
            cursor.execute(sql)
        finally:
            cursor.close()

    def _check_server_version(self, connection) -> None:
        cursor = connection.cursor()
        try:
            cursor.execute("SHOW server_version_num")
            (version,) = cursor.fetchone()
        finally:
            cursor.close()
        version_num = int(version)
        if not (160000 <= version_num < 170000):
            raise SnapshotViewError(
                "consistency='snapshot' is only supported on PostgreSQL 16 "
                f"(server reported version_num={version_num})"
            )

    def _check_tables(self, connection) -> None:
        """Reject anything that is not a plain, non-temporary PostgreSQL table."""
        for path in self.tables:
            path = tuple(path)
            _, schema, name = self.base._normalize_table_path(path)
            cursor = connection.cursor()
            try:
                cursor.execute(
                    "SELECT c.relkind, c.relpersistence, n.nspname "
                    "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE n.nspname = %s AND c.relname = %s",
                    (schema, name),
                )
                row = cursor.fetchone()
            finally:
                cursor.close()
            if row is None:
                raise SnapshotViewError(f"Table {'.'.join(path)} does not exist")
            relkind, relpersistence, nspname = row
            if relkind != "r" or relpersistence == "t" or nspname.startswith("pg_temp"):
                kind = {
                    "v": "a view",
                    "m": "a materialized view",
                    "f": "a foreign table",
                    "p": "a partitioned table",
                    "t": "a TOAST table",
                }.get(relkind, f"relkind={relkind!r}")
                raise SnapshotViewError(
                    "consistency='snapshot' only supports ordinary PostgreSQL tables, "
                    f"but {'.'.join(path)} is {kind}"
                )

    def _arm(self, connection) -> None:
        connection.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
        cursor = connection.cursor()
        try:
            # SET TRANSACTION SNAPSHOT must be the first statement of the
            # transaction; set_session() above only changes session defaults.
            cursor.execute("SET TRANSACTION SNAPSHOT %s", (self._snapshot_id,))
        finally:
            cursor.close()
        self._armed.add(connection)

    def _execute_one(self, connection, sql_code) -> QueryResult:
        try:
            if connection not in self._armed:
                self._arm(connection)
        except Exception:
            self._armed.discard(connection)
            raise
        return self.base._query_conn(connection, sql_code)

    def _discard(self, connection) -> None:
        self._armed.discard(connection)
        self._rollback(connection)

    def _close_connections(self) -> None:
        self._armed.clear()


def open_snapshot_views(
    databases: Sequence[Database], worker_counts: Sequence[int], tables: Sequence[Sequence[Any]] = ()
) -> list[SnapshotView]:
    """Open one view per side, all-or-nothing.

    If a later view cannot be established, the views already opened are closed
    before the original exception is raised, so no half-prepared side leaks
    connections.
    """
    if not tables:
        tables = [() for _ in databases]
    if len(tables) != len(databases):
        raise ValueError("Expected one table list per database")

    views: list[SnapshotView] = []
    try:
        for database, workers, side_tables in zip(databases, worker_counts, tables):
            opener = getattr(database, "open_snapshot_view", None)
            if opener is None or not database.supports_snapshot_consistency():
                raise SnapshotViewError(f"{database.name} does not support snapshot consistency")
            view = opener(workers, side_tables)
            view.start()
            views.append(view)
    except BaseException:
        for view in views:
            try:
                view.close()
            except Exception:  # pragma: no cover - never mask the original error
                logger.debug("Error while cleaning up a partially opened snapshot view", exc_info=True)
        raise
    return views


__all__ = [
    "PostgresSnapshotView",
    "SnapshotDatabase",
    "SnapshotView",
    "SnapshotViewClosedError",
    "SnapshotViewError",
    "open_snapshot_views",
]
