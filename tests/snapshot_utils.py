"""Helpers for the snapshot-consistency tests.

These tests need to *drive* the diff step by step (so that writes can be
committed at precisely defined points) and to observe the real PostgreSQL
sessions.  Everything here is aimed at Real PostgreSQL 16; there is no sleep
based synchronisation.
"""

import threading
import time

import attrs

import data_diff.databases.postgresql as pg_module
from data_diff.databases._connect import connect
from data_diff.queries.api import commit, table
from data_diff.tracking import disable_tracking

disable_tracking()

PG_URI = "postgresql://postgres:Password1@localhost:5432/postgres"


def admin_connection(uri=PG_URI, **kw):
    return connect(uri, thread_count=1, shared=False, **kw)


class Barrier:
    """A one-shot, thread-safe gate.

    ``arrive()`` is called from the code under test; ``wait()`` is called from
    the test thread.  When ``expected`` callers have arrived, everyone is
    released.  No sleeping is involved.
    """

    def __init__(self, expected=1):
        self.expected = expected
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._count = 0
        self._open = False

    def arrive(self):
        with self._cond:
            self._count += 1
            if self._count >= self.expected:
                self._open = True
                self._cond.notify_all()
            else:
                while not self._open:
                    self._cond.wait()

    def wait(self, timeout=10.0):
        with self._cond:
            if not self._open:
                self._cond.wait(timeout)
            return self._open


class HookedConnection:
    """Wraps a psycopg2 connection, calling ``on_execute`` before each execute.

    Only used to insert deterministic barriers into the query stream; it
    forwards everything else untouched.
    """

    def __init__(self, conn, on_execute=None):
        self._conn = conn
        self._on_execute = on_execute

    def cursor(self, *args, **kwargs):
        return HookedCursor(self._conn.cursor(*args, **kwargs), self._on_execute)

    def __getattr__(self, name):
        return getattr(self._conn, name)


class HookedCursor:
    def __init__(self, cursor, on_execute=None):
        self._cursor = cursor
        self._on_execute = on_execute

    def execute(self, *args, **kwargs):
        if self._on_execute is not None:
            self._on_execute(args[0] if args else "")
        return self._cursor.execute(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._cursor, name)

    def __iter__(self):
        return iter(self._cursor)


@attrs.define
class QueryTap:
    """Records the SQL of every query a given database runs.

    ``before`` may raise to abort a query at a chosen point.
    """

    before: object = None
    queries: list = attrs.field(factory=list)
    lock: threading.Lock = attrs.field(factory=threading.Lock)

    def on_execute(self, sql):
        with self.lock:
            self.queries.append(sql)
        if self.before is not None:
            self.before(sql)

    def matching(self, needle):
        with self.lock:
            return [q for q in self.queries if needle in q]

    def clear(self):
        with self.lock:
            self.queries.clear()


def install_connection_hook(monkeypatch, tap):
    """Make all snapshot connections for PostgreSQL go through a QueryTap."""

    original = pg_module.PostgreSQL._new_snapshot_connection

    def hooked(self, role="worker"):
        conn = original(self, role)
        return HookedConnection(conn, tap.on_execute)

    monkeypatch.setattr(pg_module.PostgreSQL, "_new_snapshot_connection", hooked)
    return original


def pg_activity(queryable, application_name_like="data_diff_snapshot%"):
    """Read pg_stat_activity.

    ``queryable`` may be a data_diff Database or a raw psycopg2 connection.
    """
    sql = (
        "SELECT pid, state, application_name, query FROM pg_stat_activity "
        f"WHERE application_name LIKE '{application_name_like}'"
    )
    if hasattr(queryable, "cursor"):
        cur = queryable.cursor()
        try:
            cur.execute(sql)
            rows = cur.fetchall()
        finally:
            cur.close()
    else:
        rows = queryable.query(sql, list)
    return [dict(pid=r[0], state=r[1], application_name=r[2], query=r[3]) for r in rows]


def activity_connection():
    """A raw autocommit psycopg2 connection for reading pg_stat_activity.

    pg_stat_activity only shows backends that existed when the reading
    transaction took its snapshot.  A non-autocommit connection (like a
    long-lived data_diff Database) can therefore miss sessions created later,
    so observation must happen on an autocommit connection.
    """
    import psycopg2

    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        user="postgres",
        password="Password1",
        dbname="postgres",
        application_name="test_observer",
    )
    conn.autocommit = True
    return conn


def wait_until(predicate, timeout=10.0, interval=0.01):
    """Small polling helper for *server-side* observations (activity views).

    Only used to wait for PostgreSQL to report a state change; it never decides
    *where* in the diff we are, so it does not make the test timing-dependend in
    the way ``sleep`` would.
    """
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def make_tables(admin, src_name, dst_name, rows, schema="id int primary key, v text"):
    admin.query(
        [
            f"DROP TABLE IF EXISTS {src_name}",
            f"DROP TABLE IF EXISTS {dst_name}",
            f"CREATE TABLE {src_name}({schema})",
            f"CREATE TABLE {dst_name}({schema})",
            table(src_name).insert_rows(rows),
            commit,
        ]
    )


def insert(admin, name, rows):
    admin.query([table(name).insert_rows(rows), commit])


def commit_sql(admin, *sql):
    admin.query(list(sql) + [commit])
