"""Real-PostgreSQL-16 tests for ``consistency='snapshot'``.

Test choreography uses deterministic DB barriers (blocking functions in a
scratch *gate* table, and hook points in the snapshot layer) instead of sleeps.
"""

import threading
import time
import unittest

import attrs

from data_diff import Algorithm, HashDiffer, TableSegment, diff_tables
from data_diff.diff_tables import Consistency
from data_diff.errors import (
    DataDiffConsistencyError,
    DataDiffIncompleteResultError,
    DataDiffUnsupportedConsistencyError,
)
from data_diff.queries.api import commit
from data_diff.utils import Vector
from tests.snapshot_utils import (
    PG_URI,
    QueryTap,
    activity_connection,
    admin_connection,
    insert,
    pg_activity,
    wait_until,
)


def snapshot_database(thread_count=4):
    from data_diff.databases._connect import connect

    return connect(PG_URI, thread_count=thread_count, shared=False)


class SnapshotTestBase(unittest.TestCase):
    SRC = "snap_src"
    DST = "snap_dst"

    def setUp(self):
        self.admin = admin_connection()
        self.admin.query(
            [
                f"DROP TABLE IF EXISTS {self.SRC}",
                f"DROP TABLE IF EXISTS {self.DST}",
                f"CREATE TABLE {self.SRC}(id int primary key, v text)",
                f"CREATE TABLE {self.DST}(id int primary key, v text)",
                commit,
            ]
        )
        self.databases = []

    def tearDown(self):
        for db in self.databases:
            try:
                db.close()
            except Exception:
                pass
        try:
            drop_gate(self.admin)
        except Exception:
            pass
        self.admin.query([f"DROP TABLE IF EXISTS {self.SRC}", f"DROP TABLE IF EXISTS {self.DST}", commit])
        self.admin.close()
        # No snapshot sessions may survive a test.
        observer = activity_connection()
        try:
            leftover = pg_activity(observer)
        finally:
            observer.close()
        assert leftover == [], leftover

    def db(self, thread_count=4, uri=PG_URI):
        from data_diff.databases._connect import connect

        d = connect(uri, thread_count=thread_count, shared=False)
        self.databases.append(d)
        return d

    def segment(self, db, name, extra_columns=("v",), **kw):
        return TableSegment(db, ("public", name), ("id",), extra_columns=extra_columns, **kw)

    def segments(self, db1, db2, **kw):
        return self.segment(db1, self.SRC, **kw), self.segment(db2, self.DST, **kw)

    def rows(self, name):
        return self.admin.query(f"SELECT id, v FROM {name} ORDER BY id", list)

    def seed(self, src_rows, dst_rows):
        if src_rows:
            insert(self.admin, self.SRC, src_rows)
        if dst_rows:
            insert(self.admin, self.DST, dst_rows)

    def small_differ(self):
        return HashDiffer(bisection_factor=4, bisection_threshold=8, max_threadpool_size=4)


# SQL that blocks on an advisory lock until the test releases it.  Used as a
# deterministic barrier inside a *particular* query of the diff.
class Gate:
    """A server-side barrier that blocks *only* matching queries.

    ``gate_fn(k, marker)`` checks ``current_query()``; if the statement text
    contains ``marker`` it waits on a shared advisory lock until the test
    releases the exclusive lock.  Other statements pass straight through, so we
    can, for example, leave the min/max probing alone and hold only the
    count/checksum queries at the segment level.
    """

    _next_key = 700000

    def __init__(self, admin):
        self.admin = admin
        # ``admin`` is a thread_count=1 Database, so hold and release always
        # run on the same server session (advisory locks are session-scoped).
        self.admin.query("DROP FUNCTION IF EXISTS gate_fn(bigint, text) CASCADE", None)
        self.admin.query(
            """
            CREATE FUNCTION gate_fn(k bigint, marker text) RETURNS int LANGUAGE plpgsql AS $$
            DECLARE
              hits int;
            BEGIN
              -- The marker is also present in the gate's own literal, so we
              -- count occurrences and require the target statement to contain
              -- it at least twice (once for real, once for the literal).
              hits := (length(current_query()) - length(replace(current_query(), marker, ''))) / length(marker);
              IF hits >= 2 THEN
                -- Shared locks let many sessions wait at once; the test holds
                -- the exclusive lock and releases them all together.
                PERFORM pg_advisory_lock_shared(k);
                PERFORM pg_advisory_unlock_shared(k);
              END IF;
              RETURN 1;
            END $$;
            """,
            None,
        )

    def new_key(self):
        Gate._next_key += 1
        return Gate._next_key

    def hold(self, key):
        self.admin.query(f"SELECT pg_advisory_lock({key})", None)

    def release(self, key):
        self.admin.query(f"SELECT pg_advisory_unlock({key})", None)

    def close(self):
        pass


def gate_where(key, marker):
    "A WHERE clause that invokes the gate for statements containing ``marker``."
    escaped = marker.replace("'", "''")
    return f"(SELECT gate_fn({key}, '{escaped}')) IS NOT NULL"


def drop_gate(admin):
    admin.query("DROP FUNCTION IF EXISTS gate_fn(bigint) CASCADE", None)
    admin.query("DROP FUNCTION IF EXISTS gate_fn(bigint, text) CASCADE", None)


# ---------------------------------------------------------------------------


class TestSnapshotCorrectness(SnapshotTestBase):
    def test_simple_diff(self):
        self.seed([(i, f"x{i}") for i in range(1, 101)], [(i, f"x{i}") for i in range(1, 101)])
        self.admin.query("UPDATE snap_dst SET v='changed' WHERE id=50")
        a, b = self.segments(self.db(), self.db())
        result = self.small_differ().diff_tables(a, b, consistency="snapshot")
        self.assertEqual(list(result), [("-", ("50", "x50")), ("+", ("50", "changed"))])
        stats = result.get_stats_dict()
        self.assertEqual(stats["rows_A"], 100)
        self.assertEqual(stats["rows_B"], 100)
        self.assertEqual(stats["updated"], 1)
        result.close()

    def test_derived_segments_keep_the_view(self):
        # Bisection-derived TableSegments must keep the view-bound database, so
        # every recursive query stays inside the same snapshot.
        from data_diff.consistency import SnapshotDatabase

        db = self.db()
        ts = self.segment(db, self.SRC)
        view = db.open_snapshot_view(2, [ts.table_path])
        view.start()
        try:
            bound = ts.new(database=view.database)
            child = bound.new_key_bounds(min_key=Vector((1,)), max_key=Vector((10,)))
            self.assertIsInstance(child.database, SnapshotDatabase)
            self.assertIs(child.database, view.database)
            points = [[1, 11, 21]]
            for seg in bound.new_key_bounds(min_key=Vector((1,)), max_key=Vector((30,))).segment_by_checkpoints(points):
                self.assertIs(seg.database, view.database)
        finally:
            view.close()

    def test_lazy_result_opens_no_connection(self):
        self.seed([(1, "a")], [(1, "a")])
        a, b = self.segments(self.db(), self.db())
        tap = QueryTap()
        import data_diff.databases.postgresql as pg_mod
        from tests.snapshot_utils import HookedConnection

        original = pg_mod.PostgreSQL._new_snapshot_connection

        def hooked(dbself, role="worker"):
            return HookedConnection(original(dbself, role), tap.on_execute)

        pg_mod.PostgreSQL._new_snapshot_connection = hooked
        try:
            result = self.small_differ().diff_tables(a, b, consistency="snapshot")
            self.assertEqual(tap.queries, [])
            list(result)
            self.assertGreater(len(tap.queries), 0)
            result.close()
        finally:
            pg_mod.PostgreSQL._new_snapshot_connection = original

    def test_same_database_object_both_sides(self):
        # Two TableSegments sharing one Database object still get two separate
        # per-side snapshots and views.
        self.seed([(1, "a"), (2, "b")], [(1, "a"), (2, "c")])
        db = self.db()
        a, b = self.segment(db, self.SRC), self.segment(db, self.DST)
        result = self.small_differ().diff_tables(a, b, consistency="snapshot")
        rows = list(result)
        self.assertEqual({r[0] for r in rows}, {"-", "+"})
        self.assertEqual(rows[0][1][0], "2")
        self.assertEqual(result.get_stats_dict()["rows_A"], 2)
        result.close()

    def test_live_default_unchanged(self):
        self.seed([(1, "a")], [(1, "a")])
        a, b = self.segments(self.db(), self.db())
        # No consistency argument at all -> live; result object is the usual wrapper.
        from data_diff.diff_tables import DiffResultWrapper

        result = self.small_differ().diff_tables(a, b)
        self.assertIsInstance(result, DiffResultWrapper)
        self.assertEqual(result.consistency, Consistency.LIVE)
        self.assertEqual(list(result), [])
        result.close()

    def test_empty_tables(self):
        a, b = self.segments(self.db(), self.db())
        result = self.small_differ().diff_tables(a, b, consistency="snapshot")
        with self.assertRaises(ValueError):
            list(result)
        result.close()

    def test_empty_one_side(self):
        self.seed([(i, f"x{i}") for i in range(1, 20)], [])
        a, b = self.segments(self.db(), self.db())
        with self.assertRaises(ValueError):
            list(self.small_differ().diff_tables(a, b, consistency="snapshot"))

    def test_filter(self):
        self.seed([(i, f"x{i}") for i in range(1, 51)], [(i, f"x{i}") for i in range(1, 51)])
        self.admin.query("UPDATE snap_dst SET v='diff' WHERE id=10")
        self.admin.query("UPDATE snap_dst SET v='outside' WHERE id=40")
        a, b = self.segments(self.db(), self.db())
        a = attrs.evolve(a, where="id < 30")
        b = attrs.evolve(b, where="id < 30")
        result = self.small_differ().diff_tables(a, b, consistency="snapshot")
        rows = list(result)
        self.assertEqual([r[0] for r in rows], ["-", "+"])
        self.assertEqual(rows[0][1][0], "10")
        stats = result.get_stats_dict()
        self.assertEqual(stats["rows_A"], 29)
        self.assertEqual(stats["rows_B"], 29)
        result.close()

    def test_compound_key(self):
        self.admin.query(
            [
                f"DROP TABLE IF EXISTS {self.SRC}",
                f"DROP TABLE IF EXISTS {self.DST}",
                f"CREATE TABLE {self.SRC}(a int, b int, v text, primary key(a,b))",
                f"CREATE TABLE {self.DST}(a int, b int, v text, primary key(a,b))",
                commit,
            ]
        )
        rows = [(i // 7, i % 7, f"x{i}") for i in range(1, 60)]
        insert(self.admin, self.SRC, rows)
        insert(self.admin, self.DST, rows)
        self.admin.query(f"UPDATE {self.DST} SET v='diff' WHERE a=3 AND b=2")
        d1, d2 = self.db(), self.db()
        a = TableSegment(d1, ("public", self.SRC), ("a", "b"), extra_columns=("v",))
        b = TableSegment(d2, ("public", self.DST), ("a", "b"), extra_columns=("v",))
        result = self.small_differ().diff_tables(a, b, consistency="snapshot")
        rows_out = list(result)
        self.assertEqual(len(rows_out), 2)
        self.assertEqual({r[0] for r in rows_out}, {"-", "+"})
        self.assertEqual(rows_out[0][1][:2], ("3", "2"))
        result.close()

    def test_precision_semantics_unchanged(self):
        # Numeric precision handling is shared with live mode; just verify a
        # rounded float column still matches.
        self.admin.query(
            [
                f"DROP TABLE IF EXISTS {self.SRC}",
                f"DROP TABLE IF EXISTS {self.DST}",
                f"CREATE TABLE {self.SRC}(id int primary key, v numeric(10,2))",
                f"CREATE TABLE {self.DST}(id int primary key, v numeric(10,2))",
                commit,
            ]
        )
        insert(self.admin, self.SRC, [(1, 1.25), (2, 3.50)])
        insert(self.admin, self.DST, [(1, 1.25), (2, 3.50)])
        a, b = self.segments(self.db(), self.db())
        result = self.small_differ().diff_tables(a, b, consistency="snapshot")
        self.assertEqual(list(result), [])
        result.close()

    def test_two_connections_same_database(self):
        # Two distinct Database objects pointing at the same PostgreSQL db.
        self.seed([(1, "a"), (2, "b")], [(1, "a"), (2, "b")])
        d1, d2 = self.db(), self.db()
        self.assertIsNot(d1, d2)
        a, b = self.segments(d1, d2)
        result = self.small_differ().diff_tables(a, b, consistency="snapshot")
        self.assertEqual(list(result), [])
        self.assertEqual(result.get_stats_dict()["rows_A"], 2)
        result.close()


class TestReadOnlyAccount(SnapshotTestBase):
    ROLE = "snap_ro_test"
    PASSWORD = "ro_pass"

    def setUp(self):
        super().setUp()
        self.admin.query(
            [
                f"DROP ROLE IF EXISTS {self.ROLE}",
                f"CREATE ROLE {self.ROLE} LOGIN PASSWORD '{self.PASSWORD}'",
                f"GRANT USAGE ON SCHEMA public TO {self.ROLE}",
                f"GRANT SELECT ON {self.SRC}, {self.DST} TO {self.ROLE}",
                commit,
            ]
        )

    def tearDown(self):
        try:
            self.admin.query(
                [
                    f"REVOKE ALL ON {self.SRC}, {self.DST} FROM {self.ROLE}",
                    f"REVOKE ALL ON SCHEMA public FROM {self.ROLE}",
                    f"DROP ROLE IF EXISTS {self.ROLE}",
                    commit,
                ]
            )
        finally:
            super().tearDown()

    def test_snapshot_works_for_select_only_role(self):
        self.seed([(i, f"x{i}") for i in range(1, 51)], [(i, f"x{i}") for i in range(1, 51)])
        self.admin.query("UPDATE snap_dst SET v='d' WHERE id=11")
        uri = f"postgresql://{self.ROLE}:{self.PASSWORD}@localhost:5432/postgres"
        db1 = self.db(uri=uri)
        db2 = self.db(uri=uri)
        a, b = self.segments(db1, db2)
        result = self.small_differ().diff_tables(a, b, consistency="snapshot")
        rows = list(result)
        self.assertEqual(rows, [("-", ("11", "x11")), ("+", ("11", "d"))])
        self.assertEqual(result.get_stats_dict()["rows_A"], 50)
        result.close()


class TestWritesDuringSnapshot(SnapshotTestBase):
    """Writes committed after the views exist must not influence this run."""

    def _run_with_mutation(self, mutate_after):
        self.seed([(i, f"x{i}") for i in range(1, 81)], [(i, f"x{i}") for i in range(1, 81)])
        self.admin.query("UPDATE snap_dst SET v='diff' WHERE id=20")

        a, b = self.segments(self.db(), self.db())
        differ = self.small_differ()
        result = differ.diff_tables(a, b, consistency="snapshot")

        # Consume a couple of rows to force the views to exist, then mutate.
        it = iter(result)
        first = next(it)
        mutate_after()
        rest = list(it)
        rows = [first] + rest
        stats = result.get_stats_dict()
        result.close()
        return rows, stats

    def test_insert_after_view_established(self):
        def mutate():
            insert(self.admin, self.DST, [(1000, "new")])
            self.admin.query("DELETE FROM snap_src WHERE id=30")
            # Change a key: delete 79 and re-insert it under a new id.
            self.admin.query("DELETE FROM snap_dst WHERE id=79")
            insert(self.admin, self.DST, [(9999, "moved")])

        rows, stats = self._run_with_mutation(mutate)
        # Only the pre-view difference is reported.
        self.assertEqual({r[0] for r in rows}, {"-", "+"})
        self.assertEqual(rows[0][1][0], "20")
        self.assertEqual(stats["rows_A"], 80)
        self.assertEqual(stats["rows_B"], 80)
        self.assertEqual(stats["exclusive_B"], 0)
        self.assertEqual(stats["updated"], 1)

        # The next run sees the new version.
        a, b = self.segments(self.db(), self.db())
        result2 = self.small_differ().diff_tables(a, b, consistency="snapshot")
        rows2 = list(result2)
        stats2 = result2.get_stats_dict()
        result2.close()
        self.assertEqual(stats2["rows_A"], 79)
        self.assertEqual(stats2["rows_B"], 81)
        ids = {r[1][0] for r in rows2}
        self.assertIn("1000", ids)
        self.assertIn("30", ids)
        self.assertIn("79", ids)
        self.assertIn("9999", ids)

    def test_mutation_between_key_range_and_checksum(self):
        self.seed([(i, f"x{i}") for i in range(1, 81)], [(i, f"x{i}") for i in range(1, 81)])
        self.admin.query("UPDATE snap_dst SET v='diff' WHERE id=20")
        a, b = self.segments(self.db(), self.db())
        tap = QueryTap()
        inserted = {"done": False}

        def before(sql):
            if not inserted["done"] and "min(" in sql:
                inserted["done"] = True
                insert(self.admin, self.DST, [(5000, "late")])
                self.admin.query("UPDATE snap_dst SET v='late-update' WHERE id=1")

        tap.before = before
        import data_diff.databases.postgresql as pg_mod

        original = pg_mod.PostgreSQL._new_snapshot_connection
        from tests.snapshot_utils import HookedConnection

        def hooked(dbself, role="worker"):
            return HookedConnection(original(dbself, role), tap.on_execute)

        pg_mod.PostgreSQL._new_snapshot_connection = hooked
        try:
            result = self.small_differ().diff_tables(a, b, consistency="snapshot")
            rows = list(result)
            stats = result.get_stats_dict()
            result.close()
        finally:
            pg_mod.PostgreSQL._new_snapshot_connection = original

        self.assertEqual({r[0] for r in rows}, {"-", "+"})
        self.assertEqual(stats["rows_B"], 80)
        self.assertNotIn("late", {r[1][1] for r in rows})

    def test_mutation_between_checksum_and_leaf_download(self):
        self.seed([(i, f"x{i}") for i in range(1, 81)], [(i, f"x{i}") for i in range(1, 81)])
        self.admin.query("UPDATE snap_dst SET v='diff' WHERE id=20")
        a, b = self.segments(self.db(), self.db())
        tap = QueryTap()
        state = {"checksummed": False, "mutated": False}

        def before(sql):
            if not state["mutated"] and "count(*)" in sql:
                state["checksummed"] = True
            if state["checksummed"] and not state["mutated"] and sql.strip().startswith("SELECT") and "count(*)" not in sql and "min(" not in sql and "FROM \"public\"" in sql:
                # First leaf-segment download after a checksum query.
                state["mutated"] = True
                self.admin.query("UPDATE snap_dst SET v='post-checksum' WHERE id=21")

        tap.before = before
        import data_diff.databases.postgresql as pg_mod
        from tests.snapshot_utils import HookedConnection

        original = pg_mod.PostgreSQL._new_snapshot_connection

        def hooked(dbself, role="worker"):
            return HookedConnection(original(dbself, role), tap.on_execute)

        pg_mod.PostgreSQL._new_snapshot_connection = hooked
        try:
            result = self.small_differ().diff_tables(a, b, consistency="snapshot")
            rows = list(result)
            result.close()
        finally:
            pg_mod.PostgreSQL._new_snapshot_connection = original

        # The post-checksum update must NOT appear in this run.
        self.assertNotIn("post-checksum", {r[1][1] for r in rows})
        self.assertEqual(len(rows), 2)


class TestRejection(SnapshotTestBase):
    def test_bad_consistency_value(self):
        a, b = self.segments(self.db(), self.db())
        differ = self.small_differ()
        with self.assertRaises(DataDiffConsistencyError):
            differ.diff_tables(a, b, consistency="bogus")
        with self.assertRaises(DataDiffConsistencyError):
            diff_tables(a, b, consistency="bogus")

    def test_joindiff_rejected_before_querying(self):
        a, b = self.segments(self.db(), self.db())
        tap = QueryTap()
        import data_diff.databases.postgresql as pg_mod
        from tests.snapshot_utils import HookedConnection

        original = pg_mod.PostgreSQL._new_snapshot_connection

        def hooked(dbself, role="worker"):
            return HookedConnection(original(dbself, role), tap.on_execute)

        pg_mod.PostgreSQL._new_snapshot_connection = hooked
        try:
            with self.assertRaises(DataDiffUnsupportedConsistencyError):
                diff_tables(a, b, algorithm=Algorithm.JOINDIFF, consistency="snapshot")
        finally:
            pg_mod.PostgreSQL._new_snapshot_connection = original
        # Rejected before any snapshot connection was even created.
        self.assertEqual(tap.queries, [])

    def test_auto_joindiff_rejected(self):
        # AUTO picks JOINDIFF when both tables share one Database object.
        db = self.db()
        a, b = self.segment(db, self.SRC), self.segment(db, self.DST)
        with self.assertRaises(DataDiffUnsupportedConsistencyError):
            diff_tables(a, b, algorithm=Algorithm.AUTO, consistency="snapshot")

    def test_auto_hashdiff_accepted(self):
        self.seed([(1, "a")], [(1, "a")])
        a, b = self.segments(self.db(), self.db())
        result = diff_tables(a, b, algorithm=Algorithm.AUTO, consistency="snapshot")
        self.assertEqual(list(result), [])
        result.close()

    def test_non_postgres_rejected(self):
        from data_diff.databases.duckdb import DuckDB

        try:
            db = DuckDB(filepath=":memory:", dbname="main")
        except Exception as exc:  # pragma: no cover - duckdb unavailable
            self.skipTest(f"duckdb unavailable: {exc}")
        self.admin.query(
            f"CREATE TABLE IF NOT EXISTS {self.SRC}(id int primary key, v text)", None
        )
        db.query("CREATE TABLE t(id int primary key, v text)")
        a = TableSegment(db, ("t",), ("id",), extra_columns=("v",))
        b = TableSegment(db, ("t",), ("id",), extra_columns=("v",))
        with self.assertRaises(DataDiffUnsupportedConsistencyError):
            HashDiffer().diff_tables(a, b, consistency="snapshot")
        db.close()

    def test_view_table_rejected(self):
        self.admin.query(
            f"CREATE OR REPLACE VIEW {self.SRC}_view AS SELECT id, v FROM {self.SRC}", None
        )
        try:
            a = self.segment(self.db(), f"{self.SRC}_view")
            b = self.segment(self.db(), self.DST)
            with self.assertRaises(Exception) as ctx:
                list(self.small_differ().diff_tables(a, b, consistency="snapshot"))
            self.assertIn("ordinary PostgreSQL tables", str(ctx.exception))
        finally:
            self.admin.query(f"DROP VIEW IF EXISTS {self.SRC}_view", None)


class TestLifecycle(SnapshotTestBase):
    def test_close_never_consumed(self):
        self.seed([(1, "a")], [(1, "a")])
        a, b = self.segments(self.db(), self.db())
        tap = QueryTap()
        import data_diff.databases.postgresql as pg_mod
        from tests.snapshot_utils import HookedConnection

        original = pg_mod.PostgreSQL._new_snapshot_connection

        def hooked(dbself, role="worker"):
            return HookedConnection(original(dbself, role), tap.on_execute)

        pg_mod.PostgreSQL._new_snapshot_connection = hooked
        try:
            result = self.small_differ().diff_tables(a, b, consistency="snapshot")
            result.close()
            result.close()  # idempotent
        finally:
            pg_mod.PostgreSQL._new_snapshot_connection = original
        self.assertEqual(tap.queries, [])
        with self.assertRaises(DataDiffIncompleteResultError):
            list(result)
        with self.assertRaises(DataDiffIncompleteResultError):
            result.get_stats_dict()
        observer = activity_connection()
        try:
            self.assertEqual(pg_activity(observer), [])
        finally:
            observer.close()

    def test_close_twice_after_exhaustion(self):
        self.seed([(1, "a"), (2, "b")], [(1, "a")])
        a, b = self.segments(self.db(), self.db())
        result = self.small_differ().diff_tables(a, b, consistency="snapshot")
        rows = list(result)
        result.close()
        result.close()
        self.assertEqual(list(result), rows)
        self.assertEqual(result.get_stats_dict()["rows_A"], 2)

    def test_partial_consumption_close_is_bounded(self):
        self.seed([(i, f"x{i}") for i in range(1, 4001)], [(i, f"x{i}") for i in range(1, 4001)])
        for i in range(7, 4000, 13):
            self.admin.query(f"UPDATE {self.DST} SET v='d{i}' WHERE id={i}")

        a, b = self.segments(self.db(thread_count=4), self.db(thread_count=4))
        differ = HashDiffer(bisection_factor=4, bisection_threshold=5, max_threadpool_size=4)
        result = differ.diff_tables(a, b, consistency="snapshot")
        it = iter(result)
        next(it)  # consume one row, leave the rest in flight
        start = time.monotonic()
        result.close()
        elapsed = time.monotonic() - start
        self.assertLess(elapsed, 5.0)
        with self.assertRaises(DataDiffIncompleteResultError):
            list(result)
        with self.assertRaises(DataDiffIncompleteResultError):
            result.get_stats_string()
        # No sessions left behind.
        observer = activity_connection()
        try:
            assert pg_activity(observer) == []
        finally:
            observer.close()

    def test_exhausted_result_reread_after_close(self):
        self.seed([(i, f"x{i}") for i in range(1, 101)], [(i, f"x{i}") for i in range(1, 101)])
        self.admin.query("UPDATE snap_dst SET v='z' WHERE id=9")
        a, b = self.segments(self.db(), self.db())
        result = self.small_differ().diff_tables(a, b, consistency="snapshot")
        with result as res:
            first = list(res)
            stats = res.get_stats_dict()
        self.assertEqual(first, [("-", ("9", "x9")), ("+", ("9", "z"))])
        # After close (and __exit__), re-reading the cache still works.
        self.assertEqual(list(result), first)
        self.assertEqual(result.get_stats_dict(), stats)
        result.close()

    def test_keyboard_interrupt_in_consumer_triggers_bounded_close(self):
        self.seed([(i, f"x{i}") for i in range(1, 1001)], [(i, f"x{i}") for i in range(1, 1001)])
        for i in range(5, 999, 11):
            self.admin.query(f"UPDATE {self.DST} SET v='ki{i}' WHERE id={i}")
        a, b = self.segments(self.db(), self.db())
        differ = HashDiffer(bisection_factor=4, bisection_threshold=5, max_threadpool_size=4)
        result = differ.diff_tables(a, b, consistency="snapshot")
        it = iter(result)
        next(it)
        start = time.monotonic()
        # Simulate the caller being interrupted while iterating.
        try:
            raise KeyboardInterrupt()
        except KeyboardInterrupt:
            result.close()
        self.assertLess(time.monotonic() - start, 5.0)
        with self.assertRaises(DataDiffIncompleteResultError):
            list(result)
        observer = activity_connection()
        try:
            self.assertEqual(pg_activity(observer), [])
        finally:
            observer.close()

    def test_context_manager_returns_wrapper(self):
        self.seed([(1, "a")], [(1, "a")])
        a, b = self.segments(self.db(), self.db())
        with self.small_differ().diff_tables(a, b, consistency="snapshot") as result:
            self.assertEqual(list(result), [])

    def test_query_failure_is_sticky(self):
        self.seed([(i, f"x{i}") for i in range(1, 201)], [(i, f"x{i}") for i in range(1, 201)])
        a, b = self.segments(self.db(), self.db())
        tap = QueryTap()
        triggered = {"done": False}

        def before(sql):
            if not triggered["done"] and "count(*)" in sql:
                triggered["done"] = True
                raise RuntimeError("injected query failure")

        tap.before = before
        import data_diff.databases.postgresql as pg_mod
        from tests.snapshot_utils import HookedConnection

        original = pg_mod.PostgreSQL._new_snapshot_connection

        def hooked(dbself, role="worker"):
            return HookedConnection(original(dbself, role), tap.on_execute)

        pg_mod.PostgreSQL._new_snapshot_connection = hooked
        try:
            result = self.small_differ().diff_tables(a, b, consistency="snapshot")
            with self.assertRaises(RuntimeError):
                list(result)
            # Failure fact is retained; stats must not become a success summary.
            with self.assertRaises(RuntimeError):
                result.get_stats_dict()
            with self.assertRaises(RuntimeError):
                list(result)
            result.close()
        finally:
            pg_mod.PostgreSQL._new_snapshot_connection = original

    def test_keyboard_interrupt_cleanup(self):
        self.seed([(i, f"x{i}") for i in range(1, 2001)], [(i, f"x{i}") for i in range(1, 2001)])
        for i in range(5, 2000, 17):
            self.admin.query(f"UPDATE {self.DST} SET v='k{i}' WHERE id={i}")
        a, b = self.segments(self.db(), self.db())
        differ = HashDiffer(bisection_factor=4, bisection_threshold=5, max_threadpool_size=4)
        result = differ.diff_tables(a, b, consistency="snapshot")
        it = iter(result)
        next(it)
        start = time.monotonic()
        result.close()
        self.assertLess(time.monotonic() - start, 5.0)
        observer = activity_connection()
        try:
            assert pg_activity(observer) == []
        finally:
            observer.close()


class TestIsolationBetweenRuns(SnapshotTestBase):
    def test_interleaved_two_diffs(self):
        self.seed([(i, f"x{i}") for i in range(1, 501)], [(i, f"x{i}") for i in range(1, 501)])
        self.admin.query("UPDATE snap_dst SET v='first' WHERE id=100")
        a, b = self.segments(self.db(), self.db())
        differ = self.small_differ()
        run1 = differ.diff_tables(a, b, consistency="snapshot")
        # Consume one row so run1's views are established *now*, before the
        # next write.  Creating the lazy result alone opens no transaction.
        it1 = iter(run1)
        r1 = [next(it1)]

        self.admin.query("UPDATE snap_dst SET v='second' WHERE id=200")
        run2 = differ.diff_tables(a, b, consistency="snapshot")
        it2 = iter(run2)
        r2 = [next(it2)]

        # Interleave the rest.
        r1 += list(it1)
        r2 += list(it2)

        self.assertEqual({x[1][0] for x in r1}, {"100"})
        self.assertEqual({x[1][0] for x in r2}, {"100", "200"})
        self.assertEqual(run1.get_stats_dict()["updated"], 1)
        self.assertEqual(run2.get_stats_dict()["updated"], 2)
        run1.close()
        run2.close()

    def test_concurrent_two_diffs(self):
        self.seed([(i, f"x{i}") for i in range(1, 801)], [(i, f"x{i}") for i in range(1, 801)])
        a, b = self.segments(self.db(), self.db())
        differ = self.small_differ()
        results = {}
        errors = []

        def run(name):
            try:
                r = differ.diff_tables(a, b, consistency="snapshot")
                results[name] = (list(r), r.get_stats_dict())
                r.close()
            except Exception as exc:  # pragma: no cover - fail loudly
                errors.append(exc)

        threads = [threading.Thread(target=run, args=(n,)) for n in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(30)
        self.assertEqual(errors, [])
        self.assertEqual(len(results), 3)
        for rows, stats in results.values():
            self.assertEqual(rows, [])
            self.assertEqual(stats["rows_A"], 800)

    def test_live_queries_on_original_connection_during_snapshot(self):
        self.seed([(i, f"x{i}") for i in range(1, 201)], [(i, f"x{i}") for i in range(1, 201)])
        self.admin.query("UPDATE snap_dst SET v='pre' WHERE id=3")
        d1 = self.db()
        d2 = self.db()
        a, b = self.segments(d1, d2)
        result = self.small_differ().diff_tables(a, b, consistency="snapshot")
        it = iter(result)
        first = next(it)
        self.assertEqual(first[1][0], "3")
        # The caller's own Database objects must still see live data.
        insert(self.admin, self.DST, [(9001, "live-only")])
        live_ids = {r[0] for r in d2.query(f"SELECT id FROM {self.DST} WHERE id=9001", list)}
        self.assertEqual(live_ids, {9001})
        self.assertEqual(d2.query(f"SELECT count(*) FROM {self.DST}", int), 201)
        rest = list(it)
        self.assertNotIn("9001", {r[1][0] for r in rest})
        result.close()


class TestStalledServer(SnapshotTestBase):
    def test_close_during_server_stall_is_bounded(self):
        self.seed([(i, f"x{i}") for i in range(1, 301)], [(i, f"x{i}") for i in range(1, 301)])
        for i in range(5, 300, 7):
            self.admin.query(f"UPDATE {self.DST} SET v='s{i}' WHERE id={i}")
        gate = Gate(self.admin)
        key = gate.new_key()
        gate.hold(key)  # never released until after close

        a, b = self._gated_segments(key) if hasattr(self, "_gated_segments") else (
            self.segment(self.db(thread_count=4), self.SRC, where=gate_where(key, "count(*)")),
            self.segment(self.db(thread_count=4), self.DST, where=gate_where(key, "count(*)")),
        )
        differ = HashDiffer(bisection_factor=8, bisection_threshold=9, max_threadpool_size=4)
        result = differ.diff_tables(a, b, consistency="snapshot")
        it = iter(result)
        consumer = threading.Thread(target=lambda: list(it) if False else None)
        holder = {}

        def consume():
            try:
                holder["rows"] = list(it)
            except BaseException as exc:
                holder["error"] = exc

        consumer = threading.Thread(target=consume)
        consumer.start()
        observer = activity_connection()
        try:
            wait_until(
                lambda: len([x for x in pg_activity(observer) if "gate_fn" in (x["query"] or "")]) >= 1,
                timeout=20,
            )
            start = time.monotonic()
            result.close()
            elapsed = time.monotonic() - start
        finally:
            gate.release(key)
            consumer.join(30)
            observer.close()
            gate.close()
        self.assertLess(elapsed, 5.0)
        self.assertEqual(holder.get("rows") or [], [])
        self.assertIsInstance(holder.get("error"), DataDiffIncompleteResultError)
        observer = activity_connection()
        try:
            self.assertEqual(pg_activity(observer), [])
        finally:
            observer.close()


class TestFailureCleanup(SnapshotTestBase):
    def test_second_side_view_failure_releases_first(self):
        self.seed([(1, "a")], [(1, "a")])
        import data_diff.databases.postgresql as pg_mod
        from data_diff.consistency import SnapshotViewError

        original = pg_mod.PostgreSQL._new_snapshot_connection
        calls = {"n": 0}

        def flaky(dbself, role="worker"):
            # Two anchors are created (one per side).  Fail the second one,
            # after the first side's view is fully up.
            if role == "anchor":
                calls["n"] += 1
                if calls["n"] == 2:
                    raise SnapshotViewError("simulated failure opening the second side")
            return original(dbself, role)

        pg_mod.PostgreSQL._new_snapshot_connection = flaky
        try:
            a, b = self.segments(self.db(), self.db())
            result = self.small_differ().diff_tables(a, b, consistency="snapshot")
            with self.assertRaises(SnapshotViewError):
                list(result)
            # No diff rows were produced.
            self.assertEqual(result.result_list, [])
            result.close()
        finally:
            pg_mod.PostgreSQL._new_snapshot_connection = original
        observer = activity_connection()
        try:
            self.assertEqual(pg_activity(observer), [])
        finally:
            observer.close()

    def test_worker_connection_init_failure(self):
        self.seed([(1, "a")], [(1, "a")])
        import data_diff.databases.postgresql as pg_mod
        from data_diff.consistency import SnapshotViewError

        original = pg_mod.PostgreSQL._new_snapshot_connection

        def flaky(dbself, role="worker"):
            if role == "worker":
                raise SnapshotViewError("simulated worker connection failure")
            return original(dbself, role)

        pg_mod.PostgreSQL._new_snapshot_connection = flaky
        try:
            a, b = self.segments(self.db(), self.db())
            result = self.small_differ().diff_tables(a, b, consistency="snapshot")
            with self.assertRaises(SnapshotViewError):
                list(result)
            result.close()
        finally:
            pg_mod.PostgreSQL._new_snapshot_connection = original
        observer = activity_connection()
        try:
            self.assertEqual(pg_activity(observer), [])
        finally:
            observer.close()

    def test_cleanup_error_does_not_mask_original(self):
        self.seed([(1, "a")], [(1, "a")])
        import data_diff.databases.postgresql as pg_mod
        from data_diff.consistency import SnapshotViewError

        original = pg_mod.PostgreSQL._new_snapshot_connection
        calls = {"n": 0}
        boom = SnapshotViewError("original failure")

        def flaky(dbself, role="worker"):
            if role == "anchor":
                calls["n"] += 1
                if calls["n"] == 2:
                    raise boom
            return original(dbself, role)

        # Also make closing a view raise, to check it cannot mask the original.
        original_close = pg_mod.PostgreSQL.open_snapshot_view

        def open_view(self, worker_count, tables=()):
            view = original_close(self, worker_count, tables)
            real_close = type(view).close

            def bad_close(*a, **kw):
                try:
                    # Actually release resources, then fail as if cleanup broke.
                    real_close(view, *a, **kw)
                finally:
                    raise RuntimeError("cleanup exploded")

            view.close = bad_close
            return view

        pg_mod.PostgreSQL.open_snapshot_view = open_view
        pg_mod.PostgreSQL._new_snapshot_connection = flaky
        try:
            a, b = self.segments(self.db(), self.db())
            result = self.small_differ().diff_tables(a, b, consistency="snapshot")
            with self.assertRaises(SnapshotViewError) as ctx:
                list(result)
            self.assertIs(ctx.exception, boom)
            result.close()
        finally:
            pg_mod.PostgreSQL.open_snapshot_view = original_close
            pg_mod.PostgreSQL._new_snapshot_connection = original
        observer = activity_connection()
        try:
            self.assertEqual(pg_activity(observer), [])
        finally:
            observer.close()


class TestParallelism(SnapshotTestBase):
    def _gated_segments(self, key, marker="count(*)"):
        where = gate_where(key, marker)
        return (
            self.segment(self.db(thread_count=4), self.SRC, where=where),
            self.segment(self.db(thread_count=4), self.DST, where=where),
        )

    def test_two_segment_queries_run_concurrently(self):
        self.seed([(i, f"x{i}") for i in range(1, 1201)], [(i, f"x{i}") for i in range(1, 1201)])
        for i in range(10, 1200, 29):
            self.admin.query(f"UPDATE {self.DST} SET v='p{i}' WHERE id={i}")
        gate = Gate(self.admin)
        key = gate.new_key()
        gate.hold(key)

        a, b = self._gated_segments(key)
        differ = HashDiffer(bisection_factor=8, bisection_threshold=9, max_threadpool_size=4)
        result = differ.diff_tables(a, b, consistency="snapshot")
        holder = {}

        def consume():
            try:
                holder["rows"] = list(result)
            except BaseException:  # pragma: no cover - diagnostic
                import traceback

                holder["error"] = traceback.format_exc()

        observer_db = activity_connection()

        def same_side_segment_queries():
            return [
                x
                for x in pg_activity(observer_db)
                if x["state"] == "active"
                and x["application_name"] == "data_diff_snapshot:worker"
                and '"public"."snap_src"' in (x["query"] or "")
            ]

        consumer = threading.Thread(target=consume)
        consumer.start()
        try:
            blocked = wait_until(lambda: len(same_side_segment_queries()) >= 2, timeout=20)
            if not blocked:
                import sys
                import traceback

                frames = sys._current_frames()
                stack = []
                if consumer.ident in frames:
                    stack = traceback.format_list(traceback.extract_stack(frames[consumer.ident])[-15:])
                acts = pg_activity(observer_db, "%")
                self.fail(
                    {
                        "error": holder.get("error"),
                        "alive": consumer.is_alive(),
                        "stack": "".join(stack),
                        "match": same_side_segment_queries(),
                        "all": [(a["state"], a["application_name"], (a["query"] or "")[:80]) for a in acts],
                    }
                )
        finally:
            gate.release(key)
            consumer.join(30)
            observer_db.close()
            gate.close()
        self.assertGreater(len(holder["rows"]), 0)
        result.close()

    def test_connection_count_bounded(self):
        self.seed([(i, f"x{i}") for i in range(1, 401)], [(i, f"x{i}") for i in range(1, 401)])
        thread_count = 4
        gate = Gate(self.admin)
        key = gate.new_key()
        gate.hold(key)

        a, b = self._gated_segments(key)
        differ = HashDiffer(bisection_factor=8, bisection_threshold=9, max_threadpool_size=thread_count)
        result = differ.diff_tables(a, b, consistency="snapshot")
        holder = {}

        def consume():
            holder["rows"] = list(result)

        consumer = threading.Thread(target=consume)
        consumer.start()
        peak = {"value": 0}
        observer_db = activity_connection()
        try:
            wait_until(
                lambda: len([x for x in pg_activity(observer_db) if "gate_fn" in (x["query"] or "")]) >= 2,
                timeout=20,
            )

            def sample():
                # Any further change is a violation; poll a few times while
                # everything is blocked.
                for _ in range(20):
                    peak["value"] = max(peak["value"], len(pg_activity(observer_db)))
                    time.sleep(0.01)

            sampler = threading.Thread(target=sample)
            sampler.start()
            sampler.join()
        finally:
            gate.release(key)
            consumer.join(30)
            observer_db.close()
            gate.close()
        result.close()
        # per side: thread_count workers + 1 anchor, for two sides.
        self.assertLessEqual(peak["value"], 2 * (thread_count + 1) + 2, peak)


if __name__ == "__main__":
    unittest.main()
