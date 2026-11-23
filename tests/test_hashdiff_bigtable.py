"""Big-table hash-diff regressions: 100k rows with long unicode text.

With ``bisection_factor=4`` and ``bisection_threshold=128``:

- equal tables must be verified inside the database - no whole-table download;
- the "collision records" (values that used to checksum identically) planted in
  a few separated segments must produce a complete diff, while downloading less
  than a tenth of the table;
- tables whose JSON is equal in content but written differently are fully
  drilled down - the download count of that worst case is recorded here.

The actual SQL, checksums and download counts are printed as REPORT lines and
mirrored in docs/comparison-semantics.md.
"""

import collections
import time
import unittest

from parameterized import parameterized_class

from data_diff import databases as db
from data_diff import tracking
from data_diff.hashdiff_tables import HashDiffer
from data_diff.queries.api import commit
from data_diff.table_segment import TableSegment
from tests.common import CONN_STRINGS
from tests.test_hashdiff_encoding import (
    DbPairFixture,
    diff_multiset,
    flipped,
)

tracking.disable_tracking()

N_ROWS = 100_000
BISECTION_FACTOR = 4
BISECTION_THRESHOLD = 128

_DB_PAIR_CLASSES = [
    (db.PostgreSQL, db.PostgreSQL),
    (db.DuckDB, db.DuckDB),
    (db.PostgreSQL, db.DuckDB),
]
_DB_PAIRS = [
    (f"{a.__name__}_{b.__name__}", a, b) for a, b in _DB_PAIR_CLASSES if a in CONN_STRINGS and b in CONN_STRINGS
]

REPORTS = collections.OrderedDict()


def report(pair_name, scenario, **fields):
    line = " | ".join(f"{k}={v}" for k, v in fields.items())
    REPORTS[f"{pair_name}/{scenario}"] = fields
    print(f"\nREPORT {pair_name} | {scenario} | {line}")


def make_differ(**kw):
    kw.setdefault("bisection_factor", BISECTION_FACTOR)
    kw.setdefault("bisection_threshold", BISECTION_THRESHOLD)
    kw.setdefault("threaded", False)
    return HashDiffer(**kw)


@parameterized_class(("name", "cls1", "cls2"), _DB_PAIRS)
class TestBigTables(unittest.TestCase):
    """100k rows of long unicode text; collision records in separated segments."""

    def setUp(self):
        self.fx = DbPairFixture(self, self.cls1, self.cls2)
        self.t1, self.t2 = self.fx.make_tables("(id INTEGER, x VARCHAR, y VARCHAR)")
        self._fill(self.fx.conn1, self.t1)
        self._fill(self.fx.conn2, self.t2)

    def tearDown(self):
        self.fx.close()

    @staticmethod
    def _fill(conn, table):
        "Deterministic 100k rows with long unicode text, generated in SQL."
        if isinstance(conn, db.PostgreSQL):
            conn.query(
                f"INSERT INTO {table} "
                f"SELECT i, 'row-' || i || '-' || repeat('é中🙂', 25), 'y' || i "
                f"FROM generate_series(1, {N_ROWS}) AS i"
            )
        else:
            conn.query(
                f"INSERT INTO {table} "
                f"SELECT i, 'row-' || i || '-' || repeat('é中🙂', 25), 'y' || i "
                f"FROM range(1, {N_ROWS + 1}) AS t(i)"
            )
        conn.query(commit)

    def _update(self, conn, table, id_, x, y):
        def lit(v):
            return "NULL" if v is None else "'" + v.replace("'", "''") + "'"

        conn.query(f"UPDATE {table} SET x = {lit(x)}, y = {lit(y)} WHERE id = {id_}")
        conn.query(commit)

    def _segments(self):
        a = TableSegment(self.fx.conn1, (self.t1,), ("id",), extra_columns=("x", "y"))
        b = TableSegment(self.fx.conn2, (self.t2,), ("id",), extra_columns=("x", "y"))
        return a, b

    def test_equal_tables_not_downloaded(self):
        a, b = self._segments()
        differ = make_differ()
        start = time.monotonic()
        diff_res = differ.diff_tables(a, b)
        diff = list(diff_res)
        duration = time.monotonic() - start
        self.assertEqual([], diff)
        downloaded = differ.stats.get("rows_downloaded", 0)
        self.assertEqual(0, downloaded, "equal tables must be verified inside the database")
        self.assertEqual({1: N_ROWS, 2: N_ROWS}, diff_res.info_tree.info.rowcounts)
        report(
            self.name,
            "equal-100k",
            rows=N_ROWS,
            diff_rows=len(diff),
            rows_downloaded=downloaded,
            duration_sec=round(duration, 2),
        )

    def test_collision_records_complete_with_little_download(self):
        # The pre-fix collision records, planted in well-separated segments:
        planted = [
            # (id, src(x, y), dst(x, y)) - each pair checksummed identically before the fix
            (25_000, ("a|b", "c"), ("a", "b|c")),
            (50_000, (None, "z"), ("<null>", "z")),
            (75_000, (None, "q"), ("", "q")),
            (90_000, ("é中🙂" * 500 + "A", "z"), ("é中🙂" * 500 + "B", "z")),
        ]
        for id_, (x1, y1), (x2, y2) in planted:
            self._update(self.fx.conn1, self.t1, id_, x1, y1)
            self._update(self.fx.conn2, self.t2, id_, x2, y2)

        a, b = self._segments()
        differ = make_differ()
        diff = diff_multiset(differ.diff_tables(a, b))

        # Expected multiset from the raw planted tuples (ids normalize to text)
        expected = collections.Counter()
        for id_, row1, row2 in planted:
            expected[("-", (str(id_),) + row1)] += 1
            expected[("+", (str(id_),) + row2)] += 1
        self.assertEqual(expected, diff)

        downloaded = differ.stats.get("rows_downloaded", 0)
        self.assertLess(
            downloaded,
            N_ROWS // 10,
            f"downloaded {downloaded} rows, must stay below a tenth of {N_ROWS}",
        )
        report(
            self.name,
            "collisions-100k",
            rows=N_ROWS,
            diff_rows=sum(expected.values()),
            rows_downloaded=downloaded,
            download_fraction=round(downloaded / N_ROWS, 5),
        )

        # Swapped sides: identical content, flipped signs
        differ2 = make_differ()
        self.assertEqual(flipped(expected), diff_multiset(differ2.diff_tables(b, a)))


@parameterized_class(("name", "cls1", "cls2"), _DB_PAIRS)
class TestJsonDrilldownCost(unittest.TestCase):
    """Content-equal JSON written differently: correct result, but every row drills down."""

    N_JSON = 20_000

    def setUp(self):
        self.fx = DbPairFixture(self, self.cls1, self.cls2)

    def tearDown(self):
        self.fx.close()

    def test_json_formatting_difference_drilldown(self):
        # Side 1 stores canonical JSON (jsonb on PG, spaced text on DuckDB);
        # side 2 stores the same content minified (json on PG, JSON on DuckDB).
        type1 = "JSONB" if self.fx.cls1 is db.PostgreSQL else "JSON"
        type2 = "JSON"
        t1, t2 = self.fx.make_tables(f"(id INTEGER, j {type1})", f"(id INTEGER, j {type2})")

        def generate(conn, table, json_type, spaced: bool):
            "One JSON object per row: {'k': i, 's': 'row-i-é中🙂é中🙂'}, generated in SQL."
            if isinstance(conn, db.PostgreSQL):
                series = f"generate_series(1, {self.N_JSON}) AS i"
            else:
                series = f"range(1, {self.N_JSON + 1}) AS t(i)"
            if spaced:
                doc = "'{\"k\": ' || i || ', \"s\": \"row-' || i || '-é中🙂é中🙂\"}'"
            else:
                doc = "'{\"k\":' || i || ',\"s\":\"row-' || i || '-é中🙂é中🙂\"}'"
            conn.query(f"INSERT INTO {table} SELECT i, ({doc})::{json_type} FROM {series}")
            conn.query(commit)

        generate(self.fx.conn1, t1, type1, spaced=True)
        generate(self.fx.conn2, t2, type2, spaced=False)

        a = TableSegment(self.fx.conn1, (t1,), ("id",), extra_columns=("j",))
        b = TableSegment(self.fx.conn2, (t2,), ("id",), extra_columns=("j",))
        differ = make_differ()
        diff = list(differ.diff_tables(a, b))
        self.assertEqual([], diff, "content-equal JSON must not be reported")
        downloaded = differ.stats.get("rows_downloaded", 0)
        self.assertEqual(
            self.N_JSON,
            downloaded,
            "formatting differences on every row force a full drill-down (recorded cost)",
        )
        report(
            self.name,
            "json-formatting-drilldown",
            rows=self.N_JSON,
            diff_rows=0,
            rows_downloaded=downloaded,
        )


if __name__ == "__main__":
    unittest.main()
