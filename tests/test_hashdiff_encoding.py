"""Regression tests for the hash-diff row encoding and JSON content comparison.

The hash-diff algorithm decides equality in two layers, which must agree on
what "same value" means:

1. The SQL checksum decides whether a segment needs a closer look.  Its input
   is a concatenation of per-column tokens: 'N' for SQL NULL, or 'V' followed
   by the md5 hex of the normalized value.  Column boundaries therefore never
   depend on the content, and no string can impersonate a NULL.
2. Downloaded leaf segments are compared value by value in Python.  Text
   columns compare as exact text (even when the text happens to parse as
   JSON); JSON-typed columns compare by JSON content.

Expected values in this file are computed from the raw tuples and from an
independently written typed JSON comparator (``reference_json_equal``), never
from the checksum implementation under test.  See docs/comparison-semantics.md.
"""

import collections
import decimal
import json
import unittest

from parameterized import parameterized_class

from data_diff import databases as db
from data_diff import tracking
from data_diff.hashdiff_tables import HashDiffer, diff_sets
from data_diff.queries.api import commit
from data_diff.table_segment import TableSegment
from data_diff.utils import diffs_are_equiv_jsons, jsons_equal_by_content
from tests.common import (
    CONN_STRINGS,
    get_conn,
    random_table_suffix,
    row_values_to_checksum,
)

tracking.disable_tracking()


# ---------------------------------------------------------------------------
# Reference implementations (written independently of the code under test)
# ---------------------------------------------------------------------------


def _ref_canonical(v):
    "Canonical form for typed JSON comparison.  Reference implementation."
    if isinstance(v, bool):
        # Must be checked before numbers: in Python, bool is a subclass of int.
        return ("bool", v)
    if isinstance(v, decimal.Decimal):
        # Decimal compares by exact decimal value: 1 == 1.0 == 1e0, while big
        # integers that doubles cannot represent stay distinct.
        return ("num", v)
    if v is None:
        return ("null",)
    if isinstance(v, str):
        return ("str", v)
    if isinstance(v, list):
        # Array order and multiplicity are significant.
        return ("arr", tuple(_ref_canonical(x) for x in v))
    if isinstance(v, dict):
        # Object member order is insignificant.
        return ("obj", frozenset((k, _ref_canonical(x)) for k, x in v.items()))
    raise TypeError(f"Unexpected JSON value: {v!r}")


def reference_json_equal(text_a: str, text_b: str) -> bool:
    "True if both strings are valid JSON with equal content.  Reference implementation."
    try:
        a = json.loads(text_a, parse_float=decimal.Decimal, parse_int=decimal.Decimal)
        b = json.loads(text_b, parse_float=decimal.Decimal, parse_int=decimal.Decimal)
    except (ValueError, TypeError):
        return False
    return _ref_canonical(a) == _ref_canonical(b)


def assert_json_content_equal(test: unittest.TestCase, expected_doc, downloaded):
    "Assert that a downloaded JSON value matches the inserted document (or SQL NULL)."
    if expected_doc is None:
        test.assertIsNone(downloaded)
    else:
        test.assertIsNotNone(downloaded)
        test.assertTrue(
            reference_json_equal(expected_doc, downloaded), f"{expected_doc!r} vs downloaded {downloaded!r}"
        )


def diff_multiset(diff_iter) -> collections.Counter:
    "The full multiset of (sign, row) produced by a diff."
    return collections.Counter((sign, tuple(row)) for sign, row in diff_iter)


def flipped(multiset: collections.Counter) -> collections.Counter:
    "The same multiset with '-' and '+' exchanged (what swapping the sides must yield)."
    flip = {"-": "+", "+": "-"}
    return collections.Counter({(flip[sign], row): n for (sign, row), n in multiset.items()})


# ---------------------------------------------------------------------------
# Pure-Python tests: the typed JSON comparator
# ---------------------------------------------------------------------------

# (json_text_a, json_text_b, expected_equal_by_content)
JSON_CONTENT_CASES = [
    # Booleans are not numbers
    ('{"v": true}', '{"v": 1}', False),
    ('{"v": false}', '{"v": 0}', False),
    ('{"v": true}', '{"v": true}', True),
    ('true', "1", False),
    # Exact decimal numeric equivalence: 1 == 1.0 == 1e0
    ("1", "1.0", True),
    ("1", "1e0", True),
    ("1.0", "1e0", True),
    ("1.50", "1.5", True),
    ("1e2", "100", True),
    ("0.1", "0.10", True),
    ('{"n": 1}', '{"n": 1.0}', True),
    ('{"n": 1}', '{"n": 1e0}', True),
    ("-0.0", "0.0", True),
    # Adjacent big integers that doubles cannot represent stay distinct
    ("9007199254740992", "9007199254740993", False),
    ("9007199254740993.0", "9007199254740992.0", False),
    ("123456789012345678901234567890", "123456789012345678901234567891", False),
    # Object member order and insignificant whitespace do not matter
    ('{"a": 1, "b": 2}', '{"b": 2, "a": 1}', True),
    ('{ "a" : 1 }', '{"a":1}', True),
    ('{"x": {"p": 1, "q": [1, 2]}}', '{"x": {"q": [1, 2], "p": 1}}', True),
    # Array order and multiplicity do matter
    ("[1, 2]", "[2, 1]", False),
    ("[1, 1]", "[1]", False),
    ("[[1, 2], [3]]", "[[1, 2], [3]]", True),
    ("[[1, 2], [3]]", "[[1, 2], [4]]", False),
    # Nested structures follow the same rules
    ('{"x": [[1, 2], {"a": null}]}', '{"x": [[1, 2], {"a": null}]}', True),
    ('{"x": {"y": {"z": [true, 1]}}}', '{"x": {"y": {"z": [true, 1]}}}', True),
    ('{"x": {"y": {"z": [true, 1]}}}', '{"x": {"y": {"z": [1, true]}}}', False),
    # null vs the string "null" vs a missing member
    ("null", "null", True),
    ("null", '"null"', False),
    ('"null"', '"null"', True),
    ('{"a": null}', '{"a": "null"}', False),
    ('{"a": null}', "{}", False),
    # Numbers are not strings
    ('{"n": 1}', '{"n": "1"}', False),
    # Different JSON types
    ("[]", "{}", False),
    ("[]", "null", False),
    # Strings: escapes and unicode compare by content
    ('{"s": "é"}', '{"s": "é"}', True),
    ('{"s": "A"}', '{"s": "A"}', True),
    ('{"s": "a|b"}', '{"s": "a|b"}', True),
    ('{"s": "a|b"}', '{"s": "a"}', False),
    # Invalid JSON is never "equal by content" (the caller falls back to text)
    ("not json", "not json", False),
    ('{"a": 1}', "not json", False),
]


class TestTypedJsonComparator(unittest.TestCase):
    def test_reference_comparator_battery(self):
        "The library comparator must agree with the independent reference on every case."
        for text_a, text_b, expected in JSON_CONTENT_CASES:
            with self.subTest(a=text_a, b=text_b):
                self.assertEqual(expected, reference_json_equal(text_a, text_b), "reference implementation")
                self.assertEqual(expected, jsons_equal_by_content(text_a, text_b), "library implementation")

    def test_unicode_escape_case(self):
        "Escaped and literal unicode are the same JSON string."
        a = '{"s": "é"}'
        b = '{"s": "\\u00e9"}'
        self.assertTrue(reference_json_equal(a, b))
        self.assertTrue(jsons_equal_by_content(a, b))


class TestDiffsAreEquivJsons(unittest.TestCase):
    "diffs_are_equiv_jsons honours the column positions it is given."

    def test_json_position_matters(self):
        # JSON column at index 2 (e.g. after a compound key): equivalent JSON
        diff = [("-", ("1", "2", '{"a":1}')), ("+", ("1", "2", '{"a": 1}'))]
        match, overridden = diffs_are_equiv_jsons(diff, {2: "j"})
        self.assertTrue(match)
        self.assertEqual({"j"}, overridden)
        # ... but not when the JSON column is said to be at index 1
        match, _ = diffs_are_equiv_jsons(diff, {1: "j"})
        self.assertFalse(match)

    def test_bool_is_not_number(self):
        diff = [("-", ("1", '{"v": true}')), ("+", ("1", '{"v": 1}'))]
        match, _ = diffs_are_equiv_jsons(diff, {1: "j"})
        self.assertFalse(match)

    def test_sql_null_is_not_json_null(self):
        diff = [("-", ("1", None)), ("+", ("1", "null"))]
        match, _ = diffs_are_equiv_jsons(diff, {1: "j"})
        self.assertFalse(match)

    def test_text_column_not_treated_as_json(self):
        # No JSON columns declared: '{"a":1}' vs '{"a": 1}' differ as text.
        diff = [("-", ("1", '{"a":1}')), ("+", ("1", '{"a": 1}'))]
        match, _ = diffs_are_equiv_jsons(diff, {})
        self.assertFalse(match)


class TestDiffSetsPositions(unittest.TestCase):
    "diff_sets applies JSON semantics at the positions of the actual compared columns."

    @staticmethod
    def _run(a, b, **kw):
        kw.setdefault("columns1", ["k1", "k2", "u", "j", "t"])
        kw.setdefault("columns2", ["k1", "k2", "u", "j", "t"])
        kw.setdefault("key_columns1", ["k1", "k2"])
        kw.setdefault("key_columns2", ["k1", "k2"])
        kw.setdefault("ignored_columns1", frozenset())
        kw.setdefault("ignored_columns2", frozenset())
        kw.setdefault("json_cols", {3: "j"})  # position in the compared (cut) layout
        return list(diff_sets(a, b, **kw))

    def test_json_after_compound_key_and_update_column(self):
        a = [("1", "1", "7", '{"a":1,"b":2}', "x")]
        b = [("1", "1", "7", '{"b":2,"a":1}', "x")]
        self.assertEqual([], self._run(a, b))

    def test_text_change_not_cancelled_by_json_equivalence(self):
        a = [("1", "1", "7", '{"a":1,"b":2}', "x")]
        b = [("1", "1", "7", '{"b":2,"a":1}', "y")]
        self.assertEqual([("-", a[0]), ("+", b[0])], self._run(a, b))

    def test_ignored_columns_still_in_output_but_not_compared(self):
        a = [("1", "1", "7", '{"a":1}', "x")]
        b = [("1", "1", "7", '{"a":1}', "y")]
        # 't' ignored: the rows compare equal, and nothing is emitted
        self.assertEqual([], self._run(a, b, ignored_columns1={"t"}, ignored_columns2={"t"}))
        # 'j' changed and 't' ignored: the diff is emitted with the full rows
        b2 = [("1", "1", "7", '{"a":2}', "y")]
        self.assertEqual(
            [("-", a[0]), ("+", b2[0])],
            self._run(a, b2, ignored_columns1={"t"}, ignored_columns2={"t"}),
        )

    def test_renamed_columns_align_by_position(self):
        a = [("1", "1", "7", '{"a":1,"b":2}', "x")]
        b = [("1", "1", "7", '{"b":2,"a":1}', "x")]
        self.assertEqual(
            [],
            self._run(a, b, columns2=["id1", "id2", "u2", "j2", "t2"], key_columns2=["id1", "id2"]),
        )

    def test_expected_diffs_from_raw_tuples(self):
        rows_a = [("1", "1", "7", '{"a":1}', "x"), ("2", "2", "7", '{"a":3}', "p")]
        rows_b = [("1", "1", "7", '{"a":2}', "x"), ("3", "3", "7", '{"a":4}', "q")]
        expected = [("-", rows_a[0]), ("+", rows_b[0]), ("-", rows_a[1]), ("+", rows_b[1])]
        self.assertEqual(expected, self._run(rows_a, rows_b))


# ---------------------------------------------------------------------------
# Database fixtures
# ---------------------------------------------------------------------------

PG_JSON_TYPE = "JSONB"
DUCK_JSON_TYPE = "JSON"

_DB_PAIR_CLASSES = [
    (db.PostgreSQL, db.PostgreSQL),
    (db.DuckDB, db.DuckDB),
    (db.PostgreSQL, db.DuckDB),
    (db.DuckDB, db.PostgreSQL),
]
_DB_PAIRS = [
    (f"{a.__name__}_{b.__name__}", a, b) for a, b in _DB_PAIR_CLASSES if a in CONN_STRINGS and b in CONN_STRINGS
]


def sql_literal(v) -> str:
    "Render a Python value as a SQL literal (standard-conforming strings)."
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def insert_rows_sql(name: str, rows) -> str:
    values = ", ".join("(" + ", ".join(sql_literal(v) for v in row) + ")" for row in rows)
    return f"INSERT INTO {name} VALUES {values}"


class DbPairFixture:
    "Owns the connections and table lifecycle for one ordered pair of databases."

    def __init__(self, test: unittest.TestCase, cls1, cls2):
        self.test = test
        self.cls1 = cls1
        self.cls2 = cls2
        self.same_db = cls1 is cls2
        self.conn1 = get_conn(cls1)
        self.conn2 = self.conn1 if self.same_db else get_conn(cls2)
        self.json_type1 = PG_JSON_TYPE if cls1 is db.PostgreSQL else DUCK_JSON_TYPE
        self.json_type2 = PG_JSON_TYPE if cls2 is db.PostgreSQL else DUCK_JSON_TYPE
        self.suffix = random_table_suffix()
        self.tables = []

    def make_tables(self, columns_ddl1: str, columns_ddl2: "str | None" = None, name1="src", name2="dst"):
        "Create both tables; pass a second DDL when the sides need different types."
        t1 = f"{name1}{self.suffix}"
        t2 = f"{name2}{self.suffix}"
        self.conn1.query(f"DROP TABLE IF EXISTS {t1}")
        self.conn2.query(f"DROP TABLE IF EXISTS {t2}")
        self.conn1.query(f"CREATE TABLE {t1} {columns_ddl1}")
        self.conn2.query(f"CREATE TABLE {t2} {columns_ddl2 or columns_ddl1}")
        self.conn1.query(commit)
        if not self.same_db:
            self.conn2.query(commit)
        self.tables += [(self.conn1, t1), (self.conn2, t2)]
        return t1, t2

    def insert(self, conn, name, rows):
        if not rows:
            return
        conn.query(insert_rows_sql(name, rows))
        conn.query(commit)

    def segment(self, conn, name, **kw):
        return TableSegment(conn, (name,), **kw)

    def close(self):
        for conn, t in self.tables:
            conn.query(f"DROP TABLE IF EXISTS {t}")
            conn.query(commit)
        self.conn1.close()
        if not self.same_db:
            self.conn2.close()


@parameterized_class(("name", "cls1", "cls2"), _DB_PAIRS)
class HashdiffDbTestCase(unittest.TestCase):
    "Base class: sets up a fresh fixture per test."

    def setUp(self):
        self.fx = DbPairFixture(self, self.cls1, self.cls2)
        self.differ = HashDiffer(bisection_factor=2, bisection_threshold=3, threaded=False)

    def tearDown(self):
        self.fx.close()

    def run_diff(self, a, b, differ=None) -> collections.Counter:
        return diff_multiset((differ or self.differ).diff_tables(a, b))


# ---------------------------------------------------------------------------
# Battery A: the checksum row encoding (text columns)
# ---------------------------------------------------------------------------

FILLER_ROWS = [(i, "same", "same") for i in range(2, 102)]


@parameterized_class(("name", "cls1", "cls2"), _DB_PAIRS)
class TestChecksumEncoding(HashdiffDbTestCase):
    """The two original missed diffs, plus NULL/empty/special-character cases.

    Every case runs the normal HASHDIFF and a forced leaf comparison, and
    asserts the same full multiset from both, with signs flipped when the
    sides are swapped.  Expected rows are the raw inserted tuples (with the
    key normalized to text, as the databases return it).
    """

    def _case(self, row1, row2):
        "row1/row2: the id=1 rows on each side; ids 2..101 are identical filler."
        t1, t2 = self.fx.make_tables("(id INTEGER, x VARCHAR, y VARCHAR)")
        self.fx.insert(self.fx.conn1, t1, FILLER_ROWS + [row1])
        self.fx.insert(self.fx.conn2, t2, FILLER_ROWS + [row2])
        a = self.fx.segment(self.fx.conn1, t1, key_columns=("id",), extra_columns=("x", "y"))
        b = self.fx.segment(self.fx.conn2, t2, key_columns=("id",), extra_columns=("x", "y"))
        return a, b

    def _expected(self, row1, row2):
        "Expected diff multiset, from the raw tuples (the id key normalizes to text)."
        expected = []
        if row1 != row2:
            expected.append(("-", (str(row1[0]),) + tuple(row1[1:])))
            expected.append(("+", (str(row2[0]),) + tuple(row2[1:])))
        return collections.Counter(expected)

    def _check_case(self, row1, row2):
        a, b = self._case(row1, row2)
        expected = self._expected(row1, row2)

        # Normal HASHDIFF
        self.assertEqual(expected, self.run_diff(a, b))
        # Forced leaf comparison must produce the same multiset
        forced = HashDiffer(bisection_factor=2, bisection_threshold=3, threaded=False, bisection_disabled=True)
        self.assertEqual(expected, self.run_diff(a, b, forced))
        # Swapped sides: signs flipped, same content
        self.assertEqual(flipped(expected), self.run_diff(b, a))

    def test_pipe_shifts_column_boundary(self):
        "Original miss #1: ('a|b','c') vs ('a','b|c') used to checksum identically."
        self._check_case((1, "a|b", "c"), (1, "a", "b|c"))

    def test_pipe_boundary_mirror(self):
        self._check_case((1, "x|", "y"), (1, "x", "|y"))

    def test_null_vs_null_literal_string(self):
        "Original miss #2: (NULL,'z') vs ('<null>','z') used to checksum identically."
        self._check_case((1, None, "z"), (1, "<null>", "z"))

    def test_null_vs_empty_string(self):
        self._check_case((1, None, "w"), (1, "", "w"))

    def test_empty_string_vs_text(self):
        self._check_case((1, "", "w"), (1, " ", "w"))

    def test_token_letters_are_plain_text(self):
        # 'N' and 'V' are the token markers; as values they must behave like any text
        self._check_case((1, "N", "V"), (1, "N", "V"))
        self._check_case((1, "V", "N"), (1, "N", "V"))
        self._check_case((1, "N", "N"), (1, None, "N"))

    def test_quotes_backslash_newline_unicode(self):
        tricky = "a'b\\c\nd\"e\te|fé中🙂"
        self._check_case((1, tricky, "tail"), (1, tricky, "tail"))
        self._check_case((1, tricky, "tail"), (1, tricky, "tail!"))

    def test_long_unicode_text(self):
        long_text = "é中🙂" * 1500 + "tail"
        self._check_case((1, long_text, "z"), (1, long_text, "z"))
        self._check_case((1, long_text, "z"), (1, long_text + "!", "z"))

    def test_json_text_in_plain_text_column(self):
        "A text column that happens to contain JSON compares as plain text."
        self._check_case((1, '{"a":1}', "t"), (1, '{"a": 1}', "t"))

    def test_checksum_sql_matches_documented_encoding(self):
        "The SQL checksum must match the reference encoding, NULLs and pipes included."
        rows = [(1, "a|b", None), (2, "<null>", "N"), (3, "", "é中🙂"), (4, "V", "v")]
        t1, _ = self.fx.make_tables("(id INTEGER, x VARCHAR, y VARCHAR)")
        self.fx.insert(self.fx.conn1, t1, rows)
        seg = self.fx.segment(self.fx.conn1, t1, key_columns=("id",), extra_columns=("x", "y")).with_schema()
        count, checksum = seg.count_and_checksum()
        self.assertEqual(len(rows), count)
        normalized = [("1", "a|b", None), ("2", "<null>", "N"), ("3", "", "é中🙂"), ("4", "V", "v")]
        expected = sum(row_values_to_checksum(row) for row in normalized)
        self.assertEqual(expected, checksum)


# ---------------------------------------------------------------------------
# Battery B: JSON content comparison
# ---------------------------------------------------------------------------

JSON_FILLER = [(i, '{"w": 1}', "same") for i in range(2, 102)]


@parameterized_class(("name", "cls1", "cls2"), _DB_PAIRS)
class TestJsonContent(HashdiffDbTestCase):
    """JSON columns compare by JSON content, end to end through the databases."""

    def _case(self, doc1, doc2):
        jtype1, jtype2 = self.fx.json_type1, self.fx.json_type2
        t1, t2 = self.fx.make_tables(
            f"(id INTEGER, j {jtype1}, t VARCHAR)", f"(id INTEGER, j {jtype2}, t VARCHAR)"
        )
        self.fx.insert(self.fx.conn1, t1, JSON_FILLER + [(1, doc1, "same")])
        self.fx.insert(self.fx.conn2, t2, JSON_FILLER + [(1, doc2, "same")])
        a = self.fx.segment(self.fx.conn1, t1, key_columns=("id",), extra_columns=("j", "t"))
        b = self.fx.segment(self.fx.conn2, t2, key_columns=("id",), extra_columns=("j", "t"))
        return a, b

    def _check_json_case(self, doc1, doc2, expect_diff: bool):
        a, b = self._case(doc1, doc2)
        diff = self.run_diff(a, b)
        if not expect_diff:
            self.assertEqual(collections.Counter(), diff)
            return diff

        # Expected rows from the raw tuples: the JSON element must be
        # content-equal to the inserted document, everything else exact.
        self.assertEqual(2, len(diff))
        by_sign = {}
        for sign, row in diff:
            by_sign[sign] = row
        self.assertEqual({"-", "+"}, set(by_sign))
        minus_doc, plus_doc = by_sign["-"][1], by_sign["+"][1]
        self.assertEqual("1", by_sign["-"][0])
        self.assertEqual("1", by_sign["+"][0])
        self.assertEqual("same", by_sign["-"][2])
        self.assertEqual("same", by_sign["+"][2])
        assert_json_content_equal(self, doc1, minus_doc)
        assert_json_content_equal(self, doc2, plus_doc)

        # Forced leaf comparison and side-swapping agree
        forced = HashDiffer(bisection_factor=2, bisection_threshold=3, threaded=False, bisection_disabled=True)
        self.assertEqual(diff, self.run_diff(a, b, forced))
        self.assertEqual(flipped(diff), self.run_diff(b, a))
        return diff

    # -- booleans vs numbers -------------------------------------------------
    def test_true_is_not_one(self):
        "Original leaf miss: {'v': true} and {'v': 1} were treated as equal."
        self._check_json_case('{"v": true}', '{"v": 1}', expect_diff=True)

    def test_false_is_not_zero(self):
        self._check_json_case('{"v": false}', '{"v": 0}', expect_diff=True)

    def test_true_equals_true(self):
        self._check_json_case('{"v": true}', '{"v": true}', expect_diff=False)

    # -- numeric forms --------------------------------------------------------
    def test_numeric_equivalence(self):
        self._check_json_case('{"n": 1}', '{"n": 1.0}', expect_diff=False)

    def test_numeric_exponent_form(self):
        self._check_json_case('{"n": 1e0}', '{"n": 1}', expect_diff=False)

    def test_big_adjacent_integers(self):
        self._check_json_case('{"n": 9007199254740992}', '{"n": 9007199254740993}', expect_diff=True)

    def test_big_adjacent_decimals(self):
        self._check_json_case('{"n": 9007199254740993.0}', '{"n": 9007199254740992.0}', expect_diff=True)

    # -- object/array structure ------------------------------------------------
    def test_object_member_reorder(self):
        self._check_json_case('{"a": 1, "b": 2}', '{"b": 2, "a": 1}', expect_diff=False)

    def test_insignificant_whitespace(self):
        self._check_json_case('{"a":1}', '{  "a" : 1 }', expect_diff=False)

    def test_array_order_matters(self):
        self._check_json_case("[1, 2]", "[2, 1]", expect_diff=True)

    def test_array_multiplicity_matters(self):
        self._check_json_case("[1, 1]", "[1]", expect_diff=True)

    def test_nested_object_and_array(self):
        self._check_json_case('{"x": {"p": 1, "q": [1, {"k": null}]}}', '{"x": {"q": [1, {"k": null}], "p": 1}}', expect_diff=False)
        self._check_json_case('{"x": [1, {"k": true}]}', '{"x": [1, {"k": 1}]}', expect_diff=True)

    # -- nulls ------------------------------------------------------------------
    def test_sql_null_vs_json_null(self):
        self._check_json_case(None, "null", expect_diff=True)

    def test_json_string_null_vs_json_null(self):
        self._check_json_case('"null"', "null", expect_diff=True)

    def test_json_null_equals_json_null(self):
        self._check_json_case("null", "null", expect_diff=False)

    # -- strings ------------------------------------------------------------------
    def test_number_vs_string(self):
        self._check_json_case('{"n": 1}', '{"n": "1"}', expect_diff=True)

    def test_unicode_string_content(self):
        self._check_json_case('{"s": "é中"}', '{"s": "é中"}', expect_diff=False)

    def test_text_preserving_json_type_drills_down(self):
        """A text-preserving JSON type (PG json, not jsonb) serializes differently
        for equal content: the checksum differs, the segment is downloaded, and
        the leaf comparison then finds the content equal (allowed extra work)."""
        if self.fx.cls1 is not db.PostgreSQL or self.fx.cls2 is not db.PostgreSQL:
            self.skipTest("specific to PostgreSQL's text-preserving json type")
        t1, t2 = self.fx.make_tables("(id INTEGER, j JSON, t VARCHAR)", "(id INTEGER, j JSONB, t VARCHAR)")
        self.fx.insert(self.fx.conn1, t1, JSON_FILLER + [(1, '{"a":1,"b":2}', "same")])
        self.fx.insert(self.fx.conn2, t2, JSON_FILLER + [(1, '{"a":1,"b":2}', "same")])
        a = self.fx.segment(self.fx.conn1, t1, key_columns=("id",), extra_columns=("j", "t"))
        b = self.fx.segment(self.fx.conn2, t2, key_columns=("id",), extra_columns=("j", "t"))
        differ = HashDiffer(bisection_factor=2, bisection_threshold=3, threaded=False)
        self.assertEqual(collections.Counter(), self.run_diff(a, b, differ))
        # json keeps the source text ('{"a":1,"b":2}'), jsonb canonicalizes
        # ('{"a": 1, "b": 2}'): checksums differ, so the leaf segment is downloaded
        self.assertGreater(differ.stats.get("rows_downloaded", 0), 0)


# ---------------------------------------------------------------------------
# Battery C: column layout, ignoring, empty tables, filters, repeated runs
# ---------------------------------------------------------------------------


@parameterized_class(("name", "cls1", "cls2"), _DB_PAIRS)
class TestColumnLayoutAndScenarios(HashdiffDbTestCase):
    def test_json_after_compound_key_and_update_column(self):
        "JSON sits behind two keys and an update column; positions must follow the compared columns."
        t1, t2 = self.fx.make_tables(
            f"(k1 INTEGER, k2 INTEGER, u INTEGER, j {self.fx.json_type1}, t VARCHAR)",
            f"(k1 INTEGER, k2 INTEGER, u INTEGER, j {self.fx.json_type2}, t VARCHAR)",
        )
        rows1 = [(1, 1, 7, '{"a":1,"b":2}', "x"), (2, 2, 7, '{"a":1}', "p"), (3, 3, 7, '{"a":1,"b":2}', "x3")]
        rows2 = [(1, 1, 7, '{"b":2,"a":1}', "x"), (2, 2, 7, '{"a":2}', "p"), (3, 3, 7, '{"b":2,"a":1}', "y3")]
        self.fx.insert(self.fx.conn1, t1, rows1)
        self.fx.insert(self.fx.conn2, t2, rows2)
        seg_kw = {"key_columns": ("k1", "k2"), "update_column": "u", "extra_columns": ("j", "t")}
        a = self.fx.segment(self.fx.conn1, t1, **seg_kw)
        b = self.fx.segment(self.fx.conn2, t2, **seg_kw)
        diff = self.run_diff(a, b)
        # Row (1,1) is JSON-equivalent -> not reported.
        # Row (2,2) differs in JSON content -> reported.
        # Row (3,3): JSON is equivalent but the ordinary text column changed ->
        # the JSON equivalence must NOT cancel the row difference.
        self.assertEqual(4, len(diff))
        by_key = collections.defaultdict(dict)
        for sign, row in diff:
            by_key[row[:2]][sign] = row
        self.assertEqual({("2", "2"), ("3", "3")}, set(by_key))
        assert_json_content_equal(self, rows1[1][3], by_key[("2", "2")]["-"][3])
        assert_json_content_equal(self, rows2[1][3], by_key[("2", "2")]["+"][3])
        self.assertEqual("x3", by_key[("3", "3")]["-"][4])
        self.assertEqual("y3", by_key[("3", "3")]["+"][4])

    def test_json_position_with_renamed_columns(self):
        "Different field names on both sides, aligned by position."
        t1, t2 = self.fx.make_tables(
            f"(id INTEGER, j {self.fx.json_type1}, t VARCHAR)",
            f"(id2 INTEGER, j2 {self.fx.json_type2}, t2 VARCHAR)",
        )
        self.fx.insert(self.fx.conn1, t1, [(1, '{"a":1,"b":2}', "same"), (2, '{"k":9}', "same")])
        self.fx.insert(self.fx.conn2, t2, [(1, '{"b":2,"a":1}', "same"), (2, '{"k":9}', "same")])
        a = self.fx.segment(self.fx.conn1, t1, key_columns=("id",), extra_columns=("j", "t"))
        b = self.fx.segment(self.fx.conn2, t2, key_columns=("id2",), extra_columns=("j2", "t2"))
        self.assertEqual(collections.Counter(), self.run_diff(a, b))

    def test_ignore_columns(self):
        t1, t2 = self.fx.make_tables(
            f"(id INTEGER, j {self.fx.json_type1}, t VARCHAR)",
            f"(id INTEGER, j {self.fx.json_type2}, t VARCHAR)",
        )
        self.fx.insert(self.fx.conn1, t1, [(1, '{"a":1}', "x")])
        self.fx.insert(self.fx.conn2, t2, [(1, '{"a":2}', "y")])
        a = self.fx.segment(self.fx.conn1, t1, key_columns=("id",), extra_columns=("j", "t"))
        b = self.fx.segment(self.fx.conn2, t2, key_columns=("id",), extra_columns=("j", "t"))

        # Ignore the JSON column: only the text difference is reported,
        # and the ignored JSON values still appear in the emitted rows.
        differ = HashDiffer(bisection_factor=2, bisection_threshold=3, threaded=False)
        differ.ignore_column("j", "j")
        diff = diff_multiset(differ.diff_tables(a, b))
        self.assertEqual(2, len(diff))
        for sign, row in diff:
            self.assertEqual("1", row[0])
            self.assertIn(row[1], ('{"a":1}', '{"a": 1}', '{"a":2}', '{"a": 2}'))
        self.assertEqual({"x", "y"}, {row[2] for _, row in diff})

        # Ignore the text column: only the JSON difference is reported
        differ2 = HashDiffer(bisection_factor=2, bisection_threshold=3, threaded=False)
        differ2.ignore_column("t", "t")
        diff2 = diff_multiset(differ2.diff_tables(a, b))
        self.assertEqual(2, len(diff2))

        # Ignore both: no difference
        differ3 = HashDiffer(bisection_factor=2, bisection_threshold=3, threaded=False)
        differ3.ignore_column("j", "j")
        differ3.ignore_column("t", "t")
        self.assertEqual(collections.Counter(), diff_multiset(differ3.diff_tables(a, b)))

    def test_empty_tables_raise_valueerror(self):
        "The established contract: an empty table has no key range."
        t1, t2 = self.fx.make_tables("(id INTEGER, x VARCHAR)")
        a = self.fx.segment(self.fx.conn1, t1, key_columns=("id",), extra_columns=("x",))
        b = self.fx.segment(self.fx.conn2, t2, key_columns=("id",), extra_columns=("x",))
        self.assertRaises(ValueError, list, self.differ.diff_tables(a, b))

    def test_where_filter_restricts_diff(self):
        t1, t2 = self.fx.make_tables("(id INTEGER, x VARCHAR)")
        rows1 = [(i, f"l{i}") for i in range(1, 21)]
        rows2 = [(i, f"l{i}") for i in range(1, 21)]
        rows1[4] = (5, "left")  # id=5 differs
        rows1[14] = (15, "left")  # id=15 differs
        self.fx.insert(self.fx.conn1, t1, rows1)
        self.fx.insert(self.fx.conn2, t2, rows2)
        a = self.fx.segment(self.fx.conn1, t1, key_columns=("id",), extra_columns=("x",), where="id < 10")
        b = self.fx.segment(self.fx.conn2, t2, key_columns=("id",), extra_columns=("x",), where="id < 10")
        self.assertEqual(
            collections.Counter([("-", ("5", "left")), ("+", ("5", "l5"))]),
            self.run_diff(a, b),
        )

    def test_repeated_diffs_are_identical(self):
        "No column-position or encoding state leaks between consecutive diffs."
        t1, t2 = self.fx.make_tables(
            f"(id INTEGER, j {self.fx.json_type1}, x VARCHAR)",
            f"(id INTEGER, j {self.fx.json_type2}, x VARCHAR)",
        )
        self.fx.insert(self.fx.conn1, t1, [(1, '{"a":1,"b":2}', "p"), (2, '{"k":1}', "q")])
        self.fx.insert(self.fx.conn2, t2, [(1, '{"b":2,"a":1}', "p"), (2, '{"k":2}', "q")])
        a = self.fx.segment(self.fx.conn1, t1, key_columns=("id",), extra_columns=("j", "x"))
        b = self.fx.segment(self.fx.conn2, t2, key_columns=("id",), extra_columns=("j", "x"))
        first = self.run_diff(a, b)
        for _ in range(3):
            self.assertEqual(first, self.run_diff(a, b))
        # ... also with a fresh differ and fresh segments
        a2 = self.fx.segment(self.fx.conn1, t1, key_columns=("id",), extra_columns=("j", "x"))
        b2 = self.fx.segment(self.fx.conn2, t2, key_columns=("id",), extra_columns=("j", "x"))
        self.assertEqual(first, self.run_diff(a2, b2, HashDiffer(bisection_factor=2, bisection_threshold=3, threaded=False)))
        self.assertEqual(2, len(first))  # only id=2 differs

    def test_one_differ_two_layouts_no_state_leak(self):
        "One HashDiffer diffing two table pairs with JSON at different positions."
        t1, t2 = self.fx.make_tables(
            f"(id INTEGER, j {self.fx.json_type1})",
            f"(id INTEGER, j {self.fx.json_type2})",
            name1="lay1_src",
            name2="lay1_dst",
        )
        t3, t4 = self.fx.make_tables(
            f"(k1 INTEGER, k2 INTEGER, u INTEGER, j {self.fx.json_type1})",
            f"(k1 INTEGER, k2 INTEGER, u INTEGER, j {self.fx.json_type2})",
            name1="lay2_src",
            name2="lay2_dst",
        )
        self.fx.insert(self.fx.conn1, t1, [(1, '{"a":1,"b":2}')])
        self.fx.insert(self.fx.conn2, t2, [(1, '{"b":2,"a":1}')])
        self.fx.insert(self.fx.conn1, t3, [(1, 1, 7, '{"v":true}')])
        self.fx.insert(self.fx.conn2, t4, [(1, 1, 7, '{"v":1}')])

        differ = HashDiffer(bisection_factor=2, bisection_threshold=3, threaded=False)
        # First diff: JSON at position 1; reordered content -> no diff
        a = self.fx.segment(self.fx.conn1, t1, key_columns=("id",), extra_columns=("j",))
        b = self.fx.segment(self.fx.conn2, t2, key_columns=("id",), extra_columns=("j",))
        self.assertEqual(collections.Counter(), self.run_diff(a, b, differ))
        # Second diff with the SAME differ: JSON at position 3; true vs 1 -> diff
        c = self.fx.segment(self.fx.conn1, t3, key_columns=("k1", "k2"), update_column="u", extra_columns=("j",))
        d = self.fx.segment(self.fx.conn2, t4, key_columns=("k1", "k2"), update_column="u", extra_columns=("j",))
        self.assertEqual(2, len(self.run_diff(c, d, differ)))
        # ... and the first pair still diffs correctly afterwards
        self.assertEqual(collections.Counter(), self.run_diff(a, b, differ))

    def test_json_column_vs_text_column_compares_as_text(self):
        "JSON-typed on one side, plain text on the other: plain text comparison."
        if self.fx.same_db:
            self.skipTest("needs two different column types, one per side")
        t1, t2 = self.fx.make_tables(
            f"(id INTEGER, j {self.fx.json_type1})",
            "(id INTEGER, j VARCHAR)",
        )
        # The JSON side serializes with a space, the text side stores it minified:
        # content-equal JSON, but the text column must compare as text -> diff.
        doc_minified = '{"a":1}'
        self.fx.insert(self.fx.conn1, t1, [(1, doc_minified)])
        self.fx.insert(self.fx.conn2, t2, [(1, doc_minified)])
        a = self.fx.segment(self.fx.conn1, t1, key_columns=("id",), extra_columns=("j",))
        b = self.fx.segment(self.fx.conn2, t2, key_columns=("id",), extra_columns=("j",))
        diff = self.run_diff(a, b)
        json_side_serializes_with_space = self.fx.cls1 is db.PostgreSQL
        if json_side_serializes_with_space:
            # jsonb renders '{"a": 1}' while the text stays '{"a":1}' -> reported
            self.assertEqual(2, len(diff))
        else:
            # DuckDB JSON preserves the minified text -> identical text -> no diff
            self.assertEqual(collections.Counter(), diff)

    def test_stats_and_rowcounts(self):
        t1, t2 = self.fx.make_tables("(id INTEGER, x VARCHAR)")
        self.fx.insert(self.fx.conn1, t1, [(i, "same") for i in range(1, 51)])
        self.fx.insert(self.fx.conn2, t2, [(i, "same") for i in range(1, 51)])
        a = self.fx.segment(self.fx.conn1, t1, key_columns=("id",), extra_columns=("x",))
        b = self.fx.segment(self.fx.conn2, t2, key_columns=("id",), extra_columns=("x",))
        differ = HashDiffer(bisection_factor=2, bisection_threshold=3, threaded=False)
        diff_res = differ.diff_tables(a, b)
        self.assertEqual([], list(diff_res))
        self.assertEqual({1: 50, 2: 50}, diff_res.info_tree.info.rowcounts)
        self.assertEqual(0, differ.stats.get("rows_downloaded", 0))

    def test_cross_db_checksum_equality(self):
        "Identical contents (NULLs, pipes, unicode) must checksum identically on both databases."
        if self.fx.same_db:
            self.skipTest("cross-database check")
        rows = [(1, "a|b", None), (2, "<null>", "N"), (3, "", "é中🙂"), (4, "V", "v"), (5, "same", "same")]
        t1, t2 = self.fx.make_tables("(id INTEGER, x VARCHAR, y VARCHAR)")
        self.fx.insert(self.fx.conn1, t1, rows)
        self.fx.insert(self.fx.conn2, t2, rows)
        seg1 = self.fx.segment(self.fx.conn1, t1, key_columns=("id",), extra_columns=("x", "y")).with_schema()
        seg2 = self.fx.segment(self.fx.conn2, t2, key_columns=("id",), extra_columns=("x", "y")).with_schema()
        count1, checksum1 = seg1.count_and_checksum()
        count2, checksum2 = seg2.count_and_checksum()
        self.assertEqual((count1, checksum1), (count2, checksum2))
        self.assertEqual(5, count1)
        self.assertIsNotNone(checksum1)


if __name__ == "__main__":
    unittest.main()
