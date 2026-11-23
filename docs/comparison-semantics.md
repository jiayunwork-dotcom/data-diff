# Hash-diff comparison semantics

This document defines what "same value" means in the hash-diff algorithm
(`HashDiffer`), how the SQL checksum input is encoded, how JSON columns are
compared, what changed and why, and how to re-run the regressions that pin all
of this down.

The algorithm decides equality in **two layers**, and the two layers must agree:

1. **SQL checksum.** Each segment is checksummed inside the database. Equal
   checksums prune the segment without downloading it. A segment skipped here
   can never be "rescued" by the leaf comparison, so the checksum's notion of
   "same value" must imply the leaf's notion.
2. **Leaf comparison.** Segments below the bisection threshold are downloaded
   and compared value by value in Python.

The contract after this fix:

- **checksum-equal ⇒ leaf-equal.** No missed differences (up to the
  probabilistic hash boundary discussed below).
- **checksum-different ⇏ leaf-different.** Some contents serialize differently
  per database even when they are equal by the comparison rules (JSON, see
  below). Those cases only cost an extra segment download; they never change
  the result.

## The checksum row encoding

For every compared column, the normalized value (the same type-aware string
normalization as before: common precision rules for numbers and timestamps,
`::text` serialization for JSON, plain casts otherwise) is encoded as a
**self-delimiting token**:

```
SQL NULL          ->  'N'
any other value v ->  'V' || md5_lower_hex(v)   -- always 33 characters
```

(`md5_lower_hex` is the MD5 as a *lowercase* hex string. The dialects render
it canonically — `lower(md5(...))` on PostgreSQL/DuckDB/MySQL/...,
`TO_HEX(md5(...))` on BigQuery, `LOWER(CONVERT(...HashBytes...))` on MSSQL,
`LOWER(RAWTOHEX(standard_hash(...)))` on Oracle — because some databases
otherwise return uppercase hex or raw bytes, which would make the tokens of
two databases differ for identical contents.)

The row string is the concatenation of all column tokens, in the aligned
column order. The segment checksum is, as before,
`sum(trunc48(md5(row_string)) - CHECKSUM_OFFSET)` over the segment's rows.

Why this is unambiguous (and the old `value1|value2` with `coalesce(...,
'<null>')` was not):

- **Column boundaries do not depend on content.** A value token is `V` plus
  exactly 32 hex digits; the NULL token is the single character `N`. The token
  stream is uniquely decodable no matter what the values contain — `|`,
  quotes, backslashes, newlines, and non-ASCII text are all legal values. The
  old encoding made `('a|b', 'c')` and `('a', 'b|c')` checksum identically.
- **NULL cannot be impersonated.** `NULL`, `''`, `'<null>'`, `'N'` and `'V'`
  are five different inputs. The old encoding mapped both `NULL` and the
  string `'<null>'` to the same text.
- **Hashing each column separately bounds the intermediate string** at
  33 characters per column, no matter how long the value is (see the design
  comparison below).

Generated SQL (PostgreSQL 16, table `(id INTEGER, x VARCHAR, y VARCHAR)`):

```sql
SELECT count(*), sum(('x' || substring(md5((coalesce(CASE WHEN "id"::varchar IS NULL THEN 'N' ELSE ('V' || lower(md5("id"::varchar))) END::varchar, '<null>') || coalesce(CASE WHEN "x"::varchar IS NULL THEN 'N' ELSE ('V' || lower(md5("x"::varchar))) END::varchar, '<null>') || coalesce(CASE WHEN "y"::varchar IS NULL THEN 'N' ELSE ('V' || lower(md5("y"::varchar))) END::varchar, '<null>'))), 21))::bit(48)::bigint - 140737488355327) FROM "doc_t1" WHERE ("id" >= 1) AND ("id" < 52)
```

The same shape is generated for DuckDB (`concat(...)` instead of `||`,
`('0x' || SUBSTRING(md5(...), 21, 12))::BIGINT - ...`). The residual
`coalesce(..., '<null>')` wrappers come from the shared `Concat` node and are
dead code here — tokens are never NULL.

Before the fix, the same query read:

```sql
... md5((coalesce(coalesce("id"::varchar, '<null>')::varchar, '<null>') || '|' || coalesce(coalesce("x"::varchar, '<null>')::varchar, '<null>') || '|' || ...)) ...
```

### Recorded checksum values

Table `(id INTEGER, x VARCHAR, y VARCHAR)` with rows
`(1,'a|b',NULL), (2,'<null>','N'), (3,'','é中🙂'), (4,'V','v'), (5,'same','same')`:

| source             | count | checksum        |
|--------------------|-------|-----------------|
| PostgreSQL 16      | 5     | -117015566951935 |
| DuckDB 2.0.1       | 5     | -117015566951935 |
| Python reference   | 5     | -117015566951935 |

The Python reference (`tests/common.py::row_values_to_checksum`) implements
this encoding independently of the SQL, and the regression suite asserts all
three agree.

## Leaf comparison and JSON content

Downloaded rows are compared as tuples of normalized values: SQL `NULL` is
`None`, everything else is text. Text columns compare as **exact text**, even
when the text happens to parse as JSON.

Columns typed `JSON` **on both aligned sides** compare by **JSON content**:

- Object member order and insignificant whitespace are not differences.
- Array order and multiplicity are differences.
- Booleans are not numbers: `{"v": true}` differs from `{"v": 1}` (Python's
  `True == 1` must not leak into the comparison — this was one of the misses).
- SQL `NULL`, JSON `null`, and the JSON string `"null"` are three different
  values.
- Numbers compare by **exact decimal value**: `1`, `1.0` and `1e0` are equal;
  `9007199254740992` and `9007199254740993` (adjacent integers that doubles
  cannot both represent) are different; the same holds for decimals with
  fractional parts and for nested structures.
- The comparator parses with `decimal.Decimal` for both integers and floats
  and rejects non-finite constants; legal JSON has no duplicate object keys.

Which columns are JSON is decided **from the actual compared columns**: the
position inside `key_columns + [update_column] + extra_columns`, minus the
ignored ones, aligned positionally between the two sides (field names may
differ). A position counts as JSON only when *both* sides type it as JSON, so
a plain text column is never parsed as JSON, and a JSON column is never
skipped because a compound key or an update column shifted the positions.

A change in any ordinary column is reported even when another column's JSON
is content-equal.

### JSON cases that legitimately do extra work

The checksum compares serializations; the leaf compares content. Whenever the
two sides serialize equal content differently, the segment is downloaded and
then found equal. This happens with:

- text-preserving JSON types — PostgreSQL `json` (not `jsonb`) and DuckDB
  `JSON` — when the spelling differs (whitespace, key order, `1e0` vs `1`,
  `\uXXXX` escapes vs literal characters);
- mixed JSON types across the sides (`json` vs `jsonb`, `jsonb` vs DuckDB
  `JSON`), whose canonical serializations differ;
- number spellings that jsonb rewrites (`1e0` → `1`).

Recorded worst case (20,000 rows, every row's JSON spelled differently but
content-equal; `bisection_factor=4`, `bisection_threshold=128`): **0
differences reported, 20,000 rows downloaded** — a full drill-down. If you
diff JSON columns across databases, prefer the same JSON type on both sides
(or canonical `jsonb` on both PostgreSQL sides) to avoid this cost.

## Design options considered

Two unambiguous row encodings were evaluated:

**A. Per-column tagged hash tokens (chosen).** `NULL → 'N'`, else
`'V' || md5_hex(value)`; concatenate; hash the row.

- Cross-database comparability: needs only `md5`, `CASE` and concatenation —
  every supported dialect already has all three (md5 was already required).
  The per-column hash is rendered as canonical lowercase hex
  (`md5_as_lower_hex`), so uppercase-hex and raw-bytes dialects (ClickHouse,
  Oracle, MSSQL, BigQuery) stay comparable. No length semantics to align.
- Long text intermediates: bounded at 33 chars per column; a 1 MB value still
  produces a 33-char token.
- Database compute: `n_columns + 1` hash evaluations per row. Measured at
  100k rows × 3 columns: the whole equal-table check (schema, key range and
  the full-table checksum) completes in ~0.1–0.2 s on local PostgreSQL 16 and
  DuckDB 2.0.1.
- Leaf download: unchanged; only JSON serialization differences drill down.

**B. Length-prefixed concatenation (rejected).** `NULL → '-1:'`, else
`<length>:<value>`; concatenate; one hash per row.

- Cross-database comparability: needs a character-length function with
  identical semantics everywhere — `length()` counts characters in
  PostgreSQL/DuckDB but bytes in MySQL; per-dialect fixes and encoding edge
  cases make this fragile.
- Long text intermediates: unbounded — the concatenated row is as long as the
  data itself, which stresses varchar limits (the reason
  `PREVENT_OVERFLOW_WHEN_CONCAT` exists) and database memory.
- Database compute: one hash per row plus one `length()` per column, but the
  hash input is the full text anyway, and the concatenation moves much larger
  strings.
- Leaf download: same as A.

A third variant — combining per-column truncated hashes arithmetically
without a row-level hash — was rejected as well: it loses column-position
information and weakens the combination for no measurable win.

Option A keeps intermediates bounded, reuses the already-required md5
machinery, and introduces no new cross-database semantics. Its extra cost is
one md5 per column per row, which is small next to the normalization casts
the checksum already performed.

## Compatibility impact

- **The checksum is internal and per-run.** It is computed on each side
  within one diff execution and compared only inside that run. Changing the
  encoding changes no public API, no diff output (still `('-'/'+'`, tuple of
  normalized values)), and no statistics (`rows_downloaded`, `rowcounts`,
  `get_stats_*` are computed the same way).
- **Checksum values themselves changed.** Anything that pinned the old
  `md5(concat(col, '|', col))` values must be updated; the repository had one
  such test (`test_get_values`), now pinned to the documented encoding through
  an independent reference implementation. Cross-database diffs already
  required both sides to run the same data-diff version, so this is not a new
  constraint.
- **The probabilistic boundary is unchanged.** Each row still reduces to a
  48-bit truncated MD5 (`CHECKSUM_HEXDIGITS = 12`), summed per segment. A
  segment can compare equal despite different contents only through a hash
  collision (≈ 2⁻⁴⁸ per row pair, plus the slack of the sum). This is the
  pre-existing, accepted boundary of a finite hash — orthogonal to the
  deterministic ambiguities (column boundaries, NULL) that this fix removes.
  Switching hash names would not have fixed those; fixing the representation
  did.
- **Live and snapshot reads are untouched.** The view/transaction lifecycle
  keeps the baseline contract (`SNAPSHOT_CONSISTENCY.md`); the snapshot
  regression suite passes unchanged.

## Regression coverage and how to re-run

All regressions run locally against PostgreSQL 16 (port 5432, user `postgres`,
password `Password1`) and DuckDB 2.0.1 (`duckdb://main:@:memory:`), Python
3.11. Connection strings are configured in `tests/common.py` and can be
overridden via the git-ignored `tests/local_settings.py`.

```bash
# New regressions: encoding, JSON content, layouts, big tables
python3 -m unittest tests.test_hashdiff_encoding tests.test_hashdiff_bigtable -v

# Existing suites touched by the change
python3 -m unittest tests.test_diff_tables tests.test_database_types \
    tests.test_snapshot_consistency tests.test_joindiff tests.test_format \
    tests.test_query tests.test_duckdb
```

What is covered:

- `tests/test_hashdiff_encoding.py`
  - the two original missed diffs (`('a|b','c')` vs `('a','b|c')`;
    `(NULL,'z')` vs `('<null>','z')`), now reported as one `-` and one `+`;
  - NULL vs empty string, the token letters `N`/`V` as data, quotes,
    backslashes, newlines, non-ASCII and long (6,000-character) unicode values;
  - the typed JSON comparator against an independently written reference
    (booleans vs numbers, exact decimals, big adjacent integers, object
    reordering, nested arrays, `null` vs `"null"` vs SQL NULL);
  - JSON behind compound keys and update columns, renamed fields on both
    sides, ignoring the JSON or the text column separately (ignored columns
    still appear in the emitted rows);
  - empty tables (the established `ValueError` contract), `where` filters,
    repeated diffs (no leaked column/encoding state), stats and rowcounts;
  - every database case runs the normal HASHDIFF and a forced leaf
    comparison (`bisection_disabled=True`) and asserts the same full
    multiset, and asserts flipped signs when the sides are swapped;
  - expected values come from the raw tuples and the reference JSON
    comparator — never from the checksum under test;
  - all of the above on PostgreSQL↔PostgreSQL, DuckDB↔DuckDB,
    PostgreSQL→DuckDB and DuckDB→PostgreSQL.
- `tests/test_hashdiff_bigtable.py` — 100,000 rows with long unicode text,
  `bisection_factor=4`, `bisection_threshold=128` (see the recorded numbers
  below).
- Existing suites: `test_diff_tables` (segmentation, compound keys, NULL
  rows), `test_database_types` (type matrix incl. cross-database JSON),
  `test_snapshot_consistency` (consistent reads), `test_query` (query
  compilation), `test_joindiff`, `test_format`, `test_duckdb`.

### Recorded results (this environment)

```
tests.test_hashdiff_encoding ......... 163 tests, OK (7 pair-specific skips)
tests.test_hashdiff_bigtable ......... 9 tests, OK
tests.test_diff_tables ............... 27 tests OK (+2 MySQL-only, no server here)
tests.test_database_types ............ 86 tests, OK
tests.test_snapshot_consistency ...... 38 tests, OK
tests.test_joindiff (14) / test_format (4) / test_query (13) /
test_duckdb (2) / test_config (3) / test_parse_time (1) ...... OK
```

Big-table runs (100,000 rows, factor 4, threshold 128):

| scenario (per pair)                    | diff rows | rows downloaded | note |
|----------------------------------------|-----------|-----------------|------|
| equal tables (PG↔PG, D↔D, PG→D)        | 0         | 0               | verified inside the database, ~0.1–0.2 s |
| 4 collision records in separated segments | 8      | 388 (0.39 %)    | complete diff, < 10 % downloaded |
| JSON spelled differently, content equal (20k) | 0  | 20,000          | worst-case drill-down, recorded |

Before the fix, the reproduction (`('a|b','c')` vs `('a','b|c')`,
`(NULL,'z')` vs `('<null>','z')`, `{"v":true}` vs `{"v":1}`; 101 rows,
`bisection_factor=2`, `bisection_threshold=3`, `threaded=False`) reported **0
differences** for all three; after the fix each reports exactly one `-` and
one `+` row.
