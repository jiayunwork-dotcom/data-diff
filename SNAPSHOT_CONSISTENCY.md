# `consistency="snapshot"` — implementation & verification notes

## What was added

**Public API** (`data_diff.diff_tables` and the top-level `data_diff.diff_tables`):

```python
HashDiffer.diff_tables(t1, t2, consistency="snapshot")   # direct use of a differ
diff_tables(t1, t2, consistency="snapshot")               # top-level helper
```

- `consistency` accepts `"live"` (default, unchanged behaviour) or `"snapshot"`.
  It is validated eagerly; an illegal value raises `DataDiffConsistencyError`.
- The return value is still the existing `DiffResultWrapper` (now with
  `consistency`, `close()`, `with` support, and cache/lifecycle semantics).
- Rejection happens before any business table is touched:
  - non-PostgreSQL databases (`supports_snapshot_consistency()` is `False`),
    Redshift included;
  - `JOINDIFF` / `AUTO` when `AUTO` resolves to JOINDIFF;
  - non-transactional (autocommit-only) connections;
  - at view startup: server not PostgreSQL 16, or a target that is not an
    ordinary table (view / matview / foreign / partitioned / temp).

## Design

Per side (`data_diff/consistency.py`):

- **Anchor** connection: `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY`,
  then `SELECT pg_export_snapshot()`. The anchor transaction stays open for the
  whole run — that is what keeps the exported snapshot importable.
- **Workers** (at most the caller's `thread_count`): each opens its own
  `REPEATABLE READ READ ONLY` transaction and imports the snapshot with
  `SET TRANSACTION SNAPSHOT`. If a worker's statement fails, its transaction is
  rolled back and re-armed (re-import the same snapshot) on next use.
- Per-side total connections = `thread_count` workers + 1 anchor. The caller's
  own `Database` object / pool / transactions / lifetime are never modified;
  the caller can keep querying its original connections live during the diff.

Binding the view to the diff:

- On first consumption only, `_diff_tables_wrapper` opens both views
  (`open_snapshot_views`, all-or-nothing) and rewrites `table1`/`table2` to use
  `SnapshotDatabase` façades. `TableSegment` is frozen/derived via `attrs.evolve`,
  so every recursive/key-bounded/checkpoint-derived segment keeps the façade and
  therefore the snapshot. Schema inspection, key-range probing,
  count/checksum, and leaf downloads all go through it.
- The existing HASHDIFF pruning/threshold/scheduling is untouched; only the
  connection each query runs on changed.

Lazy start and "both sides ready": views are created inside the producer
generator, which runs on the first `next()`. No connection is opened when the
result object is merely created. If the second side fails to open, the first is
closed and the original error propagates — no diff rows are produced.

Lifecycle (`DiffResultWrapper`):

- `close()` is idempotent; on a never-consumed result it opens nothing.
- Partial consumption + `close()`: stops dispatching, cancels/waits (bounded)
  for in-flight work, releases views; further iteration/statistics raise
  `DataDiffIncompleteResultError`.
- Full exhaustion: `close()` (and later `close()`) keep the cache readable; the
  stats describe the snapshot that was actually scanned (no live re-read).
- Query errors stick: already-produced rows remain, but stats/iteration re-raise
  the original error; a failed run never turns into a success summary.
- Bounded teardown for exhaustion, `close()`, exceptions and `KeyboardInterrupt`;
  with a healthy server it returns well under 5 s. A stalled server is handled by
  cancelling the running statement (`PQcancel`), with a short grace period.

MVCC / operational notes: the anchor's open transaction holds old row versions
for the run's duration (vacuum pressure during very long diffs). Per-side query
parallelism is limited to `thread_count`, matching the caller's pool.

## Verification (real PostgreSQL 16.16, aarch64, trust auth on localhost:5432)

Environment: PostgreSQL 16.16 built from the PGDG packages, `max_connections=200`,
default `REPEATABLE READ`-capable. Python 3.11, psycopg2 2.9.

`tests/test_snapshot_consistency.py` — **38 tests, all pass** (`~1.5–4.6 s`):

| Area | Tests |
| --- | --- |
| Correctness | simple diff, live default, lazy (no connection until consumed), same DB object both sides, two connections same DB, empty tables / one empty side, filter, compound key, precision semantics, derived segments keep view |
| Writes during run | insert/delete/move after views opened; mutation between key-range and checksum; mutation between checksum and leaf download — all excluded from this run; next run sees the new version |
| Rejection | bad value, JOINDIFF, AUTO→JOINDIFF, AUTO→HASHDIFF, non-Postgres (DuckDB), view target; all before touching business tables |
| Read-only account | `SELECT`-only role works end to end |
| Lifecycle | close-never-consumed, close twice after exhaustion (cache re-readable), partial-close bounded + incomplete error, context manager, sticky query failure, KeyboardInterrupt cleanup |
| Isolation | interleaved two diffs, concurrent two diffs (3 threads), live queries on caller's connection during snapshot |
| Failure cleanup | second-side open failure releases first side, worker init failure, cleanup error does not mask original |
| Parallelism | two same-side segment queries observed concurrently in `pg_stat_activity` (advisory-lock gate), per-side connection peak ≤ `thread_count + 1` |
| No leftovers | every test asserts no `data_diff_snapshot%` sessions remain (autocommit observer) |

Write-timing experiments use server-side barriers (a `gate_fn` on advisory
locks) and hook points keyed off the actual SQL, not `sleep`.

Regression (PostgreSQL rows): `tests/test_diff_tables.py -k PostgreSQL` 26
passed; `test_joindiff -k PostgreSQL` 7 passed; `test_format`/`test_query`/
`test_duckdb` 19 passed. Live-mode iteration, stats and cache re-read remain
available (covered by the live-default and lifecycle tests). Pre-existing
MySQL/Snowflake/dbt-dependent suites cannot run in this container (no servers /
`dbt` package); they fail identically on the unmodified tree.

## Large-table benchmark (`tests/bench_snapshot.py`)

Two tables of **250,001 rows** each, differences scattered over many segments
(every 5000th payload changed, 2 exclusive rows, 1 deleted, 1 moved key),
`thread_count=4`, `max_threadpool_size=4`. Observed via an autocommit
`pg_stat_activity` sampler:

| consistency | factor/threshold | rows downloaded | time (s) | max concurrent count queries (src / dst) | peak snapshot sessions | peak RSS (MB) |
| --- | --- | ---: | ---: | --- | ---: | ---: |
| live | 32 / 4096 | 20,092 | 3.19 | 4 / 4 | 0 | 49.0 |
| snapshot | 32 / 4096 | 14,747 | 2.55 | 4 / 4 | 10 | 50.7 |
| live | 16 / 2048 | 78,182 | 2.19 | 4 / 4 | 0 | 54.5 |
| snapshot | 16 / 2048 | 58,990 | 2.16 | 4 / 4 | 10 | 55.2 |

- Downloads are far below 250k → existing equality pruning + leaf threshold are
  in effect; the diff algorithm was not replaced.
- 4 concurrent count/checksum queries per side → per-side parallelism retained
  (not one global lock / single connection).
- Peak snapshot sessions 10 = 2 × (4 workers + 1 anchor) → the stated bound.
- Peak host RSS stays ~50–55 MB (small Python delta; no whole-table buffering).

## How to rerun

```bash
# start PostgreSQL 16 (any instance), e.g.:
#   initdb -D pgdata -U postgres -A trust
#   pg_ctl -D pgdata -l pg.log -o "-p 5432 -c max_connections=200" start
# set the test URI via tests/local_settings.py:
#   TEST_POSTGRESQL_CONN_STRING = "postgresql://postgres:Password1@localhost:5432/postgres"

python -m pytest tests/test_snapshot_consistency.py -q
python -m pytest tests/test_diff_tables.py tests/test_joindiff.py \
                tests/test_format.py tests/test_query.py -q -k PostgreSQL
python -m tests.bench_snapshot
```

## Unverified / boundaries

- Only PostgreSQL 16 was exercised; PostgreSQL 13–15 are rejected by the version
  check, and other engines by `supports_snapshot_consistency()`.
- Partitioned tables, views/matviews, foreign tables and temporary tables are
  rejected (ordinary tables only, first version).
- MySQL/Snowflake/Oracle/Redshift/BigQuery/Presto/Trino/Vertica/DuckDB and dbt
  integration could not be run here (no servers / missing `dbt`), so cross-DB
  behaviour beyond the rejection tests is unverified.
- `close()`'s 5 s bound was measured with a healthy server and with an
  advisory-lock stall (statement cancellation path); a fully hung network socket
  was not simulated.
