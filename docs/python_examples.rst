Python API Examples
---------

**Example 1: Diff tables in mysql and postgresql**

.. code-block:: python
    # Optional: Set logging to display the progress of the diff
    import logging
    logging.basicConfig(level=logging.INFO)

    from data_diff import connect_to_table, diff_tables

    table1 = connect_to_table("postgresql:///", "table_name", "id")
    table2 = connect_to_table("mysql:///", "table_name", "id")

    for different_row in diff_tables(table1, table2):
        plus_or_minus, columns = different_row
        print(plus_or_minus, columns)


**Example 2: Connect to snowflake using dictionary configuration**

.. code-block:: python
    SNOWFLAKE_CONN_INFO = {
        "driver": "snowflake",
        "user": "erez",
        "account": "whatever",
        "database": "TESTS",
        "warehouse": "COMPUTE_WH",
        "role": "ACCOUNTADMIN",
        "schema": "PUBLIC",
        "key": "snowflake_rsa_key.p8",
    }

    snowflake_table = connect_to_table(SNOWFLAKE_CONN_INFO, "table_name")  # Uses id by default

Run `help(connect_to_table)` and `help(diff_tables)` or read our API reference to learn more about the different options:

- connect_to_table_

- diff_tables_

.. _connect_to_table: https://data-diff.readthedocs.io/en/latest/python-api.html#data_diff.connect_to_table
.. _diff_tables: https://data-diff.readthedocs.io/en/latest/python-api.html#data_diff.diff_tables

**Example 3: Consistent per-side reads with ``consistency="snapshot"``**

When both tables are being written to while you diff them, ``consistency="live"``
(the default) may read different committed states for different queries. With
``consistency="snapshot"`` each side is read from one committed state, fixed when
the result is first consumed.

.. code-block:: python
    from data_diff import connect_to_table, diff_tables, Algorithm

    table1 = connect_to_table("postgresql:///", "table_a", "id", thread_count=4)
    table2 = connect_to_table("postgresql:///", "table_b", "id", thread_count=4)

    result = diff_tables(
        table1,
        table2,
        algorithm=Algorithm.HASHDIFF,
        consistency="snapshot",
    )
    try:
        for op, row in result:
            print(op, row)
    finally:
        result.close()

Notes on the snapshot mode:

- It is supported for **PostgreSQL 16**, ordinary (non-temporary, non-partitioned,
  non-foreign) tables, and the **HASHDIFF** algorithm. ``AUTO`` is accepted only
  when it resolves to HASHDIFF. Anything else is rejected before the business
  tables are read. An invalid ``consistency`` value is rejected immediately too.
- Each side gets its own read view. It proves each side was read from a single
  committed state; it does **not** prove the two databases were equal at the
  same wall-clock instant.
- Read views are opened lazily, on first consumption — not when the result
  object is created. Both sides must be ready before any diff row is returned.
- Snapshot mode uses its own short-lived connections (per side: up to the
  caller's ``thread_count`` workers + one anchor). The caller's ``Database``
  object, its connection pool, its open transactions and its lifetime are never
  modified, and the caller can keep using it for ordinary queries.
- The returned object is the usual ``DiffResultWrapper``. It can be iterated,
  read for statistics, closed explicitly, or used as a context manager.

Result lifecycle:

.. code-block:: python
    result = diff_tables(t1, t2, consistency="snapshot")

    # 1. Never consumed: closing touches no database.
    result.close()

    # 2. Partially consumed, then closed: you keep the prefix you already read,
    #    but statistics and further iteration raise DataDiffIncompleteResultError.
    result = diff_tables(t1, t2, consistency="snapshot")
    first = next(iter(result))
    result.close()
    result.get_stats_dict()  # raises DataDiffIncompleteResultError

    # 3. Fully consumed: after close() the cached rows and statistics remain
    #    readable; they describe the snapshot that was actually scanned.
    result = diff_tables(t1, t2, consistency="snapshot")
    rows = list(result)
    result.close()
    assert list(result) == rows
    result.get_stats_dict()  # fine

    # 4. With a context manager, close() happens on exit (including on exceptions).
    with diff_tables(t1, t2, consistency="snapshot") as result:
        rows = list(result)

If a query fails mid-run, the rows already produced stay available, but the
failure is retained: later iteration and statistics re-raise it rather than
returning a success summary. Exhaustion, explicit ``close()``, exceptions and
``KeyboardInterrupt`` all run the same bounded cleanup; with a healthy server
``close()`` returns well within 5 seconds.

How the snapshot mode is implemented (for operators):

- Per side, one *anchor* connection opens ``REPEATABLE READ, READ ONLY`` and
  exports its snapshot. The anchor stays open for the whole run; this is what
  keeps the exported snapshot valid. Up to ``thread_count`` *worker* connections
  each open their own ``REPEATABLE READ, READ ONLY`` transaction and import the
  anchor snapshot with ``SET TRANSACTION SNAPSHOT``.
- All queries for one side — key-range probing, count/checksum, leaf downloads,
  and any queries added later by recursive bisection — run through that side's
  view, i.e. on one of its workers and inside its snapshot. ``TableSegment``
  derivatives carry the view-bound database, so they inherit this.
- Connections are created lazily on first consumption, only when the run
  actually starts, and closed on exhaustion/close. The caller's ``thread_count``
  pool is untouched, so it can keep serving ordinary queries.
- Cost trade-offs: the anchor transaction keeps old row versions alive for the
  duration of the diff, so very long diffs against busy tables can increase
  vacuum pressure. Concurrency is limited to ``thread_count`` per side (plus the
  anchor), matching the existing pool size. If opening either view fails, no
  diff rows are returned and the other side's resources are closed; cleanup
  errors never mask the original failure.
