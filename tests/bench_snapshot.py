"""Two large tables (>=200k rows each) benchmarked through the normal HASHDIFF path.

Run with::

    python -m tests.bench_snapshot

It records:
  * actual concurrent queries (observed in pg_stat_activity),
  * peak connection count,
  * peak host RSS for this process,
  * wall time,
  * rows downloaded (from HashDiffer.stats['rows_downloaded']).

The point is to show that snapshot mode does not download whole tables (the
existing equality pruning + leaf threshold still control downloads), and that
per-side parallelism is preserved.
"""

import json
import resource
import threading
import time

import psycopg2

from data_diff import HashDiffer, TableSegment
from data_diff.databases._connect import connect
from data_diff.queries.api import commit

PG_URI = "postgresql://postgres:Password1@localhost:5432/postgres"
ROWS = 250_000


def peak_rss_mb():
    # ru_maxrss is in KiB on Linux.
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def build_tables(admin):
    admin.query(
        [
            "DROP TABLE IF EXISTS bench_src",
            "DROP TABLE IF EXISTS bench_dst",
            "CREATE TABLE bench_src(id int primary key, payload text, grp int)",
            "CREATE TABLE bench_dst(id int primary key, payload text, grp int)",
            commit,
        ]
    )
    admin.query(
        f"INSERT INTO bench_src SELECT i, md5(i::text) || repeat('p', 40), i % 997 "
        f"FROM generate_series(1, {ROWS}) i",
        None,
    )
    admin.query(
        f"INSERT INTO bench_dst SELECT i, md5(i::text) || repeat('p', 40), i % 997 "
        f"FROM generate_series(1, {ROWS}) i",
        None,
    )
    # Differences scattered over many segments: every 5000th row differs, plus
    # an exclusive row on each side, plus one moved key.
    admin.query(
        "UPDATE bench_dst SET payload = 'CHANGED-' || payload WHERE id % 5000 = 0",
        None,
    )
    admin.query("INSERT INTO bench_src VALUES (300001, 'only-in-src', 0)", None)
    admin.query("INSERT INTO bench_dst VALUES (300002, 'only-in-dst', 0)", None)
    admin.query("DELETE FROM bench_dst WHERE id = 123456", None)
    admin.query("INSERT INTO bench_dst VALUES (400001, 'moved', 0)", None)
    admin.query(commit)


class ActivityObserver(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.conn = psycopg2.connect(
            host="localhost",
            port=5432,
            user="postgres",
            password="Password1",
            dbname="postgres",
            application_name="bench_observer",
        )
        self.conn.autocommit = True
        self.stop = threading.Event()
        self.max_total = 0
        self.max_snapshot = 0
        self.max_active_workers_per_query_prefix = 0
        self.max_active_src = 0
        self.max_active_dst = 0

    def run(self):
        cur = self.conn.cursor()
        while not self.stop.is_set():
            cur.execute(
                "SELECT count(*), "
                "count(*) FILTER (WHERE application_name LIKE 'data_diff_snapshot%'), "
                "count(*) FILTER (WHERE state = 'active' AND query LIKE '%\"public\".\"bench_src\"%' "
                "                  AND query LIKE 'SELECT count(*)%'), "
                "count(*) FILTER (WHERE state = 'active' AND query LIKE '%\"public\".\"bench_dst\"%' "
                "                  AND query LIKE 'SELECT count(*)%') "
                "FROM pg_stat_activity WHERE application_name <> 'bench_observer'"
            )
            total, snapshot, src, dst = cur.fetchone()
            self.max_total = max(self.max_total, total)
            self.max_snapshot = max(self.max_snapshot, snapshot)
            self.max_active_src = max(self.max_active_src, src)
            self.max_active_dst = max(self.max_active_dst, dst)
            time.sleep(0.002)
        cur.close()

    def finish(self):
        self.stop.set()
        self.join(5)
        self.conn.close()


def run_once(admin, consistency, bisection_factor, bisection_threshold, threads):
    db1 = connect(PG_URI, thread_count=threads, shared=False)
    db2 = connect(PG_URI, thread_count=threads, shared=False)
    a = TableSegment(db1, ("public", "bench_src"), ("id",), extra_columns=("payload", "grp"))
    b = TableSegment(db2, ("public", "bench_dst"), ("id",), extra_columns=("payload", "grp"))
    differ = HashDiffer(
        bisection_factor=bisection_factor,
        bisection_threshold=bisection_threshold,
        max_threadpool_size=threads,
    )
    observer = ActivityObserver()
    observer.start()
    rss_before = peak_rss_mb()
    start = time.monotonic()
    result = differ.diff_tables(a, b, consistency=consistency)
    rows = list(result)
    stats = result.get_stats_dict()
    elapsed = time.monotonic() - start
    result.close()
    observer.finish()
    rss_peak = peak_rss_mb()
    db1.close()
    db2.close()
    return {
        "consistency": consistency,
        "elapsed_s": round(elapsed, 3),
        "diff_rows": len(rows),
        "rows_A": stats["rows_A"],
        "rows_B": stats["rows_B"],
        "updated": stats["updated"],
        "exclusive_A": stats["exclusive_A"],
        "exclusive_B": stats["exclusive_B"],
        "rows_downloaded": differ.stats.get("rows_downloaded", 0),
        "max_concurrent_count_queries_src": observer.max_active_src,
        "max_concurrent_count_queries_dst": observer.max_active_dst,
        "peak_snapshot_sessions": observer.max_snapshot,
        "peak_total_sessions": observer.max_total,
        "rss_before_mb": round(rss_before, 1),
        "rss_peak_mb": round(rss_peak, 1),
    }


def main():
    admin = connect(PG_URI, thread_count=1, shared=False)
    print(f"PostgreSQL row count target: {ROWS}")
    build_tables(admin)
    print("built tables:", admin.query("SELECT count(*) FROM bench_src", int), admin.query("SELECT count(*) FROM bench_dst", int))

    results = []
    # Live run first (baseline), then snapshot.  Several segment sizes so that
    # the reader can see pruning/threshold effects.
    for factor, threshold in ((32, 4096), (16, 2048)):
        results.append(run_once(admin, "live", factor, threshold, 4))
        results.append(run_once(admin, "snapshot", factor, threshold, 4))

    print(json.dumps(results, indent=2))
    admin.query(["DROP TABLE bench_src", "DROP TABLE bench_dst", commit])
    admin.close()
    return results


if __name__ == "__main__":
    main()
