"""Acceptance driver for ``dependency_policy="independent"`` on real PostgreSQL 16.

Run with::

    PYTHONPATH=/workspace python3 -m tests.acceptance_dependency_policy

It is self-contained: it creates the ``acc_*`` schemas/tables, writes a
hand-made development manifest, and drives ``dbt_diff`` for each scenario.  It
prints, per model, the structured outcome, the audit trail (start/finish and
real query counts), and the reason list for blocked models.  Exit code is 0
only if every expectation holds.
"""

import io
import json
import os
import sys
import threading
from contextlib import redirect_stdout

import psycopg2
from psycopg2 import sql

from data_diff import tracking
from data_diff.dbt import dbt_diff
from data_diff.dbt_dependency import ModelOutcome

tracking.disable_tracking()

PG = {
    "host": os.environ.get("DIFF_TEST_PG_HOST", "localhost"),
    "port": int(os.environ.get("DIFF_TEST_PG_PORT", "5432")),
    "user": os.environ.get("DIFF_TEST_PG_USER", "postgres"),
    "password": os.environ.get("DIFF_TEST_PG_PASSWORD", ""),
    "dbname": os.environ.get("DIFF_TEST_PG_DB", "postgres"),
}
DEV = "acc_dev"
PROD = "acc_prod"
CONNECTION = dict(PG, driver="postgresql")


def admin():
    conn = psycopg2.connect(**PG)
    conn.autocommit = True
    return conn


def node(uid, *, schema=DEV, depends_on=(), columns=None, materialized="table"):
    name = uid.split(".")[-1]
    return {
        "unique_id": uid,
        "resource_type": "model",
        "name": name,
        "alias": name,
        "database": PG["dbname"],
        "schema": schema,
        "columns": columns if columns is not None else {"id": {"meta": {}, "tags": []}},
        "meta": {},
        "config": {"database": None, "schema": None, "tags": [], "materialized": materialized},
        "tags": [],
        "test_metadata": None,
        "depends_on": {"macros": [], "nodes": list(depends_on)},
    }


def manifest(nodes):
    return {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v11.json",
            "dbt_version": "1.7.19",
            "project_id": "acceptance",
            "user_id": "acceptance",
        },
        "nodes": {n["unique_id"]: n for n in nodes},
        "sources": {},
    }


class Parser:
    def __init__(self, nodes, *, threads=3, models=None):
        from data_diff.dbt_config_validators import ManifestJsonConfig

        self.dev_manifest_obj = ManifestJsonConfig.parse_obj(manifest(nodes))
        self.prod_manifest_obj = None
        self.connection = dict(CONNECTION)
        self.threads = threads
        self.requires_upper = False
        self.unique_columns = {}
        self.dbt_user_id = "u"
        self.dbt_version = "1.7.19"
        self.dbt_project_id = "p"
        self._models = models

    def get_models(self, selection=None):
        if self._models is not None:
            return [self.dev_manifest_obj.nodes[u] for u in self._models]
        return list(self.dev_manifest_obj.nodes.values())

    def get_datadiff_config(self):
        from data_diff.dbt_parser import TDatadiffConfig

        return TDatadiffConfig(prod_database=PG["dbname"], prod_schema=PROD)

    def set_connection(self):
        pass

    def set_casing_policy_for(self, connection_type):
        pass

    def get_datadiff_model_config(self, meta):
        from data_diff.dbt_parser import TDatadiffModelConfig

        return TDatadiffModelConfig(where_filter=None, include_columns=[], exclude_columns=[])

    def get_pk_from_model(self, node_, unique_columns, pk_tag):
        return ["id"] if "id" in (node_.columns or {}) else []


class Audit:
    def __init__(self):
        self.events = []
        self.queries = {}
        self.records = []
        self.lock = threading.Lock()

    def event(self, kind, uid):
        with self.lock:
            self.events.append((kind, uid))

    def query(self, uid):
        with self.lock:
            self.queries[uid] = self.queries.get(uid, 0) + 1

    def record(self, result):
        with self.lock:
            self.records.append(result)


def run(nodes, *, models=None, threads=3, gates=None, json_output=True):
    from data_diff import dbt as dbt_module
    from data_diff.databases.base import Database

    audit = Audit()
    parser = Parser(nodes, threads=threads, models=models)

    original_query = Database.query
    local = threading.local()

    def counting_query(self, sql_ast, res_type=None, log_message=None):
        uid = getattr(local, "uid", None)
        if uid is not None:
            audit.query(uid)
        return original_query(self, sql_ast, res_type, log_message)

    original_run = dbt_module._run_local_model

    def wrapped_run(diff_vars, connection, threads_, json_output_, **kw):
        local.uid = diff_vars.dbt_model
        try:
            return original_run(diff_vars, connection, threads_, json_output_, **kw)
        finally:
            local.uid = None

    def factory(compare_one):
        def runner(uid):
            audit.event("start", uid)
            gate = (gates or {}).get(uid)
            try:
                if gate:
                    gate(uid)
                return compare_one(uid)
            finally:
                audit.event("finish", uid)

        return runner

    original_report = dbt_module._LocalReport.__init__

    def report_init(self, json_output_):
        original_report(self, json_output_)
        emit = self.emit

        def wrapped(result):
            audit.record(result)
            emit(result)

        self.emit = wrapped

    stdout = io.StringIO()
    import unittest.mock as mock

    with mock.patch.object(Database, "query", counting_query), \
         mock.patch.object(dbt_module, "_run_local_model", side_effect=wrapped_run), \
         mock.patch.object(dbt_module._LocalReport, "__init__", report_init), \
         redirect_stdout(stdout):
        dbt_diff(json_output=json_output, dependency_policy="independent", _parser=parser, _runner_factory=factory)

    records = [json.loads(line) for line in stdout.getvalue().splitlines() if line.startswith("{")]
    by_model = {r["model"]: r for r in records}
    return by_model, audit, stdout.getvalue()


# --------------------------------------------------------------------------
# Scenarios
# --------------------------------------------------------------------------

FAILURES = []


def check(label, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    print(f"  [{status}] {label}" + (f"  ({detail})" if detail and not condition else ""))
    if not condition:
        FAILURES.append(label)


def make_schemas(conn):
    with conn.cursor() as cur:
        for schema in (DEV, PROD):
            cur.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))
            cur.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))


def table(conn, schema, name, columns="id integer primary key, v text", rows=((1, "a"),)):
    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE TABLE {}.{} ({})").format(
            sql.Identifier(schema), sql.Identifier(name), sql.SQL(columns)
        ))
        for row in rows:
            cur.execute(
                sql.SQL("INSERT INTO {}.{} VALUES ({})").format(
                    sql.Identifier(schema), sql.Identifier(name),
                    sql.SQL(", ".join(["%s"] * len(row))),
                ),
                list(row),
            )


def break_relation(conn, schema, name):
    with conn.cursor() as cur:
        cur.execute(sql.SQL(
            "CREATE OR REPLACE FUNCTION {}() RETURNS integer LANGUAGE plpgsql AS "
            "$$ BEGIN RAISE EXCEPTION 'injected query failure'; END; $$"
        ).format(sql.Identifier(schema, f"{name}_boom")))
        cur.execute(sql.SQL("DROP TABLE {}.{}").format(sql.Identifier(schema), sql.Identifier(name)))
        cur.execute(sql.SQL(
            "CREATE VIEW {}.{} AS SELECT k AS id, k AS v FROM (SELECT {}.{}() AS k) AS s"
        ).format(
            sql.Identifier(schema), sql.Identifier(name),
            sql.Identifier(schema), sql.Identifier(f"{name}_boom"),
        ))


def scenario_diamond(conn, slow):
    print(f"\n== Diamond, {slow} settles last ==")
    A, B, C, D = "model.acc.A", "model.acc.B", "model.acc.C", "model.acc.D"
    make_schemas(conn)
    table(conn, PROD, "A", rows=((1, "a"),))
    table(conn, DEV, "A", rows=((1, "changed"),))  # different
    table(conn, PROD, "B")
    table(conn, DEV, "B")
    break_relation(conn, DEV, "B")  # error
    table(conn, PROD, "C")
    table(conn, DEV, "C")
    table(conn, PROD, "D")
    table(conn, DEV, "D")

    nodes = [node(A), node(B), node(C, depends_on=[A, B]), node(D)]
    gate = threading.Event()

    def limit(uid):
        if uid == slow:
            gate.wait(5)
        else:
            gate.set()

    by_model, audit, output = run(nodes, threads=3, gates={A: limit, B: limit})
    print("  outcomes:", {m: by_model[m]["outcome"] for m in sorted(by_model)})
    print("  starts:  ", [u for k, u in audit.events if k == "start"])
    print("  queries: ", dict(sorted(audit.queries.items())))
    check("A is different", by_model[A]["outcome"] == "different")
    check("B is error", by_model[B]["outcome"] == "error")
    check("C is blocked", by_model[C]["outcome"] == "blocked")
    check(
        "C reasons are A different + B error",
        by_model[C]["blocked_by"]
        == [{"unique_id": A, "outcome": "different"}, {"unique_id": B, "outcome": "error"}],
        str(by_model[C]["blocked_by"]),
    )
    check("C was never started", C not in [u for k, u in audit.events if k == "start"])
    check("C made no queries", C not in audit.queries)
    check("D ran independently", by_model[D]["outcome"] == "identical")
    check("D started before the slow ancestor finished", audit.events.index(("start", D)) < audit.events.index(("finish", slow)))
    return by_model[C]["blocked_by"]


def scenario_independent_overlap(conn):
    print("\n== Two independent chains, threads=2 ==")
    L, R = "model.acc.left", "model.acc.right"
    make_schemas(conn)
    table(conn, PROD, "left")
    table(conn, DEV, "left")
    table(conn, PROD, "right")
    table(conn, DEV, "right")

    barrier = threading.Barrier(2, timeout=5)
    state = {"now": 0, "peak": 0}
    lock = threading.Lock()

    def gate(_uid):
        with lock:
            state["now"] += 1
            state["peak"] = max(state["peak"], state["now"])
        barrier.wait()
        with lock:
            state["now"] -= 1

    by_model, audit, _ = run([node(L), node(R)], threads=2, gates={L: gate, R: gate})
    print("  peak concurrency:", state["peak"])
    check("both identical", by_model[L]["outcome"] == by_model[R]["outcome"] == "identical")
    check("two checks overlap at threads=2", state["peak"] == 2)


def scenario_data(conn):
    print("\n== Data conclusions ==")
    make_schemas(conn)
    table(conn, PROD, "ident", rows=((1, "a"), (2, "b")))
    table(conn, DEV, "ident", rows=((1, "a"), (2, "b")))
    table(conn, PROD, "empty", rows=())
    table(conn, DEV, "empty", rows=())
    table(conn, PROD, "emptydiff", rows=())
    table(conn, DEV, "emptydiff", rows=((1, "x"),))
    table(conn, PROD, "cols", "id integer primary key, old_col text, v integer", ((1, "x", 5),))
    table(conn, DEV, "cols", "id integer primary key, new_col text, v text", ((1, "y", "5"),))

    nodes = [node(f"model.acc.{n}") for n in ("ident", "empty", "emptydiff", "cols")]
    by_model, _, _ = run(nodes, threads=3)
    print("  outcomes:", {m.split('.')[-1]: by_model[m]["outcome"] for m in sorted(by_model)})
    check("identical tables -> identical", by_model["model.acc.ident"]["outcome"] == "identical")
    check("both empty -> identical", by_model["model.acc.empty"]["outcome"] == "identical")
    check("empty vs rows -> different", by_model["model.acc.emptydiff"]["outcome"] == "different")
    check("column add/drop/type -> different", by_model["model.acc.cols"]["outcome"] == "different")


def scenario_unavailable_error(conn):
    print("\n== Unavailable / error propagation ==")
    A, B = "model.acc.uA", "model.acc.uB"
    make_schemas(conn)
    table(conn, DEV, "uA")  # prod missing
    table(conn, PROD, "uB")
    table(conn, DEV, "uB")
    nodes = [node(A), node(B, depends_on=[A])]
    by_model, audit, _ = run(nodes, threads=2)
    print("  outcomes:", {m: by_model[m]["outcome"] for m in sorted(by_model)})
    check("missing prod relation -> unavailable", by_model[A]["outcome"] == "unavailable")
    check("descendant -> blocked", by_model[B]["outcome"] == "blocked")
    check("descendant reason is unavailable", by_model[B]["blocked_by"] == [{"unique_id": A, "outcome": "unavailable"}])
    check("descendant made no queries", B not in audit.queries)

    print("\n== Injected query failure ==")
    make_schemas(conn)
    table(conn, PROD, "eA")
    table(conn, DEV, "eA")
    break_relation(conn, DEV, "eA")
    table(conn, PROD, "eB")
    table(conn, DEV, "eB")
    nodes = [node(A.replace("u", "e")), node(B.replace("u", "e"), depends_on=[A.replace("u", "e")])]
    by_model, audit, _ = run(nodes, threads=2)
    print("  outcomes:", {m: by_model[m]["outcome"] for m in sorted(by_model)})
    check("query failure -> error", by_model[nodes[0]["unique_id"]]["outcome"] == "error")
    check("descendant -> blocked on error", by_model[nodes[1]["unique_id"]]["blocked_by"] == [{"unique_id": nodes[0]["unique_id"], "outcome": "error"}])


def main():
    conn = admin()
    try:
        first = scenario_diamond(conn, "model.acc.A")
        second = scenario_diamond(conn, "model.acc.B")
        check("reason list stable across completion orders", first == second)
        scenario_independent_overlap(conn)
        scenario_data(conn)
        scenario_unavailable_error(conn)
    finally:
        with conn.cursor() as cur:
            for schema in (DEV, PROD):
                cur.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))
        conn.close()
    print("\nRESULT:", "ALL CHECKS PASSED" if not FAILURES else f"{len(FAILURES)} FAILURE(S): {FAILURES}")
    return 0 if not FAILURES else 1


if __name__ == "__main__":
    sys.exit(main())
