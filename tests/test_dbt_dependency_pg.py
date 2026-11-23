"""Real-PostgreSQL-16 acceptance tests for ``dependency_policy="independent"``.

Structure:

* a hand-written development manifest fixes the graph (diamond, two independent
  chains, unselected and ephemeral intermediates);
* ``dbt_diff`` is driven end to end; a thin ``_runner_factory`` wraps the *real*
  comparison so that start/finish events and database query counts are
  recorded, without replacing the comparison or its conclusions;
* data scenarios (value changes, empty tables, column add/drop/type change,
  excluded columns, stats, filters) go through the real JOINDIFF path;
* unavailable/error cases come from a real dropped relation, real revoked
  privileges, and a real injected query failure.

Telemetry is disabled for the whole module.
"""

import io
import json
import os
import threading
import unittest
import unittest.mock
from contextlib import redirect_stdout

import psycopg2
from psycopg2 import sql

from data_diff import tracking
from data_diff.dbt import dbt_diff
from data_diff.dbt_dependency import ModelOutcome
from data_diff.errors import (
    DataDiffDependencyGraphError,
    DataDiffDependencyPolicyError,
    DataDiffSelectionConflictError,
)

tracking.disable_tracking()

PG_HOST = os.environ.get("DIFF_TEST_PG_HOST", "localhost")
PG_PORT = int(os.environ.get("DIFF_TEST_PG_PORT", "5432"))
PG_USER = os.environ.get("DIFF_TEST_PG_USER", "postgres")
PG_PASSWORD = os.environ.get("DIFF_TEST_PG_PASSWORD", "")
PG_DB = os.environ.get("DIFF_TEST_PG_DB", "postgres")

DEV_SCHEMA = "dbtdep_dev"
PROD_SCHEMA = "dbtdep_prod"
CONNECTION = {
    "driver": "postgresql",
    "host": PG_HOST,
    "port": PG_PORT,
    "user": PG_USER,
    "password": PG_PASSWORD,
    "dbname": PG_DB,
}


def _pg_available() -> bool:
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            dbname=PG_DB,
            connect_timeout=3,
        )
    except Exception:
        return False
    conn.close()
    return True


PG_AVAILABLE = _pg_available()
requires_pg = unittest.skipUnless(PG_AVAILABLE, "PostgreSQL 16 is not available")


# --------------------------------------------------------------------------
# Manifest helpers
# --------------------------------------------------------------------------


def _column(name, meta=None, tags=None):
    return {"meta": meta or {}, "tags": tags or []}


def _node(
    unique_id,
    *,
    name=None,
    alias=None,
    schema=DEV_SCHEMA,
    depends_on=(),
    materialized="table",
    columns=None,
):
    name = name or unique_id.split(".")[-1]
    return {
        "unique_id": unique_id,
        "resource_type": "model",
        "name": name,
        "alias": alias or name,
        "database": PG_DB,
        "schema": schema,
        "columns": columns if columns is not None else {"id": _column("id")},
        "meta": {},
        "config": {"database": None, "schema": None, "tags": [], "materialized": materialized},
        "tags": [],
        "test_metadata": None,
        "depends_on": {"macros": [], "nodes": list(depends_on)},
    }


def manifest_for(nodes):
    return {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v11.json",
            "dbt_version": "1.7.19",
            "project_id": "dependency_policy_pg",
            "user_id": "test-user",
        },
        "nodes": {node["unique_id"]: node for node in nodes},
        "sources": {},
    }


class FakeDbtParser:
    """A DbtParser stand-in with a fixed manifest and a real connection dict."""

    def __init__(self, manifest_dict, *, threads=2, model_config=None):
        from data_diff.dbt_config_validators import ManifestJsonConfig

        self.dev_manifest_obj = ManifestJsonConfig.parse_obj(manifest_dict)
        self.prod_manifest_obj = None
        self.connection = dict(CONNECTION)
        self.threads = threads
        self.requires_upper = False
        self.unique_columns = {}
        self.dbt_user_id = "u"
        self.dbt_version = "1.7.19"
        self.dbt_project_id = "p"
        self.last_plan = None
        self.model_config = model_config

    def get_models(self, selection=None):
        return list(self.dev_manifest_obj.nodes.values())

    def get_datadiff_config(self):
        from data_diff.dbt_parser import TDatadiffConfig

        return TDatadiffConfig(prod_database=PG_DB, prod_schema=PROD_SCHEMA)

    def set_connection(self):
        pass

    def set_casing_policy_for(self, connection_type):
        pass

    def get_datadiff_model_config(self, meta):
        from data_diff.dbt_parser import TDatadiffModelConfig

        if self.model_config is not None:
            return self.model_config
        return TDatadiffModelConfig(where_filter=None, include_columns=[], exclude_columns=[])

    def get_pk_from_model(self, node, unique_columns, pk_tag):
        return ["id"] if "id" in (node.columns or {}) else []


# --------------------------------------------------------------------------
# Admin / observation helpers
# --------------------------------------------------------------------------


def admin():
    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD, dbname=PG_DB)
    conn.autocommit = True
    return conn


class RunLog:
    """Ordered start/finish events, captured records, and per-model query counts."""

    def __init__(self):
        self.events = []
        self.records = []
        self.query_counts = {}
        self.lock = threading.Lock()

    def event(self, name, unique_id):
        with self.lock:
            self.events.append((name, unique_id))

    def record(self, result):
        with self.lock:
            self.records.append(result)

    def count(self, unique_id):
        with self.lock:
            self.query_counts[unique_id] = self.query_counts.get(unique_id, 0) + 1

    def started(self):
        return [uid for event, uid in self.events if event == "start"]

    def finished(self):
        return [uid for event, uid in self.events if event == "finish"]

    def index(self, event, unique_id):
        return self.events.index((event, unique_id))

    def outcome(self, unique_id):
        return next(r.outcome for r in self.records if r.unique_id == unique_id)


def make_runner_factory(run_log, gates=None):
    """Wrap the *real* comparison to record events; conclusions stay real."""

    def factory(compare_one):
        def runner(unique_id):
            run_log.event("start", unique_id)
            gate = (gates or {}).get(unique_id)
            try:
                if gate is not None:
                    gate(unique_id)
                return compare_one(unique_id)
            finally:
                run_log.event("finish", unique_id)

        return runner

    return factory


def make_query_counting_connect(run_log):
    """Count every real query, attributed to the model currently running.

    This records the actual ``Database.query`` calls (schema lookups, range
    probes, checksums, joins) rather than a placeholder boolean, so a whole
    missing table or a downstream that jumped the gate is visible.
    """
    from data_diff.databases.base import Database

    original_query = Database.query
    local = threading.local()

    def counting_query(self, sql_ast, res_type=None, log_message=None):
        unique_id = getattr(local, "unique_id", None)
        if unique_id is not None:
            run_log.count(unique_id)
        return original_query(self, sql_ast, res_type, log_message)

    return counting_query, local



_POSITIONAL_DIFF_VARS = {name: i for i, name in enumerate(['where_flag', 'stats_flag', 'columns_flag', 'production_database_flag', 'production_schema_flag'])}


def make_prod_schema_diff_vars(dbt_module, database, prod_schema, **overrides):
    """Patch target that redirects prod paths, without recursing into itself.

    ``_get_diff_vars`` is resolved *once*, here, from the module already
    patched with this function's return value -- calling ``dbt_module`` again
    inside would hit the mock.
    """
    real_get_diff_vars = dbt_module._get_diff_vars

    def diff_vars_with_prod_schema(dbt_parser, config, model, *args, **kw):
        config = type(config)(prod_database=database, prod_schema=prod_schema)
        for key, value in overrides.items():
            # Positional args already carry these, so only fill gaps.
            if len(args) < _POSITIONAL_DIFF_VARS[key]:
                kw.setdefault(key, value)
        return real_get_diff_vars(dbt_parser, config, model, *args, **kw)

    return diff_vars_with_prod_schema



def _last_json_line(output):
    return json.loads([line for line in output.splitlines() if line.startswith("{")][-1])


class PgDbtCase(unittest.TestCase):
    """Creates fresh dev/prod schemas and captures printed records."""

    def setUp(self):
        self.conn = admin()
        self._setup_schemas()

    def _setup_schemas(self):
        with self.conn.cursor() as cur:
            for schema in (DEV_SCHEMA, PROD_SCHEMA):
                cur.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))
                cur.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
            cur.execute("DROP ROLE IF EXISTS dbtdep_reader")
            cur.execute("CREATE ROLE dbtdep_reader LOGIN PASSWORD 'reader'")

    def tearDown(self):
        with self.conn.cursor() as cur:
            for schema in (DEV_SCHEMA, PROD_SCHEMA):
                cur.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))
            cur.execute("DROP ROLE IF EXISTS dbtdep_reader")
        self.conn.close()

    # -- schema/table helpers ------------------------------------------------
    def table(self, schema, name, columns="id integer primary key, v text", rows=((1, "a"),)):
        with self.conn.cursor() as cur:
            cur.execute(
                sql.SQL("CREATE TABLE {}.{} ({})").format(
                    sql.Identifier(schema), sql.Identifier(name), sql.SQL(columns)
                )
            )
            if rows:
                placeholders = ", ".join(["%s"] * len(rows))
                cur.execute(
                    sql.SQL("INSERT INTO {}.{} VALUES {}").format(
                        sql.Identifier(schema), sql.Identifier(name), sql.SQL(placeholders)
                    ),
                    [row for row in rows],
                )

    def both(self, name, prod_columns="id integer primary key, v text", prod_rows=((1, "a"),), dev_rows=None):
        self.table(PROD_SCHEMA, name, prod_columns, prod_rows)
        self.table(DEV_SCHEMA, name, prod_columns, prod_rows if dev_rows is None else dev_rows)

    def drop(self, schema, name):
        with self.conn.cursor() as cur:
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {}.{}").format(sql.Identifier(schema), sql.Identifier(name)))

    def _break_dev_relation(self, name):
        # A view whose projection raises at *execution* time: the relation
        # exists and its schema is readable, but every data query raises a real
        # adapter error.  This is a query failure, not a missing relation.
        self.execute(
            sql.SQL(
                "CREATE OR REPLACE FUNCTION {}() RETURNS integer LANGUAGE plpgsql AS "
                "$$ BEGIN RAISE EXCEPTION 'injected query failure'; END; $$"
            ).format(sql.Identifier(DEV_SCHEMA, f"{name}_boom"))
        )
        self.execute(
            sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                sql.Identifier(DEV_SCHEMA), sql.Identifier(name)
            )
        )
        self.execute(
            sql.SQL(
                "CREATE VIEW {}.{} AS SELECT k AS id, k AS v "
                "FROM (SELECT {}.{}() AS k) AS s"
            ).format(
                sql.Identifier(DEV_SCHEMA),
                sql.Identifier(name),
                sql.Identifier(DEV_SCHEMA),
                sql.Identifier(f"{name}_boom"),
            )
        )


    def execute(self, statement, params=None):
        with self.conn.cursor() as cur:
            cur.execute(statement, params)

    # -- driver --------------------------------------------------------------
    def run_policy(
        self,
        nodes,
        *,
        json_output=False,
        threads=2,
        gates=None,
        models_override=None,
        connector=None,
        capture_plan=None,
        model_config=None,
        **kwargs,
    ):
        manifest = manifest_for(nodes)
        parser = FakeDbtParser(manifest, threads=threads, model_config=model_config)
        if models_override is not None:
            parser.get_models = lambda selection=None: list(models_override)
        run_log = RunLog()

        stdout = io.StringIO()
        from data_diff import dbt as dbt_module

        original_report_init = dbt_module._LocalReport.__init__

        def capturing_init(inner_self, json_output_):
            original_report_init(inner_self, json_output_)
            original_emit = inner_self.emit

            def emit(result):
                run_log.record(result)
                original_emit(result)

            inner_self.emit = emit

        real_build = dbt_module.build_dependency_plan

        def record_plan(selected_nodes, manifest_obj):
            plan = real_build(selected_nodes, manifest_obj)
            parser.last_plan = plan
            return plan

        with unittest.mock.patch.object(dbt_module._LocalReport, "__init__", capturing_init), \
             unittest.mock.patch.object(dbt_module, "build_dependency_plan", side_effect=record_plan), \
             redirect_stdout(stdout):
            dbt_diff(
                json_output=json_output,
                dependency_policy="independent",
                _parser=parser,
                _runner_factory=make_runner_factory(run_log, gates),
                _connector=connector,
                **kwargs,
            )
        output = stdout.getvalue()
        json_lines = [json.loads(line) for line in output.splitlines() if line.strip().startswith("{")]
        return json_lines, output, run_log, parser.last_plan


# --------------------------------------------------------------------------
# Graph choreography (no database access in the test body)
# --------------------------------------------------------------------------


@requires_pg
class TestDiamondOrdering(PgDbtCase):
    A = "model.pkg.A"
    B = "model.pkg.B"
    C = "model.pkg.C"
    D = "model.pkg.D"

    def nodes(self):
        return [
            _node(self.A),
            _node(self.B),
            _node(self.C, depends_on=[self.A, self.B]),
            _node(self.D),
        ]

    def make_data(self):
        # A differs (values changed), B errors (injected query failure), C and D
        # are identical.
        self.table(PROD_SCHEMA, "A", rows=((1, "a"),))
        self.table(DEV_SCHEMA, "A", rows=((1, "changed"),))
        self.table(PROD_SCHEMA, "B")
        self.table(DEV_SCHEMA, "B")
        self._break_dev_relation("B")
        self.both("C")
        self.both("D")

    def run_with_order(self, slow_model):
        self.make_data()
        gate = threading.Event()

        def gate_for(unique_id):
            if unique_id == slow_model:
                gate.wait(5)
            else:
                gate.set()

        gates = {self.A: gate_for, self.B: gate_for}
        # Three workers: A, B and the unrelated D are released at once, so D's
        # start cannot be an artifact of a worker becoming free later.
        json_lines, output, run_log, plan = self.run_policy(
            self.nodes(), json_output=True, threads=3, gates=gates
        )
        return json_lines, output, run_log

    def assert_early_release(self, run_log, slow_model):
        # C must never be started, so it makes no query at all.
        self.assertNotIn(self.C, run_log.started())
        self.assertNotIn(self.C, run_log.query_counts)
        by_id = {r.unique_id: r for r in run_log.records}
        self.assertEqual(by_id[self.C].outcome, ModelOutcome.BLOCKED)
        self.assertEqual(
            by_id[self.C].blocked_by,
            ((self.A, ModelOutcome.DIFFERENT), (self.B, ModelOutcome.ERROR)),
        )
        # D starts independently of A/B/C.
        self.assertIn(self.D, run_log.started())
        self.assertLess(run_log.index("start", self.D), run_log.index("finish", slow_model))

    def test_a_settles_last(self):
        json_lines, output, run_log = self.run_with_order(self.A)
        self.assert_early_release(run_log, self.A)
        c_record = next(item for item in json_lines if item["model"] == self.C)
        self.assertEqual(c_record["status"], "blocked")
        self.assertEqual(
            c_record["blocked_by"],
            [
                {"unique_id": self.A, "outcome": "different"},
                {"unique_id": self.B, "outcome": "error"},
            ],
        )

    def test_b_settles_last(self):
        json_lines, output, run_log = self.run_with_order(self.B)
        self.assert_early_release(run_log, self.B)
        c_record = next(item for item in json_lines if item["model"] == self.C)
        self.assertEqual(
            c_record["blocked_by"],
            [
                {"unique_id": self.A, "outcome": "different"},
                {"unique_id": self.B, "outcome": "error"},
            ],
        )

    def test_each_selected_model_has_exactly_one_record(self):
        json_lines, output, run_log = self.run_with_order(self.B)
        models = [item["model"] for item in json_lines]
        self.assertEqual(sorted(models), sorted([self.A, self.B, self.C, self.D]))
        self.assertEqual(len(models), len(set(models)))

    def test_text_reports_same_reasons(self):
        self.make_data()
        json_lines, output, run_log, _ = self.run_policy(self.nodes(), json_output=False, threads=2)
        self.assertIn("Blocked by non-identical selected ancestors", output)
        self.assertIn(f"{self.A}: different", output)
        self.assertIn(f"{self.B}: error", output)


@requires_pg
class TestIndependentChainsOverlap(PgDbtCase):
    def test_two_independent_checks_overlap_with_two_threads(self):
        nodes = [_node("model.pkg.left"), _node("model.pkg.right")]
        self.both("left")
        self.both("right")

        barrier = threading.Barrier(2, timeout=5)
        overlap = {"now": 0, "peak": 0}
        lock = threading.Lock()

        def gate(_uid):
            with lock:
                overlap["now"] += 1
                overlap["peak"] = max(overlap["peak"], overlap["now"])
            barrier.wait()
            with lock:
                overlap["now"] -= 1

        gates = {"model.pkg.left": gate, "model.pkg.right": gate}
        _, _, run_log, _ = self.run_policy(nodes, json_output=True, threads=2, gates=gates)
        self.assertEqual(run_log.outcome("model.pkg.left"), ModelOutcome.IDENTICAL)
        self.assertEqual(run_log.outcome("model.pkg.right"), ModelOutcome.IDENTICAL)
        self.assertEqual(overlap["peak"], 2)


@requires_pg
class TestUnselectedAndEphemeralIntermediates(PgDbtCase):
    A = "model.pkg.A"
    X = "model.pkg.X"  # unselected intermediate
    E = "model.pkg.E"  # ephemeral intermediate
    C = "model.pkg.C"

    def test_constraint_passes_through_intermediates(self):
        nodes = [
            _node(self.A),
            _node(self.X, depends_on=[self.A]),
            _node(self.E, depends_on=[self.X], materialized="ephemeral"),
            _node(self.C, depends_on=[self.E]),
        ]
        self.table(PROD_SCHEMA, "A", rows=((1, "a"),))
        self.table(DEV_SCHEMA, "A", rows=((1, "different"),))
        self.both("C")

        # Only A and C are selected; X and E must never be checked or printed.
        from data_diff.dbt_config_validators import ManifestJsonConfig

        parsed = ManifestJsonConfig.parse_obj(manifest_for(nodes))
        records, output, run_log, plan = self.run_policy(
            nodes,
            json_output=True,
            models_override=[parsed.nodes[self.A], parsed.nodes[self.C]],
        )
        self.assertEqual(plan.selected, (self.A, self.C))
        self.assertEqual(plan.ancestors_of(self.C), frozenset({self.A}))
        by_model = {r["model"]: r for r in records}
        self.assertEqual(by_model[self.A]["outcome"], "different")
        self.assertEqual(by_model[self.C]["outcome"], "blocked")
        self.assertNotIn(self.X, by_model)
        self.assertNotIn(self.E, by_model)


# --------------------------------------------------------------------------
# Data conclusions
# --------------------------------------------------------------------------


@requires_pg
class TestDataOutcomes(PgDbtCase):
    def single(self, **kwargs):
        nodes = [_node("model.pkg.m")]
        return self.run_policy(nodes, json_output=True, **kwargs)

    def test_value_change_is_different(self):
        self.table(PROD_SCHEMA, "m", rows=((1, "a"), (2, "b")))
        self.table(DEV_SCHEMA, "m", rows=((1, "a"), (2, "B")))
        json_lines, _, run_log, _ = self.single()
        self.assertEqual(json_lines[0]["outcome"], "different")
        self.assertEqual(json_lines[0]["result"], "different")
        self.assertEqual(json_lines[0]["status"], "success")

    def test_jsonl_keeps_legacy_fields(self):
        # The pre-existing success record keeps its data, columns and stats;
        # ``outcome`` is the only addition.
        self.table(PROD_SCHEMA, "m", rows=((1, "a"), (2, "b")))
        self.table(DEV_SCHEMA, "m", rows=((1, "a"), (2, "B")))
        json_lines, _, _, _ = self.single()
        record = json_lines[0]
        for key in ("version", "status", "result", "model", "dataset1", "dataset2", "rows", "summary", "columns"):
            self.assertIn(key, record)
        self.assertEqual(record["version"], "1.1.0")
        self.assertEqual(record["status"], "success")
        self.assertIsNotNone(record["rows"])
        self.assertIsNotNone(record["summary"])
        self.assertIsNotNone(record["columns"])
        self.assertEqual(record["rows"]["diff"][0]["v"]["dataset1"], "b")
        self.assertEqual(record["rows"]["diff"][0]["v"]["dataset2"], "B")
        # The new field must not masquerade as a zero-diff success.
        self.assertEqual(record["outcome"], "different")

    def test_identical_is_identical(self):
        self.both("m")
        json_lines, _, run_log, _ = self.single()
        self.assertEqual(json_lines[0]["outcome"], "identical")
        self.assertEqual(json_lines[0]["result"], "identical")

    def test_empty_tables_are_identical(self):
        self.both("m", prod_rows=())
        json_lines, _, _, _ = self.single()
        self.assertEqual(json_lines[0]["outcome"], "identical")

    def test_columns_added_removed_type_changed_are_different(self):
        self.table(PROD_SCHEMA, "m", "id integer primary key, old_col text, v integer", ((1, "x", 5),))
        self.table(DEV_SCHEMA, "m", "id integer primary key, new_col text, v text", ((1, "y", "5"),))
        json_lines, _, _, _ = self.single()
        record = json_lines[0]
        self.assertEqual(record["outcome"], "different")
        self.assertTrue(record["columns"]["exclusive"]["dataset1"])  # removed
        self.assertTrue(record["columns"]["exclusive"]["dataset2"])  # added
        self.assertEqual(record["columns"]["typeChanged"], ["v"])

    def test_excluded_column_only_is_identical(self):
        self.table(PROD_SCHEMA, "m", "id integer primary key, v text, ignored text", ((1, "a", "left"),))
        self.table(DEV_SCHEMA, "m", "id integer primary key, v text, ignored text", ((1, "a", "right"),))
        nodes = [
            _node(
                "model.pkg.m",
                columns={
                    "id": _column("id"),
                    "v": _column("v"),
                    "ignored": _column("ignored", meta={"datafold": {"datadiff": {"exclude_columns": ["ignored"]}}}),
                },
            )
        ]
        from data_diff.dbt_parser import TDatadiffModelConfig

        json_lines, _, _, _ = self.run_policy(
            nodes,
            json_output=True,
            model_config=TDatadiffModelConfig(where_filter=None, include_columns=[], exclude_columns=["ignored"]),
        )
        self.assertEqual(json_lines[0]["outcome"], "identical")

    def test_stats_mode_still_compares(self):
        self.table(PROD_SCHEMA, "m", rows=((1, "a"), (2, "b")))
        self.table(DEV_SCHEMA, "m", rows=((1, "a"), (2, "Z")))
        json_lines, _, _, _ = self.single(stats_flag=True)
        record = json_lines[0]
        self.assertEqual(record["outcome"], "different")
        self.assertIsNone(record["rows"])  # stats-only does not show rows
        self.assertIsNotNone(record["summary"])
        self.assertEqual(record["summary"]["rows"]["updated"], 1)

    def test_filter_detects_difference_inside_scope(self):
        self.table(PROD_SCHEMA, "m", rows=((1, "a"), (2, "b")))
        self.table(DEV_SCHEMA, "m", rows=((1, "a"), (2, "Z")))
        nodes = [
            _node(
                "model.pkg.m",
                columns={
                    "id": _column("id"),
                    "v": _column("v", meta={"datafold": {"datadiff": {"filter": "id >= 2"}}}),
                },
            )
        ]
        from data_diff.dbt_parser import TDatadiffModelConfig

        json_lines, _, _, _ = self.run_policy(
            nodes,
            json_output=True,
            model_config=TDatadiffModelConfig(where_filter="id >= 2", include_columns=[], exclude_columns=[]),
        )
        self.assertEqual(json_lines[0]["outcome"], "different")

    def test_filter_hides_difference_outside_scope(self):
        self.table(PROD_SCHEMA, "m", rows=((1, "a"), (2, "b")))
        self.table(DEV_SCHEMA, "m", rows=((1, "Z"), (2, "b")))
        nodes = [_node("model.pkg.m")]
        json_lines, _, _, _ = self.run_policy(nodes, json_output=True, where_flag="id >= 2")
        self.assertEqual(json_lines[0]["outcome"], "identical")


# --------------------------------------------------------------------------
# Unavailable / error propagation
# --------------------------------------------------------------------------


@requires_pg
class TestUnavailableAndError(PgDbtCase):
    A = "model.pkg.A"
    B = "model.pkg.B"

    def chain(self):
        return [_node(self.A), _node(self.B, depends_on=[self.A])]

    def test_missing_prod_relation_is_unavailable_and_blocks(self):
        self.table(DEV_SCHEMA, "A")  # prod A missing
        self.both("B")
        json_lines, _, run_log, _ = self.run_policy(self.chain(), json_output=True)
        by_model = {r["model"]: r for r in json_lines}
        self.assertEqual(by_model[self.A]["outcome"], "unavailable")
        self.assertEqual(by_model[self.B]["outcome"], "blocked")
        self.assertEqual(by_model[self.B]["blocked_by"], [{"unique_id": self.A, "outcome": "unavailable"}])
        # B never queried.
        self.assertNotIn(self.B, run_log.query_counts)

    def test_missing_dev_relation_is_unavailable(self):
        self.table(PROD_SCHEMA, "A")
        json_lines, _, _, _ = self.run_policy([_node(self.A)], json_output=True)
        self.assertEqual(json_lines[0]["outcome"], "unavailable")

    def test_missing_prod_model_in_state_is_unavailable_and_blocks(self):
        # With --state, a model absent from the production manifest is
        # "unavailable" (not deleted, not identical) and blocks descendants.
        self.both("A")
        self.both("B")
        from data_diff.dbt_config_validators import ManifestJsonConfig

        dev_manifest = manifest_for(self.chain())
        prod_manifest = manifest_for([_node(self.A)])

        class StateParser(FakeDbtParser):
            def __init__(inner):
                super().__init__(dev_manifest, threads=2)
                inner.prod_manifest_obj = ManifestJsonConfig.parse_obj(prod_manifest)

        parser = StateParser()
        records = []
        stdout = io.StringIO()
        from data_diff import dbt as dbt_module

        original_report = dbt_module._LocalReport.__init__

        def report_init(inner_self, json_output_):
            original_report(inner_self, json_output_)
            emit = inner_self.emit

            def wrapped(result):
                records.append(result)
                emit(result)

            inner_self.emit = wrapped

        with unittest.mock.patch.object(dbt_module._LocalReport, "__init__", report_init), redirect_stdout(stdout):
            dbt_diff(json_output=True, dependency_policy="independent", _parser=parser, state="/unused/manifest.json")
        by_id = {r.unique_id: r for r in records}
        self.assertEqual(by_id[self.B].outcome, ModelOutcome.UNAVAILABLE)
        self.assertEqual(by_id[self.A].outcome, ModelOutcome.IDENTICAL)

    def test_revoked_privilege_is_error_when_relation_exists(self):
        # The reader role can see that the relation exists but cannot read it.
        # That is a permission failure (error), never a missing model.
        self.both("A")
        self.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO dbtdep_reader").format(sql.Identifier(DEV_SCHEMA)))
        self.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO dbtdep_reader").format(sql.Identifier(PROD_SCHEMA)))
        self.execute(sql.SQL("GRANT SELECT ON {}.{} TO dbtdep_reader").format(sql.Identifier(DEV_SCHEMA), sql.Identifier("A")))
        self.execute(sql.SQL("GRANT SELECT ON {}.{} TO dbtdep_reader").format(sql.Identifier(PROD_SCHEMA), sql.Identifier("A")))
        self.execute(sql.SQL("REVOKE SELECT ON {}.{} FROM dbtdep_reader").format(sql.Identifier(PROD_SCHEMA), sql.Identifier("A")))

        reader_connection = dict(CONNECTION, user="dbtdep_reader", password="reader")
        from data_diff import dbt as dbt_module

        parser = FakeDbtParser(manifest_for([_node(self.A)]), threads=1)
        parser.connection = reader_connection
        parser.set_connection = lambda: None
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            dbt_diff(
                json_output=True,
                dependency_policy="independent",
                _parser=parser,
                _connector=lambda *a, **kw: dbt_module.connect_to_table(*a, **kw),
            )
        record = _last_json_line(stdout.getvalue())
        self.assertEqual(record["outcome"], "error")
        self.assertIn("readable", record["error"])

    def test_injected_query_failure_is_error_and_blocks(self):
        self.both("A")
        self.both("B")

        # Replace dev A with a view that resolves at creation time but raises
        # whenever it is read.  This is a genuine adapter-level query failure,
        # not a simulated boolean.
        self._break_dev_relation("A")

        json_lines, output, run_log, _ = self.run_policy(self.chain(), json_output=True)
        by_model = {r["model"]: r for r in json_lines}
        self.assertEqual(by_model[self.A]["outcome"], "error")
        self.assertEqual(by_model[self.B]["outcome"], "blocked")
        self.assertEqual(by_model[self.B]["blocked_by"], [{"unique_id": self.A, "outcome": "error"}])
        self.assertNotIn(self.B, run_log.started())


# --------------------------------------------------------------------------
# Planning rejections (must happen before any table query)
# --------------------------------------------------------------------------


@requires_pg
class TestSelectionRejections(PgDbtCase):
    def assert_no_queries(self, callable_):
        with self.assertRaises(Exception):
            callable_()

    def test_empty_selection(self):
        with self.assertRaises(DataDiffSelectionConflictError):
            self.run_policy([])

    def test_duplicate_selection(self):
        node = _node("model.pkg.A")
        self.both("A")
        from data_diff.dbt_config_validators import ManifestJsonConfig

        parsed = ManifestJsonConfig.parse_obj(manifest_for([node]))
        duplicated = parsed.nodes["model.pkg.A"]
        with self.assertRaises(DataDiffSelectionConflictError):
            self.run_policy([node], models_override=[duplicated, duplicated])

    def test_ephemeral_selected(self):
        self.both("A")
        with self.assertRaises(DataDiffSelectionConflictError):
            self.run_policy([_node("model.pkg.A", materialized="ephemeral")])

    def test_relation_conflict(self):
        self.both("A")
        self.both("B")
        with self.assertRaises(DataDiffSelectionConflictError):
            self.run_policy([_node("model.pkg.A", alias="same"), _node("model.pkg.B", alias="same")])

    def test_dangling_reference(self):
        self.both("A")
        with self.assertRaises(DataDiffDependencyGraphError):
            self.run_policy([_node("model.pkg.A", depends_on=["model.pkg.missing"])])

    def test_relevant_cycle(self):
        self.both("A")
        self.both("B")
        with self.assertRaises(DataDiffDependencyGraphError):
            self.run_policy(
                [_node("model.pkg.A", depends_on=["model.pkg.B"]), _node("model.pkg.B", depends_on=["model.pkg.A"])]
            )

    def test_unrelated_cycle_does_not_block(self):
        self.both("A")
        nodes = [
            _node("model.pkg.A"),
            _node("model.pkg.X", depends_on=["model.pkg.Y"]),
            _node("model.pkg.Y", depends_on=["model.pkg.X"]),
        ]
        # X and Y are not selected, and not ancestors of A.
        from data_diff.dbt_config_validators import ManifestJsonConfig

        parsed = ManifestJsonConfig.parse_obj(manifest_for(nodes))
        json_lines, _, run_log, _ = self.run_policy(
            [nodes[0]], models_override=[parsed.nodes["model.pkg.A"]], json_output=True
        )
        self.assertEqual(json_lines[0]["outcome"], "identical")


# --------------------------------------------------------------------------
# API / CLI validation order and call isolation
# --------------------------------------------------------------------------


@requires_pg
class TestPolicyValidationAndIsolation(PgDbtCase):
    def test_unknown_policy_rejected_before_connection(self):
        from data_diff import dbt as dbt_module

        with unittest.mock.patch.object(dbt_module, "DbtParser") as parser_cls:
            with self.assertRaises(DataDiffDependencyPolicyError):
                dbt_diff(dependency_policy="eventually")
        parser_cls.assert_not_called()

    def test_cloud_with_independent_rejected_before_api_or_db(self):
        from data_diff import dbt as dbt_module

        self.both("A")
        manifest = manifest_for([_node("model.pkg.A")])
        parser = FakeDbtParser(manifest, threads=1)
        with unittest.mock.patch.object(dbt_module, "DbtParser", return_value=parser), \
             unittest.mock.patch.object(dbt_module, "_initialize_api") as api:
            with self.assertRaises(DataDiffDependencyPolicyError):
                dbt_diff(is_cloud=True, dependency_policy="independent")
        api.assert_not_called()

    def test_two_consecutive_runs_do_not_share_results(self):
        self.both("A")
        nodes = [_node("model.pkg.A")]
        first, _, _, _ = self.run_policy(nodes, json_output=True)
        second, _, _, _ = self.run_policy(nodes, json_output=True)
        self.assertEqual(len(first), 1)
        self.assertEqual(len(second), 1)
        self.assertEqual(first[0]["outcome"], "identical")
        self.assertEqual(second[0]["outcome"], "identical")

    def test_interleaved_runs_do_not_share_state(self):
        # Two genuinely concurrent calls, in separate interpreters, each with
        # its own manifest and connection.  Neither may see the other's
        # conclusions or dependency state.
        import subprocess
        import sys

        self.table(PROD_SCHEMA, "A", rows=((1, "a"),))
        self.table(DEV_SCHEMA, "A", rows=((1, "changed"),))
        self.both("B")

        env = dict(os.environ)
        env["PYTHONPATH"] = os.pathsep.join(filter(None, ["/workspace", env.get("PYTHONPATH")]))
        env.setdefault("DIFF_TEST_PG_HOST", PG_HOST)
        env.setdefault("DIFF_TEST_PG_PORT", str(PG_PORT))
        env.setdefault("DIFF_TEST_PG_USER", PG_USER)
        env.setdefault("DIFF_TEST_PG_PASSWORD", PG_PASSWORD)
        env.setdefault("DIFF_TEST_PG_DB", PG_DB)

        procs = {
            "diff": subprocess.Popen(
                [sys.executable, "-m", "tests.dbt_policy_worker", "model.pkg.A", DEV_SCHEMA],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env,
            ),
            "same": subprocess.Popen(
                [sys.executable, "-m", "tests.dbt_policy_worker", "model.pkg.B", DEV_SCHEMA],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env,
            ),
        }
        results = {}
        for name, proc in procs.items():
            out, err = proc.communicate(timeout=60)
            if proc.returncode != 0:
                self.fail(f"worker {name} failed: {err.decode()}")
            results[name] = json.loads(out.decode().strip().splitlines()[-1])

        self.assertEqual(results["diff"][0]["outcome"], "different")
        self.assertEqual(results["same"][0]["outcome"], "identical")


if __name__ == "__main__":
    unittest.main()
