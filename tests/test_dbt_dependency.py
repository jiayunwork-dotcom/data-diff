"""Unit tests for dependency planning and scheduling (no database involved).

The manifests here are hand-written, and every comparison is a controlled local
task, so completion order and query counts are fully determined by the test.
"""

import threading
import time
import unittest
from dataclasses import dataclass

from data_diff.dbt_dependency import (
    DependencyPlan,
    DependencyPolicy,
    DependencyScheduler,
    ModelOutcome,
    ModelResult,
    build_dependency_plan,
    failure_result,
)
from data_diff.errors import (
    DataDiffDependencyGraphError,
    DataDiffDependencyPolicyError,
    DataDiffSelectionConflictError,
)


@dataclass
class FakeConfig:
    database: str = "db"
    schema_: str = "dev"
    materialized: str = "table"


@dataclass
class FakeNode:
    unique_id: str
    depends_on: list
    name: str = ""
    alias: str = ""
    database: str = "db"
    schema_: str = "dev"
    materialized: str = "table"

    def __post_init__(self):
        if not self.name:
            self.name = self.unique_id.split(".")[-1]
        if not self.alias:
            self.alias = self.name

    @property
    def config(self):
        return FakeConfig(database=self.database, schema_=self.schema_, materialized=self.materialized)


class FakeDependsOn:
    def __init__(self, nodes):
        self.nodes = nodes
        self.macros = []


class FakeManifest:
    def __init__(self, nodes):
        self.nodes = {node.unique_id: _with_depends(node) for node in nodes}


def _with_depends(node):
    if isinstance(node.depends_on, FakeDependsOn):
        return node
    node.depends_on = FakeDependsOn(list(node.depends_on))
    return node


def model(name, depends_on=(), *, package="pkg", database="db", schema="dev", alias=None, materialized="table"):
    return FakeNode(
        unique_id=f"model.{package}.{name}",
        depends_on=list(depends_on),
        database=database,
        schema_=schema,
        alias=alias or name,
        materialized=materialized,
    )


def uid(name, package="pkg"):
    return f"model.{package}.{name}"


# -- controlled run_model implementations -----------------------------------


class RecordingRunner:
    """A run_model that records start/finish, query counts and audit markers."""

    def __init__(self, outcomes, events=None, lock=None, barriers=None):
        self.outcomes = outcomes
        self.events = events if events is not None else []
        self.lock = lock or threading.Lock()
        self.barriers = barriers or {}
        self.active = 0
        self.max_active = 0

    def __call__(self, unique_id):
        self._record("start", unique_id)
        barrier = self.barriers.get(unique_id)
        with self.lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        try:
            if barrier is not None:
                barrier(unique_id)
            outcome = self.outcomes[unique_id]
            result = _result(unique_id, outcome)
        finally:
            with self.lock:
                self.active -= 1
            self._record("finish", unique_id)
        return result

    def _record(self, name, unique_id):
        with self.lock:
            self.events.append((name, unique_id))


def _result(unique_id, outcome):
    outcome = ModelOutcome(outcome)
    return ModelResult(
        unique_id=unique_id,
        outcome=outcome,
        dev_path=("db", "dev", unique_id.split(".")[-1]),
        prod_path=("db", "prod", unique_id.split(".")[-1]),
        text=f"{unique_id}: {outcome.value}",
        json_record={"model": unique_id, "outcome": outcome.value},
    )


class SchedulerTestBase(unittest.TestCase):
    def plan(self, nodes, selected_names):
        manifest = FakeManifest(nodes)
        selected = [manifest.nodes[uid(n)] for n in selected_names]
        return build_dependency_plan(selected, manifest)


class TestPolicyParsing(SchedulerTestBase):
    def test_default_is_require_identical(self):
        self.assertIs(DependencyPolicy.parse(None), DependencyPolicy.REQUIRE_IDENTICAL)

    def test_accepts_enum_and_string(self):
        self.assertIs(DependencyPolicy.parse("independent"), DependencyPolicy.INDEPENDENT)
        self.assertIs(
            DependencyPolicy.parse(DependencyPolicy.INDEPENDENT), DependencyPolicy.INDEPENDENT
        )

    def test_rejects_unknown(self):
        with self.assertRaises(DataDiffDependencyPolicyError):
            DependencyPolicy.parse("eventually")


class TestPlanBuilding(SchedulerTestBase):
    def test_diamond_and_unrelated_model(self):
        nodes = [model("A"), model("B"), model("C", [uid("A"), uid("B")]), model("D")]
        plan = self.plan(nodes, ["A", "B", "C", "D"])
        self.assertEqual(plan.ancestors_of(uid("A")), frozenset())
        self.assertEqual(plan.ancestors_of(uid("C")), frozenset({uid("A"), uid("B")}))
        self.assertEqual(plan.ancestors_of(uid("D")), frozenset())

    def test_constraints_pass_through_unselected_and_ephemeral_intermediates(self):
        nodes = [
            model("A"),
            model("X", [uid("A")], materialized="view"),
            model("E", [uid("X")], materialized="ephemeral"),
            model("C", [uid("E")]),
        ]
        plan = self.plan(nodes, ["A", "C"])
        self.assertEqual(plan.ancestors_of(uid("C")), frozenset({uid("A")}))
        # Intermediates are not part of the check set at all.
        self.assertEqual(plan.selected, (uid("A"), uid("C")))

    def test_sources_and_tests_are_not_gates(self):
        nodes = [
            model("A"),
            # A model that depends on a source and on a test-ish node.
            model("C", [uid("A"), "source.pkg.raw", "test.pkg.some_test"]),
        ]
        plan = self.plan(nodes, ["A", "C"])
        self.assertEqual(plan.ancestors_of(uid("C")), frozenset({uid("A")}))

    def test_same_name_in_two_packages_is_two_models(self):
        nodes = [
            model("orders", package="one", database="db", schema="dev"),
            model("orders", package="two", database="db", schema="dev2"),
        ]
        manifest = FakeManifest(nodes)
        selected = [manifest.nodes[uid("orders", "one")], manifest.nodes[uid("orders", "two")]]
        plan = build_dependency_plan(selected, manifest)
        self.assertEqual(plan.selected, (uid("orders", "one"), uid("orders", "two")))

    def test_empty_selection_rejected(self):
        with self.assertRaises(DataDiffSelectionConflictError):
            build_dependency_plan([], FakeManifest([]))

    def test_duplicate_selection_rejected(self):
        nodes = [model("A")]
        manifest = FakeManifest(nodes)
        node = manifest.nodes[uid("A")]
        with self.assertRaises(DataDiffSelectionConflictError):
            build_dependency_plan([node, node], manifest)

    def test_ephemeral_selected_rejected(self):
        nodes = [model("A", materialized="ephemeral")]
        with self.assertRaises(DataDiffSelectionConflictError):
            self.plan(nodes, ["A"])

    def test_two_selected_models_same_relation_rejected(self):
        nodes = [
            model("a", alias="same"),
            model("b", alias="same"),
        ]
        with self.assertRaises(DataDiffSelectionConflictError):
            self.plan(nodes, ["a", "b"])

    def test_dangling_reference_in_relevant_subgraph(self):
        nodes = [model("A"), model("C", [uid("A"), uid("missing")])]
        with self.assertRaises(DataDiffDependencyGraphError) as ctx:
            self.plan(nodes, ["C"])
        self.assertIn(uid("missing"), str(ctx.exception))

    def test_unrelated_bad_graph_does_not_block(self):
        # B -> missing, but B is not selected and not an ancestor of the selection.
        nodes = [model("A"), model("B", [uid("missing")]), model("C", [uid("A")])]
        plan = self.plan(nodes, ["A", "C"])
        self.assertEqual(plan.selected, (uid("A"), uid("C")))

    def test_cycle_in_relevant_subgraph(self):
        nodes = [model("A", [uid("B")]), model("B", [uid("A")])]
        with self.assertRaises(DataDiffDependencyGraphError) as ctx:
            self.plan(nodes, ["A"])
        self.assertIn(uid("A"), str(ctx.exception))
        self.assertIn(uid("B"), str(ctx.exception))

    def test_unrelated_cycle_does_not_block(self):
        nodes = [
            model("A"),
            model("C", [uid("A")]),
            model("X", [uid("Y")]),
            model("Y", [uid("X")]),
        ]
        plan = self.plan(nodes, ["A", "C"])
        self.assertEqual(plan.ancestors_of(uid("C")), frozenset({uid("A")}))


class TestScheduling(SchedulerTestBase):
    def test_diamond_blocked_reports_both_ancestors_in_either_order(self):
        nodes = [model("A"), model("B"), model("C", [uid("A"), uid("B")]), model("D")]
        events = []
        runner = RecordingRunner(
            {uid("A"): "different", uid("B"): "error", uid("C"): "identical", uid("D"): "identical"},
            events=events,
        )
        plan = self.plan(nodes, ["A", "B", "C", "D"])
        results = DependencyScheduler(plan, runner, workers=2).run()

        self.assertEqual(results[uid("C")].outcome, ModelOutcome.BLOCKED)
        self.assertEqual(
            results[uid("C")].blocked_by,
            ((uid("A"), ModelOutcome.DIFFERENT), (uid("B"), ModelOutcome.ERROR)),
        )
        # C is never started, only the two ancestors and the independent D run.
        starts = {name for event, name in events if event == "start"}
        self.assertEqual(starts, {uid("A"), uid("B"), uid("D")})
        self.assertEqual(results[uid("D")].outcome, ModelOutcome.IDENTICAL)

    def test_downstream_waits_for_all_selected_ancestors(self):
        nodes = [model("A"), model("B"), model("C", [uid("A"), uid("B")])]
        events = []
        runner = RecordingRunner(
            {uid("A"): "identical", uid("B"): "identical", uid("C"): "identical"}, events=events
        )
        plan = self.plan(nodes, ["A", "B", "C"])
        DependencyScheduler(plan, runner, workers=2).run()
        index = {(event, name): i for i, (event, name) in enumerate(events)}
        self.assertLess(index[("finish", uid("A"))], index[("start", uid("C"))])
        self.assertLess(index[("finish", uid("B"))], index[("start", uid("C"))])

    def test_blocked_does_not_use_worker_and_reason_excludes_blocked_intermediates(self):
        # A (different) -> B (selected) -> C (selected) via A and B.
        nodes = [model("A"), model("B", [uid("A")]), model("C", [uid("A"), uid("B")])]
        events = []
        runner = RecordingRunner(
            {uid("A"): "different", uid("B"): "identical", uid("C"): "identical"}, events=events
        )
        plan = self.plan(nodes, ["A", "B", "C"])
        results = DependencyScheduler(plan, runner, workers=1).run()

        self.assertEqual(results[uid("B")].outcome, ModelOutcome.BLOCKED)
        self.assertEqual(results[uid("C")].outcome, ModelOutcome.BLOCKED)
        # Only the root cause A is listed, never the blocked intermediate B.
        self.assertEqual(results[uid("C")].blocked_by, ((uid("A"), ModelOutcome.DIFFERENT),))
        # Blocked models never started.
        starts = {name for event, name in events if event == "start"}
        self.assertEqual(starts, {uid("A")})

    def test_independent_models_overlap_with_two_threads(self):
        nodes = [model("A"), model("B")]
        overlap = threading.Barrier(2, timeout=5)
        in_flight = {"peak": 0, "now": 0}
        lock = threading.Lock()

        def run(unique_id):
            with lock:
                in_flight["now"] += 1
                in_flight["peak"] = max(in_flight["peak"], in_flight["now"])
            overlap.wait()
            with lock:
                in_flight["now"] -= 1
            return _result(unique_id, "identical")

        plan = self.plan(nodes, ["A", "B"])
        results = DependencyScheduler(plan, run, workers=2).run()
        self.assertEqual(results[uid("A")].outcome, ModelOutcome.IDENTICAL)
        self.assertEqual(results[uid("B")].outcome, ModelOutcome.IDENTICAL)
        self.assertEqual(in_flight["peak"], 2)

    def test_model_waiting_on_ancestor_does_not_hold_a_worker(self):
        # A is slow; C depends on A.  D is independent.  With workers=1 D must
        # still run while C is waiting on A.
        nodes = [model("A"), model("C", [uid("A")]), model("D")]
        gate = threading.Event()
        started = []

        def run(unique_id):
            started.append(unique_id)
            if unique_id == uid("A"):
                gate.set()
                time.sleep(0.2)
            return _result(unique_id, "identical")

        plan = self.plan(nodes, ["A", "C", "D"])
        results = DependencyScheduler(plan, run, workers=1).run()
        self.assertEqual({r.outcome for r in results.values()}, {ModelOutcome.IDENTICAL})
        self.assertLess(started.index(uid("A")), started.index(uid("C")))
        self.assertLess(started.index(uid("D")), started.index(uid("C")))

    def test_release_is_not_layer_by_layer(self):
        # A (fast) and B (slow) are independent; C depends on A only.
        # C must start before B finishes.
        nodes = [model("A"), model("B"), model("C", [uid("A")])]
        events = []
        b_gate = threading.Event()

        def run(unique_id):
            events.append(("start", unique_id))
            if unique_id == uid("B"):
                b_gate.wait(5)
            events.append(("finish", unique_id))
            return _result(unique_id, "identical")

        plan = self.plan(nodes, ["A", "B", "C"])
        DependencyScheduler(plan, run, workers=2).run()
        index = {e: i for i, e in enumerate(events)}
        self.assertLess(index[("finish", uid("A"))], index[("start", uid("C"))])
        self.assertLess(index[("start", uid("C"))], index[("finish", uid("B"))])

    def test_preflight_unavailable_blocks_descendants(self):
        nodes = [model("A"), model("B", [uid("A")])]
        events = []
        runner = RecordingRunner({uid("B"): "identical"}, events=events)
        plan = self.plan(nodes, ["A", "B"])
        preflight = {
            uid("A"): failure_result(uid("A"), (), error="no pk", outcome=ModelOutcome.UNAVAILABLE)
        }
        results = DependencyScheduler(plan, runner, workers=1, preflight=preflight).run()
        self.assertEqual(results[uid("A")].outcome, ModelOutcome.UNAVAILABLE)
        self.assertEqual(results[uid("B")].outcome, ModelOutcome.BLOCKED)
        self.assertEqual(results[uid("B")].blocked_by, ((uid("A"), ModelOutcome.UNAVAILABLE),))
        self.assertEqual([e for e in events if e[0] == "start"], [])

    def test_one_result_per_model(self):
        nodes = [model("A"), model("B", [uid("A")]), model("C", [uid("A"), uid("B")])]
        runner = RecordingRunner({uid("A"): "different", uid("B"): "identical", uid("C"): "different"})
        plan = self.plan(nodes, ["A", "B", "C"])
        results = DependencyScheduler(plan, runner, workers=2).run()
        self.assertEqual(set(results), {uid("A"), uid("B"), uid("C")})
        self.assertEqual(len(results), 3)

    def test_keyboard_interrupt_stops_new_work(self):
        nodes = [model("A"), model("B"), model("C", [uid("A")]), model("D")]
        started = []
        lock = threading.Lock()

        def run(unique_id):
            with lock:
                started.append(unique_id)
            if unique_id == uid("A"):
                raise KeyboardInterrupt()
            return _result(unique_id, "identical")

        plan = self.plan(nodes, ["A", "C", "D"])  # A -> C; D independent
        with self.assertRaises(KeyboardInterrupt):
            DependencyScheduler(plan, run, workers=1).run()
        # C depends on A, which never settled, so C must not run.
        self.assertNotIn(uid("C"), started)

    def test_interleaved_calls_do_not_share_state(self):
        nodes = [model("A"), model("B", [uid("A")])]
        plan1 = self.plan(nodes, ["A", "B"])
        plan2 = self.plan(nodes, ["A", "B"])

        runner1 = RecordingRunner({uid("A"): "different", uid("B"): "identical"})
        runner2 = RecordingRunner({uid("A"): "identical", uid("B"): "identical"})
        results1 = DependencyScheduler(plan1, runner1, workers=2).run()
        results2 = DependencyScheduler(plan2, runner2, workers=2).run()
        self.assertEqual(results1[uid("B")].outcome, ModelOutcome.BLOCKED)
        self.assertEqual(results2[uid("B")].outcome, ModelOutcome.IDENTICAL)

    def test_emit_sees_each_result_once(self):
        nodes = [model("A"), model("B")]
        seen = []
        plan = self.plan(nodes, ["A", "B"])
        runner = RecordingRunner({uid("A"): "identical", uid("B"): "identical"})
        DependencyScheduler(plan, runner, workers=2, emit=seen.append).run()
        self.assertEqual(sorted(r.unique_id for r in seen), [uid("A"), uid("B")])

    def test_blocked_settles_while_all_workers_are_busy(self):
        # A (different) and B (blocked on A) plus a long independent C.
        # With one worker, B must be reported while C is still running, and
        # must never occupy that worker itself.
        nodes = [model("A"), model("B", [uid("A")]), model("C")]
        events = []
        c_started = threading.Event()
        c_gate = threading.Event()
        results = {}

        def run(unique_id):
            events.append(("start", unique_id))
            if unique_id == uid("C"):
                c_started.set()
                c_gate.wait(5)
            events.append(("finish", unique_id))
            return _result(unique_id, "identical" if unique_id == uid("C") else "different")

        plan = self.plan(nodes, ["A", "B", "C"])

        def emit(result):
            results[result.unique_id] = result

        scheduler = DependencyScheduler(plan, run, workers=1, emit=emit)
        thread = threading.Thread(target=lambda: results.update({"__done__": scheduler.run()}))
        thread.start()
        # Wait until C is the model holding the only worker.
        self.assertTrue(c_started.wait(5))
        # B depends only on A, which already settled; it must appear immediately
        # without waiting for C.
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and uid("B") not in results:
            time.sleep(0.005)
        self.assertIn(uid("B"), results)
        self.assertEqual(results[uid("B")].outcome, ModelOutcome.BLOCKED)
        c_gate.set()
        thread.join(5)
        self.assertFalse(thread.is_alive())
        # B was never handed to the worker.
        self.assertNotIn(uid("B"), [name for event, name in events if event == "start"])


if __name__ == "__main__":
    unittest.main()
