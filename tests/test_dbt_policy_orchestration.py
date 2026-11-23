"""Orchestration tests for the local dependency policy (no real database).

These stub the comparison itself with ``_runner_factory`` so the scheduler,
reporting, and validation order are exercised in isolation.  Real JOINDIFF
behaviour is covered in ``test_dbt_dependency_pg.py``.
"""

import io
import json
import threading
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from data_diff import tracking
from data_diff.dbt import dbt_diff
from data_diff.dbt_dependency import ModelOutcome, ModelResult
from data_diff.errors import (
    DataDiffDependencyGraphError,
    DataDiffDependencyPolicyError,
    DataDiffSelectionConflictError,
)

from tests.test_dbt_dependency_pg import (
    FakeDbtParser,
    _node,
    manifest_for,
    PROD_SCHEMA,
    PG_DB,
)

tracking.disable_tracking()


def _stub_result(unique_id, outcome, error=None):
    outcome = ModelOutcome(outcome)
    return ModelResult(
        unique_id=unique_id,
        outcome=outcome,
        dev_path=("db", "dev", unique_id.split(".")[-1]),
        prod_path=("db", "prod", unique_id.split(".")[-1]),
        text=f"{unique_id}: {outcome.value}",
        json_record={"model": unique_id, "outcome": outcome.value},
        error=error,
    )


def _parser(nodes, *, threads=2, models_override=None, model_config=None):
    parser = FakeDbtParser(manifest_for(nodes), threads=threads, model_config=model_config)
    if models_override is not None:
        parser.get_models = lambda selection=None: list(models_override)
    return parser


class OrchestrationTest(unittest.TestCase):
    def run_with_stub(self, nodes, outcomes, *, policy="independent", parser=None, **kwargs):
        calls = []
        lock = threading.Lock()

        def factory(_compare_one):
            def runner(unique_id):
                with lock:
                    calls.append(unique_id)
                return _stub_result(unique_id, outcomes[unique_id])

            return runner

        parser = parser or _parser(nodes)
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            dbt_diff(
                dependency_policy=policy,
                json_output=True,
                _parser=parser,
                _runner_factory=factory,
                **kwargs,
            )
        records = [json.loads(line) for line in stdout.getvalue().splitlines() if line.startswith("{")]
        return records, calls

    def test_diamond_blocks_with_both_reasons(self):
        A, B, C, D = ("model.pkg.A", "model.pkg.B", "model.pkg.C", "model.pkg.D")
        nodes = [_node(A), _node(B), _node(C, depends_on=[A, B]), _node(D)]
        records, calls = self.run_with_stub(
            nodes, {A: "different", B: "error", C: "identical", D: "identical"}
        )
        by_model = {r["model"]: r for r in records}
        self.assertEqual(by_model[C]["outcome"], "blocked")
        self.assertEqual(
            by_model[C]["blocked_by"],
            [{"unique_id": A, "outcome": "different"}, {"unique_id": B, "outcome": "error"}],
        )
        self.assertNotIn(C, calls)
        self.assertEqual(by_model[D]["outcome"], "identical")
        self.assertIn(D, calls)

    def test_every_selected_model_gets_one_record(self):
        A, C = "model.pkg.A", "model.pkg.C"
        nodes = [_node(A), _node(C, depends_on=[A])]
        records, _ = self.run_with_stub(nodes, {A: "unavailable", C: "identical"})
        self.assertEqual(sorted(r["model"] for r in records), sorted([A, C]))

    def test_invalid_policy_rejected_before_parser(self):
        with patch("data_diff.dbt.DbtParser") as parser_cls:
            with self.assertRaises(DataDiffDependencyPolicyError):
                dbt_diff(dependency_policy="bogus")
        parser_cls.assert_not_called()

    def test_cloud_with_independent_rejected_before_api(self):
        nodes = [_node("model.pkg.A")]
        parser = _parser(nodes)
        with patch("data_diff.dbt._initialize_api") as api:
            with self.assertRaises(DataDiffDependencyPolicyError):
                dbt_diff(dependency_policy="independent", is_cloud=True, _parser=parser)
        api.assert_not_called()

    def test_require_identical_keeps_legacy_round_trip(self):
        # Default policy must still defer to the historical executor path.
        nodes = [_node("model.pkg.A")]
        parser = _parser(nodes)
        with patch("data_diff.dbt._local_diff") as legacy:
            with redirect_stdout(io.StringIO()):
                dbt_diff(_parser=parser)
        legacy.assert_called_once()

    def test_selection_errors_surface(self):
        A = "model.pkg.A"
        parser = _parser([_node(A)])
        with self.assertRaises(DataDiffDependencyGraphError):
            dbt_diff(
                dependency_policy="independent",
                _parser=_parser([_node(A, depends_on=["model.pkg.missing"])]),
            )
        with self.assertRaises(DataDiffSelectionConflictError):
            dbt_diff(dependency_policy="independent", _parser=_parser([]))


if __name__ == "__main__":
    unittest.main()
