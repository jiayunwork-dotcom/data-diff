"""Drives the dependency policy state machine with controlled tasks (no DB).

This is the acceptance harness for the ordering rules: it prints the emitted
records and the start/finish/query-count audit trail, so the report can be
checked item by item against the expected reason lists.
"""

import json
import sys
import threading

from data_diff.dbt_dependency import (
    DependencyScheduler,
    ModelOutcome,
    ModelResult,
    build_dependency_plan,
)
from data_diff.dbt_config_validators import ManifestJsonConfig

from tests.test_dbt_dependency_pg import _node, manifest_for


def make_plan(nodes, selected):
    parsed = ManifestJsonConfig.parse_obj(manifest_for(nodes))
    return build_dependency_plan([parsed.nodes[uid] for uid in selected], parsed)


def sentinel(unique_id, outcome):
    outcome = ModelOutcome(outcome)
    return ModelResult(
        unique_id=unique_id,
        outcome=outcome,
        dev_path=("db", "dev", unique_id.split(".")[-1]),
        prod_path=("db", "prod", unique_id.split(".")[-1]),
        text=f"{unique_id}: {outcome.value}",
        json_record={"model": unique_id, "outcome": outcome.value},
    )


def scenario(title, nodes, selected, outcomes, slow, workers=2):
    A, B, C, D = selected
    plan = make_plan(nodes, selected)
    log = []
    gate = threading.Event()
    lock = threading.Lock()

    def runner(unique_id):
        with lock:
            log.append(("start", unique_id))
        try:
            if unique_id == slow:
                gate.wait(5)
            else:
                gate.set()
            return sentinel(unique_id, outcomes[unique_id])
        finally:
            with lock:
                log.append(("finish", unique_id))

    results = DependencyScheduler(plan, runner, workers=workers).run()
    print(f"\n== {title} ==")
    for uid in selected:
        result = results[uid]
        reasons = ", ".join(f"{a}: {o.value}" for a, o in result.blocked_by)
        print(f"  {uid}: {result.outcome.value}" + (f"  [{reasons}]" if reasons else ""))
    starts = [uid for event, uid in log if event == "start"]
    print(f"  started: {starts}")
    c_reasons = results[C].blocked_by
    return c_reasons


def main():
    A, B, C, D = "model.pkg.A", "model.pkg.B", "model.pkg.C", "model.pkg.D"
    nodes = [_node(A), _node(B), _node(C, depends_on=[A, B]), _node(D)]
    outcomes = {A: "different", B: "error", C: "identical", D: "identical"}

    first = scenario("A settles first", nodes, [A, B, C, D], outcomes, slow=B)
    second = scenario("B settles first", nodes, [A, B, C, D], outcomes, slow=A)
    print("\nreasons identical in both orders:", first == second)
    print("reasons:", json.dumps([[a, o.value] for a, o in first]))
    return 0 if first == second else 1


if __name__ == "__main__":
    sys.exit(main())
