# Dependency-aware local dbt checks (`dependency_policy="independent"`)

## Public interface

### Python

```python
from data_diff.dbt import dbt_diff

dbt_diff(dependency_policy="independent", json_output=True)
```

`dbt_diff(..., dependency_policy=...)` accepts:

| value | behaviour |
| --- | --- |
| `None` or `"require_identical"` (default) | Historical behaviour: every selected model is submitted to the thread pool at once. Nothing about selection or execution changes. |
| `"independent"` | Local-only dependency-aware execution (described below). |
| anything else | `DataDiffDependencyPolicyError`, raised before the dbt parser, the database, or the cloud API is touched. |

`"independent"` combined with `is_cloud=True` raises `DataDiffDependencyPolicyError`
**before** any API request or database connection.

### CLI

```bash
data-diff --dbt --dependency-policy independent
```

`--dependency-policy` is a plain CLI option; it is only meaningful with `--dbt`.
Omitting it keeps the existing behaviour.

### Semantics of `independent`

* The selected model set is **unchanged**; models outside the selection are never executed.
* Relationships come from the development manifest's `depends_on.nodes`. Identity is always
  `unique_id`; names, aliases and relation paths are never used to merge two models.
* A model is compared only after **every selected ancestor** has settled with `identical`.
  Unselected models and `ephemeral` models in between only pass the constraint through; they are
  never compared and never produce a record.
* `source` and `test` nodes are not gates.
* A model whose selected ancestors are not all `identical` is `blocked` and its relations are
  never queried. The record lists every non-identical selected ancestor (direct or transitive),
  sorted by `unique_id`, deduplicated; blocked intermediates are never listed as root causes.
* Independent models start as soon as a worker is free. Waiting on an ancestor never occupies a
  worker, and readiness is released per completion, not per dependency layer.

### Validation (all before the first table query)

* empty selection, duplicate `unique_id`, a selected `ephemeral` model, a selected model absent
  from the manifest, or two selected models mapping to the same development-side physical
  relation → `DataDiffSelectionConflictError`
* a model reference that is missing from the manifest, or a cycle in the ancestor subgraph
  reachable from the selection → `DataDiffDependencyGraphError` (message names the `unique_id`s)
* An unrelated broken node or cycle elsewhere in the manifest does **not** block the run.
* Same-named models in different packages are distinct models, because identity is `unique_id`.

### Outcomes

Every selected model gets exactly one terminal record:

| outcome | meaning |
| --- | --- |
| `identical` | Comparison completed; no row, column or type differences. The only outcome that releases descendants. |
| `different` | Comparison completed; rows differ, or columns were added/removed/type-changed. |
| `error` | Permission, connection or query failure. Never guessed as a new model or as identical. |
| `unavailable` | No primary key, no corresponding production model (`--state`), or the relation does not exist. |
| `blocked` | A selected ancestor was not `identical`; the model's relations were never queried. |

`error` and `unavailable` both block descendants. `blocked` is never a root cause.

## JSONL

Existing success records are unchanged; the only new key is `outcome` (also added to failed
records, which keep `status`/`error`). `blocked` and `unavailable` never pretend to be a
zero-diff `success`.

```
{"status": "success", "result": "different", "model": "model.pkg.A",
 "dataset1": ["postgres", "cli_prod", "A"], "dataset2": ["postgres", "cli_dev", "A"],
 "rows": {"exclusive": {"dataset1": [], "dataset2": []},
          "diff": [{"id": {"dataset1": "1", "dataset2": "1", "isDiff": false, "isPK": true},
                    "v": {"dataset1": "a", "dataset2": "b", "isDiff": true, "isPK": false}}]},
 "summary": {"rows": {"total": {"dataset1": 1, "dataset2": 1},
                      "exclusive": {"dataset1": 0, "dataset2": 0},
                      "updated": 1, "unchanged": 0},
             "stats": {"diffCounts": {"v": 1}}},
 "columns": {"dataset1": [{"name": "id", "type": "integer", "kind": "integer"},
                          {"name": "v", "type": "text", "kind": "unsupported"}],
             "dataset2": [{"name": "id", "type": "integer", "kind": "integer"},
                          {"name": "v", "type": "text", "kind": "unsupported"}],
             "primaryKey": ["id"],
             "exclusive": {"dataset1": [], "dataset2": []}, "typeChanged": []},
 "version": "1.1.0", "outcome": "different"}
{"version": "1.1.0", "status": "blocked", "outcome": "blocked", "model": "model.pkg.C",
 "dataset1": ["postgres", "cli_prod", "C"], "dataset2": ["postgres", "cli_dev", "C"],
 "blocked_by": [{"unique_id": "model.pkg.A", "outcome": "different"}]}
```

`unavailable` records carry `status: "unavailable"`, `outcome: "unavailable"` and an `error`
message (for example `"No primary key found..."`). `error` records carry `status: "failed"` and
`outcome: "error"`.

## Reproducing

### Controlled state machine (no database)

```bash
PYTHONPATH=/workspace python3 -m tests.dbt_dependency_driver
PYTHONPATH=/workspace python3 -m pytest tests/test_dbt_dependency.py tests/test_dbt_policy_orchestration.py -q
```

`tests/dbt_dependency_driver.py` runs the diamond example with both completion orders and prints
each model's outcome, the start list, and C's reason list; the two orders must agree.

### Real PostgreSQL 16 acceptance

Requires a reachable PostgreSQL 16 (defaults: `postgres@localhost:5432`, empty password; override
with `DIFF_TEST_PG_HOST`, `DIFF_TEST_PG_PORT`, `DIFF_TEST_PG_USER`, `DIFF_TEST_PG_PASSWORD`,
`DIFF_TEST_PG_DB`).

```bash
PYTHONPATH=/workspace python3 -m tests.acceptance_dependency_policy
PYTHONPATH=/workspace python3 -m pytest tests/test_dbt_dependency_pg.py -q
```

The acceptance driver creates `acc_dev`/`acc_prod` schemas, runs the diamond in both completion
orders, two independent chains at `threads=2`, data scenarios (identical, empty, empty-vs-rows,
column add/drop/type change), a missing production relation (→ `unavailable`), and a real
function-backed view that raises at query time (→ `error`). It records per-model real query counts
and asserts that blocked descendants never start and make no queries.

## Resource model / organisation

* `data_diff/dbt_dependency.py` holds the plan (`build_dependency_plan`) and the scheduler
  (`DependencyScheduler`). All state is per call; there are no module globals, so two runs (even
  interleaved) cannot share results or dependency state.
* The scheduler runs on the caller's thread and hands only ready-and-released models to a
  `ThreadPoolExecutor` sized by the dbt project's `threads`. Models waiting on ancestors, `blocked`
  models, and preflight conclusions never occupy a worker.
* Comparisons are emitted in completion order through a callback, and only a slim copy (no
  rendered rows) is retained for downstream decisions. Large JSONL row details are written once and
  never copied into a second structure.
* Existing relation lookup, primary-key discovery, column selection and the JOINDIFF comparison are
  reused unchanged. The new policy only decides *whether* to run a comparison and *how to classify*
  its result.

## Known limitations / not verified here

* `unavailable` vs `error` for an empty schema result depends on the adapter's
  `relation_exists()` hook. PostgreSQL uses `to_regclass` and distinguishes
  "missing" from "exists but unreadable". Adapters that cannot tell (the default returns `None`)
  report `error` rather than guessing.
* Only local comparisons are supported with `"independent"`; cloud diffs are rejected.
* The `test_integration_basic_dbt` DuckDB fixture and `test_sql.py`/`test_database_types.py`
  cross-database tests were not part of this change's target; the DuckDB integration test still
  passes, and the MySQL/other-adapter suites require services not available in this environment.
