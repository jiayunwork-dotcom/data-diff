"""Dependency-aware planning and scheduling for local dbt model checks.

The local dbt mode used to submit every selected model to a thread pool at
once.  ``dependency_policy="independent"`` keeps the selected model set
unchanged but only releases a model for comparison once every *selected*
ancestor has settled with ``identical``.  A model whose ancestors are not all
identical is reported as ``blocked`` and never touches the database.

Everything in this module is per-call state: building a plan and running a plan
share no globals, so two runs (or two interleaved calls) cannot leak results or
dependency state into each other.
"""

from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Dict, List, Mapping, Optional, Sequence, Set, Tuple, Union

from data_diff.errors import (
    DataDiffDependencyGraphError,
    DataDiffDependencyPolicyError,
    DataDiffSelectionConflictError,
)


class DependencyPolicy(str, Enum):
    """How the selected models' dependency constraints are applied."""

    REQUIRE_IDENTICAL = "require_identical"
    INDEPENDENT = "independent"

    @classmethod
    def parse(cls, value: Union["DependencyPolicy", str, None]) -> "DependencyPolicy":
        """Validate a policy value, raising before any database is contacted."""
        if value is None:
            return cls.REQUIRE_IDENTICAL
        if isinstance(value, cls):
            return value
        try:
            return cls(value)
        except ValueError:
            valid = ", ".join(repr(p.value) for p in cls)
            raise DataDiffDependencyPolicyError(
                f"Unknown dependency_policy {value!r}; expected one of {valid}."
            ) from None


class ModelOutcome(str, Enum):
    """The terminal conclusion of comparing one selected model."""

    IDENTICAL = "identical"
    DIFFERENT = "different"
    ERROR = "error"
    UNAVAILABLE = "unavailable"
    BLOCKED = "blocked"


# Outcomes that must not release a downstream model.  ``blocked`` is not
# included: blocked nodes are never root causes and therefore never appear in
# a downstream model's reason list.
NON_IDENTICAL_OUTCOMES = frozenset(
    {ModelOutcome.DIFFERENT, ModelOutcome.ERROR, ModelOutcome.UNAVAILABLE}
)


@dataclass(frozen=True)
class ModelResult:
    """One terminal record for one selected model.

    ``text`` is the human-readable report, ``json_record`` the JSONL record.
    ``blocked_by`` is a unique_id-sorted tuple of (ancestor unique_id, outcome)
    pairs, containing only selected ancestors (direct or transitive) that ended
    ``different``, ``error`` or ``unavailable``.
    """

    unique_id: str
    outcome: ModelOutcome
    dev_path: Tuple[str, ...] = ()
    prod_path: Tuple[str, ...] = ()
    text: str = ""
    json_record: Optional[dict] = None
    error: Optional[str] = None
    blocked_by: Tuple[Tuple[str, ModelOutcome], ...] = ()

    @property
    def releases_descendants(self) -> bool:
        return self.outcome is ModelOutcome.IDENTICAL

    def slim(self) -> "ModelResult":
        """Drop the (potentially large) rendered payload, keep the conclusion."""
        return ModelResult(
            unique_id=self.unique_id,
            outcome=self.outcome,
            dev_path=self.dev_path,
            prod_path=self.prod_path,
            error=self.error,
            blocked_by=self.blocked_by,
        )


@dataclass(frozen=True)
class DependencyPlan:
    """The selected models and their *selected-ancestor* constraints."""

    selected: Tuple[str, ...]
    ancestors: Mapping[str, frozenset]

    def ancestors_of(self, unique_id: str) -> frozenset:
        return self.ancestors.get(unique_id, frozenset())


def _model_dependencies(node) -> List[str]:
    """Return the model unique_ids a manifest node depends on.

    Only ``model.*`` references are model dependencies.  Sources, tests, seeds
    and other resources are not gates: they are ignored and not traversed.
    """
    depends_on = getattr(node, "depends_on", None)
    references = getattr(depends_on, "nodes", None) or ()
    return [ref for ref in references if isinstance(ref, str) and ref.startswith("model.")]


def _physical_relation(node) -> Tuple[str, str, str]:
    """The development-side physical relation of a model (not its identity)."""
    return (
        (getattr(node, "database", None) or ""),
        (getattr(node, "schema_", None) or ""),
        (getattr(node, "alias", None) or ""),
    )


def _is_ephemeral(node) -> bool:
    config = getattr(node, "config", None)
    return getattr(config, "materialized", None) == "ephemeral"


def build_dependency_plan(selected_nodes: Sequence, manifest) -> DependencyPlan:
    """Validate a selection and compute selected-ancestor relationships.

    Identity is ``unique_id`` throughout; names, aliases and relation paths are
    never used to merge two models.  The graph check is limited to the ancestor
    subgraph reachable from the selection, so an unrelated broken node or cycle
    in the manifest does not block this run.

    Raises:
        DataDiffSelectionConflictError: empty selection, duplicate selection,
            a selected ephemeral model, a selected model absent from the
            manifest, or two selected models resolving to the same
            development-side physical relation.
        DataDiffDependencyGraphError: a referenced model is missing from the
            manifest, or the reachable ancestor subgraph contains a cycle.
    """
    nodes: Mapping[str, object] = manifest.nodes

    if not selected_nodes:
        raise DataDiffSelectionConflictError("No dbt models were selected; nothing can be checked.")

    selected: List[str] = []
    seen: Set[str] = set()
    relations: Dict[Tuple[str, str, str], str] = {}
    for node in selected_nodes:
        if node is None:
            raise DataDiffSelectionConflictError(
                "A selected model is not present in the development manifest."
            )
        unique_id = node.unique_id
        if unique_id in seen:
            raise DataDiffSelectionConflictError(f"Model {unique_id!r} was selected more than once.")
        seen.add(unique_id)

        if nodes.get(unique_id) is None:
            raise DataDiffSelectionConflictError(
                f"Selected model {unique_id!r} is not present in the development manifest."
            )
        if _is_ephemeral(node):
            raise DataDiffSelectionConflictError(
                f"Model {unique_id!r} is ephemeral and has no materialised relation to compare."
            )
        relation = _physical_relation(node)
        other = relations.get(relation)
        if other is not None:
            rendered = ".".join(part for part in relation if part)
            raise DataDiffSelectionConflictError(
                f"Models {other!r} and {unique_id!r} both resolve to the development relation "
                f"{rendered!r}."
            )
        relations[relation] = unique_id
        selected.append(unique_id)

    selected_set = set(selected)
    reachable, reverse = _collect_reachable(selected, nodes)
    order = _topological_order(reachable, reverse)
    ancestors = _compute_ancestors(reachable, order)

    selected_ancestors = {
        unique_id: frozenset(ancestors[unique_id] & selected_set) for unique_id in selected
    }
    return DependencyPlan(selected=tuple(selected), ancestors=selected_ancestors)


def _collect_reachable(selected: Sequence[str], nodes: Mapping[str, object]):
    """Collect model nodes reachable from the selection, and reverse edges.

    ``reverse[dep]`` lists nodes that depend on ``dep``.
    """
    reachable: Dict[str, object] = {}
    reverse: Dict[str, Set[str]] = {}
    stack = list(selected)
    while stack:
        unique_id = stack.pop()
        if unique_id in reachable:
            continue
        node = nodes.get(unique_id)
        if node is None:
            raise DataDiffDependencyGraphError(
                f"Model {unique_id!r} is referenced by the selection but is missing from the "
                "development manifest."
            )
        reachable[unique_id] = node
        for dep in _model_dependencies(node):
            if nodes.get(dep) is None:
                raise DataDiffDependencyGraphError(
                    f"Model {unique_id!r} references model {dep!r}, which is missing from the "
                    "development manifest."
                )
            reverse.setdefault(dep, set()).add(unique_id)
            if dep not in reachable:
                stack.append(dep)
    return reachable, reverse


def _topological_order(
    reachable: Mapping[str, object], reverse: Mapping[str, Set[str]]
) -> List[str]:
    """Return dependencies before dependents; raise on a cycle in this subgraph."""
    deps = {unique_id: _model_dependencies(node) for unique_id, node in reachable.items()}
    indegree = {unique_id: len(dep_list) for unique_id, dep_list in deps.items()}
    ready = deque(sorted(unique_id for unique_id, count in indegree.items() if count == 0))
    order: List[str] = []
    while ready:
        unique_id = ready.popleft()
        order.append(unique_id)
        for dependent in sorted(reverse.get(unique_id, ())):
            indegree[dependent] -= 1
            if indegree[dependent] == 0:
                ready.append(dependent)
    if len(order) != len(reachable):
        cyclic = sorted(unique_id for unique_id, count in indegree.items() if count > 0)
        raise DataDiffDependencyGraphError(
            "Cycle detected in the selected models' dependencies involving: " + ", ".join(cyclic)
        )
    return order


def _compute_ancestors(
    reachable: Mapping[str, object], order: Sequence[str]
) -> Dict[str, Set[str]]:
    ancestors: Dict[str, Set[str]] = {}
    for unique_id in order:
        node = reachable[unique_id]
        acc: Set[str] = set()
        for dep in _model_dependencies(node):
            acc.add(dep)
            acc.update(ancestors[dep])
        ancestors[unique_id] = acc
    return ancestors


class DependencyScheduler:
    """Release comparison work as dependency constraints are satisfied.

    The scheduler runs on the calling thread.  Only models that are ready *and*
    whose selected ancestors all settled ``identical`` are handed to the worker
    pool; models waiting on ancestors never occupy a worker.  Newly ready
    models are released as each completion arrives, not layer by layer.

    ``run_model(unique_id) -> ModelResult`` does the actual comparison and may
    touch the database.  ``preflight`` maps unique_ids to results that must be
    settled without running (unknown primary key, missing production model, bad
    per-model configuration); a blocked model wins over its preflight result,
    so reasons are always complete.  ``paths`` supplies the dev/prod relation
    paths used to render blocked models without querying anything.

    ``emit`` is called once per settled model, on the calling thread, in
    completion order.  Only a slim copy (no rendered rows) is retained for
    downstream decisions.
    """

    def __init__(
        self,
        plan: DependencyPlan,
        run_model: Callable[[str], ModelResult],
        *,
        workers: int,
        preflight: Optional[Mapping[str, ModelResult]] = None,
        paths: Optional[Mapping[str, Tuple[Sequence[str], Sequence[str]]]] = None,
        emit: Optional[Callable[[ModelResult], None]] = None,
        observer: Optional[Callable[[str, str], None]] = None,
    ) -> None:
        self.plan = plan
        self.run_model = run_model
        self.workers = max(1, int(workers))
        self.preflight = dict(preflight or {})
        self.paths = dict(paths or {})
        self.emit = emit
        self.observer = observer

        self.results: Dict[str, ModelResult] = {}
        self._remaining: Dict[str, int] = {}
        self._ready: deque = deque()
        self._children: Dict[str, Set[str]] = {unique_id: set() for unique_id in plan.selected}
        for unique_id in plan.selected:
            for ancestor in plan.ancestors_of(unique_id):
                self._children.setdefault(ancestor, set()).add(unique_id)

    def _event(self, name: str, unique_id: str) -> None:
        if self.observer is not None:
            self.observer(name, unique_id)

    def _settle(self, result: ModelResult) -> None:
        self._event("finish", result.unique_id)
        if self.emit is not None:
            self.emit(result)
        self.results[result.unique_id] = result.slim()
        for child in sorted(self._children.get(result.unique_id, ())):
            self._remaining[child] -= 1
            if self._remaining[child] == 0:
                self._event("ready", child)
                self._ready.append(child)

    def _blocked_reasons(self, unique_id: str) -> Tuple[Tuple[str, ModelOutcome], ...]:
        reasons = [
            (ancestor, self.results[ancestor].outcome)
            for ancestor in self.plan.ancestors_of(unique_id)
            if self.results[ancestor].outcome in NON_IDENTICAL_OUTCOMES
        ]
        reasons.sort(key=lambda item: item[0])
        return tuple(reasons)

    def run(self) -> Dict[str, ModelResult]:
        plan = self.plan
        self._remaining = {unique_id: len(plan.ancestors_of(unique_id)) for unique_id in plan.selected}
        self._ready = deque(
            sorted(unique_id for unique_id in plan.selected if self._remaining[unique_id] == 0)
        )
        for unique_id in self._ready:
            self._event("ready", unique_id)

        pending: Dict[object, str] = {}
        runnable: deque = deque()  # ready, not blocked/preflight, awaiting a worker
        executor = ThreadPoolExecutor(max_workers=self.workers)
        try:
            while self._ready or runnable or pending:
                # First, settle everything that needs no worker: blocked models
                # and preflight conclusions never consume a thread, and must
                # not queue behind runnable work.
                while self._ready:
                    unique_id = self._ready.popleft()
                    reasons = self._blocked_reasons(unique_id)
                    if reasons:
                        self._settle(blocked_result(unique_id, self.paths.get(unique_id), reasons))
                        continue
                    preflight_result = self.preflight.get(unique_id)
                    if preflight_result is not None:
                        self._settle(preflight_result)
                        continue
                    runnable.append(unique_id)

                while runnable and len(pending) < self.workers:
                    unique_id = runnable.popleft()
                    future = executor.submit(self._run_one, unique_id)
                    pending[future] = unique_id
                    self._event("start", unique_id)

                if not pending:
                    # Only possible if everything was settled without workers.
                    continue

                done, _ = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    unique_id = pending.pop(future)
                    self._settle(future.result())  # KeyboardInterrupt propagates
        except BaseException:
            # Stop accepting work, cancel everything still queued, and let the
            # already-started comparisons finish before handing the interrupt
            # (or error) back to the caller.  Nothing about this run is then
            # reported as a complete success.
            for future in pending:
                future.cancel()
            executor.shutdown(wait=True, cancel_futures=True)
            raise

        executor.shutdown(wait=True)
        return dict(self.results)

    def _run_one(self, unique_id: str) -> ModelResult:
        try:
            return self.run_model(unique_id)
        except KeyboardInterrupt:
            raise
        except Exception as exc:  # pragma: no cover - run_model classifies its own failures
            return failure_result(unique_id, self.paths.get(unique_id), error=str(exc) or type(exc).__name__)


def _base_paths(
    path_pair: Optional[Tuple[Sequence[str], Sequence[str]]]
) -> Tuple[Tuple[str, ...], Tuple[str, ...]]:
    if not path_pair:
        return (), ()
    dev_path, prod_path = path_pair
    return tuple(dev_path), tuple(prod_path)


def output_header(dev_path: Sequence[str], prod_path: Sequence[str]) -> str:
    return f"\n[blue]{'.'.join(prod_path)}[/] <> [green]{'.'.join(dev_path)}[/] \n"


def blocked_result(
    unique_id: str,
    path_pair: Optional[Tuple[Sequence[str], Sequence[str]]],
    reasons: Tuple[Tuple[str, ModelOutcome], ...],
) -> ModelResult:
    dev_path, prod_path = _base_paths(path_pair)
    reason_lines = "\n".join(f"  {ancestor}: {outcome.value}" for ancestor, outcome in reasons)
    text = output_header(dev_path, prod_path) + "Blocked by non-identical selected ancestors:\n" + reason_lines + "\n"
    json_record = {
        "version": "1.1.0",
        "status": "blocked",
        "outcome": ModelOutcome.BLOCKED.value,
        "model": unique_id,
        "dataset1": list(prod_path),
        "dataset2": list(dev_path),
        "blocked_by": [{"unique_id": ancestor, "outcome": outcome.value} for ancestor, outcome in reasons],
    }
    return ModelResult(
        unique_id=unique_id,
        outcome=ModelOutcome.BLOCKED,
        dev_path=dev_path,
        prod_path=prod_path,
        text=text,
        json_record=json_record,
        blocked_by=reasons,
    )


def failure_result(
    unique_id: str,
    path_pair: Optional[Tuple[Sequence[str], Sequence[str]]],
    *,
    error: str,
    outcome: ModelOutcome = ModelOutcome.ERROR,
) -> ModelResult:
    """Build an ``error`` / ``unavailable`` record without any database access."""
    dev_path, prod_path = _base_paths(path_pair)
    label = "Unavailable" if outcome is ModelOutcome.UNAVAILABLE else "Error"
    text = output_header(dev_path, prod_path) + f"{label}: {error}\n"
    json_record = {
        "version": "1.1.0",
        "status": outcome.value,
        "outcome": outcome.value,
        "model": unique_id,
        "dataset1": list(prod_path),
        "dataset2": list(dev_path),
        "error": error,
    }
    return ModelResult(
        unique_id=unique_id,
        outcome=outcome,
        dev_path=dev_path,
        prod_path=prod_path,
        text=text,
        json_record=json_record,
        error=error,
    )
