from contextlib import nullcontext
import json
import os
import re
import time
from typing import Callable, Dict, List, Optional, Tuple, Union
import keyring
import pydantic
import rich
from rich.prompt import Prompt
from rich.markdown import Markdown
from concurrent.futures import ThreadPoolExecutor, as_completed

from data_diff.errors import (
    DataDiffCustomSchemaNoConfigError,
    DataDiffDbtProjectVarsNotFoundError,
    DataDiffDependencyPolicyError,
    DataDiffNoAPIKeyError,
    DataDiffNoDatasourceIdError,
    DataDiffTableNotFoundError,
    DataDiffTableNotReadableError,
)

from data_diff import connect_to_table, diff_tables, Algorithm
from data_diff.cloud import DatafoldAPI, TCloudApiDataDiff, TCloudApiOrgMeta
from data_diff.dbt_dependency import (
    DependencyPolicy,
    DependencyScheduler,
    ModelOutcome,
    ModelResult,
    build_dependency_plan,
    failure_result,
)
from data_diff.dbt_parser import DbtParser, TDatadiffConfig
from data_diff.diff_tables import DiffResultWrapper
from data_diff.format import jsonify, jsonify_error
from data_diff.tracking import (
    bool_ask_for_email,
    bool_notify_about_extension,
    create_email_signup_event_json,
    set_entrypoint_name,
    set_dbt_user_id,
    set_dbt_version,
    set_dbt_project_id,
    create_end_event_json,
    create_start_event_json,
    send_event_json,
    is_tracking_enabled,
)
from data_diff.utils import (
    dbt_diff_string_template,
    getLogger,
    columns_added_template,
    columns_removed_template,
    no_differences_template,
    columns_type_changed_template,
    run_as_daemon,
    truncate_error,
    print_version_info,
    LogStatusHandler,
)

logger = getLogger(__name__)
CLOUD_DOC_URL = "https://docs.datafold.com/development_testing/cloud"
DATAFOLD_TRIAL_URL = "https://app.datafold.com/org-signup"
DATAFOLD_INSTRUCTIONS_URL = "https://docs.datafold.com/development_testing/datafold_cloud"


class TDiffVars(pydantic.BaseModel):
    dev_path: List[str]
    prod_path: List[str]
    primary_keys: List[str]
    connection: Dict[str, Optional[str]]
    threads: Optional[int] = None
    where_filter: Optional[str] = None
    include_columns: List[str]
    exclude_columns: List[str]
    dbt_model: Optional[str] = None
    stats_flag: bool = False


def dbt_diff(
    profiles_dir_override: Optional[str] = None,
    project_dir_override: Optional[str] = None,
    is_cloud: bool = False,
    dbt_selection: Optional[str] = None,
    json_output: bool = False,
    state: Optional[str] = None,
    log_status_handler: Optional[LogStatusHandler] = None,
    where_flag: Optional[str] = None,
    stats_flag: bool = False,
    columns_flag: Optional[Tuple[str]] = None,
    production_database_flag: Optional[str] = None,
    production_schema_flag: Optional[str] = None,
    dependency_policy: Union[DependencyPolicy, str, None] = None,
    _parser: Optional["DbtParser"] = None,
    _runner_factory: Optional[Callable[[Callable[[str], ModelResult]], Callable[[str], ModelResult]]] = None,
    _connector: Optional[Callable[..., object]] = None,
) -> None:
    """Compare dbt models.

    ``dependency_policy`` defaults to ``"require_identical"``, which preserves
    the historical behaviour of submitting every selected model at once.
    ``"independent"`` only checks a model after all of its *selected* ancestors
    settled ``identical``; the rest are reported ``blocked``.  ``"independent"``
    is local-only and is rejected before any cloud request.

    ``_parser`` and ``_runner_factory`` are test seams; production callers
    leave them unset.
    """
    print_version_info()
    # Validate the policy before anything else, so a bad value never reaches
    # the database (or the cloud API).  The default keeps the historical
    # "submit every selected model at once" behaviour.
    policy = DependencyPolicy.parse(dependency_policy)
    set_entrypoint_name(os.getenv("DATAFOLD_TRIGGERED_BY", "CLI-dbt"))
    dbt_parser = _parser if _parser is not None else DbtParser(profiles_dir_override, project_dir_override, state)
    models = dbt_parser.get_models(dbt_selection)
    config = dbt_parser.get_datadiff_config()
    _initialize_events(dbt_parser.dbt_user_id, dbt_parser.dbt_version, dbt_parser.dbt_project_id)

    if policy is DependencyPolicy.INDEPENDENT:
        # Enforce the dependency policy for local comparisons only.  Cloud
        # diffs are request-based, so refuse before sending anything.
        if is_cloud:
            raise DataDiffDependencyPolicyError(
                "dependency_policy='independent' is only supported for local dbt comparisons; "
                "it cannot be combined with --cloud."
            )
        _local_dbt_diff(
            dbt_parser,
            models,
            config,
            json_output=json_output,
            where_flag=where_flag,
            stats_flag=stats_flag,
            columns_flag=columns_flag,
            production_database_flag=production_database_flag,
            production_schema_flag=production_schema_flag,
            state=state,
            log_status_handler=log_status_handler,
            runner_factory=_runner_factory,
            connector=_connector,
        )
        _extension_notification()
        return

    if not state and not (config.prod_database or config.prod_schema):
        doc_url = "https://docs.datafold.com/development_testing/open_source#configure-your-dbt-project"
        raise DataDiffDbtProjectVarsNotFoundError(
            f"""vars: data_diff: section not found in dbt_project.yml.\n\nTo solve this, please configure your dbt project: \n{doc_url}\n\nOr specify a production manifest using the `--state` flag."""
        )

    if is_cloud:
        api = _initialize_api()
        # exit so the user can set the key
        if not api:
            return
        org_meta = api.get_org_meta()
        if config.datasource_id is None:
            rich.print("[red]Data source ID not found in dbt_project.yml")
            raise DataDiffNoDatasourceIdError(
                f"Datasource ID not found. Please include it as a dbt variable in the dbt_project.yml. \nInstructions: {CLOUD_DOC_URL}\n\nvars:\n data_diff:\n   datasource_id: 1234"
            )

        data_source = api.get_data_source(config.datasource_id)
        dbt_parser.set_casing_policy_for(connection_type=data_source.type)
        rich.print("[green][bold]\nDiffs in progress...[/][/]\n")

    else:
        dbt_parser.set_connection()

    futures = {}

    with log_status_handler.status if log_status_handler else nullcontext(), ThreadPoolExecutor(
        max_workers=dbt_parser.threads
    ) as executor:
        for model in models:
            if log_status_handler:
                log_status_handler.set_prefix(f"Diffing {model.alias} \n")

            diff_vars = _get_diff_vars(
                dbt_parser,
                config,
                model,
                where_flag,
                stats_flag,
                columns_flag,
                production_database_flag,
                production_schema_flag,
            )

            # we won't always have a prod path when using state
            # when the model DNE in prod manifest, skip the model diff
            if (
                state and len(diff_vars.prod_path) < 2
            ):  # < 2 because some providers like databricks can legitimately have *only* 2
                diff_output_str = _diff_output_base(".".join(diff_vars.dev_path), ".".join(diff_vars.prod_path))
                diff_output_str += "[green]New model: nothing to diff![/] \n"
                rich.print(diff_output_str)
                continue

            if diff_vars.primary_keys:
                if is_cloud:
                    future = executor.submit(
                        _cloud_diff, diff_vars, config.datasource_id, api, org_meta, log_status_handler
                    )
                else:
                    future = executor.submit(_local_diff, diff_vars, json_output, log_status_handler)
                futures[future] = model
            else:
                if json_output:
                    print(
                        json.dumps(
                            jsonify_error(
                                table1=diff_vars.prod_path,
                                table2=diff_vars.dev_path,
                                dbt_model=diff_vars.dbt_model,
                                error="No primary key found. Add uniqueness tests, meta, or tags.",
                            )
                        ),
                        flush=True,
                    )
                else:
                    rich.print(
                        _diff_output_base(".".join(diff_vars.dev_path), ".".join(diff_vars.prod_path))
                        + "Skipped due to unknown primary key. Add uniqueness tests, meta, or tags.\n"
                    )

    for future in as_completed(futures):
        model = futures[future]
        try:
            future.result()  # if error occurred, it will be raised here
        except Exception as e:
            logger.error(f"An error occurred during the execution of a diff task: {model.unique_id} - {e}")

    _extension_notification()


def _diff_vars_for_model(
    dbt_parser: "DbtParser",
    config: TDatadiffConfig,
    model,
    where_flag: Optional[str],
    stats_flag: bool,
    columns_flag: Optional[Tuple[str]],
    production_database_flag: Optional[str],
    production_schema_flag: Optional[str],
) -> TDiffVars:
    return _get_diff_vars(
        dbt_parser,
        config,
        model,
        where_flag,
        stats_flag,
        columns_flag,
        production_database_flag,
        production_schema_flag,
    )


class _LocalReport:
    """Prints one terminal record per model, in completion order.

    Nothing is buffered except the record itself, so large JSONL row details
    are written straight out and never copied into a second structure.
    """

    def __init__(self, json_output: bool) -> None:
        self.json_output = json_output

    def emit(self, result: ModelResult) -> None:
        if self.json_output:
            print(json.dumps(result.json_record), flush=True)
        else:
            rich.print(result.text)


def _unavailable_result(
    diff_vars: TDiffVars, *, reason: str, model_label: Optional[str] = None
) -> ModelResult:
    return failure_result(
        model_label or diff_vars.dbt_model,
        (diff_vars.dev_path, diff_vars.prod_path),
        error=reason,
        outcome=ModelOutcome.UNAVAILABLE,
    )


def _error_result(diff_vars: TDiffVars, *, reason: str) -> ModelResult:
    return failure_result(
        diff_vars.dbt_model,
        (diff_vars.dev_path, diff_vars.prod_path),
        error=reason,
        outcome=ModelOutcome.ERROR,
    )


def _local_dbt_diff(
    dbt_parser: "DbtParser",
    models,
    config: TDatadiffConfig,
    *,
    json_output: bool,
    where_flag: Optional[str],
    stats_flag: bool,
    columns_flag: Optional[Tuple[str]],
    production_database_flag: Optional[str],
    production_schema_flag: Optional[str],
    state: Optional[str],
    log_status_handler: Optional[LogStatusHandler],
    runner_factory: Optional[Callable[[Callable[[str], ModelResult]], Callable[[str], ModelResult]]] = None,
    connector: Optional[Callable[..., object]] = None,
) -> None:
    """Run a local dbt comparison constrained by ``dependency_policy``.

    The plan is validated before a single connection is opened, so a bad
    selection (duplicates, ephemeral, relation clashes, dangling refs, cycles)
    or a cloud request fails before any table is queried.
    """
    manifest = dbt_parser.dev_manifest_obj
    plan = build_dependency_plan(models, manifest)

    dbt_parser.set_connection()
    connection = dbt_parser.connection
    threads = dbt_parser.threads
    databases: List = []
    base_connector = connector or connect_to_table

    def opening_connector(db_info, table_name, key_columns, thread_count=1, **kwargs):
        segment = base_connector(db_info, table_name, key_columns, thread_count=thread_count, **kwargs)
        if all(db is not segment.database for db in databases):
            databases.append(segment.database)
        return segment

    nodes = {node.unique_id: node for node in models}
    diff_vars_by_id: Dict[str, TDiffVars] = {}
    paths: Dict[str, Tuple[List[str], List[str]]] = {}
    preflight: Dict[str, ModelResult] = {}

    for unique_id in plan.selected:
        model = nodes[unique_id]
        try:
            diff_vars = _diff_vars_for_model(
                dbt_parser,
                config,
                model,
                where_flag,
                stats_flag,
                columns_flag,
                production_database_flag,
                production_schema_flag,
            )
        except Exception as exc:  # e.g. custom schema with no prod_custom_schema
            preflight[unique_id] = failure_result(
                unique_id, (), error=str(exc) or type(exc).__name__, outcome=ModelOutcome.ERROR
            )
            continue
        diff_vars_by_id[unique_id] = diff_vars
        paths[unique_id] = (diff_vars.dev_path, diff_vars.prod_path)

        # Missing production model (--state) is "unavailable", not identical.
        if state and len(diff_vars.prod_path) < 2:
            preflight[unique_id] = _unavailable_result(
                diff_vars,
                reason="New model: production manifest has no corresponding model. Nothing to diff.",
                model_label=unique_id,
            )
            continue
        if not diff_vars.primary_keys:
            preflight[unique_id] = _unavailable_result(
                diff_vars,
                reason="No primary key found. Add uniqueness tests, meta, or tags.",
                model_label=unique_id,
            )

    def _compare_one(unique_id: str) -> ModelResult:
        return _run_local_model(
            diff_vars_by_id[unique_id],
            connection,
            threads,
            json_output,
            connector=opening_connector,
        )

    runner = runner_factory(_compare_one) if runner_factory is not None else _compare_one

    scheduler = DependencyScheduler(
        plan,
        runner,
        workers=threads or 1,
        preflight=preflight,
        paths=paths,
        emit=_LocalReport(json_output).emit,
        observer=log_status_handler.observe if log_status_handler else None,
    )
    try:
        with log_status_handler.status if log_status_handler else nullcontext():
            scheduler.run()
    finally:
        # The per-model connections belong to this run; releasing them also
        # prevents a later, independent call from inheriting a stale session.
        for database in databases:
            try:
                database.close()
            except Exception:  # pragma: no cover - cleanup must not mask results
                logger.debug("Error while closing a comparison connection", exc_info=True)


EMPTY_TABLE_ERROR = "Table appears to be empty"


def _is_empty_table_error(exc: BaseException) -> bool:
    return EMPTY_TABLE_ERROR in str(exc)


def _empty_relations_result(
    diff_vars: TDiffVars,
    table1,
    table2,
    table1_columns: Dict,
    table2_columns: Dict,
    columns_added,
    columns_removed,
    columns_type_changed,
    diff_output_str: str,
    json_output: bool,
) -> ModelResult:
    """Conclude a comparison where JOINDIFF could not find a key range.

    An empty relation is not "no differences": if the other side has rows (in
    scope), the result is ``different``.  Row counts respect the same filter,
    and column differences still count.  This only runs after a real query.
    """
    try:
        count1 = table1.count()
        count2 = table2.count()
    except Exception as ex:
        return _error_result(diff_vars, reason=str(ex) or type(ex).__name__)

    is_different = bool(
        count1 != count2 or columns_added or columns_removed or columns_type_changed
    )
    outcome = ModelOutcome.DIFFERENT if is_different else ModelOutcome.IDENTICAL
    dev_path = tuple(diff_vars.dev_path)
    prod_path = tuple(diff_vars.prod_path)

    if json_output:
        record = {
            "version": "1.1.0",
            "status": "success",
            "result": outcome.value,
            "outcome": outcome.value,
            "model": diff_vars.dbt_model,
            "dataset1": list(prod_path),
            "dataset2": list(dev_path),
            "rows": None,
            "summary": {
                "rows": {
                    "total": {"dataset1": count1, "dataset2": count2},
                    "exclusive": {"dataset1": 0, "dataset2": 0},
                    "updated": 0,
                    "unchanged": min(count1, count2),
                },
                "stats": {"diffCounts": {}},
            },
            "columns": {
                "dataset1": _column_records(table1, table1_columns),
                "dataset2": _column_records(table2, table2_columns),
                "primaryKey": list(diff_vars.primary_keys),
                "exclusive": {
                    "dataset1": list(columns_removed),
                    "dataset2": list(columns_added),
                },
                "typeChanged": list(columns_type_changed),
            },
        }
        return ModelResult(
            unique_id=diff_vars.dbt_model,
            outcome=outcome,
            dev_path=dev_path,
            prod_path=prod_path,
            json_record=record,
        )

    header = _diff_output_base(".".join(diff_vars.dev_path), ".".join(diff_vars.prod_path))
    if is_different:
        text = (
            header
            + diff_output_str
            + dbt_diff_string_template(
                total_rows_table1=count1,
                total_rows_table2=count2,
                total_rows_diff=count2 - count1,
                rows_added=max(count2 - count1, 0),
                rows_removed=max(count1 - count2, 0),
                rows_updated=0,
                rows_unchanged=min(count1, count2),
                extra_info_dict=None,
            )
            + "\n"
        )
    else:
        text = header + diff_output_str + no_differences_template()
    return ModelResult(
        unique_id=diff_vars.dbt_model,
        outcome=outcome,
        dev_path=dev_path,
        prod_path=prod_path,
        text=text,
    )


def _column_records(table, columns: Dict) -> list:
    """Render a schema for JSONL, using the same shape as ``jsonify``."""
    records = []
    for info in columns.values():
        try:
            parsed = table.database.dialect.parse_type(table.table_path, info)
        except Exception:
            parsed = None
        records.append({"name": info.column_name, "type": info.data_type, "kind": _kind_name(parsed)})
    return records


def _kind_name(col_type) -> str:
    from data_diff.format import ColumnKind, _map_kind

    return _map_kind(col_type).value if col_type is not None else ColumnKind.UNSUPPORTED.value


def _local_model_result(
    diff_vars: TDiffVars,
    table1_columns: Dict,
    table2_columns: Dict,
    diff: DiffResultWrapper,
    columns_added,
    columns_removed,
    columns_type_changed,
    table1,
    table2,
    json_output: bool,
    diff_output_str: str,
) -> ModelResult:
    """Turn a completed local comparison into a structured terminal record.

    The conclusion drives scheduling, text and JSONL alike; it is never
    inferred back from a printed string.
    """
    dev_path = tuple(diff_vars.dev_path)
    prod_path = tuple(diff_vars.prod_path)
    header = _diff_output_base(".".join(diff_vars.dev_path), ".".join(diff_vars.prod_path))

    is_different = bool(diff.result_list or columns_added or columns_removed or columns_type_changed)
    outcome = ModelOutcome.DIFFERENT if is_different else ModelOutcome.IDENTICAL

    if json_output:
        dataset1_columns = [
            (info.column_name, info.data_type, table1.database.dialect.parse_type(table1.table_path, info))
            for info in table1_columns.values()
        ]
        dataset2_columns = [
            (info.column_name, info.data_type, table2.database.dialect.parse_type(table2.table_path, info))
            for info in table2_columns.values()
        ]
        record = jsonify(
            diff,
            dbt_model=diff_vars.dbt_model,
            dataset1_columns=dataset1_columns,
            dataset2_columns=dataset2_columns,
            with_summary=True,
            columns_diff={
                "added": columns_added,
                "removed": columns_removed,
                "changed": columns_type_changed,
            },
            stats_only=diff_vars.stats_flag,
        )
        record["outcome"] = outcome.value
        return ModelResult(
            unique_id=diff_vars.dbt_model,
            outcome=outcome,
            dev_path=dev_path,
            prod_path=prod_path,
            json_record=record,
        )

    if diff.result_list:
        text = header + diff_output_str + f"{diff.get_stats_string(is_dbt=True)} \n"
    else:
        text = header + diff_output_str + no_differences_template()
    return ModelResult(
        unique_id=diff_vars.dbt_model,
        outcome=outcome,
        dev_path=dev_path,
        prod_path=prod_path,
        text=text,
    )


def _run_local_model(
    diff_vars: TDiffVars,
    connection: Dict[str, Optional[str]],
    threads: Optional[int],
    json_output: bool = False,
    connector: Optional[Callable[..., object]] = None,
) -> ModelResult:
    """Compare one model's dev/prod relations and classify the outcome.

    Missing primary key / missing prod relation are ``unavailable``; permission,
    connection and query failures are ``error``.  A real not-found error from
    the adapter is also classified ``unavailable``; anything the adapter cannot
    distinguish stays ``error``.  Only a completed comparison with no row,
    column or type difference is ``identical``.
    """
    connect = connector or connect_to_table
    dev_qualified_str = ".".join(diff_vars.dev_path)
    prod_qualified_str = ".".join(diff_vars.prod_path)

    dev_table1 = connect(connection, prod_qualified_str, tuple(diff_vars.primary_keys), thread_count=threads)
    dev_table2 = connect(connection, dev_qualified_str, tuple(diff_vars.primary_keys), thread_count=threads)

    try:
        table1_columns = dev_table1.get_schema()
    except DataDiffTableNotFoundError as ex:
        return _unavailable_result(
            diff_vars, reason=f"Production relation does not exist: {ex}", model_label=diff_vars.dbt_model
        )
    except DataDiffTableNotReadableError as ex:
        # The relation is there, but this user cannot read it: a permission
        # failure, not a brand-new model.
        return _error_result(diff_vars, reason=str(ex) or type(ex).__name__)
    except Exception as ex:
        return _error_result(diff_vars, reason=str(ex) or type(ex).__name__)

    try:
        table2_columns = dev_table2.get_schema()
    except DataDiffTableNotFoundError as ex:
        return _unavailable_result(
            diff_vars, reason=f"Development relation does not exist: {ex}", model_label=diff_vars.dbt_model
        )
    except DataDiffTableNotReadableError as ex:
        return _error_result(diff_vars, reason=str(ex) or type(ex).__name__)
    except Exception as ex:
        return _error_result(diff_vars, reason=str(ex) or type(ex).__name__)

    table1_column_names = set(table1_columns.keys())
    table2_column_names = set(table2_columns.keys())
    column_set = table1_column_names.intersection(table2_column_names)
    columns_added = table2_column_names.difference(table1_column_names)
    columns_removed = table1_column_names.difference(table2_column_names)
    columns_type_changed = {
        k for k, v in table2_columns.items() if k in table1_columns and v.data_type != table1_columns[k].data_type
    }

    diff_output_str = f"Primary Keys: {diff_vars.primary_keys} \n"
    if diff_vars.where_filter:
        diff_output_str += f"Where Filter: '{str(diff_vars.where_filter)}' \n"
    if diff_vars.include_columns:
        diff_output_str += f"Included Columns: {diff_vars.include_columns} \n"
    if diff_vars.exclude_columns:
        diff_output_str += f"Excluded Columns: {diff_vars.exclude_columns} \n"
    if columns_removed:
        diff_output_str += columns_removed_template(columns_removed)
    if columns_added:
        diff_output_str += columns_added_template(columns_added)
    if columns_type_changed:
        diff_output_str += columns_type_changed_template(columns_type_changed)
        column_set = column_set.difference(columns_type_changed)

    column_set = column_set - set(diff_vars.primary_keys)
    if diff_vars.include_columns:
        column_set = {x for x in column_set if x.upper() in [y.upper() for y in diff_vars.include_columns]}
    if diff_vars.exclude_columns:
        column_set = {x for x in column_set if x.upper() not in [y.upper() for y in diff_vars.exclude_columns]}

    diff: DiffResultWrapper = diff_tables(
        dev_table1,
        dev_table2,
        threaded=True,
        algorithm=Algorithm.JOINDIFF,
        extra_columns=tuple(column_set),
        where=diff_vars.where_filter,
        skip_null_keys=True,
    )
    try:
        # Drain the iterator so stats/info_tree describe a complete comparison
        # even in stats-only mode; never show the rows here.
        list(diff)
    except Exception as ex:
        if _is_empty_table_error(ex):
            # JOINDIFF bisection needs a key range; an empty relation has none.
            # Compare real row counts (honouring the filter) instead of
            # pretending the tables are identical.
            return _empty_relations_result(
                diff_vars,
                dev_table1,
                dev_table2,
                table1_columns,
                table2_columns,
                columns_added,
                columns_removed,
                columns_type_changed,
                diff_output_str,
                json_output,
            )
        return _error_result(diff_vars, reason=str(ex) or type(ex).__name__)
    finally:
        diff.close()

    return _local_model_result(
        diff_vars,
        table1_columns,
        table2_columns,
        diff,
        columns_added,
        columns_removed,
        columns_type_changed,
        dev_table1,
        dev_table2,
        json_output,
        diff_output_str,
    )


def _get_diff_vars(
    dbt_parser: "DbtParser",
    config: TDatadiffConfig,
    model,
    where_flag: Optional[str] = None,
    stats_flag: bool = False,
    columns_flag: Optional[Tuple[str]] = None,
    production_database_flag: Optional[str] = None,
    production_schema_flag: Optional[str] = None,
) -> TDiffVars:
    cli_columns = list(columns_flag) if columns_flag else []
    dev_database = model.database
    dev_schema = model.schema_
    dev_alias = prod_alias = model.alias
    primary_keys = dbt_parser.get_pk_from_model(model, dbt_parser.unique_columns, "primary-key")

    # prod path is constructed via configuration or the prod manifest via --state
    if dbt_parser.prod_manifest_obj:
        prod_database, prod_schema, prod_alias = _get_prod_path_from_manifest(model, dbt_parser.prod_manifest_obj)
    else:
        prod_database, prod_schema = _get_prod_path_from_config(config, model, dev_database, dev_schema)

    # cli flags take precedence over any project level config
    prod_database = production_database_flag or prod_database
    prod_schema = production_schema_flag or prod_schema

    if dbt_parser.requires_upper:
        dev_qualified_list = [x.upper() for x in [dev_database, dev_schema, dev_alias] if x]
        prod_qualified_list = [x.upper() for x in [prod_database, prod_schema, prod_alias] if x]
        primary_keys = [x.upper() for x in primary_keys]
    else:
        dev_qualified_list = [x for x in [dev_database, dev_schema, dev_alias] if x]
        prod_qualified_list = [x for x in [prod_database, prod_schema, prod_alias] if x]

    datadiff_model_config = dbt_parser.get_datadiff_model_config(model.meta)

    return TDiffVars(
        dbt_model=model.unique_id,
        dev_path=dev_qualified_list,
        prod_path=prod_qualified_list,
        primary_keys=primary_keys,
        connection=dbt_parser.connection,
        threads=dbt_parser.threads,
        # cli flags take precedence over any model level config
        where_filter=where_flag or datadiff_model_config.where_filter,
        include_columns=cli_columns or datadiff_model_config.include_columns,
        exclude_columns=[] if cli_columns else datadiff_model_config.exclude_columns,
        stats_flag=stats_flag,
    )


def _get_prod_path_from_config(config, model, dev_database, dev_schema) -> Tuple[str, str]:
    # "custom" dbt config database
    if model.config.database:
        prod_database = model.config.database
    elif config.prod_database:
        prod_database = config.prod_database
    else:
        prod_database = dev_database

    # prod schema name differs from dev schema name
    if config.prod_schema:
        custom_schema = model.config.schema_

        # the model has a custom schema config(schema='some_schema')
        if custom_schema:
            if not config.prod_custom_schema:
                raise DataDiffCustomSchemaNoConfigError(
                    f"Found a custom schema on model {model.name}, but no value for\nvars:\n  data_diff:\n    prod_custom_schema:\nPlease set a value or utilize the `--state` flag!\n\n"
                    + "For more details see: https://docs.datafold.com/development_testing/open_source"
                )
            prod_schema = config.prod_custom_schema.replace("<custom_schema>", custom_schema)
            # no custom schema, use the default
        else:
            prod_schema = config.prod_schema
    else:
        prod_schema = dev_schema
    return prod_database, prod_schema


def _get_prod_path_from_manifest(model, prod_manifest) -> Union[Tuple[str, str, str], Tuple[None, None, None]]:
    prod_database = None
    prod_schema = None
    prod_alias = None
    prod_model = prod_manifest.nodes.get(model.unique_id, None)
    if prod_model:
        prod_database = prod_model.database
        prod_schema = prod_model.schema_
        prod_alias = prod_model.alias
    return prod_database, prod_schema, prod_alias


def _local_diff(
    diff_vars: TDiffVars, json_output: bool = False, log_status_handler: Optional[LogStatusHandler] = None
) -> None:
    if log_status_handler:
        log_status_handler.diff_started(diff_vars.dev_path[-1])
    dev_qualified_str = ".".join(diff_vars.dev_path)
    prod_qualified_str = ".".join(diff_vars.prod_path)
    diff_output_str = _diff_output_base(dev_qualified_str, prod_qualified_str)

    table1 = connect_to_table(diff_vars.connection, prod_qualified_str, tuple(diff_vars.primary_keys))
    table2 = connect_to_table(diff_vars.connection, dev_qualified_str, tuple(diff_vars.primary_keys))

    try:
        table1_columns = table1.get_schema()
    # Not ideal, but we don't have more specific exceptions yet
    except Exception as ex:
        logger.debug(ex)
        diff_output_str += "[red]New model or no access to prod table.[/] \n"
        rich.print(diff_output_str)
        return

    table2_columns = table2.get_schema()

    table1_column_names = set(table1_columns.keys())
    table2_column_names = set(table2_columns.keys())
    column_set = table1_column_names.intersection(table2_column_names)
    columns_added = table2_column_names.difference(table1_column_names)
    columns_removed = table1_column_names.difference(table2_column_names)
    # col type is i = 1 in tuple
    columns_type_changed = {
        k for k, v in table2_columns.items() if k in table1_columns and v.data_type != table1_columns[k].data_type
    }

    diff_output_str += f"Primary Keys: {diff_vars.primary_keys} \n"

    if diff_vars.where_filter:
        diff_output_str += f"Where Filter: '{str(diff_vars.where_filter)}' \n"

    if diff_vars.include_columns:
        diff_output_str += f"Included Columns: {diff_vars.include_columns} \n"

    if diff_vars.exclude_columns:
        diff_output_str += f"Excluded Columns: {diff_vars.exclude_columns} \n"

    if columns_removed:
        diff_output_str += columns_removed_template(columns_removed)

    if columns_added:
        diff_output_str += columns_added_template(columns_added)

    if columns_type_changed:
        diff_output_str += columns_type_changed_template(columns_type_changed)
        column_set = column_set.difference(columns_type_changed)

    column_set = column_set - set(diff_vars.primary_keys)

    if diff_vars.include_columns:
        column_set = {x for x in column_set if x.upper() in [y.upper() for y in diff_vars.include_columns]}

    if diff_vars.exclude_columns:
        column_set = {x for x in column_set if x.upper() not in [y.upper() for y in diff_vars.exclude_columns]}

    extra_columns = tuple(column_set)

    diff: DiffResultWrapper = diff_tables(
        table1,
        table2,
        threaded=True,
        algorithm=Algorithm.JOINDIFF,
        extra_columns=extra_columns,
        where=diff_vars.where_filter,
        skip_null_keys=True,
    )
    if json_output:
        # drain the iterator to get accumulated stats in diff.info_tree
        try:
            list(diff)
        except Exception as e:
            print(
                json.dumps(
                    jsonify_error(list(table1.table_path), list(table2.table_path), diff_vars.dbt_model, str(e))
                ),
                flush=True,
            )
            return

        dataset1_columns = [
            (info.column_name, info.data_type, table1.database.dialect.parse_type(table1.table_path, info))
            for info in table1_columns.values()
        ]
        dataset2_columns = [
            (info.column_name, info.data_type, table2.database.dialect.parse_type(table2.table_path, info))
            for info in table2_columns.values()
        ]

        print(
            json.dumps(
                jsonify(
                    diff,
                    dbt_model=diff_vars.dbt_model,
                    dataset1_columns=dataset1_columns,
                    dataset2_columns=dataset2_columns,
                    with_summary=True,
                    columns_diff={
                        "added": columns_added,
                        "removed": columns_removed,
                        "changed": columns_type_changed,
                    },
                    stats_only=diff_vars.stats_flag,
                )
            ),
            flush=True,
        )
        return

    if list(diff):
        diff_output_str += f"{diff.get_stats_string(is_dbt=True)} \n"
        rich.print(diff_output_str)
    else:
        diff_output_str += no_differences_template()
        rich.print(diff_output_str)

    if log_status_handler:
        log_status_handler.diff_finished(diff_vars.dev_path[-1])


def _initialize_api() -> Optional[DatafoldAPI]:
    datafold_host = os.environ.get("DATAFOLD_HOST")
    if datafold_host is None:
        datafold_host = "https://app.datafold.com"
    datafold_host = datafold_host.rstrip("/")
    rich.print(f"Cloud datafold host: {datafold_host}")

    api_key = os.environ.get("DATAFOLD_API_KEY")
    if not api_key:
        rich.print("[red]API key not found. Getting from the keyring service")
        api_key = keyring.get_password("data-diff", "DATAFOLD_API_KEY")
        if not api_key:
            raise DataDiffNoAPIKeyError(
                f"API key not found. Please follow the steps at {CLOUD_DOC_URL} to use the --cloud flag."
            )
    rich.print("Saving the API key to the system keyring service")
    try:
        keyring.set_password("data-diff", "DATAFOLD_API_KEY", api_key)
    except Exception as e:
        rich.print(f"[red]Failed when saving the API key to the system keyring service. Reason: {e}")

    return DatafoldAPI(api_key=api_key, host=datafold_host)


def _cloud_diff(
    diff_vars: TDiffVars,
    datasource_id: int,
    api: DatafoldAPI,
    org_meta: TCloudApiOrgMeta,
    log_status_handler: Optional[LogStatusHandler] = None,
) -> None:
    if log_status_handler:
        log_status_handler.diff_started(diff_vars.dev_path[-1])
    diff_output_str = _diff_output_base(".".join(diff_vars.dev_path), ".".join(diff_vars.prod_path))
    payload = TCloudApiDataDiff(
        data_source1_id=datasource_id,
        data_source2_id=datasource_id,
        table1=diff_vars.prod_path,
        table2=diff_vars.dev_path,
        pk_columns=diff_vars.primary_keys,
        filter1=diff_vars.where_filter,
        filter2=diff_vars.where_filter,
        include_columns=diff_vars.include_columns,
        exclude_columns=diff_vars.exclude_columns,
    )

    if is_tracking_enabled():
        event_json = create_start_event_json({"is_cloud": True, "datasource_id": datasource_id})
        run_as_daemon(send_event_json, event_json)

    start = time.monotonic()
    error = None
    diff_id = None
    diff_url = None
    try:
        diff_id = api.create_data_diff(payload=payload)
        diff_url = f"{api.host}/datadiffs/{diff_id}/overview"
        rich.print(f"{diff_vars.dev_path[-1]}: {diff_url}")

        if diff_id is None:
            raise Exception(f"Api response did not contain a diff_id")

        diff_results = api.poll_data_diff_results(diff_id)

        rows_added_count = diff_results.pks.exclusives[1]
        rows_removed_count = diff_results.pks.exclusives[0]

        rows_updated = diff_results.values.rows_with_differences
        total_rows_table1 = diff_results.pks.total_rows[0]
        total_rows_table2 = diff_results.pks.total_rows[1]
        total_rows_diff = total_rows_table2 - total_rows_table1

        rows_unchanged = int(total_rows_table1) - int(rows_updated) - int(rows_removed_count)
        diff_percent_list = {
            x.column_name: f"{str(round(100.00 - x.match, 2))}%"
            for x in diff_results.values.columns_diff_stats
            if x.match != 100.0
        }
        columns_added = set(diff_results.schema_.exclusive_columns[1])
        columns_removed = set(diff_results.schema_.exclusive_columns[0])
        column_type_changes = diff_results.schema_.column_type_differs

        diff_output_str += f"Primary Keys: {diff_vars.primary_keys} \n"
        if diff_vars.where_filter:
            diff_output_str += f"Where Filter: '{str(diff_vars.where_filter)}' \n"

        if diff_vars.include_columns:
            diff_output_str += f"Included Columns: {diff_vars.include_columns} \n"

        if diff_vars.exclude_columns:
            diff_output_str += f"Excluded Columns: {diff_vars.exclude_columns} \n"

        if columns_removed:
            diff_output_str += columns_removed_template(columns_removed)

        if columns_added:
            diff_output_str += columns_added_template(columns_added)

        if column_type_changes:
            diff_output_str += columns_type_changed_template(column_type_changes)

        deps_impacts = {
            key: len(value) + sum(len(item.get("BiHtSync", [])) for item in value) if key == "hightouch" else len(value)
            for key, value in diff_results.deps.deps.items()
        }

        if any([rows_added_count, rows_removed_count, rows_updated]):
            diff_output = dbt_diff_string_template(
                total_rows_table1=total_rows_table1,
                total_rows_table2=total_rows_table2,
                total_rows_diff=total_rows_diff,
                rows_added=rows_added_count,
                rows_removed=rows_removed_count,
                rows_updated=rows_updated,
                rows_unchanged=str(rows_unchanged),
                deps_impacts=deps_impacts,
                is_cloud=True,
                extra_info_dict=diff_percent_list,
                extra_info_str="Value Changed:",
            )
            diff_output_str += f"\n{diff_url}\n {diff_output} \n"
            rich.print(diff_output_str)
        else:
            diff_output_str += f"\n{diff_url}\n{no_differences_template()}\n"
            rich.print(diff_output_str)

        if log_status_handler:
            log_status_handler.diff_finished(diff_vars.dev_path[-1])
    except BaseException as ex:  # Catch KeyboardInterrupt too
        error = ex
    finally:
        # we don't currently have much of this information
        # but I imagine a future iteration of this _cloud method
        # will poll for results
        if is_tracking_enabled():
            err_message = truncate_error(repr(error))
            event_json = create_end_event_json(
                is_success=error is None,
                runtime_seconds=time.monotonic() - start,
                data_source_1_type="",
                data_source_2_type="",
                table1_count=0,
                table2_count=0,
                diff_count=0,
                error=err_message,
                diff_id=diff_id,
                is_cloud=True,
                org_id=org_meta.org_id,
                org_name=org_meta.org_name,
                user_id=org_meta.user_id,
            )
            send_event_json(event_json)

        if error:
            rich.print(diff_output_str)
            if diff_id:
                diff_url = f"{api.host}/datadiffs/{diff_id}/overview"
                rich.print(f"{diff_url} \n")
            logger.error(error)


def _diff_output_base(dev_path: str, prod_path: str) -> str:
    return f"\n[blue]{prod_path}[/] <> [green]{dev_path}[/] \n"


def _initialize_events(dbt_user_id: Optional[str], dbt_version: Optional[str], dbt_project_id: Optional[str]) -> None:
    set_dbt_user_id(dbt_user_id)
    set_dbt_version(dbt_version)
    set_dbt_project_id(dbt_project_id)
    _email_signup()


def _email_signup() -> None:
    email_regex = r"^[\w\.\+-]+@[\w\.-]+\.\w+$"
    prompt = "\nWould you like to be notified when a new data-diff version is available?\n\nEnter email or leave blank to opt out (we'll only ask once).\n"

    if bool_ask_for_email():
        while True:
            email_input = Prompt.ask(
                prompt=prompt,
                default="",
                show_default=False,
            )
            email = email_input.strip()

            if email == "" or re.match(email_regex, email):
                break

            prompt = ""
            rich.print("[red]Invalid email. Please enter a valid email or leave it blank to opt out.[/]")

        if email:
            event_json = create_email_signup_event_json(email)
            run_as_daemon(send_event_json, event_json)


def _extension_notification() -> None:
    if bool_notify_about_extension():
        message = "\n\nHaving a good time diffing?\n\nMake sure to check out the free Datafold Cloud Trial for an evolved experience:\n\n- value-level diffs\n- column-level lineage\n"
        rich.print(message)
        rich.print(Markdown(f"[Sign Up Here]({DATAFOLD_TRIAL_URL})"))
        rich.print("")
        rich.print(Markdown(f"[Follow the instructions to get started]({DATAFOLD_INSTRUCTIONS_URL})"))
