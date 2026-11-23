class DataDiffDbtProjectVarsNotFoundError(Exception):
    "Raised when an expected dbt_project.yml section is missing."


class DataDiffDbtProfileNotFoundError(Exception):
    "Raised when an expected profiles.yml section is missing."


class DataDiffDbtNoSuccessfulModelsInRunError(Exception):
    "Raised when there are no successful model runs in the run_results.json"


class DataDiffDbtRunResultsVersionError(Exception):
    "Raised when the dbt version in run_results.json is lower than the minimum version."


class DataDiffDbtSelectNoMatchingModelsError(Exception):
    "Raised when the `--select` flag returns no models."


class DataDiffDbtSelectUnexpectedError(Exception):
    "Catch all for unexpected dbt list --select results."


class DataDiffDbtSnowflakeSetConnectionError(Exception):
    "Raised when a dbt snowflake profile has unexpected values."


class DataDiffDbtBigQueryUnsupportedMethodError(Exception):
    "Raised when trying to use an unsupported connection with BigQuery."


class DataDiffDbtRedshiftPasswordOnlyError(Exception):
    "Raised when using a non-password connection method with Redshift."


class DataDiffDbtConnectionNotImplementedError(Exception):
    "Raised when trying to use an unsupported dbt connection method."


class DataDiffDbtCoreNoRunnerError(Exception):
    "Raised when the manifest version >= 1.5, but the dbt-core package is < 1.5. This is an edge case most likely to occur in development."


class DataDiffCustomSchemaNoConfigError(Exception):
    "Raised when a model has a custom schema, but there is no prod_custom_schema config. (And not using --state)."


class DataDiffNoAPIKeyError(Exception):
    "Raised when using --cloud but no API key is present in the DATAFOLD_API_KEY env var or keyring"


class DataDiffNoDatasourceIdError(Exception):
    "Raised when using --cloud but no datasource_id was found in dbt_project.yml"


class DataDiffDatasourceIdNotFoundError(Exception):
    "Raised when using --cloud but the datasource_id is not found for a particular org."


class DataDiffCloudDiffFailed(Exception):
    "Raised when using --cloud and the remote diff fails."


class DataDiffCloudDiffTimedOut(Exception):
    "Raised when using --cloud and the diff did not return finish before the timeout value."


class DataDiffSimpleSelectNotFound(Exception):
    "Raised when using --select on dbt < 1.5 and a model node is not found in the manifest."


class DataDiffMismatchingKeyTypesError(Exception):
    "Raised when the key types of two tables do not match, like VARCHAR and INT."


class DataDiffConsistencyError(ValueError):
    "Raised when the requested consistency mode cannot be honoured for the given databases/algorithm."


class DataDiffUnsupportedConsistencyError(DataDiffConsistencyError, NotImplementedError):
    "Raised when consistency='snapshot' is requested for a database or algorithm that does not support it."


class DataDiffIncompleteResultError(RuntimeError):
    "Raised when consuming or summarising a diff result that was closed before it was fully consumed."


class DataDiffDependencyPolicyError(ValueError):
    "Raised when dbt_diff is given an unknown dependency policy, or one that cannot be honoured."


class DataDiffDependencyGraphError(Exception):
    "Raised when the selected dependency subgraph is invalid (dangling model reference or cycle)."


class DataDiffSelectionConflictError(Exception):
    "Raised when the selected models cannot be identified unambiguously (duplicates, ephemeral, etc.)."


class DataDiffTableNotFoundError(RuntimeError):
    "Raised by a database adapter when a relation does not exist (as opposed to being inaccessible)."


class DataDiffTableNotReadableError(RuntimeError):
    "Raised by a database adapter when a relation exists but its columns cannot be read by this user."
