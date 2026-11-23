"""Subprocess worker: run one dependency-policy comparison and print JSONL.

Used by the interleaved-call test so that two truly independent calls run at
the same time in separate interpreters.  Global test scaffolding (stdout
redirection, patches) would otherwise leak between concurrent in-process calls.
"""

import io
import json
import sys
from contextlib import redirect_stdout

from data_diff import tracking
from data_diff.dbt import dbt_diff

from tests.test_dbt_dependency_pg import FakeDbtParser, _node, manifest_for

tracking.disable_tracking()


def main(argv):
    model_name = argv[1]
    schema = argv[2]
    node = _node(model_name, schema=schema)
    parser = FakeDbtParser(manifest_for([node]), threads=1)
    stdout = io.StringIO()
    with redirect_stdout(stdout):
        dbt_diff(json_output=True, dependency_policy="independent", _parser=parser)
    records = [json.loads(line) for line in stdout.getvalue().splitlines() if line.startswith("{")]
    print(json.dumps(records), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
