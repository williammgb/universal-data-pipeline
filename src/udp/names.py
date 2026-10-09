import re
from enum import StrEnum

MAX_IDENTIFIER_BYTES = 63
RESERVED_COLUMNS = ("_run_id", "_loaded_at", "_record_hash")

_NAME = re.compile(r"^[a-z][a-z0-9_]*$")


def name_problem(name: str) -> str | None:
    """Why a source or dataset name is not allowed, or None when it is."""
    if not _NAME.fullmatch(name):
        return "must start with a lowercase letter and contain only a-z, 0-9 and _"
    if "__" in name:
        return "must not contain '__'"
    if name.endswith("_"):
        return "must not end with '_'"
    if len(name.encode()) > MAX_IDENTIFIER_BYTES:
        return f"must be at most {MAX_IDENTIFIER_BYTES} bytes"
    return None


def table_name(source: str, dataset: str) -> str:
    return f"{source}__{dataset}"


class Stage(StrEnum):
    """A dataset's three copies; each is the PostgreSQL schema of the same name.

    RAW is what was ingested and is only ever appended to. STAGING is the working copy one
    pipeline run transforms, dropped when that run ends. CLEAN is the last successful run's result.
    """

    RAW = "raw"
    STAGING = "staging"
    CLEAN = "clean"


def stage_table(stage: Stage, source: str, dataset: str) -> tuple[str, str]:
    """The schema and table a dataset's stage lives in: `<stage>.<source>__<dataset>`.

    The table is named exactly as `table_name` names the V1 table in `datasets`, under the same
    limit. Raises ValueError for a name the rules refuse or a table name over the limit, so no
    stage table can be created under any other name.
    """
    for kind, name in (("source", source), ("dataset", dataset)):
        problem = name_problem(name)
        if problem is not None:
            raise ValueError(f"{kind} name '{name}' {problem}")
    table = table_name(source, dataset)
    if len(table.encode()) > MAX_IDENTIFIER_BYTES:
        raise ValueError(f"table name '{table}' is longer than {MAX_IDENTIFIER_BYTES} bytes")
    return Stage(stage).value, table
