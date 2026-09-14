import re

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
