"""`udp new`: a source.yaml written from the data files already in a source's folder.

Every refusal is a ConfigError whose message says what to do, and nothing is written when one is
raised.
"""

import errno
import json
import re
from pathlib import Path

from udp.config.source import SOURCE_FILE
from udp.errors import ConfigError
from udp.names import MAX_IDENTIFIER_BYTES, name_problem, table_name
from udp.pipeline.transform import clean_column_names

KINDS = {".csv": "csv", ".xlsx": "excel", ".xls": "excel", ".json": "json", ".jsonl": "json"}
HEADER = "# Made by udp new. Every setting: docs/adding-a-source.md"

_PLAIN = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_./-]*")
_READ_ONLY = {errno.EROFS, errno.EACCES, errno.EPERM}


def find_data_files(folder: Path) -> list[Path]:
    """The data files directly in `folder`, then those directly in `folder/data`, by name."""
    found: list[Path] = []
    for place in (folder, folder / "data"):
        if place.is_dir():
            files = [p for p in place.iterdir() if p.is_file() and p.suffix.lower() in KINDS]
            found += sorted(files, key=lambda p: p.name)
    return found


def _names(files: list[Path]) -> list[str]:
    return clean_column_names([path.stem for path in files])


def _scalar(text: str) -> str:
    # A JSON string is a YAML double-quoted scalar, so a path with spaces or '#' reads back as is.
    return text if _PLAIN.fullmatch(text) else json.dumps(text, ensure_ascii=False)


def source_yaml(source: str, files: list[Path]) -> str:
    """A source.yaml's text, one dataset per file; `files` are relative to the source's folder."""
    kinds: dict[str, Path] = {}
    for path in files:
        kinds.setdefault(KINDS[path.suffix.lower()], path)
    if not kinds:
        raise ConfigError("No data files to describe.")
    if len(kinds) > 1:
        found = [f"{kind} ({path.as_posix()})" for kind, path in kinds.items()]
        listed = ", ".join(found[:-1]) + " and " + found[-1]
        raise ConfigError(
            f"A source reads one kind of file; found {listed}. "
            "Put each kind in its own folder under sources/."
        )
    names = _names(files)
    problems: list[str] = []
    for path, name in zip(files, names, strict=True):
        problem = name_problem(name)
        table = table_name(source, name)
        if problem is None and len(table.encode()) > MAX_IDENTIFIER_BYTES:
            problem = f"makes the table name '{table}', longer than {MAX_IDENTIFIER_BYTES} bytes"
        if problem is not None:
            problems.append(f"dataset name '{name}' (from {path.as_posix()}) {problem}")
    if problems:
        raise ConfigError("\n".join(problems))
    (kind,) = kinds
    lines = [HEADER, "connection:", f"  type: {kind}", "datasets:"]
    for path, name in zip(files, names, strict=True):
        path_line = f"    path: {_scalar(path.as_posix())}"
        lines += [f"  - name: {name}", path_line, "    load_mode: full"]
    return "\n".join(lines) + "\n"


def write_source(sources_dir: Path, source: str) -> list[str]:
    """Write `sources_dir/source/source.yaml` from the folder's data files; the dataset names."""
    folder = sources_dir / source
    shown = folder.as_posix()
    target = folder / SOURCE_FILE
    problem = name_problem(source)
    if problem is not None:
        raise ConfigError(f"source name '{source}' {problem}")
    if not folder.is_dir():
        raise ConfigError(
            f"No folder {shown}. Make it and put your data files in it (or in {shown}/data/), "
            f"then run udp new {source} again."
        )
    exists = ConfigError(f"{target.as_posix()} already exists; udp new never overwrites it.")
    if target.exists():
        raise exists
    files = [path.relative_to(folder) for path in find_data_files(folder)]
    if not files:
        raise ConfigError(f"No .csv, .xlsx, .xls, .json or .jsonl file in {shown} or {shown}/data.")
    text = source_yaml(source, files)
    created = False
    try:
        with target.open("x", encoding="utf-8", newline="\n") as file:
            created = True
            file.write(text)
    except FileExistsError as error:
        raise exists from error
    except OSError as error:
        if created:
            target.unlink(missing_ok=True)
        reason = "read-only. Use udp new." if error.errno in _READ_ONLY else f"{error.strerror}."
        raise ConfigError(f"Cannot write {target.as_posix()}: {reason}") from error
    return _names(files)
