from pathlib import Path
from typing import Any

import pydantic
import yaml
from pydantic import BaseModel, ConfigDict, Field

from udp.connectors import CONNECTORS
from udp.connectors.base import ConnectionBase, DatasetBase
from udp.errors import ConfigError
from udp.names import MAX_IDENTIFIER_BYTES, name_problem, table_name

SOURCE_FILE = "source.yaml"


class SourceConfig[C: ConnectionBase, D: DatasetBase](BaseModel):
    model_config = ConfigDict(extra="forbid")

    connection: C
    datasets: list[D] = Field(min_length=1)


def _location(loc: tuple[int | str, ...]) -> str:
    text = ""
    for part in loc:
        text += f"[{part}]" if isinstance(part, int) else (f".{part}" if text else part)
    return text


def load_source(sources_dir: Path, name: str) -> SourceConfig[Any, Any]:
    """Read and check sources/<name>/source.yaml. Every problem raises ConfigError."""
    path = sources_dir / name / SOURCE_FILE
    shown = path.as_posix()

    def fail(*problems: str) -> ConfigError:
        return ConfigError("\n".join(f"{shown}: {problem}" for problem in problems))

    problem = name_problem(name)
    if problem is not None:
        raise fail(f"source name '{name}' {problem}")
    if not path.is_file():
        raise fail("file not found")

    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as error:
        mark = getattr(error, "problem_mark", None)
        where = f"line {mark.line + 1}: " if mark is not None else ""
        raise fail(f"{where}invalid YAML: {getattr(error, 'problem', error)}") from error

    if not isinstance(data, dict):
        raise fail("expected a mapping with 'connection' and 'datasets'")
    connection = data.get("connection")
    if not isinstance(connection, dict) or "type" not in connection:
        raise fail("connection.type: Field required")
    connector = CONNECTORS.get(str(connection["type"]))
    if connector is None:
        known = ", ".join(sorted(CONNECTORS))
        raise fail(f"connection.type: unknown connector '{connection['type']}' (known: {known})")

    model = SourceConfig[connector.connection_model, connector.dataset_model]  # type: ignore[name-defined]
    try:
        config: SourceConfig[Any, Any] = model.model_validate(data)
    except pydantic.ValidationError as error:
        raise fail(*(f"{_location(e['loc'])}: {e['msg']}" for e in error.errors())) from error

    problems: list[str] = []
    seen: set[str] = set()
    for index, dataset in enumerate(config.datasets):
        if dataset.name in seen:
            problems.append(f"datasets[{index}].name: duplicate dataset '{dataset.name}'")
        seen.add(dataset.name)
        table = table_name(name, dataset.name)
        if len(table.encode()) > MAX_IDENTIFIER_BYTES:
            problems.append(
                f"datasets[{index}].name: table name '{table}' is longer than "
                f"{MAX_IDENTIFIER_BYTES} bytes"
            )
    if problems:
        raise fail(*problems)
    return config
