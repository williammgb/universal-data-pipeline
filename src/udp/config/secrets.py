import os
import re
from collections.abc import Mapping
from pathlib import Path

from dotenv import dotenv_values

_NAME = re.compile(r"[A-Z_][A-Z0-9_]*")


def read_environment(dotenv: Path) -> dict[str, str]:
    """Values from a .env file, with the real environment taking precedence."""
    values = {key: value for key, value in dotenv_values(dotenv).items() if value is not None}
    values.update(os.environ)
    return values


def _fill_string(value: str, env: Mapping[str, str], where: str, problems: list[str]) -> str:
    parts: list[str] = []
    position = 0
    while (start := value.find("${", position)) >= 0:
        parts.append(value[position:start])
        end = value.find("}", start + 2)
        if end < 0:
            problems.append(f"{where}: unclosed '${{' reference")
            parts.append(value[start:])
            return "".join(parts)
        name = value[start + 2 : end]
        if not _NAME.fullmatch(name):
            problems.append(
                f"{where}: '${{{name}}}' is not a valid reference "
                "(use uppercase letters, digits and _)"
            )
        elif not env.get(name):
            problems.append(f"{where}: environment variable {name} is not set")
        else:
            parts.append(env[name])
        position = end + 1
    parts.append(value[position:])
    return "".join(parts)


def fill_references(data: object, env: Mapping[str, str]) -> tuple[object, list[str]]:
    """Replace ${NAME} inside string values; returns the filled data and every problem found."""
    problems: list[str] = []

    def walk(node: object, where: str) -> object:
        if isinstance(node, str):
            return _fill_string(node, env, where, problems)
        if isinstance(node, dict):
            return {
                key: walk(value, f"{where}.{key}" if where else str(key))
                for key, value in node.items()
            }
        if isinstance(node, list):
            return [walk(item, f"{where}[{index}]") for index, item in enumerate(node)]
        return node

    return walk(data, ""), problems
