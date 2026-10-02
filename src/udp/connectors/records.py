"""Records as parsed from JSON — a REST response or a JSON file — turned into one typed frame."""

import json
from collections.abc import Mapping, Sequence
from typing import Any

import polars as pl

from udp.config.columns import json_series
from udp.errors import ExtractError

INT64_MIN, INT64_MAX = -(2**63), 2**63 - 1

MISSING = object()


def value_at(document: Any, path: str) -> Any:
    """The value at a dotted path of object keys, or MISSING when any step is not there."""
    node = document
    for part in path.split("."):
        if not isinstance(node, dict) or part not in node:
            return MISSING
        node = node[part]
    return node


def _kind(value: Any) -> str:
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int" if INT64_MIN <= value <= INT64_MAX else "big"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "str"
    return "nested"


def _as_text(value: Any) -> str | None:
    if value is None or isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False)


def _as_json(name: str, value: Any) -> str | None:
    if value is None:
        return None
    try:
        return json.dumps(value, ensure_ascii=False, allow_nan=False)
    except ValueError:
        raise ExtractError(
            f"column '{name}' holds NaN or Infinity, which JSON cannot store"
        ) from None


def records_frame(records: Sequence[Mapping[str, Any]]) -> pl.DataFrame:
    """One column per key seen on any record, each typed over the whole dataset.

    A column holding an object or an array anywhere is a JSON column, every value in it JSON;
    whole numbers are integers, numbers floats and true/false booleans when every value is one;
    anything else mixed, and integers beyond 64 bits, are text.
    """
    names: dict[str, None] = {}
    for record in records:
        names.update(dict.fromkeys(record))
    series = []
    for name in names:
        values = [record.get(name) for record in records]
        kinds = {_kind(value) for value in values if value is not None}
        if not kinds:
            series.append(pl.Series(name, values, dtype=pl.Null))
        elif "nested" in kinds:
            series.append(json_series(name, [_as_json(name, v) for v in values]))
        elif kinds == {"int"}:
            series.append(pl.Series(name, values, dtype=pl.Int64))
        elif kinds <= {"int", "float"}:
            floats = [None if v is None else float(v) for v in values]
            series.append(pl.Series(name, floats, dtype=pl.Float64))
        elif kinds == {"bool"}:
            series.append(pl.Series(name, values, dtype=pl.Boolean))
        else:
            series.append(pl.Series(name, [_as_text(v) for v in values], dtype=pl.String))
    return pl.DataFrame(series)
