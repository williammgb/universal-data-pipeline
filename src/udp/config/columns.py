"""Declared storage types for a dataset's columns: the `columns:` block of source.yaml."""

import re
from typing import Annotated

import polars as pl
from pydantic import AfterValidator

SIMPLE_TYPES = ("text", "integer", "float", "boolean", "date", "timestamp", "json")
MAX_PRECISION = 38

_DECIMAL = re.compile(r"decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)")


def decimal_digits(declared: str) -> tuple[int, int] | None:
    """(precision, scale) for a canonical `decimal(P,S)`, None for any other type."""
    match = _DECIMAL.fullmatch(declared)
    return (int(match[1]), int(match[2])) if match else None


def _declared_type(value: str) -> str:
    if value in SIMPLE_TYPES:
        return value
    if value == "decimal":
        raise ValueError(
            "decimal needs its total digits and digits after the point, like decimal(12,2)"
        )
    digits = decimal_digits(value)
    if digits is None:
        allowed = ", ".join([*SIMPLE_TYPES, "decimal(P,S)"])
        raise ValueError(f"unknown type '{value}' (allowed: {allowed})")
    precision, scale = digits
    if not 1 <= precision <= MAX_PRECISION:
        raise ValueError(f"decimal precision must be between 1 and {MAX_PRECISION}")
    if scale > precision:
        raise ValueError("decimal scale must not be larger than its precision")
    return f"decimal({precision},{scale})"


DeclaredType = Annotated[str, AfterValidator(_declared_type)]

WATERMARK_DECLARED_TYPES = ("integer", "date", "timestamp")


def storage_dtype(declared: str) -> pl.DataType:
    """The Polars type a declared column holds once converted."""
    digits = decimal_digits(declared)
    if digits is not None:
        return pl.Decimal(*digits)
    return {
        "text": pl.String(),
        "integer": pl.Int64(),
        "float": pl.Float64(),
        "boolean": pl.Boolean(),
        "date": pl.Date(),
        "timestamp": pl.Datetime("us", "UTC"),
        "json": pl.String(),
    }[declared]
