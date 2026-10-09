"""Declared column types: convert each declared column exactly, or quarantine the row.

A column read as a type that can never become the declared one (a date declared integer)
fails the run, because that is a config mistake. A single value that does not fit (text
"12.345" in decimal(12,2), "2026-02-30" in a date) sends only its row to quarantine, with a
reason naming every such column. Digits are ASCII only, dates and timestamps ISO 8601 only,
with hours 00-23 and seconds 00-59.
"""

import json
from collections.abc import Callable, Iterator, Mapping
from decimal import ROUND_HALF_UP, Decimal, localcontext
from typing import Any, cast

import polars as pl
import structlog

from udp.config.columns import as_json, decimal_digits, is_json, json_text, storage_dtype
from udp.errors import ValidationError
from udp.quality.quarantine import quarantine
from udp.storage.loader import RunFindings

log = structlog.get_logger(step="columns")

_INTEGER_TEXT = r"^[+-]?[0-9]+$"
_PLAIN_NUMBER = r"^[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)$"
_NUMBER = r"^[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?$"
_FLOAT_WORD = r"^[+-]?([Nn][Aa][Nn]|[Ii][Nn][Ff]|[Ii][Nn][Ff][Ii][Nn][Ii][Tt][Yy])$"
_DATE_TEXT = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$"
_TIMESTAMP_TEXT = (
    r"^(?<date>[0-9]{4}-[0-9]{2}-[0-9]{2})"
    r"(?:[T ](?<hour>[0-9]{2}):(?<minute>[0-9]{2})"
    r"(?::(?<second>[0-9]{2})(?:\.(?<fraction>[0-9]{1,6}))?)?"
    r"(?<offset>Z|[+-][0-9]{2}:[0-9]{2})?)?$"
)
_TRUE = ["true", "t", "yes", "y", "1"]
_FALSE = ["false", "f", "no", "n", "0"]
_UTC = pl.Datetime("us", "UTC")


def _is_integer(dtype: pl.DataType) -> bool:
    return dtype.is_integer()


def _is_number(dtype: pl.DataType) -> bool:
    return dtype.is_integer() or dtype.is_float() or isinstance(dtype, pl.Decimal)


def _is_text_or_null(dtype: pl.DataType) -> bool:
    return isinstance(dtype, pl.String | pl.Null)


def _is_temporal(dtype: pl.DataType) -> bool:
    return isinstance(dtype, pl.Date | pl.Datetime)


_READABLE_AS: dict[str, Callable[[pl.DataType], bool]] = {
    "text": lambda t: (
        _is_text_or_null(t)
        or _is_number(t)
        or isinstance(t, pl.Boolean | pl.Date | pl.Datetime | pl.Time)
        or is_json(t)
    ),
    "integer": lambda t: _is_text_or_null(t) or _is_number(t),
    "decimal": lambda t: _is_text_or_null(t) or _is_number(t),
    "float": lambda t: _is_text_or_null(t) or _is_number(t),
    "boolean": lambda t: _is_text_or_null(t) or _is_integer(t) or isinstance(t, pl.Boolean),
    "date": lambda t: _is_text_or_null(t) or _is_temporal(t),
    "timestamp": lambda t: _is_text_or_null(t) or _is_temporal(t),
    "json": lambda t: (
        _is_text_or_null(t)
        or _is_integer(t)
        or t.is_float()
        or isinstance(t, pl.Boolean)
        or is_json(t)
    ),
}


def _family(declared: str) -> str:
    return "decimal" if decimal_digits(declared) else declared


def check_declared_columns(schema: pl.Schema, declared: Mapping[str, str]) -> None:
    for name, kind in declared.items():
        if name not in schema:
            raise ValidationError(f"declared column '{name}' is not in the data")
        if not _READABLE_AS[_family(kind)](schema[name]):
            shown = "json" if is_json(schema[name]) else schema[name]
            raise ValidationError(
                f"column '{name}' is read as {shown} and cannot be stored as {kind}"
            )


def apply_column_types(
    chunks: Iterator[pl.DataFrame], declared: Mapping[str, str], findings: RunFindings
) -> Iterator[pl.DataFrame]:
    """Every declared column in its declared type; rows with a value that does not fit go to
    quarantine as they were read."""
    checked = False
    for chunk in chunks:
        if not checked:
            check_declared_columns(chunk.schema, declared)
            checked = True
            log.info("columns declared", columns=dict(declared))
        converted = []
        unfit = []
        for name, kind in declared.items():
            values = convert(chunk[name], kind)
            converted.append(values)
            unfit.append(unfit_values(chunk[name], values))
        result = chunk.with_columns(converted)
        flags = pl.DataFrame(unfit)
        rejected = flags.select(pl.any_horizontal(pl.all())).to_series()
        if rejected.any():
            reasons = flags.filter(rejected).select(
                pl.concat_str(
                    [
                        pl.when(pl.col(name)).then(pl.lit(f"column '{name}' is not {kind}"))
                        for name, kind in declared.items()
                    ],
                    separator="; ",
                    ignore_nulls=True,
                )
            )
            quarantine(findings, chunk.filter(rejected), reasons.to_series())
            result = result.filter(~rejected)
        yield result


def unfit_values(series: pl.Series, converted: pl.Series) -> pl.Series:
    """True where a value is present but did not survive `convert`: the one definition of an
    invalid value, shared by the load's quarantine and the profile's data-quality count."""
    return (converted.is_null() & series.is_not_null()).alias(series.name)


def convert(series: pl.Series, declared: str) -> pl.Series:
    """The series in the declared type; null where a value does not convert exactly."""
    digits = decimal_digits(declared)
    if isinstance(series.dtype, pl.Null):
        result = series.cast(storage_dtype(declared))
    elif digits is not None:
        result = _to_decimal(series, *digits)
    else:
        result = _CONVERTERS[declared](series)
    return result.alias(series.name)


def _select(series: pl.Series, expression: pl.Expr) -> pl.Series:
    return series.to_frame("v").select(expression.alias("v")).to_series()


def _to_text(series: pl.Series) -> pl.Series:
    dtype = series.dtype
    if isinstance(dtype, pl.Datetime):
        pattern = "%Y-%m-%dT%H:%M:%S%.f" + ("%:z" if dtype.time_zone else "")
        return _select(series, pl.col("v").dt.to_string(pattern))
    if is_json(dtype):
        return _select(series, json_text(pl.col("v")))
    return series.cast(pl.String)


def _to_integer(series: pl.Series) -> pl.Series:
    value = pl.col("v")
    dtype = series.dtype
    expression: pl.Expr
    if isinstance(dtype, pl.String):
        expression = pl.when(value.str.contains(_INTEGER_TEXT)).then(
            value.str.strip_prefix("+").cast(pl.Int64, strict=False)
        )
    elif dtype.is_float():
        whole = value.is_finite() & (value == value.floor())
        in_range = (value >= -(2.0**63)) & (value < 2.0**63)
        expression = pl.when(whole & in_range).then(value.cast(pl.Int64, strict=False))
    elif isinstance(dtype, pl.Decimal):
        text = value.cast(pl.String)
        expression = pl.when(text.str.contains(r"^-?[0-9]+(\.0*)?$")).then(
            text.str.extract(r"^(-?[0-9]+)", 1).cast(pl.Int64, strict=False)
        )
    else:
        expression = value.cast(pl.Int64, strict=False)
    return _select(series, expression)


def _plain_decimal_text(text: str, precision: int, scale: int) -> str | None:
    """Text with an exponent as plain digits, or None when it does not fit decimal(P,S)."""
    sign, digits, raw_exponent = Decimal(text).as_tuple()
    exponent = cast(int, raw_exponent)  # finite: the text matched _NUMBER
    kept = list(digits)
    while len(kept) > 1 and kept[-1] == 0:
        kept.pop()
        exponent += 1
    if not any(kept):
        return "0"
    places = max(0, -exponent)
    integer_digits = len(kept) + exponent
    if places > scale or integer_digits > precision - scale:
        return None
    return format(Decimal((sign, tuple(kept), exponent)), "f")


def _rounded_text(text: str, precision: int, scale: int) -> str | None:
    """A float's shortest text rounded half away from zero to the scale, or None if too big."""
    value = Decimal(text)
    if value.adjusted() >= precision - scale:
        return None
    with localcontext() as context:
        context.prec = precision + 2
        return format(value.quantize(Decimal(1).scaleb(-scale), ROUND_HALF_UP), "f")


def _to_decimal(series: pl.Series, precision: int, scale: int) -> pl.Series:
    # A copy, because scatter below writes in place and must not touch the chunk's column.
    text = series.cast(pl.String).clone()
    if series.dtype.is_float():
        # A float is binary, so 731.94 from a spreadsheet arrives as 731.9399999999999: its
        # extra digits are how it was stored, not what was typed, and it is rounded to the
        # scale. Text is never rounded — "12.345" in decimal(12,2) is refused.
        fraction = text.str.extract(r"\.([0-9]*)", 1).str.len_bytes().fill_null(0)
        long = text.str.contains(_NUMBER) & ((fraction > scale) | text.str.contains(r"[eE]"))
        long = long.fill_null(False)
        if long.any():
            rows = long.arg_true()
            rounded = [_rounded_text(value, precision, scale) for value in text.filter(long)]
            text = text.scatter(rows, rounded)
    exponent = (text.str.contains(r"[eE]") & text.str.contains(_NUMBER)).fill_null(False)
    if exponent.any():
        rows = exponent.arg_true()
        plain = [_plain_decimal_text(value, precision, scale) for value in text.filter(exponent)]
        text = text.scatter(rows, plain)
    parts = text.str.extract_groups(r"^([+-]?)([0-9]*)\.?([0-9]*)$")
    sign = parts.struct.field("1")
    integer = parts.struct.field("2").str.strip_chars_start("0")
    fraction = parts.struct.field("3").str.strip_chars_end("0")
    fits = (
        text.str.contains(_PLAIN_NUMBER)
        & (integer.str.len_bytes() <= precision - scale)
        & (fraction.str.len_bytes() <= scale)
    )
    frame = pl.DataFrame({"sign": sign, "integer": integer, "fraction": fraction, "fits": fits})
    normalised = frame.select(
        pl.when(pl.col("fits"))
        .then(
            pl.concat_str(
                pl.when(pl.col("sign") == "-").then(pl.lit("-")).otherwise(pl.lit("")),
                pl.when(pl.col("integer") == "").then(pl.lit("0")).otherwise(pl.col("integer")),
                pl.when(pl.col("fraction") == "")
                .then(pl.lit(""))
                .otherwise(pl.lit(".") + pl.col("fraction")),
            )
        )
        .alias("v")
    ).to_series()
    return normalised.cast(pl.Decimal(precision, scale), strict=False)


def _to_float(series: pl.Series) -> pl.Series:
    value = pl.col("v")
    dtype = series.dtype
    expression: pl.Expr
    if isinstance(dtype, pl.String):
        parsed = value.cast(pl.Float64, strict=False)
        expression = (
            pl.when(value.str.contains(_FLOAT_WORD))
            .then(parsed)
            .when(value.str.contains(_NUMBER) & parsed.is_finite())
            .then(parsed)
        )
    elif isinstance(dtype, pl.Int64 | pl.UInt64):
        limit = 2**53
        expression = pl.when((value <= limit) & (value >= -limit)).then(value.cast(pl.Float64))
    elif isinstance(dtype, pl.Decimal):
        expression = value.cast(pl.String).cast(pl.Float64)
    else:
        expression = value.cast(pl.Float64)
    return _select(series, expression)


def _to_boolean(series: pl.Series) -> pl.Series:
    value = pl.col("v")
    dtype = series.dtype
    expression: pl.Expr
    if isinstance(dtype, pl.String):
        lower = value.str.to_lowercase()
        ascii_word = value.str.contains(r"^[A-Za-z01]+$")
        expression = (
            pl.when(ascii_word & lower.is_in(_TRUE))
            .then(pl.lit(True))
            .when(ascii_word & lower.is_in(_FALSE))
            .then(pl.lit(False))
        )
    elif dtype.is_integer():
        expression = pl.when(value == 1).then(pl.lit(True)).when(value == 0).then(pl.lit(False))
    else:
        expression = value
    return _select(series, expression)


def _in_utc(value: pl.Expr, dtype: pl.Datetime) -> pl.Expr:
    if dtype.time_zone:
        return value.dt.convert_time_zone("UTC")
    return value.dt.replace_time_zone("UTC")


def _to_date(series: pl.Series) -> pl.Series:
    value = pl.col("v")
    dtype = series.dtype
    expression: pl.Expr
    if isinstance(dtype, pl.String):
        parsed = value.str.to_date("%Y-%m-%d", strict=False)
        expression = pl.when(value.str.contains(_DATE_TEXT) & (parsed.dt.year() >= 1)).then(parsed)
    elif isinstance(dtype, pl.Datetime):
        utc = _in_utc(value, dtype)
        midnight = (
            (utc.dt.hour() == 0)
            & (utc.dt.minute() == 0)
            & (utc.dt.second() == 0)
            & (utc.dt.nanosecond() == 0)
        )
        expression = pl.when(midnight).then(utc.dt.date())
    else:
        expression = value
    return _select(series, expression)


def _to_timestamp(series: pl.Series) -> pl.Series:
    value = pl.col("v")
    dtype = series.dtype
    expression: pl.Expr
    if isinstance(dtype, pl.String):
        return _timestamp_from_text(series)
    if isinstance(dtype, pl.Datetime):
        utc = _in_utc(value, dtype)
        exact = utc.dt.nanosecond() % 1000 == 0 if dtype.time_unit == "ns" else pl.lit(True)
        expression = pl.when(exact).then(utc.cast(_UTC))
    else:
        expression = value.cast(pl.Datetime("us")).dt.replace_time_zone("UTC")
    return _select(series, expression)


def _timestamp_from_text(series: pl.Series) -> pl.Series:
    parts = series.str.extract_groups(_TIMESTAMP_TEXT).struct.unnest()
    frame = parts.with_columns(pl.Series("text", series))
    hour = pl.col("hour").fill_null("00")
    minute = pl.col("minute").fill_null("00")
    second = pl.col("second").fill_null("00")
    fraction = pl.col("fraction").fill_null("").str.pad_end(6, "0")
    naive = pl.concat_str(
        pl.col("date"),
        pl.lit("T"),
        hour,
        pl.lit(":"),
        minute,
        pl.lit(":"),
        second,
        pl.lit("."),
        fraction,
    ).str.to_datetime("%Y-%m-%dT%H:%M:%S%.f", time_unit="us", strict=False)
    offset = pl.col("offset")
    offset_hours = offset.str.slice(1, 2).cast(pl.Int64, strict=False)
    offset_minutes = offset.str.slice(4, 2).cast(pl.Int64, strict=False)
    offset_sign = pl.when(offset.str.starts_with("-")).then(-1).otherwise(1)
    shift = (
        pl.when(offset.is_null() | (offset == "Z"))
        .then(pl.lit(0))
        .otherwise(offset_sign * (offset_hours * 60 + offset_minutes))
    )
    valid_offset = (
        offset.is_null() | (offset == "Z") | ((offset_hours <= 23) & (offset_minutes <= 59))
    )
    utc = (naive - pl.duration(minutes=shift)).dt.replace_time_zone("UTC")
    fits = (
        pl.col("date").is_not_null()
        & (second.cast(pl.Int64, strict=False) < 60)
        & valid_offset
        & naive.is_not_null()
        & (naive.dt.year() >= 1)
        & (utc.dt.year() >= 1)
        & (utc.dt.year() <= 9999)
    )
    return frame.select(pl.when(fits).then(utc).alias("v")).to_series()


def _json_valid(text: str) -> bool:
    def refuse(constant: str) -> Any:
        raise ValueError(constant)

    try:
        json.loads(text, parse_constant=refuse)
    except ValueError:
        return False
    return True


def _to_json(series: pl.Series) -> pl.Series:
    value = pl.col("v")
    dtype = series.dtype
    expression: pl.Expr
    if is_json(dtype):
        return series
    if isinstance(dtype, pl.String):
        valid = [text for text in series.drop_nulls().unique() if _json_valid(text)]
        expression = pl.when(value.is_in(pl.Series(valid, dtype=pl.String).implode())).then(value)
    elif dtype.is_float():
        expression = pl.when(value.is_finite()).then(value.cast(pl.String))
    else:
        expression = value.cast(pl.String)
    return _select(series, as_json(expression))


_CONVERTERS: dict[str, Callable[[pl.Series], pl.Series]] = {
    "text": _to_text,
    "integer": _to_integer,
    "float": _to_float,
    "boolean": _to_boolean,
    "date": _to_date,
    "timestamp": _to_timestamp,
    "json": _to_json,
}
