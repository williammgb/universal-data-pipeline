import re
import unicodedata
from collections.abc import Iterator, Sequence

import polars as pl
import structlog

from udp.names import MAX_IDENTIFIER_BYTES

log = structlog.get_logger(step="transform")

_NOT_IDENTIFIER = re.compile(r"[^a-z0-9_]+")
_FALLBACK = "column"


def _clean_one(name: str) -> str:
    folded = unicodedata.normalize("NFKD", name.casefold())
    ascii_only = "".join(ch for ch in folded if not unicodedata.combining(ch))
    cleaned = _NOT_IDENTIFIER.sub("_", ascii_only).strip("_")
    if not cleaned:
        cleaned = _FALLBACK
    elif not cleaned[0].isalpha():
        cleaned = f"col_{cleaned}"
    return cleaned[:MAX_IDENTIFIER_BYTES].rstrip("_")


def clean_column_names(names: Sequence[str]) -> list[str]:
    """Lowercase snake_case, unique, at most 63 bytes, never starting with '_'."""
    cleaned = [_clean_one(name) for name in names]
    taken: set[str] = set()
    result: list[str] = []
    for name in cleaned:
        candidate = name
        counter = 2
        while candidate in taken:
            suffix = f"_{counter}"
            candidate = name[: MAX_IDENTIFIER_BYTES - len(suffix)].rstrip("_") + suffix
            counter += 1
        taken.add(candidate)
        result.append(candidate)
    return result


def tidy_text(chunk: pl.DataFrame) -> pl.DataFrame:
    """Trim leading and trailing whitespace from every text column, then empty text becomes null.

    Whitespace is what Polars' `str.strip_chars()` removes: the Unicode White_Space characters.
    Trimming first is what makes this idempotent: "  " becomes "" and then null in one pass.
    """
    trimmed = [
        pl.col(name).str.strip_chars() for name, dtype in chunk.schema.items() if dtype == pl.String
    ]
    return chunk.with_columns(pl.when(column != "").then(column) for column in trimmed)


def transform(chunks: Iterator[pl.DataFrame]) -> Iterator[pl.DataFrame]:
    """The common transforms every dataset gets: clean column names, then tidy text."""
    renames: dict[str, str] | None = None
    for chunk in chunks:
        if renames is None:
            renames = dict(zip(chunk.columns, clean_column_names(chunk.columns), strict=True))
            log.info("columns cleaned", columns=list(renames.values()))
        yield tidy_text(chunk.rename(renames))
