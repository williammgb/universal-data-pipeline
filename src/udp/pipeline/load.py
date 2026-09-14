from collections.abc import Iterator
from datetime import datetime
from hashlib import sha256
from uuid import UUID

import polars as pl
import structlog

from udp.names import RESERVED_COLUMNS
from udp.storage.loader import LoadTransaction

log = structlog.get_logger(step="load")


def record_hashes(frame: pl.DataFrame) -> pl.Series:
    """SHA-256 of each row's source columns as a JSON object with sorted keys.

    Floats are encoded as text first, because JSON has no NaN or infinity and would
    turn both into null.
    """
    columns = sorted(name for name in frame.columns if name not in RESERVED_COLUMNS)
    values = [
        pl.col(name).cast(pl.String) if frame.schema[name].is_float() else pl.col(name)
        for name in columns
    ]
    encoded = frame.select(pl.struct(values).struct.json_encode()).to_series()
    return pl.Series(
        "_record_hash", [sha256(row.encode()).hexdigest() for row in encoded], dtype=pl.String
    )


def with_platform_columns(chunk: pl.DataFrame, run_id: UUID, loaded_at: datetime) -> pl.DataFrame:
    return chunk.with_columns(
        pl.lit(str(run_id), dtype=pl.String).alias("_run_id"),
        pl.lit(loaded_at, dtype=pl.Datetime("us", "UTC")).alias("_loaded_at"),
        record_hashes(chunk),
    )


def load(
    transaction: LoadTransaction,
    table: str,
    chunks: Iterator[pl.DataFrame],
    run_id: UUID,
    loaded_at: datetime,
) -> int:
    rows = transaction.replace_table(
        table, (with_platform_columns(chunk, run_id, loaded_at) for chunk in chunks)
    )
    log.info("loaded", table=f"datasets.{table}", rows=rows)
    return rows
