from collections.abc import Iterator

import polars as pl
import structlog

from udp.errors import ValidationError

log = structlog.get_logger(step="validate")


def validate(chunks: Iterator[pl.DataFrame]) -> Iterator[pl.DataFrame]:
    schema: pl.Schema | None = None
    for chunk in chunks:
        if schema is None:
            if chunk.width == 0:
                raise ValidationError("the source has no columns")
            schema = chunk.schema
            log.info("schema checked", columns=dict((k, str(v)) for k, v in schema.items()))
        elif chunk.schema != schema:
            raise ValidationError(f"a chunk has schema {chunk.schema}, the first had {schema}")
        yield chunk
