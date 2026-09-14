from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import polars as pl
import structlog

from udp.connectors.base import Connector, ExtractRequest
from udp.errors import ExtractError

log = structlog.get_logger(step="extract")


@dataclass
class RowCounter:
    rows: int = 0


def extract(
    connector: Connector[Any, Any], request: ExtractRequest[Any, Any], counter: RowCounter
) -> Iterator[pl.DataFrame]:
    received = False
    for chunk in connector.extract(request):
        received = True
        counter.rows += chunk.height
        yield chunk
    if not received:
        raise ExtractError("the source returned no data, not even column names")
    log.info("extracted", rows=counter.rows)
