"""Start a real run that stops for good after its first chunk, so a test can kill it mid-load.

Usage: python stalled_run.py <sources dir> <source> <marker file>
The marker file appears once the first chunk has gone into the database.
"""

import sys
import time
from collections.abc import Iterator
from pathlib import Path

import polars as pl

from udp.config.source import load_source
from udp.connectors import CONNECTORS
from udp.connectors.base import ExtractRequest
from udp.connectors.csv import CsvConnection, CsvConnector, CsvDataset
from udp.pipeline.runner import run_source
from udp.settings import Settings
from udp.storage.postgres import PostgresLoader


class StallAfterFirstChunk(CsvConnector):
    def __init__(self, marker: Path) -> None:
        self._marker = marker

    def extract(self, request: ExtractRequest[CsvConnection, CsvDataset]) -> Iterator[pl.DataFrame]:
        chunks = super().extract(request)
        yield next(chunks)
        self._marker.write_text("first chunk handed over", encoding="utf-8")
        time.sleep(3600)
        yield from chunks


def main() -> None:
    sources_dir, source, marker = Path(sys.argv[1]), sys.argv[2], Path(sys.argv[3])
    CONNECTORS["csv"] = StallAfterFirstChunk(marker)
    config = load_source(sources_dir, source)
    with PostgresLoader(Settings().database_url) as loader:  # type: ignore[call-arg]
        run_source(source, config, sources_dir, loader, chunk_size=10)


if __name__ == "__main__":
    main()
