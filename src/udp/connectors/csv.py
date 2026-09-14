from collections.abc import Iterator
from pathlib import PurePosixPath, PureWindowsPath
from typing import Literal

import polars as pl
from pydantic import field_validator

from udp.connectors.base import ConnectionBase, DatasetBase, ExtractRequest
from udp.errors import ExtractError

INFER_SCHEMA_ROWS = 10_000


class CsvConnection(ConnectionBase):
    type: Literal["csv"]


class CsvDataset(DatasetBase):
    path: str

    @field_validator("path")
    @classmethod
    def _inside_source_folder(cls, value: str) -> str:
        path = PureWindowsPath(value)
        if path.drive or path.root or PurePosixPath(value).is_absolute():
            raise ValueError("must be a relative path inside the source folder")
        if ".." in path.parts:
            raise ValueError("must not contain '..'")
        return value


class CsvConnector:
    connection_model = CsvConnection
    dataset_model = CsvDataset

    def extract(self, request: ExtractRequest[CsvConnection, CsvDataset]) -> Iterator[pl.DataFrame]:
        path = request.source_dir / request.dataset.path
        if not path.is_file():
            raise ExtractError(f"CSV file not found: {path.as_posix()}")
        frame = pl.scan_csv(path, infer_schema_length=INFER_SCHEMA_ROWS)
        try:
            yielded = False
            for batch in frame.collect_batches(chunk_size=request.chunk_size):
                for chunk in batch.iter_slices(request.chunk_size):
                    yielded = True
                    yield chunk
            if not yielded:
                yield frame.head(0).collect()
        except pl.exceptions.PolarsError as error:
            raise ExtractError(f"could not read CSV file {path.as_posix()}: {error}") from error
