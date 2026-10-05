from collections.abc import Iterator
from typing import Literal

import polars as pl

from udp.connectors.base import (
    ConnectionBase,
    DatasetBase,
    ExtractRequest,
    FileVersion,
    SourcePath,
    file_sha256,
)
from udp.errors import ExtractError
from udp.pipeline.transform import clean_column_names

INFER_SCHEMA_ROWS = 10_000


class CsvConnection(ConnectionBase):
    type: Literal["csv"]


class CsvDataset(DatasetBase):
    path: SourcePath


class CsvConnector:
    connection_model = CsvConnection
    dataset_model = CsvDataset

    def file_version(self, request: ExtractRequest[CsvConnection, CsvDataset]) -> FileVersion:
        path = request.source_dir / request.dataset.path
        if not path.is_file():
            raise ExtractError(f"CSV file not found: {path.as_posix()}")
        return FileVersion(request.dataset.path, file_sha256(path))

    def extract(self, request: ExtractRequest[CsvConnection, CsvDataset]) -> Iterator[pl.DataFrame]:
        path = request.source_dir / request.dataset.path
        if not path.is_file():
            raise ExtractError(f"CSV file not found: {path.as_posix()}")
        try:
            text_columns: dict[str, pl.DataType] = {}
            if request.dataset.columns:
                # Declared columns are read as text so type guessing never rounds a value or
                # fails on a late row; the pipeline converts them to their declared type.
                header = pl.scan_csv(path, infer_schema=False).collect_schema().names()
                cleaned = clean_column_names(header)
                text_columns = {
                    raw: pl.String()
                    for raw, name in zip(header, cleaned, strict=True)
                    if name in request.dataset.columns
                }
            frame = pl.scan_csv(
                path, infer_schema_length=INFER_SCHEMA_ROWS, schema_overrides=text_columns
            )
            yielded = False
            for batch in frame.collect_batches(chunk_size=request.chunk_size):
                for chunk in batch.iter_slices(request.chunk_size):
                    yielded = True
                    yield chunk
            if not yielded:
                yield frame.head(0).collect()
        except pl.exceptions.NoDataError:
            # An empty file: no rows, and no header to learn columns from. The declared ones,
            # empty, as an empty JSON file gives; with none declared the load says so.
            yield pl.DataFrame(
                [pl.Series(name, [], dtype=pl.Null) for name in request.dataset.columns]
            )
        except pl.exceptions.PolarsError as error:
            raise ExtractError(f"could not read CSV file {path.as_posix()}: {error}") from error
