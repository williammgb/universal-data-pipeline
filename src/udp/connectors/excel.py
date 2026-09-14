from collections.abc import Iterator
from typing import Literal

import polars as pl

from udp.connectors.base import ConnectionBase, DatasetBase, ExtractRequest, SourcePath
from udp.errors import ExtractError


class ExcelConnection(ConnectionBase):
    type: Literal["excel"]


class ExcelDataset(DatasetBase):
    path: SourcePath
    sheet: str | None = None


class ExcelConnector:
    connection_model = ExcelConnection
    dataset_model = ExcelDataset

    def extract(
        self, request: ExtractRequest[ExcelConnection, ExcelDataset]
    ) -> Iterator[pl.DataFrame]:
        path = request.source_dir / request.dataset.path
        sheet = request.dataset.sheet
        shown = f"{path.as_posix()} (sheet {sheet!r})" if sheet else path.as_posix()
        if not path.is_file():
            raise ExtractError(f"Excel file not found: {path.as_posix()}")
        try:
            if sheet is None:
                frame = pl.read_excel(path, sheet_id=1, engine="calamine", infer_schema_length=None)
            else:
                frame = pl.read_excel(
                    path, sheet_name=sheet, engine="calamine", infer_schema_length=None
                )
        except Exception as error:  # the reader raises its own types for unreadable files
            raise ExtractError(f"could not read Excel file {shown}: {error}") from error
        if frame.height == 0:
            yield frame
            return
        yield from frame.iter_slices(request.chunk_size)
