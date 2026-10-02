"""JSON files: a file, or a folder of them, each one JSON document or one document per line.

Every file is read and parsed before the first row is handed on, so a malformed file stops the
run before anything is loaded. The records are typed over the whole dataset, the same way an
API's are, so records of different shapes still share one schema.
"""

import hashlib
import json
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any, Literal

import polars as pl

from udp.connectors.base import (
    ConnectionBase,
    DatasetBase,
    ExtractRequest,
    FileVersion,
    SourcePath,
    file_sha256,
)
from udp.connectors.records import MISSING, records_frame, value_at
from udp.errors import ExtractError

JSON_SUFFIXES = (".json", ".jsonl", ".ndjson")


class JsonConnection(ConnectionBase):
    type: Literal["json"]


class JsonDataset(DatasetBase):
    # A file, or a folder whose .json, .jsonl and .ndjson files are read in name order.
    path: SourcePath
    # Where each document keeps its list of records, as object keys joined by dots.
    records_path: str | None = None


def _refuse_constant(constant: str) -> Any:
    raise ValueError(f"{constant} is not a JSON value")


def _parse(text: str) -> Any:
    return json.loads(text, parse_constant=_refuse_constant)


def _problem(error: ValueError) -> str:
    if isinstance(error, json.JSONDecodeError):
        return f"line {error.lineno} column {error.colno}: {error.msg}"
    return str(error)


def documents(path: Path) -> list[tuple[str, Any]]:
    """Every document in the file, each with where it was: the whole file, or each line of
    newline-delimited JSON. An empty file has none."""
    shown = path.as_posix()
    try:
        text = path.read_bytes().decode("utf-8-sig")
    except UnicodeDecodeError as error:
        raise ExtractError(
            f"could not read JSON file {shown}: not UTF-8 at byte {error.start}"
        ) from None
    if not text.strip():
        return []
    try:
        return [(shown, _parse(text))]
    except ValueError as error:
        # More than one value in the file: newline-delimited JSON, read a line at a time.
        if not (isinstance(error, json.JSONDecodeError) and error.msg == "Extra data"):
            raise ExtractError(f"could not read JSON file {shown}: {_problem(error)}") from None
    found = []
    for number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            found.append((f"{shown} line {number}", _parse(line)))
        except ValueError as error:
            problem = _problem(error)
            if isinstance(error, json.JSONDecodeError):
                problem = f"line {number} column {error.colno}: {error.msg}"
            raise ExtractError(f"could not read JSON file {shown}: {problem}") from None
    return found


def document_records(
    document: Any, where: str, records_path: str | None
) -> list[Mapping[str, Any]]:
    """The records a document holds: its list of objects, or one object as one record."""
    found = document if records_path is None else value_at(document, records_path)
    if found is MISSING:
        raise ExtractError(f"JSON file {where}: no '{records_path}' in the document")
    if isinstance(found, dict):
        return [found]
    if isinstance(found, list) and all(isinstance(record, dict) for record in found):
        return found
    at = f"'{records_path}'" if records_path is not None else "the document"
    raise ExtractError(f"JSON file {where}: {at} is not a list of objects")


class JsonConnector:
    connection_model = JsonConnection
    dataset_model = JsonDataset

    def _files(self, request: ExtractRequest[JsonConnection, JsonDataset]) -> list[Path]:
        path = request.source_dir / request.dataset.path
        if path.is_file():
            return [path]
        if not path.is_dir():
            raise ExtractError(f"JSON file or folder not found: {path.as_posix()}")
        files = sorted(
            (
                file
                for file in path.iterdir()
                if file.is_file() and file.suffix.lower() in JSON_SUFFIXES
            ),
            key=lambda file: file.name,
        )
        if not files:
            raise ExtractError(f"no .json, .jsonl or .ndjson files in {path.as_posix()}")
        return files

    def file_version(self, request: ExtractRequest[JsonConnection, JsonDataset]) -> FileVersion:
        """A file's own hash; for a folder, one hash over each file's name and contents, so
        adding, removing, renaming or changing any file is a new version."""
        files = self._files(request)
        if (request.source_dir / request.dataset.path).is_file():
            return FileVersion(request.dataset.path, file_sha256(files[0]))
        digest = hashlib.sha256()
        for file in files:
            digest.update(f"{file.name}\0{file_sha256(file)}\n".encode())
        return FileVersion(request.dataset.path, digest.hexdigest())

    def extract(
        self, request: ExtractRequest[JsonConnection, JsonDataset]
    ) -> Iterator[pl.DataFrame]:
        records: list[Mapping[str, Any]] = []
        for file in self._files(request):
            for where, document in documents(file):
                records.extend(document_records(document, where, request.dataset.records_path))
        frame = records_frame(records)
        if frame.height == 0:
            # No records, so nothing to learn columns from: the declared ones, empty.
            yield pl.DataFrame(
                [pl.Series(name, [], dtype=pl.Null) for name in request.dataset.columns]
            )
            return
        yield from frame.iter_slices(request.chunk_size)
