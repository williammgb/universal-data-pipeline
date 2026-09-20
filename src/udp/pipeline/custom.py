"""Custom transforms: an optional `sources/<source>/transform.py` for every dataset of a source.

The file defines `transform(df, context) -> pl.DataFrame`. It is called once per chunk of up
to 100,000 rows, after the common transforms, and each result must be a DataFrame with clean
column names, storable types and the same schema as the first result. The file is read once,
hashed, and those same bytes are compiled and run, so the stored hash always describes the
code that ran. It is not imported: nothing goes into `sys.modules` and no `__pycache__` is
written next to it.

It is not sandboxed either: it runs in the pipeline process with the pipeline's permissions,
environment and network, so writing to `sources/` carries the same trust as editing the repo.
"""

import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from types import ModuleType
from typing import cast
from uuid import UUID

import polars as pl
import structlog

from udp.errors import LoadError, TransformError
from udp.names import RESERVED_COLUMNS
from udp.pipeline.transform import clean_column_names
from udp.storage.loader import column_type

log = structlog.get_logger(step="transform")

FILE_NAME = "transform.py"
# A transform holds its dataset's lock while it runs, so a slow one delays every later run of
# that dataset. The budget is checked between chunks: a transform that keeps returning slowly
# is stopped, one that never returns from a single chunk still hangs its run, which would take
# a worker thread nobody can safely abandon. Fifteen minutes is far past any transform we have.
TRANSFORM_TIME_LIMIT = 900.0


@dataclass(frozen=True)
class TransformContext:
    source: str
    dataset: str
    run_id: UUID


TransformFunction = Callable[[pl.DataFrame, TransformContext], pl.DataFrame]


@dataclass(frozen=True)
class TransformFile:
    path: Path
    code: bytes
    sha256: str

    @property
    def name(self) -> str:
        return self.path.as_posix()


def find_transform(source_dir: Path) -> TransformFile | None:
    path = source_dir / FILE_NAME
    try:
        code = path.read_bytes()
    except FileNotFoundError:
        return None
    except OSError as error:
        raise TransformError(
            f"{path.as_posix()}: could not be read: {type(error).__name__}: {error}"
        ) from error
    return TransformFile(path, code, sha256(code).hexdigest())


def load_transform(file: TransformFile) -> TransformFunction:
    module = ModuleType("udp_custom_transform")
    module.__file__ = str(file.path)
    try:
        code = compile(file.code, str(file.path), "exec", dont_inherit=True)
        exec(code, module.__dict__)
    except (Exception, SystemExit) as error:
        raise TransformError(
            f"{file.name}: could not be loaded: {type(error).__name__}: {error}"
        ) from error
    function = module.__dict__.get("transform")
    if not callable(function):
        raise TransformError(f"{file.name}: defines no function 'transform'")
    return cast(TransformFunction, function)


def apply_transform(
    chunks: Iterator[pl.DataFrame],
    function: TransformFunction,
    context: TransformContext,
    file: TransformFile,
    time_limit: float = TRANSFORM_TIME_LIMIT,
) -> Iterator[pl.DataFrame]:
    schema: pl.Schema | None = None
    rows_in = rows_out = 0
    started = time.monotonic()

    def check_the_clock() -> None:
        """A transform that keeps taking longer fails the run instead of holding the lock."""
        spent = time.monotonic() - started
        if spent > time_limit:
            raise TransformError(
                f"{file.name}: transform took longer than {time_limit:.0f}s "
                f"on dataset '{context.dataset}' ({spent:.0f}s so far)"
            )

    for chunk in chunks:
        check_the_clock()
        rows_in += chunk.height
        try:
            result = function(chunk, context)
        except (Exception, SystemExit) as error:
            raise TransformError(
                f"{file.name}: transform failed on dataset '{context.dataset}': "
                f"{type(error).__name__}: {error}"
            ) from error
        if not isinstance(result, pl.DataFrame):
            raise TransformError(
                f"{file.name}: transform returned {type(result).__name__}, "
                "expected a polars DataFrame"
            )
        if schema is None:
            _check_columns(file, result.schema)
            schema = result.schema
        elif result.schema != schema:
            raise TransformError(
                f"{file.name}: chunk schema {result.schema} differs from the first chunk's {schema}"
            )
        check_the_clock()
        rows_out += result.height
        yield result
    log.info("custom transform applied", path=file.name, rows_in=rows_in, rows_out=rows_out)


def _check_columns(file: TransformFile, schema: pl.Schema) -> None:
    if not schema:
        raise TransformError(f"{file.name}: transform returned no columns")
    for name, dtype in schema.items():
        if name in RESERVED_COLUMNS:
            raise TransformError(
                f"{file.name}: returned column '{name}', which is a platform column"
            )
        (clean,) = clean_column_names([name])
        if clean != name:
            raise TransformError(
                f"{file.name}: returned column '{name}', which is not a clean column name "
                f"(lowercase letters, digits and _, starting with a letter); use '{clean}'"
            )
        try:
            column_type(name, dtype)
        except LoadError as error:
            raise TransformError(
                f"{file.name}: returned column '{name}', which has type {dtype} "
                "that cannot be stored"
            ) from error
