from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from hashlib import sha256

import polars as pl
import structlog

from udp.config.columns import storage_dtype
from udp.connectors.base import DatasetBase, FileVersion
from udp.errors import ConfigError, SchemaDriftError, ValidationError
from udp.storage.loader import WATERMARK_TYPES, DatasetState, Watermark, column_type

log = structlog.get_logger(step="filter")


def config_sha256(dataset: DatasetBase, transform_sha256: str | None) -> str:
    """Fingerprint of everything that decides a dataset's rows besides the data itself.

    Settings left at their defaults are not part of it, and a source without transform.py
    gets the same fingerprint it had before custom transforms existed. The schedule decides
    when rows are read, not which, so it is left out too.
    """
    payload = dataset.model_dump_json(exclude_defaults=True, exclude={"schedule"})
    if transform_sha256 is not None:
        payload += f"\ntransform.py {transform_sha256}"
    return sha256(payload.encode()).hexdigest()


def is_unchanged_file(
    state: DatasetState | None, file: FileVersion | None, config_digest: str
) -> bool:
    """True when the same file content was already loaded with the same dataset settings."""
    return (
        state is not None
        and file is not None
        and (state.file_path, state.file_sha256) == (file.path, file.sha256)
        and state.config_sha256 == config_digest
    )


def check_same_load_settings(state: DatasetState, dataset: DatasetBase) -> None:
    """Changing how a dataset is loaded needs a rebuild, which only --full-refresh does."""
    before = (state.load_mode, state.watermark_column, tuple(state.primary_key))
    after = (dataset.load_mode, dataset.watermark, tuple(dataset.primary_key or ()))
    labels = ("load_mode", "watermark", "primary_key")
    changed = [
        f"{label} from {old!r} to {new!r}"
        for label, old, new in zip(labels, before, after, strict=True)
        if old != new
    ]
    if changed:
        raise ConfigError(
            f"dataset '{dataset.name}' changed {', '.join(changed)} since its last load; "
            "run with --full-refresh to rebuild the table"
        )


def rebuild_reasons(
    state: DatasetState | None, columns: Sequence[tuple[str, str]], dataset: DatasetBase
) -> list[str]:
    """Why these settings cannot be used without rebuilding the dataset's table first.

    Empty when the next run takes them as they are. It mirrors the two rules a run enforces:
    how a dataset is loaded may not change under an existing table, and a column may not be
    stored as one type and then another. A dataset that has never run has nothing recorded to
    clash with, so it gets no reason. `checks`, `quarantine_threshold_percent`, `schedule` and
    `exclude_columns` never need a rebuild.
    """
    reasons = []
    if state is not None:
        try:
            check_same_load_settings(state, dataset)
        except ConfigError as error:
            reasons.append(str(error))
    stored = dict(columns)
    for name, declared in dataset.columns.items():
        kind = column_type(name, storage_dtype(declared))
        if name in stored and stored[name] != kind:
            reasons.append(
                f"column '{name}' is stored as {stored[name]} and would be declared "
                f"{declared}, which is stored as {kind}"
            )
    return reasons


@dataclass
class WatermarkTracker:
    """The highest watermark seen and the column's stored type, filled in while rows flow."""

    highest: Watermark | None = None
    kind: str | None = None


def new_rows(
    chunks: Iterator[pl.DataFrame],
    *,
    watermark: str,
    primary_key: Sequence[str],
    saved: Watermark | None,
    inclusive: bool,
    expected_kind: str | None,
    tracker: WatermarkTracker,
) -> Iterator[pl.DataFrame]:
    """Only rows above the saved watermark (or equal to it when inclusive).

    Rows with an empty watermark or key fail the run; once one is seen the remaining
    chunks are only counted, so the error gives the total.
    """
    checked = False
    held: pl.DataFrame | None = None
    kept = 0
    empty_watermarks = 0
    empty_keys = dict.fromkeys(primary_key, 0)
    for chunk in chunks:
        if not checked:
            missing = [name for name in (watermark, *primary_key) if name not in chunk.columns]
            if missing:
                raise ValidationError(f"column '{missing[0]}' is not in the data")
            if chunk.height == 0:
                # Empty chunks before the first row carry no values to check or load.
                held = chunk
                continue
        empty_watermarks += chunk[watermark].null_count()
        for name in primary_key:
            empty_keys[name] += chunk[name].null_count()
        if empty_watermarks or any(empty_keys.values()):
            continue
        if not checked:
            kind = column_type(watermark, chunk.schema[watermark])
            if kind not in WATERMARK_TYPES:
                raise ValidationError(
                    f"watermark column '{watermark}' is {kind}; it must hold whole numbers, "
                    "dates or timestamps"
                )
            if expected_kind is not None and kind != expected_kind:
                raise SchemaDriftError(
                    f"watermark column '{watermark}' was {expected_kind} and is now {kind}; "
                    "run with --full-refresh to rebuild the table"
                )
            tracker.kind = kind
            checked = True
        if chunk.height:
            highest = chunk[watermark].max()
            if tracker.highest is None or highest > tracker.highest:  # type: ignore[operator]
                tracker.highest = highest  # type: ignore[assignment]
        if saved is not None:
            column = pl.col(watermark)
            chunk = chunk.filter(column >= saved if inclusive else column > saved)
        kept += chunk.height
        yield chunk
    if not checked and held is not None and not empty_watermarks and not any(empty_keys.values()):
        # A file with a header and no rows gives no types to check; its columns are passed
        # on as empty so they fit whatever the table already holds, and no state is saved.
        yield pl.DataFrame(schema=dict.fromkeys(held.columns, pl.Null))
    if empty_watermarks:
        raise ValidationError(
            f"{empty_watermarks} rows have an empty watermark column '{watermark}'"
        )
    for name, count in empty_keys.items():
        if count:
            raise ValidationError(f"{count} rows have an empty primary key '{name}'")
    log.info(
        "new rows selected", rows=kept, saved_watermark=str(saved), highest=str(tracker.highest)
    )
