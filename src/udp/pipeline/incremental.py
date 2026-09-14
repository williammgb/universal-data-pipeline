from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from hashlib import sha256

import polars as pl
import structlog

from udp.connectors.base import DatasetBase, FileVersion
from udp.errors import ConfigError, SchemaDriftError, ValidationError
from udp.storage.loader import WATERMARK_TYPES, DatasetState, Watermark, column_type

log = structlog.get_logger(step="filter")


def config_sha256(dataset: DatasetBase) -> str:
    return sha256(dataset.model_dump_json(exclude_defaults=True).encode()).hexdigest()


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
    kept = 0
    empty_watermarks = 0
    empty_keys = dict.fromkeys(primary_key, 0)
    for chunk in chunks:
        if not checked:
            missing = [name for name in (watermark, *primary_key) if name not in chunk.columns]
            if missing:
                raise ValidationError(f"column '{missing[0]}' is not in the data")
        empty_watermarks += chunk[watermark].null_count()
        for name in primary_key:
            empty_keys[name] += chunk[name].null_count()
        if empty_watermarks or any(empty_keys.values()):
            continue
        if not checked:
            kind = column_type(watermark, chunk.schema[watermark])
            if kind not in WATERMARK_TYPES and chunk.height == 0:
                # A file with a header and no rows gives no types to check; its columns
                # are passed on as empty so they fit whatever the table already holds.
                yield pl.DataFrame(schema=dict.fromkeys(chunk.columns, pl.Null))
                continue
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
