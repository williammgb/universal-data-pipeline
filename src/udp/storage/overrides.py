"""Reading and writing platform.config_edits, the history of edits to dataset settings.

The newest row of a dataset holds its whole current override, so reading what is in force is
one statement and no row is ever changed or removed. Both the loader and the API's catalog go
through here, on their own connections and with their own row factories, so the statements live
in one place and are asked for by cursors that spell their rows out.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import psycopg
from psycopg.rows import tuple_row
from psycopg.types.json import Jsonb


@dataclass(frozen=True)
class ConfigEdit:
    """One save: what it changed, as field -> {"from": …, "to": …}, and when."""

    changed: dict[str, Any]
    changed_at: datetime


def read_overrides(conn: psycopg.Connection[Any], source: str) -> dict[str, dict[str, Any]]:
    """Every dataset of this source that has an override in force, by dataset name.

    A dataset whose edits have all been taken back has an empty override and is left out.
    """
    rows = conn.cursor(row_factory=tuple_row).execute(
        "SELECT DISTINCT ON (dataset) dataset, override FROM platform.config_edits "
        "WHERE source = %s ORDER BY dataset, id DESC",
        [source],
    )
    return {dataset: override for dataset, override in rows if override}


def read_edits(conn: psycopg.Connection[Any], source: str, dataset: str) -> list[ConfigEdit]:
    """Every save for this dataset, newest first."""
    rows = conn.cursor(row_factory=tuple_row).execute(
        "SELECT changed, changed_at FROM platform.config_edits "
        "WHERE source = %s AND dataset = %s ORDER BY id DESC",
        [source, dataset],
    )
    return [ConfigEdit(changed=changed, changed_at=changed_at) for changed, changed_at in rows]


def save_override(
    conn: psycopg.Connection[Any],
    source: str,
    dataset: str,
    override: Mapping[str, Any],
    changed: Mapping[str, Any],
    changed_at: datetime,
) -> None:
    """Add one row: the whole override as it now stands, and what this save changed."""
    conn.execute(
        "INSERT INTO platform.config_edits (source, dataset, override, changed, changed_at) "
        "VALUES (%s, %s, %s, %s, %s)",
        [source, dataset, Jsonb(dict(override)), Jsonb(dict(changed)), changed_at],
    )
