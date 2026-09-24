"""Edits to a dataset's settings, stored in the platform database and layered over source.yaml.

The file stays as it was written and is never touched. An override replaces whole fields of
one dataset — `columns:` or `checks:` is replaced entire, never merged entry by entry, because
merging lists gives no way to remove an entry — and it is applied before anything is validated,
so an edit is refused for exactly the reasons the same lines in the file would be refused.
"""

from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel

# Everything a dataset's page may change. `name` is not here: it decides the table's name, and
# the connection block, adding and removing datasets and transform.py are not editable either.
EDITABLE = (
    "schedule",
    "load_mode",
    "watermark",
    "primary_key",
    "columns",
    "checks",
    "quarantine_threshold_percent",
    "exclude_columns",
)


def editable_fields(dataset_model: type[BaseModel]) -> tuple[str, ...]:
    """The editable fields this connector's datasets actually have, in EDITABLE's order.

    Only database datasets have `exclude_columns`, and a field the model does not know is
    refused by the model as an extra field rather than silently ignored.
    """
    return tuple(name for name in EDITABLE if name in dataset_model.model_fields)


def apply_overrides(data: object, overrides: Mapping[str, Mapping[str, Any]]) -> object:
    """A source.yaml mapping with each dataset's stored override laid over it.

    Anything that is not the expected shape is passed through untouched: this runs before the
    file is validated, so a broken file must still reach validation and fail with its own
    message. An override for a dataset the file does not have is ignored — the dataset was
    renamed or removed, and its edits no longer name anything.
    """
    if not overrides or not isinstance(data, dict):
        return data
    datasets = data.get("datasets")
    if not isinstance(datasets, list):
        return data
    merged = []
    for dataset in datasets:
        override = (
            overrides.get(dataset["name"])
            if isinstance(dataset, dict) and isinstance(dataset.get("name"), str)
            else None
        )
        merged.append({**dataset, **override} if override else dataset)
    return {**data, "datasets": merged}


def differences(
    file_values: Mapping[str, Any], effective: Mapping[str, Any], before: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    """What to store for an edit, and what the edit changed.

    All three mappings hold the same fields as they come out of validation, so a value that is
    spelled differently but means the same — `decimal( 12 , 2 )` for `decimal(12,2)` — counts
    as unchanged.

    The override is what differs from the *file*, so a field is followed again once its edit is
    taken back, and a field later changed in the file is picked up. What is recorded as changed
    is what differs from what this dataset was using *before* this edit, which is what a person
    reading the history is looking for — and it is how taking an edit back is recorded at all.
    """
    override = {name: value for name, value in effective.items() if value != file_values.get(name)}
    changed = {
        name: {"from": before.get(name), "to": value}
        for name, value in effective.items()
        if value != before.get(name)
    }
    return override, changed
