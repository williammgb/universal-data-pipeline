"""Configuration edits: the override layered over source.yaml, and what refuses one."""

from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from uuid import uuid7

import pytest
import yaml
from hypothesis import given
from hypothesis import strategies as st

from udp.config.overrides import EDITABLE, apply_overrides, differences, editable_fields
from udp.config.source import load_source
from udp.connectors.csv import CsvDataset
from udp.connectors.database import DatabaseDataset
from udp.errors import ConfigError
from udp.pipeline.incremental import check_same_load_settings, rebuild_reasons
from udp.storage.loader import DatasetState

SOURCE = (
    "connection:\n  type: csv\n"
    "datasets:\n"
    "  - name: customers\n    path: data/customers.csv\n"
    "    quarantine_threshold_percent: 1\n"
    "    columns:\n      customer_id: integer\n"
    "  - name: orders\n    path: data/orders.csv\n"
)


def write(sources_dir: Path, text: str = SOURCE, name: str = "shop") -> Path:
    folder = sources_dir / name
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "source.yaml").write_text(text, encoding="utf-8")
    return sources_dir


def state(
    load_mode: str = "full",
    primary_key: tuple[str, ...] = (),
    watermark_column: str | None = None,
) -> DatasetState:
    return DatasetState(
        source="shop",
        dataset="customers",
        load_mode=load_mode,
        primary_key=primary_key,
        watermark_column=watermark_column,
        watermark_type=None,
        watermark=None,
        file_path=None,
        file_sha256=None,
        config_sha256="x",
        run_id=uuid7(),
        saved_at=datetime.now(UTC),
    )


# --- what an override does to a dataset ---------------------------------------------------------

EDITS = st.fixed_dictionaries(
    {},
    optional={
        "schedule": st.sampled_from([None, "0 6 * * *", "*/15 * * * *"]),
        "quarantine_threshold_percent": st.floats(0, 100, allow_nan=False, allow_infinity=False),
        "columns": st.dictionaries(
            st.sampled_from(["customer_id", "signup_date", "city"]),
            st.sampled_from(["text", "integer", "date", "decimal(12,2)"]),
            max_size=3,
        ),
        "checks": st.just([{"check": "not_null", "column": "customer_id"}]),
    },
)


@given(EDITS)
def test_an_override_decides_the_fields_it_sets_and_nothing_else(edit: dict[str, Any]) -> None:
    with TemporaryDirectory() as folder:
        sources_dir = write(Path(folder) / "sources")

        from_file = load_source(sources_dir, "shop", {})
        with_edit = load_source(sources_dir, "shop", {}, {"customers": edit})

    edited, plain = with_edit.datasets[0], from_file.datasets[0]
    for name in editable_fields(CsvDataset):
        # Compared as JSON, because a check comes back as the model the file's own lines make.
        was_set = edit[name] if name in edit else getattr(plain, name)
        expected = CsvDataset(name="x", path="x.csv", **{name: was_set}).model_dump(mode="json")
        assert edited.model_dump(mode="json")[name] == expected[name], name
    # Nothing outside the edit changes, and the other dataset is untouched.
    assert (edited.name, edited.path) == (plain.name, plain.path)
    assert with_edit.datasets[1] == from_file.datasets[1]


@given(EDITS)
def test_what_a_run_records_is_the_settings_it_used(edit: dict[str, Any]) -> None:
    """`written` feeds platform.datasets.definition, so it must hold the edits, not the file."""
    with TemporaryDirectory() as folder:
        sources_dir = write(Path(folder) / "sources")

        config = load_source(sources_dir, "shop", {}, {"customers": edit})

    written = config.written.datasets["customers"]
    for name, value in edit.items():
        assert written[name] == value


BAD_EDITS = [
    ({"schedule": "0 6 * *"}, "schedule"),
    ({"columns": {"customer_id": "whole numbers"}}, "columns"),
    ({"watermark": "signup_date"}, "watermark"),
    ({"quarantine_threshold_percent": 101}, "quarantine_threshold_percent"),
    ({"load_mode": "merge", "primary_key": ["id", "id"]}, "primary_key"),
    ({"columns": {"_run_id": "text"}}, "columns"),
]


@pytest.mark.parametrize(("edit", "field"), BAD_EDITS)
def test_an_edit_is_refused_exactly_as_the_same_lines_in_the_file_are(
    tmp_path: Path, edit: dict[str, Any], field: str
) -> None:
    sources_dir = write(tmp_path / "sources")
    as_a_file = yaml.safe_load(SOURCE)
    as_a_file["datasets"][0].update(edit)
    write(tmp_path / "from-file", yaml.safe_dump(as_a_file))

    with pytest.raises(ConfigError) as refused_edit:
        load_source(sources_dir, "shop", {}, {"customers": edit})
    with pytest.raises(ConfigError) as refused_file:
        load_source(tmp_path / "from-file", "shop", {})

    said = str(refused_edit.value).replace("sources/shop", "shop")
    assert said == str(refused_file.value).replace("from-file/shop", "shop")
    assert field in said


def test_an_override_for_a_dataset_the_file_no_longer_has_is_ignored(tmp_path: Path) -> None:
    sources_dir = write(tmp_path / "sources")

    config = load_source(sources_dir, "shop", {}, {"renamed": {"schedule": "0 6 * * *"}})

    assert [dataset.schedule for dataset in config.datasets] == [None, None]


@given(st.one_of(st.none(), st.text(), st.lists(st.text()), st.integers()))
def test_a_file_that_is_not_a_mapping_still_reaches_its_own_error(data: Any) -> None:
    assert apply_overrides(data, {"customers": {"schedule": "0 6 * * *"}}) == data


@pytest.mark.parametrize(
    "entry",
    [{}, {"path": "data/customers.csv"}, {"name": None}, {"name": 5}, "customers", None, 5],
)
def test_a_dataset_the_file_did_not_name_is_left_for_validation_to_refuse(entry: Any) -> None:
    """An entry with no usable name cannot be matched to an override, and must survive to be
    refused by validation with its own message rather than raising here."""
    data = {"connection": {"type": "csv"}, "datasets": [entry]}

    assert apply_overrides(data, {"customers": {"schedule": "0 6 * * *"}}) == data


# --- what is stored, and what the history says --------------------------------------------------


def test_only_what_differs_from_the_file_is_stored() -> None:
    file_values = {"schedule": None, "load_mode": "full", "quarantine_threshold_percent": 1.0}
    effective = {"schedule": "0 6 * * *", "load_mode": "full", "quarantine_threshold_percent": 5.0}

    override, changed = differences(file_values, effective, file_values)

    assert override == {"schedule": "0 6 * * *", "quarantine_threshold_percent": 5.0}
    assert changed == {
        "schedule": {"from": None, "to": "0 6 * * *"},
        "quarantine_threshold_percent": {"from": 1.0, "to": 5.0},
    }


def test_taking_an_edit_back_stores_nothing_and_is_still_recorded() -> None:
    file_values = {"schedule": None}
    before = {"schedule": "0 6 * * *"}

    override, changed = differences(file_values, file_values, before)

    assert override == {}
    assert changed == {"schedule": {"from": "0 6 * * *", "to": None}}


def test_the_editable_fields_follow_the_connector() -> None:
    assert "exclude_columns" not in editable_fields(CsvDataset)
    assert "exclude_columns" in editable_fields(DatabaseDataset)
    assert set(editable_fields(CsvDataset)) <= set(EDITABLE)
    assert "name" not in EDITABLE


# --- when a change needs the table rebuilt ------------------------------------------------------


def dataset(**settings: Any) -> CsvDataset:
    return CsvDataset(name="customers", path="data/customers.csv", **settings)


@pytest.mark.parametrize(
    ("saved", "settings"),
    [
        (state(), {}),
        (state(), {"load_mode": "append", "watermark": "signup_date"}),
        (state("append", (), "signup_date"), {"load_mode": "append", "watermark": "signup_date"}),
        (state("append", (), "signup_date"), {}),
        (state("merge", ("customer_id",), "signup_date"), {}),
    ],
)
def test_a_load_settings_reason_appears_exactly_when_a_run_would_refuse(
    saved: DatasetState, settings: dict[str, Any]
) -> None:
    proposed = dataset(**settings)
    refused = True
    try:
        check_same_load_settings(saved, proposed)
        refused = False
    except ConfigError:
        pass

    assert bool(rebuild_reasons(saved, [], proposed)) is refused


@pytest.mark.parametrize(
    ("stored", "declared", "needs_rebuild"),
    [
        ("bigint", "integer", False),
        ("double precision", "decimal(12,2)", True),
        ("text", "text", False),
        ("text", "integer", True),
        ("numeric(12,2)", "decimal(12,2)", False),
    ],
)
def test_a_column_reason_appears_when_the_declared_type_is_stored_differently(
    stored: str, declared: str, needs_rebuild: bool
) -> None:
    reasons = rebuild_reasons(
        state(), [("customer_id", stored)], dataset(columns={"customer_id": declared})
    )

    assert bool(reasons) is needs_rebuild
    assert all("customer_id" in reason for reason in reasons)


def test_a_dataset_that_never_ran_needs_no_rebuild() -> None:
    assert rebuild_reasons(None, [], dataset(columns={"customer_id": "text"})) == []
