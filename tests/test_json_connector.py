import json
import shutil
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from uuid import uuid4

import polars as pl
import psycopg
import pytest
from fakes import MemoryLoader
from fastapi.testclient import TestClient
from hypothesis import given
from hypothesis import strategies as st
from psycopg import sql

from udp.api.app import create_app
from udp.api.catalog import PostgresCatalog
from udp.config.columns import JSON_DTYPE, JSON_FIELD
from udp.config.source import load_source
from udp.connectors.base import ExtractRequest
from udp.connectors.json import JsonConnection, JsonConnector, JsonDataset
from udp.connectors.records import records_frame
from udp.errors import ExtractError
from udp.pipeline.runner import run_source
from udp.settings import Settings
from udp.storage.postgres import PostgresLoader

SOURCES = Path("sources")


def _request(
    folder: Path, dataset: Mapping[str, Any], chunk_size: int = 1000
) -> ExtractRequest[JsonConnection, JsonDataset]:
    return ExtractRequest(
        source_dir=folder,
        connection=JsonConnection(type="json"),
        dataset=JsonDataset.model_validate({"name": "items", **dataset}),
        chunk_size=chunk_size,
    )


def _write(folder: Path, files: Mapping[str, str]) -> None:
    for name, text in files.items():
        (folder / name).parent.mkdir(parents=True, exist_ok=True)
        (folder / name).write_text(text, encoding="utf-8")


def _read(
    tmp_path: Path, files: Mapping[str, str], chunk_size: int = 1000, **dataset: Any
) -> pl.DataFrame:
    _write(tmp_path, files)
    request = _request(tmp_path, {"path": "data.json", **dataset}, chunk_size)
    chunks = list(JsonConnector().extract(request))
    assert len({tuple(chunk.schema.items()) for chunk in chunks}) == 1
    return pl.concat(chunks)


def _decoded(frame: pl.DataFrame, name: str) -> list[Any]:
    """A JSON column's values as the JSON they hold."""
    assert frame.schema[name] == JSON_DTYPE, frame.schema[name]
    return [None if v is None else json.loads(v[JSON_FIELD]) for v in frame[name].to_list()]


# --- the two shapes of a JSON file, and where its records are ----------------------------------


def test_a_top_level_array_is_one_row_per_object(tmp_path: Path) -> None:
    frame = _read(
        tmp_path,
        {"data.json": '[{"id": 1, "name": "mug", "price": 12.5}, {"id": 2, "name": "pot"}]'},
    )

    assert frame.schema["id"] == pl.Int64 and frame.schema["price"] == pl.Float64
    assert frame.rows() == [(1, "mug", 12.5), (2, "pot", None)]


def test_newline_delimited_json_is_one_row_per_line_and_blank_lines_are_skipped(
    tmp_path: Path,
) -> None:
    frame = _read(
        tmp_path,
        {"data.json": '{"id": 1, "tags": ["a"]}\n\n{"id": 2, "tags": []}\r\n{"id": 3}\n'},
    )

    assert frame["id"].to_list() == [1, 2, 3]
    assert _decoded(frame, "tags") == [["a"], [], None]


def test_the_records_path_names_the_list_inside_a_wrapping_document(tmp_path: Path) -> None:
    document = {"meta": {"count": 2}, "data": {"items": [{"id": 1}, {"id": 2}]}}

    frame = _read(tmp_path, {"data.json": json.dumps(document)}, records_path="data.items")

    assert frame.rows() == [(1,), (2,)]


def test_the_records_path_applies_to_every_line_of_newline_delimited_json(tmp_path: Path) -> None:
    lines = [{"page": 1, "rows": [{"id": 1}, {"id": 2}]}, {"page": 2, "rows": [{"id": 3}]}]

    frame = _read(
        tmp_path, {"data.json": "\n".join(json.dumps(line) for line in lines)}, records_path="rows"
    )

    assert frame["id"].to_list() == [1, 2, 3]


@pytest.mark.parametrize(
    ("text", "records_path", "message"),
    [
        ('{"data": {"rows": []}}', "data.items", "no 'data.items' in the document"),
        ('{"data": {"items": 5}}', "data.items", "'data.items' is not a list of objects"),
        ('{"data": {"items": [1, 2]}}', "data.items", "'data.items' is not a list of objects"),
        ("[1, 2]", None, "the document is not a list of objects"),
        ('"just text"', None, "the document is not a list of objects"),
    ],
)
def test_a_document_without_a_list_of_objects_where_it_is_expected_is_refused(
    tmp_path: Path, text: str, records_path: str | None, message: str
) -> None:
    with pytest.raises(ExtractError, match=message):
        _read(tmp_path, {"data.json": text}, records_path=records_path)


def test_a_file_holding_one_object_is_one_record(tmp_path: Path) -> None:
    frame = _read(tmp_path, {"data.json": '{"id": 7, "address": {"city": "Delft"}}'})

    assert frame["id"].to_list() == [7]
    assert _decoded(frame, "address") == [{"city": "Delft"}]


def test_a_folder_is_read_file_by_file_in_name_order_and_other_files_are_left_alone(
    tmp_path: Path,
) -> None:
    _write(
        tmp_path,
        {
            "drop/b.jsonl": '{"id": 3}\n{"id": 4}\n',
            "drop/a.json": '[{"id": 1}, {"id": 2}]',
            "drop/c.NDJSON": '{"id": 5}',
            "drop/notes.txt": "not data",
            "drop/empty.json": "",
        },
    )
    (tmp_path / "drop" / "inner.json").mkdir()

    chunks = list(JsonConnector().extract(_request(tmp_path, {"path": "drop"}, chunk_size=2)))

    assert [chunk.height for chunk in chunks] == [2, 2, 1]
    assert pl.concat(chunks)["id"].to_list() == [1, 2, 3, 4, 5]


@pytest.mark.parametrize(
    ("files", "path", "message"),
    [
        ({}, "missing.json", "JSON file or folder not found"),
        ({"drop/notes.txt": "x"}, "drop", r"no \.json, \.jsonl or \.ndjson files in"),
    ],
)
def test_a_path_with_nothing_to_read_fails_clearly(
    tmp_path: Path, files: dict[str, str], path: str, message: str
) -> None:
    _write(tmp_path, files)
    request = _request(tmp_path, {"path": path})

    with pytest.raises(ExtractError, match=message):
        list(JsonConnector().extract(request))
    with pytest.raises(ExtractError, match=message):
        JsonConnector().file_version(request)


# --- malformed, empty and mixed documents ---------------------------------------------------


@pytest.mark.parametrize(
    ("text", "message"),
    [
        ('[{"id": 1}, {"id": 2', "line 1 column 21: Expecting ',' delimiter"),
        ('[\n  {"id": 1},\n  {"id": }\n]', "line 3 column 10: Expecting value"),
        ('{"id": 1}\n{"id": 2}\n{"id": 3,}\n', "line 3 column 9: Illegal trailing comma"),
        ('[{"price": NaN}]', "NaN is not a JSON value"),
        ('{"id": 1}\n{"price": -Infinity}', "-Infinity is not a JSON value"),
    ],
)
def test_a_malformed_document_names_where_it_is_wrong(
    tmp_path: Path, text: str, message: str
) -> None:
    with pytest.raises(ExtractError, match=f"could not read JSON file .*data.json: {message}"):
        _read(tmp_path, {"data.json": text})


def test_a_file_that_is_not_utf8_is_refused(tmp_path: Path) -> None:
    (tmp_path / "data.json").write_bytes(b'[{"city": "Delft\xff"}]')

    with pytest.raises(ExtractError, match="not UTF-8 at byte 16"):
        list(JsonConnector().extract(_request(tmp_path, {"path": "data.json"})))


def test_a_malformed_file_loads_nothing_even_after_good_files(tmp_path: Path) -> None:
    sources = tmp_path / "sources"
    _write(
        sources / "shop",
        {
            "source.yaml": "connection:\n  type: json\n"
            "datasets:\n  - name: orders\n    path: drop\n",
            "drop/1.json": '[{"id": 1}, {"id": 2}]',
            "drop/2.json": '[{"id": 3}, {"id": ',
        },
    )
    loader = MemoryLoader()

    (outcome,) = run_source("shop", load_source(sources, "shop", {}), sources, loader)

    assert outcome.status == "failed"
    (run,) = loader.runs.values()
    assert run["error_class"] == "ExtractError"
    assert "2.json" in run["error_message"]
    assert "shop__orders" not in loader.tables


@pytest.mark.parametrize("text", ["", "  \n\n", "[]", '{"data": {"items": []}}'])
def test_an_empty_document_has_no_rows_and_only_the_declared_columns(
    tmp_path: Path, text: str
) -> None:
    path = "data.items" if "data" in text else None

    frame = _read(
        tmp_path, {"data.json": text}, records_path=path, columns={"id": "integer", "doc": "json"}
    )

    assert (frame.height, frame.columns) == (0, ["id", "doc"])


def test_an_empty_document_loads_no_rows_into_a_dataset_that_declares_its_columns(
    tmp_path: Path,
) -> None:
    sources = tmp_path / "sources"
    _write(
        sources / "shop",
        {
            "source.yaml": "connection:\n  type: json\ndatasets:\n  - name: orders\n"
            "    path: orders.json\n    columns:\n      id: integer\n      lines: json\n",
            "orders.json": '[{"id": 1, "lines": [{"sku": "A"}]}]',
        },
    )
    loader = MemoryLoader()
    config = load_source(sources, "shop", {})
    assert [o.rows_loaded for o in run_source("shop", config, sources, loader)] == [1]

    (sources / "shop" / "orders.json").write_text("[]", encoding="utf-8")
    (outcome,) = run_source("shop", config, sources, loader)

    assert (outcome.status, outcome.rows_loaded) == ("succeeded", 0)
    table = loader.tables["shop__orders"]
    assert table.height == 0 and table.schema["lines"] == JSON_DTYPE


def test_records_of_different_shapes_share_one_schema_typed_over_every_record(
    tmp_path: Path,
) -> None:
    records = [
        {"id": 1, "amount": 3, "code": "A", "meta": {"a": 1}},
        {"id": 2, "amount": 2.5, "code": 7, "meta": "plain"},
        {"id": 3, "flag": True, "meta": [1, 2]},
        {"id": 4, "big": 2**70, "meta": None, "late": None},
    ]

    frame = _read(
        tmp_path,
        {"data.json": "\n".join(json.dumps(record) for record in records)},
        chunk_size=1,
    )

    assert frame.columns == ["id", "amount", "code", "meta", "flag", "big", "late"]
    assert frame["amount"].to_list() == [3.0, 2.5, None, None]
    assert frame["code"].to_list() == ["A", "7", None, None]
    assert frame["flag"].to_list() == [None, None, True, None]
    assert frame["big"].to_list() == [None, None, None, str(2**70)]
    assert frame.schema["late"] == pl.Null
    # One object anywhere makes the whole column JSON, and its text value stays JSON text.
    assert _decoded(frame, "meta") == [{"a": 1}, "plain", [1, 2], None]


JSON_LEAF = (
    st.none()
    | st.booleans()
    | st.integers()
    | st.floats(allow_nan=False, allow_infinity=False)
    | st.text()
)
NESTED = st.recursive(
    JSON_LEAF,
    lambda inner: st.lists(inner, max_size=3) | st.dictionaries(st.text(max_size=3), inner),
    max_leaves=8,
)
RECORDS = st.lists(st.dictionaries(st.sampled_from(["a", "b", "c"]), NESTED, max_size=3))


@given(RECORDS)
def test_every_nested_value_comes_out_as_the_json_it_went_in_as(
    records: list[dict[str, Any]],
) -> None:
    frame = records_frame(records)

    for name in frame.columns:
        values = [record.get(name) for record in records]
        if any(isinstance(value, dict | list) for value in values):
            assert _decoded(frame, name) == values
        else:
            assert frame.schema[name] != JSON_DTYPE


def test_a_nested_value_holding_nan_is_refused_rather_than_stored_wrongly() -> None:
    # An API's response can hold NaN, which Python's reader accepts and jsonb does not.
    with pytest.raises(ExtractError, match="column 'meta' holds NaN or Infinity"):
        records_frame([{"meta": {"ratio": float("nan")}}])


# --- what is stored: declared types, and the content hash --------------------------------------


def test_declaring_a_nested_column_text_keeps_its_json_as_text_and_json_makes_text_jsonb(
    tmp_path: Path,
) -> None:
    sources = tmp_path / "sources"
    _write(
        sources / "shop",
        {
            "source.yaml": "connection:\n  type: json\ndatasets:\n  - name: orders\n"
            "    path: orders.json\n    quarantine_threshold_percent: 50\n"
            "    columns:\n      address: text\n      raw: json\n",
            "orders.json": '[{"address": {"city": "Delft"}, "raw": "{\\"a\\": [1]}", "id": 1},'
            ' {"address": null, "raw": "not json", "id": 2}]',
        },
    )
    loader = MemoryLoader()

    (outcome,) = run_source("shop", load_source(sources, "shop", {}), sources, loader)

    assert (outcome.status, outcome.rows_loaded) == ("succeeded", 1)
    table = loader.tables["shop__orders"]
    assert table.schema["address"] == pl.String
    assert table["address"].to_list() == ['{"city": "Delft"}']
    assert _decoded(table, "raw") == [{"a": [1]}]
    # The row whose text is not JSON is set aside, with the JSON column shown as its text.
    (quarantined,) = loader.quarantine
    assert quarantined["reason"] == "column 'raw' is not json"
    assert quarantined["record"] == {"address": None, "raw": "not json", "id": 2}


def test_a_folder_version_changes_when_any_file_is_added_renamed_or_changed(
    tmp_path: Path,
) -> None:
    _write(tmp_path, {"drop/a.json": "[]", "drop/b.jsonl": '{"id": 1}'})
    request = _request(tmp_path, {"path": "drop"})
    connector = JsonConnector()
    seen = [connector.file_version(request)]

    (tmp_path / "drop" / "notes.txt").write_text("ignored", encoding="utf-8")
    assert connector.file_version(request) == seen[0]
    _write(tmp_path, {"drop/c.json": "[]"})
    seen.append(connector.file_version(request))
    (tmp_path / "drop" / "c.json").rename(tmp_path / "drop" / "d.json")
    seen.append(connector.file_version(request))
    _write(tmp_path, {"drop/b.jsonl": '{"id": 2}'})
    seen.append(connector.file_version(request))

    assert len({version.sha256 for version in seen}) == 4
    assert {version.path for version in seen} == {"drop"}


# --- loading incrementally, in each mode ----------------------------------------------------


def _shop(tmp_path: Path, dataset: str) -> Path:
    sources = tmp_path / "sources"
    _write(sources / "shop", {"source.yaml": f"connection:\n  type: json\ndatasets:\n{dataset}"})
    return sources


def _orders(sources: Path, orders: list[dict[str, Any]], name: str = "orders.json") -> None:
    _write(sources / "shop", {name: json.dumps({"data": orders})})


def _loaded(sources: Path, loader: MemoryLoader, **options: Any) -> list[int | None]:
    config = load_source(sources, "shop", {})
    outcomes = run_source("shop", config, sources, loader, **options)
    assert {outcome.status for outcome in outcomes} == {"succeeded"}
    return [outcome.rows_loaded for outcome in outcomes]


def test_full_mode_replaces_the_table_and_an_unchanged_file_is_skipped(tmp_path: Path) -> None:
    sources = _shop(tmp_path, "  - name: orders\n    path: orders.json\n    records_path: data\n")
    loader = MemoryLoader()
    _orders(sources, [{"id": 1, "lines": [1]}, {"id": 2, "lines": [2]}])

    assert _loaded(sources, loader) == [2]
    assert _loaded(sources, loader) == [0]
    _orders(sources, [{"id": 3, "lines": {"x": 1}}])
    assert _loaded(sources, loader) == [1]

    table = loader.tables["shop__orders"]
    assert table["id"].to_list() == [3]
    assert _decoded(table, "lines") == [{"x": 1}]


def test_append_mode_adds_only_records_past_the_watermark(tmp_path: Path) -> None:
    sources = _shop(
        tmp_path, "  - name: events\n    path: drop\n    load_mode: append\n    watermark: seq\n"
    )
    loader = MemoryLoader()
    _write(sources / "shop", {"drop/1.jsonl": '{"seq": 1, "p": {"a": 1}}\n{"seq": 2, "p": {}}'})

    assert _loaded(sources, loader) == [2]
    assert _loaded(sources, loader) == [0]
    _write(sources / "shop", {"drop/2.jsonl": '{"seq": 3, "p": {"b": [true]}}'})
    assert _loaded(sources, loader) == [1]

    table = loader.tables["shop__events"]
    assert table["seq"].to_list() == [1, 2, 3]
    assert _decoded(table, "p") == [{"a": 1}, {}, {"b": [True]}]


def test_merge_mode_updates_a_record_whose_nested_value_changed(tmp_path: Path) -> None:
    sources = _shop(
        tmp_path,
        "  - name: orders\n    path: orders.json\n    records_path: data\n"
        "    load_mode: merge\n    watermark: version\n    primary_key: [id]\n",
    )
    loader = MemoryLoader()
    _orders(sources, [{"id": 1, "version": 1, "c": {"city": "Delft"}}, {"id": 2, "version": 1}])

    assert _loaded(sources, loader) == [2]
    _orders(
        sources,
        [
            {"id": 1, "version": 2, "c": {"city": "Leiden"}},
            {"id": 2, "version": 1},
            {"id": 3, "version": 2, "c": {"city": "Delft"}},
        ],
    )
    # id 2 is read again at the saved version, but nothing in it changed: it is not rewritten.
    assert _loaded(sources, loader) == [2]

    table = loader.tables["shop__orders"].sort("id")
    assert _decoded(table, "c") == [{"city": "Leiden"}, None, {"city": "Delft"}]


def test_the_demo_json_source_loads_both_its_datasets() -> None:
    loader = MemoryLoader()

    assert _loaded_demo(loader) == [8, 9]
    assert _loaded_demo(loader) == [0, 0]
    orders = loader.tables["demo_json__orders"]
    assert orders.schema["total"] == pl.Decimal(10, 2)
    assert orders.schema["updated_at"] == pl.Datetime("us", "UTC")
    first = orders.filter(pl.col("id") == 3)
    assert _decoded(first, "customer")[0]["address"] == {
        "street": "Oudegracht 12",
        "postcode": "3511 AB",
    }
    assert _decoded(orders.filter(pl.col("id") == 4), "lines") == [[]]
    assert _decoded(orders.filter(pl.col("id") == 6), "customer") == [None]
    events = loader.tables["demo_json__events"]
    assert events["seq"].to_list() == list(range(1, 10))
    assert events.schema["payload"] == JSON_DTYPE


def _loaded_demo(loader: MemoryLoader) -> list[int | None]:
    outcomes = run_source("demo_json", load_source(SOURCES, "demo_json", {}), SOURCES, loader)
    assert {outcome.status for outcome in outcomes} == {"succeeded"}
    return [outcome.rows_loaded for outcome in outcomes]


# --- a real PostgreSQL, and the API on top of it ----------------------------------------------


def _url() -> str:
    return str(Settings().database_url)  # type: ignore[call-arg]


def _stored(conn: psycopg.Connection[Any], table: str) -> dict[str, str]:
    rows = conn.execute(
        "SELECT attname, format_type(atttypid, atttypmod) FROM pg_attribute "
        "WHERE attrelid = to_regclass(%s) AND attnum > 0 AND NOT attisdropped",
        [f"datasets.{table}"],
    ).fetchall()
    return {name: kind for name, kind in rows}


CUSTOMER = {
    "name": "Chloé",
    "address": {"street": "Oudegracht 12", "floor": None},
    "tags": ["vip", 3, 2.5, True],
    "big": 2**70,
}


@pytest.mark.db
def test_a_nested_object_is_stored_as_jsonb_and_reads_back_as_the_same_json(
    tmp_path: Path,
) -> None:
    source = f"json_{uuid4().hex[:10]}"
    sources = tmp_path / "sources"
    _write(
        sources / source,
        {
            "source.yaml": "connection:\n  type: json\ndatasets:\n  - name: orders\n"
            "    path: orders.json\n    records_path: data.items\n    load_mode: merge\n"
            "    watermark: version\n    primary_key: [id]\n",
            "orders.json": json.dumps(
                {
                    "data": {
                        "items": [
                            {"id": 1, "version": 1, "customer": CUSTOMER},
                            {"id": 2, "version": 1, "customer": None},
                        ]
                    }
                }
            ),
        },
    )
    table = f"{source}__orders"

    with PostgresLoader(_url()) as loader:
        (outcome,) = run_source(source, load_source(sources, source, {}), sources, loader)
    assert (outcome.status, outcome.rows_loaded) == ("succeeded", 2)

    with psycopg.connect(_url()) as conn:
        assert _stored(conn, table)["customer"] == "jsonb"
        stored = conn.execute(
            sql.SQL("SELECT customer FROM {} ORDER BY id").format(sql.Identifier("datasets", table))
        ).fetchall()
    assert stored == [(CUSTOMER,), (None,)]

    catalog = PostgresCatalog(_url())
    try:
        client = TestClient(create_app(catalog, sources, {}, lambda: PostgresLoader(_url())))
        page = client.get(f"/api/datasets/{source}/orders/rows")
        assert page.status_code == 200
        # In the body as a JSON object, not as a quoted string holding one.
        assert '"customer":{' in page.text and '"customer":"' not in page.text
        assert {"name": "customer", "type": "jsonb"} in page.json()["columns"]
        assert [row["customer"] for row in page.json()["rows"]] == [CUSTOMER, None]

        profile = client.get(f"/api/datasets/{source}/orders/profile")
        assert profile.status_code == 200
        (customer,) = [c for c in profile.json()["columns"] if c["name"] == "customer"]
        assert (customer["type"], customer["kind"], customer["missing"]) == ("jsonb", "other", 1)
    finally:
        catalog.close()

    # Merging again: unchanged JSON is not rewritten, and changed JSON is.
    changed = {**CUSTOMER, "tags": []}
    items = [
        {"id": 1, "version": 2, "customer": changed},
        {"id": 2, "version": 1, "customer": None},
    ]
    _write(sources / source, {"orders.json": json.dumps({"data": {"items": items}})})
    with PostgresLoader(_url()) as loader:
        (outcome,) = run_source(source, load_source(sources, source, {}), sources, loader)
    assert (outcome.status, outcome.rows_loaded) == ("succeeded", 1)
    with psycopg.connect(_url()) as conn:
        (row,) = conn.execute(
            sql.SQL("SELECT customer FROM {} WHERE id = 1").format(
                sql.Identifier("datasets", table)
            )
        ).fetchall()
        conn.execute(sql.SQL("DROP TABLE {}").format(sql.Identifier("datasets", table)))
    assert row == (changed,)


@pytest.mark.db
def test_the_demo_json_source_loads_into_postgres(tmp_path: Path) -> None:
    # A copy under a name of its own, so it never meets another test's tables or state.
    source = f"demo_json_{uuid4().hex[:8]}"
    sources = tmp_path / "sources"
    shutil.copytree(SOURCES / "demo_json", sources / source)

    with PostgresLoader(_url()) as loader:
        outcomes = run_source(source, load_source(sources, source, {}), sources, loader)
        again = run_source(source, load_source(sources, source, {}), sources, loader)

    assert [(o.dataset, o.status, o.rows_loaded) for o in outcomes] == [
        ("orders", "succeeded", 8),
        ("events", "succeeded", 9),
    ]
    assert [o.rows_loaded for o in again] == [0, 0]
    with psycopg.connect(_url()) as conn:
        orders = _stored(conn, f"{source}__orders")
        events = _stored(conn, f"{source}__events")
        (lines,) = conn.execute(
            sql.SQL("SELECT jsonb_array_length(lines) FROM {} WHERE id = 5").format(
                sql.Identifier("datasets", f"{source}__orders")
            )
        ).fetchone() or (None,)
        for table in (f"{source}__orders", f"{source}__events"):
            conn.execute(sql.SQL("DROP TABLE {}").format(sql.Identifier("datasets", table)))
    assert (orders["customer"], orders["lines"], orders["total"]) == (
        "jsonb",
        "jsonb",
        "numeric(10,2)",
    )
    assert (events["payload"], events["seq"]) == ("jsonb", "bigint")
    assert lines == 2
