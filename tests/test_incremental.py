import json
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from itertools import pairwise
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from fakes import MemoryLoader
from hypothesis import given
from hypothesis import strategies as st

from udp.config.source import load_source
from udp.connectors import CONNECTORS
from udp.connectors.base import ExtractRequest
from udp.connectors.csv import CsvConnection, CsvConnector, CsvDataset
from udp.errors import ExtractError, SchemaDriftError, ValidationError
from udp.log import configure_logging
from udp.pipeline.incremental import WatermarkTracker, config_sha256, new_rows
from udp.pipeline.runner import RunOutcome, run_source

# --- the new-rows filter -------------------------------------------------------------

WATERMARK_VALUES: dict[str, tuple[pl.DataType, st.SearchStrategy[Any]]] = {
    "int": (pl.Int64(), st.integers(-(2**63), 2**63 - 1)),
    "date": (pl.Date(), st.dates()),
    "naive": (pl.Datetime("us"), st.datetimes()),
    "utc": (pl.Datetime("us", "UTC"), st.datetimes(timezones=st.just(UTC))),
}


@st.composite
def filter_inputs(draw: st.DrawFn) -> tuple[pl.DataType, list[Any], list[int], Any, bool]:
    kind = draw(st.sampled_from(sorted(WATERMARK_VALUES)))
    dtype, strategy = WATERMARK_VALUES[kind]
    values = draw(st.lists(strategy, max_size=25))
    if values and kind in ("naive", "utc") and draw(st.booleans()):
        values.append(values[0] + timedelta(microseconds=1))
    cuts = sorted(draw(st.lists(st.integers(0, len(values)), max_size=4)))
    saved_options = [st.none(), strategy]
    if values:
        saved_options.append(st.sampled_from(values))
    saved = draw(st.one_of(*saved_options))
    return dtype, values, cuts, saved, draw(st.booleans())


def _chunks(dtype: pl.DataType, values: list[Any], cuts: list[int]) -> list[pl.DataFrame]:
    edges = [0, *cuts, len(values)]
    frame = pl.DataFrame(
        {"id": list(range(len(values))), "wm": pl.Series(values, dtype=dtype)},
        schema={"id": pl.Int64, "wm": dtype},
    )
    return [frame[start:end] for start, end in pairwise(edges)]


def _filter(chunks: list[pl.DataFrame], saved: Any, inclusive: bool) -> tuple[list[int], Any]:
    tracker = WatermarkTracker()
    kept = new_rows(
        iter(chunks),
        watermark="wm",
        primary_key=(),
        saved=saved,
        inclusive=inclusive,
        expected_kind=None,
        tracker=tracker,
    )
    ids = [row_id for chunk in kept for row_id in chunk["id"].to_list()]
    return ids, tracker.highest


def test_empty_watermarks_and_keys_are_counted_across_every_chunk() -> None:
    chunks = [
        pl.DataFrame({"id": [1, None], "wm": [None, 2]}, schema={"id": pl.Int64, "wm": pl.Int64}),
        pl.DataFrame({"id": [3, None], "wm": [None, 4]}, schema={"id": pl.Int64, "wm": pl.Int64}),
        pl.DataFrame({"id": [5, 6], "wm": [None, 6]}, schema={"id": pl.Int64, "wm": pl.Int64}),
    ]

    def consume(primary_key: tuple[str, ...]) -> None:
        list(
            new_rows(
                iter(chunks),
                watermark="wm",
                primary_key=primary_key,
                saved=None,
                inclusive=True,
                expected_kind=None,
                tracker=WatermarkTracker(),
            )
        )

    with pytest.raises(ValidationError, match=r"^3 rows have an empty watermark column 'wm'$"):
        consume(("id",))
    only_keys = [chunk.with_columns(pl.col("wm").fill_null(0)) for chunk in chunks]
    chunks[:] = only_keys
    with pytest.raises(ValidationError, match=r"^2 rows have an empty primary key 'id'$"):
        consume(("id",))


@pytest.mark.parametrize(
    ("first", "message"),
    [
        ({"id": [1, 2], "wm": [None, 2]}, "^1 rows have an empty watermark column 'wm'$"),
        ({"id": [None, 2], "wm": [1, 2]}, "^1 rows have an empty primary key 'id'$"),
    ],
)
def test_after_an_empty_watermark_or_key_no_chunk_is_passed_on(
    first: dict[str, list[int | None]], message: str
) -> None:
    schema = {"id": pl.Int64, "wm": pl.Int64}
    chunks = [pl.DataFrame(first, schema=schema), pl.DataFrame({"id": [3], "wm": [3]})]
    passed_on = []

    with pytest.raises(ValidationError, match=message):
        for chunk in new_rows(
            iter(chunks),
            watermark="wm",
            primary_key=("id",),
            saved=None,
            inclusive=True,
            expected_kind=None,
            tracker=WatermarkTracker(),
        ):
            passed_on.append(chunk)

    assert passed_on == []


def test_a_watermark_column_that_changes_type_needs_a_full_refresh() -> None:
    chunk = pl.DataFrame({"wm": [date(2024, 1, 1)]})

    with pytest.raises(SchemaDriftError) as caught:
        list(
            new_rows(
                iter([chunk]),
                watermark="wm",
                primary_key=(),
                saved=None,
                inclusive=False,
                expected_kind="bigint",
                tracker=WatermarkTracker(),
            )
        )

    assert str(caught.value) == (
        "watermark column 'wm' was bigint and is now date; "
        "run with --full-refresh to rebuild the table"
    )


def test_an_empty_first_chunk_does_not_let_a_text_watermark_through() -> None:
    chunks = [
        pl.DataFrame(schema={"id": pl.String, "wm": pl.String}),
        pl.DataFrame({"id": ["1"], "wm": ["soon"]}),
    ]

    with pytest.raises(ValidationError, match="watermark column 'wm' is text"):
        list(
            new_rows(
                iter(chunks),
                watermark="wm",
                primary_key=(),
                saved=None,
                inclusive=False,
                expected_kind=None,
                tracker=WatermarkTracker(),
            )
        )


def test_the_filter_logs_what_it_kept(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging()
    chunks = [pl.DataFrame({"wm": [1, 2]}), pl.DataFrame({"wm": [3]})]

    list(
        new_rows(
            iter(chunks),
            watermark="wm",
            primary_key=(),
            saved=1,
            inclusive=False,
            expected_kind=None,
            tracker=WatermarkTracker(),
        )
    )

    events = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    (selected,) = [e for e in events if e["event"] == "new rows selected"]
    assert (selected["rows"], selected["saved_watermark"], selected["highest"]) == (2, "1", "3")


@given(filter_inputs())
def test_filter_keeps_exactly_the_newer_rows_whatever_the_chunks(
    generated: tuple[pl.DataType, list[Any], list[int], Any, bool],
) -> None:
    dtype, values, cuts, saved, inclusive = generated

    ids, highest = _filter(_chunks(dtype, values, cuts), saved, inclusive)
    whole_ids, whole_highest = _filter(_chunks(dtype, values, []), saved, inclusive)

    def newer(value: Any) -> bool:
        return saved is None or (value >= saved if inclusive else value > saved)

    assert ids == [index for index, value in enumerate(values) if newer(value)]
    assert (ids, highest) == (whole_ids, whole_highest)
    assert highest == (max(values) if values else None)
    present = [value for value in (saved, highest) if value is not None]
    new_watermark = max(present) if present else None
    if saved is not None:
        assert new_watermark is not None and new_watermark >= saved


# --- whole runs against the in-memory loader ----------------------------------------------

MERGE = "    load_mode: merge\n    watermark: updated\n    primary_key: [id]\n"


class Shop:
    """A source folder in tmp with one CSV dataset, run against one MemoryLoader."""

    def __init__(self, root: Path, settings: str, file: str = "orders.csv") -> None:
        self.sources = root / "sources"
        self.folder = self.sources / "shop"
        self.folder.mkdir(parents=True)
        self.loader = MemoryLoader()
        self.configure(settings, file)

    def configure(self, settings: str, file: str = "orders.csv") -> None:
        (self.folder / "source.yaml").write_text(
            f"connection:\n  type: csv\ndatasets:\n  - name: orders\n    path: {file}\n{settings}",
            encoding="utf-8",
        )

    def write(self, rows: list[dict[str, Any]], file: str = "orders.csv") -> None:
        pl.DataFrame(rows).write_csv(self.folder / file)

    def write_transform(self, code: str) -> None:
        (self.folder / "transform.py").write_bytes(code.encode())

    def run(self, full_refresh: bool = False) -> RunOutcome:
        config = load_source(self.sources, "shop", {})
        (outcome,) = run_source(
            "shop", config, self.sources, self.loader, full_refresh=full_refresh
        )
        return outcome

    @property
    def table(self) -> pl.DataFrame:
        return self.loader.tables["shop__orders"]

    def record(self, outcome: RunOutcome) -> dict[str, Any]:
        return self.loader.runs[outcome.run_id]


ORDERS = [
    {"id": 1, "amount": 10, "updated": 100},
    {"id": 2, "amount": 20, "updated": 100},
    {"id": 3, "amount": 30, "updated": 101},
]


def test_unchanged_file_is_skipped_and_the_table_stays_identical(tmp_path: Path) -> None:
    shop = Shop(tmp_path, MERGE)
    shop.write(ORDERS)
    assert shop.run().status == "succeeded"
    before = shop.table

    second = shop.run()

    record = shop.record(second)
    assert (record["status"], record["rows_extracted"], record["rows_loaded"]) == (
        "succeeded",
        0,
        0,
    )
    assert shop.table.equals(before)


def test_merge_writes_only_changed_and_new_rows(tmp_path: Path) -> None:
    shop = Shop(tmp_path, MERGE)
    shop.write(ORDERS)
    first = shop.run()
    shop.write(
        [
            {"id": 1, "amount": 11, "updated": 102},
            {"id": 2, "amount": 20, "updated": 100},
            {"id": 3, "amount": 33, "updated": 102},
            {"id": 4, "amount": 40, "updated": 102},
        ]
    )

    second = shop.run()

    assert second.rows_loaded == 3
    run_ids = dict(zip(shop.table["id"], shop.table["_run_id"], strict=True))
    assert run_ids == {
        1: str(second.run_id),
        2: str(first.run_id),
        3: str(second.run_id),
        4: str(second.run_id),
    }
    assert dict(zip(shop.table["id"], shop.table["amount"], strict=True)) == {
        1: 11,
        2: 20,
        3: 33,
        4: 40,
    }
    assert shop.loader.states[("shop", "orders")].watermark == 102


def test_append_loads_only_rows_above_the_saved_watermark(tmp_path: Path) -> None:
    shop = Shop(tmp_path, "    load_mode: append\n    watermark: updated\n")
    shop.write(ORDERS)
    shop.run()
    shop.write(
        [*ORDERS, {"id": 9, "amount": 90, "updated": 101}, {"id": 4, "amount": 40, "updated": 105}]
    )

    second = shop.run()

    assert second.rows_loaded == 1
    assert sorted(shop.table["id"].to_list()) == [1, 2, 3, 4]


def test_full_mode_skips_an_unchanged_file_but_reloads_the_same_bytes_at_a_new_path(
    tmp_path: Path,
) -> None:
    shop = Shop(tmp_path, "")
    shop.write(ORDERS)
    shop.run()
    assert shop.record(shop.run())["rows_loaded"] == 0

    shop.write(ORDERS, file="renamed.csv")
    shop.configure("", file="renamed.csv")

    assert shop.run().rows_loaded == 3


def test_settings_written_at_their_default_keep_the_fingerprint(tmp_path: Path) -> None:
    # The value every existing platform.source_state row was saved with before transform.py
    # existed; changing it would reload every unchanged file once.
    dataset = CsvDataset(name="orders", path="orders.csv")
    assert config_sha256(dataset, None) == (
        "ff8f602ea61e3540449fd04e07aab1f66d2a5b7cbe0cd4b582b718b7b3f2abf3"
    )
    scheduled = CsvDataset(name="orders", path="orders.csv", schedule="0 6 * * mon-fri")
    assert config_sha256(scheduled, None) == config_sha256(dataset, None)

    shop = Shop(tmp_path, "")
    shop.write(ORDERS)
    shop.run()
    shop.configure("    load_mode: full\n")

    assert shop.record(shop.run())["rows_loaded"] == 0


TOTAL = (
    "import polars as pl\n\n"
    "def transform(df, context):\n"
    "    return df.with_columns(total=pl.col('amount') * {factor})\n"
)
TOTAL_CHANGED_FOR_ID_2 = (
    "import polars as pl\n\n"
    "def transform(df, context):\n"
    "    factor = pl.when(pl.col('id') == 2).then(3).otherwise(2)\n"
    "    return df.with_columns(total=pl.col('amount') * factor)\n"
)


def test_editing_transform_py_reloads_an_unchanged_file(tmp_path: Path) -> None:
    shop = Shop(tmp_path, "")
    shop.write(ORDERS)
    shop.write_transform(TOTAL.format(factor=2))
    shop.run()
    assert shop.record(shop.run())["rows_loaded"] == 0

    shop.write_transform(TOTAL.format(factor=3))

    assert shop.run().rows_loaded == 3
    assert shop.table["total"].to_list() == [30, 60, 90]


def test_adding_transform_py_reloads_an_unchanged_file(tmp_path: Path) -> None:
    shop = Shop(tmp_path, "")
    shop.write(ORDERS)
    shop.run()
    assert shop.record(shop.run())["rows_loaded"] == 0

    shop.write_transform(TOTAL.format(factor=2))

    assert shop.run().rows_loaded == 3
    assert shop.table["total"].to_list() == [20, 40, 60]


def test_merge_after_a_transform_edit_rewrites_only_rows_whose_result_changed(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    configure_logging()
    shop = Shop(tmp_path, MERGE)
    shop.write(ORDERS)
    shop.write_transform(TOTAL.format(factor=2))
    first = shop.run()
    shop.write_transform(TOTAL_CHANGED_FOR_ID_2)

    second = shop.run()

    assert (second.status, second.rows_loaded) == ("succeeded", 1)
    rows = shop.table.sort("id").select("id", "total", "_run_id").rows()
    assert rows == [
        (1, 20, str(first.run_id)),
        (2, 60, str(second.run_id)),
        (3, 60, str(first.run_id)),
    ]
    assert shop.loader.states[("shop", "orders")].watermark == 101
    logged = capsys.readouterr().out
    assert "settings or transform.py changed; reading every source row once" in logged
    assert shop.run().rows_loaded == 0


def test_append_after_a_transform_edit_keeps_loaded_rows_and_says_so(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    configure_logging()
    shop = Shop(tmp_path, "    load_mode: append\n    watermark: updated\n")
    shop.write(ORDERS)
    shop.write_transform(TOTAL.format(factor=2))
    shop.run()
    shop.write_transform(TOTAL.format(factor=3))

    outcome = shop.run()

    assert (outcome.status, outcome.rows_loaded) == ("succeeded", 0)
    assert shop.table["total"].to_list() == [20, 40, 60]
    assert (
        "rows already loaded keep their earlier result until --full-refresh"
        in capsys.readouterr().out
    )


def test_a_broken_transform_after_a_load_leaves_table_and_state_unchanged(tmp_path: Path) -> None:
    shop = Shop(tmp_path, MERGE)
    shop.write(ORDERS)
    shop.write_transform(TOTAL.format(factor=2))
    shop.run()
    before, state = shop.table, shop.loader.states[("shop", "orders")]
    shop.write_transform("def transform(df, context):\n    return None\n")

    outcome = shop.run()

    assert shop.record(outcome)["error_class"] == "TransformError"
    assert shop.table.equals(before)
    assert shop.loader.states[("shop", "orders")] == state


def test_full_refresh_reloads_an_unchanged_file_and_forgets_deleted_rows(tmp_path: Path) -> None:
    shop = Shop(tmp_path, MERGE)
    shop.write(ORDERS)
    shop.run()

    assert shop.run(full_refresh=True).rows_loaded == 3

    shop.write(ORDERS[:1])
    assert shop.run(full_refresh=True).rows_loaded == 1
    assert shop.table["id"].to_list() == [1]
    assert shop.loader.states[("shop", "orders")].watermark == 100


@pytest.mark.parametrize(
    ("changed", "change"),
    [
        (
            "    load_mode: append\n    watermark: updated\n",
            "load_mode from 'merge' to 'append', primary_key from ('id',) to ()",
        ),
        (
            "    load_mode: merge\n    watermark: amount\n    primary_key: [id]\n",
            "watermark from 'updated' to 'amount'",
        ),
        (
            "    load_mode: merge\n    watermark: updated\n    primary_key: [id, amount]\n",
            "primary_key from ('id',) to ('id', 'amount')",
        ),
    ],
)
def test_changing_how_a_dataset_loads_needs_a_full_refresh(
    tmp_path: Path, changed: str, change: str
) -> None:
    shop = Shop(tmp_path, MERGE)
    shop.write(ORDERS)
    shop.run()
    before, state = shop.table, shop.loader.states[("shop", "orders")]
    shop.configure(changed)

    outcome = shop.run()

    record = shop.record(outcome)
    assert (record["status"], record["error_class"]) == ("failed", "ConfigError")
    assert record["error_message"] == (
        f"dataset 'orders' changed {change} since its last load; "
        "run with --full-refresh to rebuild the table"
    )
    assert shop.table.equals(before)
    assert shop.loader.states[("shop", "orders")] == state
    assert shop.run(full_refresh=True).status == "succeeded"


def test_new_column_is_added_and_recorded(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    configure_logging()
    shop = Shop(tmp_path, MERGE)
    shop.write(ORDERS)
    shop.run()
    shop.write([{**row, "updated": 102, "Coupon Code": "SAVE"} for row in ORDERS[:1]])

    shop.run()

    assert shop.table.columns[-1] == "coupon_code"
    assert dict(zip(shop.table["id"], shop.table["coupon_code"], strict=True)) == {
        1: "SAVE",
        2: None,
        3: None,
    }
    assert len(shop.loader.versions[("shop", "orders")]) == 2
    events = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert {"event": "columns added", "columns": ["coupon_code"]}.items() <= next(
        e for e in events if e["event"] == "columns added"
    ).items()


def test_column_missing_from_the_file_is_kept(tmp_path: Path) -> None:
    shop = Shop(tmp_path, MERGE)
    shop.write(ORDERS)
    shop.run()
    shop.write([{"id": 4, "updated": 103}])

    shop.run()

    assert "amount" in shop.table.columns
    assert dict(zip(shop.table["id"], shop.table["amount"], strict=True)) == {
        1: 10,
        2: 20,
        3: 30,
        4: None,
    }


def test_type_change_fails_until_full_refresh_rebuilds_the_table(tmp_path: Path) -> None:
    shop = Shop(tmp_path, MERGE)
    shop.write(ORDERS)
    shop.run()
    before = shop.table
    shop.write([{**row, "amount": f"x{row['amount']}", "updated": 200} for row in ORDERS])

    outcome = shop.run()

    record = shop.record(outcome)
    assert record["error_class"] == "SchemaDriftError"
    assert "'amount'" in record["error_message"]
    assert "bigint" in record["error_message"] and "text" in record["error_message"]
    assert shop.table.equals(before)
    assert shop.run(full_refresh=True).status == "succeeded"
    assert shop.table.schema["amount"] == pl.String


class BrokenAfterFirstChunk:
    connection_model = CsvConnection
    dataset_model = CsvDataset

    def file_version(self, request: ExtractRequest[CsvConnection, CsvDataset]) -> None:
        return None

    def extract(self, request: ExtractRequest[CsvConnection, CsvDataset]) -> Iterator[pl.DataFrame]:
        yield next(CsvConnector().extract(request))
        raise ExtractError("connection to the source was lost")


def test_failure_mid_load_leaves_table_and_state_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    shop = Shop(tmp_path, MERGE)
    shop.write(ORDERS)
    shop.run()
    before, state = shop.table, shop.loader.states[("shop", "orders")]
    shop.write([{**row, "amount": 0, "updated": 300} for row in ORDERS])
    monkeypatch.setitem(CONNECTORS, "csv", BrokenAfterFirstChunk())

    config = load_source(shop.sources, "shop", {})
    (outcome,) = run_source("shop", config, shop.sources, shop.loader, chunk_size=1)

    assert outcome.status == "failed"
    assert shop.table.equals(before)
    assert shop.loader.states[("shop", "orders")] == state


def test_header_only_file_loads_nothing_and_the_next_file_starts_fresh(tmp_path: Path) -> None:
    shop = Shop(tmp_path, MERGE)
    (shop.folder / "orders.csv").write_text("id,amount,updated\n", encoding="utf-8")

    empty = shop.run()

    assert (empty.status, empty.rows_loaded) == ("succeeded", 0)
    assert ("shop", "orders") not in shop.loader.states
    shop.write(ORDERS)
    assert shop.run().rows_loaded == 3
    state = shop.loader.states[("shop", "orders")]
    assert (state.watermark_type, state.watermark) == ("bigint", 101)


def test_header_only_file_after_a_load_keeps_table_and_state(tmp_path: Path) -> None:
    shop = Shop(tmp_path, MERGE)
    shop.write(ORDERS)
    shop.run()
    before, state = shop.table, shop.loader.states[("shop", "orders")]
    (shop.folder / "orders.csv").write_text("id,amount,updated\n", encoding="utf-8")

    outcome = shop.run()

    assert (outcome.status, outcome.rows_loaded) == ("succeeded", 0)
    assert shop.table.equals(before)
    assert shop.loader.states[("shop", "orders")] == state


def test_settings_can_change_after_a_first_run_that_failed(tmp_path: Path) -> None:
    shop = Shop(tmp_path, MERGE)
    shop.write([{"id": 1, "amount": 1, "changed": 5}])
    assert shop.run().status == "failed"

    shop.configure("    load_mode: merge\n    watermark: changed\n    primary_key: [id]\n")

    assert shop.run().rows_loaded == 1


@pytest.mark.parametrize(
    ("rows", "message"),
    [
        ([{"id": 1, "amount": 1, "changed": 5}], "column 'updated' is not in the data"),
        ([{"id": 1, "amount": 1, "updated": 1.5}], "'updated' is double precision"),
        ([{"id": 1, "amount": 1, "updated": "soon"}], "'updated' is text"),
        (
            [{"id": 1, "amount": 1, "updated": 5}, {"id": 2, "amount": 1, "updated": None}],
            "1 rows have an empty watermark",
        ),
        (
            [{"id": 1, "amount": 1, "updated": 5}, {"id": None, "amount": 1, "updated": 6}],
            "1 rows have an empty primary key 'id'",
        ),
    ],
)
def test_bad_watermark_or_key_data_fails_the_run(
    tmp_path: Path, rows: list[dict[str, Any]], message: str
) -> None:
    shop = Shop(tmp_path, MERGE)
    shop.write(rows)

    outcome = shop.run()

    record = shop.record(outcome)
    assert record["error_class"] == "ValidationError"
    assert message in record["error_message"]


TIMESTAMP_MERGE = MERGE + "    columns:\n      updated: timestamp\n"


def test_a_csv_timestamp_declared_as_the_watermark_loads_all_then_nothing_then_changes(
    tmp_path: Path,
) -> None:
    shop = Shop(tmp_path, TIMESTAMP_MERGE)
    rows = [
        {"id": 1, "amount": 10, "updated": "2024-01-01T10:00:00+02:00"},
        {"id": 2, "amount": 20, "updated": "2024-01-01 09:00"},
    ]
    shop.write(rows)

    assert shop.run().rows_loaded == 2
    state = shop.loader.states[("shop", "orders")]
    assert (state.watermark_type, state.watermark) == (
        "timestamp with time zone",
        datetime(2024, 1, 1, 9, tzinfo=UTC),
    )
    shop.write([*rows, {"id": 3, "amount": 1, "updated": "2023-12-31T00:00:00Z"}])
    assert shop.run().rows_loaded == 0

    shop.write([rows[0], {"id": 2, "amount": 21, "updated": "2024-01-02T00:00:00Z"}])
    changed = shop.run()

    assert changed.rows_loaded == 1
    assert dict(zip(shop.table["id"], shop.table["amount"], strict=True)) == {1: 10, 2: 21}


def test_a_header_only_csv_with_a_declared_watermark_saves_no_state(tmp_path: Path) -> None:
    shop = Shop(tmp_path, TIMESTAMP_MERGE)
    (shop.folder / "orders.csv").write_text("id,amount,updated\n", encoding="utf-8")

    empty = shop.run()

    assert (empty.status, empty.rows_loaded) == ("succeeded", 0)
    assert ("shop", "orders") not in shop.loader.states
    shop.write([{"id": 1, "amount": 10, "updated": "2024-01-01T00:00:00Z"}])
    assert shop.run().rows_loaded == 1
    assert shop.loader.states[("shop", "orders")].watermark_type == "timestamp with time zone"


def test_declaring_a_new_type_for_a_loaded_column_needs_a_full_refresh(tmp_path: Path) -> None:
    shop = Shop(tmp_path, MERGE)
    shop.write([{**row, "amount": f"{row['amount']}.5"} for row in ORDERS])
    shop.run()
    assert shop.table.schema["amount"] == pl.Float64
    shop.configure(MERGE + "    columns:\n      amount: decimal(12,2)\n")

    outcome = shop.run()

    record = shop.record(outcome)
    assert record["error_class"] == "SchemaDriftError"
    assert "'amount'" in record["error_message"] and "numeric(12,2)" in record["error_message"]
    assert shop.run(full_refresh=True).status == "succeeded"
    assert shop.table.schema["amount"] == pl.Decimal(12, 2)
    versions = shop.loader.versions[("shop", "orders")]
    assert len(versions) == 2
    assert ("amount", "numeric(12,2)") in versions[-1]


def test_date_watermark_from_a_spreadsheet_continues_from_the_saved_day(tmp_path: Path) -> None:
    # CSV dates arrive as text unless `columns:` declares them; spreadsheets carry real dates.
    folder = tmp_path / "sources" / "sheets"
    folder.mkdir(parents=True)
    (folder / "source.yaml").write_text(
        "connection:\n  type: excel\ndatasets:\n  - name: days\n    path: days.xlsx\n"
        "    load_mode: append\n    watermark: day\n",
        encoding="utf-8",
    )
    loader = MemoryLoader()

    def run(rows: list[tuple[int, date]]) -> RunOutcome:
        pl.DataFrame(rows, schema={"id": pl.Int64, "day": pl.Date}, orient="row").write_excel(
            folder / "days.xlsx"
        )
        config = load_source(tmp_path / "sources", "sheets", {})
        (outcome,) = run_source("sheets", config, tmp_path / "sources", loader)
        return outcome

    run([(1, date(2024, 1, 1)), (2, date(2024, 1, 2))])
    second = run([(2, date(2024, 1, 2)), (3, date(2024, 1, 3))])

    assert second.rows_loaded == 1
    assert loader.states[("sheets", "days")].watermark == date(2024, 1, 3)
    assert loader.states[("sheets", "days")].watermark_type == "date"
