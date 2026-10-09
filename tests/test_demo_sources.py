import json
import sqlite3
from collections.abc import Mapping
from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import httpx
import polars as pl
import pytest
from fakes import MemoryLoader
from fastapi.testclient import TestClient
from mock_api import create_app
from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    DateTime,
    MetaData,
    Numeric,
    String,
    Table,
    create_engine,
    insert,
)

from udp.config.pipeline import load_pipeline
from udp.config.source import load_source
from udp.connectors import CONNECTORS
from udp.connectors.rest_api import RestApiConnector
from udp.pipeline.column_types import convert
from udp.pipeline.execution import run_pipeline
from udp.pipeline.runner import run_source

SOURCES = Path("sources")
DECLARED_CUSTOMERS = {
    "customer_id": pl.Int64,
    "signup_date": pl.Date,
    "lifetime_value": pl.Decimal(12, 2),
    "is_active": pl.Boolean,
}


def _run(source: str, env: Mapping[str, str], loader: MemoryLoader) -> list[str]:
    config = load_source(SOURCES, source, env)
    return [outcome.status for outcome in run_source(source, config, SOURCES, loader)]


def test_csv_demo_loads_declared_types_and_records_its_checks() -> None:
    loader = MemoryLoader()

    assert _run("demo_csv", {}, loader) == ["succeeded"]

    table = loader.tables["demo_csv__customers"]
    assert {name: table.schema[name] for name in DECLARED_CUSTOMERS} == DECLARED_CUSTOMERS
    assert table.filter(pl.col("customer_id") == 1)["lifetime_value"].item() == Decimal("1520.50")
    results = [(r["check_type"], r["severity"], r["passed"]) for r in loader.quality_results]
    assert results == [
        ("not_null", "error", True),
        ("unique", "error", True),
        ("min_rows", "error", True),
        ("range", "error", True),
        ("regex", "warn", False),
    ]
    assert loader.quality_results[4]["failing_rows"] == 1
    assert loader.quarantine == []


def test_excel_demo_loads() -> None:
    loader = MemoryLoader()

    assert _run("demo_excel", {}, loader) == ["succeeded"]
    table = loader.tables["demo_excel__products"]
    assert table.height == 30
    assert "unit_price_eur" in table.columns
    # The demo sheet has an empty cell on purpose, so one product has no stock value.
    assert table["stock_value_eur"].to_list() == pytest.approx(
        (table["unit_price_eur"] * table["stock_qty"]).to_list()
    )
    assert table["stock_value_eur"].drop_nulls().len() == 29


def test_database_demo_loads_from_sqlite(tmp_path: Path) -> None:
    path = tmp_path / "shop.db"
    engine = create_engine(f"sqlite:///{path.as_posix()}")
    orders = Table(
        "orders",
        MetaData(),
        Column("id", BigInteger, primary_key=True),
        Column("customer", String),
        Column("amount", Numeric(10, 2)),
        Column("ordered_on", Date),
        Column("updated_at", DateTime),
    )
    with engine.begin() as connection:
        orders.create(connection)
        connection.execute(
            insert(orders),
            [
                {
                    "id": i,
                    "customer": f"c{i}",
                    "amount": i * 3,
                    "ordered_on": date(2024, 1, 1),
                    "updated_at": datetime(2024, 1, 1) + timedelta(minutes=i),
                }
                for i in range(1, 43)
            ],
        )
    engine.dispose()
    loader = MemoryLoader()
    env = {"DEMO_DB_URL": f"sqlite:///{path.as_posix()}"}

    assert _run("demo_db", env, loader) == ["succeeded"]
    loaded = loader.tables["demo_db__orders"]
    assert loaded.height == 42
    assert loaded.schema["amount"] == pl.Decimal(10, 2)
    assert loaded.filter(pl.col("id") == 7)["amount"].item() == Decimal("21.00")
    assert _run("demo_db", env, loader) == ["succeeded"]
    assert [run["rows_loaded"] for run in loader.runs.values()] == [42, 0]


def test_api_demo_loads_every_dataset(monkeypatch: pytest.MonkeyPatch) -> None:
    app = create_app(token="demo-token")

    def factory(base_url: str, headers: Mapping[str, str], timeout: float) -> httpx.Client:
        return TestClient(app, base_url=base_url, headers=dict(headers))

    monkeypatch.setitem(CONNECTORS, "rest_api", RestApiConnector(client_factory=factory, waits=()))
    loader = MemoryLoader()
    env = {"DEMO_API_URL": "http://testserver", "DEMO_API_TOKEN": "demo-token"}

    assert _run("demo_api", env, loader) == ["succeeded"] * 5
    assert {name: table.height for name, table in loader.tables.items()} == {
        f"demo_api__items_{mode}": 2000 for mode in ("all", "pages", "offsets", "cursor", "linked")
    }


# --- the messy demo sources --------------------------------------------------------------------


@dataclass(frozen=True)
class Messy:
    """A messy source, its pipeline and the columns its problems are built into."""

    source: str
    dataset: str
    pipeline: str
    key: str
    whole_number: str
    when: str
    amount: str
    category: str
    rows_in: int
    rows_out: int


MESSY = (
    Messy("messy_csv", "orders", "messy_csv_orders", "order_id", "quantity", "ordered_on",
          "unit_price", "country", 20, 18),
    Messy("messy_excel", "stock", "messy_excel_stock", "sku", "units", "counted_on",
          "unit_cost", "category", 16, 14),
    Messy("messy_db", "orders", "messy_db_orders", "id", "quantity", "ordered_on",
          "amount", "region", 16, 14),
    Messy("messy_api", "readings", "messy_api_readings", "id", "score", "measured_on",
          "temp_c", "team", 14, 12),
    Messy("messy_json", "customers", "messy_json_customers", "id", "age", "signed_up",
          "spend", "plan", 15, 13),
)  # fmt: skip


def _messy_env(messy: Messy, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    """What the source needs to be read without services: the database part of init.sql run
    in SQLite, or the mock API answering in process."""
    if messy.source == "messy_db":
        script = Path("deploy/source-postgres/init.sql").read_text(encoding="utf-8")
        path = tmp_path / "source.db"
        with closing(sqlite3.connect(path)) as connection:
            connection.executescript(script[script.index("CREATE TABLE messy_orders") :])
        return {"DEMO_DB_URL": f"sqlite:///{path.as_posix()}"}
    if messy.source == "messy_api":
        app = create_app(token="demo-token")

        def factory(base_url: str, headers: Mapping[str, str], timeout: float) -> httpx.Client:
            return TestClient(app, base_url=base_url, headers=dict(headers))

        connector = RestApiConnector(client_factory=factory, waits=())
        monkeypatch.setitem(CONNECTORS, "rest_api", connector)
        return {"DEMO_API_URL": "http://testserver", "DEMO_API_TOKEN": "demo-token"}
    return {}


def _unreadable(series: pl.Series, declared: str) -> int:
    """How many values are there but cannot be read as the declared type."""
    return convert(series.cast(pl.String), declared).null_count() - series.null_count()


@pytest.mark.parametrize("messy", MESSY, ids=[m.source for m in MESSY])
def test_each_messy_source_loads_holding_the_problems_it_claims(
    messy: Messy, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    loader = MemoryLoader()

    statuses = _run(messy.source, _messy_env(messy, tmp_path, monkeypatch), loader)

    assert set(statuses) == {"succeeded"}
    raw = loader.tables[f"{messy.source}__{messy.dataset}"]
    assert raw.height == messy.rows_in
    assert raw.null_count().sum_horizontal().item() > 0, "missing values"
    assert _unreadable(raw[messy.whole_number], "integer") == 1, "a word in a number column"
    assert _unreadable(raw[messy.when], "timestamp") == 1, "a date that does not exist"
    assert raw[messy.key].is_duplicated().sum() == 2, "one row twice"
    amounts = raw[messy.amount].cast(pl.Float64)
    assert amounts.max() > 100 * amounts.median(), "an extreme outlier"  # type: ignore[operator]
    categories = raw[messy.category].drop_nulls()
    lowered = categories.str.strip_chars().str.to_lowercase()
    assert categories.n_unique() > lowered.n_unique(), "one value spelt in several cases"


PLATFORM_COLUMNS = {"_run_id", "_loaded_at", "_record_hash"}


def test_the_messy_csv_returns_are_an_empty_file_that_loads_no_rows() -> None:
    assert (SOURCES / "messy_csv" / "data" / "returns.csv").read_bytes() == b""
    loader = MemoryLoader()

    assert _run("messy_csv", {}, loader) == ["succeeded", "succeeded"]

    returns = loader.tables["messy_csv__returns"]
    assert returns.height == 0
    assert set(returns.columns) - PLATFORM_COLUMNS == {"order_id", "returned_on", "reason"}


def test_the_messy_json_records_do_not_all_have_the_same_keys() -> None:
    path = SOURCES / "messy_json" / "data" / "customers.json"
    records = json.loads(path.read_text(encoding="utf-8"))

    shapes = {frozenset(record) for record in records}

    assert len(shapes) == 5
    loader = MemoryLoader()
    assert _run("messy_json", {}, loader) == ["succeeded"]
    raw = loader.tables["messy_json__customers"]
    assert set(raw.columns) - PLATFORM_COLUMNS == set().union(*shapes)


@pytest.mark.parametrize("messy", MESSY, ids=[m.source for m in MESSY])
def test_each_messy_pipeline_cleans_its_source_and_reports_what_it_could_not(
    messy: Messy, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env = _messy_env(messy, tmp_path, monkeypatch)
    loader = MemoryLoader()
    _run(messy.source, env, loader)

    run = run_pipeline(loader, load_pipeline(SOURCES, messy.pipeline, env))

    assert (run.status, run.rows_in, run.rows_out) == ("succeeded", messy.rows_in, messy.rows_out)
    assert all(step.status == "succeeded" for step in run.steps)
    # Every step that is there to change something changed something.
    assert all(step.values_changed or step.rows_out != step.rows_in for step in run.steps)
    assert all(check.passed for check in run.validation if check.critical)
    assert [
        (check.constraint, check.failing_rows) for check in run.validation if not check.passed
    ] == [("unique", 2), ("min", 1)]
    clean = loader.clean[f"clean.{messy.source}__{messy.dataset}"]
    assert clean.height == messy.rows_out
    assert clean.schema[messy.whole_number] == pl.Int64
    assert clean.schema[messy.when] == pl.Date
    assert clean[messy.amount].null_count() == 0
