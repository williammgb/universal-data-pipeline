from collections.abc import Mapping
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

from udp.config.source import load_source
from udp.connectors import CONNECTORS
from udp.connectors.rest_api import RestApiConnector
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
