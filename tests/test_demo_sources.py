from collections.abc import Mapping
from datetime import date
from pathlib import Path

import httpx
import pytest
from fakes import MemoryLoader
from fastapi.testclient import TestClient
from mock_api import create_app
from sqlalchemy import (
    BigInteger,
    Column,
    Date,
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


def _run(source: str, env: Mapping[str, str], loader: MemoryLoader) -> list[str]:
    config = load_source(SOURCES, source, env)
    return [outcome.status for outcome in run_source(source, config, SOURCES, loader)]


def test_excel_demo_loads() -> None:
    loader = MemoryLoader()

    assert _run("demo_excel", {}, loader) == ["succeeded"]
    table = loader.tables["demo_excel__products"]
    assert table.height == 30
    assert "unit_price_eur" in table.columns


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
    )
    with engine.begin() as connection:
        orders.create(connection)
        connection.execute(
            insert(orders),
            [
                {"id": i, "customer": f"c{i}", "amount": i * 3, "ordered_on": date(2024, 1, 1)}
                for i in range(1, 43)
            ],
        )
    engine.dispose()
    loader = MemoryLoader()

    assert _run("demo_db", {"DEMO_DB_URL": f"sqlite:///{path.as_posix()}"}, loader) == ["succeeded"]
    assert loader.tables["demo_db__orders"].height == 42


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
