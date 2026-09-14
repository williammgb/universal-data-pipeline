import os
from pathlib import Path

import polars as pl
import psycopg
import pytest
from typer.testing import CliRunner

from udp.cli import app

API_ENDPOINTS = ("all", "pages", "offsets", "cursor", "linked")


def _count(table: str) -> int:
    with psycopg.connect(os.environ["UDP_DATABASE_URL"]) as conn:
        row = conn.execute(f"SELECT count(*) FROM datasets.{table}").fetchone()
    assert row is not None
    return int(row[0])


def _run(source: str, sources_dir: Path | str = "sources") -> None:
    result = CliRunner().invoke(app, ["run", source], env={"UDP_SOURCES_DIR": str(sources_dir)})
    assert result.exit_code == 0, result.output[-3000:]


def _write_source(sources_dir: Path, name: str, text: str) -> None:
    folder = sources_dir / name
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "source.yaml").write_text(text, encoding="utf-8")


@pytest.mark.db
@pytest.mark.services
def test_database_demo_loads_twice_from_the_source_postgres() -> None:
    for _ in range(2):
        _run("demo_db")
        assert _count("demo_db__orders") == 1000


@pytest.mark.db
@pytest.mark.services
def test_api_demo_loads_twice_from_the_mock_api() -> None:
    for _ in range(2):
        _run("demo_api")
        for endpoint in API_ENDPOINTS:
            assert _count(f"demo_api__items_{endpoint}") == 2000


@pytest.mark.db
@pytest.mark.services
@pytest.mark.parametrize(
    ("name", "prefix", "auth"),
    [
        ("public_api", "/public", "    type: none\n"),
        ("keyed_api", "/api-key", "    type: api_key\n    key: ${DEMO_API_TOKEN}\n"),
    ],
)
def test_other_auth_modes_load_from_the_mock_api(
    tmp_path: Path, name: str, prefix: str, auth: str
) -> None:
    _write_source(
        tmp_path,
        name,
        "connection:\n  type: rest_api\n  base_url: ${DEMO_API_URL}\n"
        f"  auth:\n{auth}"
        f"datasets:\n  - name: items\n    endpoint: {prefix}/all\n    records_path: data\n",
    )

    _run(name, tmp_path)

    assert _count(f"{name}__items") == 2000


SCALE_ROWS = 500_000


@pytest.mark.db
@pytest.mark.services
@pytest.mark.scale
def test_half_million_row_source_table_loads_twice(tmp_path: Path) -> None:
    with psycopg.connect(os.environ["DEMO_DB_URL"], autocommit=True) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS scale_orders AS "
            "SELECT i AS id, 'customer ' || (i % 997) AS customer, "
            "(i * 1.17)::numeric(12, 2) AS amount, "
            "timestamptz '2024-01-01 00:00:00+00' + i * interval '1 second' AS updated_at "
            f"FROM generate_series(1, {SCALE_ROWS}) AS i"
        )
    _write_source(
        tmp_path,
        "scale_db",
        "connection:\n  type: database\n  url: ${DEMO_DB_URL}\n"
        "datasets:\n  - name: orders\n    table: scale_orders\n",
    )

    for _ in range(2):
        _run("scale_db", tmp_path)
        assert _count("scale_db__orders") == SCALE_ROWS


SHEET_ROWS = 50_000


@pytest.mark.db
@pytest.mark.scale
def test_fifty_thousand_row_spreadsheet_loads_twice(tmp_path: Path) -> None:
    folder = tmp_path / "scale_excel" / "data"
    folder.mkdir(parents=True)
    pl.select(
        pl.int_range(SHEET_ROWS).alias("Row ID"),
        (pl.lit("sku-") + pl.int_range(SHEET_ROWS).cast(pl.String)).alias("SKU"),
        (pl.int_range(SHEET_ROWS) * 0.25).alias("Price"),
    ).write_excel(folder / "rows.xlsx", worksheet="rows")
    _write_source(
        tmp_path,
        "scale_excel",
        "connection:\n  type: excel\ndatasets:\n  - name: rows\n    path: data/rows.xlsx\n",
    )

    for _ in range(2):
        _run("scale_excel", tmp_path)
        assert _count("scale_excel__rows") == SHEET_ROWS
