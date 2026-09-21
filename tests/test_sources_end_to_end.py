import os
from pathlib import Path

import polars as pl
import psycopg
import pytest
from typer.testing import CliRunner

from udp.cli import app

API_ENDPOINTS = ("all", "pages", "offsets", "cursor", "linked")


def _scalar(query: str) -> object:
    with psycopg.connect(os.environ["UDP_DATABASE_URL"]) as conn:
        row = conn.execute(query).fetchone()
    assert row is not None
    return row[0]


def _count(table: str) -> int:
    return int(str(_scalar(f"SELECT count(*) FROM datasets.{table}")))


def _fingerprint(table: str, key: str) -> str:
    """One hash over every column of every row, so "identical" includes _run_id and _loaded_at."""
    query = f"SELECT md5(string_agg(t::text, '|' ORDER BY {key})) FROM datasets.{table} t"
    return str(_scalar(query))


def _run(source: str, sources_dir: Path | str = "sources", *, refresh: bool = False) -> int:
    """Run a source and return the rows its most recent run loaded."""
    arguments = ["run", source, *(["--full-refresh"] if refresh else [])]
    result = CliRunner().invoke(app, arguments, env={"UDP_SOURCES_DIR": str(sources_dir)})
    assert result.exit_code == 0, result.output[-3000:]
    loaded = _scalar(
        "SELECT rows_loaded FROM platform.pipeline_runs "
        f"WHERE source = '{source}' ORDER BY started_at DESC LIMIT 1"
    )
    return int(str(loaded))


def _write_source(sources_dir: Path, name: str, text: str) -> None:
    folder = sources_dir / name
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "source.yaml").write_text(text, encoding="utf-8")


@pytest.mark.db
@pytest.mark.services
def test_database_demo_merges_nothing_on_its_second_run() -> None:
    assert _run("demo_db", refresh=True) == 1000
    before = _fingerprint("demo_db__orders", "id")
    amount_type = _scalar(
        "SELECT format_type(atttypid, atttypmod) FROM pg_attribute "
        "WHERE attrelid = 'datasets.demo_db__orders'::regclass AND attname = 'amount'"
    )
    assert amount_type == "numeric(10,2)"

    assert _run("demo_db") == 0
    assert _count("demo_db__orders") == 1000
    assert _fingerprint("demo_db__orders", "id") == before


def _stored_type(table: str, column: str) -> str:
    return str(
        _scalar(
            "SELECT format_type(atttypid, atttypmod) FROM pg_attribute "
            f"WHERE attrelid = 'datasets.{table}'::regclass AND attname = '{column}'"
        )
    )


@pytest.mark.db
@pytest.mark.services
def test_a_table_with_postgres_own_types_loads_like_pagila_film(tmp_path: Path) -> None:
    # The shapes Pagila's film table has: a domain, an enum, a text array and a search
    # vector, plus sized and unsized numeric and a binary column that is left out.
    with psycopg.connect(os.environ["DEMO_DB_URL"], autocommit=True) as conn:
        conn.execute("DROP TABLE IF EXISTS own_types")
        conn.execute("DROP DOMAIN IF EXISTS film_year")
        conn.execute("DROP TYPE IF EXISTS film_rating")
        conn.execute("CREATE DOMAIN film_year AS integer CHECK (VALUE BETWEEN 1901 AND 2155)")
        conn.execute("CREATE TYPE film_rating AS ENUM ('G', 'PG', 'R')")
        conn.execute(
            "CREATE TABLE own_types (id integer PRIMARY KEY, release_year film_year, "
            "rating film_rating, features text[], fulltext tsvector, length interval, "
            "rental_rate numeric(4,2), ratio numeric, poster bytea)"
        )
        conn.execute(
            "INSERT INTO own_types VALUES "
            "(1, 2006, 'PG', ARRAY['Trailers', 'Deleted Scenes'], to_tsvector('an epic drama'), "
            "interval '86 minutes', 0.99, 1.5, '\\x00'), "
            "(2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
        )
    _write_source(
        tmp_path,
        "own_types",
        "connection:\n  type: database\n  url: ${DEMO_DB_URL}\n"
        "datasets:\n  - name: films\n    table: own_types\n    exclude_columns: [poster]\n"
        "    checks:\n      - check: range\n        column: rental_rate\n        min: 0\n",
    )

    assert _run("own_types", tmp_path, refresh=True) == 2
    table = "own_types__films"
    assert _stored_type(table, "release_year") == "bigint"
    assert _stored_type(table, "rental_rate") == "numeric(4,2)"
    assert _stored_type(table, "rating") == "text"
    assert _stored_type(table, "features") == "text"
    assert (
        _scalar(
            f"SELECT count(*) FROM pg_attribute WHERE attname = 'poster' "
            f"AND attrelid = 'datasets.{table}'::regclass"
        )
        == 0
    )
    row = _scalar(
        f"SELECT row(release_year, rental_rate, rating, features, ratio)::text "
        f"FROM datasets.{table} WHERE id = 1"
    )
    assert row == '(2006,0.99,PG,"[""Trailers"", ""Deleted Scenes""]",1.5)'


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


@pytest.mark.db
@pytest.mark.services
def test_api_merge_loads_all_then_nothing_then_only_the_revision(tmp_path: Path) -> None:
    def configure(revision: int) -> None:
        _write_source(
            tmp_path,
            "revised_api",
            "connection:\n  type: rest_api\n  base_url: ${DEMO_API_URL}\n"
            "datasets:\n  - name: items\n    endpoint: /public/cursor\n    records_path: data\n"
            f"    params:\n      limit: 100\n      revision: {revision}\n"
            "    pagination:\n      type: cursor\n      cursor_path: meta.next_cursor\n"
            "    load_mode: merge\n    watermark: changed_in\n    primary_key: [id]\n",
        )

    configure(0)
    assert _run("revised_api", tmp_path, refresh=True) == 2000
    before = _fingerprint("revised_api__items", "id")
    assert _run("revised_api", tmp_path) == 0
    assert _fingerprint("revised_api__items", "id") == before

    configure(1)
    assert _run("revised_api", tmp_path) == 70
    assert _count("revised_api__items") == 2050


SCALE_ROWS = 500_000


@pytest.mark.db
@pytest.mark.services
@pytest.mark.scale
def test_half_million_row_source_table_merges_only_what_changed(tmp_path: Path) -> None:
    with psycopg.connect(os.environ["DEMO_DB_URL"], autocommit=True) as conn:
        conn.execute("DROP TABLE IF EXISTS scale_orders")
        conn.execute(
            "CREATE TABLE scale_orders AS "
            "SELECT i AS id, 'customer ' || (i % 997) AS customer, "
            "(i * 1.17)::numeric(12, 2) AS amount, "
            "timestamptz '2024-01-01 00:00:00+00' + i * interval '1 second' AS updated_at "
            f"FROM generate_series(1, {SCALE_ROWS}) AS i"
        )
    _write_source(
        tmp_path,
        "scale_db",
        "connection:\n  type: database\n  url: ${DEMO_DB_URL}\n"
        "datasets:\n  - name: orders\n    table: scale_orders\n"
        "    load_mode: merge\n    watermark: updated_at\n    primary_key: [id]\n",
    )

    assert _run("scale_db", tmp_path, refresh=True) == SCALE_ROWS
    before = _fingerprint("scale_db__orders", "id")
    assert _run("scale_db", tmp_path) == 0
    assert _fingerprint("scale_db__orders", "id") == before

    with psycopg.connect(os.environ["DEMO_DB_URL"], autocommit=True) as conn:
        conn.execute(
            "UPDATE scale_orders SET amount = amount + 1, "
            "updated_at = timestamptz '2025-01-01 00:00:00+00' + id * interval '1 second' "
            "WHERE id % 1000 = 0"
        )
        conn.execute(
            "INSERT INTO scale_orders SELECT i, 'customer new', 1.00, "
            "timestamptz '2025-02-01 00:00:00+00' + i * interval '1 second' "
            f"FROM generate_series({SCALE_ROWS + 1}, {SCALE_ROWS + 500}) AS i"
        )

    assert _run("scale_db", tmp_path) == 1000
    assert _count("scale_db__orders") == SCALE_ROWS + 500


SHEET_ROWS = 50_000


@pytest.mark.db
@pytest.mark.scale
def test_fifty_thousand_row_spreadsheet_appends_only_new_rows(tmp_path: Path) -> None:
    folder = tmp_path / "scale_excel" / "data"
    folder.mkdir(parents=True)

    def write(rows: int) -> None:
        pl.select(
            pl.int_range(rows).alias("Row ID"),
            (pl.lit("sku-") + pl.int_range(rows).cast(pl.String)).alias("SKU"),
            (pl.int_range(rows) * 0.25).alias("Price"),
        ).write_excel(folder / "rows.xlsx", worksheet="rows")

    _write_source(
        tmp_path,
        "scale_excel",
        "connection:\n  type: excel\ndatasets:\n  - name: rows\n    path: data/rows.xlsx\n"
        "    load_mode: append\n    watermark: row_id\n",
    )

    write(SHEET_ROWS)
    assert _run("scale_excel", tmp_path, refresh=True) == SHEET_ROWS
    assert _run("scale_excel", tmp_path) == 0

    write(SHEET_ROWS + 1000)
    assert _run("scale_excel", tmp_path) == 1000
    assert _count("scale_excel__rows") == SHEET_ROWS + 1000
