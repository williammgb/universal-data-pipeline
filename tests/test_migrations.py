"""`udp migrate` creates the V2 tables on an empty database and upgrades an existing V1 one
without touching what is in it; a second run changes nothing."""

import os
import uuid
from collections.abc import Iterator
from typing import Any
from urllib.parse import urlsplit

import psycopg
import pytest
from alembic import command
from alembic.config import Config
from typer.testing import CliRunner

from udp.cli import app
from udp.settings import Settings

pytestmark = pytest.mark.db

NEW_SCHEMAS = {"raw", "staging", "clean"}
NEW_TABLES = {
    "pipelines",
    "pipeline_versions",
    "pipeline_steps",
    "pipeline_executions",
    "step_executions",
    "profiles",
    "constraint_results",
    "constraint_violations",
    "lineage",
}
WATCHED = ["platform", "datasets", "raw", "staging", "clean"]


def _snapshot(url: str) -> dict[str, Any]:
    """Every schema, column, constraint, index, function and the revision the database is at."""
    with psycopg.connect(url) as conn:

        def rows(query: str) -> list[tuple[Any, ...]]:
            return conn.execute(query, [WATCHED]).fetchall()

        return {
            "schemas": rows("SELECT nspname FROM pg_namespace WHERE nspname = ANY(%s) ORDER BY 1"),
            "columns": rows(
                "SELECT table_schema, table_name, column_name, data_type, is_nullable "
                "FROM information_schema.columns WHERE table_schema = ANY(%s) ORDER BY 1, 2, 3"
            ),
            "constraints": rows(
                "SELECT n.nspname, c.conname, pg_get_constraintdef(c.oid) FROM pg_constraint c "
                "JOIN pg_namespace n ON n.oid = c.connamespace WHERE n.nspname = ANY(%s) "
                "ORDER BY 1, 2, 3"
            ),
            "indexes": rows(
                "SELECT schemaname, indexname, indexdef FROM pg_indexes "
                "WHERE schemaname = ANY(%s) ORDER BY 1, 2"
            ),
            "functions": rows(
                "SELECT n.nspname, p.proname FROM pg_proc p "
                "JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = ANY(%s) "
                "ORDER BY 1, 2"
            ),
            "revision": conn.execute("SELECT version_num FROM alembic_version").fetchall(),
        }


def _tables(snapshot: dict[str, Any], schema: str) -> set[str]:
    return {table for owner, table, *_ in snapshot["columns"] if owner == schema}


def _migrate() -> None:
    result = CliRunner().invoke(app, ["migrate"])
    assert result.exit_code == 0, result.output[-2000:]


def test_the_migrated_database_has_every_v2_table_and_a_second_migrate_changes_nothing() -> None:
    url = os.environ["UDP_DATABASE_URL"]
    before = _snapshot(url)

    _migrate()

    assert _snapshot(url) == before
    assert _tables(before, "platform") >= NEW_TABLES
    assert {name for (name,) in before["schemas"]} >= NEW_SCHEMAS
    assert ("platform", "refuse_raw_change") in before["functions"]


@pytest.fixture
def empty_database(monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    """A database of its own, which `udp migrate` and Alembic reach through UDP_DATABASE_URL."""
    url = Settings().database_url  # type: ignore[call-arg]
    name = f"udp_migrate_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(url, autocommit=True) as admin:
        admin.execute(f"CREATE DATABASE {name}")
        try:
            own = urlsplit(url)._replace(path=f"/{name}").geturl()
            monkeypatch.setenv("UDP_DATABASE_URL", own)
            yield own
        finally:
            admin.execute(f"DROP DATABASE {name} WITH (FORCE)")


def test_an_existing_v1_database_upgrades_and_keeps_its_data(empty_database: str) -> None:
    config = Config()
    config.set_main_option("script_location", "migrations")
    command.upgrade(config, "0005")
    run_id = uuid.uuid4()
    with psycopg.connect(empty_database) as conn:
        conn.execute(
            "INSERT INTO platform.pipeline_runs (run_id, source, dataset, trigger, status, "
            "started_at) VALUES (%s, 'shop', 'orders', 'manual', 'running', now())",
            [run_id],
        )
        conn.execute("CREATE TABLE datasets.shop__orders AS SELECT 1 AS id, 'ada' AS name")
    v1 = _snapshot(empty_database)

    _migrate()
    upgraded = _snapshot(empty_database)
    _migrate()

    assert _snapshot(empty_database) == upgraded
    assert _tables(upgraded, "platform") == _tables(v1, "platform") | NEW_TABLES
    assert _tables(upgraded, "datasets") == {"shop__orders"}
    with psycopg.connect(empty_database) as conn:
        assert conn.execute("SELECT id, name FROM datasets.shop__orders").fetchall() == [(1, "ada")]
        assert conn.execute(
            "SELECT status FROM platform.pipeline_runs WHERE run_id = %s", [run_id]
        ).fetchall() == [("running",)]

    command.downgrade(config, "0005")
    assert _snapshot(empty_database) == v1
    command.upgrade(config, "head")
    assert _snapshot(empty_database) == upgraded
