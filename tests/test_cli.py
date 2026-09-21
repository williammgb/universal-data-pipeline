import importlib
import os
from pathlib import Path

import psycopg
import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory
from typer.testing import CliRunner

from udp import __version__
from udp.cli import app

APPROVED_DEPENDENCIES = [
    "alembic",
    "apscheduler",
    "dotenv",
    "fastapi",
    "fastexcel",
    "httpx",
    "polars",
    "psycopg",
    "pydantic",
    "pydantic_settings",
    "sqlalchemy",
    "structlog",
    "typer",
    "uvicorn",
    "yaml",
]


def test_version_prints_package_version() -> None:
    result = CliRunner().invoke(app, ["--version"])

    assert result.exit_code == 0
    assert result.stdout.strip() == __version__


@pytest.mark.parametrize("module", APPROVED_DEPENDENCIES)
def test_approved_dependency_imports(module: str) -> None:
    importlib.import_module(module)


def test_migrate_without_migrations_exits_2_before_touching_the_database(
    tmp_path: Path,
) -> None:
    env = {"UDP_DATABASE_URL": "postgresql://nobody:nothing@127.0.0.1:1/none"}
    result = CliRunner().invoke(app, ["migrate", "--migrations", str(tmp_path)], env=env)

    assert result.exit_code == 2
    assert "no migrations found" in result.output


@pytest.mark.db
def test_migrate_twice_leaves_the_database_at_the_newest_revision() -> None:
    for _ in range(2):
        result = CliRunner().invoke(app, ["migrate"])
        assert result.exit_code == 0, result.output[-2000:]
    with psycopg.connect(os.environ["UDP_DATABASE_URL"]) as conn:
        row = conn.execute("SELECT version_num FROM alembic_version").fetchone()
    config = Config()
    config.set_main_option("script_location", "migrations")
    newest = ScriptDirectory.from_config(config).get_current_head()
    assert row is not None and row[0] == newest
