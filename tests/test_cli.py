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


def test_update_without_migrations_exits_2_before_touching_the_database(
    tmp_path: Path,
) -> None:
    env = {"UDP_DATABASE_URL": "postgresql://nobody:nothing@127.0.0.1:1/none"}
    result = CliRunner().invoke(app, ["update", "--migrations", str(tmp_path)], env=env)

    assert result.exit_code == 2
    assert "no migrations found" in result.output


def _csv_source(root: Path, name: str, rows: str) -> None:
    folder = root / name
    (folder / "data").mkdir(parents=True)
    (folder / "data" / "rows.csv").write_text(f"id,word\n{rows}", encoding="utf-8")
    (folder / "source.yaml").write_text(
        "connection:\n  type: csv\ndatasets:\n  - name: rows\n    path: data/rows.csv\n"
        "    load_mode: full\n    columns:\n      id: integer\n",
        encoding="utf-8",
    )


def test_load_refuses_every_source_when_one_of_them_has_a_bad_config(tmp_path: Path) -> None:
    _csv_source(tmp_path, "first", "1,a\n")
    env = {
        "UDP_SOURCES_DIR": str(tmp_path),
        "UDP_DATABASE_URL": "postgresql://nobody:nothing@127.0.0.1:1/none",
    }

    result = CliRunner().invoke(app, ["load", "first", "missing"], env=env)

    # Exit 2 for the config, not 1 for a failed run: the database was never opened.
    assert result.exit_code == 2
    assert "invalid source config" in result.output


def test_load_with_no_names_lists_the_sources_without_a_database(tmp_path: Path) -> None:
    _csv_source(tmp_path, "second", "1,a\n")
    _csv_source(tmp_path, "first", "1,a\n")
    (tmp_path / "no_file_yet").mkdir()
    env = {"UDP_SOURCES_DIR": str(tmp_path), "UDP_DATABASE_URL": None}

    result = CliRunner().invoke(app, ["load"], env=env)

    assert result.exit_code == 2
    assert (
        result.output.strip() == "Name a source to load: udp load <source>. Sources: first, second"
    )


def test_load_with_no_names_and_no_sources_says_how_to_add_one(tmp_path: Path) -> None:
    env = {"UDP_SOURCES_DIR": str(tmp_path / "sources"), "UDP_DATABASE_URL": None}

    result = CliRunner().invoke(app, ["load"], env=env)

    assert result.exit_code == 2
    shown = (tmp_path / "sources").as_posix()
    assert result.output.strip() == (
        f"No sources yet. Put your files in {shown}/<name>/ and run: udp new <name>"
    )


def test_new_writes_source_yaml_and_says_how_to_load_it(tmp_path: Path) -> None:
    (tmp_path / "my_shop" / "data").mkdir(parents=True)
    (tmp_path / "my_shop" / "data" / "customers.csv").write_text("id\n1\n", encoding="utf-8")
    (tmp_path / "my_shop" / "orders.csv").write_text("id\n1\n", encoding="utf-8")
    env = {"UDP_SOURCES_DIR": str(tmp_path), "UDP_DATABASE_URL": None}

    result = CliRunner().invoke(app, ["new", "my_shop"], env=env)

    assert result.exit_code == 0, result.output
    shown = (tmp_path / "my_shop" / "source.yaml").as_posix()
    assert result.output.strip() == (
        f"Wrote {shown} with 2 datasets: orders, customers. Load it: udp load my_shop"
    )
    assert (tmp_path / "my_shop" / "source.yaml").is_file()


def test_new_with_one_file_says_dataset(tmp_path: Path) -> None:
    (tmp_path / "my_shop").mkdir()
    (tmp_path / "my_shop" / "customers.csv").write_text("id\n1\n", encoding="utf-8")

    result = CliRunner().invoke(app, ["new", "my_shop"], env={"UDP_SOURCES_DIR": str(tmp_path)})

    assert result.exit_code == 0, result.output
    assert " with 1 dataset: customers. " in result.output


def test_new_refusal_prints_the_reason_and_exits_2(tmp_path: Path) -> None:
    result = CliRunner().invoke(app, ["new", "my_shop"], env={"UDP_SOURCES_DIR": str(tmp_path)})

    assert result.exit_code == 2
    assert result.output.startswith(f"No folder {(tmp_path / 'my_shop').as_posix()}. ")


@pytest.mark.db
def test_load_loads_several_sources_in_one_command(tmp_path: Path) -> None:
    _csv_source(tmp_path, "first", "1,a\n2,b\n")
    _csv_source(tmp_path, "second", "3,c\n")
    env = {"UDP_SOURCES_DIR": str(tmp_path), "UDP_DATABASE_URL": os.environ["UDP_DATABASE_URL"]}

    result = CliRunner().invoke(app, ["load", "first", "second", "--full-refresh"], env=env)

    assert result.exit_code == 0, result.output[-2000:]
    with psycopg.connect(os.environ["UDP_DATABASE_URL"]) as conn:
        loaded = conn.execute(
            "SELECT source, rows_loaded FROM platform.pipeline_runs "
            "WHERE source IN ('first', 'second') ORDER BY source"
        ).fetchall()
    assert loaded == [("first", 2), ("second", 1)]


@pytest.mark.parametrize("old", ["run", "migrate"])
def test_the_old_command_names_are_gone(old: str) -> None:
    result = CliRunner().invoke(app, [old])

    assert result.exit_code != 0
    assert "No such command" in result.output


@pytest.mark.db
def test_update_twice_leaves_the_database_at_the_newest_revision() -> None:
    for _ in range(2):
        result = CliRunner().invoke(app, ["update"])
        assert result.exit_code == 0, result.output[-2000:]
    with psycopg.connect(os.environ["UDP_DATABASE_URL"]) as conn:
        row = conn.execute("SELECT version_num FROM alembic_version").fetchone()
    config = Config()
    config.set_main_option("script_location", "migrations")
    newest = ScriptDirectory.from_config(config).get_current_head()
    assert row is not None and row[0] == newest
