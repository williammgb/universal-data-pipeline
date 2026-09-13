import importlib

import pytest
from typer.testing import CliRunner

from udp import __version__
from udp.cli import app

APPROVED_DEPENDENCIES = [
    "alembic",
    "apscheduler",
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
