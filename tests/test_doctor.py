import pytest
from typer.testing import CliRunner

from udp.cli import app


@pytest.mark.db
def test_doctor_reports_postgres_18() -> None:
    result = CliRunner().invoke(app, ["doctor"])

    assert result.exit_code == 0, result.output
    assert result.stdout.startswith("postgres 18.")
