import json
import os
from pathlib import Path
from uuid import uuid7

import psycopg
import pytest
from psycopg.rows import dict_row
from typer.testing import CliRunner

from udp.cli import app
from udp.names import RESERVED_COLUMNS
from udp.pipeline.runner import RunOutcome

UNREACHABLE_DATABASE = "postgresql://x:x@127.0.0.1:1/x"


def _write_source(sources_dir: Path, name: str, text: str) -> None:
    folder = sources_dir / name
    folder.mkdir(parents=True)
    (folder / "source.yaml").write_text(text, encoding="utf-8")


def test_invalid_config_exits_2_without_touching_the_database(tmp_path: Path) -> None:
    sources_dir = tmp_path / "sources"
    _write_source(
        sources_dir,
        "shop",
        "connection:\n  type: csv\ndatasets:\n"
        "  - name: orders\n    path: orders.csv\n"
        "  - name: items\n",
    )

    result = CliRunner().invoke(
        app,
        ["run", "shop"],
        env={"UDP_DATABASE_URL": UNREACHABLE_DATABASE, "UDP_SOURCES_DIR": str(sources_dir)},
    )

    assert result.exit_code == 2, result.output
    (line,) = [json.loads(text) for text in result.stdout.splitlines()]
    assert line["step"] == "config"
    assert "sources/shop/source.yaml" in line["error"]
    assert "datasets[1].path" in line["error"]


def test_unset_secret_exits_2_without_touching_the_database(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sources_dir = Path("sources").resolve()
    monkeypatch.chdir(tmp_path)  # away from any developer's .env

    result = CliRunner().invoke(
        app,
        ["run", "demo_api"],
        env={
            "UDP_DATABASE_URL": UNREACHABLE_DATABASE,
            "UDP_SOURCES_DIR": str(sources_dir),
            "DEMO_API_URL": "http://api.test",
            "DEMO_API_TOKEN": None,
        },
    )

    assert result.exit_code == 2, result.output
    (line,) = [json.loads(text) for text in result.stdout.splitlines()]
    assert "connection.auth.token" in line["error"]
    assert "DEMO_API_TOKEN" in line["error"]


def _query(sql: str, *params: object) -> list[dict[str, object]]:
    with psycopg.connect(os.environ["UDP_DATABASE_URL"], row_factory=dict_row) as conn:
        return conn.execute(sql, params).fetchall()


def test_full_refresh_flag_reaches_the_runner(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_run_source(*args: object, **kwargs: object) -> list[object]:
        calls.append(kwargs)
        return []

    monkeypatch.setattr("udp.cli.run_source", fake_run_source)
    runner = CliRunner()
    env = {"UDP_DATABASE_URL": UNREACHABLE_DATABASE, "UDP_SOURCES_DIR": "sources"}

    assert runner.invoke(app, ["run", "demo_csv", "--full-refresh"], env=env).exit_code == 0
    assert runner.invoke(app, ["run", "demo_csv"], env=env).exit_code == 0
    assert [call["full_refresh"] for call in calls] == [True, False]


@pytest.mark.parametrize(("status", "exit_code"), [("skipped", 0), ("failed", 1)])
def test_only_a_failed_run_makes_the_command_fail(
    monkeypatch: pytest.MonkeyPatch, status: str, exit_code: int
) -> None:
    def fake_run_source(*args: object, **kwargs: object) -> list[RunOutcome]:
        return [
            RunOutcome(uuid7(), "customers", "succeeded", 20),
            RunOutcome(uuid7(), "customers", status, None),  # type: ignore[arg-type]
        ]

    monkeypatch.setattr("udp.cli.run_source", fake_run_source)
    env = {"UDP_DATABASE_URL": UNREACHABLE_DATABASE, "UDP_SOURCES_DIR": "sources"}

    assert CliRunner().invoke(app, ["run", "demo_csv"], env=env).exit_code == exit_code


def test_schedule_command_serves_the_configured_sources(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[object, ...]] = []
    monkeypatch.setattr("udp.cli.serve", lambda *args: calls.append(args))
    env = {"UDP_DATABASE_URL": UNREACHABLE_DATABASE, "UDP_SOURCES_DIR": "sources"}

    result = CliRunner().invoke(app, ["schedule"], env=env)

    assert result.exit_code == 0, result.output
    ((sources_dir, database_url, _),) = calls
    assert (sources_dir, database_url) == (Path("sources"), UNREACHABLE_DATABASE)


def test_api_command_serves_on_localhost_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, object]] = []
    monkeypatch.setattr("udp.cli.uvicorn.run", lambda app, **options: calls.append(options))
    env = {"UDP_DATABASE_URL": UNREACHABLE_DATABASE, "UDP_SOURCES_DIR": "sources"}

    result = CliRunner().invoke(app, ["api"], env=env)

    assert result.exit_code == 0, result.output
    ((options),) = calls
    assert (options["host"], options["port"]) == ("127.0.0.1", 8000)


@pytest.mark.db
def test_demo_source_loads_then_skips_the_unchanged_file() -> None:
    runner = CliRunner()

    for arguments in (["run", "demo_csv", "--full-refresh"], ["run", "demo_csv"]):
        result = runner.invoke(app, arguments, env={"UDP_SOURCES_DIR": "sources"})
        assert result.exit_code == 0, result.output

        (count,) = _query("SELECT count(*) AS n FROM datasets.demo_csv__customers")
        assert count["n"] == 20

    nulls = _query(
        "SELECT count(*) AS n FROM datasets.demo_csv__customers "
        "WHERE _run_id IS NULL OR _loaded_at IS NULL OR _record_hash IS NULL"
    )
    assert nulls[0]["n"] == 0
    columns = _query(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'datasets' AND table_name = 'demo_csv__customers' "
        "ORDER BY ordinal_position"
    )
    assert [c["column_name"] for c in columns][-3:] == list(RESERVED_COLUMNS)

    runs = _query(
        "SELECT * FROM platform.pipeline_runs WHERE source = 'demo_csv' ORDER BY started_at"
    )
    assert len(runs) >= 2
    loaded, skipped = runs[-2:]
    for run in (loaded, skipped):
        assert (run["status"], run["trigger"], run["dataset"]) == (
            "succeeded",
            "manual",
            "customers",
        )
        assert run["started_at"] <= run["ended_at"]  # type: ignore[operator]
    assert loaded["rows_extracted"] == loaded["rows_loaded"] == 20
    assert skipped["rows_extracted"] == skipped["rows_loaded"] == 0
    latest = _query("SELECT DISTINCT _run_id::text AS id FROM datasets.demo_csv__customers")
    assert [row["id"] for row in latest] == [str(loaded["run_id"])]


@pytest.mark.db
def test_missing_file_exits_1_and_records_a_failed_run(tmp_path: Path) -> None:
    sources_dir = tmp_path / "sources"
    _write_source(
        sources_dir,
        "lost_file",
        "connection:\n  type: csv\ndatasets:\n  - name: orders\n    path: data/gone.csv\n",
    )

    result = CliRunner().invoke(
        app, ["run", "lost_file"], env={"UDP_SOURCES_DIR": str(sources_dir)}
    )

    assert result.exit_code == 1, result.output
    (run,) = _query("SELECT * FROM platform.pipeline_runs WHERE source = 'lost_file'")
    assert run["status"] == "failed"
    assert run["error_class"] == "ExtractError"
    assert "data/gone.csv" in str(run["error_message"])
    assert run["ended_at"] is not None
