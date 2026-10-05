import json
import logging
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any
from uuid import UUID

import psycopg
import structlog
import typer
import uvicorn
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory

from udp import __version__
from udp.api.app import create_app
from udp.api.auth import parse_keys
from udp.api.catalog import PostgresCatalog
from udp.config.pipeline import load_pipeline, read_pipeline_file
from udp.config.secrets import read_environment
from udp.config.source import SourceConfig, load_source
from udp.errors import ConfigError, LoadError
from udp.log import configure_logging
from udp.names import Stage
from udp.orchestration.scheduler import serve
from udp.pipeline.execution import PipelineBusy, describe_run, read_run, run_pipeline
from udp.pipeline.runner import run_source
from udp.profiling.frame import ProfileSettings, parse_outlier_rule
from udp.profiling.models import StageProfile
from udp.profiling.stage import describe
from udp.settings import Settings
from udp.storage.postgres import PostgresLoader

app = typer.Typer(no_args_is_help=True)


def _print_version(value: bool) -> None:
    if value:
        typer.echo(__version__)
        raise typer.Exit()


@app.callback()
def main(
    version: Annotated[
        bool,
        typer.Option("--version", callback=_print_version, is_eager=True, help="Print version."),
    ] = False,
) -> None:
    """Universal data platform."""


@app.command()
def run(
    sources: Annotated[list[str], typer.Argument(help="Folder names under sources/.")],
    full_refresh: Annotated[
        bool,
        typer.Option("--full-refresh", help="Delete each dataset's table and load it again."),
    ] = False,
) -> None:
    """Load every dataset of each source, in the order given.

    Exit 0 when every run succeeded or was skipped because another run of its dataset was in
    progress, 1 when a run failed, 2 on invalid config.
    """
    configure_logging()
    settings = Settings()  # type: ignore[call-arg]
    environment = read_environment(Path(".env"))
    failed = False

    def read(source: str, overrides: Mapping[str, Mapping[str, Any]]) -> SourceConfig[Any, Any]:
        try:
            return load_source(settings.sources_dir, source, environment, overrides)
        except ConfigError as error:
            structlog.get_logger().error(
                "invalid source config", step="config", source=source, error=str(error)
            )
            raise typer.Exit(2) from error

    # Every file is read and checked before anything connects to anything, so a typo in one of
    # them costs nothing. The settings a run actually uses are read again below, with the edits
    # made from the dashboard, which are stored in the platform database.
    for source in sources:
        read(source, {})
    with PostgresLoader(settings.database_url) as loader:
        configs = [(source, read(source, loader.read_overrides(source))) for source in sources]
        for source, config in configs:
            outcomes = run_source(
                source, config, settings.sources_dir, loader, full_refresh=full_refresh
            )
            if any(outcome.status == "failed" for outcome in outcomes):
                failed = True
    if failed:
        raise typer.Exit(1)


def _fail(message: str, code: int) -> typer.Exit:
    typer.echo(message, err=True)
    return typer.Exit(code)


@app.command()
def profile(
    source: Annotated[str, typer.Argument(help="Folder name under sources/.")],
    dataset: Annotated[str, typer.Argument(help="Dataset name in the source's source.yaml.")],
    stage: Annotated[
        Stage, typer.Option(help="Which copy of the dataset to profile.", case_sensitive=False)
    ] = Stage.RAW,
    outliers: Annotated[
        list[str] | None,
        typer.Option(
            "--outliers",
            help="How outliers are found: [COLUMN=]iqr[:K], percentile[:LOWER:UPPER] or none; "
            "without COLUMN= it sets every column's rule. Repeat it for more columns. "
            "Default: iqr:1.5.",
        ),
    ] = None,
) -> None:
    """Profile a dataset's RAW, STAGING or CLEAN table, print the profile and store it.

    The profile belongs to the newest run that made the table. Exit 0 when it was stored, 1 when
    no run has made the table, 2 on invalid config or settings.
    """
    configure_logging()
    settings = Settings()  # type: ignore[call-arg]
    environment = read_environment(Path(".env"))
    try:
        rules = [parse_outlier_rule(text) for text in outliers or []]
        load_source(settings.sources_dir, source, environment)
    except (ConfigError, ValueError) as error:
        raise _fail(str(error), 2) from error
    per_column = {column: rule for column, rule in rules if column is not None}
    every = [rule for column, rule in rules if column is None]
    with PostgresLoader(settings.database_url) as loader:
        try:
            config = load_source(
                settings.sources_dir, source, environment, loader.read_overrides(source)
            )
        except ConfigError as error:
            raise _fail(str(error), 2) from error
        found = next((item for item in config.datasets if item.name == dataset), None)
        if found is None:
            raise _fail(f"source '{source}' has no dataset '{dataset}'", 2)
        chosen = ProfileSettings.for_dataset(found, per_column, every[-1] if every else None)
        named = f"{source}.{dataset}"
        with loader.stages() as stages:
            run = stages.newest_run(stage, source, dataset)
            if run is None:
                raise _fail(f"no run has made the {stage.value} table of {named} yet", 1)
            try:
                stored = stages.profile(
                    stage, source, dataset, run, profiled_at=datetime.now(UTC), settings=chosen
                )
            except LoadError as error:
                raise _fail(str(error), 1) from error
    result = StageProfile.model_validate(stored.profile.result)
    typer.echo(describe(result, f"{named} at {stage.value}"))
    by = (
        f"ingest run {run.ingest_run_id}"
        if run.ingest_run_id is not None
        else f"pipeline run {run.execution_id}"
    )
    typer.echo(f"\nStored as profile {stored.profile_id}, of {by}.")
    profiled = {column.name for column in result.columns}
    for column in per_column:
        if column not in profiled:
            unused = f"no column '{column}' in this table: its outlier rule was not used"
            typer.echo(unused, err=True)


pipeline_app = typer.Typer(
    no_args_is_help=True, help="Run a pipeline over its dataset, and read its runs back."
)
app.add_typer(pipeline_app, name="pipeline")


@pipeline_app.command("run")
def pipeline_run(
    name: Annotated[str, typer.Argument(help="File name under pipelines/, without .yaml.")],
) -> None:
    """Run pipelines/<name>.yaml: its dataset's rows from RAW through each step to CLEAN.

    Exit 0 when the run succeeded, 1 when it failed or another run holds the dataset, 2 on
    invalid config.
    """
    configure_logging()
    settings = Settings()  # type: ignore[call-arg]
    environment = read_environment(Path(".env"))
    try:
        written = read_pipeline_file(settings.sources_dir, name)
        load_source(settings.sources_dir, written.source, environment)
    except ConfigError as error:
        raise _fail(str(error), 2) from error
    with PostgresLoader(settings.database_url) as loader:
        try:
            pipeline = load_pipeline(
                settings.sources_dir, name, environment, loader.read_overrides(written.source)
            )
        except ConfigError as error:
            raise _fail(str(error), 2) from error
        try:
            record = run_pipeline(loader, pipeline)
        except PipelineBusy as busy:
            raise _fail(str(busy), 1) from busy
    typer.echo(describe_run(record))
    if record.status != "succeeded":
        raise typer.Exit(1)


@pipeline_app.command("status")
def pipeline_status(
    run_id: Annotated[UUID, typer.Argument(help="The run's id, as `udp pipeline run` printed.")],
    as_json: Annotated[
        bool, typer.Option("--json", help="Print the whole record as JSON.")
    ] = False,
) -> None:
    """Print a pipeline run's record: its status, steps, constraints and profiles.

    Exit 0 when the run is found, 1 when there is no such run.
    """
    configure_logging()
    settings = Settings()  # type: ignore[call-arg]
    with PostgresLoader(settings.database_url) as loader, loader.stages() as stages:
        record = read_run(stages, run_id)
    if record is None:
        raise _fail(f"no pipeline run {run_id}", 1)
    typer.echo(record.model_dump_json(indent=2) if as_json else describe_run(record))


@app.command()
def schedule() -> None:
    """Run every dataset that has a `schedule:` on its cron schedule, in UTC, until stopped."""
    configure_logging()
    settings = Settings()  # type: ignore[call-arg]
    serve(settings.sources_dir, settings.database_url, read_environment(Path(".env")))


class _StructlogHandler(logging.Handler):
    """Passes uvicorn's own log records on as JSON lines, like every other log line."""

    def emit(self, record: logging.LogRecord) -> None:
        structlog.get_logger(step="api").log(
            record.levelno, record.getMessage(), logger=record.name
        )


@app.command()
def api(
    host: Annotated[str, typer.Option(help="Address to listen on.")] = "127.0.0.1",
    port: Annotated[int, typer.Option(help="Port to listen on.")] = 8000,
    dashboard: Annotated[Path, typer.Option(help="Folder holding the built dashboard.")] = Path(
        "frontend/dist"
    ),
) -> None:
    """Serve the dashboard and the HTTP API under /api (documentation at /api/docs)."""
    configure_logging()
    settings = Settings()  # type: ignore[call-arg]
    url = settings.database_url
    keys = parse_keys(settings.api_keys)
    if not keys:
        structlog.get_logger(step="api").warning(
            "no API key set; every address is open to anyone who can reach this port"
        )
    web = create_app(
        PostgresCatalog(url),
        settings.sources_dir,
        read_environment(Path(".env")),
        lambda: PostgresLoader(url),
        dashboard,
        keys,
    )
    handler = _StructlogHandler(logging.WARNING)
    for name in ("uvicorn", "uvicorn.error"):
        server_log = logging.getLogger(name)
        server_log.handlers = [handler]
        server_log.propagate = False
    structlog.get_logger(step="api").info("api starting", host=host, port=port)
    uvicorn.run(web, host=host, port=port, log_config=None, access_log=False)


@app.command()
def openapi() -> None:
    """Print the API's description as JSON; the dashboard's types are generated from it."""
    unused = "postgresql://openapi:openapi@127.0.0.1:1/openapi"
    web = create_app(PostgresCatalog(unused), Path("sources"), {}, lambda: PostgresLoader(unused))
    typer.echo(json.dumps(web.openapi(), indent=2, sort_keys=True))


@app.command()
def migrate(
    migrations: Annotated[
        Path, typer.Option(help="Folder holding the migrations (Alembic's script location).")
    ] = Path("migrations"),
) -> None:
    """Create or update the platform's tables; safe to run again on an up-to-date database."""
    configure_logging()
    if not (migrations / "env.py").is_file():
        structlog.get_logger(step="migrate").error(
            "no migrations found", migrations=migrations.as_posix()
        )
        raise typer.Exit(2)
    config = Config()
    config.set_main_option("script_location", str(migrations))
    command.upgrade(config, "head")
    head = ScriptDirectory.from_config(config).get_current_head()
    structlog.get_logger(step="migrate").info("database migrated", revision=head)


@app.command()
def doctor() -> None:
    """Connect to the platform database and print its server version."""
    settings = Settings()  # type: ignore[call-arg]
    with psycopg.connect(settings.database_url) as conn:
        row = conn.execute("SHOW server_version").fetchone()
    typer.echo(f"postgres {row[0] if row else 'unknown'}")
