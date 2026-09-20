import json
import logging
from pathlib import Path
from typing import Annotated

import psycopg
import structlog
import typer
import uvicorn

from udp import __version__
from udp.api.app import create_app
from udp.api.auth import parse_keys
from udp.api.catalog import PostgresCatalog
from udp.config.secrets import read_environment
from udp.config.source import load_source
from udp.errors import ConfigError
from udp.log import configure_logging
from udp.orchestration.scheduler import serve
from udp.pipeline.runner import run_source
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
    source: Annotated[str, typer.Argument(help="Folder name under sources/.")],
    full_refresh: Annotated[
        bool,
        typer.Option("--full-refresh", help="Delete each dataset's table and load it again."),
    ] = False,
) -> None:
    """Load every dataset of a source.

    Exit 0 when every run succeeded or was skipped because another run of its dataset was in
    progress, 1 when a run failed, 2 on invalid config.
    """
    configure_logging()
    settings = Settings()  # type: ignore[call-arg]
    try:
        config = load_source(settings.sources_dir, source, read_environment(Path(".env")))
    except ConfigError as error:
        structlog.get_logger().error(
            "invalid source config", step="config", source=source, error=str(error)
        )
        raise typer.Exit(2) from error
    with PostgresLoader(settings.database_url) as loader:
        outcomes = run_source(
            source, config, settings.sources_dir, loader, full_refresh=full_refresh
        )
    if any(outcome.status == "failed" for outcome in outcomes):
        raise typer.Exit(1)


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
def doctor() -> None:
    """Connect to the platform database and print its server version."""
    settings = Settings()  # type: ignore[call-arg]
    with psycopg.connect(settings.database_url) as conn:
        row = conn.execute("SHOW server_version").fetchone()
    typer.echo(f"postgres {row[0] if row else 'unknown'}")
