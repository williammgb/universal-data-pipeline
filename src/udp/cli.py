from pathlib import Path
from typing import Annotated

import psycopg
import structlog
import typer

from udp import __version__
from udp.config.secrets import read_environment
from udp.config.source import load_source
from udp.errors import ConfigError
from udp.log import configure_logging
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
    """Load every dataset of a source. Exit 0 all succeeded, 1 a run failed, 2 invalid config."""
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
    if any(outcome.status != "succeeded" for outcome in outcomes):
        raise typer.Exit(1)


@app.command()
def doctor() -> None:
    """Connect to the platform database and print its server version."""
    settings = Settings()  # type: ignore[call-arg]
    with psycopg.connect(settings.database_url) as conn:
        row = conn.execute("SHOW server_version").fetchone()
    typer.echo(f"postgres {row[0] if row else 'unknown'}")
