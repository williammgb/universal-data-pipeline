from typing import Annotated

import psycopg
import typer

from udp import __version__
from udp.settings import Settings

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
def doctor() -> None:
    """Connect to the platform database and print its server version."""
    settings = Settings()  # type: ignore[call-arg]
    with psycopg.connect(settings.database_url) as conn:
        row = conn.execute("SHOW server_version").fetchone()
    typer.echo(f"postgres {row[0] if row else 'unknown'}")
