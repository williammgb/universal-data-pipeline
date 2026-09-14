import os
from pathlib import Path

import polars as pl
import psycopg
import pytest
from typer.testing import CliRunner

from udp.cli import app

ROWS = 1_000_000


@pytest.mark.db
@pytest.mark.scale
def test_million_row_csv_loads_twice_with_the_same_count(tmp_path: Path) -> None:
    sources_dir = tmp_path / "sources"
    folder = sources_dir / "scale_csv"
    folder.mkdir(parents=True)
    (folder / "source.yaml").write_text(
        "connection:\n  type: csv\ndatasets:\n  - name: events\n    path: events.csv\n",
        encoding="utf-8",
    )
    pl.select(
        pl.int_range(ROWS).alias("Event ID"),
        (pl.lit("user_") + (pl.int_range(ROWS) % 5000).cast(pl.String)).alias("User"),
        (pl.int_range(ROWS) * 0.37).alias("Amount"),
        (pl.int_range(ROWS) % 2 == 0).alias("Is Refund"),
    ).write_csv(folder / "events.csv")

    runner = CliRunner()
    for _ in range(2):
        result = runner.invoke(app, ["run", "scale_csv"], env={"UDP_SOURCES_DIR": str(sources_dir)})
        assert result.exit_code == 0, result.output[-2000:]

        with psycopg.connect(os.environ["UDP_DATABASE_URL"]) as conn:
            count = conn.execute("SELECT count(*) FROM datasets.scale_csv__events").fetchone()
            runs = conn.execute(
                "SELECT rows_loaded FROM platform.pipeline_runs "
                "WHERE source = 'scale_csv' ORDER BY started_at DESC LIMIT 1"
            ).fetchone()
        assert count == (ROWS,)
        assert runs == (ROWS,)
