import os
from pathlib import Path

import polars as pl
import psycopg
import pytest
from typer.testing import CliRunner

from udp.cli import app

ROWS = 1_000_000
CHANGED = 1_000


def _query(sql: str) -> tuple[object, ...]:
    with psycopg.connect(os.environ["UDP_DATABASE_URL"]) as conn:
        row = conn.execute(sql).fetchone()
    assert row is not None
    return tuple(row)


@pytest.mark.db
@pytest.mark.scale
def test_million_row_csv_merges_all_then_nothing_then_only_the_changes(tmp_path: Path) -> None:
    sources_dir = tmp_path / "sources"
    folder = sources_dir / "scale_csv"
    folder.mkdir(parents=True)
    (folder / "source.yaml").write_text(
        "connection:\n  type: csv\ndatasets:\n  - name: events\n    path: events.csv\n"
        "    load_mode: merge\n    watermark: change_seq\n    primary_key: [event_id]\n",
        encoding="utf-8",
    )
    events = pl.select(
        pl.int_range(ROWS).alias("Event ID"),
        (pl.lit("user_") + (pl.int_range(ROWS) % 5000).cast(pl.String)).alias("User"),
        (pl.int_range(ROWS) * 0.37).alias("Amount"),
        (pl.int_range(ROWS) % 2 == 0).alias("Is Refund"),
        pl.int_range(ROWS).alias("Change Seq"),
    )
    events.write_csv(folder / "events.csv")
    runner = CliRunner()

    def run(*extra: str) -> tuple[int, int, str]:
        arguments = ["load", "scale_csv", *extra]
        result = runner.invoke(app, arguments, env={"UDP_SOURCES_DIR": str(sources_dir)})
        assert result.exit_code == 0, result.output[-2000:]
        (loaded,) = _query(
            "SELECT rows_loaded FROM platform.pipeline_runs "
            "WHERE source = 'scale_csv' ORDER BY started_at DESC LIMIT 1"
        )
        count, fingerprint = _query(
            "SELECT count(*), md5(string_agg(t::text, '|' ORDER BY event_id)) "
            "FROM datasets.scale_csv__events t"
        )
        return int(str(loaded)), int(str(count)), str(fingerprint)

    loaded, count, first_fingerprint = run("--full-refresh")
    assert (loaded, count) == (ROWS, ROWS)
    assert run() == (0, ROWS, first_fingerprint)

    changed = events.with_columns(
        pl.when(pl.col("Event ID") % (ROWS // CHANGED) == 0)
        .then(pl.col("Amount") + 1)
        .otherwise(pl.col("Amount"))
        .alias("Amount"),
        pl.when(pl.col("Event ID") % (ROWS // CHANGED) == 0)
        .then(pl.col("Event ID") + ROWS)
        .otherwise(pl.col("Change Seq"))
        .alias("Change Seq"),
    )
    appended = pl.select(
        (pl.int_range(CHANGED) + ROWS).alias("Event ID"),
        pl.lit("user_new").alias("User"),
        pl.lit(1.0).alias("Amount"),
        pl.lit(False).alias("Is Refund"),
        (pl.int_range(CHANGED) + 3 * ROWS).alias("Change Seq"),
    )
    pl.concat([changed, appended]).write_csv(folder / "events.csv")

    loaded, count, _ = run()
    assert (loaded, count) == (2 * CHANGED, ROWS + CHANGED)
