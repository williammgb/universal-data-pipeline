import os
import subprocess
import sys
import time
from pathlib import Path

import polars as pl
import psycopg
import pytest
from typer.testing import CliRunner

from udp.cli import app

ROWS = 50


def _snapshot() -> tuple[object, ...]:
    with psycopg.connect(os.environ["UDP_DATABASE_URL"]) as conn:
        conn.execute("SET lock_timeout = '30s'")
        fingerprint = conn.execute(
            "SELECT md5(string_agg(t::text, '|' ORDER BY id)) FROM datasets.killed__orders t"
        ).fetchone()
        columns = conn.execute(
            "SELECT string_agg(column_name, ',' ORDER BY ordinal_position) "
            "FROM information_schema.columns "
            "WHERE table_schema = 'datasets' AND table_name = 'killed__orders'"
        ).fetchone()
        state = conn.execute(
            "SELECT watermark, file_sha256, run_id FROM platform.source_state "
            "WHERE source = 'killed' AND dataset = 'orders'"
        ).fetchone()
    return fingerprint, columns, state


def _latest_run_status() -> object:
    with psycopg.connect(os.environ["UDP_DATABASE_URL"]) as conn:
        row = conn.execute(
            "SELECT status FROM platform.pipeline_runs WHERE source = 'killed' "
            "ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
    assert row is not None
    return row[0]


@pytest.mark.db
def test_killing_a_run_mid_load_leaves_table_columns_and_state_unchanged(tmp_path: Path) -> None:
    sources_dir = tmp_path / "sources"
    folder = sources_dir / "killed"
    folder.mkdir(parents=True)
    (folder / "source.yaml").write_text(
        "connection:\n  type: csv\ndatasets:\n  - name: orders\n    path: orders.csv\n"
        "    load_mode: merge\n    watermark: updated\n    primary_key: [id]\n",
        encoding="utf-8",
    )
    ids = pl.int_range(ROWS, eager=True)
    pl.DataFrame({"id": ids, "amount": ids * 2, "updated": 1}).write_csv(folder / "orders.csv")
    result = CliRunner().invoke(
        app, ["run", "killed", "--full-refresh"], env={"UDP_SOURCES_DIR": str(sources_dir)}
    )
    assert result.exit_code == 0, result.output
    before = _snapshot()

    pl.DataFrame({"id": ids, "amount": ids * 3, "updated": 2, "note": "changed"}).write_csv(
        folder / "orders.csv"
    )
    marker = tmp_path / "stalled.marker"
    process = subprocess.Popen(
        [
            sys.executable,
            str(Path(__file__).with_name("stalled_run.py")),
            str(sources_dir),
            "killed",
            str(marker),
        ]
    )
    try:
        deadline = time.monotonic() + 60
        while not marker.exists():
            assert process.poll() is None, "the stalled run exited before its first chunk"
            assert time.monotonic() < deadline, "the stalled run never reached its first chunk"
            time.sleep(0.2)
        assert _latest_run_status() == "running"
    finally:
        process.kill()
        process.wait()

    assert _snapshot() == before
    assert _latest_run_status() == "running"
