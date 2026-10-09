import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import polars as pl
import psycopg
import pytest
from typer.testing import CliRunner

from udp.cli import app
from udp.storage.loader import INTERRUPTED

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


def _latest_run() -> tuple[Any, ...]:
    with psycopg.connect(os.environ["UDP_DATABASE_URL"]) as conn:
        row = conn.execute(
            "SELECT run_id, status, error_class, error_message FROM platform.pipeline_runs "
            "WHERE source = 'killed' ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
    assert row is not None
    return tuple(row)


def _run_status(run_id: object) -> tuple[Any, ...]:
    with psycopg.connect(os.environ["UDP_DATABASE_URL"]) as conn:
        row = conn.execute(
            "SELECT status, error_class FROM platform.pipeline_runs WHERE run_id = %s", [run_id]
        ).fetchone()
    assert row is not None
    return tuple(row)


def _dataset_lock_is_held() -> bool:
    with psycopg.connect(os.environ["UDP_DATABASE_URL"]) as conn:
        row = conn.execute(
            "SELECT count(*) FROM pg_locks, "
            "(SELECT hashtextextended('killed/orders', 0) AS k) AS key "
            "WHERE locktype = 'advisory' AND objsubid = 1 "
            "AND classid::bigint = (key.k >> 32) & 4294967295 "
            "AND objid::bigint = key.k & 4294967295"
        ).fetchone()
    return bool(row and row[0])


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
    runner = CliRunner()
    env = {"UDP_SOURCES_DIR": str(sources_dir)}
    result = runner.invoke(app, ["load", "killed", "--full-refresh"], env=env)
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
        stalled, status, _, _ = _latest_run()
        assert status == "running"

        overlapping = runner.invoke(app, ["load", "killed"], env=env)
        assert overlapping.exit_code == 0, overlapping.output
        assert _latest_run()[1] == "skipped"
    finally:
        process.kill()
        process.wait()

    assert _snapshot() == before
    assert _run_status(stalled) == ("running", None)
    # Postgres frees a killed session's advisory lock a moment after the process is gone.
    deadline = time.monotonic() + 10
    while _dataset_lock_is_held():
        assert time.monotonic() < deadline, "the killed run's dataset lock was never freed"
        time.sleep(0.2)

    after = runner.invoke(app, ["load", "killed"], env=env)

    assert after.exit_code == 0, after.output
    assert _latest_run()[1] == "succeeded"
    assert _run_status(stalled) == ("failed", INTERRUPTED)
