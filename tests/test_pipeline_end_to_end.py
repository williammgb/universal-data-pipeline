"""The messy demo sources, from the command line against the real stack: each read into RAW and
prepared by its pipeline into CLEAN; then that a pipeline run twice gives the same result, that
a finished run's lineage is complete, and that a run failing half-way leaves RAW and CLEAN as
they were. The same sources run in memory in tests/test_demo_sources.py."""

import os
import re
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psycopg
import pytest
import yaml
from typer.testing import CliRunner

from udp.cli import app
from udp.pipeline.execution import PipelineRun


@dataclass(frozen=True)
class Messy:
    source: str
    dataset: str
    pipeline: str
    read_from: str
    rows_in: int
    rows_out: int
    whole_number: str
    when: str


MESSY = (
    Messy("messy_csv", "orders", "messy_csv_orders", "csv: data/orders.csv", 20, 18,
          "quantity", "ordered_on"),
    Messy("messy_excel", "stock", "messy_excel_stock", "excel: data/stock.xlsx", 16, 14,
          "units", "counted_on"),
    Messy("messy_db", "orders", "messy_db_orders", "database: messy_orders", 16, 14,
          "quantity", "ordered_on"),
    Messy("messy_api", "readings", "messy_api_readings", "rest_api: /bearer/messy", 14, 12,
          "score", "measured_on"),
    Messy("messy_json", "customers", "messy_json_customers", "json: data/customers.json", 15, 13,
          "age", "signed_up"),
)  # fmt: skip
IDS = [messy.source for messy in MESSY]


def _query(query: str, *params: object) -> tuple[Any, ...]:
    with psycopg.connect(os.environ["UDP_DATABASE_URL"]) as conn:
        row = conn.execute(query, params).fetchone()
    assert row is not None
    return tuple(row)


def _fingerprint(table: str) -> tuple[int, str | None]:
    """The table's row count and one hash over every column of every row, in any stored order."""
    digest = "md5(string_agg(t::text, '|' ORDER BY t::text))"
    count, digest = _query(f"SELECT count(*), {digest} FROM {table} t")
    return int(count), digest


def _invoke(arguments: list[str], sources: Path | str = "sources") -> Any:
    return CliRunner().invoke(app, arguments, env={"UDP_SOURCES_DIR": str(sources)})


def _ingest(source: str, sources: Path | str = "sources") -> None:
    result = _invoke(["load", source], sources)
    assert result.exit_code == 0, result.output[-3000:]


def _run_pipeline(name: str, sources: Path | str = "sources") -> tuple[int, PipelineRun]:
    """Run a pipeline as a person would, and read back the record of that run."""
    ran = _invoke(["pipeline", "run", name], sources)
    found = re.search(r"^Run ([0-9a-f-]{36}) ", ran.output, re.MULTILINE)
    assert found is not None, ran.output[-3000:]
    shown = _invoke(["pipeline", "status", found[1], "--json"], sources)
    assert shown.exit_code == 0, shown.output[-3000:]
    return ran.exit_code, PipelineRun.model_validate_json(shown.output)


def _prepared(messy: Messy) -> PipelineRun:
    _ingest(messy.source)
    code, run = _run_pipeline(messy.pipeline)
    assert (code, run.status, run.error) == (0, "succeeded", None)
    return run


def _written_steps(pipeline: str) -> list[dict[str, Any]]:
    text = Path("pipelines", f"{pipeline}.yaml").read_text(encoding="utf-8")
    return list(yaml.safe_load(text)["steps"])


def _holds(written: Any, recorded: Any) -> bool:
    """Whether the recorded settings hold every setting written, defaults added beside them."""
    if isinstance(written, dict):
        return isinstance(recorded, dict) and all(
            key in recorded and _holds(value, recorded[key]) for key, value in written.items()
        )
    if isinstance(written, list):
        return (
            isinstance(recorded, list)
            and len(written) == len(recorded)
            and all(_holds(w, r) for w, r in zip(written, recorded, strict=True))
        )
    return bool(written == recorded)


@pytest.mark.db
@pytest.mark.services
@pytest.mark.parametrize("messy", MESSY, ids=IDS)
def test_each_messy_source_runs_from_ingest_to_clean(messy: Messy) -> None:
    table = f"{messy.source}__{messy.dataset}"

    run = _prepared(messy)

    (read,) = _query(f"SELECT count(*) FROM raw.{table} WHERE _run_id = %s", run.input_run_id)
    assert read == run.rows_in == messy.rows_in
    assert run.rows_out == messy.rows_out
    assert _fingerprint(f"clean.{table}")[0] == messy.rows_out
    input_profile, clean_profile = run.profiles
    assert (input_profile.stage, input_profile.table_rows) == ("raw", messy.rows_in)
    assert (clean_profile.stage, clean_profile.table_rows) == ("clean", messy.rows_out)
    assert clean_profile.missing_values < input_profile.missing_values
    assert all(check.passed for check in run.validation if check.critical)
    assert [(c.constraint, c.failing_rows) for c in run.validation if not c.passed] == [
        ("unique", 2),
        ("min", 1),
    ]
    types = {
        column: _query(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_schema = 'clean' AND table_name = %s AND column_name = %s",
            table,
            column,
        )[0]
        for column in (messy.whole_number, messy.when)
    }
    assert types == {messy.whole_number: "bigint", messy.when: "date"}


@pytest.mark.db
@pytest.mark.services
def test_the_empty_csv_file_loads_no_rows_into_raw() -> None:
    _ingest("messy_csv")

    assert _fingerprint("raw.messy_csv__returns") == (0, None)


@pytest.mark.db
@pytest.mark.services
@pytest.mark.parametrize("messy", MESSY, ids=IDS)
def test_running_a_pipeline_twice_gives_the_same_clean_table_and_step_counts(
    messy: Messy,
) -> None:
    clean = f"clean.{messy.source}__{messy.dataset}"

    first = _prepared(messy)
    clean_first = _fingerprint(clean)
    _, second = _run_pipeline(messy.pipeline)

    assert second.status == "succeeded"
    assert second.execution_id != first.execution_id
    assert (first.version, first.input_run_id) == (second.version, second.input_run_id)
    assert _fingerprint(clean) == clean_first

    def counts(run: PipelineRun) -> list[tuple[Any, ...]]:
        return [
            (s.position, s.type, s.status, s.rows_in, s.rows_out, s.values_changed)
            for s in run.steps
        ]

    def checks(run: PipelineRun) -> list[tuple[Any, ...]]:
        return [(c.position, c.passed, c.failing_rows, c.failing_values) for c in run.validation]

    assert counts(second) == counts(first)
    assert checks(second) == checks(first)


@pytest.mark.db
@pytest.mark.services
@pytest.mark.parametrize("messy", MESSY, ids=IDS)
def test_a_finished_runs_lineage_names_the_source_raw_each_step_and_clean(messy: Messy) -> None:
    table = f"{messy.source}__{messy.dataset}"
    written = _written_steps(messy.pipeline)

    run = _prepared(messy)

    source, raw, *steps, clean = run.lineage
    assert (source.kind, source.name, source.ingest_run_id) == (
        "source",
        messy.read_from,
        run.input_run_id,
    )
    assert (raw.kind, raw.name, raw.ingest_run_id) == ("raw", f"raw.{table}", run.input_run_id)
    assert [(node.kind, node.step_position, node.name) for node in steps] == [
        ("step", position, step["type"]) for position, step in enumerate(written, 1)
    ]
    for node, step in zip(steps, written, strict=True):
        settings = {key: value for key, value in step.items() if key != "type"}
        assert node.execution_id == run.execution_id
        assert _holds(settings, node.configuration), (settings, node.configuration)
    assert (clean.kind, clean.name, clean.execution_id) == (
        "clean",
        f"clean.{table}",
        run.execution_id,
    )


@pytest.mark.db
@pytest.mark.services
def test_a_failure_mid_pipeline_leaves_raw_and_clean_and_names_the_step(tmp_path: Path) -> None:
    # The same pipeline with its validate step left out: converting the quantities then meets
    # "three", and fails at step 2 of 6.
    sources = tmp_path / "sources"
    shutil.copytree(Path("sources", "messy_csv"), sources / "messy_csv")
    pipelines = tmp_path / "pipelines"
    pipelines.mkdir()
    definition = yaml.safe_load(Path("pipelines", "messy_csv_orders.yaml").read_text("utf-8"))
    definition["steps"] = [step for step in definition["steps"] if step["type"] != "validate"]
    (pipelines / "messy_csv_unvalidated.yaml").write_text(yaml.safe_dump(definition), "utf-8")
    _prepared(MESSY[0])
    raw_before = _fingerprint("raw.messy_csv__orders")
    clean_before = _fingerprint("clean.messy_csv__orders")

    code, run = _run_pipeline("messy_csv_unvalidated", sources)

    assert (code, run.status, run.failed_step, run.rows_out) == (1, "failed", 2, None)
    assert run.error is not None
    assert run.error.startswith("step 2 (convert_type) with {")
    assert "'three'" in run.error
    assert [step.status for step in run.steps] == ["succeeded", "failed"] + ["not_run"] * 4
    assert _fingerprint("raw.messy_csv__orders") == raw_before
    assert _fingerprint("clean.messy_csv__orders") == clean_before
    assert _query("SELECT to_regclass('staging.messy_csv__orders') IS NULL") == (True,)


@pytest.mark.db
@pytest.mark.scale
@pytest.mark.skipif(
    "UDP_PERF_ROWS" not in os.environ, reason="a measurement, not a gate: set UDP_PERF_ROWS"
)
def test_pipeline_performance_on_the_messy_csv_scaled_up(tmp_path: Path) -> None:
    """The figure in docs/performance.md. The messy CSV's rows repeated, every problem in them
    included, with each copy's order ids moved on so copies are not duplicates of each other."""
    rows = int(os.environ["UDP_PERF_ROWS"])
    sources = tmp_path / "sources"
    shutil.copytree(Path("sources", "messy_csv"), sources / "messy_csv_scaled")
    header, *lines = Path("sources/messy_csv/data/orders.csv").read_text("utf-8").splitlines()
    with (sources / "messy_csv_scaled" / "data" / "orders.csv").open("w", encoding="utf-8") as out:
        out.write(header + "\n")
        for copy in range(-(-rows // len(lines))):
            for line in lines[: rows - copy * len(lines)]:
                order_id, rest = line.split(",", 1)
                out.write(f"{int(order_id) + copy * 1000},{rest}\n")
    pipelines = tmp_path / "pipelines"
    pipelines.mkdir()
    definition = yaml.safe_load(Path("pipelines", "messy_csv_orders.yaml").read_text("utf-8"))
    definition["source"] = "messy_csv_scaled"
    (pipelines / "messy_csv_scaled_orders.yaml").write_text(yaml.safe_dump(definition), "utf-8")

    started = time.perf_counter()
    _ingest("messy_csv_scaled", sources)
    loaded = time.perf_counter()
    code, run = _run_pipeline("messy_csv_scaled_orders", sources)
    prepared = time.perf_counter()

    assert (code, run.status, run.rows_in) == (0, "succeeded", rows)
    print(
        f"\nperformance: {rows} rows; udp load {loaded - started:.1f}s; "
        f"udp pipeline run {prepared - loaded:.1f}s ({run.rows_out} rows out); "
        + "; ".join(f"step {s.position} {s.type} {s.duration_seconds:.2f}s" for s in run.steps)
    )
