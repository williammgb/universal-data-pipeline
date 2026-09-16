import json
from collections.abc import Iterator
from pathlib import Path

import polars as pl
import pytest
import yaml
from fakes import MemoryLoader

from udp.config.source import load_source
from udp.connectors import CONNECTORS
from udp.connectors.base import ExtractRequest
from udp.connectors.csv import CsvConnection, CsvConnector, CsvDataset
from udp.connectors.rest_api import RestApiConnector
from udp.errors import ConfigError, ExtractError
from udp.log import configure_logging
from udp.names import RESERVED_COLUMNS
from udp.pipeline.runner import run_source

SOURCES = Path("sources")
DEMO_TABLE = "demo_csv__customers"
DEMO_COLUMNS = [
    "customer_id",
    "first_name",
    "last_name",
    "city",
    "signup_date",
    "lifetime_value",
    "is_active",
]


def _run_demo(loader: MemoryLoader, chunk_size: int = 7) -> list[str]:
    config = load_source(SOURCES, "demo_csv")
    outcomes = run_source("demo_csv", config, SOURCES, loader, chunk_size=chunk_size)
    return [outcome.status for outcome in outcomes]


def _write_source(sources_dir: Path, name: str, datasets: str) -> None:
    folder = sources_dir / name
    folder.mkdir(parents=True)
    (folder / "source.yaml").write_text(
        f"connection:\n  type: csv\ndatasets:\n{datasets}", encoding="utf-8"
    )


def test_demo_source_loads_every_row_with_platform_columns() -> None:
    loader = MemoryLoader()

    assert _run_demo(loader) == ["succeeded"]

    table = loader.tables[DEMO_TABLE]
    assert table.height == 20
    assert table.columns == [*DEMO_COLUMNS, *RESERVED_COLUMNS]
    assert table.select(RESERVED_COLUMNS).null_count().sum_horizontal().item() == 0
    assert table.filter(pl.col("customer_id") == 4)["city"].item() == "The Hague, Centrum"


def test_successful_run_is_recorded_with_counts_and_times() -> None:
    loader = MemoryLoader()
    _run_demo(loader)

    (run,) = loader.runs.values()
    assert run["status"] == "succeeded"
    assert run["trigger"] == "manual"
    assert (run["source"], run["dataset"]) == ("demo_csv", "customers")
    assert run["rows_extracted"] == run["rows_loaded"] == 20
    assert run["started_at"] <= run["ended_at"]
    assert loader.tables[DEMO_TABLE]["_run_id"].unique().to_list() == [str(run["run_id"])]


def test_second_run_replaces_rows_instead_of_adding_them() -> None:
    loader = MemoryLoader()
    _run_demo(loader)
    first_hashes = set(loader.tables[DEMO_TABLE]["_record_hash"])

    assert _run_demo(loader, chunk_size=3) == ["succeeded"]

    assert loader.tables[DEMO_TABLE].height == 20
    assert set(loader.tables[DEMO_TABLE]["_record_hash"]) == first_hashes
    assert len(loader.runs) == 2


def test_missing_file_fails_the_run_and_the_next_dataset_still_runs(tmp_path: Path) -> None:
    sources_dir = tmp_path / "sources"
    _write_source(
        sources_dir,
        "shop",
        "  - name: orders\n    path: data/missing.csv\n  - name: items\n    path: items.csv\n",
    )
    (sources_dir / "shop" / "items.csv").write_text("sku,qty\nA,1\n", encoding="utf-8")
    loader = MemoryLoader()

    outcomes = run_source("shop", load_source(sources_dir, "shop"), sources_dir, loader)

    assert [(o.dataset, o.status) for o in outcomes] == [
        ("orders", "failed"),
        ("items", "succeeded"),
    ]
    failed = loader.runs[outcomes[0].run_id]
    assert failed["status"] == "failed"
    assert failed["error_class"] == "ExtractError"
    assert "data/missing.csv" in failed["error_message"]
    assert "Traceback" in failed["error_traceback"]
    assert failed["ended_at"] is not None
    assert "shop__orders" not in loader.tables
    assert loader.tables["shop__items"].height == 1


def test_invalid_second_dataset_stops_everything_before_any_run(tmp_path: Path) -> None:
    sources_dir = tmp_path / "sources"
    _write_source(
        sources_dir,
        "shop",
        "  - name: items\n    path: items.csv\n  - name: orders\n    pth: orders.csv\n",
    )
    (sources_dir / "shop" / "items.csv").write_text("sku,qty\nA,1\n", encoding="utf-8")
    loader = MemoryLoader()

    with pytest.raises(ConfigError, match=r"datasets\[1\]"):
        run_source("shop", load_source(sources_dir, "shop"), sources_dir, loader)

    assert loader.runs == {}
    assert loader.tables == {}


class BrokenAfterFirstChunk:
    connection_model = CsvConnection
    dataset_model = CsvDataset

    def file_version(self, request: ExtractRequest[CsvConnection, CsvDataset]) -> None:
        return None

    def extract(self, request: ExtractRequest[CsvConnection, CsvDataset]) -> Iterator[pl.DataFrame]:
        yield next(CsvConnector().extract(request))
        raise ExtractError("connection to the source was lost")


def test_failure_mid_load_keeps_the_previous_table(monkeypatch: pytest.MonkeyPatch) -> None:
    loader = MemoryLoader()
    _run_demo(loader)
    before = loader.tables[DEMO_TABLE]
    monkeypatch.setitem(CONNECTORS, "csv", BrokenAfterFirstChunk())

    assert _run_demo(loader, chunk_size=5) == ["failed"]

    assert loader.tables[DEMO_TABLE] is before
    statuses = sorted(run["status"] for run in loader.runs.values())
    assert statuses == ["failed", "succeeded"]


def test_every_log_line_is_json_with_run_context(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging()
    _run_demo(MemoryLoader())

    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]

    run_lines = [line for line in lines if "run_id" in line]
    assert run_lines
    for line in run_lines:
        assert {"run_id", "source", "dataset", "step", "timestamp", "level"} <= line.keys()
    assert {"extract", "validate", "transform", "load", "run"} <= {
        line["step"] for line in run_lines
    }
    finished = [line for line in run_lines if line["event"] == "run finished"]
    assert [(line["status"], line["rows_loaded"]) for line in finished] == [("succeeded", 20)]


def test_a_run_copies_its_source_settings_as_written() -> None:
    loader = MemoryLoader()

    assert _run_demo(loader) == ["succeeded"]

    (run_id,) = loader.runs
    source = loader.sources["demo_csv"]
    assert (source["connector_type"], source["connection"], source["run_id"]) == (
        "csv",
        {"type": "csv"},
        run_id,
    )
    copied = loader.datasets[("demo_csv", "customers")]
    written = yaml.safe_load((SOURCES / "demo_csv" / "source.yaml").read_text(encoding="utf-8"))
    assert copied["definition"] == written["datasets"][0]
    assert (copied["table_name"], copied["schedule"]) == (DEMO_TABLE, "* * * * *")


def test_a_run_of_a_source_with_secrets_copies_only_their_references(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The copy is made before extraction, so the unreachable API only needs to fail fast.
    monkeypatch.setitem(CONNECTORS, "rest_api", RestApiConnector(waits=()))
    loader = MemoryLoader()
    env = {"DEMO_API_URL": "http://api.invalid", "DEMO_API_TOKEN": "tok-3f9a1c77e2"}
    config = load_source(SOURCES, "demo_api", env)
    narrowed = config.model_copy(update={"datasets": [config.datasets[0]]})

    run_source("demo_api", narrowed, SOURCES, loader)

    copied = json.dumps([loader.sources, list(loader.datasets.values())], default=str)
    assert "tok-3f9a1c77e2" not in copied
    assert "api.invalid" not in copied
    assert loader.sources["demo_api"]["connection"]["auth"]["token"] == "${DEMO_API_TOKEN}"


def test_a_skipped_run_copies_nothing() -> None:
    loader = MemoryLoader()
    loader.lock_dataset("demo_csv", "customers")

    assert _run_demo(loader) == ["skipped"]

    assert (loader.sources, loader.datasets) == ({}, {})


def test_a_run_of_one_dataset_leaves_the_other_datasets_copy(tmp_path: Path) -> None:
    sources = tmp_path / "sources"
    _write_source(sources, "shop", "  - name: a\n    path: a.csv\n  - name: b\n    path: b.csv\n")
    for name in ("a", "b"):
        (sources / "shop" / f"{name}.csv").write_text("id\n1\n", encoding="utf-8")
    loader = MemoryLoader()
    config = load_source(sources, "shop")
    run_source("shop", config, sources, loader)
    first_b = loader.datasets[("shop", "b")]

    narrowed = config.model_copy(update={"datasets": [config.datasets[0]]})
    (outcome,) = run_source("shop", narrowed, sources, loader)

    assert loader.datasets[("shop", "a")]["run_id"] == outcome.run_id
    assert loader.datasets[("shop", "b")] == first_b
