import json
import math
from collections import Counter
from collections.abc import Iterator, Sequence
from contextlib import contextmanager, nullcontext
from datetime import UTC, date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from threading import Event
from time import monotonic, sleep
from typing import Any, cast
from urllib.parse import unquote
from uuid import UUID, uuid4, uuid7

import polars as pl
import psycopg
import pydantic
import pytest
from fakes import MemoryCatalog, MemoryLoader
from fastapi.testclient import TestClient
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from prometheus_client.parser import text_string_to_metric_families
from psycopg import sql

from udp.api.app import CHECKS_LIMIT, RUNS_QUEUED, create_app
from udp.api.catalog import PostgresCatalog, json_value
from udp.config.pipeline import PipelineFile, draft_problems, load_pipeline, resolve_pipeline
from udp.config.source import load_source
from udp.errors import ConfigError
from udp.pipeline.execution import start_pipeline
from udp.pipeline.runner import run_source
from udp.settings import Settings
from udp.storage.loader import ConfigCopy, DatasetState, RunFailure, RunStart
from udp.storage.postgres import PostgresLoader
from udp.transformations import TRANSFORMATIONS

UNREACHABLE = "postgresql://x:x@127.0.0.1:1/x"
SOURCES = Path("sources")

# --- turning stored values into JSON -----------------------------------------------------------

STORED_VALUES = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(-(2**63), 2**63 - 1),
    st.sampled_from([-(2**63), 2**63 - 1]),
    st.floats(),
    st.sampled_from([math.nan, math.inf, -math.inf, -0.0]),
    st.decimals(allow_nan=True, allow_infinity=False),
    st.sampled_from([Decimal("1E+3"), Decimal("0.000001"), Decimal("12345678901234567890.12")]),
    st.datetimes(),
    st.datetimes(timezones=st.sampled_from([UTC, timezone(timedelta(hours=-5, minutes=-30))])),
    st.dates(),
    st.times(),
    st.uuids(),
    st.text(),
)


@given(STORED_VALUES)
def test_every_stored_value_becomes_strict_json_by_one_rule(value: Any) -> None:
    converted = json_value(value)

    json.dumps(converted, allow_nan=False)
    if isinstance(value, bool) or value is None or isinstance(value, int | str):
        assert converted is value
    elif isinstance(value, float):
        if math.isnan(value):
            assert converted == "NaN"
        elif math.isinf(value):
            assert converted == ("Infinity" if value > 0 else "-Infinity")
        else:
            assert converted is value
    elif isinstance(value, Decimal):
        assert converted == str(value)
        if value.is_finite():
            assert Decimal(converted) == value
    elif isinstance(value, datetime | date | time):
        assert converted == value.isoformat()
    else:
        assert isinstance(value, UUID)
        assert converted == str(value)


# --- routes that need no database ---------------------------------------------------------------


OPENED: list[PostgresCatalog] = []
SHARED: dict[str, tuple[TestClient, Any]] = {}


@pytest.fixture(autouse=True)
def _give_back_every_connection() -> Iterator[None]:
    """A test that makes a catalog leaves its pool behind; Postgres only has so many clients."""
    yield
    SHARED.clear()
    while OPENED:
        OPENED.pop().close()


def _catalog(url: str) -> PostgresCatalog:
    catalog = PostgresCatalog(url)
    OPENED.append(catalog)
    return catalog


def _client(sources: Path = SOURCES, loader: MemoryLoader | None = None) -> tuple[TestClient, Any]:
    app = create_app(
        _catalog(UNREACHABLE), sources, {}, lambda: nullcontext(loader or MemoryLoader())
    )
    return TestClient(app), app


def test_the_openapi_document_lists_every_route_and_the_docs_page_loads() -> None:
    client, _ = _client()

    paths = client.get("/api/openapi.json").json()["paths"]

    assert sorted(paths) == [
        "/api/datasets",
        "/api/datasets/{source}/{dataset}",
        "/api/datasets/{source}/{dataset}/config",
        "/api/datasets/{source}/{dataset}/pipelines/{name}",
        "/api/datasets/{source}/{dataset}/pipelines/{name}/runs",
        "/api/datasets/{source}/{dataset}/profile",
        "/api/datasets/{source}/{dataset}/quality",
        "/api/datasets/{source}/{dataset}/rows",
        "/api/health",
        "/api/pipeline-runs/{execution_id}",
        "/api/pipelines/{name}/runs",
        "/api/runs",
        "/api/runs/{run_id}",
        "/api/sources",
        "/api/sources/{source}",
    ]
    assert set(paths["/api/runs"]) == {"get", "post"}
    assert set(paths["/api/pipelines/{name}/runs"]) == {"post"}
    assert set(paths["/api/datasets/{source}/{dataset}/pipelines/{name}"]) == {"get", "put"}
    assert set(paths["/api/datasets/{source}/{dataset}/pipelines/{name}/runs"]) == {"post"}
    assert set(paths["/api/datasets/{source}/{dataset}/config"]) == {"get", "put"}
    assert client.get("/api/docs").status_code == 200


def test_health_is_unavailable_when_the_database_cannot_be_reached(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def refuse(self: PostgresCatalog) -> None:
        raise OSError("connection refused")

    monkeypatch.setattr(PostgresCatalog, "ping", refuse)
    client, _ = _client()

    response = client.get("/api/health")

    assert (response.status_code, response.json()) == (503, {"status": "unavailable"})


def test_the_run_queue_is_capped_and_frees_up_again() -> None:
    release = Event()

    @contextmanager
    def open_loader() -> Iterator[MemoryLoader]:
        assert release.wait(10), "the blocked runs were never released"
        yield MemoryLoader()

    app = create_app(_catalog(UNREACHABLE), SOURCES, {}, open_loader)
    client = TestClient(app)

    accepted = [
        client.post("/api/runs", json={"source": "demo_csv"}).status_code
        for _ in range(RUNS_QUEUED)
    ]
    refused = client.post("/api/runs", json={"source": "demo_csv"})
    release.set()

    assert accepted == [202] * RUNS_QUEUED
    assert refused.status_code == 429
    assert "already waiting" in refused.json()["detail"]
    deadline = monotonic() + 10
    while app.state.queued and monotonic() < deadline:
        sleep(0.05)
    assert app.state.queued == 0
    assert client.post("/api/runs", json={"source": "demo_csv"}).status_code == 202
    app.state.runs.shutdown(wait=True)


@pytest.mark.parametrize(
    "path",
    [
        "/api/sources",
        "/api/sources/demo_csv",
        "/api/datasets",
        "/api/datasets/demo_csv/customers",
        "/api/datasets/demo_csv/customers/rows",
        "/api/datasets/demo_csv/customers/quality",
        "/api/datasets/demo_csv/customers/profile",
        "/api/runs",
        "/api/runs/01a0a3de-9cb0-73ec-be37-1caa01588b64",
    ],
)
def test_every_read_says_the_database_is_unavailable_without_leaking_the_connection(
    monkeypatch: pytest.MonkeyPatch, path: str
) -> None:
    def refuse(*args: object, **kwargs: object) -> None:
        raise psycopg.OperationalError(
            f'connection failed: password authentication failed for user "udp" ({UNREACHABLE})'
        )

    reads = (
        "sources",
        "source",
        "datasets",
        "dataset",
        "rows",
        "quality",
        "profile",
        "runs",
        "run",
    )
    for read in reads:
        monkeypatch.setattr(PostgresCatalog, read, refuse)
    client, _ = _client()

    response = client.get(path)

    assert response.status_code == 503, path
    assert response.json() == {"detail": "the platform database is unavailable"}
    assert "udp" not in response.text and "127.0.0.1" not in response.text


def test_metrics_are_unavailable_when_the_database_cannot_be_reached(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def refuse(self: PostgresCatalog) -> None:
        raise OSError("connection refused")

    monkeypatch.setattr(PostgresCatalog, "metrics", refuse)
    client, _ = _client()

    response = client.get("/api/metrics")

    assert response.status_code == 503
    assert response.text.startswith("# ")
    assert list(text_string_to_metric_families(response.text)) == []
    assert "/api/metrics" not in client.get("/api/openapi.json").json()["paths"]


@pytest.mark.parametrize(
    "path",
    [
        "/api/runs?limit=0",
        "/api/runs?limit=501",
        "/api/runs?offset=-1",
        "/api/runs?since=2026-01-01T00:00:00",
        "/api/runs?until=2026-01-01",
        "/api/runs?status=done",
        "/api/runs/not-a-run-id",
        "/api/datasets/demo_csv/customers/rows?limit=0",
    ],
)
def test_a_malformed_query_is_refused_before_the_database_is_asked(path: str) -> None:
    client, _ = _client()

    assert client.get(path).status_code == 422


@pytest.mark.parametrize(
    ("body", "status", "detail"),
    [
        ({"source": "nowhere"}, 404, "source 'nowhere' not found"),
        ({"source": "../sources"}, 404, "not found"),
        ({"source": "shop"}, 422, "sources/shop/source.yaml: datasets[0].path"),
        ({"source": "demo_csv", "dataset": "orders"}, 404, "dataset 'orders'"),
        ({"source": "demo_csv", "rebuild": True}, 422, "rebuild"),
    ],
)
def test_a_run_request_that_cannot_run_says_why(
    tmp_path: Path, body: dict[str, Any], status: int, detail: str
) -> None:
    sources = tmp_path / "sources"
    (sources / "shop").mkdir(parents=True)
    (sources / "shop" / "source.yaml").write_text(
        "connection:\n  type: csv\ndatasets:\n  - name: items\n", encoding="utf-8"
    )
    (sources / "demo_csv").mkdir()
    (sources / "demo_csv" / "source.yaml").write_bytes(
        (SOURCES / "demo_csv" / "source.yaml").read_bytes()
    )
    client, _ = _client(sources)

    response = client.post("/api/runs", json=body)

    assert response.status_code == status
    assert detail in json.dumps(response.json())


def test_a_run_request_starts_the_run_in_the_background_as_a_manual_run() -> None:
    loader = MemoryLoader()
    client, app = _client(loader=loader)

    response = client.post("/api/runs", json={"source": "demo_csv", "dataset": "customers"})
    app.state.runs.shutdown(wait=True)

    assert response.status_code == 202
    accepted = response.json()
    assert (accepted["source"], accepted["datasets"]) == ("demo_csv", ["customers"])
    (record,) = loader.runs.values()
    assert (record["status"], record["trigger"]) == ("succeeded", "manual")
    assert record["started_at"] >= datetime.fromisoformat(accepted["requested_at"])
    assert ("demo_csv", "customers") in loader.datasets


# --- pipeline runs -------------------------------------------------------------------------------


def _loaded_demo() -> MemoryLoader:
    """A store whose RAW holds demo_csv, as `udp run demo_csv` leaves it."""
    loader = MemoryLoader()
    run_source("demo_csv", load_source(SOURCES, "demo_csv", {}), SOURCES, loader)
    return loader


def test_a_pipeline_run_started_over_the_api_can_be_read_back_by_its_id() -> None:
    loader = _loaded_demo()
    client, app = _client(loader=loader)

    response = client.post("/api/pipelines/demo_csv_customers/runs")
    app.state.runs.shutdown(wait=True)

    assert response.status_code == 202
    accepted = response.json()
    assert (accepted["pipeline"], accepted["version"]) == ("demo_csv_customers", 1)
    assert (accepted["source"], accepted["dataset"]) == ("demo_csv", "customers")
    record = client.get(f"/api/pipeline-runs/{accepted['execution_id']}")
    assert record.status_code == 200
    run = record.json()
    assert (run["status"], run["version"], run["rows_in"], run["rows_out"]) == (
        "succeeded",
        1,
        20,
        20,
    )
    assert [step["type"] for step in run["steps"]] == ["normalize_values", "fill_missing"]
    assert run["steps"][1]["values_changed"] == 1
    assert [profile["stage"] for profile in run["profiles"]] == ["raw", "clean"]
    assert all(check["passed"] for check in run["validation"])
    assert [node["kind"] for node in run["lineage"]] == ["source", "raw", "step", "step", "clean"]
    assert run["lineage"][0]["ingest_run_id"] == run["input_run_id"] is not None
    assert ".".join(["clean", "demo_csv__customers"]) in loader.clean


def test_a_pipeline_run_of_a_dataset_already_being_prepared_is_refused_naming_the_run() -> None:
    loader = _loaded_demo()
    pipeline = load_pipeline(SOURCES, "demo_csv_customers", {})
    going = start_pipeline(loader, pipeline)
    client, _ = _client(loader=loader)

    refused = client.post("/api/pipelines/demo_csv_customers/runs")

    assert refused.status_code == 409
    assert str(going.execution_id) in refused.json()["detail"]
    assert client.get(f"/api/pipeline-runs/{going.execution_id}").json()["status"] == "running"


@pytest.mark.parametrize(
    ("path", "status", "detail"),
    [
        ("/api/pipelines/nope/runs", 404, "pipeline 'nope' not found"),
        ("/api/pipelines/Not-A-Name/runs", 404, "pipeline 'Not-A-Name' not found"),
    ],
)
def test_a_pipeline_run_that_cannot_start_says_why(path: str, status: int, detail: str) -> None:
    client, _ = _client()

    response = client.post(path)

    assert (response.status_code, response.json()["detail"]) == (status, detail)


def test_an_invalid_pipeline_file_is_refused_naming_the_step(tmp_path: Path) -> None:
    sources = tmp_path / "sources"
    (sources / "demo_csv").mkdir(parents=True)
    (sources / "demo_csv" / "source.yaml").write_text(
        (SOURCES / "demo_csv" / "source.yaml").read_text(encoding="utf-8"), encoding="utf-8"
    )
    (tmp_path / "pipelines").mkdir()
    (tmp_path / "pipelines" / "bad.yaml").write_text(
        "source: demo_csv\ndataset: customers\nsteps:\n  - type: fill_missing\n    columns: [a]\n",
        encoding="utf-8",
    )
    client, _ = _client(sources=sources)

    response = client.post("/api/pipelines/bad/runs")

    assert response.status_code == 422
    assert "step 1 (fill_missing): method: Field required" in response.json()["detail"]


def test_an_unknown_pipeline_run_is_not_found() -> None:
    client, _ = _client()

    response = client.get("/api/pipeline-runs/01a0a3de-9cb0-73ec-be37-1caa01588b64")

    assert response.status_code == 404
    assert response.json()["detail"] == (
        "pipeline run 01a0a3de-9cb0-73ec-be37-1caa01588b64 not found"
    )


# --- pipelines built in the dashboard ------------------------------------------------------------

BUILT = "/api/datasets/demo_csv/customers/pipelines/customers_built"
CITY_STEP = {"type": "normalize_values", "columns": ["city"], "trim": True}
NOT_NULL = {"constraint": "not_null", "column": "customer_id", "critical": True}


def _builder_client(loader: MemoryLoader | None = None) -> tuple[TestClient, Any, MemoryLoader]:
    store = loader or MemoryLoader()
    columns = [("customer_id", "bigint"), ("city", "text"), ("_run_id", "uuid")]
    catalog = MemoryCatalog(None, columns)
    app = create_app(cast(PostgresCatalog, catalog), SOURCES, {}, lambda: nullcontext(store))
    return TestClient(app), app, store


def test_a_pipeline_never_saved_is_new_with_the_datasets_columns_and_no_steps() -> None:
    client, _, _ = _builder_client()

    response = client.get(BUILT)

    assert response.status_code == 200
    assert response.json() == {
        "name": "customers_built",
        "source": "demo_csv",
        "dataset": "customers",
        "version": None,
        "saved_at": None,
        "profile": "ends",
        "constraints": [],
        "steps": [],
        # The platform's own columns cannot be a step's column, so they are not offered.
        "columns": [{"name": "customer_id", "type": "bigint"}, {"name": "city", "type": "text"}],
    }


def test_saving_runs_nothing_and_the_saved_version_is_what_runs() -> None:
    loader = _loaded_demo()
    client, app, _ = _builder_client(loader)
    draft = {"profile": "ends", "constraints": [NOT_NULL], "steps": [CITY_STEP]}

    saved = client.put(BUILT, json=draft)
    again = client.put(BUILT, json=draft)

    assert saved.status_code == 200, saved.text
    assert (saved.json()["version"], again.json()["version"]) == (1, 1)
    # Every setting the step runs with comes back, defaults included.
    assert saved.json()["steps"] == [{**CITY_STEP, "case": None, "mapping": {}}]
    assert client.get(BUILT).json()["steps"] == saved.json()["steps"]
    with loader.stages() as stages:
        assert stages.running_executions("demo_csv", "customers") == []
    assert "clean.demo_csv__customers" not in loader.clean

    accepted = client.post(f"{BUILT}/runs")
    app.state.runs.shutdown(wait=True)

    assert accepted.status_code == 202, accepted.text
    assert (accepted.json()["pipeline"], accepted.json()["version"]) == ("customers_built", 1)
    run = client.get(f"/api/pipeline-runs/{accepted.json()['execution_id']}").json()
    assert (run["status"], run["version"], run["rows_out"]) == ("succeeded", 1, 20)
    assert [step["type"] for step in run["steps"]] == ["normalize_values"]
    assert [check["constraint"] for check in run["validation"]] == ["not_null"]


def test_an_invalid_draft_is_refused_naming_each_step_and_field_and_nothing_is_saved() -> None:
    client, _, _ = _builder_client()
    draft = {
        "constraints": [NOT_NULL, {"constraint": "min", "column": "lifetime_value"}],
        "steps": [
            CITY_STEP,
            {"type": "outliers", "column": "city", "action": "flag", "upper_percentile": 150},
            {"type": "nope"},
        ],
    }

    response = client.put(BUILT, json=draft)

    assert response.status_code == 422
    problems = response.json()["detail"]
    assert [problem["loc"] for problem in problems] == [
        ["constraints", 2, "value"],
        ["steps", 2, "upper_percentile"],
        ["steps", 3, "type"],
    ]
    assert "less than or equal to 100" in problems[1]["msg"]
    assert client.get(BUILT).json()["version"] is None


DRAFT_KEYS = st.sampled_from(
    [
        *("columns", "column", "method", "value", "action", "upper_percentile", "to", "trim"),
        *("case", "mapping", "on_invalid", "script", "timeout", "bogus"),
    ]
)
DRAFT_VALUES = st.sampled_from(
    [
        *(["city"], [], "city", "median", "value", "flag", 50, 150, -1, True, "lower", {}),
        *(None, "keep", "text", "decimal", {"a": "b"}),
    ]
)


@settings(max_examples=300)
@given(
    steps=st.lists(
        st.dictionaries(DRAFT_KEYS, DRAFT_VALUES, max_size=4).flatmap(
            lambda settings: st.sampled_from([*sorted(TRANSFORMATIONS), "nope", 3, None]).map(
                lambda kind: {**settings, "type": kind}
            )
        ),
        max_size=3,
    ),
    constraints=st.lists(
        st.fixed_dictionaries(
            {"constraint": st.sampled_from(["not_null", "min", "unique", "pattern", "zz"])},
            optional={
                "column": st.sampled_from(["city", 3]),
                "columns": st.sampled_from([["city"], []]),
                "value": st.sampled_from([0, "x"]),
                "pattern": st.sampled_from(["[a-z]+", "("]),
            },
        ),
        max_size=2,
    ),
)
def test_a_draft_with_no_problems_is_exactly_one_the_engine_loads(
    steps: list[dict[str, Any]], constraints: list[dict[str, Any]]
) -> None:
    # The builder is told "saved" only for what a run then loads: the same rule, both ways.
    config = load_source(SOURCES, "demo_csv", {})
    written = {"source": "demo_csv", "dataset": "customers", "constraints": constraints}
    try:
        resolve_pipeline("p", PipelineFile.model_validate({**written, "steps": steps}), config, "p")
        loads = True
    except ConfigError, pydantic.ValidationError:
        loads = False

    assert (draft_problems(constraints, steps) == []) == loads


@pytest.mark.parametrize(
    ("script", "status"),
    [
        ("scripts/custom/customer_transform.py", 200),
        ("scripts/custom/not_there.py", 422),
        ("pyproject.toml", 422),
        ("scripts/custom/../../pyproject.toml", 422),
    ],
)
def test_a_python_step_saved_over_the_api_may_only_name_a_script_in_scripts_custom(
    script: str, status: int
) -> None:
    client, _, _ = _builder_client()

    response = client.put(BUILT, json={"steps": [{"type": "python", "script": script}]})

    assert response.status_code == status, response.text
    if status == 422:
        assert response.json()["detail"][0]["loc"] == ["steps", 1, "script"]


@pytest.mark.parametrize(
    ("method", "path", "status", "detail"),
    [
        (
            "PUT",
            "/api/datasets/demo_csv/customers/pipelines/demo_csv_customers",
            409,
            "pipelines/demo_csv_customers.yaml declares a pipeline called 'demo_csv_customers'",
        ),
        ("POST", f"{BUILT}/runs", 404, "pipeline 'customers_built' of demo_csv.customers is not"),
        ("GET", "/api/datasets/demo_csv/nope/pipelines/x", 404, "dataset 'nope' is not in source"),
        ("GET", "/api/datasets/nope/customers/pipelines/x", 404, "source 'nope' not found"),
    ],
)
def test_a_pipeline_the_builder_cannot_save_or_run_says_why(
    method: str, path: str, status: int, detail: str
) -> None:
    client, _, _ = _builder_client()

    response = client.request(method, path, json={"steps": []} if method == "PUT" else None)

    assert response.status_code == status
    assert response.json()["detail"].startswith(detail)


def test_a_pipeline_name_that_is_not_a_name_is_refused() -> None:
    client, _, _ = _builder_client()

    response = client.get("/api/datasets/demo_csv/customers/pipelines/Not-A-Name")

    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["name"]


# --- serving the built dashboard -----------------------------------------------------------------

SECRET = "the-secret-next-door-8f21"
INDEX = '<!doctype html><html><body><div id="root"></div></body></html>'
ASSET = "console.log('dashboard');"


def _dashboard(root: Path) -> Path:
    built = root / "dist"
    (built / "assets").mkdir(parents=True, exist_ok=True)
    (built / "index.html").write_text(INDEX, encoding="utf-8")
    (built / "assets" / "app-x.js").write_text(ASSET, encoding="utf-8")
    (root / "secret.txt").write_text(SECRET, encoding="utf-8")
    return built


SEGMENTS = st.sampled_from(
    [
        "",
        "..",
        "%2e%2e",
        "%2f",
        "\\",
        "api",
        "apix",
        "API",
        "docs",
        "assets",
        "app-x.js",
        "index.html",
        "secret.txt",
        "datasets",
        "demo_csv",
        "café",
    ]
)
PATHS = st.builds(
    lambda parts, lead, trail: ("/" if lead else "") + "/".join(parts) + ("/" if trail else ""),
    st.lists(SEGMENTS, max_size=4),
    st.booleans(),
    st.booleans(),
)


def test_a_run_started_over_the_api_uses_the_edits_made_from_the_dashboard() -> None:
    loader = MemoryLoader()
    loader.overrides["demo_csv"] = {"customers": {"quarantine_threshold_percent": 50}}
    client, app = _client(loader=loader)

    response = client.post(
        "/api/runs", json={"source": "demo_csv", "dataset": "customers", "full_refresh": True}
    )
    app.state.runs.shutdown(wait=True)

    assert response.status_code == 202
    recorded = loader.datasets[("demo_csv", "customers")]["definition"]
    assert recorded["quarantine_threshold_percent"] == 50


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(PATHS)
def test_any_address_gives_a_page_a_file_or_the_apis_own_404(tmp_path: Path, path: str) -> None:
    built = _dashboard(tmp_path)
    app = create_app(_catalog(UNREACHABLE), SOURCES, {}, lambda: nullcontext(MemoryLoader()), built)

    response = TestClient(app).get(f"/{path.lstrip('/')}")

    assert SECRET not in response.text
    # The client resolves '..' and the server decodes %2f before either is matched, so judge
    # the address the server was really asked for, with exactly one leading slash removed.
    asked = unquote(response.request.url.path)[1:]
    if asked == "api" or asked.startswith("api/"):
        assert response.status_code != 200 or asked in ("api/docs", "api/openapi.json")
        if response.status_code == 404:
            assert "detail" in response.json()
    else:
        assert response.status_code == 200
        assert response.text in (INDEX, ASSET)


def test_the_dashboards_own_files_and_addresses_are_served(tmp_path: Path) -> None:
    built = _dashboard(tmp_path)
    client = TestClient(
        create_app(_catalog(UNREACHABLE), SOURCES, {}, lambda: nullcontext(MemoryLoader()), built)
    )

    asset = client.get("/assets/app-x.js")

    assert (asset.status_code, asset.text) == (200, ASSET)
    assert "javascript" in asset.headers["content-type"]
    assert asset.headers["cache-control"] == "max-age=31536000, immutable"
    deep = client.get("/datasets/demo_csv/customers?tab=preview")
    assert (deep.status_code, deep.text) == (200, INDEX)
    assert deep.headers["cache-control"] == "no-cache"
    assert client.get("/api/docs").status_code == 200
    assert client.get("/api/nope").status_code == 404


def test_without_a_built_dashboard_only_the_api_answers(tmp_path: Path) -> None:
    client = TestClient(
        create_app(
            _catalog(UNREACHABLE),
            SOURCES,
            {},
            lambda: nullcontext(MemoryLoader()),
            tmp_path / "dist",
        )
    )

    assert client.get("/").status_code == 404
    assert client.get("/api/docs").status_code == 200


# --- editing a dataset's configuration ----------------------------------------------------------

CONFIG = "/api/datasets/demo_csv/customers/config"


def _config_client(
    state: DatasetState | None = None, columns: Sequence[tuple[str, str]] = ()
) -> tuple[TestClient, MemoryCatalog]:
    catalog = MemoryCatalog(state, columns)
    app = create_app(
        cast(PostgresCatalog, catalog), SOURCES, {}, lambda: nullcontext(MemoryLoader())
    )
    return TestClient(app), catalog


def _loaded_state(**settings: Any) -> DatasetState:
    return DatasetState(
        source="demo_csv",
        dataset="customers",
        load_mode=settings.get("load_mode", "full"),
        primary_key=settings.get("primary_key", ()),
        watermark_column=settings.get("watermark_column"),
        watermark_type=None,
        watermark=None,
        file_path="data/customers.csv",
        file_sha256="abc",
        config_sha256="def",
        run_id=uuid7(),
        saved_at=datetime.now(UTC),
    )


def test_a_dataset_with_no_edits_reads_back_exactly_its_file() -> None:
    client, _ = _config_client()

    answer = client.get(CONFIG).json()

    assert (answer["overridden"], answer["history"]) == ([], [])
    assert answer["file"] == answer["effective"]
    assert answer["file"]["columns"]["lifetime_value"] == "decimal(12,2)"
    assert answer["checks_yaml"] == answer["file_checks_yaml"]
    assert "not_null" in answer["checks_yaml"]
    assert "exclude_columns" not in answer["editable"]  # a csv source has no such setting


@pytest.mark.parametrize(
    ("path", "status"),
    [
        ("/api/datasets/nowhere/customers/config", 404),
        ("/api/datasets/demo_csv/orders/config", 404),
    ],
)
def test_a_configuration_that_is_not_there_is_a_404(path: str, status: int) -> None:
    client, _ = _config_client()

    assert client.get(path).status_code == status
    assert client.put(path, json={"values": {}}).status_code == status


def test_a_value_the_file_could_not_hold_is_refused_with_the_files_own_message() -> None:
    client, catalog = _config_client()

    response = client.put(CONFIG, json={"values": {"schedule": "0 6 * *"}})

    # The same value written into the file is refused with the very same words.
    with pytest.raises(ConfigError) as from_the_file:
        load_source(SOURCES, "demo_csv", {"DEMO_SCHEDULE": "0 6 * *"})

    assert response.status_code == 422
    assert response.json()["detail"] == str(from_the_file.value)
    assert catalog.saved == []


@pytest.mark.parametrize(
    "values",
    [
        {"schedule": "${DEMO_DB_URL} 1 1 1 1"},
        {"columns": {"customer_id": "${DEMO_DB_URL}"}},
        {"columns": {"${DEMO_DB_URL}": "text"}},
        {"primary_key": ["${DEMO_DB_URL}"]},
        {"checks": "- check: not_null\n  column: ${DEMO_DB_URL}\n"},
    ],
)
def test_an_edit_may_not_read_the_platforms_own_environment(values: dict[str, Any]) -> None:
    """A ${NAME} is filled before anything is checked, so it would come back in the refusal."""
    client, catalog = _config_client()

    response = client.put(CONFIG, json={"values": values})

    assert response.status_code == 422
    assert "may not name an environment variable" in response.json()["detail"]
    assert "postgresql" not in response.text
    assert catalog.saved == []


def test_a_field_that_is_not_editable_is_refused() -> None:
    client, catalog = _config_client()

    response = client.put(CONFIG, json={"values": {"name": "other", "path": "x.csv"}})

    assert response.status_code == 422
    assert "not editable" in response.json()["detail"]
    assert catalog.saved == []


def test_an_edit_is_stored_and_comes_back_as_what_the_dataset_now_uses() -> None:
    client, catalog = _config_client()

    saved = client.put(
        CONFIG, json={"values": {"schedule": "0 6 * * *", "quarantine_threshold_percent": 5}}
    )

    assert saved.status_code == 200
    answer = saved.json()
    assert answer["effective"]["schedule"] == "0 6 * * *"
    assert answer["file"]["schedule"] is None
    assert set(answer["overridden"]) == {"schedule", "quarantine_threshold_percent"}
    (edit,) = catalog.saved
    assert edit["override"] == {"schedule": "0 6 * * *", "quarantine_threshold_percent": 5.0}
    assert edit["changed"]["schedule"] == {"from": None, "to": "0 6 * * *"}
    assert answer["history"][0]["changed"]["schedule"]["to"] == "0 6 * * *"


def test_checks_are_edited_as_the_yaml_the_file_holds() -> None:
    client, catalog = _config_client()

    saved = client.put(
        CONFIG, json={"values": {"checks": "- check: not_null\n  column: customer_id\n"}}
    )

    assert saved.status_code == 200
    assert saved.json()["checks_yaml"].startswith("- check: not_null")
    assert catalog.saved[0]["override"]["checks"] == [
        {"check": "not_null", "column": "customer_id", "severity": "error"}
    ]
    refused = client.put(CONFIG, json={"values": {"checks": "- check: not_null\n   column: x\n"}})
    assert refused.status_code == 422
    assert "invalid YAML" in refused.json()["detail"]


@pytest.mark.parametrize(
    ("checks", "said"),
    [
        # A handful of nested aliases turns a few hundred bytes into billions of values.
        pytest.param(
            "a: &a [x,x,x,x]\nb: &b [*a,*a,*a,*a]\nc: [*b,*b,*b,*b]\n",
            "may not use an alias",
            id="aliases",
        ),
        # Nested deeply enough, PyYAML ends its own recursion rather than its parse.
        pytest.param("[" * 5000 + "]" * 5000, "too deeply nested", id="deeply nested"),
        pytest.param("x" * (CHECKS_LIMIT + 1), "more than the", id="longer than the limit"),
    ],
)
def test_checks_that_would_cost_more_to_read_than_to_send_are_refused(
    checks: str, said: str
) -> None:
    client, catalog = _config_client()

    response = client.put(CONFIG, json={"values": {"checks": checks}})

    assert response.status_code == 422
    assert said in response.json()["detail"]
    assert catalog.saved == []


def test_a_change_that_needs_the_table_rebuilt_is_refused_once_and_then_saved() -> None:
    client, catalog = _config_client(_loaded_state(), [("customer_id", "bigint")])
    change = {"values": {"load_mode": "append", "watermark": "signup_date"}}

    refused = client.put(CONFIG, json=change)
    accepted = client.put(CONFIG, json={**change, "accept_rebuild": True})

    assert refused.status_code == 409
    assert "load_mode" in refused.json()["detail"]
    assert accepted.status_code == 200
    (edit,) = catalog.saved
    assert edit["override"]["load_mode"] == "append"


def test_a_declared_type_the_table_already_contradicts_needs_a_rebuild() -> None:
    client, _ = _config_client(_loaded_state(), [("city", "text")])

    refused = client.put(CONFIG, json={"values": {"columns": {"city": "integer"}}})

    assert refused.status_code == 409
    assert "city" in refused.json()["detail"]


def test_an_edit_taken_back_leaves_the_dataset_on_its_file_again() -> None:
    client, catalog = _config_client()

    client.put(CONFIG, json={"values": {"schedule": "0 6 * * *"}})
    back = client.put(CONFIG, json={"values": {}})

    assert back.status_code == 200
    assert back.json()["overridden"] == []
    assert catalog.saved[-1]["override"] == {}
    assert catalog.saved[-1]["changed"]["schedule"] == {"from": "0 6 * * *", "to": None}


# --- the catalog against Postgres ---------------------------------------------------------------


# Each example opens several fresh connections, as the API itself does, and a few hundred
# examples use up Windows' range of outgoing ports faster than it releases them ("Address
# already in use" in the full gate). A pool is slice 10's job; until then these properties
# run a fixed number of examples instead of the ci profile's 500.
# Back to a full run: these were capped at 60 while every call opened its own connection and
# exhausted Windows' outgoing ports. The catalog now reads through a pool of 8.
db_property = settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])


def _url() -> str:
    return str(Settings().database_url)  # type: ignore[call-arg]


def _prefix() -> str:
    return f"api_{uuid4().hex[:10]}"


def _shared_db_client() -> tuple[TestClient, Any]:
    """One client for every example of a property: a pool per example runs Postgres out of
    clients long before a property has finished generating."""
    if "client" not in SHARED:
        SHARED["client"] = _db_client()
    return SHARED["client"]


def _db_client(sources: Path = SOURCES) -> tuple[TestClient, Any]:
    url = _url()
    app = create_app(_catalog(url), sources, {}, lambda: PostgresLoader(url))
    return TestClient(app), app


def _copy(
    loader: PostgresLoader,
    source: str,
    dataset: str,
    primary_key: tuple[str, ...] = (),
    table: str | None = None,
) -> None:
    run_id = uuid7()
    loader.start_run(RunStart(run_id, source, dataset, "manual", datetime.now(UTC)))
    loader.record_config(
        ConfigCopy(
            source=source,
            connector_type="csv",
            connection={"type": "csv"},
            dataset=dataset,
            table=table or f"{source}__{dataset}",
            load_mode="merge" if primary_key else "full",
            primary_key=primary_key,
            watermark="k1" if primary_key else None,
            schedule=None,
            definition={"name": dataset},
            run_id=run_id,
            recorded_at=datetime.now(UTC),
        )
    )


QUALITY_SOURCE = (
    "connection:\n  type: csv\ndatasets:\n  - name: customers\n    path: customers.csv\n"
    "    load_mode: merge\n    watermark: customer_id\n    primary_key: [customer_id]\n"
    "    columns:\n      customer_id: integer\n      amount: decimal(8,2)\n"
    "    checks:\n      - check: not_null\n        column: customer_id\n"
    "      - check: regex\n        column: city\n        pattern: '[A-Z][a-z]+'\n"
    "        severity: warn\n"
)


@pytest.mark.db
def test_a_loaded_source_is_described_by_the_api(tmp_path: Path) -> None:
    source = _prefix()
    sources = tmp_path / "sources"
    (sources / source).mkdir(parents=True)
    (sources / source / "source.yaml").write_text(QUALITY_SOURCE, encoding="utf-8")
    (sources / source / "customers.csv").write_text(
        "Customer ID,Amount,City\n1,1.50,Delft\n2,2.25,delft\n", encoding="utf-8"
    )
    with PostgresLoader(_url()) as loader:
        (outcome,) = run_source(source, load_source(sources, source, {}), sources, loader)
    assert outcome.status == "succeeded"
    client, _ = _db_client(sources)

    assert client.get("/api/health").json() == {"status": "ok"}
    (listed,) = client.get("/api/sources", params={"q": source}).json()
    for route in ("/api/sources", "/api/datasets"):
        assert client.get(route, params={"q": ""}).json() == client.get(route).json()
    assert (listed["connector_type"], listed["datasets"]) == ("csv", 1)
    detail = client.get(f"/api/datasets/{source}/customers").json()
    assert detail["definition"]["columns"] == {"customer_id": "integer", "amount": "decimal(8,2)"}
    assert (detail["load_mode"], detail["primary_key"]) == ("merge", ["customer_id"])
    assert {"name": "amount", "type": "numeric(8,2)"} in detail["columns"]
    assert [version["version"] for version in detail["versions"]] == [1]
    assert detail["state"]["watermark"] == "2"
    assert detail["last_run"]["run_id"] == str(outcome.run_id)
    assert client.get(f"/api/sources/{source}").json()["datasets"][0]["dataset"] == "customers"
    page = client.get(f"/api/datasets/{source}/customers/rows").json()
    assert [(row["customer_id"], row["amount"]) for row in page["rows"]] == [
        (1, "1.50"),
        (2, "2.25"),
    ]
    report = client.get(f"/api/datasets/{source}/customers/quality").json()
    assert report["run_id"] == str(outcome.run_id)
    assert [(r["check_type"], r["severity"], r["passed"]) for r in report["results"]] == [
        ("not_null", "error", True),
        ("regex", "warn", False),
    ]
    assert client.get(f"/api/datasets/{source}/missing").status_code == 404
    assert client.get(f"/api/runs/{uuid7()}").status_code == 404
    # The table's own row count and the source's type, on the list and on the dataset.
    (item,) = client.get("/api/datasets", params={"q": source}).json()
    assert (item["connector_type"], item["table_rows"]) == ("csv", 2)
    assert (detail["connector_type"], detail["table_rows"]) == ("csv", 2)
    profile = client.get(f"/api/datasets/{source}/customers/profile").json()
    assert (profile["table_rows"], profile["profiled_rows"], profile["sampled"]) == (2, 2, False)
    assert [column["name"] for column in profile["columns"]] == ["customer_id", "amount", "city"]
    assert client.get(f"/api/datasets/{source}/missing/profile").status_code == 404


@pytest.mark.db
def test_a_real_run_shows_up_in_the_metrics_prometheus_reads(tmp_path: Path) -> None:
    source = _prefix()
    sources = tmp_path / "sources"
    (sources / source).mkdir(parents=True)
    (sources / source / "source.yaml").write_text(QUALITY_SOURCE, encoding="utf-8")
    (sources / source / "customers.csv").write_text(
        "Customer ID,Amount,City\n1,1.50,Delft\n2,2.25,delft\n", encoding="utf-8"
    )
    with PostgresLoader(_url()) as loader:
        (outcome,) = run_source(source, load_source(sources, source, {}), sources, loader)
    client, _ = _db_client(sources)

    response = client.get("/api/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"] == "text/plain; version=0.0.4; charset=utf-8"
    ours: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
    for family in text_string_to_metric_families(response.text):
        for sample in family.samples:
            if sample.labels.get("source") == source:
                others = tuple(
                    sorted((k, v) for k, v in sample.labels.items() if k not in {"source"})
                )
                ours[(sample.name, others)] = sample.value
    dataset = ("dataset", "customers")
    assert ours[("udp_runs_total", (dataset, ("status", "succeeded")))] == 1
    assert ours[("udp_runs_total", (dataset, ("status", "failed")))] == 0
    assert ours[("udp_runs_running", (dataset,))] == 0
    assert ours[("udp_rows_loaded_total", (dataset,))] == outcome.rows_loaded == 2
    assert ours[("udp_last_run_status", (dataset, ("status", "succeeded")))] == 1
    assert ours[("udp_quality_checks_failed", (dataset, ("severity", "warn")))] == 1
    assert ours[("udp_quality_checks_failed", (dataset, ("severity", "error")))] == 0
    assert ("udp_last_success_timestamp_seconds", (dataset,)) in ours


@pytest.mark.db
def test_every_read_answers_503_when_the_database_is_really_not_there() -> None:
    # A real pool at a real server with no such database: it answers at once, unlike a closed
    # port on Windows, so this drives the pool itself rather than a stand-in for it.
    missing = _url().rsplit("/", 1)[0] + "/no_such_database"
    app = create_app(_catalog(missing), SOURCES, {}, lambda: nullcontext(MemoryLoader()))
    client = TestClient(app)

    for path in ("/api/sources", "/api/datasets", "/api/runs"):
        response = client.get(path)
        assert response.status_code == 503, path
        assert response.json() == {"detail": "the platform database is unavailable"}
        assert "no_such_database" not in response.text and "udp" not in response.text
    assert client.get("/api/health").json() == {"status": "unavailable"}
    assert client.get("/api/metrics").status_code == 503


@pytest.mark.db
def test_a_failed_run_shows_its_error(tmp_path: Path) -> None:
    source = _prefix()
    sources = tmp_path / "sources"
    (sources / source).mkdir(parents=True)
    (sources / source / "source.yaml").write_text(
        "connection:\n  type: csv\ndatasets:\n  - name: items\n    path: missing.csv\n",
        encoding="utf-8",
    )
    with PostgresLoader(_url()) as loader:
        (outcome,) = run_source(source, load_source(sources, source, {}), sources, loader)
    client, _ = _db_client(sources)

    detail = client.get(f"/api/runs/{outcome.run_id}").json()

    assert (detail["status"], detail["error_class"]) == ("failed", "ExtractError")
    assert "missing.csv" in detail["error_message"]
    assert "Traceback" in detail["error_traceback"]


@pytest.mark.db
def test_a_run_requested_over_http_is_recorded_as_a_manual_run(tmp_path: Path) -> None:
    source = _prefix()
    sources = tmp_path / "sources"
    (sources / source).mkdir(parents=True)
    (sources / source / "source.yaml").write_text(
        "connection:\n  type: csv\ndatasets:\n  - name: items\n    path: items.csv\n",
        encoding="utf-8",
    )
    (sources / source / "items.csv").write_text("id\n1\n", encoding="utf-8")
    client, app = _db_client(sources)

    accepted = client.post("/api/runs", json={"source": source}).json()
    app.state.runs.shutdown(wait=True)

    runs = client.get(
        "/api/runs", params={"source": source, "since": accepted["requested_at"]}
    ).json()["runs"]
    assert [(run["status"], run["trigger"]) for run in runs] == [("succeeded", "manual")]


@pytest.mark.db
def test_a_pipeline_run_requested_over_http_runs_against_postgres_and_reads_back(
    tmp_path: Path,
) -> None:
    # The route opens a real loader in the request, takes the dataset's lock on its connection,
    # and hands both to the background thread that carries the run out and frees the lock.
    source = _prefix()
    sources = tmp_path / "sources"
    (sources / source).mkdir(parents=True)
    (sources / source / "source.yaml").write_text(
        "connection:\n  type: csv\ndatasets:\n  - name: items\n    path: items.csv\n",
        encoding="utf-8",
    )
    (sources / source / "items.csv").write_text("id,name\n1, ada \n2,bo\n", encoding="utf-8")
    (tmp_path / "pipelines").mkdir()
    (tmp_path / "pipelines" / f"{source}_items.yaml").write_text(
        f"source: {source}\ndataset: items\n"
        "constraints:\n  - constraint: not_null\n    column: id\n    critical: true\n"
        "steps:\n  - type: normalize_values\n    columns: [name]\n    trim: true\n",
        encoding="utf-8",
    )
    with PostgresLoader(_url()) as loader:
        (ingest,) = run_source(source, load_source(sources, source, {}), sources, loader)
    assert ingest.status == "succeeded"
    client, app = _db_client(sources)

    accepted = client.post(f"/api/pipelines/{source}_items/runs")
    app.state.runs.shutdown(wait=True)

    assert accepted.status_code == 202, accepted.text
    execution_id = accepted.json()["execution_id"]
    run = client.get(f"/api/pipeline-runs/{execution_id}").json()
    assert (run["status"], run["rows_in"], run["rows_out"], run["error"]) == (
        "succeeded",
        2,
        2,
        None,
    )
    assert run["input_run_id"] == str(ingest.run_id)
    assert [(node["kind"], node["ingest_run_id"] is not None) for node in run["lineage"]] == [
        ("source", True),
        ("raw", True),
        ("step", False),
        ("clean", False),
    ]
    assert [check["passed"] for check in run["validation"]] == [True]
    with psycopg.connect(_url()) as conn:
        clean = sql.Identifier("clean", f"{source}__items")
        rows = conn.execute(sql.SQL("SELECT id, name FROM {} ORDER BY id").format(clean))
        assert rows.fetchall() == [(1, "ada"), (2, "bo")]
    # The background thread freed the dataset's lock: another connection can take it.
    with PostgresLoader(_url()) as other:
        assert other.lock_dataset(source, "items")
        other.unlock_dataset(source, "items")


SEARCHED_DATASETS = ["orders", "order_lines", "ordersx", "a_b", "ab"]


@pytest.mark.db
@db_property
@given(
    st.lists(
        st.tuples(st.sampled_from(["shop", "sales_eu"]), st.sampled_from(SEARCHED_DATASETS)),
        min_size=1,
        max_size=5,
        unique=True,
    ),
    st.sampled_from(
        ["_", "a_", "A_B", "ORDER", "order_", "lines", "shop", "sales_e", "orders", "x"]
    ),
)
def test_dataset_search_finds_exactly_the_names_containing_the_text(
    datasets: list[tuple[str, str]], q: str
) -> None:
    prefix = _prefix()
    with PostgresLoader(_url()) as loader:
        for source, dataset in datasets:
            _copy(loader, f"{prefix}_{source}", dataset)
    client, _ = _shared_db_client()

    found = client.get("/api/datasets", params={"q": q}).json()

    ours = sorted(
        (item["source"], item["dataset"]) for item in found if item["source"].startswith(prefix)
    )
    expected = sorted(
        (f"{prefix}_{source}", dataset)
        for source, dataset in datasets
        if q.lower() in f"{prefix}_{source}" or q.lower() in dataset
    )
    assert ours == expected


@st.composite
def stored_tables(draw: st.DrawFn) -> tuple[bool, pl.DataFrame, int]:
    keyed = draw(st.booleans())
    height = draw(st.sampled_from([0, 1, 2, 5, 7]))
    keys = draw(
        st.lists(
            # Lower-case letters sort the same in every collation Postgres might use.
            st.tuples(st.integers(-3, 3), st.sampled_from(["a", "ab", "b", ""])),
            min_size=height,
            max_size=height,
            unique=True,
        )
    )
    frame = pl.DataFrame(
        {
            "k1": [key[0] for key in keys],
            "k2": [key[1] for key in keys],
            "amount": draw(
                st.lists(
                    st.none() | st.decimals("-999.99", "999.99", places=2),
                    min_size=height,
                    max_size=height,
                )
            ),
            "score": draw(
                st.lists(
                    st.none() | st.sampled_from([math.nan, 1.5, -2.25]),
                    min_size=height,
                    max_size=height,
                )
            ),
            "at": draw(
                st.lists(
                    st.datetimes(
                        datetime(2000, 1, 1), datetime(2030, 1, 1), timezones=st.just(UTC)
                    ),
                    min_size=height,
                    max_size=height,
                )
            ),
        },
        schema={
            "k1": pl.Int64,
            "k2": pl.String,
            "amount": pl.Decimal(8, 2),
            "score": pl.Float64,
            "at": pl.Datetime("us", "UTC"),
        },
    )
    page_size = draw(st.sampled_from(sorted({1, 2, max(height, 1), height + 1})))
    return keyed, frame, page_size


@pytest.mark.db
@db_property
@given(stored_tables())
def test_preview_pages_join_back_into_the_whole_table(
    generated: tuple[bool, pl.DataFrame, int],
) -> None:
    keyed, frame, page_size = generated
    source = _prefix()
    table = f"{source}__rows"
    with PostgresLoader(_url()) as loader:
        with loader.transaction() as transaction:
            transaction.replace_table(table, iter([frame]))
        _copy(loader, source, "rows", ("k1", "k2") if keyed else (), table)
    client, _ = _shared_db_client()

    pages = []
    offset = 0
    while True:
        page = client.get(
            f"/api/datasets/{source}/rows/rows", params={"limit": page_size, "offset": offset}
        ).json()
        pages.append(page)
        offset += page_size
        if not page["has_more"]:
            break
    beyond = client.get(
        f"/api/datasets/{source}/rows/rows", params={"limit": page_size, "offset": offset}
    ).json()

    assert [page["has_more"] for page in pages] == [True] * (len(pages) - 1) + [False]
    assert (beyond["rows"], beyond["has_more"]) == ([], False)
    joined = [row for page in pages for row in page["rows"]]
    expected = [
        {name: json_value(value) for name, value in row.items()} for row in frame.rows(named=True)
    ]
    if keyed:
        expected.sort(key=lambda row: (int(row["k1"] or 0), str(row["k2"])))
        assert [(row["k1"], row["k2"]) for row in joined] == [
            (row["k1"], row["k2"]) for row in expected
        ]
    assert Counter(json.dumps(row, sort_keys=True) for row in joined) == Counter(
        json.dumps(row, sort_keys=True) for row in expected
    )
    with psycopg.connect(_url(), autocommit=True) as conn:
        conn.execute(sql.SQL("DROP TABLE {}").format(sql.Identifier("datasets", table)))


@st.composite
def run_histories(draw: st.DrawFn) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    base = datetime(2001, 1, 1, tzinfo=UTC)
    runs: list[dict[str, Any]] = draw(
        st.lists(
            st.fixed_dictionaries(
                {
                    "source": st.sampled_from(["x", "x", "xy"]),
                    "dataset": st.sampled_from(["d", "e"]),
                    "status": st.sampled_from(["running", "succeeded", "failed", "skipped"]),
                    "trigger": st.sampled_from(["manual", "scheduled"]),
                    "minute": st.sampled_from([0, 1, 1, 2, 3]),
                }
            ),
            max_size=8,
        )
    )
    # Time bounds are drawn mostly from the runs' own start times, where inclusive and
    # exclusive differ; the other filters are often left out so those runs stay in play.
    started = [base + timedelta(minutes=run["minute"]) for run in runs] or [base]
    bound = st.sampled_from([None, *started, *started, base + timedelta(minutes=4)])
    query = {
        "source": draw(st.sampled_from(["x", "x", "xy"])),
        "dataset": draw(st.sampled_from([None, None, "d", "e"])),
        "status": draw(
            st.sampled_from([None, None, None, "running", "succeeded", "failed", "skipped"])
        ),
        "trigger": draw(st.sampled_from([None, None, "manual", "scheduled"])),
        "since": draw(bound),
        "until": draw(bound),
        "limit": draw(st.sampled_from([1, 2, 50])),
    }
    return runs, query


@pytest.mark.db
@db_property
@given(run_histories())
def test_run_filters_and_pages_match_a_simple_model(
    generated: tuple[list[dict[str, Any]], dict[str, Any]],
) -> None:
    drawn, query = generated
    prefix = _prefix()
    base = datetime(2001, 1, 1, tzinfo=UTC)
    created = []
    with PostgresLoader(_url()) as loader:
        for item in drawn:
            run = RunStart(
                uuid7(),
                f"{prefix}_{item['source']}",
                item["dataset"],
                item["trigger"],
                base + timedelta(minutes=item["minute"]),
            )
            ended_at = run.started_at + timedelta(seconds=1)
            if item["status"] == "skipped":
                loader.skip_run(run, ended_at=ended_at)
            else:
                loader.start_run(run)
            if item["status"] == "succeeded":
                with loader.transaction() as transaction:
                    transaction.succeed_run(
                        run.run_id, ended_at=ended_at, rows_extracted=0, rows_loaded=0
                    )
            if item["status"] == "failed":
                failure = RunFailure("ExtractError", "file not found", "Traceback ...")
                loader.fail_run(run, ended_at=ended_at, rows_extracted=0, failure=failure)
            created.append((run, item["status"]))
    client, _ = _shared_db_client()
    source = f"{prefix}_{query['source']}"
    expected = sorted(
        (
            run.run_id
            for run, status in created
            if run.source == source
            and query["dataset"] in (None, run.dataset)
            and query["status"] in (None, status)
            and query["trigger"] in (None, run.trigger)
            and (query["since"] is None or run.started_at >= query["since"])
            and (query["until"] is None or run.started_at < query["until"])
        ),
        key=lambda run_id: next(
            (run.started_at, run.run_id) for run, _ in created if run.run_id == run_id
        ),
        reverse=True,
    )
    params: dict[str, Any] = {
        key: value.isoformat() if isinstance(value, datetime) else value
        for key, value in {**query, "source": source}.items()
        if value is not None
    }

    joined: list[str] = []
    offset = 0
    while True:
        page = client.get("/api/runs", params={**params, "offset": offset}).json()
        joined.extend(run["run_id"] for run in page["runs"])
        assert len(page["runs"]) <= query["limit"]
        offset += query["limit"]
        if not page["has_more"]:
            break

    assert joined == [str(run_id) for run_id in expected]
