import json
import math
from collections import Counter
from contextlib import nullcontext
from datetime import UTC, date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import unquote
from uuid import UUID, uuid4, uuid7

import polars as pl
import psycopg
import pytest
from fakes import MemoryLoader
from fastapi.testclient import TestClient
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from psycopg import sql

from udp.api.app import create_app
from udp.api.catalog import PostgresCatalog, json_value
from udp.config.source import load_source
from udp.pipeline.runner import run_source
from udp.settings import Settings
from udp.storage.loader import ConfigCopy, RunFailure, RunStart
from udp.storage.postgres import PostgresLoader

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


def _client(sources: Path = SOURCES, loader: MemoryLoader | None = None) -> tuple[TestClient, Any]:
    app = create_app(
        PostgresCatalog(UNREACHABLE), sources, {}, lambda: nullcontext(loader or MemoryLoader())
    )
    return TestClient(app), app


def test_the_openapi_document_lists_every_route_and_the_docs_page_loads() -> None:
    client, _ = _client()

    paths = client.get("/api/openapi.json").json()["paths"]

    assert sorted(paths) == [
        "/api/datasets",
        "/api/datasets/{source}/{dataset}",
        "/api/datasets/{source}/{dataset}/quality",
        "/api/datasets/{source}/{dataset}/rows",
        "/api/health",
        "/api/runs",
        "/api/runs/{run_id}",
        "/api/sources",
        "/api/sources/{source}",
    ]
    assert set(paths["/api/runs"]) == {"get", "post"}
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
        ({"source": "demo_csv", "full_refresh": True}, 422, "full_refresh"),
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


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(PATHS)
def test_any_address_gives_a_page_a_file_or_the_apis_own_404(tmp_path: Path, path: str) -> None:
    built = _dashboard(tmp_path)
    app = create_app(
        PostgresCatalog(UNREACHABLE), SOURCES, {}, lambda: nullcontext(MemoryLoader()), built
    )

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
        create_app(
            PostgresCatalog(UNREACHABLE), SOURCES, {}, lambda: nullcontext(MemoryLoader()), built
        )
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
            PostgresCatalog(UNREACHABLE),
            SOURCES,
            {},
            lambda: nullcontext(MemoryLoader()),
            tmp_path / "dist",
        )
    )

    assert client.get("/").status_code == 404
    assert client.get("/api/docs").status_code == 200


# --- the catalog against Postgres ---------------------------------------------------------------


# Each example opens several fresh connections, as the API itself does, and a few hundred
# examples use up Windows' range of outgoing ports faster than it releases them ("Address
# already in use" in the full gate). A pool is slice 10's job; until then these properties
# run a fixed number of examples instead of the ci profile's 500.
db_property = settings(max_examples=60, suppress_health_check=[HealthCheck.too_slow])


def _url() -> str:
    return str(Settings().database_url)  # type: ignore[call-arg]


def _prefix() -> str:
    return f"api_{uuid4().hex[:10]}"


def _db_client(sources: Path = SOURCES) -> tuple[TestClient, Any]:
    url = _url()
    app = create_app(PostgresCatalog(url), sources, {}, lambda: PostgresLoader(url))
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
    client, _ = _db_client()

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
    client, _ = _db_client()

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
                loader.fail_run(run.run_id, ended_at=ended_at, rows_extracted=0, failure=failure)
            created.append((run, item["status"]))
    client, _ = _db_client()
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
