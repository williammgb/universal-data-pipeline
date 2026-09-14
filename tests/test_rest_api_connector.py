import json
import re
import traceback
from collections.abc import Callable, Iterator, Mapping
from pathlib import Path
from typing import Any

import httpx
import polars as pl
import pytest
from fastapi.testclient import TestClient
from hypothesis import given
from hypothesis import strategies as st
from mock_api import create_app

from udp.config.source import load_source
from udp.connectors.base import ExtractRequest
from udp.connectors.rest_api import (
    RestApiConnection,
    RestApiConnector,
    RestApiDataset,
    records_frame,
)
from udp.errors import ConfigError, ExtractError

BASE = "http://api.test"
Handler = Callable[[httpx.Request], httpx.Response]


def _request(
    dataset: Mapping[str, Any], auth: Mapping[str, Any] | None = None, chunk_size: int = 1000
) -> ExtractRequest[RestApiConnection, RestApiDataset]:
    return ExtractRequest(
        source_dir=Path("."),
        connection=RestApiConnection.model_validate(
            {"type": "rest_api", "base_url": BASE, "auth": auth or {"type": "none"}}
        ),
        dataset=RestApiDataset.model_validate({"name": "items", **dataset}),
        chunk_size=chunk_size,
    )


def _mock_connector(
    handler: Handler, max_pages: int = 30, waits: tuple[float, ...] = ()
) -> RestApiConnector:
    def factory(base_url: str, headers: Mapping[str, str], timeout: float) -> httpx.Client:
        return httpx.Client(
            base_url=base_url, headers=dict(headers), transport=httpx.MockTransport(handler)
        )

    return RestApiConnector(
        client_factory=factory, max_pages=max_pages, waits=waits, sleep=lambda _: None
    )


def _rows(chunks: Iterator[pl.DataFrame]) -> list[dict[str, Any]]:
    return pl.concat(list(chunks), how="vertical").to_dicts()


def _page_of(
    request: httpx.Request, records: list[dict[str, Any]], size: int
) -> list[dict[str, Any]]:
    query = request.url.params
    if "page" in query:
        start = (int(query["page"]) - 1) * size
    elif "offset" in query:
        start = int(query["offset"])
    elif "cursor" in query:
        start = int(query["cursor"])
    elif "after" in query:
        start = int(query["after"])
    else:
        start = 0
    return records[start : start + size]


MODES = {
    "page": {"pagination": {"type": "page"}},
    "offset": {"pagination": {"type": "offset"}},
    "cursor": {"pagination": {"type": "cursor", "cursor_path": "next"}},
    "next_link": {"pagination": {"type": "next_link", "next_path": "next"}},
}


def _well_behaved(records: list[dict[str, Any]], size: int, mode: str) -> Handler:
    def handler(request: httpx.Request) -> httpx.Response:
        page = _page_of(request, records, size)
        query = request.url.params
        start = int(query.get("cursor") or query.get("after") or 0)
        following = start + size
        body: dict[str, Any] = {"data": page}
        if mode == "cursor":
            body["next"] = str(following) if following < len(records) else None
        if mode == "next_link":
            body["next"] = f"{BASE}/items?after={following}" if following < len(records) else None
        return httpx.Response(200, json=body)

    return handler


@given(
    st.integers(0, 120),
    st.integers(1, 25),
    st.sampled_from(sorted(MODES)),
    st.integers(1, 50),
)
def test_well_behaved_servers_give_every_record_in_order(
    total: int, size: int, mode: str, chunk_size: int
) -> None:
    records = [{"id": i} for i in range(total)]
    connector = _mock_connector(_well_behaved(records, size, mode), max_pages=200)

    chunks = list(
        connector.extract(
            _request(
                {"endpoint": "/items", "records_path": "data", **MODES[mode]}, chunk_size=chunk_size
            )
        )
    )

    assert all(chunk.height <= chunk_size for chunk in chunks)
    assert sum(chunk.height for chunk in chunks) == total
    if total:
        assert pl.concat(chunks)["id"].to_list() == list(range(total))


NEXT_VALUES = st.sampled_from(["a", "b", "c", None, "", "missing", "self", "earlier", "other-host"])


@st.composite
def scripted_servers(draw: st.DrawFn) -> tuple[str, Handler, list[int]]:
    mode = draw(st.sampled_from(sorted(MODES)))
    script = draw(st.lists(NEXT_VALUES, max_size=40))
    sizes = draw(st.lists(st.integers(0, 3), min_size=1, max_size=40))
    ignores_parameters = draw(st.booleans())
    served: list[int] = []
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        index = calls["n"]
        calls["n"] += 1
        size = sizes[index % len(sizes)] if not ignores_parameters else max(sizes[0], 1)
        served.append(size)
        body: dict[str, Any] = {"data": [{"id": index, "n": n} for n in range(size)]}
        step = script[index] if index < len(script) else None
        if step != "missing":
            if mode == "next_link":
                links: dict[str | None, str | None] = {
                    None: None,
                    "": None,
                    "self": str(request.url),
                    "earlier": f"{BASE}/items?page=0",
                    "other-host": "http://evil.test/items",
                }
                body["next"] = links.get(step, f"{BASE}/items?step={step}")
            else:
                body["next"] = step
        return httpx.Response(200, json=body)

    return mode, handler, served


@given(scripted_servers())
def test_pagination_always_ends(server: tuple[str, Handler, list[int]]) -> None:
    mode, handler, served = server
    connector = _mock_connector(handler, max_pages=30)

    try:
        rows = _rows(
            connector.extract(
                _request({"endpoint": "/items", "records_path": "data", **MODES[mode]})
            )
        )
    except ExtractError:
        assert len(served) <= 30
        return

    assert len(served) <= 30
    assert len(rows) == sum(served)


def _json_value(value: Any) -> str:
    return value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)


def _expected_column(values: list[Any]) -> tuple[pl.DataType, list[Any]]:
    present = [v for v in values if v is not None]
    kinds = {
        "bool"
        if isinstance(v, bool)
        else "int"
        if isinstance(v, int) and -(2**63) <= v < 2**63
        else "float"
        if isinstance(v, float)
        else "str"
        if isinstance(v, str)
        else "other"
        for v in present
    }
    if not kinds:
        return pl.Null(), values
    if kinds == {"int"}:
        return pl.Int64(), values
    if kinds <= {"int", "float"}:
        return pl.Float64(), [None if v is None else float(v) for v in values]
    if kinds == {"bool"}:
        return pl.Boolean(), values
    return pl.String(), [None if v is None else _json_value(v) for v in values]


VALUE = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(-(2**40), 2**40),
    st.sampled_from([2**63 - 1, 2**63, -(2**63), -(2**63) - 1]),
    st.floats(allow_nan=False, allow_infinity=False),
    st.text(max_size=5),
    st.dictionaries(st.sampled_from(["x", "y"]), st.integers(0, 3), max_size=2),
    st.lists(st.integers(0, 3), max_size=2),
)
RECORD = st.dictionaries(st.sampled_from(["a", "b", "c", "late"]), VALUE, max_size=4)


@given(st.lists(st.lists(RECORD, max_size=6), min_size=1, max_size=5), st.integers(1, 7))
def test_api_columns_are_typed_over_the_whole_dataset(
    pages: list[list[dict[str, Any]]], chunk_size: int
) -> None:
    pages = [*pages[:-1], [{**record, "late": 1} for record in pages[-1]]]
    records = [record for page in pages for record in page]

    # Cursor paging, because it carries on past an empty page in the middle.
    def handler(request: httpx.Request) -> httpx.Response:
        index = int(request.url.params.get("cursor", 0))
        following = str(index + 1) if index + 1 < len(pages) else None
        return httpx.Response(200, json={"data": pages[index], "next": following})

    chunks = list(
        _mock_connector(handler).extract(
            _request(
                {"endpoint": "/items", "records_path": "data", **MODES["cursor"]},
                chunk_size=chunk_size,
            )
        )
    )

    assert len({tuple(chunk.schema.items()) for chunk in chunks}) == 1
    if not any(records):
        # No record has any field: there are no columns, which the validate stage rejects.
        assert [chunk.width for chunk in chunks] == [0]
        return
    assert sum(chunk.height for chunk in chunks) == len(records)
    whole = pl.concat(chunks)
    for name in whole.columns:
        dtype, values = _expected_column([record.get(name) for record in records])
        assert whole.schema[name] == dtype, name
        assert whole[name].to_list() == values, name


def test_mixed_booleans_and_numbers_become_text() -> None:
    frame = records_frame([{"flag": True}, {"flag": 1}, {"flag": 2.5}])

    assert frame.schema["flag"] == pl.String
    assert frame["flag"].to_list() == ["true", "1", "2.5"]


def test_integers_beyond_64_bits_keep_every_digit_as_text() -> None:
    frame = records_frame([{"n": 1}, {"n": 2**63}, {"n": None}, {"n": -(2**63)}])

    assert frame.schema["n"] == pl.String
    assert frame["n"].to_list() == ["1", str(2**63), None, str(-(2**63))]


@pytest.mark.parametrize(
    ("pagination", "param"),
    [
        ({"type": "page"}, "page"),
        ({"type": "offset", "offset_param": "skip"}, "skip"),
        ({"type": "cursor", "cursor_path": "next"}, "cursor"),
    ],
)
def test_params_cannot_also_set_the_pagination_parameter(
    pagination: dict[str, Any], param: str
) -> None:
    with pytest.raises(ValueError, match=f"params.{param} is set by pagination"):
        RestApiDataset.model_validate(
            {"name": "items", "endpoint": "/items", "params": {param: 5}, "pagination": pagination}
        )


TOKEN = "Leak-T0ken"


def _mock_api_connector(rows: int = 250) -> RestApiConnector:
    app = create_app(rows=rows, token=TOKEN)

    def factory(base_url: str, headers: Mapping[str, str], timeout: float) -> httpx.Client:
        return TestClient(app, base_url=base_url, headers=dict(headers))

    return RestApiConnector(client_factory=factory, waits=())


MOCK_DATASETS: dict[str, dict[str, Any]] = {
    "all": {"endpoint": "/public/all", "records_path": "data"},
    "page": {
        "endpoint": "/public/pages",
        "records_path": "data",
        "params": {"per_page": 100},
        "pagination": {"type": "page"},
    },
    "offset": {
        "endpoint": "/public/offsets",
        "records_path": "items",
        "params": {"limit": 100},
        "pagination": {"type": "offset"},
    },
    "cursor": {
        "endpoint": "/public/cursor",
        "records_path": "data",
        "params": {"limit": 100},
        "pagination": {"type": "cursor", "cursor_path": "meta.next_cursor"},
    },
    "next_link": {
        "endpoint": "/public/linked",
        "records_path": "results",
        "pagination": {"type": "next_link", "next_path": "links.next"},
    },
}


@pytest.mark.parametrize("mode", sorted(MOCK_DATASETS))
def test_every_pagination_mode_loads_every_mock_record(mode: str) -> None:
    request = ExtractRequest(
        source_dir=Path("."),
        connection=RestApiConnection.model_validate(
            {"type": "rest_api", "base_url": "http://testserver"}
        ),
        dataset=RestApiDataset.model_validate({"name": "items", **MOCK_DATASETS[mode]}),
        chunk_size=100,
    )

    frame = pl.concat(list(_mock_api_connector().extract(request)))

    assert frame["id"].to_list() == list(range(1, 251))
    assert frame.schema["attributes"] == pl.String
    assert frame.schema["discontinued_note"] == pl.String


@pytest.mark.parametrize(
    ("prefix", "auth"),
    [
        ("/public", {"type": "none"}),
        ("/api-key", {"type": "api_key", "key": TOKEN}),
        ("/bearer", {"type": "bearer", "token": TOKEN}),
    ],
)
def test_every_auth_mode_is_accepted(prefix: str, auth: dict[str, Any]) -> None:
    request = ExtractRequest(
        source_dir=Path("."),
        connection=RestApiConnection.model_validate(
            {"type": "rest_api", "base_url": "http://testserver", "auth": auth}
        ),
        dataset=RestApiDataset.model_validate(
            {"name": "items", "endpoint": f"{prefix}/all", "records_path": "data"}
        ),
        chunk_size=1000,
    )

    assert pl.concat(list(_mock_api_connector().extract(request))).height == 250


def test_wrong_token_fails_after_one_request_without_showing_the_token() -> None:
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.headers.get("Authorization"))
        return httpx.Response(401, json={"detail": "Not authenticated"})

    connector = _mock_connector(handler, waits=(0, 0, 0))

    with pytest.raises(ExtractError, match="HTTP 401") as raised:
        list(
            connector.extract(
                _request({"endpoint": "/items"}, auth={"type": "bearer", "token": TOKEN})
            )
        )

    assert calls == [f"Bearer {TOKEN}"]
    shown = str(raised.value) + "".join(traceback.format_exception(raised.value))
    assert TOKEN not in shown


def test_temporary_server_errors_are_retried() -> None:
    statuses = iter([503, 503, 200])

    def handler(request: httpx.Request) -> httpx.Response:
        status = next(statuses)
        return httpx.Response(
            status, json={"data": [{"id": 1}, {"id": 2}]} if status == 200 else {}
        )

    rows = _rows(
        _mock_connector(handler, waits=(0, 0, 0)).extract(
            _request({"endpoint": "/items", "records_path": "data"})
        )
    )

    assert [row["id"] for row in rows] == [1, 2]


def test_connection_errors_give_up_after_every_retry() -> None:
    attempts = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(1)
        raise httpx.ConnectError("refused", request=request)

    with pytest.raises(ExtractError, match="ConnectError"):
        list(_mock_connector(handler, waits=(0, 0, 0)).extract(_request({"endpoint": "/items"})))
    assert len(attempts) == 4


def test_next_link_to_another_host_is_refused() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, json={"data": [{"id": 1}], "next": "https://evil.test/items?page=2"}
        )

    with pytest.raises(ExtractError, match="another host"):
        list(
            _mock_connector(handler).extract(
                _request({"endpoint": "/items", "records_path": "data", **MODES["next_link"]})
            )
        )


def test_missing_records_path_raises() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"items": []})

    with pytest.raises(ExtractError, match="no 'data'"):
        list(
            _mock_connector(handler).extract(
                _request({"endpoint": "/items", "records_path": "data"})
            )
        )


@pytest.mark.parametrize(
    ("dataset", "field"),
    [
        ("    endpoint: /items\n    pagination:\n      type: cursor\n", "cursor_path"),
        ("    endpoint: items\n", "datasets[0].endpoint"),
    ],
)
def test_invalid_api_config_names_the_field(tmp_path: Path, dataset: str, field: str) -> None:
    folder = tmp_path / "sources" / "api"
    folder.mkdir(parents=True)
    connection = "connection:\n  type: rest_api\n  base_url: http://api.test\n"
    (folder / "source.yaml").write_text(
        f"{connection}datasets:\n  - name: items\n{dataset}", encoding="utf-8"
    )

    with pytest.raises(ConfigError, match=re.escape(field)):
        load_source(tmp_path / "sources", "api")
