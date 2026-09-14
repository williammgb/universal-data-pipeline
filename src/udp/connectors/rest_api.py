import json
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import Annotated, Any, Literal, Self

import httpx
import polars as pl
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    SecretStr,
    field_validator,
    model_validator,
)

from udp.connectors.base import ConnectionBase, DatasetBase, ExtractRequest
from udp.connectors.retry import RETRY_WAITS, retry
from udp.errors import ExtractError

RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
REQUEST_TIMEOUT_SECONDS = 30.0
MAX_PAGES = 10_000
INT64_MIN, INT64_MAX = -(2**63), 2**63 - 1

ClientFactory = Callable[[str, Mapping[str, str], float], httpx.Client]


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class NoAuth(_Strict):
    type: Literal["none"]


class ApiKeyAuth(_Strict):
    type: Literal["api_key"]
    header: str = "X-API-Key"
    key: SecretStr


class BearerAuth(_Strict):
    type: Literal["bearer"]
    token: SecretStr


class NoPagination(_Strict):
    type: Literal["none"]


class PagePagination(_Strict):
    type: Literal["page"]
    page_param: str = "page"
    first_page: int = 1


class OffsetPagination(_Strict):
    type: Literal["offset"]
    offset_param: str = "offset"


class CursorPagination(_Strict):
    type: Literal["cursor"]
    cursor_param: str = "cursor"
    cursor_path: str


class NextLinkPagination(_Strict):
    type: Literal["next_link"]
    next_path: str


Auth = Annotated[NoAuth | ApiKeyAuth | BearerAuth, Field(discriminator="type")]
Pagination = Annotated[
    NoPagination | PagePagination | OffsetPagination | CursorPagination | NextLinkPagination,
    Field(discriminator="type"),
]


class RestApiConnection(ConnectionBase):
    type: Literal["rest_api"]
    base_url: HttpUrl
    auth: Auth = Field(default_factory=lambda: NoAuth(type="none"))


class RestApiDataset(DatasetBase):
    endpoint: str
    records_path: str | None = None
    params: dict[str, str | int | float | bool] = Field(default_factory=dict)
    pagination: Pagination = Field(default_factory=lambda: NoPagination(type="none"))

    @field_validator("endpoint")
    @classmethod
    def _starts_with_slash(cls, value: str) -> str:
        if not value.startswith("/"):
            raise ValueError("must start with '/'")
        return value

    @model_validator(mode="after")
    def _params_leave_room_for_pagination(self) -> Self:
        pagination = self.pagination
        owned = (
            pagination.page_param
            if isinstance(pagination, PagePagination)
            else pagination.offset_param
            if isinstance(pagination, OffsetPagination)
            else pagination.cursor_param
            if isinstance(pagination, CursorPagination)
            else None
        )
        if owned is not None and owned in self.params:
            raise ValueError(f"params.{owned} is set by pagination and cannot also be in params")
        return self


_MISSING = object()


def _at(body: Any, path: str) -> Any:
    node = body
    for part in path.split("."):
        if not isinstance(node, dict) or part not in node:
            return _MISSING
        node = node[part]
    return node


def _default_client(base_url: str, headers: Mapping[str, str], timeout: float) -> httpx.Client:
    return httpx.Client(base_url=base_url, headers=dict(headers), timeout=timeout)


def _auth_headers(auth: NoAuth | ApiKeyAuth | BearerAuth) -> dict[str, str]:
    if isinstance(auth, ApiKeyAuth):
        return {auth.header: auth.key.get_secret_value()}
    if isinstance(auth, BearerAuth):
        return {"Authorization": f"Bearer {auth.token.get_secret_value()}"}
    return {}


class _RetryableStatus(Exception):
    def __init__(self, status: int) -> None:
        super().__init__(f"HTTP {status}")
        self.status = status


def _kind(value: Any) -> str:
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int" if INT64_MIN <= value <= INT64_MAX else "other"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "str"
    return "other"


def _as_text(value: Any) -> str | None:
    if value is None or isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False)


def records_frame(records: Sequence[Mapping[str, Any]]) -> pl.DataFrame:
    """One column per key seen on any record, each typed over the whole dataset."""
    names: dict[str, None] = {}
    for record in records:
        names.update(dict.fromkeys(record))
    series = []
    for name in names:
        values = [record.get(name) for record in records]
        kinds = {_kind(value) for value in values if value is not None}
        if not kinds:
            series.append(pl.Series(name, values, dtype=pl.Null))
        elif kinds == {"int"}:
            series.append(pl.Series(name, values, dtype=pl.Int64))
        elif kinds <= {"int", "float"}:
            floats = [None if v is None else float(v) for v in values]
            series.append(pl.Series(name, floats, dtype=pl.Float64))
        elif kinds == {"bool"}:
            series.append(pl.Series(name, values, dtype=pl.Boolean))
        else:
            series.append(pl.Series(name, [_as_text(v) for v in values], dtype=pl.String))
    return pl.DataFrame(series)


class RestApiConnector:
    connection_model = RestApiConnection
    dataset_model = RestApiDataset

    def __init__(
        self,
        client_factory: ClientFactory = _default_client,
        waits: Sequence[float] = RETRY_WAITS,
        max_pages: int = MAX_PAGES,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._client_factory = client_factory
        self._waits = waits
        self._max_pages = max_pages
        self._sleep = sleep

    def extract(
        self, request: ExtractRequest[RestApiConnection, RestApiDataset]
    ) -> Iterator[pl.DataFrame]:
        frame = records_frame(list(self._records(request.connection, request.dataset)))
        if frame.height == 0:
            yield frame
            return
        yield from frame.iter_slices(request.chunk_size)

    def _records(
        self, connection: RestApiConnection, dataset: RestApiDataset
    ) -> Iterator[Mapping[str, Any]]:
        base_url = httpx.URL(str(connection.base_url))
        endpoint = dataset.endpoint
        headers = _auth_headers(connection.auth)
        requests_made = 0

        with self._client_factory(str(base_url), headers, REQUEST_TIMEOUT_SECONDS) as client:

            def get(url: str, params: Mapping[str, Any] | None) -> Any:
                nonlocal requests_made
                requests_made += 1
                if requests_made > self._max_pages:
                    raise ExtractError(
                        f"GET {endpoint}: more than {self._max_pages} page requests; "
                        "pagination did not end"
                    )

                def call() -> httpx.Response:
                    response = client.get(url, params=params)
                    if response.status_code in RETRY_STATUSES:
                        raise _RetryableStatus(response.status_code)
                    return response

                try:
                    response = retry(
                        call,
                        transient=lambda e: isinstance(e, httpx.TransportError | _RetryableStatus),
                        waits=self._waits,
                        sleep=self._sleep,
                    )
                except _RetryableStatus as error:
                    raise ExtractError(f"GET {endpoint}: HTTP {error.status}") from None
                except httpx.TransportError as error:
                    raise ExtractError(f"GET {endpoint}: {type(error).__name__}") from None
                if not response.is_success:
                    raise ExtractError(f"GET {endpoint}: HTTP {response.status_code}")
                try:
                    return response.json()
                except ValueError:
                    raise ExtractError(f"GET {endpoint}: the response is not JSON") from None

            def records(body: Any) -> list[Mapping[str, Any]]:
                found = body if dataset.records_path is None else _at(body, dataset.records_path)
                if found is _MISSING:
                    raise ExtractError(f"GET {endpoint}: no '{dataset.records_path}' in response")
                if not isinstance(found, list) or not all(isinstance(r, dict) for r in found):
                    raise ExtractError(f"GET {endpoint}: records are not a list of objects")
                return found

            params = dict(dataset.params)
            pagination = dataset.pagination
            if isinstance(pagination, NoPagination):
                yield from records(get(endpoint, params))

            elif isinstance(pagination, PagePagination):
                page = pagination.first_page
                while page_records := records(
                    get(endpoint, {**params, pagination.page_param: page})
                ):
                    yield from page_records
                    page += 1

            elif isinstance(pagination, OffsetPagination):
                offset = 0
                while page_records := records(
                    get(endpoint, {**params, pagination.offset_param: offset})
                ):
                    yield from page_records
                    offset += len(page_records)

            elif isinstance(pagination, CursorPagination):
                cursor: Any = None
                seen: set[str] = set()
                while True:
                    query = (
                        params if cursor is None else {**params, pagination.cursor_param: cursor}
                    )
                    body = get(endpoint, query)
                    yield from records(body)
                    cursor = _at(body, pagination.cursor_path)
                    if cursor is _MISSING or cursor is None or cursor == "":
                        return
                    key = json.dumps(cursor, sort_keys=True)
                    if key in seen:
                        raise ExtractError(f"GET {endpoint}: the API repeated a cursor")
                    seen.add(key)

            else:
                url, query = endpoint, params
                seen_links: set[str] = set()
                while True:
                    body = get(url, query)
                    yield from records(body)
                    link = _at(body, pagination.next_path)
                    if link is _MISSING or link is None:
                        return
                    if not isinstance(link, str):
                        raise ExtractError(f"GET {endpoint}: the next link is not text")
                    target = base_url.join(link)
                    same_origin = (target.scheme, target.host, target.port) == (
                        base_url.scheme,
                        base_url.host,
                        base_url.port,
                    )
                    if not same_origin:
                        raise ExtractError(f"GET {endpoint}: next link points to another host")
                    if str(target) in seen_links:
                        raise ExtractError(f"GET {endpoint}: the API repeated a next link")
                    seen_links.add(str(target))
                    url, query = str(target), None
