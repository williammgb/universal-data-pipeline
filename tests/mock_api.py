"""A fake REST API for tests: three ways to log in, five ways to page through records."""

import os
from typing import Any

from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Request

COLOURS = ("red", "green", "blue", "black")


def record(index: int, changed_in: int = 0) -> dict[str, Any]:
    note = None if index < 100 or index % 7 else f"replaced by item {index + 1}"
    return {
        "id": index + 1,
        "name": f"item {index + 1}",
        "price": index * 1.25 + 0.5,
        "in_stock": index % 3 != 0,
        "created_at": f"2024-{index % 12 + 1:02d}-{index % 28 + 1:02d}T08:30:00Z",
        "attributes": {"colour": COLOURS[index % 4], "size": index % 5},
        "discontinued_note": note,
        "changed_in": changed_in,
    }


def records_at(rows: int, revision: int) -> list[dict[str, Any]]:
    """The dataset as it looks at a revision: every 100th record renamed, 50 more per revision."""
    data = [record(index) for index in range(rows)]
    if revision:
        for item in data[::100]:
            item["name"] += " (revised)"
            item["changed_in"] = revision
        data += [record(index, revision) for index in range(rows, rows + 50 * revision)]
    return data


# Records of mixed quality for sources/messy_api, built in on purpose: a score sent as a word,
# a date that does not exist, a team spelt and cased several ways, missing and absent fields, a
# reading repeated, a temperature from a broken sensor and a negative score.
MESSY: list[dict[str, Any]] = [
    {"id": 1, "team": "Red", "score": 7, "temp_c": 21.5, "measured_on": "2024-05-01"},
    {"id": 2, "team": "red", "score": 5, "temp_c": 22.0, "measured_on": "2024-05-01"},
    {"id": 3, "team": "RED ", "score": "seven", "temp_c": 20.5, "measured_on": "2024-05-02"},
    {"id": 4, "team": "Blue", "score": 6, "temp_c": None, "measured_on": "2024-05-02"},
    {"id": 5, "team": "blu", "score": 8, "temp_c": 19.0, "measured_on": "2024-05-32"},
    {"id": 6, "team": "Blue", "score": 4, "temp_c": 18.5, "measured_on": "2024-05-03"},
    {"id": 7, "team": "Green", "score": 9, "temp_c": 9999.0, "measured_on": "2024-05-03"},
    {"id": 7, "team": "Green", "score": 9, "temp_c": 9999.0, "measured_on": "2024-05-03"},
    {"id": 8, "team": "green", "score": -2, "temp_c": 21.0, "measured_on": "2024-05-04"},
    {"id": 9, "team": "GREEN", "temp_c": 23.5, "measured_on": "2024-05-04"},
    {"id": 10, "team": "Red", "score": 6, "temp_c": 20.0, "measured_on": "2024-05-05"},
    {"id": 11, "team": "Blue", "score": 7, "temp_c": 22.5, "measured_on": "2024-05-05"},
    {"id": 12, "team": "Green", "score": 5, "temp_c": 19.5, "measured_on": "2024-05-06"},
    {"id": 13, "team": "Red", "score": 8, "temp_c": 21.0, "measured_on": "2024-05-06"},
]


def create_app(rows: int = 2000, token: str = "test-token") -> FastAPI:
    revisions: dict[int, list[dict[str, Any]]] = {}

    def dataset(revision: int) -> list[dict[str, Any]]:
        if revision not in revisions:
            revisions[revision] = records_at(rows, revision)
        return revisions[revision]

    app = FastAPI()

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    def public() -> None:
        return None

    def api_key(x_api_key: str | None = Header(default=None)) -> None:
        if x_api_key != token:
            raise HTTPException(status_code=401)

    def bearer(authorization: str | None = Header(default=None)) -> None:
        if authorization != f"Bearer {token}":
            raise HTTPException(status_code=401)

    for prefix, check in (("/public", public), ("/api-key", api_key), ("/bearer", bearer)):
        router = APIRouter(dependencies=[Depends(check)])

        @router.get("/all")
        def all_records(revision: int = 0) -> dict[str, Any]:
            return {"data": dataset(revision)}

        @router.get("/messy")
        def messy() -> dict[str, Any]:
            return {"data": MESSY}

        @router.get("/pages")
        def pages(page: int = 1, per_page: int = 100, revision: int = 0) -> dict[str, Any]:
            start = (page - 1) * per_page
            return {"data": dataset(revision)[start : start + per_page]}

        @router.get("/offsets")
        def offsets(offset: int = 0, limit: int = 100, revision: int = 0) -> dict[str, Any]:
            return {"items": dataset(revision)[offset : offset + limit]}

        @router.get("/cursor")
        def cursor(
            cursor: str | None = None, limit: int = 100, revision: int = 0
        ) -> dict[str, Any]:
            data = dataset(revision)
            start = int(cursor) if cursor else 0
            following = start + limit
            next_cursor = str(following) if following < len(data) else None
            return {"data": data[start:following], "meta": {"next_cursor": next_cursor}}

        @router.get("/linked")
        def linked(
            request: Request, after: int = 0, limit: int = 100, revision: int = 0
        ) -> dict[str, Any]:
            data = dataset(revision)
            following = after + limit
            link = (
                str(request.url.include_query_params(after=following, limit=limit))
                if following < len(data)
                else None
            )
            return {"results": data[after:following], "links": {"next": link}}

        app.include_router(router, prefix=prefix)

    return app


def app_from_env() -> FastAPI:
    return create_app(token=os.environ["MOCK_API_TOKEN"])
