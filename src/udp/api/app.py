"""The HTTP API: dataset discovery, previews, quality, run history, and starting runs by hand."""

import time
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from contextlib import AbstractContextManager, asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any
from uuid import UUID

import structlog
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse
from pydantic import AwareDatetime

from udp.api.catalog import PostgresCatalog
from udp.api.models import (
    DatasetDetail,
    DatasetItem,
    Health,
    QualityReport,
    RowsPage,
    RunAccepted,
    RunDetail,
    RunRequest,
    RunsPage,
    RunStatus,
    RunTrigger,
    SourceDetail,
    SourceItem,
)
from udp.config.source import SOURCE_FILE, SourceConfig, load_source
from udp.errors import ConfigError
from udp.names import name_problem
from udp.orchestration.scheduler import RUNS_AT_ONCE
from udp.pipeline.runner import run_source
from udp.storage.loader import Loader

log = structlog.get_logger(step="api")

Limit = Annotated[int, Query(ge=1, le=500)]
Offset = Annotated[int, Query(ge=0)]


def create_app(
    catalog: PostgresCatalog,
    sources_dir: Path,
    env: Mapping[str, str],
    open_loader: Callable[[], AbstractContextManager[Loader]],
    dashboard_dir: Path | None = None,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        yield
        # Runs cut off here are marked Interrupted by the next run of their dataset.
        app.state.runs.shutdown(wait=False, cancel_futures=True)

    app = FastAPI(
        title="Universal data platform",
        docs_url="/api/docs",
        openapi_url="/api/openapi.json",
        redoc_url=None,
        lifespan=lifespan,
    )
    app.state.runs = ThreadPoolExecutor(RUNS_AT_ONCE, thread_name_prefix="api-run")

    @app.middleware("http")
    async def log_request(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        started = time.perf_counter()
        response = await call_next(request)
        log.info(
            "request handled",
            method=request.method,
            path=request.url.path,
            status=response.status_code,
            duration_ms=round((time.perf_counter() - started) * 1000, 1),
        )
        return response

    def found[T](value: T | None, what: str) -> T:
        if value is None:
            raise HTTPException(404, f"{what} not found")
        return value

    @app.get("/api/health")
    def health(response: Response) -> Health:
        try:
            catalog.ping()
        except Exception as error:
            log.warning("database unavailable", error=str(error))
            response.status_code = 503
            return Health(status="unavailable")
        return Health(status="ok")

    @app.get("/api/sources")
    def sources(q: str | None = None) -> list[SourceItem]:
        return catalog.sources(q)

    @app.get("/api/sources/{source}")
    def source(source: str) -> SourceDetail:
        return found(catalog.source(source), f"source '{source}'")

    @app.get("/api/datasets")
    def datasets(q: str | None = None, source: str | None = None) -> list[DatasetItem]:
        return catalog.datasets(q, source)

    @app.get("/api/datasets/{source}/{dataset}")
    def dataset(source: str, dataset: str) -> DatasetDetail:
        return found(catalog.dataset(source, dataset), f"dataset '{source}.{dataset}'")

    @app.get("/api/datasets/{source}/{dataset}/rows")
    def rows(source: str, dataset: str, limit: Limit = 50, offset: Offset = 0) -> RowsPage:
        page = catalog.rows(source, dataset, limit, offset)
        return found(page, f"dataset '{source}.{dataset}'")

    @app.get("/api/datasets/{source}/{dataset}/quality")
    def quality(source: str, dataset: str) -> QualityReport:
        return found(catalog.quality(source, dataset), f"dataset '{source}.{dataset}'")

    @app.get("/api/runs")
    def runs(
        source: str | None = None,
        dataset: str | None = None,
        status: RunStatus | None = None,
        trigger: RunTrigger | None = None,
        since: AwareDatetime | None = None,
        until: AwareDatetime | None = None,
        limit: Limit = 50,
        offset: Offset = 0,
    ) -> RunsPage:
        return catalog.runs(
            source=source,
            dataset=dataset,
            status=status,
            trigger=trigger,
            since=since,
            until=until,
            limit=limit,
            offset=offset,
        )

    @app.get("/api/runs/{run_id}")
    def run(run_id: UUID) -> RunDetail:
        return found(catalog.run(run_id), f"run {run_id}")

    def run_in_background(source: str, config: SourceConfig[Any, Any]) -> None:
        try:
            with open_loader() as loader:
                run_source(source, config, sources_dir, loader, trigger="manual")
        except Exception as error:
            log.error("run started over the API could not run", source=source, error=repr(error))

    @app.post("/api/runs", status_code=202)
    def start_run(request: RunRequest) -> RunAccepted:
        if (
            name_problem(request.source)
            or not (sources_dir / request.source / SOURCE_FILE).is_file()
        ):
            raise HTTPException(404, f"source '{request.source}' not found")
        try:
            config = load_source(sources_dir, request.source, env)
        except ConfigError as error:
            raise HTTPException(422, str(error)) from error
        if request.dataset is not None:
            chosen = [item for item in config.datasets if item.name == request.dataset]
            if not chosen:
                raise HTTPException(
                    404, f"dataset '{request.dataset}' is not in source '{request.source}'"
                )
            config = config.model_copy(update={"datasets": chosen})
        requested_at = datetime.now(UTC)
        app.state.runs.submit(run_in_background, request.source, config)
        log.info("run requested", source=request.source, dataset=request.dataset)
        return RunAccepted(
            source=request.source,
            datasets=[item.name for item in config.datasets],
            requested_at=requested_at,
        )

    _serve_dashboard(app, dashboard_dir)
    return app


def _serve_dashboard(app: FastAPI, dashboard_dir: Path | None) -> None:
    """Serve the built dashboard for every address that is not part of the API.

    A page address like /datasets/demo_csv/customers is handled by the dashboard itself, so a
    reload must answer with index.html; a real file is served as itself, and anything under
    /api keeps the API's JSON 404.
    """
    if dashboard_dir is None:
        return
    root = dashboard_dir.resolve()
    index = root / "index.html"
    if not index.is_file():
        log.warning("dashboard not built; serving the API only", path=str(root))
        return

    @app.get("/{path:path}", include_in_schema=False)
    def dashboard(path: str) -> FileResponse:
        if path == "api" or path.startswith("api/"):
            raise HTTPException(404, "not found")
        try:
            target = (root / path).resolve()
            is_file = path != "" and target.is_relative_to(root) and target.is_file()
        except OSError, ValueError:
            # A path the filesystem refuses outright, such as one holding a null byte.
            is_file = False
        if not is_file:
            return FileResponse(index, headers={"Cache-Control": "no-cache"})
        # Built file names carry a content hash, so they never change under a caller.
        cache = "max-age=31536000, immutable" if path.startswith("assets/") else "no-cache"
        return FileResponse(target, headers={"Cache-Control": cache})
