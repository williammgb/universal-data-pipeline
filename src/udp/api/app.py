"""The HTTP API: dataset discovery, previews, quality, run history, and starting runs by hand."""

import time
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import AbstractContextManager, ExitStack, asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from typing import Annotated, Any
from uuid import UUID

import psycopg
import structlog
import yaml
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse
from pydantic import AwareDatetime

from udp.api.auth import accepted, needs_a_key, presented
from udp.api.catalog import PostgresCatalog
from udp.api.metrics import CONTENT_TYPE, render
from udp.api.models import (
    Column,
    ConfigUpdate,
    DatasetConfig,
    DatasetDetail,
    DatasetItem,
    DatasetProfile,
    Health,
    PipelineRun,
    PipelineRunAccepted,
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
from udp.config.overrides import differences, editable_fields
from udp.config.pipeline import load_pipeline, pipeline_exists, read_pipeline_file
from udp.config.source import SOURCE_FILE, load_source
from udp.connectors.base import DatasetBase
from udp.errors import ConfigError
from udp.names import RESERVED_COLUMNS, name_problem
from udp.orchestration.scheduler import RUNS_AT_ONCE
from udp.pipeline.execution import PipelineBusy, carry_out, read_run, start_pipeline
from udp.pipeline.execution import Started as StartedPipeline
from udp.pipeline.incremental import rebuild_reasons
from udp.pipeline.runner import run_source
from udp.storage.loader import Loader

log = structlog.get_logger(step="api")

RUNS_QUEUED = 16

Limit = Annotated[int, Query(ge=1, le=500)]
Offset = Annotated[int, Query(ge=0)]


def checks_text(checks: Any) -> str:
    """A dataset's quality checks as the YAML the file would hold, or empty when there are none.

    The tab edits them as text, because a check's settings differ from one type to the next and
    a form for all of them would be a worse editor than the block the file already uses.
    """
    if not checks:
        return ""
    # `check:` first in every block, the way the file writes them; validation puts the settings
    # every check shares — severity — in front of the one that says which check it is.
    ordered = [
        {name: value for name, value in sorted(check.items(), key=lambda pair: pair[0] != "check")}
        for check in checks
    ]
    return yaml.safe_dump(ordered, sort_keys=False, allow_unicode=True)


CHECKS_LIMIT = 100_000


class ChecksLoader(yaml.SafeLoader):
    """SafeLoader that also refuses aliases.

    An alias repeats a node it names, and a handful of nested ones turn a few hundred bytes
    into billions of values while the API expands them. A dataset's checks never need one.
    """

    def compose_node(self, parent: Any, index: Any) -> Any:
        if self.check_event(yaml.events.AliasEvent):
            raise yaml.YAMLError("checks may not use an alias (*name)")
        return super().compose_node(parent, index)


def names_a_secret(value: Any) -> bool:
    """True when anything in a submitted edit holds a `${NAME}` reference, however deep.

    The reference is filled from the platform's own environment when a source is read, so an
    edit carrying one would read a secret out of the process that serves the API.
    """
    if isinstance(value, str):
        return "${" in value
    if isinstance(value, Mapping):
        return any(names_a_secret(item) for item in [*value, *value.values()])
    if isinstance(value, list | tuple):
        return any(names_a_secret(item) for item in value)
    return False


def checks_from_text(text: str) -> Any:
    """The YAML the tab sent, read the way the file's own `checks:` block is read.

    Anything the reader refuses is a 422 with the reason, never a crash: this text comes
    straight from a caller, and a block nested deeply enough ends PyYAML's recursion rather
    than its parse.
    """
    if len(text) > CHECKS_LIMIT:
        raise HTTPException(
            422, f"checks: {len(text)} characters is more than the {CHECKS_LIMIT} allowed"
        )
    try:
        parsed = yaml.load(text, ChecksLoader)
    except yaml.YAMLError as error:
        mark = getattr(error, "problem_mark", None)
        where = f"line {mark.line + 1}: " if mark is not None else ""
        raise HTTPException(
            422, f"checks: {where}invalid YAML: {getattr(error, 'problem', error)}"
        ) from error
    except RecursionError as error:
        raise HTTPException(422, "checks: too deeply nested to read") from error
    return [] if parsed is None else parsed


def create_app(
    catalog: PostgresCatalog,
    sources_dir: Path,
    env: Mapping[str, str],
    open_loader: Callable[[], AbstractContextManager[Loader]],
    dashboard_dir: Path | None = None,
    api_keys: Sequence[str] = (),
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        yield
        # Runs cut off here are marked Interrupted by the next run of their dataset.
        app.state.runs.shutdown(wait=False, cancel_futures=True)
        catalog.close()

    app = FastAPI(
        title="Universal data platform",
        docs_url="/api/docs",
        openapi_url="/api/openapi.json",
        redoc_url=None,
        lifespan=lifespan,
    )
    app.state.runs = ThreadPoolExecutor(RUNS_AT_ONCE, thread_name_prefix="api-run")
    # Runs accepted but not finished. Without a cap, a caller in a loop can queue thousands,
    # and the API would keep saying yes long after nothing can be run.
    app.state.queued = 0
    app.state.queue = Lock()

    # Registered before the request log, so the log sits outside it and a refusal is logged
    # like any other answer. With no keys configured every address stays open.
    @app.middleware("http")
    async def check_the_key(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        if needs_a_key(request.url.path) and not accepted(api_keys, presented(request.headers)):
            return JSONResponse({"detail": "an API key is required"}, status_code=401)
        return await call_next(request)

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

    # The database being unreachable is an answer, not a crash — and the connection string
    # never reaches the caller, because it carries the password.
    @app.exception_handler(psycopg.Error)
    def database_unavailable(request: Request, error: Exception) -> JSONResponse:
        log.warning("database unavailable", path=request.url.path, error=type(error).__name__)
        return JSONResponse({"detail": "the platform database is unavailable"}, status_code=503)

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

    # Prometheus text, not JSON, so it stays out of the description the dashboard's types use.
    @app.get("/api/metrics", include_in_schema=False)
    def metrics() -> PlainTextResponse:
        try:
            snapshot = catalog.metrics()
        except Exception as error:
            log.warning("database unavailable", error=str(error))
            return PlainTextResponse(
                "# the platform database is unavailable\n", 503, media_type=CONTENT_TYPE
            )
        return PlainTextResponse(render(snapshot), media_type=CONTENT_TYPE)

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

    @app.get("/api/datasets/{source}/{dataset}/profile")
    def profile(source: str, dataset: str) -> DatasetProfile:
        return found(catalog.profile(source, dataset), f"dataset '{source}.{dataset}'")

    @app.get("/api/datasets/{source}/{dataset}/quality")
    def quality(source: str, dataset: str) -> QualityReport:
        return found(catalog.quality(source, dataset), f"dataset '{source}.{dataset}'")

    def a_source(source: str) -> None:
        if name_problem(source) or not (sources_dir / source / SOURCE_FILE).is_file():
            raise HTTPException(404, f"source '{source}' not found")

    def read_config(
        source: str, dataset: str, overrides: Mapping[str, Mapping[str, Any]]
    ) -> tuple[DatasetBase, DatasetBase, str]:
        """The dataset as the file has it, as these overrides make it, and its connector.

        A source or dataset that is not there is a 404, and anything the file or the overrides
        make invalid is a 422 carrying the message the config itself gives.
        """
        a_source(source)
        try:
            from_file = load_source(sources_dir, source, env)
            with_edits = load_source(sources_dir, source, env, overrides)
        except ConfigError as error:
            raise HTTPException(422, str(error)) from error
        chosen = [
            next((item for item in config.datasets if item.name == dataset), None)
            for config in (from_file, with_edits)
        ]
        if chosen[0] is None or chosen[1] is None:
            raise HTTPException(404, f"dataset '{dataset}' is not in source '{source}'")
        return chosen[0], chosen[1], from_file.connection.type

    def values_of(dataset: DatasetBase, fields: tuple[str, ...]) -> dict[str, Any]:
        """The editable fields as validation leaves them, so two spellings of one value match."""
        return dataset.model_dump(mode="json", include=set(fields))

    def config_view(source: str, dataset: str) -> DatasetConfig:
        overrides = catalog.overrides(source)
        from_file, in_force, connector_type = read_config(source, dataset, overrides)
        fields = editable_fields(type(from_file))
        file_values = values_of(from_file, fields)
        effective = values_of(in_force, fields)
        _, columns = catalog.loaded_state(source, dataset)
        return DatasetConfig(
            source=source,
            dataset=dataset,
            connector_type=connector_type,
            editable=list(fields),
            file=file_values,
            effective=effective,
            overridden=[name for name in fields if effective[name] != file_values[name]],
            # The platform's own columns are left out: they cannot be declared, excluded or
            # made a watermark, so offering them would only offer a refusal.
            columns=[
                Column(name=name, type=kind)
                for name, kind in columns
                if name not in RESERVED_COLUMNS
            ],
            checks_yaml=checks_text(effective["checks"]),
            file_checks_yaml=checks_text(file_values["checks"]),
            history=catalog.edits(source, dataset),
        )

    @app.get("/api/datasets/{source}/{dataset}/config")
    def dataset_config(source: str, dataset: str) -> DatasetConfig:
        return config_view(source, dataset)

    @app.put("/api/datasets/{source}/{dataset}/config")
    def save_dataset_config(source: str, dataset: str, update: ConfigUpdate) -> DatasetConfig:
        overrides = catalog.overrides(source)
        from_file, before, _ = read_config(source, dataset, overrides)
        fields = editable_fields(type(from_file))
        # `${NAME}` is filled from the platform's own environment before anything is checked,
        # which is right for a file the operator wrote and wrong for an edit anyone with a key
        # can send: the filled value would come back in the refusal, or be stored in clear.
        if names_a_secret(update.values):
            raise HTTPException(
                422,
                "an edit may not name an environment variable with ${...}; those belong in "
                "source.yaml, which only the operator writes",
            )
        unknown = sorted(set(update.values) - set(fields))
        if unknown:
            raise HTTPException(
                422,
                f"{', '.join(unknown)}: not editable from here (editable: {', '.join(fields)})",
            )
        values = dict(update.values)
        if isinstance(values.get("checks"), str):
            values["checks"] = checks_from_text(values["checks"])
        _, proposed, _ = read_config(source, dataset, {**overrides, dataset: values})
        state, columns = catalog.loaded_state(source, dataset)
        # Only what this edit newly clashes with is worth refusing: a dataset already carrying a
        # clash — its file changed under it — must still be editable in every other field.
        already = set(rebuild_reasons(state, columns, before))
        reasons = [
            reason for reason in rebuild_reasons(state, columns, proposed) if reason not in already
        ]
        if reasons and not update.accept_rebuild:
            # One reason per line, which is how the tab lists them.
            raise HTTPException(409, "\n".join(reasons))
        override, changed = differences(
            values_of(from_file, fields), values_of(proposed, fields), values_of(before, fields)
        )
        if changed:
            catalog.save_override(source, dataset, override, changed, datetime.now(UTC))
            log.info(
                "dataset configuration saved",
                source=source,
                dataset=dataset,
                fields=sorted(changed),
            )
        return config_view(source, dataset)

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

    def run_in_background(source: str, dataset: str | None, full_refresh: bool = False) -> None:
        """The run itself, off the request. The source is read again here, with the edits made
        from its configuration tab, because reading those needs the platform database and the
        request is answered without touching it."""
        try:
            with open_loader() as loader:
                config = load_source(sources_dir, source, env, loader.read_overrides(source))
                if dataset is not None:
                    chosen = [item for item in config.datasets if item.name == dataset]
                    config = config.model_copy(update={"datasets": chosen})
                run_source(
                    source,
                    config,
                    sources_dir,
                    loader,
                    trigger="manual",
                    full_refresh=full_refresh,
                )
        except Exception as error:
            log.error("run started over the API could not run", source=source, error=repr(error))
        finally:
            with app.state.queue:
                app.state.queued -= 1

    def take_a_place() -> bool:
        """Claim one of the queue's places, or say the queue is full."""
        with app.state.queue:
            if app.state.queued >= RUNS_QUEUED:
                return False
            app.state.queued += 1
            return True

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
        datasets = [item.name for item in config.datasets]
        if request.dataset is not None:
            if request.dataset not in datasets:
                raise HTTPException(
                    404, f"dataset '{request.dataset}' is not in source '{request.source}'"
                )
            datasets = [request.dataset]
        if not take_a_place():
            raise HTTPException(429, f"{RUNS_QUEUED} runs are already waiting; try again later")
        requested_at = datetime.now(UTC)
        app.state.runs.submit(
            run_in_background, request.source, request.dataset, request.full_refresh
        )
        log.info(
            "run requested",
            source=request.source,
            dataset=request.dataset,
            full_refresh=request.full_refresh,
        )
        return RunAccepted(
            source=request.source,
            datasets=datasets,
            requested_at=requested_at,
        )

    def give_back_a_place() -> None:
        with app.state.queue:
            app.state.queued -= 1

    def carry_out_in_background(
        resources: ExitStack, loader: Loader, started: StartedPipeline
    ) -> None:
        """The pipeline run's steps, off the request, on the connection that holds its lock."""
        try:
            with resources:
                carry_out(loader, started)
        except Exception as error:
            log.error(
                "pipeline run started over the API could not finish",
                execution_id=str(started.execution_id),
                error=repr(error),
            )
        finally:
            give_back_a_place()

    @app.post("/api/pipelines/{name}/runs", status_code=202)
    def start_pipeline_run(name: str) -> PipelineRunAccepted:
        """Start a run of pipelines/<name>.yaml. It is recorded as running before this answers,
        so its id can be followed at once; a dataset another run holds is refused (409)."""
        if not pipeline_exists(sources_dir, name):
            raise HTTPException(404, f"pipeline '{name}' not found")
        try:
            written = read_pipeline_file(sources_dir, name)
        except ConfigError as error:
            raise HTTPException(422, str(error)) from error
        if not take_a_place():
            raise HTTPException(429, f"{RUNS_QUEUED} runs are already waiting; try again later")
        resources = ExitStack()
        try:
            loader = resources.enter_context(open_loader())
            pipeline = load_pipeline(sources_dir, name, env, loader.read_overrides(written.source))
            started = start_pipeline(loader, pipeline)
            app.state.runs.submit(carry_out_in_background, resources, loader, started)
        except BaseException as error:
            resources.close()
            give_back_a_place()
            if isinstance(error, ConfigError):
                raise HTTPException(422, str(error)) from error
            if isinstance(error, PipelineBusy):
                raise HTTPException(409, str(error)) from error
            raise
        log.info("pipeline run requested", pipeline=name, execution_id=str(started.execution_id))
        return PipelineRunAccepted(
            execution_id=started.execution_id,
            pipeline=name,
            version=started.version.version,
            source=pipeline.source,
            dataset=pipeline.dataset.name,
            requested_at=started.started_at,
        )

    @app.get("/api/pipeline-runs/{execution_id}")
    def pipeline_run(execution_id: UUID) -> PipelineRun:
        """A pipeline run's record: its steps, constraints, profiles and lineage so far."""
        with open_loader() as loader, loader.stages() as stages:
            record = read_run(stages, execution_id)
        return found(record, f"pipeline run {execution_id}")

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
