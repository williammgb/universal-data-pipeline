# Universal Data Platform

## Goal
A Dockerized data platform where a new CSV/Excel, database or REST API source is added with one `source.yaml` (plus an optional `transform.py`), loaded incrementally into its own PostgreSQL table, run on a schedule or by hand, tracked with run/quality metadata, and browsable in a React dashboard — without changing the core pipeline.

## Out of scope
- Live streaming; reading database change logs (change data capture)
- Detecting rows deleted at the source in `append`/`merge` mode (only `full` mode reflects deletes)
- OAuth sign-in for API sources; cloud storage (S3 etc.) sources
- Cloud deployment, Kubernetes; user logins / multi-user
- dbt or a SQL transformation layer; lineage graphs; charts beyond simple previews
- Custom connector *types* defined inside a source folder (new types go in `src/udp/connectors/`)

## Environment
Measured by `env-doctor` on 2026-09-13.
- OS: Windows 11 Home (build 10.0.26200), 12 logical cores, 15.8 GB RAM (~7.6 GB free), 372 GB free on C:
- WSL2: Ubuntu, default distro, `networkingMode=mirrored` in `.wslconfig`, 7.7 GiB RAM cap
- Python: 3.14.3 (Windows) / 3.14.4 (WSL) is the only interpreter on either side
- uv 0.12.5 on Windows and WSL; no pip/pipx in WSL; no mise, no pnpm anywhere
- Node v24.18.0 + npm 11.16.0 on Windows only; WSL has no Node
- Docker: Docker Desktop 29.6.1 installed but NOT running; native docker-ce 29.6.0 running inside WSL Ubuntu (also hosts 4 unrelated `clab-*` containers). A port published by the WSL engine is reachable from Windows and WSL via localhost (proven)
- No local Postgres; port 5432 free
- git 2.54.0, gh 2.98.0 logged in as `williammgb` with `repo` + `workflow` scopes
- PyPI, npm registry, Docker Hub, GitHub reachable
- runs on: code and tests on native Windows (uv Python); services in the WSL docker-ce engine, called as `wsl docker compose`
- pinned by: `.python-version` + `uv.lock` (Python); `engines` + `engine-strict` + `package-lock.json` (Node, from slice 8); exact Docker image tags
- known blocker: `docker` on Windows points at stopped Docker Desktop — worked around by `./run` calling `wsl docker ...`; never start Docker Desktop for this project
- known blocker: the WSL distro shuts down ~15s after the last `wsl.exe` command ends, stopping every container (measured in slice 0) — worked around by `./run` holding one idle `wsl.exe` session open for the life of a stack
- known blocker: from Windows, `localhost` to a port published in WSL tries IPv6 first and hangs ~130s (measured in slice 0) — worked around by always using `127.0.0.1`
- known blocker: mutmut 3 needs `fork` — worked around by running mutation tests inside WSL only

## Stack
- language: Python 3.14 via uv
- pipeline core: own code (NOT dlt / Airbyte / Meltano)
- dataframes + custom transform contract: Polars (`polars.DataFrame` in, `polars.DataFrame` out)
- Excel: fastexcel (Polars default); openpyxl fallback only for files fastexcel cannot read
- central store: PostgreSQL 18, Docker, exact tag pinned
- database access: psycopg 3 (COPY) + SQLAlchemy Core 2 (source databases) + Alembic (platform tables only)
- HTTP client: httpx
- config: Pydantic 2 + pydantic-settings + PyYAML
- CLI: Typer, command `udp`, package `src/udp`
- scheduler: APScheduler 3.11 (NOT 4.x alpha) in a `scheduler` container
- API: FastAPI + Uvicorn
- logging: structlog, JSON lines to stdout
- tests/checks: pytest, Hypothesis, ruff, mypy
- dashboard: React + Vite + TypeScript + TanStack Query (details fixed in slice 8 after HTML preview)
- containers: Docker Compose on the WSL docker-ce engine
- CI: GitHub Actions

## Decisions
- repository: own git repo in `universal-data-pipeline/`; folder added to Maxxing's `.gitignore`
- layout: root `pyproject.toml`; `src/udp/{config,connectors,pipeline,storage,metadata,quality,orchestration,api,cli}`; `tests/`; `migrations/`; `sources/<name>/`; `deploy/` (compose + Dockerfiles); `fixtures/`; `frontend/` (slice 8); `.github/workflows/`
- pipeline order per dataset: extract (chunks) → validate → common transforms → custom transform → output contract check → load → quality checks
- connector interface: every connector type yields Polars DataFrame chunks of up to 100,000 rows given dataset config + saved state; registered by `type:` key in config
- source definition: `sources/<name>/source.yaml` = one connection + list of datasets; optional `sources/<name>/transform.py` exposing `transform(df, context) -> df`, found by folder convention
- secrets: only `${ENV_VAR}` references in YAML, filled from uncommitted `.env`; missing secret fails before extraction
- config truth: files; copied into `platform.sources` / `platform.datasets` every run
- column types: inferred on first run, recorded as schema version; optional `columns:` overrides in YAML
- Postgres layout: schema `datasets` → one table per dataset named `<source>__<dataset>`; schema `platform` → sources, datasets, schema_versions, pipeline_runs, source_state, quarantine, quality_results
- names: lowercase snake_case, validated against Postgres 63-char identifier limit
- added columns on every dataset row: `_run_id`, `_loaded_at`, `_record_hash`
- load modes per dataset: `full` (replace table), `append` (watermark column), `merge` (primary key + watermark column); files tracked by path + content hash
- re-run with no new source data changes nothing; `--full-refresh` resets state and reloads
- transactions: chunks COPY into temp table; merge/replace + watermark update + run stats commit in ONE transaction per run
- concurrency: Postgres advisory lock per dataset; overlapping run recorded as `skipped`
- retries: transient extract errors (network, connection) retried 3× with increasing wait; failed runs not auto-retried
- errors: typed exceptions (ConfigError, ExtractError, ValidationError, SchemaDriftError, LoadError) caught only at the runner boundary → run marked `failed` with class, message, traceback; CLI exits non-zero; never swallowed
- column changes: new column added + recorded; removed column kept (nulls); type change fails the run
- bad records: to `platform.quarantine` (original row as JSON, reason, run id); run fails above per-dataset threshold, default 1%
- scale target: ≤ 5,000,000 rows (~2 GB) per dataset per run; 100,000-row chunks; pipeline memory under ~1 GB
- source databases: any SQLAlchemy URL; tested against Postgres and SQLite only
- API sources: pagination none | page | offset | cursor field | next link; auth none | API-key header | bearer token (from env)
- dashboard reads the API only, never Postgres directly
- API auth: none, bound to localhost until slice 10
- demo sources for gates: generated files, a second "source" Postgres container, a local mock API container — never public internet in a gate

## Overturned defaults
- none — all four escalated recommendations accepted (core, transforms, scheduler, where code runs); ledger items 01–15 taken as proposed

## Constraints
- adding a connector type, source or transformation must not change files under `src/udp/pipeline/`
- a dataset table never holds a partial run; the watermark never disagrees with the table
- all commands go through `./run`; `docker` is always `wsl docker`
- no secrets in YAML, source or committed config
- UI: publish a static HTML preview Artifact and get approval before building the dashboard

## Verification
Fast gate: ./run fast     # after every edit, no services, target <30s — ruff check + ruff format --check + mypy + pytest -m "not db" (unit, fixtures, fake API, in-memory fake loader, Hypothesis dev profile)
DB tests:  ./run db       # slices touching load/merge/lock SQL — pytest -m db against running compose Postgres
Full gate: ./run full     # once per slice, background — compose up postgres + source-postgres + mock-api; alembic upgrade; all tests (Hypothesis ci profile); end-to-end: 1M-row CSV, 50k-row xlsx, 500k-row source table, 20-page mock API, each loaded twice (2nd loads 0 rows) then with changed data (only changes load); build app images; compose down
Smoke:     ./run smoke    # compose up full stack; wait for API health; run demo source; confirm run `succeeded`; from slice 8 open dashboard with browser tool, zero console errors
Properties: common transforms idempotent; column-name cleanup → valid unique Postgres names for any input; watermark never moves backwards for any batch order; good + quarantined rows = input rows; concatenated chunks = whole file for any chunk size; API pagination always terminates; (full gate) loading same data twice leaves table identical
Mutation:  mutmut inside WSL, once per milestone (after slices 3, 5, 7), one module (e.g. incremental state) — never in a gate

## Skeleton (slice 0) — done means
- [ ] own git repo; folder ignored by Maxxing repo; `.gitignore` keeps only `SPEC.md`, `slices.json`, `JOURNAL.md` from `.work/`
- [ ] `./run setup` = `uv sync --frozen` installs Python 3.14 env with the full approved dependency list (proves 3.14 wheels exist)
- [ ] `uv run udp --version` prints the version
- [ ] `wsl docker compose` starts Postgres 18; `udp doctor` connects from Windows and prints the server version
- [ ] one fast test passes; one `db` test passes in the full gate
- [ ] GitHub Actions workflow runs the fast gate on push
- [ ] both gates run, real durations recorded here

Measured in slice 0, warm (second run): fast 5s (tests 1.7s); full 15s with the app image cached, ~2 min cold (first image build and Postgres pull); smoke 38s. First pytest run after a fresh install takes ~90s while bytecode compiles.

Proved by: both gates above

Measured in slice 1: fast ~20–34s (69 tests), db ~45s, full ~165–225s (million-row CSV loaded twice, Hypothesis ci profile), smoke ~35–55s.

## Slice 1 — MVP: one CSV source, config-driven — done means
- [ ] `sources/demo_csv/source.yaml` + `udp run demo_csv` loads every CSV row into `datasets.demo_csv__<dataset>` with `_run_id`, `_loaded_at`, `_record_hash`
- [ ] `platform.pipeline_runs` row: status `succeeded`, trigger `manual`, rows extracted/loaded, start/end times
- [ ] missing CSV file → run row `failed` with error message; command exits non-zero
- [ ] invalid `source.yaml` fails before extraction, naming file and field
- [ ] second `full` run leaves same row count
- [ ] CSV connector implements the connector interface; pipeline steps extract → validate → transform → load exist as separate stages
- [ ] log lines are JSON and carry `run_id`, `source`, `dataset`, `step`

Proved by: both gates, plus the smoke launch

## Later slices — done means
- 2 connectors: Excel, database table and REST API sources each load via YAML only; no file under `src/udp/pipeline/` changed; API pagination and auth modes above covered by mock-API tests
- 3 incremental: `append` and `merge` load only new/changed rows; unchanged files skipped by hash; 2nd run loads 0 rows; `--full-refresh` reloads; a run killed mid-load leaves table and watermark unchanged; new column added; type change fails
- 4 transformations: common transforms (trim text, snake_case column names, empty string → null) run on every dataset; `transform.py` plug-in applied; broken transform fails run naming the file
- 5 metadata + quality: schema versions recorded per dataset; quarantine + threshold works; YAML checks (not_null, unique, accepted_values, range, regex, min_rows, freshness, row-count change) write `platform.quality_results` with severity warn|error
- 6 orchestration: scheduler container runs datasets on their YAML cron; overlapping run recorded `skipped`; manual and scheduled runs use the same runner; trigger recorded
- 7 API: list/search sources and datasets; dataset schema + metadata; paginated row preview; runs list with filters; run detail with errors and stats; quality results; POST trigger run; health endpoint; OpenAPI docs
- 8 dashboard: HTML preview approved; pages for dataset discovery, dataset detail (schema, metadata, preview, quality), pipeline runs; smoke shows zero console errors
- 9 monitoring + CI/CD: metrics exposed and viewable; CI runs full gate on pull requests and builds images
- 10 hardening: API key auth; database backup + restore proven; resource limits in compose; 5M-row load inside memory target; README + "add a source" guide

## Open
- GitHub remote: create private repo `williammgb/universal-data-pipeline` and push? — needed by slice 0 (CI check)
- real sources the user wants beyond the demo ones — needed by slice 2
- monitoring stack (Prometheus + Grafana vs. logs-only) — needed by slice 9
- CI image registry (GitHub Container Registry or none) — needed by slice 9
- dashboard styling library — needed by slice 8, decided with the preview
