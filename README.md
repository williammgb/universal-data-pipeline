# universal-data-pipeline

A reusable data platform: every source is described in a small configuration file, and the same
pipeline extracts it, checks it, transforms it and loads it into PostgreSQL. Adding a source is a
YAML file and, where the data needs it, one Python function — never a change to the pipeline.

What it does, end to end:

- **Connectors** for CSV files, Excel workbooks, database tables and REST APIs.
- **One pipeline** per dataset: extract in chunks, check the values, clean the column names,
  apply the declared storage types, run the source's own `transform.py`, check the rows, keep
  only what is new, load it in one transaction.
- **Incremental loading**: `full`, `append` and `merge`, tracked by a watermark column or a
  file's content hash. A second run of unchanged data loads nothing.
- **Data quality**: checks declared per dataset; rows that fail an error-level check are
  quarantined, and a run fails when too many are.
- **Metadata**: every run records what it read, what it wrote, which columns the dataset has,
  and the settings the source was run with.
- **Orchestration**: `udp run` by hand, or a scheduler container that runs datasets on their
  own cron expressions, with overlapping runs skipped rather than doubled.
- **An API and a dashboard**: nine JSON routes under `/api`, and a React dashboard served by the
  same container for browsing datasets, previews, quality results and run history.
- **Monitoring**: `/api/metrics` in Prometheus's own text format, scraped by a Prometheus
  container on the `monitoring` profile.

## Getting started

Everything goes through `./run`, so there is one command to learn and one thing to allow:

```
./run setup           install the pinned toolchain and the locked dependencies
./run fast            the fast gate: lint, types, the dashboard's checks and the unit tests
./run db              the tests that need a real PostgreSQL
./run full            the full gate at real size, including million-row loads
./run smoke           bring the whole stack up and prove it works, browser and all
./run memory          load five million rows in a container and print its peak memory
./run image <tag>     build the container image
./run types           regenerate the dashboard's types from the API's description
./run check <file>    lint one file
```

To run the platform itself:

```
uv run --locked udp run demo_csv        load one source now
uv run --locked udp schedule            run every scheduled dataset until stopped
uv run --locked udp api                 serve the dashboard and the API on 127.0.0.1:8000
uv run --locked udp doctor              check the database connection
```

In containers, `deploy/compose.yaml` has the platform database, the `api` and `scheduler`
services, the demo source services (`demo` profile) and Prometheus (`monitoring` profile).

## A source in one folder

```
sources/
  demo_csv/
    source.yaml      the connection and its datasets
    transform.py     optional: transform(df, context) -> DataFrame
```

`docs/adding-a-source.md` walks through writing one, field by field.

## Settings

Read from the environment, or from an uncommitted `.env` beside the project:

| Name | What it does |
| --- | --- |
| `UDP_DATABASE_URL` | the platform database, e.g. `postgresql://udp:...@127.0.0.1:5432/udp` |
| `UDP_SOURCES_DIR` | where the source folders live (default `sources`) |
| `UDP_API_KEYS` | comma-separated API keys; empty means the API is open, and it says so at startup. A key may not contain a comma or begin or end with a space — the separator and the trimming would eat it. |

A source's own secrets are never written in its YAML: it holds `${NAME}` references, filled from
the environment when the source is read, and the copy stored in the database keeps them unfilled.

## Backup and restore

The platform's whole state is in PostgreSQL, so one dump is the backup:

```
docker compose -f deploy/compose.yaml exec postgres pg_dump -U udp -Fc -f /tmp/udp.dump udp
docker compose -f deploy/compose.yaml exec postgres \
  psql -U udp -d postgres -c 'CREATE DATABASE udp_restored OWNER udp'
docker compose -f deploy/compose.yaml exec postgres pg_restore -U udp -d udp_restored /tmp/udp.dump
```

`./run smoke` does exactly this every time, compares the restored copy with the original, and
then runs a dataset against the restored database to prove the saved state came back with it.

## What it needs to run

Measured on this project: a five-million-row CSV loads in a container that peaks at **946 MiB**,
inside the 1 GiB target. Every compose service carries a cpu and memory limit, so one runaway
container cannot take the machine down with it; `deploy/compose.yaml` holds the numbers.

## How it is checked

- the fast gate after every change: lint, strict typing, the dashboard's own tests, and the
  Python tests including property tests over generated inputs;
- the full gate once per slice, at real size (a million-row CSV, a 500,000-row source table, a
  50,000-row spreadsheet, a paged API) — also run on every pull request;
- the smoke gate: the real containers, a real browser over every dashboard page, Prometheus
  scraping the real API, and a backup restored and run against.
