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

All you need is Docker with Compose. From the project folder:

**1. Choose a database password and an API key.** Neither one is stored in the project:

```
export POSTGRES_PASSWORD=pick-a-password
export UDP_API_KEYS=pick-a-key
```

**2. Start it.** The database, the dashboard and API, and the scheduler:

```
docker compose -f deploy/compose.yaml up -d --build --wait
```

Or start it with the demo in place — the same platform plus a sample business database and a
sample REST API, with all four demo sources already loaded:

```
docker compose -f deploy/compose.yaml --profile demo up -d --build --wait
```

The platform's tables are created (or updated to a newer version) by a one-off `migrate`
container that every other container waits for.

**3. Open the dashboard** at http://127.0.0.1:8000 and enter your API key when it asks for it.
From there you can browse datasets, their columns, previews, quality results and run history,
and use **Run now** on a dataset page to load it again. The **Guide** tab in the bar explains
every page and tab in plain English, and needs no API key to read.

**4. Load your own data.** Put a folder in `sources/` (see below) and run it by name; several
names in one command run one after another:

```
docker compose -f deploy/compose.yaml run --rm app run my_source
```

Datasets that have a `schedule:` in their `source.yaml` are also run by the scheduler on their
own. The four `demo_*` folders shipped in `sources/` schedule nothing, so a start without the
demo profile leaves the scheduler with nothing to do.

To stop everything, keeping the data:

```
docker compose -f deploy/compose.yaml --profile demo down
```

Add `-v` to that command to delete the data as well.

Without containers, the same platform runs from the command line against any PostgreSQL named
by `UDP_DATABASE_URL`:

```
uv run --locked udp migrate                 create or update the platform's tables
uv run --locked udp run demo_csv            load one source now (or several, by name)
uv run --locked udp schedule                run every scheduled dataset until stopped
uv run --locked udp api                     serve the dashboard and the API on 127.0.0.1:8000
uv run --locked udp doctor                  check the database connection
```

## A source in one folder

```
sources/
  demo_csv/
    source.yaml      the connection and its datasets
    transform.py     optional: transform(df, context) -> DataFrame
```

`docs/adding-a-source.md` walks through writing one, field by field.

A dataset's settings can also be changed from its **Configuration** tab in the dashboard. The
file is never written to: the edit is stored in the platform and laid over the file on every
read, so the dataset's next run — by hand, over the API or on its schedule — uses it. A change
that would need the table rebuilt is refused until you confirm it, and the tab then either saves
it for you to rebuild later with `udp run <source> --full-refresh`, or rebuilds and runs on the
spot. An edit may not carry a `${NAME}` reference: those are filled from the platform's own
environment and belong in the file.

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
