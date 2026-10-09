# universal-data-pipeline

A reusable data platform: every source is described in a small configuration file, and the same
pipeline extracts it, checks it, transforms it and loads it into PostgreSQL. Adding a source is a
YAML file and, where the data needs it, one Python function — never a change to the pipeline.

What it does, end to end:

- **Connectors** for CSV files, Excel workbooks, JSON files, database tables and REST APIs.
  Nested JSON — from a JSON file or an API — is stored as `jsonb`, not flattened.
- **One pipeline** per dataset: extract in chunks, check the values, clean the column names,
  apply the declared storage types, run the source's own `transform.py`, check the rows, keep
  only what is new, load it in one transaction.
- **Incremental loading**: `full`, `append` and `merge`, tracked by a watermark column or a
  file's content hash. A second run of unchanged data loads nothing.
- **Data quality**: checks declared per dataset; rows that fail an error-level check are
  quarantined, and a run fails when too many are.
- **Metadata**: every run records what it read, what it wrote, which columns the dataset has,
  and the settings the source was run with.
- **Orchestration**: `./udp load` by hand, or a scheduler container that runs datasets on their
  own cron expressions, with overlapping runs skipped rather than doubled.
- **An API and a dashboard**: nine JSON routes under `/api`, and a React dashboard served by the
  same container for browsing datasets, previews, quality results and run history.
- **Monitoring**: `/api/metrics` in Prometheus's own text format, scraped by a Prometheus
  container on the `monitoring` profile.

## Getting started

All you need is Docker with Compose. On Windows or macOS that means **Docker Desktop must be
running** before any `docker` command below — start it and wait for its whale icon to say the
engine is running, or every command fails with `error during connect` or
`Cannot connect to the Docker daemon`. From the project folder:

**1. Choose a database password and an API key.** Neither one is stored in the project:

```
export POSTGRES_PASSWORD=pick-a-password
export UDP_API_KEYS=pick-a-key
```

**2. Start it.** The database, the dashboard and API, and the scheduler:

```
./udp up
```

Or start it with the demo in place — the same platform plus a sample business database and a
sample REST API, with all five demo sources already loaded:

```
./udp up --demo
```

The platform's tables are created (or updated to a newer version) by a one-off container
running `udp update`, which every other container waits for.

**3. Open the dashboard** at http://127.0.0.1:8000 and enter your API key when it asks for it.
From there you can browse datasets, their columns, previews, quality results and run history,
and use **Run now** on a dataset page to load it again. The **Guide** tab in the bar explains
every page and tab in plain English, and needs no API key to read.

**4. Load your own data.** Put a folder in `sources/` (see below) and run it by name; several
names in one command run one after another:

```
./udp load my_source --docker
```

Datasets that have a `schedule:` in their `source.yaml` are also run by the scheduler on their
own. The four `demo_*` folders shipped in `sources/` schedule nothing, so a start without the
demo profile leaves the scheduler with nothing to do.

To stop everything, keeping the data:

```
./udp down
```

To delete the data as well, run
`docker compose -f deploy/compose.yaml --profile demo down -v` instead.

Without containers, the same platform runs from the command line against any PostgreSQL named
by `UDP_DATABASE_URL` (it needs [uv](https://docs.astral.sh/uv/)):

```
./udp update                    create or update the platform's tables
./udp load demo_csv             load one source now (or several, by name)
./udp profile shop orders --stage raw
                                profile a dataset's RAW, STAGING or CLEAN table, print it and
                                store it (docs/stages-and-pipelines.md)
./udp pipeline run demo_csv_customers
                                run pipelines/demo_csv_customers.yaml: RAW through its steps
                                to CLEAN (docs/stages-and-pipelines.md)
./udp pipeline status <run id>  print a pipeline run's record
./udp schedule                  run every scheduled dataset until stopped
./udp api                       serve the dashboard and the API on 127.0.0.1:8000
./udp doctor                    check the database connection
./udp openapi                   print the API's description as JSON
```

## Run without cloning

The published image runs the same platform with no source code. In an empty folder, with
Docker running:

```
mkdir sources
curl -fsSLO https://raw.githubusercontent.com/williammgb/universal-data-pipeline/main/deploy/compose.release.yaml
export POSTGRES_PASSWORD=pick-a-password
export UDP_API_KEYS=pick-a-key
docker compose -f compose.release.yaml up -d --wait
```

Then open http://127.0.0.1:8000. Your sources go in `sources/` next to the file, and
`docker compose -f compose.release.yaml run --rm app load my_source` loads one. It runs the
`latest` release; to pin one, set `UDP_VERSION` first, as in `export UDP_VERSION=1.2.0`.

## From raw data to a clean table

Loading a source only copies it in. Preparing it for use is a second, separate step — a
**pipeline** — so the data as it arrived is always kept, and a mistake in the preparation never
destroys it. The whole way, using the messy CSV demo as the example:

1. **Load.** `./udp load messy_csv` reads the file into the dataset's **RAW** table. RAW is only
   ever added to: every load's rows stay there, marked with the load that brought them.
2. **Profile.** `./udp profile messy_csv orders --stage raw` counts, per column, the missing
   values, the values that cannot be read as their type, the outliers and the duplicates. The
   dataset's **Profile** tab in the dashboard shows the same.
3. **Constrain.** Write down what the clean data must satisfy — a column never empty, values
   from a fixed list, no number below zero, no key twice. Each rule can be **critical**: when a
   critical rule is broken, the result is not published. `docs/constraints.md` lists them.
4. **Build a pipeline.** A pipeline names the dataset, its steps in order — spellings made one,
   unreadable rows dropped, types set, outliers capped, gaps filled — and its constraints. It is
   a file, `pipelines/messy_csv_orders.yaml`, or it is built step by step on the dashboard's
   **Pipeline** page. Every change saved is a new version, and each run records which one ran.
5. **Run it.** `./udp pipeline run messy_csv_orders`, or **Run** on the Pipeline page. The run
   reads RAW, applies each step, checks the constraints and, only if no critical one failed,
   replaces the dataset's **CLEAN** table in one go. It records every step's rows in and out
   and values changed, and every constraint's outcome. A run that fails names the step that
   failed and why, and leaves RAW and the previous CLEAN table exactly as they were.
6. **Read its lineage.** The dataset's **Lineage** tab — or the `lineage` part of
   `./udp pipeline status <run id> --json` — shows the chain the data took: the file it was read from, the RAW table, each step with its
   settings and counts, and the CLEAN table.

Five demo sources show this on data that is wrong in the ways real data is: `messy_csv`,
`messy_excel`, `messy_db`, `messy_api` and `messy_json`, each with a pipeline of the same name
in `pipelines/`. Their files describe the problems built into them; two of each pipeline's
constraints — a key repeated, a number below zero — fail on purpose, so the run reports them
rather than hiding them. `docs/stages-and-pipelines.md` has the details, and
`docs/performance.md` how long a pipeline takes on a million rows.

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
it for you to rebuild later with `./udp load <source> --full-refresh`, or rebuilds and runs on the
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

## What it looks like

The datasets list: every dataset, its source type, how many rows its table holds, when it next
runs and how its last run went.

![The datasets list in the dashboard](assets/v1_datasets.png)

A dataset's Profile tab: per column, how many values are missing, how many are distinct, the
range, the spread, and the most and least used values.

![A dataset's Profile tab in the dashboard](assets/v1_dataprofiling.png)
