# Guide

Everything past starting the platform and loading a first source. The dashboard's **Guide** tab
explains every page and tab in it.

## What it does

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
- **Orchestration**: loads by hand, or a scheduler container that runs datasets on their own
  cron expressions, with overlapping runs skipped rather than doubled. Schedules are read when
  the scheduler starts, so restart it after adding one.
- **An API and a dashboard**: JSON routes under `/api`, and a React dashboard served by the same
  container for browsing datasets, previews, quality results and run history.
- **Monitoring**: `/api/metrics` in Prometheus's own text format, scraped by a Prometheus
  container on the `monitoring` profile.

## Running from the source code

From a clone of the repository, with Docker running and `POSTGRES_PASSWORD` and `UDP_API_KEYS`
set, `./udp up` builds the image and starts the database, the dashboard and API, and the
scheduler. The platform's tables are created (or updated to a newer version) by a one-off
container running `udp update`, which every other container waits for.

```
./udp up                      start the platform
./udp up --demo               the same, plus a sample database and REST API, with all five
                              demo sources loaded
./udp load my_source --docker load a source in the running stack (several names run in turn)
./udp down                    stop everything, keeping the data
```

To delete the data as well, run `docker compose -f deploy/compose.yaml --profile demo down -v`.

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
   `./udp pipeline status <run id> --json` — shows the chain the data took: the file it was read
   from, the RAW table, each step with its settings and counts, and the CLEAN table.

Five demo sources show this on data that is wrong in the ways real data is: `messy_csv`,
`messy_excel`, `messy_db`, `messy_api` and `messy_json`, each with a pipeline of the same name
in `pipelines/`. Their files describe the problems built into them; two of each pipeline's
constraints — a key repeated, a number below zero — fail on purpose, so the run reports them
rather than hiding them. `docs/stages-and-pipelines.md` has the details, and
`docs/performance.md` how long a pipeline takes on a million rows.

## Changing a source from the dashboard

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
| `POSTGRES_PASSWORD` | the platform database's password; required by both compose files |
| `UDP_API_KEYS` | comma-separated API keys; empty means the API is open, and it says so at startup. A key may not contain a comma or begin or end with a space — the separator and the trimming would eat it. |
| `UDP_VERSION` | the release `compose.release.yaml` runs, e.g. `2.0.0` (default `latest`) |
| `UDP_API_PORT`, `UDP_PG_PORT` | the ports the dashboard and the database are published on (defaults 8000 and 5432) |
| `UDP_DATABASE_URL` | without containers: the platform database, e.g. `postgresql://udp:...@127.0.0.1:5432/udp` |
| `UDP_SOURCES_DIR` | without containers: where the source folders live (default `sources`) |

A source's own secrets are never written in its YAML: it holds `${NAME}` references, filled from
the environment when the source is read, and the copy stored in the database keeps them unfilled.

## Backup and restore

The platform's whole state is in PostgreSQL, so one dump is the backup. With
`compose.release.yaml`, put `-f compose.release.yaml` in place of `-f deploy/compose.yaml`:

```
docker compose -f deploy/compose.yaml exec postgres pg_dump -U udp -Fc -f /tmp/udp.dump udp
docker compose -f deploy/compose.yaml exec postgres \
  psql -U udp -d postgres -c 'CREATE DATABASE udp_restored OWNER udp'
docker compose -f deploy/compose.yaml exec postgres pg_restore -U udp -d udp_restored /tmp/udp.dump
```
