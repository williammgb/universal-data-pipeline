# Adding a source

A source is a folder under `sources/`. It holds one `source.yaml` — the connection and the
datasets read through it — and, when the data needs shaping, a `transform.py`. Nothing under
`src/udp/pipeline/` changes when a source is added; if it seems to, something is wrong.

## The shortest possible source

```
sources/
  my_shop/
    source.yaml
    data/customers.csv
```

```yaml
connection:
  type: csv
datasets:
  - name: customers
    path: data/customers.csv
    load_mode: full
```

Then:

```
uv run --locked udp run my_shop
```

That reads the file, cleans the column names (`Customer ID` becomes `customer_id`), works out a
type per column, and replaces the table `datasets.my_shop__customers`. The run is recorded in
`platform.pipeline_runs`, and the dataset appears in the dashboard.

## The connection

One connection per source, named by `type`. Secrets are never written here: use `${NAME}` and
keep the value in an uncommitted `.env` or in the environment.

```yaml
# a folder of files
connection:
  type: csv          # or: excel

# a database, any SQLAlchemy URL (tested against PostgreSQL and SQLite)
connection:
  type: database
  url: ${SHOP_DB_URL}

# an HTTP API
connection:
  type: rest_api
  base_url: ${SHOP_API_URL}
  auth:
    type: bearer     # or: none, api_key
    token: ${SHOP_API_TOKEN}
```

## The datasets

Each dataset becomes one table, `datasets.<source>__<dataset>`. What a dataset needs depends on
its connector:

| Connector | Names the data with | Also takes |
| --- | --- | --- |
| `csv` | `path:` relative to the source folder | — |
| `excel` | `path:` plus `sheet:` | — |
| `database` | `table:` | — |
| `rest_api` | `endpoint:` and `records_path:` | `params:`, `pagination:` (`page`, `offset`, `cursor`, `next_link`) |

Every dataset also takes:

```yaml
    load_mode: merge          # full (replace), append, or merge
    watermark: updated_at     # required for append and merge: only newer rows are read
    primary_key: [id]         # required for merge: what a row is updated by
    schedule: "*/15 * * * *"  # optional: five cron fields, in UTC
    quarantine_threshold_percent: 1
```

`full` replaces the table every run. `append` adds rows past the saved watermark. `merge` updates
rows that changed and adds new ones. A file that has not changed since the last run (same path,
same contents) is skipped whatever the mode, and its table checks still run.

## Declaring column types

Without `columns:`, the types are worked out from the data on the first run and recorded as
schema version 1. With `columns:`, the storage type is yours:

```yaml
    columns:
      customer_id: integer
      signup_date: date
      lifetime_value: decimal(12,2)
      is_active: boolean
```

The types are `text`, `integer`, `decimal(P,S)`, `float`, `boolean`, `date`, `timestamp` and
`json`. A value that does not fit its declared type is quarantined with the reason, rather than
quietly becoming null. Timestamps are stored with a time zone, in UTC.

## Checks

Checks run on every run, and each is `warn` (recorded) or `error` (the default — the row is
quarantined, or the run fails for a table-level check):

```yaml
    checks:
      - check: not_null
        column: customer_id
      - check: unique
        columns: [customer_id]
      - check: accepted_values
        column: status
        values: [new, paid, shipped]
      - check: range
        column: lifetime_value
        min: 0
      - check: regex
        column: city
        pattern: "[A-Z][a-z]+"
        severity: warn
      - check: min_rows
        rows: 1
      - check: freshness
        column: updated_at
        max_age: PT24H          # a duration: PT24H is 24 hours, P7D is seven days
      - check: row_count_change
        max_percent: 50
```

Results are written to `platform.quality_results` and shown on the dataset's Quality tab.

## A transform of your own

When a dataset needs something the common transforms do not do, add `transform.py` beside
`source.yaml`. It is applied to every dataset of that source, one chunk at a time:

```python
import polars as pl

from udp.pipeline.custom import TransformContext


def transform(df: pl.DataFrame, context: TransformContext) -> pl.DataFrame:
    if context.dataset != "products":
        return df
    return df.with_columns((pl.col("price") * pl.col("stock")).alias("stock_value_eur"))
```

The rules the platform holds you to: return a `polars.DataFrame`, keep the column names clean
(lowercase, snake_case) and the types storable, never write the platform's own columns
(`_run_id`, `_loaded_at`, `_record_hash`), and give every chunk the same schema. A transform that
breaks one of those fails the run, naming the file. A transform that runs longer than fifteen
minutes fails the run too, because it holds its dataset's lock while it works.

The file is read, hashed and compiled — never imported — so the hash recorded with the run always
describes the code that ran, and editing it makes the next run reload everything it affects.

## Running it

```
uv run --locked udp run my_shop                  every dataset of the source
uv run --locked udp run my_shop --full-refresh   forget the saved state and reload
```

Exit codes: 0 when every dataset succeeded or was skipped, 1 when a run failed, 2 when the
configuration is invalid — with the file and field named.

To have it run by itself, give the dataset a `schedule:` and start the scheduler:

```
docker compose -f deploy/compose.yaml --profile app up -d scheduler
```

## Seeing it

With the API up (`udp api`, or the `api` container), the dashboard lists the dataset, its
columns, a preview of its rows, its quality results and its run history. If the API has keys
configured (`UDP_API_KEYS`), the dashboard asks for one the first time it is refused.

## When something goes wrong

- `run failed` with `ConfigError`: the YAML names the file and field that is wrong.
- `ExtractError`: the source could not be read — a missing file, a refused connection.
- `SchemaDriftError`: a column's type changed under the platform; nothing is loaded.
- `QualityError`: too many rows were quarantined, or an error-level table check failed.

Every one of those is recorded on the run, with the message and traceback, and shown on the
run's page in the dashboard.
