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

References work in every field of this file, not only the connection. A missing `${NAME}` fails
the source loudly; write `${NAME:-fallback}` where the setting is allowed to be absent, and the
fallback is used instead. The fallback may be empty — a dataset's `schedule: "${MY_TIME:-}"` is
scheduled where that setting exists and unscheduled everywhere else. A fallback is plain text:
it cannot hold another `${...}`, and saying so is refused rather than half-read.

```yaml
# a folder of files
connection:
  type: csv          # or: excel, json

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
| `json` | `path:` to a file or a folder | `records_path:` (where each document keeps its list of records) |
| `database` | `table:` | `schema:`, `exclude_columns:` (source column names never read) |
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

An empty CSV or JSON file has no rows. A dataset that declares its `columns:` loads none — a
`full` load then empties its table — and one that does not stops with "the source has no
columns", because there is nothing to learn them from. A CSV file with only its header line
loads no rows either, with the header's columns.

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
quietly becoming null. Timestamps are stored with a time zone, in UTC. A `json` column is stored
as PostgreSQL `jsonb`, and text in it must be valid JSON.

Numbers with too many decimals for `decimal(P,S)` depend on where they came from. Spreadsheets
and JSON store numbers as binary floats, so 731.94 arrives as 731.9399999999999; those are
rounded half away from zero to the scale. Text is never rounded: "12.345" in a CSV column
declared `decimal(12,2)` is quarantined, because someone wrote those three decimals.

A database table brings its own types. `numeric(10,2)` is read as exactly that, so it needs no
declaration; unbounded `numeric` arrives as text until you declare it. A domain (a named type
with a rule, like Pagila's `year`) is read as the type underneath; enums as text; arrays as JSON
text. Binary columns cannot be read — list them under `exclude_columns:`. Any other type is read
as its text, and the run logs a `column read as text` warning naming the column.

## JSON files

A `json` dataset reads one file, or every `.json`, `.jsonl` and `.ndjson` file in a folder (in
name order, not looking in folders inside it). Each file is either one JSON document or
newline-delimited JSON, one document per line; blank lines are skipped. A document is a list of
objects, one row each, or a single object, which is one row.

```yaml
connection:
  type: json
datasets:
  # {"meta": {...}, "data": {"items": [{"id": 1, ...}, ...]}}
  - name: orders
    path: data/orders.json
    records_path: data.items
    load_mode: merge
    watermark: updated_at
    primary_key: [id]
  # a folder of files with one event per line
  - name: events
    path: data/events
    load_mode: append
    watermark: seq
```

`records_path` is the list's place in each document, as object keys joined by dots; without it
the document itself is the list. A document without that path, or whose records are not
objects, fails the run and names the file and line.

Every top-level key of any record is a column. Records may differ in shape: a key one record
lacks is null there, and each column's type is worked out over every record of the dataset, not
only the first ones. A column that holds an object or an array anywhere becomes `jsonb`, and
keeps each value as the JSON it was — nothing is flattened. Declare such a column `text` to keep
it as JSON text instead, or declare a text column `json` to store it as `jsonb`. The same holds
for nested values from a `rest_api` source.

Every file is read before anything is loaded, so a malformed file — broken JSON, `NaN`, text
that is not UTF-8 — fails the run with its file, line and column, and nothing of that run is
kept. An empty file or an empty list has no rows: a dataset that declares its `columns:` loads
none, and one that does not stops with "the source has no columns", because there is nothing to
learn them from. For the unchanged-file skip, a folder counts as changed when any of its JSON
files is added, removed, renamed or edited.

The dashboard shows a `jsonb` value on one line, or indented over several when values are shown
in full; the profile counts its missing values and does not profile the values themselves.

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
uv run --locked udp run my_shop other_shop       several sources, one after another
uv run --locked udp run my_shop --full-refresh   forget the saved state and reload
```

Exit codes: 0 when every dataset succeeded or was skipped, 1 when a run failed, 2 when the
configuration is invalid — with the file and field named.

To have it run by itself, give the dataset a `schedule:` and start the scheduler:

```
docker compose -f deploy/compose.yaml up -d scheduler
```

## Seeing it

With the API up (`udp api`, or the `api` container), the dashboard lists the dataset, its
columns, a preview of its rows, its quality results and its run history. If the API has keys
configured (`UDP_API_KEYS`), the dashboard asks for one the first time it is refused.

## Changing it without editing the file

The dataset's **Configuration** tab edits the settings on this page — the schedule, the load
mode, the watermark and primary key, the declared column types, the checks, the quarantine
threshold, and `exclude_columns:` for a database source. The file is never written to: an edit
is stored in the platform database and laid over the file every time the source is read, so the
next run of that dataset uses it, whether it is started by hand, by the API or by the scheduler.

Two things follow from that:

- An edit is checked exactly as the same lines in the file would be, and refused with the same
  message.
- A change that would need the table rebuilt — the load mode, the watermark, the primary key, or
  a declared type the table contradicts — is refused once, with its reasons. You then either
  save it anyway, and rebuild when you choose with `udp run <source> --full-refresh`, or use
  **Rebuild and run now**, which saves the change and starts that rebuild in one step.
- An edit may not carry a `${NAME}` reference. Those are filled from the platform's own
  environment, so they belong in `source.yaml`, which only whoever runs the platform writes.

Every field shows whether it differs from the file and offers the file's value back, and the
tab's **Changes** list holds every save, newest first.

## When something goes wrong

- `run failed` with `ConfigError`: the YAML names the file and field that is wrong.
- `ExtractError`: the source could not be read — a missing file, a refused connection.
- `SchemaDriftError`: a column's type changed under the platform; nothing is loaded.
- `QualityError`: too many rows were quarantined, or an error-level table check failed.

Every one of those is recorded on the run, with the message and traceback, and shown on the
run's page in the dashboard.
