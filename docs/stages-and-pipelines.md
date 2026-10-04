# Stages, pipelines and what they record

A version 1 source loads each dataset into one table, `datasets.<source>__<dataset>`, and that
is still exactly what happens to a source without a pipeline. A pipeline adds three more copies
of a dataset, one per **stage**, and a set of tables recording what each run of it did.

```mermaid
flowchart LR
    S["Source<br/>a file, a database table or an API"]
    R["RAW<br/>raw.shop__orders"]
    G["STAGING<br/>staging.shop__orders"]
    C["CLEAN<br/>clean.shop__orders"]
    S -- "ingest run" --> R
    R -- "copied when a pipeline run starts" --> G
    G -- "each step changes it in turn" --> G
    G -- "becomes CLEAN when the run succeeds" --> C
```

## The three stages

| Stage | What it holds | How long it is kept |
|---|---|---|
| **RAW** | Exactly what was ingested, every ingest run's rows, each row tagged with the run that brought it (`_run_id`). | Only ever added to. Kept until the dataset is deleted. |
| **STAGING** | The working copy one pipeline run transforms, step by step. | Dropped when that run ends, whether it succeeded or failed. |
| **CLEAN** | The result of the last pipeline run that succeeded: the copy anything downstream reads. | Replaced by each successful run. A failed run leaves it as it was. |

**RAW cannot be changed.** Every RAW table is created with a database trigger that refuses any
`UPDATE`, `DELETE` or `TRUNCATE`, whoever sends it — so a pipeline, a bug or a person at a SQL
prompt gets an error saying the table is append-only. New rows and new columns can still be
added by later ingest runs; a column cannot change its type, because that would change what was
ingested.

Because RAW keeps every ingest, a dataset loaded in full every day keeps every day's copy. That
is the price of being able to rerun a pipeline over exactly what came in. STAGING and CLEAN hold
one copy each, and STAGING only while a run is going.

## The naming rule

Each stage is a PostgreSQL schema of its own — `raw`, `staging` and `clean` — and holds one table
per dataset, named `<source>__<dataset>` exactly as the version 1 table in `datasets` is:

- the `orders` dataset of the `shop` source is `raw.shop__orders`, `staging.shop__orders` and
  `clean.shop__orders`;
- source and dataset names follow the same rule as always: lowercase letters, digits and single
  underscores, starting with a letter, not ending with `_`;
- the table name is at most 63 bytes, PostgreSQL's limit, which the schema name does not count
  towards.

The rule is written once, in `stage_table` in `src/udp/names.py`. Every stage table is named
through it, and it refuses a name that breaks the rule instead of shortening it.

## What a pipeline records

All of these live in the `platform` schema. A **pipeline run** is called an *execution* in the
tables, because `platform.pipeline_runs` already holds the ingest runs and keeps doing so.

| Table | One row per |
|---|---|
| `pipelines` | pipeline, by source, dataset and name |
| `pipeline_versions` | version of a pipeline's definition. Changing a pipeline adds a version; old versions are never rewritten, so an old run still says what it ran. |
| `pipeline_steps` | step of a version, in order, with its configuration |
| `pipeline_executions` | run of a pipeline version: when, how it was started, its status, rows in and out, and — when it failed — which step and why |
| `step_executions` | step a run carried out: its status, duration, rows in and out, values changed and error |
| `profiles` | profile of a dataset at one stage in one run, optionally taken after a given step |
| `constraint_results` | constraint checked on a dataset at one stage in one run: whether it held, how many rows and values broke it, and whether it is critical |
| `constraint_violations` | value that broke a constraint: the column, the row it is in and the value |
| `lineage` | place the data passed through in one run, in order |

**Every result says which run it came from.** A profile, a constraint result and a lineage row
each belong to exactly one run: an ingest run or a pipeline run, never both and never neither;
the database refuses anything else. Because a result is kept per dataset, per stage and per run,
the same dataset can be profiled before and after a pipeline, and in every later run, and none of
those profiles replaces another.

**Lineage is a chain per run.** An ingest run's chain is the source (where it was read from) and
then its RAW table. A pipeline run's chain is the RAW table, each step in pipeline order, and the
CLEAN table. Following the two back from a CLEAN table reaches the file or table the data came
from. A run that fails part-way has a chain that stops at the step that failed.

## For the code that writes these

`PostgresLoader.stages()` opens one transaction and returns the calls that write and read all of
the above (`src/udp/storage/postgres.py`). Two of them carry the retention rules, so no caller
has to remember them: `start_staging` makes STAGING a fresh copy of RAW, and `finish_execution`
records the end of a run and, in the same transaction, turns STAGING into CLEAN when the run
succeeded or drops it when it failed. STAGING is one table per dataset, so only one pipeline run
of a dataset may be going at a time.
