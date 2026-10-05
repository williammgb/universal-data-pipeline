# Constraints

A constraint says what a dataset **should** look like: this column always has a value, that one
is never below zero, these emails are all different. Checking a constraint never changes the
data. A value that breaks one is written down as a data-quality problem — which column, which
constraint, which row and which value — and the data stays exactly as it was. Fixing what a
constraint finds is a job for a pipeline's steps, not for the constraint.

## Writing them

Constraints go in the dataset's part of `source.yaml`, under `constraints:`. They are separate
from any pipeline, so a dataset can have constraints and no pipeline at all.

```yaml
datasets:
  - name: customers
    path: customers.csv
    constraints:
      - constraint: datatype        # every value can be read as this type
        column: lifetime_value
        type: decimal(10,2)         # any type `columns:` accepts
      - constraint: not_null        # every row has a value
        column: email
        critical: true
      - constraint: min             # no value below this
        column: lifetime_value
        value: 0
      - constraint: max             # no value above this
        column: signup_date
        value: 2030-01-01
      - constraint: unique          # no two rows share these columns
        columns: [email]
      - constraint: allowed_values  # every value is one of these
        column: status
        values: [new, paid, shipped]
      - constraint: pattern         # every value matches this whole regular expression
        column: postcode
        pattern: "[0-9]{4}[A-Z]{2}"
```

- **A missing value breaks only `not_null`.** Every other constraint skips empty cells, and
  `unique` never counts two rows as the same when one of them has an empty cell in the columns.
- **`critical: true`** marks a constraint a pipeline can be told to stop on when it does not
  hold. It is `false` when left out.
- **`min` and `max` compare the column as it is stored**: a number with a number, a date with a
  date. A column stored as text cannot be compared with a number; declare its type under
  `columns:` first.
- **A wrong constraint is refused when `source.yaml` is read**, with the file and the exact field:
  `sources/shop/source.yaml: datasets[0].constraints[2].min.value: Field required`. A
  constraint naming a column the table does not have is refused when it is checked.

## What a check of them records

Each constraint gets one result per check, stored against the dataset, the stage it was checked
at (RAW, STAGING or CLEAN) and the run (see `stages-and-pipelines.md`):

| Field | What it holds |
|---|---|
| held | whether every row kept to it |
| failing rows | how many rows broke it |
| failing values | how many values broke it — the same as the rows, except for `unique` over several columns, where each row counts one value per column |
| violations | the first 100 values that broke it, each with its column, its row and the value itself; the counts above always cover every row |
| critical | whether it was marked critical |

A row is named by the dataset's `primary_key` when it has one, otherwise by its `_record_hash`
(the fingerprint of the row every stage table keeps).

**Large tables.** A check reads every row, never a sample, so its counts are exact. It reads
50,000 rows at a time and only the columns its constraints need, and leaves `unique` to the
database to count, so a large table is never held in memory at once.

## How they differ from V1's quality checks

V1's `checks:` stay as they are and keep working the same way. The two are built on the same
code — a constraint's rule is the check's rule — but they do different jobs:

| | V1 `checks:` | V2 `constraints:` |
|---|---|---|
| When | during a load, on the rows coming in and then the loaded table | at any stage, whenever a pipeline or a person asks |
| What a failure does | an `error` check sends the row to quarantine or fails the run; `warn` only counts | nothing to the data; the value is recorded as a problem |
| Results | a pass or fail and a count per check, on the load's run | counts of rows and values per constraint, and the values themselves |
| Strength | `severity: warn` or `error` | `critical: true` or not |

The same rule under both names:

| Constraint | Check |
|---|---|
| `not_null` | `not_null` |
| `min`, `max` | `range` with `min` or `max` |
| `allowed_values` | `accepted_values` |
| `pattern` | `regex` |
| `unique` | `unique` |
| `datatype` | none — it is the rule a load uses to quarantine a value that does not fit its declared type under `columns:` |

V1 also has `min_rows`, `freshness` and `row_count_change`, which are about the table rather than
its values and have no constraint.

## For the code

`src/udp/config/constraints.py` defines the constraints and turns each into its V1 check;
`src/udp/quality/constraints.py` checks them: `check_frame` against any Polars frame, and
`check_stage` against a stage table. `PostgresStages.check_constraints` checks a stage and stores
the results; `PostgresStages.read_constraint_results` reads them back by dataset, stage and run.
