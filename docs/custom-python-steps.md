# Custom Python steps

A pipeline step of type `python` runs a script of your own in its place in the pipeline, between
the standard transformations:

```yaml
steps:
  - type: fill_missing
    columns: [email]
    method: value
    value: ""
  - type: python
    script: scripts/custom/customer_transform.py
    timeout: 120          # seconds; 900 when left out
  - type: validate
```

`script` is a path relative to the project folder — the folder `sources/` is in. The pipeline
file around the steps, and running it, are in `stages-and-pipelines.md`; the step itself, its
record and its failures are what this page describes.

> **A script is trusted code.** It runs with the pipeline's own permissions, environment
> variables, files and network, exactly like the per-source `transform.py`. Nothing sandboxes
> it: anyone who can write a script a pipeline names can do anything the pipeline can,
> including reading its database password. Treat write access to `scripts/` like write access
> to the repository.

## The contract

The script defines one function:

```python
def transform(df: pl.DataFrame) -> pl.DataFrame: ...
```

- **Name:** `transform`. A script without it fails its step with `defines no function
  'transform'`.
- **It receives** the whole dataset as the steps before it left it, as one polars DataFrame —
  every row at once, not in chunks.
- **It returns** a polars DataFrame. Anything else — a `dict`, a `LazyFrame`, `None` — fails the
  step with `transform returned <type>, expected a polars DataFrame`.
- **Rows** may be added, removed or reordered.
- **Columns** may be added, removed, renamed or given a new type, as long as every column it
  returns
  - has a clean name: lowercase letters, digits and `_`, starting with a letter (`email_domain`,
    not `Email Domain` — the error names the clean spelling to use);
  - is not one of the platform's own columns, `_run_id`, `_loaded_at` and `_record_hash`;
  - has a type that can be stored: signed integers, floats, decimals, text, booleans, dates,
    times, datetimes, and JSON columns passed through unchanged. Lists, other structs and
    unsigned integers cannot be stored — `rank()` returns a `UInt32`, so cast it, for example
    with `.cast(pl.Int64)`.
- **Changing columns affects later steps.** The steps after a script are checked again against
  the columns it really returned, before each runs. A later step naming a column the script
  removed or renamed fails with `column '<name>' is not in the data`.
- **It must be deterministic.** The same input must give the same output: no random numbers
  without a fixed seed, no current time, no reading a file or a web page that changes. The
  platform runs the same code over the same input; it cannot make a script that is not
  deterministic give the same answer twice.

The function is called once per run. Anything at the top of the file runs once before it, so
imports and constants belong there.

## The example

`scripts/custom/customer_transform.py`, run by the test suite:

```python
import polars as pl


def transform(df: pl.DataFrame) -> pl.DataFrame:
    email = pl.col("email").str.strip_chars().str.to_lowercase()
    tidy = df.with_columns(email=email)
    print(f"{tidy['email'].null_count()} customers have no email")
    return tidy.with_columns(email_domain=pl.col("email").str.split("@").list.last())
```

Over `email = [" Ann@Shop.NL ", null, "bob@x.com"]` it returns `email = ["ann@shop.nl", null,
"bob@x.com"]` and a new column `email_domain = ["shop.nl", null, "x.com"]`, and the step's
output reads `1 customers have no email`.

## How it runs

Every run reads the script's file, takes the SHA-256 hash of its bytes and runs those same
bytes, so the recorded hash says exactly which code ran. Editing the script between two runs
gives the second run a different hash. The file is compiled under its own path and never
imported: nothing is cached and no `__pycache__` folder appears next to it.

The script runs in a separate Python process — the pipeline's own interpreter, working folder
and environment. That process is why a script that never returns can be stopped: when the
step's `timeout` passes, the process is killed and the step fails with `took longer than <n>s
and was stopped`. A script's packages must already be installed in the platform's environment;
nothing is installed for it.

Everything the script writes to standard output or standard error — `print`, `logging`,
warnings — is kept with the step as its **output**, the last 100,000 characters of it when it
writes more. NUL characters are dropped, because the database cannot store them.

## When it fails

A script that raises, cannot be compiled, returns the wrong thing or returns columns that cannot
be stored fails **its own step**:

- the step is marked failed, with the error as a person reads it and the script line it came
  from — `scripts/custom/customer_transform.py: transform failed: KeyError: 'id' (line 7)`;
- what the script printed before failing is kept;
- the dataset is left as the step before it left it;
- the steps before it keep their results.

Whether the pipeline then stops or carries on with the next step is the pipeline's
configuration (`on_failure: stop`, the default, or `continue`); with `continue` the next step
gets the dataset as it was before the failed step.

A script missing from disk is refused when the pipeline is loaded (`script: <path> is not a
file`); one deleted after that fails its step with `the script is not there`.

## What each run records

Per step, in `platform.step_executions`: status, start and end time (so its duration), rows in,
rows out, values changed, the error, and for a python step the script's hash
(`script_sha256`), its output and the line it failed on (`error_line`).

Values changed compares the frame before and after the script position by position: the cells
that differ in the columns the script kept with the same type, when the row count stayed the
same. A script that only reorders rows therefore counts every cell that moved as changed. When
rows were added or removed it is 0, because which row became which is not known.

## The per-source `transform.py`

`sources/<source>/transform.py` keeps working exactly as before, and is a different thing: it
runs during ingestion, on every dataset of its source, chunk by chunk, as
`transform(df, context)`. A python step runs in a pipeline, on one dataset, over the whole
frame, as `transform(df)`. Both are read, hashed and compiled the same way, and both carry the
same trust.
