# Journal

One sentence per step: what was chosen, what was tested, what the reviewers found,
and what was done about it. It is written to be learned from, so plain English
throughout — someone who never saw the code should be able to follow it.

Who writes which lines:

- **Agent lines** — a hook copies them from each agent's report.
  `designer chose: ...`, `[F3] edge-hunter found: ...`, `final-check verdict: ...`
- **Gate lines** — `./run` writes one for every full run of a gate.
  `fast gate passed (9s): 61 passed in 9.12s`
- **Everything else** — the main thread, one sentence each:
  `you decided: ...` for an answer to a question, `built: ...` for what a slice
  delivered, and an answer to every finding: `F3 fixed: ...`, `F3 rejected: ...`
  or `F3 deferred: ...`

A slice cannot be committed as finished while any `[Fn]` finding is unanswered, or
while its section has no `built:` line.

## Spec

- env-doctor found: Docker Desktop is installed but stopped, while a separate Docker engine inside WSL is running and can publish ports that Windows reaches.
- env-doctor found: Python 3.14 is the only interpreter on the machine, uv is on both Windows and WSL, and Node exists only on Windows.
- you decided: build the pipeline core ourselves instead of putting the dlt library underneath it, so the platform owns its tables, state and run records.
- you decided: custom transformations receive and return a Polars DataFrame, because its column types do not change silently the way pandas' do.
- you decided: schedule runs with APScheduler and record them in the platform's own tables, instead of Dagster or Prefect, which would keep a second run history.
- you decided: code and tests run on Windows with uv, and Postgres and the other services run in the WSL Docker engine called through `wsl docker compose`.
- spec written: the remaining ledger items went into the spec as proposed, including config inside the MVP and CI running the fast gate from the skeleton, waiting on your read of the spec.

## Slice 0 — skeleton

- you decided: the spec is approved; push to the existing private GitHub repository williammgb/universal-data-pipeline, and never add Claude as a co-author on commits.
- fast gate failed (10s): Found 1 error.
- fast gate passed (119s): 15 passed, 1 deselected in 89.18s (0:01:29)
- full gate failed (378s): 1 failed, 15 passed in 262.74s (0:04:22)
- full gate passed (148s): 16 passed in 132.14s (0:02:12)
- smoke gate passed (37s): postgres 18.6 (Debian 18.6-1.pgdg13+2)
- full gate passed (15s): 16 passed in 1.60s
- fast gate passed (5s): 15 passed, 1 deselected in 1.76s
- built: the skeleton, a `udp` command that prints its version and connects to Postgres 18 running in the WSL Docker engine, with fast, full and smoke gates in `./run`, a container image, and a GitHub Actions workflow that runs the fast gate on every push.
- built: `./run` keeps one idle WSL session open while a database stack runs, because the first full gate failed when WSL shut its Linux system down about 15 seconds after the last command and stopped Postgres mid-test.
- built: `./run` connects on 127.0.0.1 instead of localhost, because from Windows localhost tries IPv6 first and the connection hung for 130 seconds before falling back, which is where the second full gate's two minutes went.
- built: the CI workflow pins setup-uv to v10.1.0, because the first CI run failed with "unable to find version `v10`" (that action publishes only exact version tags); the second CI run passed.

## Slice 1 — MVP: one CSV source, config-driven
- designer plan: slice 1 — MVP: one CSV source, config-driven
- designer chose: a full refresh COPYs chunks into a temp table, then truncates and refills the real table and marks the run succeeded, all in one transaction, so readers are blocked only for the final insert and slice 3's merge can use the same staging step.
- designer chose: every database write goes through a small loader interface with an in-memory fake, and one contract test runs against both, so fast tests stay honest without a database.
- designer chose: each connector brings its own config models, and the config loader checks the file against them, so a new connector type never touches the pipeline code.
- designer chose: the record hash is SHA-256 of the row's cleaned columns written as JSON with sorted keys, so it depends only on content and a golden test catches a library upgrade that changes it.
- designer chose: the run row goes in as "running" in its own transaction and is marked succeeded inside the load transaction, so a failed load can still be recorded and a successful one can never disagree with the table.
- designer chose: the runner is the only place errors are caught, and it catches every exception, so a bug can't leave a run stuck at "running".
- designer chose: migrations run from Windows through `./run` right after each stack starts, and there is no `udp migrate` command until a container needs one.
- designer ruled out: building a new table and renaming it over the old one, because the table's identity changes on every run and it departs from the spec's temp-table decision.
- designer ruled out: Polars' built-in row hash, because its output isn't stable across versions, and hashing in Postgres, because the result depends on session time-zone settings and can't be checked without a database.
- designer ruled out: reading CSVs with Polars' deprecated batched reader; the connector cuts the newer streaming batches to size itself instead.
- you decided: `./run db` starts its own throwaway database, migrates it, runs the database tests and removes it, like the other gates.
- you decided: column names are cleaned to lowercase with underscores now (so "First Name" becomes `first_name`), not in slice 4.
- you decided: an invalid `source.yaml` only prints an error and exits with code 2, and writes nothing to the run history.
- you decided: the app container sees `sources/` through a read-only mount, not a copy inside the image.
- you decided: the full gate loads a generated 1,000,000-row CSV twice starting in this slice.
- plan-check checked: 24 claims agreed between SPEC.md, PLAN.md, and the slice 1 entry in slices.json, with no textual contradictions found.
- fast gate passed (18s): 21 passed, 1 deselected in 4.96s
- fast gate failed (1s): Found 1 error.
- fast gate passed (20s): 45 passed, 8 deselected in 14.22s
- db gate failed (72s): 1 failed, 7 passed, 45 deselected in 28.90s
- db gate passed (45s): 8 passed, 45 deselected in 11.94s
- fast gate passed (27s): 52 passed, 8 deselected in 20.39s
- fast gate failed (4s): Found 1 error in 1 file (checked 32 source files)
- fast gate passed (20s): 59 passed, 11 deselected in 14.74s
- db gate passed (49s): 10 passed, 60 deselected in 13.60s
- built: `udp run demo_csv` reads `sources/demo_csv/source.yaml`, cleans the column names, loads every CSV row into `datasets.demo_csv__customers` with a run id, load time and row fingerprint, and records each run in `platform.pipeline_runs`, with an in-memory stand-in for the database so the fast gate still needs no services.
- built: the random-data loader test found that 32-bit decimals looked different after reading them back; the stored value was exact, and the test now compares each column in its own type.
- built: stage log lines were plain text instead of JSON because each logger was fixed at import time, before logging was configured; loggers now pick up the configuration each time they log.
- smoke gate passed (34s): {"step": "run", "status": "succeeded", "rows_extracted": 20, "rows_loaded": 20, "event": "run finished", "run_id": "01a09ea8-c1e8-76f4-b99c-2588b606557d", "sour
- full gate passed (163s): 70 passed in 137.43s (0:02:17)
- [F1] plan-drift found: `run`'s `db_gate` filters with `pytest -q -m "db and not scale"`, silently excluding the 1M-row scale test from `./run db` though the plan specified plain `pytest -q -m db` with no mention of a scale exclusion.
- [F2] plan-drift found: `tests/test_runner.py` is missing the plan-required case where an invalid second dataset produces no runs and no tables (validation before extraction).
- final-check verdict: SHIP
- final-check noted: The record hash turns floats into text before the JSON step, which the plan didn't say; the docstring explains it but the journal has no line for it.
- final-check noted: The golden hash test covers only int, text, null and float, so a Polars change to how dates, datetimes or booleans are encoded would go unnoticed until slice 3's merge compares hashes.
- final-check noted: The `scale` marker is registered in `tests/conftest.py` while `db` is in `pyproject.toml`; they belong in one place.
- final-check noted: Two Hypothesis tests skip unwanted examples with a plain `return` instead of `assume()`, so Hypothesis can't warn if most examples get skipped.
- final-check noted: The in-memory loader raises `KeyError` on `fail_run` for an unknown run while Postgres silently updates nothing, and no contract test covers it.
- final-check noted: The full-gate time in `SPEC.md` still says 15s; it is now about 163s with the million-row load.
- [F3] edge-hunter found: path-traversal/arbitrary-file-read via unvalidated `dataset.path` in source.yaml lets a CSV source escape the sources directory (and even hit UNC network paths) — src/udp/connectors/csv.py:25, src/udp/connectors/base.py:17.
- [F4] edge-hunter found: the "no data to load" LoadError guard in replace_table is dead code because extract() and the CSV connector never produce zero chunks — src/udp/storage/postgres.py:24, src/udp/pipeline/extract.py:27. [property]
- [F5] edge-hunter found: header collisions between polars' own duplicate-column renaming and clean_column_names' folding are untested and could silently merge two distinct source columns — src/udp/pipeline/transform.py:16. [property] (guessed, not reproduced end-to-end)
- [F6] edge-hunter found: two concurrent `udp run` calls on the same source/dataset have no lock or uniqueness guard, so they can race and leave a "succeeded" run whose reported rows don't match the table — src/udp/pipeline/runner.py:47, src/udp/storage/postgres.py:22. [property]
- [F7] edge-hunter found: schema inference only samples the first 10,000 rows (INFER_SCHEMA_ROWS), so a later row with an incompatible value hard-fails the whole dataset with no recovery path — src/udp/connectors/csv.py:9, src/udp/storage/loader.py:51.
- fast gate passed (34s): 69 passed, 12 deselected in 26.45s
- F1 rejected: `./run db` leaving out the million-row test is deliberate, because you put that test in the full gate and `./run db` exists to be the quick check of database code.
- F2 fixed: a runner test now shows that a source whose second dataset is invalid raises a config error before any run is recorded or any table is written.
- F3 fixed: a CSV dataset's `path` must now be relative and stay inside its source folder, so absolute paths, drive letters, network paths and `..` are rejected when `source.yaml` is read, with seven cases tested.
- F4 rejected: the "no data to load" check stays, because the loader is a shared interface and cannot assume every future caller goes through the extract step that makes it unreachable today.
- F5 rejected: the column-name property test already proves that any list of headers, including the `id_duplicated_0` names Polars invents, becomes one distinct name per column, so two source columns can never merge (`id`, `ID`, `id_duplicated_0` give `id`, `id_2`, `id_duplicated_0`).
- F6 deferred: stopping two runs of the same dataset overlapping is the Postgres advisory lock planned for slice 6; until then the table lock taken by `TRUNCATE` makes the runs take turns, so the last one to finish wins.
- F7 deferred: a value later in the file that does not fit the type guessed from the first 10,000 rows fails the run with a clear error for now; column type overrides and quarantining bad rows are planned for slices 3 and 5.
- built: after final-check's notes, recording a failure for a run that is not running now raises an error in both the real and the in-memory loader (with a shared test), the golden hash test also pins how dates, times and booleans are encoded, and two property tests skip examples with `assume()` so Hypothesis can warn when too many are skipped.
- smoke gate passed (56s): {"step": "run", "status": "succeeded", "rows_extracted": 20, "rows_loaded": 20, "event": "run finished", "run_id": "01a09eb1-9ed7-701c-b260-a61472e600f1", "sour
- full gate passed (225s): 81 passed in 198.72s (0:03:18)
