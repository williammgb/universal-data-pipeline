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

## Slice 2 — connectors: Excel, database table, REST API

- designer plan: slice 2 — connectors: Excel, database table, REST API
- designer chose: the three new source types sit behind the existing connector interface and bring their own config models, so no pipeline file changes.
- designer chose: `${NAME}` references are filled right after the YAML is read, from the real environment on top of `.env`, and an unset or empty one fails the config before any database is touched.
- designer chose: variable names must be uppercase, because Windows upper-cases environment names and a lowercase reference would work on Linux but not here.
- designer chose: retries sit in one helper used only where repeating is safe (connecting to a database, requesting one API page), 3 retries waiting 1, 2 and 4 seconds, and HTTP 429, 500, 502, 503 and 504 count as temporary.
- designer chose: the REST connector reads every page before typing its columns, so a field that is empty on the first page cannot break the rule that every chunk has the same columns; the cost is that an API dataset must fit in memory.
- designer chose: the database connector takes column types from the table definition and streams rows, every integer becomes bigint so nothing overflows, and a type it doesn't know fails the run naming the column.
- designer chose: page-numbered and offset APIs stop at the first empty page, a repeated cursor or link fails loudly, and a hard cap on requests guarantees every extraction ends.
- designer chose: a next link pointing to a different host is refused, so the API token is never sent to another host.
- designer chose: the mock API is a FastAPI app in `tests/`, used in-process by the fast tests and served in a container by the full gate and smoke.
- designer chose: each gate stack gets its own explicit ports for the source Postgres and mock API (full 55442/55452, smoke 55443/55453), and the db gate does not start them.
- designer ruled out: typing each API page separately, because a field null on page one gives page two a different schema; storing every API value as text, because numbers and booleans would be lost.
- designer ruled out: reading spreadsheets chunk by chunk, because the reader reparses the whole sheet each time and column types could differ between chunks.
- designer ruled out: Polars' `read_database`, because it guesses types per batch; Postgres `COPY` for extraction, because it would only work for Postgres.
- you decided: python-dotenv is added as a direct dependency to read `.env` files.
- you decided: xlsxwriter is added for tests only, to write spreadsheets for the Excel tests and the demo file.
- you decided: nested API objects and lists are stored as JSON text in one column.
- you decided: the smoke gate runs all four demo sources, in the container and on Windows.
- you decided: only the demo sources are built for now, no real sources yet.
- you decided: page-numbered and offset APIs are finished at the first empty page.
- you decided: exact decimal columns from source databases are stored as text for now, because floating-point numbers can drift in the last digits and in sums.
- you decided: each column can later be given a declared storage type (text, integer, decimal, float, boolean, date, timestamp, JSON) that is applied when the data is read; it is built in slice 5 with the planned column overrides, which is where decimals become exact numeric columns.
- plan-check checked: 14 claims agreed (connector/dataset base-class shape, ExtractRequest fields, CONNECTORS registry type, load_source error format, loader's supported dtypes, retry count matching "3x increasing wait", REST pagination/auth modes matching spec's five/three modes, fastexcel already a pinned dependency, the four `./run` gate names, the `src/udp/pipeline/` no-touch constraint, slice-2 slices.jso
- fast gate failed (4s): Found 6 errors in 4 files (checked 44 source files)
- fast gate passed (52s): 122 passed, 12 deselected, 7 warnings in 45.52s
- fast gate failed (4s): Found 1 error in 1 file (checked 45 source files)
- fast gate passed (44s): 122 passed, 18 deselected, 7 warnings in 37.50s
- db gate passed (49s): 11 passed, 129 deselected, 2 warnings in 13.23s
- smoke gate passed (127s): {"step": "run", "status": "succeeded", "rows_extracted": 2000, "rows_loaded": 2000, "event": "run finished", "source": "demo_api", "dataset": "items_linked", "r
- full gate passed (222s): 140 passed, 8 warnings in 200.69s (0:03:20)
- built: Excel, database-table and REST API connectors behind the existing connector interface, `${NAME}` secrets filled from the environment and `.env`, retries for dropped connections and temporary API errors, a source Postgres and a fake API in the full and smoke stacks, and three demo sources; no file under `src/udp/pipeline/` changed.
- built: the password-leak test first took 130 seconds because connecting to a closed port from Windows hangs until the operating system gives up, so it now fakes a connection error that contains the password, which tests the scrubbing more directly.
- built: the random-spreadsheet and random-SQLite tests run a quarter of the Hypothesis examples, because each example writes a file; that kept the fast gate near 40 seconds.
- built: the API typing test first failed because a page-numbered API stops at the first empty page, as you decided, so the test now pages by cursor, which carries on past an empty page.
- [F8] plan-drift found: `tests/test_demo_sources.py:90` pins the pipeline-untouched check to `git diff --quiet 1f6145e -- src/udp/pipeline` instead of the plan's `git diff --quiet HEAD -- src/udp/pipeline` (Steps 7 and Done-means), so once a later slice touches `src/udp/pipeline` this test will fail forever rather than only flagging the current slice's changes.
- [F9] edge-hunter found: a DB password containing an unescaped special character (@, :, /, %) can be silently misparsed by `make_url`, bypassing config validation and potentially leaking part of the real secret in error text since redaction only knows the truncated password — property: the real secret should never survive in error/log text for any password.
- [F10] edge-hunter found: `create_engine(url)` in the database connector runs outside the try/except that turns SQLAlchemy errors into `ExtractError`, so an unsupported or uninstalled DB driver crashes with a raw traceback instead of a clean error.
- [F11] edge-hunter found: a REST API integer field outside int64 range, mixed with normal ints in the same column, silently collapses the whole column to text, and this boundary is never exercised by the existing property tests — property: column typing should be well-defined at the numeric boundary.
- [F12] edge-hunter found: REST API pagination parameters silently overwrite a user-supplied `params` entry with the same key, with no warning that the user's value was dropped.
- edge-hunter: found (guess, unverified): the Excel connector's narrow exception catch may let some calamine failure modes (e.g. encrypted files) escape as raw exceptions instead of `ExtractError`.
- final-check verdict: FIX FIRST — 3 blocking
- [F13] final-check found: The fast-suite test that diffs `src/udp/pipeline` against commit 1f6145e will fail in CI's shallow checkout and is bound to fail once slice 3 changes the pipeline, so move that check into the ledger evidence.
- [F14] final-check found: The retry helper logs the raw error text on every retry, so a database password the connector scrubs from its final error can still reach stdout; log only the error type and extend the leak test to a retry.
- [F15] final-check found: The compose file says source-postgres and mock-api start only when named, but services without a profile start on any plain `compose up`, so put them behind a profile that `./run` enables.
- final-check noted: Slice 2's checks in slices.json are still unticked; each tick must name the gate command and its output.
- final-check noted: The unset-secret CLI test fails for any developer whose `.env` sets DEMO_API_TOKEN, because the command reads `.env` from the working directory.
- final-check noted: The database retry test patches `time.sleep` with no effect, because retry's sleep default is bound at definition.
- final-check noted: The Excel property test compares the connector against the same `read_excel` call it makes, so it proves the chunking but not the types.
- final-check noted: `source.py` has the only production `assert` in `src`, used just to narrow a type for mypy.
- fast gate passed (28s): 128 passed, 18 deselected, 7 warnings in 23.79s
- F8, F13 fixed: the fast test that compared `src/udp/pipeline` with commit 1f6145e is deleted, because CI's shallow checkout doesn't have that commit and slices 3 and 4 have to change the pipeline; the "pipeline untouched" check is now a command recorded as this slice's evidence.
- F9 fixed: a database URL whose password has an unencoded `@` is rejected when `source.yaml` is read, with a message that doesn't repeat the password, and error text now hides both the plain and the percent-encoded form of the password.
- F10 fixed: creating the database engine now happens inside the error handling, so a missing database driver is a clean extract error, with a test.
- F11 fixed: an API integer beyond 64 bits turns its column into text so every digit is kept, which is now a stated rule with its own test, and the property test generates values at the 64-bit boundary.
- F12 fixed: a dataset whose `params` also sets the pagination parameter (such as `page`) is rejected when `source.yaml` is read, instead of the value being silently replaced.
- F14 fixed: the retry log line now records only the error's class, not its text, and the password test now runs through one retry and checks the log output too.
- F15 fixed: the source Postgres and mock API sit behind a `demo` compose profile that only `./run` enables, so a plain `docker compose up` never starts them.
- built: after final-check's notes, the unset-secret test runs from an empty folder so a developer's `.env` can't break it, a patch that did nothing was removed, the database connector takes a `sleep` argument like the API connector, the one `assert` in `src` became a typed cast, and the Excel reader wraps every error from the spreadsheet library.
- smoke gate passed (56s): {"step": "run", "status": "succeeded", "rows_extracted": 2000, "rows_loaded": 2000, "event": "run finished", "run_id": "01a0a16c-16ac-73cc-ad2b-6b3a72ad5a9e", "
- full gate passed (140s): 146 passed, 8 warnings in 126.81s (0:02:06)

## Slice 3 — incremental loading and column changes
- designer plan: slice 3 — incremental loading and column changes
- designer chose: each dataset's saved state lives in one row of `platform.source_state`, written in the same transaction as the table and the run's success, so a killed run leaves both untouched.
- designer chose: the "only new rows" filter is a pipeline stage after transform, so every source type behaves the same; the database connector also puts the condition in its query to read less, but the pipeline filter decides.
- designer chose: append loads rows strictly above the saved watermark, while merge also re-reads rows equal to it and writes none that are unchanged, so a late row with the same timestamp is not lost.
- designer chose: a merge writes a row only when its row fingerprint changed, so running again with the same data loads 0 rows and leaves every row identical.
- designer chose: a file is skipped when its path, content hash and dataset config all match the last load, so editing the config (for example the sheet name) still reloads.
- designer chose: a dataset's first run and `--full-refresh` are the same operation, and changing the load mode, watermark or key without `--full-refresh` fails the run instead of guessing.
- designer chose: a column that is empty in a batch fits any existing column type, so an incremental batch with no values in an integer column is not treated as a type change.
- designer chose: the killed-run check really kills a separate process in the middle of a load, instead of only raising an error inside the test.
- designer ruled out: working the watermark out from the table's highest value, because rows set aside by slice 5's quarantine would be read again on every run.
- designer ruled out: letting each connector skip unchanged data itself, because the rule would be written four times and a new connector could get it wrong.
- [F16] plan-check found: the plan never schedules or mentions the mutmut mutation-testing pass that SPEC.md's Verification block requires once after slice 3.
- [F17] plan-check found: the plan's "Interface assumptions" claim REST params are sent on every page request, but the actual code drops `dataset.params` after the first page under `next_link` pagination.
- F16 fixed: the plan gains step 8, the spec's milestone mutation run on `pipeline/incremental.py` inside WSL, proven to start before it is relied on and run in the background outside every gate.
- F17 fixed: the plan's interface note now says `next_link` pagination sends `params` only on the first request, which is why the API revision test uses cursor pagination.
- you decided: a watermark column must hold whole numbers, dates or timestamps; text is not allowed.
- you decided: `--full-refresh` deletes the dataset's table and builds it again.
- you decided: `platform.schema_versions` is created now, with one row each time a dataset's columns change.
- you decided: when a merge run sees the same primary key twice, the copy with the newest watermark wins, and the last one read on a tie.
- you decided: a row with an empty watermark fails the run with a count of such rows, instead of being skipped.
- fast gate failed (1s): Found 2 errors in 2 files (checked 49 source files)
- fast gate passed (29s): 173 passed, 27 deselected, 8 warnings in 25.60s
- db gate passed (39s): 19 passed, 181 deselected, 2 warnings in 16.39s
- built: every dataset can now load as `full`, `append` or `merge`; each dataset's watermark, file fingerprint and settings live in `platform.source_state`, saved in the same transaction as the load, so an unchanged file is skipped, a merge rewrites only rows whose fingerprint changed, and `udp run SOURCE --full-refresh` rebuilds the table.
- built: new source columns are added to the table and recorded in `platform.schema_versions`, columns the source stops sending are kept, and a column that changes type fails the run until `--full-refresh`.
- built: a test starts a real run in a separate process, kills it in the middle of loading, and shows the table, its columns and the saved state are exactly as before.
- built: a date written into a CSV arrives as text, so CSV date and timestamp columns can't be watermarks until slice 5's declared column types; spreadsheets and databases carry real dates, and a spreadsheet date watermark is tested.
- built: mutmut 3.8.0 was proven to start inside WSL before the milestone mutation run relies on it.
- smoke gate passed (39s): {"step": "run", "status": "succeeded", "rows_extracted": 2000, "rows_loaded": 2000, "event": "run finished", "source": "demo_api", "run_id": "01a0a18c-9b29-708f
- full gate passed (246s): 200 passed, 9 warnings in 229.97s (0:03:49)
- final-check verdict: SHIP
- final-check noted: The slice 3 ledger checks are all still false with no evidence; each needs its command and output before the slice-done commit.
- final-check noted: Plan step 7 says to record gate durations in .work/SPEC.md, and that file is not in the diff.
- final-check noted: The empty-watermark error counts only the first bad chunk, so on large files it undercounts the rows the user asked to see counted.
- final-check noted: The Watermark type is defined in two modules, and load.py and runner.py carry checks that exist only for mypy.
- final-check noted: The merge model property re-merges with the same run id, so rows == 0 and the end-to-end test are what catch a rewritten _run_id.
- final-check noted: A killed run stays "running" in pipeline_runs forever, and nothing cleans it up yet.
- final-check noted: .work/CONTEXT.md still describes the pipeline as unchanged since slice 1 and must be regenerated after the commit.
- [F18] edge-hunter found: header-only CSV (zero data rows) breaks a brand-new incremental/merge dataset because polars infers every column as text with no data rows, and the watermark-type check rejects text, so an empty source file fails a run that should just load zero rows.
- [F19] edge-hunter found: Postgres and the in-memory test double each pick their own tie-break order for two rows sharing the same primary key and the same watermark value in one run, and nothing proves the two backends agree on the winner.
- [F20] edge-hunter found: a column that is empty (all-null) in one run is allowed to silently take on a different real type in a later run because the "fits any type" rule for all-null batches doesn't check history across runs, which could let real schema drift through undetected.
- [F21] edge-hunter found: the saved-watermark-vs-just-loaded comparison in the runner mixes watermark values whose Python types may not be safely order-comparable in some same-"kind" corner cases; flagged as a guess since it wasn't exercised.
- [F22] edge-hunter found: retrying a dataset whose very first run failed, after changing its load settings, isn't covered by a test even though the code path (no saved state yet) looks like it should just treat it as a fresh dataset; flagged as a guess since it wasn't exercised.
- [F23] plan-drift found: `.work/SPEC.md` was never updated with slice 3's gate durations even though the plan's step 7 and file list both require it, and the durations already exist in `.work/JOURNAL.md`.
- plan-drift checked: 30 files (25 modified + 5 new) match the plan's design, interfaces, test scenarios and end-to-end numeric targets exactly, and no assertions, bounds, or skips were weakened anywhere in the diff.
- fast gate failed (5s): 27 deselected, 2 warnings, 1 error in 1.88s
- fast gate passed (28s): 177 passed, 27 deselected, 8 warnings in 25.60s
- designer plan: slice 4 — common and custom transformations
- designer chose: trim text first, then turn empty text into null, because the other order is not idempotent
- designer chose: the custom transform is called once per chunk of up to 100,000 rows, so memory stays within the target
- designer chose: `transform.py` is read once, hashed, and those same bytes are compiled and run, so the stored hash always matches the code that ran and nothing is written into the read-only sources folder
- designer chose: the user module is loaded fresh on every dataset run and only when data is actually read
- designer chose: output with unclean column names, reserved names, unstorable types or a changing schema fails the run, naming `transform.py` and the column
- designer chose: one new error type, `TransformError`, for every transform failure, with the user's traceback kept
- designer chose: the transform's hash is folded into the dataset's config hash, so editing it reloads an unchanged file
- designer chose: custom transforms run before the new-rows filter, so a transform can create the watermark or key column
- designer ruled out: running user code in a separate process, because it copies every chunk twice and nobody asked for isolation
- designer ruled out: silently re-cleaning column names the transform returns, because it would rename columns the user named on purpose
- designer ruled out: calling the transform once on the whole dataset, because memory would grow with the dataset
- db gate passed (37s): 19 passed, 185 deselected, 2 warnings in 16.11s
- F18 fixed: an append or merge dataset whose file has a header and no rows now loads 0 rows instead of failing; its columns are passed on as typeless so they fit an existing table, and no state is saved, so the next file with data loads as a fresh first run (three tests, including an existing table and state staying untouched).
- F19 rejected: the random merge test already runs both the in-memory and the Postgres loader against the same model with duplicate keys and equal watermarks in one batch, so the two loaders are proven to pick the same winner.
- F20 rejected: a column empty in every row of its first load is stored as text, and later real values of another type then fail as a type change, so it is not silent; letting an empty batch fit an existing column type is the decided rule, so an incremental batch with no values doesn't fail.
- F21 rejected: the watermark type saved in the state is checked against the new data before any comparison, and each saved type maps to exactly one Python type (whole number, date, or a timestamp with or without time zone), so values that can't be compared never meet.
- F22 fixed: a test now fails a dataset's first run, changes its watermark setting, and shows the next run loads normally as a fresh dataset.
- F23 fixed: slice 3's measured gate times are now recorded in `.work/SPEC.md`.
- built: after final-check's note, a run with empty watermarks or keys now counts them across every chunk before failing, so the error gives the file's total, with a test spanning three chunks.
- smoke gate passed (43s): {"step": "run", "status": "succeeded", "rows_extracted": 2000, "rows_loaded": 2000, "event": "run finished", "run_id": "01a0a197-3f30-7623-b94f-288560e564df", "
- full gate passed (215s): 204 passed, 9 warnings in 201.99s (0:03:21)
