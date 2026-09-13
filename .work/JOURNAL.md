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
