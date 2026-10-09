# Performance

One figure, measured, not a gate: how long the largest messy demo source takes from its file to
a clean table when there is a real amount of it.

## The messy CSV at a million rows

The 20 rows of `sources/messy_csv/data/orders.csv` repeated to 1,000,000, every problem in them
included, each copy's order ids moved on so copies are not duplicates of each other. Loaded
with `udp load`, then prepared by `pipelines/messy_csv_orders.yaml` with `udp pipeline run`,
into the PostgreSQL of the full gate's stack.

| | |
|---|---|
| Rows in | 1,000,000 |
| Rows out | 900,000 — the word in a quantity and the date that does not exist drop one row each per 20 |
| `udp load` (file to RAW) | 21.1 s |
| `udp pipeline run` (RAW to CLEAN) | 33.9 s |
| The seven steps together | 0.5 s — normalize_values 0.10, validate 0.22, convert_type 0.05 and 0.06, outliers 0.03, fill_missing 0.02 and 0.04 |

Almost all of a pipeline run is moving the data, not changing it: reading RAW out of PostgreSQL,
the two profiles, checking the constraints and writing CLEAN. The steps themselves run in
memory and take half a second.

Measured on 2026-10-05 on the development machine — Windows 11, PostgreSQL in Docker inside
WSL — so read it as an order of size, not a promise.

## The command that produces it

```
UDP_PERF_ROWS=1000000 ./run full -s -k pipeline_performance
```

It starts the full gate's stack, runs only
`tests/test_pipeline_end_to_end.py::test_pipeline_performance_on_the_messy_csv_scaled_up` and
prints a line starting `performance:` with the figures above. Without `UDP_PERF_ROWS` the test
is skipped, so the gates never run it. A filtered `./run full` does not count as the full gate.
