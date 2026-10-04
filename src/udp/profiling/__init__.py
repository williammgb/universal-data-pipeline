"""Profiling: what a table holds, at any stage, kept so that two stages can be compared.

- `frame.profile_frame` — the engine: a `StageProfile` of any table given as a Polars frame;
- `stage.profile_stage` — a dataset's RAW, STAGING or CLEAN table, read and profiled;
- `PostgresStages.profile` (in `udp.storage.postgres`) — the same, stored with its run, and
  `PostgresStages.compare_profiles` — two stored profiles before and after, in one call;
- `table.profile_table` — the dashboard's SQL profile of a V1 dataset table.
"""
