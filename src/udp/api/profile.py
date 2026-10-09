"""The dashboard's profile of a dataset table. The profiling itself lives in `udp.profiling`;
this module only names what the API's catalog and its tests use of it."""

from udp.profiling.table import (
    HISTOGRAM_BARS,
    PROFILE_ROW_LIMIT,
    SHOWN_LENGTH,
    best_pattern,
    kind_of,
    profile_table,
    shape_of,
    shape_regex,
)

__all__ = [
    "HISTOGRAM_BARS",
    "PROFILE_ROW_LIMIT",
    "SHOWN_LENGTH",
    "best_pattern",
    "kind_of",
    "profile_table",
    "shape_of",
    "shape_regex",
]
