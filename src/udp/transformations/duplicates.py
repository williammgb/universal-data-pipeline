"""Duplicate rows: keep the first of each group of rows that are the same."""

import polars as pl
from pydantic import Field

from udp.transformations.base import Applied, StepContext, Transformation, require_columns
from udp.transformations.registry import register


@register
class DropDuplicates(Transformation):
    """Drop every row that repeats an earlier one: in all columns, or in `columns` when named.
    The first of each group is kept, and the rows keep their order."""

    type = "drop_duplicates"
    columns: list[str] | None = Field(default=None, min_length=1)

    def check(self, schema: pl.Schema) -> pl.Schema:
        require_columns("columns", self.columns or [], schema)
        return schema

    def apply(self, frame: pl.DataFrame, context: StepContext) -> Applied:
        kept = frame.unique(subset=self.columns, keep="first", maintain_order=True)
        return Applied(kept, values_changed=0)
