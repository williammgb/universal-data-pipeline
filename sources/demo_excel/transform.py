"""Custom transform for demo_excel, called for every dataset of this source.

`transform(df, context)` gets chunks of up to 100,000 rows after the common transforms (clean
column names, trimmed text, empty text as null) and returns a polars DataFrame. Output columns
must have clean names and the same schema for every chunk; anything else fails the run.
`context` carries `source`, `dataset` and `run_id`.
"""

import polars as pl

from udp.pipeline.custom import TransformContext


def transform(df: pl.DataFrame, context: TransformContext) -> pl.DataFrame:
    if context.dataset != "products":
        return df
    return df.with_columns(stock_value_eur=pl.col("unit_price_eur") * pl.col("stock_qty"))
