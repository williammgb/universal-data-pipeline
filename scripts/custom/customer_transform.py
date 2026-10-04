"""Example `python` pipeline step: tidy customer emails and add each customer's email domain.

Declared in a pipeline as:

    - type: python
      script: scripts/custom/customer_transform.py

`transform(df)` gets the whole dataset as the steps before it left it and returns a polars
DataFrame. `docs/custom-python-steps.md` has the full contract.
"""

import polars as pl


def transform(df: pl.DataFrame) -> pl.DataFrame:
    email = pl.col("email").str.strip_chars().str.to_lowercase()
    tidy = df.with_columns(email=email)
    print(f"{tidy['email'].null_count()} customers have no email")
    return tidy.with_columns(email_domain=pl.col("email").str.split("@").list.last())
