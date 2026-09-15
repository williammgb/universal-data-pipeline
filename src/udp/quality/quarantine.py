"""Rows set aside instead of loaded, and the limit above which a run fails."""

from fractions import Fraction

import polars as pl
import structlog

from udp.storage.loader import RunFindings

log = structlog.get_logger(step="quarantine")

QUARANTINE_KEEP = 100_000


def row_records(rows: pl.DataFrame) -> pl.Series:
    """Each row as a JSON object; floats and decimals as text so NaN and digits survive."""
    if rows.width == 0:
        return pl.Series("record", ["{}"] * rows.height, dtype=pl.String)
    values = [
        pl.col(name).cast(pl.String)
        if dtype.is_float() or isinstance(dtype, pl.Decimal)
        else pl.col(name)
        for name, dtype in rows.schema.items()
    ]
    return rows.select(pl.struct(values).struct.json_encode().alias("record")).to_series()


def quarantine(findings: RunFindings, rows: pl.DataFrame, reasons: pl.Series) -> None:
    """Set rows aside with one reason each. Every row is counted; at most QUARANTINE_KEEP
    are kept, so a whole file of bad rows cannot exhaust memory."""
    if rows.height == 0:
        return
    kept = sum(frame.height for frame in findings.quarantine)
    room = max(QUARANTINE_KEEP - kept, 0)
    if room < rows.height and findings.quarantined_rows <= QUARANTINE_KEEP:
        log.warning(
            "quarantine is full; further rows are counted but not kept", kept=QUARANTINE_KEEP
        )
    findings.quarantined_rows += rows.height
    if room:
        findings.quarantine.append(
            pl.DataFrame(
                {
                    "reason": reasons[:room].cast(pl.String),
                    "record": row_records(rows[:room]),
                }
            )
        )


def threshold_exceeded(quarantined: int, rows: int, percent: float) -> bool:
    """True when more than `percent` of `rows` were quarantined, compared exactly."""
    return Fraction(quarantined) * 100 > Fraction(repr(percent)) * rows
