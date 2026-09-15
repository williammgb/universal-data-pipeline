import json
import math
from datetime import UTC, datetime, timedelta, tzinfo
from decimal import Decimal
from fractions import Fraction
from itertools import pairwise
from pathlib import Path
from typing import Any

import polars as pl
import psycopg
import pytest
from fakes import MemoryLoader
from hypothesis import given
from hypothesis import strategies as st
from polars.testing import assert_frame_equal

from udp.config.quality import Check
from udp.config.source import load_source
from udp.connectors.csv import CsvDataset
from udp.pipeline.runner import RunOutcome, run_source
from udp.quality.checks import check_rows
from udp.quality.quarantine import QUARANTINE_KEEP, quarantine, threshold_exceeded
from udp.settings import Settings
from udp.storage.loader import Loader, RunFindings
from udp.storage.postgres import PostgresLoader

# --- row checks against a model ---------------------------------------------------------------

ROW_CHECKS: list[dict[str, Any]] = [
    {"check": "not_null", "column": "code"},
    {"check": "not_null", "column": "amount"},
    {"check": "accepted_values", "column": "code", "values": ["a", "b"]},
    {"check": "range", "column": "amount", "min": 0, "max": 10},
    {"check": "regex", "column": "code", "pattern": "[a-z]+"},
]


def _checks(specs: list[dict[str, Any]]) -> list[Check]:
    dataset = CsvDataset.model_validate({"name": "d", "path": "d.csv", "checks": specs})
    return list(dataset.checks)


def _fails(spec: dict[str, Any], row: dict[str, Any]) -> bool:
    value = row[spec["column"]]
    if spec["check"] == "not_null":
        return value is None
    if value is None:
        return False
    if spec["check"] == "accepted_values":
        return value not in spec["values"]
    if spec["check"] == "range":
        return math.isnan(value) or not spec["min"] <= value <= spec["max"]
    return not (value.isascii() and value.isalpha() and value.islower())


@st.composite
def checked_chunks(draw: st.DrawFn) -> tuple[list[dict[str, Any]], list[pl.DataFrame]]:
    specs = [
        {**spec, "severity": draw(st.sampled_from(["warn", "error"]))}
        for spec in draw(st.lists(st.sampled_from(ROW_CHECKS), min_size=1, max_size=5))
    ]
    rows = draw(
        st.lists(
            st.fixed_dictionaries(
                {
                    "code": st.sampled_from([None, "a", "b", "c", "", "ab1", "A", "é", "b\n"]),
                    "amount": st.sampled_from([None, 0.0, 10.0, -0.5, 10.5, 5.0, math.nan]),
                }
            ),
            max_size=10,
        )
    )
    frame = pl.DataFrame(rows, schema={"code": pl.String, "amount": pl.Float64})
    cuts = sorted(draw(st.lists(st.integers(0, len(rows)), max_size=3)))
    return specs, [frame[a:b] for a, b in pairwise([0, *cuts, len(rows)])]


@given(checked_chunks())
def test_row_checks_count_failures_and_quarantine_error_level_ones(
    generated: tuple[list[dict[str, Any]], list[pl.DataFrame]],
) -> None:
    specs, chunks = generated
    findings = RunFindings("shop", "orders")
    rows = pl.concat(chunks).to_dicts()

    kept = pl.concat(list(check_rows(iter(chunks), _checks(specs), findings)))

    def rejected(row: dict[str, Any]) -> bool:
        return any(_fails(spec, row) for spec in specs if spec["severity"] == "error")

    expected_kept = [row for row in rows if not rejected(row)]
    assert_frame_equal(kept, pl.DataFrame(expected_kept, schema=kept.schema))
    assert findings.quarantined_rows == len(rows) - len(expected_kept)
    assert [(r.position, r.failing_rows, r.passed) for r in findings.results] == [
        (position, count, count == 0)
        for position, spec in enumerate(specs)
        for count in [sum(_fails(spec, row) for row in rows)]
    ]
    records = [json.loads(record) for f in findings.quarantine for record in f["record"]]
    assert len(records) == findings.quarantined_rows


# --- the quarantine limit ---------------------------------------------------------------------


@given(
    st.integers(0, 2000).flatmap(lambda rows: st.tuples(st.integers(0, rows), st.just(rows))),
    st.one_of(
        st.sampled_from([0.0, 100.0, 0.1, 1.0, 2.5, 33.3]),
        st.floats(0, 100, allow_nan=False),
    ),
)
def test_the_limit_is_exceeded_exactly_when_the_share_is_above_it(
    counts: tuple[int, int], percent: float
) -> None:
    quarantined, rows = counts

    assert threshold_exceeded(quarantined, rows, percent) == (
        Fraction(quarantined * 100) > Fraction(Decimal(repr(percent))) * rows
    )


@pytest.mark.parametrize(
    ("quarantined", "rows", "percent", "exceeded"),
    [
        (1, 100, 1, False),
        (2, 100, 1, True),
        (1, 1000, 0.1, False),
        (2, 1000, 0.1, True),
        (0, 0, 0, False),
        (1, 1, 100, False),
        (1, 1, 0, True),
        (1, 3, 33.3, True),
    ],
)
def test_the_limit_at_its_boundary(
    quarantined: int, rows: int, percent: float, exceeded: bool
) -> None:
    assert threshold_exceeded(quarantined, rows, percent) is exceeded


def test_quarantine_keeps_at_most_its_cap_but_counts_every_row() -> None:
    findings = RunFindings("shop", "orders")
    frame = pl.DataFrame({"id": range(QUARANTINE_KEEP - 1)})

    quarantine(findings, frame, pl.Series(["bad"] * frame.height))
    quarantine(findings, pl.DataFrame({"id": [1, 2, 3]}), pl.Series(["bad"] * 3))

    assert findings.quarantined_rows == QUARANTINE_KEEP + 2
    assert sum(f.height for f in findings.quarantine) == QUARANTINE_KEEP


# --- whole runs ---------------------------------------------------------------------------------

CUSTOMERS_HEADER = "Customer ID,Signup Date,Lifetime Value,Is Active,City\n"


class Shop:
    """A CSV source `shop` with one dataset `customers`, run against one loader."""

    def __init__(self, root: Path, settings: str, loader: Loader | None = None) -> None:
        self.sources = root / "sources"
        self.folder = self.sources / "shop"
        self.folder.mkdir(parents=True)
        self.loader: Any = loader or MemoryLoader()
        self.configure(settings)

    def configure(self, settings: str) -> None:
        (self.folder / "source.yaml").write_text(
            "connection:\n  type: csv\ndatasets:\n  - name: customers\n"
            f"    path: customers.csv\n{settings}",
            encoding="utf-8",
        )

    def write(self, lines: list[str], header: str = CUSTOMERS_HEADER) -> None:
        (self.folder / "customers.csv").write_text(header + "".join(lines), encoding="utf-8")

    def run(self) -> RunOutcome:
        (outcome,) = run_source(
            "shop", load_source(self.sources, "shop", {}), self.sources, self.loader
        )
        return outcome

    def results(self, outcome: RunOutcome) -> list[dict[str, Any]]:
        return [r for r in self.loader.quality_results if r["run_id"] == outcome.run_id]

    def quarantined(self, outcome: RunOutcome) -> list[dict[str, Any]]:
        return [r for r in self.loader.quarantine if r["run_id"] == outcome.run_id]


TYPES = (
    "    columns:\n      customer_id: integer\n      signup_date: date\n"
    "      lifetime_value: decimal(12,2)\n      is_active: boolean\n"
)


def _customers(count: int, bad: int = 0) -> list[str]:
    lines = [
        f"{i},2024-01-{i % 28 + 1:02d},{i}.50,{'true' if i % 2 else 'no'},City\n"
        for i in range(count)
    ]
    for i in range(bad):
        lines[i] = f"{i},2024-01-01,12.345,true,City\n"
    return lines


def test_declared_types_load_and_a_value_that_does_not_fit_is_quarantined(tmp_path: Path) -> None:
    shop = Shop(tmp_path, TYPES + "    quarantine_threshold_percent: 50\n")
    shop.write(_customers(3, bad=1))

    outcome = shop.run()

    assert outcome.status == "succeeded"
    table = shop.loader.tables["shop__customers"]
    assert table.schema["signup_date"] == pl.Date
    assert table.schema["lifetime_value"] == pl.Decimal(12, 2)
    assert table.schema["is_active"] == pl.Boolean
    assert table.select("customer_id", "lifetime_value", "is_active").rows() == [
        (1, Decimal("1.50"), True),
        (2, Decimal("2.50"), False),
    ]
    (row,) = shop.quarantined(outcome)
    assert row["reason"] == "column 'lifetime_value' is not decimal(12,2)"
    assert row["record"]["lifetime_value"] == "12.345"
    assert shop.loader.runs[outcome.run_id]["rows_quarantined"] == 1


def test_the_quarantine_limit_decides_the_run_and_a_failed_run_keeps_its_findings(
    tmp_path: Path,
) -> None:
    shop = Shop(tmp_path, TYPES + "    checks:\n      - check: min_rows\n        rows: 1\n")
    shop.write(_customers(100, bad=1))
    assert shop.run().status == "succeeded"
    before, state = shop.loader.tables["shop__customers"], shop.loader.states[("shop", "customers")]

    shop.write(_customers(100, bad=2))
    failed = shop.run()

    record = shop.loader.runs[failed.run_id]
    assert (record["status"], record["error_class"]) == ("failed", "QualityError")
    assert record["error_message"] == (
        "2 of 100 rows (2.00%) quarantined, above the dataset's limit of 1%"
    )
    assert shop.loader.tables["shop__customers"].equals(before)
    assert shop.loader.states[("shop", "customers")] == state
    assert len(shop.quarantined(failed)) == 2
    assert record["rows_quarantined"] == 2
    assert shop.results(failed) == []


def test_rows_added_by_transform_py_and_quarantined_fail_a_run_that_extracted_none(
    tmp_path: Path,
) -> None:
    shop = Shop(tmp_path, TYPES + "    checks:\n      - check: not_null\n        column: city\n")
    (shop.folder / "transform.py").write_bytes(
        b"import polars as pl\n\n\ndef transform(df, context):\n"
        b"    return pl.DataFrame([dict.fromkeys(df.columns)], schema=df.schema)\n"
    )
    shop.write([])

    failed = shop.run()

    record = shop.loader.runs[failed.run_id]
    assert (record["status"], record["error_class"]) == ("failed", "QualityError")
    assert record["error_message"] == ("1 of 0 rows quarantined, above the dataset's limit of 1%")


NOW = datetime.now(UTC)
CHECK_CASES: list[tuple[str, str, list[str], list[str]]] = [
    (
        "    checks:\n      - check: not_null\n        column: city\n",
        "city",
        ["1,2024-01-01,1.00,true,Delft\n"],
        ["1,2024-01-01,1.00,true,\n"],
    ),
    (
        "    checks:\n      - check: accepted_values\n        column: city\n"
        "        values: [Delft]\n",
        "city",
        ["1,2024-01-01,1.00,true,Delft\n"],
        ["1,2024-01-01,1.00,true,Leiden\n"],
    ),
    (
        "    checks:\n      - check: range\n        column: lifetime_value\n        min: 0\n",
        "lifetime_value",
        ["1,2024-01-01,0.00,true,Delft\n"],
        ["1,2024-01-01,-0.01,true,Delft\n"],
    ),
    (
        "    checks:\n      - check: regex\n        column: city\n        pattern: '[A-Z][a-z]+'\n",
        "city",
        ["1,2024-01-01,1.00,true,Delft\n"],
        ["1,2024-01-01,1.00,true,Delft2\n"],
    ),
    (
        "    checks:\n      - check: unique\n        columns: [customer_id]\n",
        "customer_id",
        ["1,2024-01-01,1.00,true,Delft\n", "2,2024-01-01,1.00,true,Delft\n"],
        ["1,2024-01-01,1.00,true,Delft\n", "1,2024-01-01,1.00,true,Delft\n"],
    ),
    (
        "    checks:\n      - check: min_rows\n        rows: 2\n",
        "",
        ["1,2024-01-01,1.00,true,Delft\n", "2,2024-01-01,1.00,true,Delft\n"],
        ["1,2024-01-01,1.00,true,Delft\n"],
    ),
    (
        "    checks:\n      - check: freshness\n        column: signup_date\n"
        "        max_age: P2D\n",
        "signup_date",
        [f"1,{NOW.date().isoformat()},1.00,true,Delft\n"],
        [f"1,{(NOW - timedelta(days=5)).date().isoformat()},1.00,true,Delft\n"],
    ),
]


@pytest.mark.parametrize(("check", "column", "passing", "failing"), CHECK_CASES)
@pytest.mark.parametrize("severity", ["warn", "error"])
def test_each_check_records_passing_and_failing_results(
    tmp_path: Path, check: str, column: str, passing: list[str], failing: list[str], severity: str
) -> None:
    settings = (
        TYPES
        + check
        + f"        severity: {severity}\n"
        + "    quarantine_threshold_percent: 100\n"
    )
    passed_shop = Shop(tmp_path / "passing", settings)
    passed_shop.write(passing)
    failed_shop = Shop(tmp_path / "failing", settings)
    failed_shop.write(failing)

    passed = passed_shop.run()
    failed = failed_shop.run()

    ((good,), (bad,)) = passed_shop.results(passed), failed_shop.results(failed)
    assert (good["passed"], good["severity"]) == (True, severity)
    assert (bad["passed"], bad["severity"]) == (False, severity)
    assert bad["columns"] == ((column,) if column else ())
    table_check = bad["check_type"] in ("unique", "min_rows", "freshness")
    expected_status = "failed" if table_check and severity == "error" else "succeeded"
    assert (passed.status, failed.status) == ("succeeded", expected_status)
    if expected_status == "failed":
        assert failed_shop.loader.runs[failed.run_id]["error_class"] == "QualityError"
    if not table_check:
        assert bad["failing_rows"] == 1
        quarantined = len(failed_shop.quarantined(failed))
        assert quarantined == (1 if severity == "error" else 0)


@pytest.mark.parametrize(("rows", "status"), [(10, "succeeded"), (16, "failed")])
def test_row_count_change_compares_with_the_last_successful_run(
    tmp_path: Path, rows: int, status: str
) -> None:
    shop = Shop(
        tmp_path, TYPES + "    checks:\n      - check: row_count_change\n        max_percent: 50\n"
    )
    shop.write(_customers(10))
    first = shop.run()
    (result,) = shop.results(first)
    assert (result["passed"], result["table_rows"]) == (True, 10)
    assert "no earlier count" in result["message"]

    shop.write([line.replace("City", "Town") for line in _customers(rows)])
    second = shop.run()

    (result,) = shop.results(second)
    assert (second.status, result["table_rows"]) == (status, rows)


def test_an_unchanged_file_is_not_read_again_but_its_table_is_still_checked(
    tmp_path: Path,
) -> None:
    shop = Shop(
        tmp_path,
        TYPES + "    checks:\n      - check: min_rows\n        rows: 1\n"
        "      - check: not_null\n        column: city\n",
    )
    shop.write(_customers(3))
    assert len(shop.results(shop.run())) == 2

    skipped = shop.run()

    assert (skipped.status, skipped.rows_loaded) == ("succeeded", 0)
    (result,) = shop.results(skipped)
    assert (result["check_type"], result["passed"], result["table_rows"]) == ("min_rows", True, 3)


def test_an_unchanged_file_whose_table_went_stale_fails_its_freshness_check(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    shop = Shop(
        tmp_path,
        TYPES + "    checks:\n      - check: freshness\n        column: signup_date\n"
        "        max_age: P2D\n",
    )
    shop.write([f"1,{NOW.date().isoformat()},1.00,true,Delft\n"])
    assert shop.run().status == "succeeded"
    table, state = shop.loader.tables["shop__customers"], shop.loader.states[("shop", "customers")]

    class TenDaysLater(datetime):
        @classmethod
        def now(cls, tz: tzinfo | None = None) -> datetime:  # type: ignore[override]
            return datetime.now(tz) + timedelta(days=10)

    monkeypatch.setattr("udp.pipeline.runner.datetime", TenDaysLater)
    stale = shop.run()

    record = shop.loader.runs[stale.run_id]
    assert (record["status"], record["error_class"]) == ("failed", "QualityError")
    assert "signup_date" in record["error_message"]
    assert shop.loader.tables["shop__customers"].equals(table)
    assert shop.loader.states[("shop", "customers")] == state
    (result,) = shop.results(stale)
    assert result["passed"] is False


@pytest.mark.db
def test_postgres_run_over_its_limit_keeps_table_and_state_and_records_why(tmp_path: Path) -> None:
    url = Settings().database_url  # type: ignore[call-arg]
    source = f"quality_{tmp_path.name[-8:].lower().replace('-', '_')}"
    sources = tmp_path / "sources"
    (sources / source).mkdir(parents=True)
    (sources / source / "source.yaml").write_text(
        "connection:\n  type: csv\ndatasets:\n"
        f"  - name: customers\n    path: customers.csv\n{TYPES}",
        encoding="utf-8",
    )

    def run(lines: list[str]) -> RunOutcome:
        (sources / source / "customers.csv").write_text(
            CUSTOMERS_HEADER + "".join(lines), encoding="utf-8"
        )
        with PostgresLoader(url) as loader:
            (outcome,) = run_source(source, load_source(sources, source, {}), sources, loader)
        return outcome

    def snapshot(reader: psycopg.Connection) -> tuple[Any, ...]:
        table = reader.execute(
            "SELECT md5(string_agg(t::text, ',' ORDER BY customer_id)) "
            f"FROM datasets.{source}__customers AS t"
        ).fetchone()
        state = reader.execute(
            "SELECT file_sha256, config_sha256, run_id FROM platform.source_state "
            "WHERE source = %s",
            [source],
        ).fetchone()
        return table, state

    assert run(_customers(100)).status == "succeeded"
    with psycopg.connect(url, autocommit=True) as reader:
        before = snapshot(reader)
        failed = run(_customers(100, bad=5))
        assert failed.status == "failed"
        assert snapshot(reader) == before
        counts = reader.execute(
            "SELECT (SELECT count(*) FROM platform.quarantine WHERE run_id = %s), "
            "rows_quarantined, error_class FROM platform.pipeline_runs WHERE run_id = %s",
            [failed.run_id, failed.run_id],
        ).fetchone()
    assert counts == (5, 5, "QualityError")
