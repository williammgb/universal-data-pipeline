"""Platform metrics as Prometheus text, read from the platform tables on every scrape.

Runs are made by the scheduler, the command line and the API, so the numbers come from
platform.pipeline_runs rather than from counters in any one process.
"""

from dataclasses import dataclass
from datetime import datetime

RUN_STATUSES = ("running", "succeeded", "failed", "skipped")
CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"


@dataclass(frozen=True)
class RunTotals:
    """Every run of one dataset that has one status, with its row counts summed."""

    source: str
    dataset: str
    status: str
    runs: int
    rows_extracted: int
    rows_loaded: int
    rows_quarantined: int


@dataclass(frozen=True)
class LastRun:
    """The newest run of one dataset, and when its newest successful run ended."""

    source: str
    dataset: str
    status: str
    started_at: datetime
    ended_at: datetime | None
    succeeded_at: datetime | None


@dataclass(frozen=True)
class QualityFailures:
    """How many checks of one severity failed in a dataset's newest checked run."""

    source: str
    dataset: str
    severity: str
    failed: int


@dataclass(frozen=True)
class MetricsSnapshot:
    totals: tuple[RunTotals, ...]
    last_runs: tuple[LastRun, ...]
    quality: tuple[QualityFailures, ...]
    version: str


type Series = dict[tuple[str, ...], str]


def render(snapshot: MetricsSnapshot) -> str:
    """The snapshot as Prometheus text; the same snapshot in any order gives the same bytes."""
    runs: dict[tuple[str, ...], int] = {}
    extracted: dict[tuple[str, ...], int] = {}
    loaded: dict[tuple[str, ...], int] = {}
    quarantined: dict[tuple[str, ...], int] = {}
    for total in snapshot.totals:
        dataset = (total.source, total.dataset)
        for status in RUN_STATUSES:
            runs.setdefault((*dataset, status), 0)
        key = (*dataset, total.status)
        runs[key] = runs.get(key, 0) + total.runs
        for counts, value in (
            (extracted, total.rows_extracted),
            (loaded, total.rows_loaded),
            (quarantined, total.rows_quarantined),
        ):
            counts[dataset] = counts.get(dataset, 0) + value

    started: Series = {}
    duration: Series = {}
    state: Series = {}
    succeeded: Series = {}
    for last in snapshot.last_runs:
        dataset = (last.source, last.dataset)
        started[dataset] = _seconds(last.started_at.timestamp())
        if last.ended_at is not None:
            duration[dataset] = _seconds((last.ended_at - last.started_at).total_seconds())
        for status in RUN_STATUSES:
            state[(*dataset, status)] = "1" if status == last.status else "0"
        if last.succeeded_at is not None:
            succeeded[dataset] = _seconds(last.succeeded_at.timestamp())

    failures: dict[tuple[str, ...], int] = {}
    for failure in snapshot.quality:
        key = (failure.source, failure.dataset, failure.severity)
        failures[key] = failures.get(key, 0) + failure.failed

    dataset_labels = ("source", "dataset")
    families = [
        _family(
            "udp_build_info",
            "gauge",
            "The running platform's version.",
            ("version",),
            {(snapshot.version,): "1"},
        ),
        _family(
            "udp_last_run_duration_seconds",
            "gauge",
            "How long the newest run of a dataset took, once it has ended.",
            dataset_labels,
            duration,
        ),
        _family(
            "udp_last_run_status",
            "gauge",
            "The newest run's status: 1 for the status it has, 0 for the others.",
            ("source", "dataset", "status"),
            state,
        ),
        _family(
            "udp_last_run_timestamp_seconds",
            "gauge",
            "When the newest run of a dataset started.",
            dataset_labels,
            started,
        ),
        _family(
            "udp_last_success_timestamp_seconds",
            "gauge",
            "When the newest successful run of a dataset ended.",
            dataset_labels,
            succeeded,
        ),
        _family(
            "udp_quality_checks_failed",
            "gauge",
            "Checks that failed in a dataset's newest checked run, by severity.",
            ("source", "dataset", "severity"),
            _text(failures),
        ),
        _family(
            "udp_rows_extracted_total",
            "counter",
            "Rows read from the source, over every run of a dataset.",
            dataset_labels,
            _text(extracted),
        ),
        _family(
            "udp_rows_loaded_total",
            "counter",
            "Rows written to the dataset's table, over every run.",
            dataset_labels,
            _text(loaded),
        ),
        _family(
            "udp_rows_quarantined_total",
            "counter",
            "Rows set aside by quality checks, over every run of a dataset.",
            dataset_labels,
            _text(quarantined),
        ),
        _family(
            "udp_runs_total",
            "counter",
            "Runs of a dataset, by status.",
            ("source", "dataset", "status"),
            _text(runs),
        ),
    ]
    return "".join(line + "\n" for family in families for line in family)


def _family(
    name: str, kind: str, help_text: str, label_names: tuple[str, ...], series: Series
) -> list[str]:
    if not series:
        return []
    lines = [f"# HELP {name} {help_text}", f"# TYPE {name} {kind}"]
    for key in sorted(series):
        labels = ",".join(
            f'{label}="{_escape(value)}"' for label, value in zip(label_names, key, strict=True)
        )
        lines.append(f"{name}{{{labels}}} {series[key]}")
    return lines


def _escape(value: str) -> str:
    """A label value as the text format requires: backslash, quote and newline escaped."""
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _seconds(value: float) -> str:
    return repr(value)


def _text(counts: dict[tuple[str, ...], int]) -> Series:
    return {key: str(value) for key, value in counts.items()}
