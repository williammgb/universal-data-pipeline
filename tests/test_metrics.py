from collections import Counter
from datetime import UTC, datetime, timedelta
from random import Random

from hypothesis import given
from hypothesis import strategies as st
from prometheus_client.parser import text_string_to_metric_families

from udp.api.metrics import LastRun, MetricsSnapshot, QualityFailures, RunTotals, render

# Written out rather than imported, so a status missing from the renderer is a failure here.
RUN_STATUSES = ("running", "succeeded", "failed", "skipped")
BIGGEST = 2**63 - 1

# Config forbids these characters in names today; the renderer must survive them anyway, so a
# later rule change can never produce a scrape Prometheus rejects.
NAMES = st.one_of(
    st.sampled_from(["customers", 'say "hi"', "back\\slash", "two\nlines", "tab\there", "müşteri"]),
    st.text(max_size=6),
)
COUNTS = st.one_of(st.integers(0, 1000), st.sampled_from([0, BIGGEST]))
MOMENTS = st.datetimes(
    min_value=datetime(2000, 1, 1), max_value=datetime(2100, 1, 1), timezones=st.just(UTC)
)


@st.composite
def snapshots(draw: st.DrawFn) -> MetricsSnapshot:
    # Few sources and few names, so the same dataset name often sits under two sources.
    sources = draw(st.lists(NAMES, unique=True, max_size=3))
    names = draw(st.lists(NAMES, unique=True, max_size=3))
    datasets = (
        draw(
            st.lists(
                st.tuples(st.sampled_from(sources), st.sampled_from(names)), unique=True, max_size=6
            )
        )
        if sources and names
        else []
    )
    totals: list[RunTotals] = []
    last_runs: list[LastRun] = []
    quality: list[QualityFailures] = []
    for source, dataset in datasets:
        # An empty status list is a dataset with no runs at all.
        statuses = draw(st.lists(st.sampled_from(RUN_STATUSES), unique=True, max_size=4))
        for status in statuses:
            totals.append(
                RunTotals(source, dataset, status, draw(st.integers(1, 50)), *draw(_rows()))
            )
        if not statuses:
            continue
        started = draw(MOMENTS)
        took = draw(st.one_of(st.none(), st.just(0.0), st.floats(0, 1e6)))
        ended = None if took is None else started + timedelta(seconds=took)
        succeeded = draw(st.one_of(st.none(), MOMENTS)) if "succeeded" in statuses else None
        last_runs.append(
            LastRun(source, dataset, draw(st.sampled_from(statuses)), started, ended, succeeded)
        )
        for severity in draw(st.lists(st.sampled_from(["warn", "error"]), unique=True)):
            quality.append(QualityFailures(source, dataset, severity, draw(st.integers(0, 50))))
    version = draw(st.sampled_from(["0.1.0", 'odd "version"\n']))
    return MetricsSnapshot(tuple(totals), tuple(last_runs), tuple(quality), version)


def _rows() -> st.SearchStrategy[tuple[int, int, int]]:
    return st.tuples(COUNTS, COUNTS, COUNTS)


def _samples(text: str) -> dict[str, list[tuple[dict[str, str], float]]]:
    found: dict[str, list[tuple[dict[str, str], float]]] = {}
    for family in text_string_to_metric_families(text):
        assert family.documentation, family.name
        assert family.type in {"counter", "gauge"}, family.name
        for sample in family.samples:
            found.setdefault(sample.name, []).append((sample.labels, sample.value))
    return found


@given(snapshots())
def test_prometheus_reads_every_series_once_with_its_names_intact(
    snapshot: MetricsSnapshot,
) -> None:
    text = render(snapshot)

    samples = _samples(text)

    keys = Counter(
        (name, tuple(sorted(labels.items())))
        for name, series in samples.items()
        for labels, _ in series
    )
    assert [key for key, seen in keys.items() if seen > 1] == []
    assert {labels["version"] for labels, _ in samples["udp_build_info"]} == {snapshot.version}
    with_runs = {(total.source, total.dataset) for total in snapshot.totals}
    assert {
        (labels["source"], labels["dataset"]) for labels, _ in samples.get("udp_runs_total", [])
    } == with_runs


@given(snapshots())
def test_run_counts_add_up_and_each_dataset_has_exactly_one_last_status(
    snapshot: MetricsSnapshot,
) -> None:
    samples = _samples(render(snapshot))

    def per_dataset(name: str) -> dict[tuple[str, str], float]:
        added: dict[tuple[str, str], float] = {}
        for labels, value in samples.get(name, []):
            key = (labels["source"], labels["dataset"])
            added[key] = added.get(key, 0) + value
        return added

    # Runs still going are a gauge; everything counted is a run that has ended, so no counter
    # can drop when a run finishes.
    expected: dict[str, dict[tuple[str, str], int]] = {
        "udp_runs_running": {},
        "udp_runs_total": {},
        "udp_rows_extracted_total": {},
        "udp_rows_loaded_total": {},
        "udp_rows_quarantined_total": {},
    }
    for total in snapshot.totals:
        key = (total.source, total.dataset)
        ended = total.status != "running"
        for name, value in (
            ("udp_runs_running", 0 if ended else total.runs),
            ("udp_runs_total", total.runs if ended else 0),
            ("udp_rows_extracted_total", total.rows_extracted if ended else 0),
            ("udp_rows_loaded_total", total.rows_loaded if ended else 0),
            ("udp_rows_quarantined_total", total.rows_quarantined if ended else 0),
        ):
            expected[name][key] = expected[name].get(key, 0) + value
    for name, counts in expected.items():
        assert per_dataset(name) == counts, name
    assert {labels["status"] for labels, _ in samples.get("udp_runs_total", [])} <= {
        "succeeded",
        "failed",
        "skipped",
    }

    states: dict[tuple[str, str], dict[str, float]] = {}
    for labels, shown in samples.get("udp_last_run_status", []):
        states.setdefault((labels["source"], labels["dataset"]), {})[labels["status"]] = shown
    assert states == {
        (last.source, last.dataset): {
            status: 1 if status == last.status else 0 for status in RUN_STATUSES
        }
        for last in snapshot.last_runs
    }
    assert per_dataset("udp_last_run_duration_seconds") == {
        (last.source, last.dataset): (last.ended_at - last.started_at).total_seconds()
        for last in snapshot.last_runs
        if last.ended_at is not None
    }


@given(snapshots(), st.randoms(use_true_random=False))
def test_the_same_snapshot_in_any_order_renders_the_same_bytes(
    snapshot: MetricsSnapshot, shuffler: Random
) -> None:
    shuffled = MetricsSnapshot(
        tuple(shuffler.sample(snapshot.totals, len(snapshot.totals))),
        tuple(shuffler.sample(snapshot.last_runs, len(snapshot.last_runs))),
        tuple(shuffler.sample(snapshot.quality, len(snapshot.quality))),
        snapshot.version,
    )

    assert render(snapshot) == render(snapshot)
    assert render(shuffled) == render(snapshot)


def test_a_platform_with_no_runs_reports_only_its_version() -> None:
    assert render(MetricsSnapshot((), (), (), "0.1.0")) == (
        "# HELP udp_build_info The running platform's version.\n"
        "# TYPE udp_build_info gauge\n"
        'udp_build_info{version="0.1.0"} 1\n'
    )


def test_a_known_snapshot_renders_the_expected_text() -> None:
    started = datetime(2026, 9, 17, 8, 0, tzinfo=UTC)
    snapshot = MetricsSnapshot(
        totals=(
            RunTotals("demo_csv", "customers", "succeeded", 2, 40, 40, 1),
            RunTotals("demo_csv", "customers", "failed", 1, 20, 0, 0),
            RunTotals("demo_csv", "customers", "running", 1, 7, 7, 7),
        ),
        last_runs=(
            LastRun(
                "demo_csv",
                "customers",
                "failed",
                started,
                started + timedelta(seconds=1.5),
                started - timedelta(hours=1),
            ),
        ),
        quality=(QualityFailures("demo_csv", "customers", "warn", 1),),
        version="0.1.0",
    )

    assert render(snapshot) == (
        "# HELP udp_build_info The running platform's version.\n"
        "# TYPE udp_build_info gauge\n"
        'udp_build_info{version="0.1.0"} 1\n'
        "# HELP udp_last_run_duration_seconds How long the newest run of a dataset took, once it "
        "has ended.\n"
        "# TYPE udp_last_run_duration_seconds gauge\n"
        'udp_last_run_duration_seconds{source="demo_csv",dataset="customers"} 1.5\n'
        "# HELP udp_last_run_status The newest run's status: 1 for the status it has, 0 for the "
        "others.\n"
        "# TYPE udp_last_run_status gauge\n"
        'udp_last_run_status{source="demo_csv",dataset="customers",status="failed"} 1\n'
        'udp_last_run_status{source="demo_csv",dataset="customers",status="running"} 0\n'
        'udp_last_run_status{source="demo_csv",dataset="customers",status="skipped"} 0\n'
        'udp_last_run_status{source="demo_csv",dataset="customers",status="succeeded"} 0\n'
        "# HELP udp_last_run_timestamp_seconds When the newest run of a dataset started.\n"
        "# TYPE udp_last_run_timestamp_seconds gauge\n"
        'udp_last_run_timestamp_seconds{source="demo_csv",dataset="customers"} 1789632000.0\n'
        "# HELP udp_last_success_timestamp_seconds When the newest successful run of a dataset "
        "ended.\n"
        "# TYPE udp_last_success_timestamp_seconds gauge\n"
        'udp_last_success_timestamp_seconds{source="demo_csv",dataset="customers"} 1789628400.0\n'
        "# HELP udp_quality_checks_failed Checks that failed in a dataset's newest checked run, "
        "by severity.\n"
        "# TYPE udp_quality_checks_failed gauge\n"
        'udp_quality_checks_failed{source="demo_csv",dataset="customers",severity="warn"} 1\n'
        "# HELP udp_rows_extracted_total Rows read from the source, over every ended run of a "
        "dataset.\n"
        "# TYPE udp_rows_extracted_total counter\n"
        'udp_rows_extracted_total{source="demo_csv",dataset="customers"} 60\n'
        "# HELP udp_rows_loaded_total Rows written to the dataset's table, over every ended run.\n"
        "# TYPE udp_rows_loaded_total counter\n"
        'udp_rows_loaded_total{source="demo_csv",dataset="customers"} 40\n'
        "# HELP udp_rows_quarantined_total Rows set aside by quality checks, over every ended run "
        "of a dataset.\n"
        "# TYPE udp_rows_quarantined_total counter\n"
        'udp_rows_quarantined_total{source="demo_csv",dataset="customers"} 1\n'
        "# HELP udp_runs_running Runs of a dataset still going.\n"
        "# TYPE udp_runs_running gauge\n"
        'udp_runs_running{source="demo_csv",dataset="customers"} 1\n'
        "# HELP udp_runs_total Ended runs of a dataset, by how they ended.\n"
        "# TYPE udp_runs_total counter\n"
        'udp_runs_total{source="demo_csv",dataset="customers",status="failed"} 1\n'
        'udp_runs_total{source="demo_csv",dataset="customers",status="skipped"} 0\n'
        'udp_runs_total{source="demo_csv",dataset="customers",status="succeeded"} 2\n'
    )
