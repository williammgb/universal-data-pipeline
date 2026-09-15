import tempfile
from pathlib import Path

import pytest
from hypothesis import assume, given
from hypothesis import strategies as st

from udp.config.source import load_source
from udp.connectors.csv import CsvDataset
from udp.errors import ConfigError
from udp.names import name_problem

VALID_DATASET = "  - name: customers\n    path: data/customers.csv\n"


def _write_source(sources_dir: Path, name: str, text: str) -> None:
    folder = sources_dir / name
    folder.mkdir(parents=True)
    (folder / "source.yaml").write_text(text, encoding="utf-8")


API_SOURCE = (
    "connection:\n  type: rest_api\n  base_url: ${API_URL}\n"
    "  auth:\n    type: bearer\n    token: ${API_TOKEN}\n"
    "datasets:\n  - name: items\n    endpoint: /items\n"
)


def test_references_are_filled_from_the_environment(tmp_path: Path) -> None:
    _write_source(tmp_path / "sources", "api", API_SOURCE)

    config = load_source(
        tmp_path / "sources", "api", {"API_URL": "http://api.test", "API_TOKEN": "secret"}
    )

    assert str(config.connection.base_url) == "http://api.test/"
    assert config.connection.auth.token.get_secret_value() == "secret"


def test_every_missing_variable_is_reported_with_its_field(tmp_path: Path) -> None:
    _write_source(tmp_path / "sources", "api", API_SOURCE)

    with pytest.raises(ConfigError) as raised:
        load_source(tmp_path / "sources", "api", {})

    message = str(raised.value)
    assert "connection.base_url: environment variable API_URL is not set" in message
    assert "connection.auth.token: environment variable API_TOKEN is not set" in message


def test_lowercase_reference_is_rejected(tmp_path: Path) -> None:
    _write_source(tmp_path / "sources", "api", API_SOURCE.replace("${API_TOKEN}", "${api_token}"))

    with pytest.raises(ConfigError, match=r"connection\.auth\.token: '\$\{api_token\}'"):
        load_source(tmp_path / "sources", "api", {"API_URL": "http://api.test", "api_token": "x"})


def test_demo_source_loads() -> None:
    config = load_source(Path("sources"), "demo_csv")

    assert config.connection.type == "csv"
    assert [dataset.name for dataset in config.datasets] == ["customers"]
    assert isinstance(config.datasets[0], CsvDataset)
    assert config.datasets[0].path == "data/customers.csv"


@pytest.mark.parametrize(
    ("settings", "field", "problem"),
    [
        ("    load_mode: append\n", "watermark", "required when load_mode is append"),
        ("    load_mode: merge\n    watermark: updated_at\n", "primary_key", "required"),
        ("    watermark: updated_at\n", "watermark", "only used by append and merge"),
        ("    primary_key: [id]\n", "primary_key", "only used by merge"),
        (
            "    load_mode: append\n    watermark: id\n    primary_key: [id]\n",
            "primary_key",
            "only used by merge",
        ),
        (
            "    load_mode: merge\n    watermark: id\n    primary_key: []\n",
            "primary_key",
            "required",
        ),
        (
            "    load_mode: merge\n    watermark: id\n    primary_key: [id, id]\n",
            "primary_key",
            "more than once",
        ),
        ("    load_mode: append\n    watermark: _loaded_at\n", "watermark", "platform column"),
        (
            "    load_mode: merge\n    watermark: id\n    primary_key: [_run_id]\n",
            "primary_key",
            "platform column",
        ),
    ],
)
def test_load_mode_settings_are_checked(
    tmp_path: Path, settings: str, field: str, problem: str
) -> None:
    sources_dir = tmp_path / "sources"
    _write_source(
        sources_dir, "shop", f"connection:\n  type: csv\ndatasets:\n{VALID_DATASET}{settings}"
    )

    with pytest.raises(ConfigError) as raised:
        load_source(sources_dir, "shop")

    assert f"datasets[0].{field}: " in str(raised.value)
    assert problem in str(raised.value)


def test_merge_settings_load() -> None:
    dataset = CsvDataset.model_validate(
        {
            "name": "orders",
            "path": "orders.csv",
            "load_mode": "merge",
            "watermark": "updated_at",
            "primary_key": ["id", "line"],
        }
    )

    assert (dataset.load_mode, dataset.watermark, dataset.primary_key) == (
        "merge",
        "updated_at",
        ["id", "line"],
    )


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("connection:\n  type: csv\ndatasets:\n  - name: customers\n", "datasets[0].path"),
        (
            "connection:\n  type: csv\ndatasets:\n  - name: customers\n    pth: data.csv\n",
            "datasets[0].pth",
        ),
        (
            f"connection:\n  type: csv\ndatasets:\n{VALID_DATASET}    load_mode: upsert\n",
            "datasets[0].load_mode",
        ),
        (f"connection:\n  type: parquet\ndatasets:\n{VALID_DATASET}", "connection.type"),
        (f"datasets:\n{VALID_DATASET}", "connection.type"),
        (
            "connection:\n  type: csv\ndatasets:\n  - name: Customers\n    path: data.csv\n",
            "datasets[0].name",
        ),
        (
            f"connection:\n  type: csv\ndatasets:\n{VALID_DATASET}{VALID_DATASET}",
            "datasets[1].name",
        ),
        (
            f"connection:\n  type: csv\ndatasets:\n  - name: {'d' * 60}\n    path: data.csv\n",
            "datasets[0].name",
        ),
        ("connection:\n  type: csv\ndatasets: []\n", "datasets"),
        ("connection:\n  type: csv\n    bad: indent\ndatasets:\n", "line 3"),
        (f"connection:\n  type: csv\n  host: x\ndatasets:\n{VALID_DATASET}", "connection.host"),
    ],
)
def test_invalid_source_names_file_and_field(tmp_path: Path, text: str, expected: str) -> None:
    sources_dir = tmp_path / "sources"
    _write_source(sources_dir, "shop", text)

    with pytest.raises(ConfigError) as raised:
        load_source(sources_dir, "shop")

    message = str(raised.value)
    assert "sources/shop/source.yaml" in message
    assert expected in message


MERGE_ON_UPDATED = "    load_mode: merge\n    watermark: updated\n    primary_key: [id]\n"


@pytest.mark.parametrize(
    ("settings", "field", "problem"),
    [
        ("    columns:\n      Signup Date: date\n", "datasets[0].columns", "'signup_date'"),
        ("    columns:\n      _run_id: text\n", "datasets[0].columns", "platform column"),
        ("    columns:\n      amount: decimal\n", "datasets[0].columns.amount", "decimal(12,2)"),
        ("    columns:\n      amount: decimal(40,2)\n", "datasets[0].columns.amount", "38"),
        ("    columns:\n      amount: decimal(2,3)\n", "datasets[0].columns.amount", "scale"),
        ("    columns:\n      amount: money\n", "datasets[0].columns.amount", "unknown type"),
        (
            f"{MERGE_ON_UPDATED}    columns:\n      updated: text\n",
            "datasets[0].columns",
            "must be integer, date or timestamp",
        ),
        (
            "    checks:\n      - check: regex\n        column: city\n        pattern: '('\n",
            "datasets[0].checks[0].regex.pattern",
            "not a valid regular expression",
        ),
        (
            "    checks:\n      - check: range\n        column: amount\n",
            "datasets[0].checks[0].range",
            "needs min, max or both",
        ),
        (
            "    checks:\n      - check: range\n        column: amount\n        min: 5\n"
            "        max: 1\n",
            "datasets[0].checks[0].range",
            "min must not be larger than max",
        ),
        ("    checks:\n      - check: foo\n", "datasets[0].checks[0]", "foo"),
        (
            "    checks:\n      - check: not_null\n        column: id\n        severity: fatal\n",
            "datasets[0].checks[0].not_null.severity",
            "'warn' or 'error'",
        ),
        (
            "    checks:\n      - check: accepted_values\n        column: id\n        values: []\n",
            "datasets[0].checks[0].accepted_values.values",
            "at least 1",
        ),
        (
            "    checks:\n      - check: freshness\n        column: at\n        max_age: P0D\n",
            "datasets[0].checks[0].freshness.max_age",
            "longer than zero",
        ),
        (
            "    quarantine_threshold_percent: 101\n",
            "datasets[0].quarantine_threshold_percent",
            "100",
        ),
    ],
)
def test_invalid_columns_and_checks_name_file_and_field(
    tmp_path: Path, settings: str, field: str, problem: str
) -> None:
    sources_dir = tmp_path / "sources"
    text = f"connection:\n  type: csv\ndatasets:\n{VALID_DATASET}{settings}"
    _write_source(sources_dir, "shop", text)

    with pytest.raises(ConfigError) as raised:
        load_source(sources_dir, "shop")

    message = str(raised.value)
    assert message.startswith(f"{(sources_dir / 'shop' / 'source.yaml').as_posix()}: {field}: ")
    assert problem in message


def test_columns_and_checks_load() -> None:
    dataset = CsvDataset.model_validate(
        {
            "name": "customers",
            "path": "data.csv",
            "columns": {"amount": "decimal( 12 , 2 )", "signup_date": "date"},
            "checks": [
                {"check": "unique", "columns": ["id"]},
                {
                    "check": "freshness",
                    "column": "signup_date",
                    "max_age": "P1D",
                    "severity": "warn",
                },
            ],
            "quarantine_threshold_percent": 0.5,
        }
    )

    assert dataset.columns == {"amount": "decimal(12,2)", "signup_date": "date"}
    assert [check.check for check in dataset.checks] == ["unique", "freshness"]
    assert dataset.checks[1].severity == "warn"


@pytest.mark.parametrize(
    "path",
    [
        "../other/data.csv",
        "data/../../data.csv",
        "/etc/passwd",
        "C:/Windows/win.ini",
        "C:data.csv",
        "\\\\fileserver\\share\\data.csv",
        "..\\data.csv",
    ],
)
def test_dataset_path_must_stay_inside_the_source_folder(tmp_path: Path, path: str) -> None:
    sources_dir = tmp_path / "sources"
    text = f"connection:\n  type: csv\ndatasets:\n  - name: orders\n    path: '{path}'\n"
    _write_source(sources_dir, "shop", text)

    with pytest.raises(ConfigError, match=r"datasets\[0\]\.path"):
        load_source(sources_dir, "shop")


def test_missing_source_file_names_the_file(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match=r"sources/nothing/source\.yaml: file not found"):
        load_source(tmp_path / "sources", "nothing")


dataset_name = st.one_of(
    st.from_regex(r"[a-z](_?[a-z0-9]){0,35}", fullmatch=True),
    st.text(alphabet="abcxyzAB019_-é", max_size=40),
    st.sampled_from(["a_", "_a", "a__b", "A", "1a"]),
)


@given(dataset_name)
def test_every_rejected_dataset_name_raises_naming_its_field(name: str) -> None:
    assume(name_problem(name) is not None or len(f"shop__{name}") > 63)
    with tempfile.TemporaryDirectory() as directory:
        sources_dir = Path(directory) / "sources"
        text = f"connection:\n  type: csv\ndatasets:\n  - name: '{name}'\n    path: data.csv\n"
        _write_source(sources_dir, "shop", text)

        with pytest.raises(ConfigError, match=r"datasets\[0\]\.name"):
            load_source(sources_dir, "shop")
