import errno
from pathlib import Path
from typing import Any

import pytest

from udp.config.scaffold import HEADER, find_data_files, write_source
from udp.config.source import load_source
from udp.errors import ConfigError


def _files(root: Path, *names: str) -> Path:
    for name in names:
        path = root / "my_shop" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"")
    return root / "my_shop"


def _loaded(root: Path) -> list[tuple[str, str]]:
    config = load_source(root, "my_shop", {})
    return [(dataset.name, dataset.path) for dataset in config.datasets]


def test_a_csv_folder_is_written_as_a_file_load_source_reads(tmp_path: Path) -> None:
    folder = _files(tmp_path, "orders.csv", "data/customers.csv", "data/summer Sale #1.CSV")

    names = write_source(tmp_path, "my_shop")

    assert names == ["orders", "customers", "summer_sale_1"]
    text = (folder / "source.yaml").read_text(encoding="utf-8")
    assert text.splitlines()[0] == HEADER
    assert "type: csv" in text
    assert _loaded(tmp_path) == [
        ("orders", "orders.csv"),
        ("customers", "data/customers.csv"),
        ("summer_sale_1", "data/summer Sale #1.CSV"),
    ]
    assert all(d.load_mode == "full" for d in load_source(tmp_path, "my_shop", {}).datasets)


def test_an_excel_folder_is_written_as_a_file_load_source_reads(tmp_path: Path) -> None:
    _files(tmp_path, "stock.xlsx", "data/Prices 2024.xls")

    assert write_source(tmp_path, "my_shop") == ["stock", "prices_2024"]
    config = load_source(tmp_path, "my_shop", {})
    assert config.connection.type == "excel"
    assert [(d.name, d.path, d.sheet) for d in config.datasets] == [
        ("stock", "stock.xlsx", None),
        ("prices_2024", "data/Prices 2024.xls", None),
    ]


def test_a_json_folder_is_written_as_a_file_load_source_reads(tmp_path: Path) -> None:
    _files(tmp_path, "events.json", "data/logs.JSONL")

    assert write_source(tmp_path, "my_shop") == ["events", "logs"]
    config = load_source(tmp_path, "my_shop", {})
    assert config.connection.type == "json"
    assert [(d.name, d.path, d.records_path) for d in config.datasets] == [
        ("events", "events.json", None),
        ("logs", "data/logs.JSONL", None),
    ]


def test_missing_folder_is_refused(tmp_path: Path) -> None:
    with pytest.raises(ConfigError) as refused:
        write_source(tmp_path, "my_shop")

    shown = (tmp_path / "my_shop").as_posix()
    assert str(refused.value) == (
        f"No folder {shown}. Make it and put your data files in it (or in {shown}/data/), "
        "then run udp new my_shop again."
    )


def test_existing_source_yaml_is_never_overwritten(tmp_path: Path) -> None:
    folder = _files(tmp_path, "orders.csv")
    (folder / "source.yaml").write_text("mine\n", encoding="utf-8")

    with pytest.raises(ConfigError) as refused:
        write_source(tmp_path, "my_shop")

    assert str(refused.value) == (
        f"{(folder / 'source.yaml').as_posix()} already exists; udp new never overwrites it."
    )
    assert (folder / "source.yaml").read_text(encoding="utf-8") == "mine\n"


def test_bad_source_name_is_refused(tmp_path: Path) -> None:
    (tmp_path / "My Shop").mkdir()
    (tmp_path / "My Shop" / "orders.csv").write_bytes(b"")

    with pytest.raises(ConfigError, match="source name 'My Shop' must start with a lowercase"):
        write_source(tmp_path, "My Shop")

    assert not (tmp_path / "My Shop" / "source.yaml").exists()


def test_no_data_files_is_refused(tmp_path: Path) -> None:
    folder = _files(tmp_path, "notes.txt", "data/readme.md")

    with pytest.raises(ConfigError) as refused:
        write_source(tmp_path, "my_shop")

    shown = folder.as_posix()
    assert str(refused.value) == (
        f"No .csv, .xlsx, .xls, .json or .jsonl file in {shown} or {shown}/data."
    )
    assert not (folder / "source.yaml").exists()


def test_mixed_kinds_are_refused(tmp_path: Path) -> None:
    folder = _files(tmp_path, "orders.csv", "stock.xlsx", "zebra.csv")

    with pytest.raises(ConfigError) as refused:
        write_source(tmp_path, "my_shop")

    assert str(refused.value) == (
        "A source reads one kind of file; found csv (orders.csv) and excel (stock.xlsx). "
        "Put each kind in its own folder under sources/."
    )
    assert not (folder / "source.yaml").exists()


def test_files_that_clean_to_the_same_name_get_numbered(tmp_path: Path) -> None:
    # One in each place: a case-insensitive file system cannot hold both in one folder.
    _files(tmp_path, "Orders.csv", "data/orders.CSV")

    assert write_source(tmp_path, "my_shop") == ["orders", "orders_2"]
    assert _loaded(tmp_path) == [("orders", "Orders.csv"), ("orders_2", "data/orders.CSV")]


def test_a_dataset_name_still_refused_after_cleaning_is_reported(tmp_path: Path) -> None:
    folder = _files(tmp_path, "orders.csv", "my__orders.csv")

    with pytest.raises(ConfigError) as refused:
        write_source(tmp_path, "my_shop")

    assert str(refused.value) == (
        "dataset name 'my__orders' (from my__orders.csv) must not contain '__'"
    )
    assert not (folder / "source.yaml").exists()


def test_a_dataset_whose_table_name_is_too_long_is_reported(tmp_path: Path) -> None:
    _files(tmp_path, f"{'a' * 60}.csv")

    with pytest.raises(ConfigError, match="longer than 63 bytes"):
        write_source(tmp_path, "my_shop")


def test_a_read_only_folder_is_reported(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    folder = _files(tmp_path, "orders.csv")

    def read_only(self: Path, *args: Any, **kwargs: Any) -> Any:
        raise OSError(errno.EROFS, "Read-only file system", str(self))

    monkeypatch.setattr(Path, "open", read_only)
    with pytest.raises(ConfigError) as refused:
        write_source(tmp_path, "my_shop")
    monkeypatch.undo()

    assert str(refused.value) == (
        f"Cannot write {(folder / 'source.yaml').as_posix()}: read-only. Use udp new."
    )
    assert not (folder / "source.yaml").exists()


def test_deeper_files_and_folders_are_ignored(tmp_path: Path) -> None:
    folder = _files(tmp_path, "orders.csv", "data/old/archive.csv", "deep/more.csv")
    (folder / "folder.csv").mkdir()
    (folder / "data" / "inner.csv").mkdir()

    assert find_data_files(folder) == [folder / "orders.csv"]
    assert write_source(tmp_path, "my_shop") == ["orders"]
