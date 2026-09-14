import re

from hypothesis import assume, given
from hypothesis import strategies as st

from udp.names import MAX_IDENTIFIER_BYTES, RESERVED_COLUMNS, name_problem, table_name
from udp.pipeline.transform import clean_column_names

VALID = re.compile(r"^[a-z][a-z0-9_]*$")

tricky_header = st.one_of(
    st.text(max_size=80),
    st.sampled_from(
        [
            "",
            "   ",
            "!!!",
            "First Name",
            "first_name",
            "FIRST-NAME",
            "1st",
            "_private",
            "__",
            "Größe",
            "Ünïcödé Ñame",
            "a" * 63 + "b",
            "a" * 63 + "c",
            *RESERVED_COLUMNS,
        ]
    ),
)

clean_name = st.from_regex(r"[a-z]([a-z0-9_]{0,20}[a-z0-9])?", fullmatch=True)


@given(st.lists(tricky_header, max_size=12))
def test_cleaned_column_names_are_valid_unique_identifiers(headers: list[str]) -> None:
    cleaned = clean_column_names(headers)

    assert len(cleaned) == len(headers)
    assert len(set(cleaned)) == len(cleaned)
    for name in cleaned:
        assert VALID.fullmatch(name), name
        assert len(name.encode()) <= MAX_IDENTIFIER_BYTES
        assert name not in RESERVED_COLUMNS


@given(st.lists(tricky_header, max_size=12))
def test_cleaning_twice_equals_cleaning_once(headers: list[str]) -> None:
    once = clean_column_names(headers)

    assert clean_column_names(once) == once


@given(st.lists(clean_name, max_size=12, unique=True))
def test_already_clean_names_come_back_unchanged(names: list[str]) -> None:
    assert clean_column_names(names) == names


def test_headers_become_snake_case() -> None:
    assert clean_column_names(["Customer ID", "First Name", "Größe", "1st", "a", "A"]) == [
        "customer_id",
        "first_name",
        "grosse",
        "col_1st",
        "a",
        "a_2",
    ]


name_like = st.one_of(
    st.from_regex(r"[a-z](_?[a-z0-9]){0,35}", fullmatch=True),
    st.text(alphabet="abcdefghijklmnopqrstuvwxyzABZ0189_-é", max_size=70),
    st.sampled_from(["a_", "_a", "a__b", "A", "1a", "é", "a" * 30, "a" * 31, "a" * 64]),
)


@given(name_like, name_like)
def test_accepted_names_give_a_table_name_that_splits_back(source: str, dataset: str) -> None:
    assume(name_problem(source) is None and name_problem(dataset) is None)
    table = table_name(source, dataset)
    assume(len(table.encode()) <= MAX_IDENTIFIER_BYTES)

    assert VALID.fullmatch(table)
    assert table.split("__", 1) == [source, dataset]


def test_name_rules_reject_the_boundary_cases() -> None:
    assert name_problem("a_") is not None
    assert name_problem("a__b") is not None
    assert name_problem("A") is not None
    assert name_problem("1a") is not None
    assert name_problem("a" * 63) is None
    assert name_problem("a" * 64) is not None
