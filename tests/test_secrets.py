from pathlib import Path
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

from udp.config.secrets import fill_references, read_environment

NAME = st.from_regex(r"[A-Z_][A-Z0-9_]{0,6}", fullmatch=True)
# No "{" in literal text, so "${" can only appear where a reference is written on purpose.
LITERAL = st.one_of(
    st.text(alphabet=st.characters(blacklist_characters="{", blacklist_categories=["Cs"])),
    st.sampled_from(["$", "$$", "}", "$ ", "a$b", "}}"]),
)
VALUE = st.one_of(
    st.text(min_size=1, max_size=8),
    st.sampled_from(["${OTHER}", "$", "${", "plain", "p@ss:w/rd"]),
)
BAD = st.sampled_from(["${}", "${a}", "${1X}", "${lower_case}", "${A${B}}", "${ X}"])

LOCATIONS = [
    "connection.url",
    "connection.items[0]",
    "connection.items[1].deep[0]",
    "datasets[0].name",
]


def _tree(strings: list[str]) -> dict[str, Any]:
    return {
        "connection": {
            "url": strings[0],
            "port": 5432,
            "flag": True,
            "items": [strings[1], {"deep": [strings[2]]}],
        },
        "datasets": [{"name": strings[3], "nothing": None}],
    }


def _good_string(data: st.DataObject, env: dict[str, str]) -> tuple[str, str]:
    text, expected = "", ""
    for _ in range(data.draw(st.integers(0, 5))):
        use_reference: bool = bool(env) and data.draw(st.booleans())
        if use_reference:
            name = data.draw(st.sampled_from(sorted(env)))
            text += "${" + name + "}"
            expected += env[name]
        else:
            literal = data.draw(LITERAL)
            text += literal
            expected += literal
    return text, expected


@given(st.dictionaries(NAME, VALUE, max_size=4), st.data())
def test_set_references_are_filled_and_everything_else_is_untouched(
    env: dict[str, str], data: st.DataObject
) -> None:
    pairs = [_good_string(data, env) for _ in LOCATIONS]

    filled, problems = fill_references(_tree([p[0] for p in pairs]), env)

    assert problems == []
    assert filled == _tree([p[1] for p in pairs])


@st.composite
def bad_strings(draw: st.DrawFn, env: dict[str, str]) -> tuple[str, int]:
    text, bad = "", 0
    for _ in range(draw(st.integers(0, 4))):
        choice = draw(st.sampled_from(["literal", "set", "unset", "empty", "malformed"]))
        if choice == "literal":
            text += draw(LITERAL)
        elif choice == "set":
            text += "${SET_NAME}"
        elif choice == "unset":
            text += "${" + draw(NAME.filter(lambda n: n not in env)) + "}"
            bad += 1
        elif choice == "empty":
            text += "${EMPTY_NAME}"
            bad += 1
        else:
            text += draw(BAD)
            bad += 1
    if draw(st.booleans()):
        text += "${UNCLOSED"
        bad += 1
    return text, bad


@given(st.data())
def test_every_bad_reference_is_one_problem_at_its_location(data: st.DataObject) -> None:
    env = {"SET_NAME": "value", "EMPTY_NAME": ""}
    drawn = [data.draw(bad_strings(env)) for _ in LOCATIONS]

    _, problems = fill_references(_tree([text for text, _ in drawn]), env)

    assert len(problems) == sum(bad for _, bad in drawn)
    for location, (_, bad) in zip(LOCATIONS, drawn, strict=True):
        assert sum(problem.startswith(f"{location}: ") for problem in problems) == bad


def test_filled_values_are_not_expanded_again() -> None:
    filled, problems = fill_references({"a": "${OUTER}"}, {"OUTER": "${INNER}", "INNER": "no"})

    assert (filled, problems) == ({"a": "${INNER}"}, [])


def test_problem_names_the_variable() -> None:
    _, problems = fill_references({"connection": {"token": "${API_TOKEN}"}}, {})

    assert problems == ["connection.token: environment variable API_TOKEN is not set"]


def test_a_fallback_is_used_only_when_the_variable_is_missing() -> None:
    env = {"SET": "real", "EMPTY": ""}
    data = {
        "a": "${SET:-other}",
        "b": "${MISSING:-other}",
        "c": "${EMPTY:-other}",
        "d": "${MISSING:-}",
        "e": "every ${MISSING:-day} at ${SET:-noon}",
    }

    filled, problems = fill_references(data, env)

    assert problems == []
    assert filled == {"a": "real", "b": "other", "c": "other", "d": "", "e": "every day at real"}


def test_a_reference_inside_a_fallback_is_a_problem_rather_than_half_read() -> None:
    # The reference ends at the first '}', so "${MISSING:-${SET}}" would otherwise fill out to
    # the text "${SET}" — which reads like a reference nobody filled — and report nothing.
    filled, problems = fill_references({"a": "${MISSING:-${SET}}"}, {"SET": "real"})

    assert problems == [
        "a: '${MISSING:-${SET}' puts a reference inside a fallback, which is not read; "
        "give a plain fallback"
    ]
    assert filled == {"a": "}"}


def test_a_fallback_does_not_excuse_a_bad_name() -> None:
    _, problems = fill_references({"a": "${lower:-x}", "b": "${GOOD}"}, {})

    assert problems == [
        "a: '${lower:-x}' is not a valid reference (use uppercase letters, digits and _)",
        "b: environment variable GOOD is not set",
    ]


def test_dotenv_values_are_used_and_the_real_environment_wins(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dotenv = tmp_path / ".env"
    dotenv.write_text("FROM_FILE=file-value\nBOTH=file-value\n", encoding="utf-8")
    monkeypatch.setenv("BOTH", "real-value")
    monkeypatch.delenv("FROM_FILE", raising=False)

    env = read_environment(dotenv)

    assert env["FROM_FILE"] == "file-value"
    assert env["BOTH"] == "real-value"


def test_missing_dotenv_file_is_fine(tmp_path: Path) -> None:
    assert "PATH" in {key.upper() for key in read_environment(tmp_path / "absent.env")}
