from collections.abc import Iterator

import pytest
from hypothesis import given
from hypothesis import strategies as st

from udp.connectors.retry import retry


class Transient(Exception):
    pass


class Permanent(Exception):
    pass


OUTCOMES = st.lists(st.sampled_from(["transient", "permanent", "success"]), min_size=1, max_size=7)
WAITS = st.lists(st.floats(0, 10, allow_nan=False), max_size=4)


def _expected_calls(outcomes: list[str], allowed: int) -> int:
    for position, outcome in enumerate(outcomes, start=1):
        if outcome != "transient":
            return min(position, allowed)
    return min(len(outcomes), allowed)


@given(OUTCOMES, WAITS)
def test_calls_and_waits_follow_the_outcomes(outcomes: list[str], waits: list[float]) -> None:
    outcomes = [*outcomes, "success"]
    calls: list[int] = []
    sleeps: list[float] = []

    def call() -> str:
        outcome = outcomes[len(calls)]
        calls.append(1)
        if outcome == "transient":
            raise Transient(len(calls))
        if outcome == "permanent":
            raise Permanent(len(calls))
        return "ok"

    allowed = len(waits) + 1
    expected = _expected_calls(outcomes, allowed)
    last = outcomes[expected - 1]

    if last == "success":
        assert (
            retry(
                call, transient=lambda e: isinstance(e, Transient), waits=waits, sleep=sleeps.append
            )
            == "ok"
        )
    else:
        error = Permanent if last == "permanent" else Transient
        with pytest.raises(error) as raised:
            retry(
                call, transient=lambda e: isinstance(e, Transient), waits=waits, sleep=sleeps.append
            )
        assert raised.value.args == (expected,)

    assert len(calls) == expected
    assert sleeps == waits[: expected - 1]


def test_success_on_exactly_the_last_allowed_attempt() -> None:
    attempts: Iterator[Exception | str] = iter([Transient(), Transient(), "done"])

    def call() -> str:
        outcome = next(attempts)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    assert (
        retry(
            call, transient=lambda e: isinstance(e, Transient), waits=[0, 0], sleep=lambda _: None
        )
        == "done"
    )


def test_no_waits_means_one_attempt() -> None:
    calls = []

    def call() -> None:
        calls.append(1)
        raise Transient()

    with pytest.raises(Transient):
        retry(call, transient=lambda e: True, waits=(), sleep=lambda _: None)
    assert calls == [1]
