"""The rules the V2 records hold before they reach the database: which run a result belongs to,
where a result between steps can be taken, and the order a run's lineage may take."""

import re
import uuid
from datetime import UTC, datetime
from itertools import pairwise
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

from udp.names import Stage
from udp.storage.loader import (
    ConstraintResult,
    LineageNode,
    Profile,
    RunRef,
    StepDefinition,
    StepRun,
    Violation,
    check_chain,
    stage_node,
)

NOW = datetime(2026, 10, 4, tzinfo=UTC)
INGEST = RunRef(ingest_run_id=uuid.uuid4())
EXECUTION = RunRef(execution_id=uuid.uuid4())


def test_a_result_belongs_to_exactly_one_kind_of_run() -> None:
    with pytest.raises(ValueError, match="exactly one run"):
        RunRef()
    with pytest.raises(ValueError, match="exactly one run"):
        RunRef(uuid.uuid4(), uuid.uuid4())
    assert RunRef(ingest_run_id=INGEST.ingest_run_id).execution_id is None


def _profile(**changes: Any) -> Profile:
    values: dict[str, Any] = {
        "source": "shop",
        "dataset": "orders",
        "stage": Stage.RAW,
        "run": INGEST,
        "table_rows": 3,
        "result": {"columns": []},
        "profiled_at": NOW,
    } | changes
    return Profile(**values)


def _constraint(**changes: Any) -> ConstraintResult:
    values: dict[str, Any] = {
        "source": "shop",
        "dataset": "orders",
        "stage": Stage.CLEAN,
        "run": EXECUTION,
        "position": 1,
        "constraint_type": "not_null",
        "columns": ("id",),
        "critical": True,
        "passed": False,
        "failing_rows": 1,
        "failing_values": 1,
        "message": "1 row has no id",
        "settings": {},
        "checked_at": NOW,
    } | changes
    return ConstraintResult(**values)


@pytest.mark.parametrize("make", [_profile, _constraint])
def test_only_a_staging_result_inside_an_execution_follows_a_step(make: Any) -> None:
    assert make(stage=Stage.STAGING, run=EXECUTION, after_step=2).after_step == 2
    assert make(stage=Stage.STAGING, run=EXECUTION).after_step is None
    with pytest.raises(ValueError, match="only a STAGING result"):
        make(stage=Stage.CLEAN, run=EXECUTION, after_step=1)
    with pytest.raises(ValueError, match="only a STAGING result"):
        make(stage=Stage.STAGING, run=INGEST, after_step=1)
    with pytest.raises(ValueError, match="1 or more"):
        make(stage=Stage.STAGING, run=EXECUTION, after_step=0)


@pytest.mark.parametrize("make", [_profile, _constraint])
def test_a_result_names_a_dataset_the_naming_rule_allows(make: Any) -> None:
    with pytest.raises(ValueError, match="dataset name"):
        make(dataset="Orders")
    with pytest.raises(ValueError):
        make(stage="datasets")


def test_counts_cannot_be_negative() -> None:
    assert _profile(table_rows=0).table_rows == 0
    with pytest.raises(ValueError):
        _profile(table_rows=-1)
    with pytest.raises(ValueError):
        _constraint(failing_rows=-1)
    with pytest.raises(ValueError):
        _constraint(failing_values=-1)
    with pytest.raises(ValueError):
        _constraint(position=0)


def test_a_constraint_that_held_has_no_violations() -> None:
    broken = Violation("id", {"row": 3}, None)
    assert _constraint(violations=(broken,)).violations == (broken,)
    with pytest.raises(ValueError, match="no violations"):
        _constraint(passed=True, violations=(broken,))


def test_a_step_ends_and_fails_consistently() -> None:
    assert StepRun(1, "running", NOW).ended_at is None
    assert StepRun(1, "failed", NOW, NOW, error_message="boom").status == "failed"
    with pytest.raises(ValueError, match="end time"):
        StepRun(1, "succeeded", NOW)
    with pytest.raises(ValueError, match="end time"):
        StepRun(1, "running", NOW, NOW)
    with pytest.raises(ValueError, match="error message"):
        StepRun(1, "failed", NOW, NOW)
    with pytest.raises(ValueError, match="error message"):
        StepRun(1, "succeeded", NOW, NOW, error_message="boom")
    with pytest.raises(ValueError, match="1 or more"):
        StepRun(0, "running", NOW)
    with pytest.raises(ValueError, match="needs a type"):
        StepDefinition("")


def test_lineage_nodes_carry_a_step_position_exactly_when_they_are_steps() -> None:
    assert LineageNode("step", "trim", 1).step_position == 1
    with pytest.raises(ValueError):
        LineageNode("step", "trim")
    with pytest.raises(ValueError):
        LineageNode("raw", "raw.shop__orders", 1)
    with pytest.raises(ValueError):
        LineageNode("step", "trim", 0)
    with pytest.raises(ValueError):
        LineageNode("source", "")
    with pytest.raises(ValueError, match="STAGING"):
        stage_node(Stage.STAGING, "shop", "orders")


RAW = stage_node(Stage.RAW, "shop", "orders")
CLEAN = stage_node(Stage.CLEAN, "shop", "orders")

any_node = st.one_of(
    st.just(LineageNode("source", "sources/shop/data/orders.csv")),
    st.just(RAW),
    st.just(LineageNode("raw", "raw.shop__customers")),
    st.integers(1, 4).map(lambda position: LineageNode("step", "trim", position)),
    st.just(CLEAN),
    st.just(LineageNode("clean", "datasets.shop__orders")),
)


def _model_accepts(run: RunRef, nodes: list[LineageNode]) -> bool:
    """The lineage rule written as a pattern over the node kinds, independently of check_chain."""
    letters = "".join({"source": "s", "raw": "r", "step": "t", "clean": "c"}[n.kind] for n in nodes)
    pattern = r"(s(r)?)?" if run.ingest_run_id is not None else r"(rt*c?)?"
    names_right = all(
        node.name == {"raw": RAW, "clean": CLEAN}[node.kind].name
        for node in nodes
        if node.kind in ("raw", "clean")
    )
    positions = [node.step_position for node in nodes if node.step_position is not None]
    ascending = all(a < b for a, b in pairwise(positions))
    return bool(re.fullmatch(pattern, letters)) and names_right and ascending


@given(st.sampled_from([INGEST, EXECUTION]), st.lists(any_node, max_size=6))
def test_a_chain_is_accepted_exactly_when_it_follows_the_lineage_rule(
    run: RunRef, nodes: list[LineageNode]
) -> None:
    try:
        check_chain("shop", "orders", run, nodes)
        accepted = True
    except ValueError:
        accepted = False

    assert accepted == _model_accepts(run, nodes)


def test_the_full_chains_of_both_kinds_of_run_are_accepted() -> None:
    source = LineageNode("source", "sources/shop/data/orders.csv")
    steps = [LineageNode("step", "trim", 1), LineageNode("step", "dedupe", 2)]

    check_chain("shop", "orders", INGEST, [source, RAW])
    check_chain("shop", "orders", EXECUTION, [RAW, *steps, CLEAN])
    check_chain("shop", "orders", EXECUTION, [RAW, CLEAN])
    with pytest.raises(ValueError, match="cannot come first"):
        check_chain("shop", "orders", EXECUTION, [source, RAW])
    with pytest.raises(ValueError, match="pipeline order"):
        check_chain("shop", "orders", EXECUTION, [RAW, *reversed(steps)])
