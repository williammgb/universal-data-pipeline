"""The ./udp script at the project root: the CLI without `uv run --locked`, plus the container
shortcuts, proven here through their dry run so no Docker is needed."""

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
COMPOSE = "docker compose -f deploy/compose.yaml"


def _udp(*arguments: str, dry: bool = True) -> subprocess.CompletedProcess[str]:
    shell = shutil.which("sh")
    assert shell is not None, "./udp needs a POSIX shell on the PATH"
    # Plain text: on GitHub's runner the help came out in colour, and no name matched.
    env = {**os.environ, "UDP_DRY_RUN": "1" if dry else "", "NO_COLOR": "1", "TERM": "dumb"}
    return subprocess.run(
        [shell, "udp", *arguments], cwd=ROOT, env=env, capture_output=True, text=True, timeout=120
    )


def test_help_lists_the_new_command_names_and_not_the_old_ones() -> None:
    result = _udp("--help", dry=False)

    assert result.returncode == 0, result.stderr
    # A command's row starts with the box's border, one space and its name.
    shown = re.sub(r"\x1b\[[0-9;]*m", "", result.stdout)
    listed = set(re.findall(r"^\S (\w+)\s{2,}\S", shown, re.MULTILINE))
    assert {"load", "update", "schedule", "api", "doctor", "openapi"} <= listed, shown
    assert not {"run", "migrate"} & listed


@pytest.mark.parametrize(
    ("arguments", "called"),
    [
        (["up"], f"{COMPOSE} up -d --build --wait"),
        (["up", "--demo"], f"{COMPOSE} --profile demo up -d --build --wait"),
        (
            ["down"],
            f"{COMPOSE} --profile app --profile demo --profile monitoring --profile scale down",
        ),
        (["load", "demo_csv", "--docker"], f"{COMPOSE} run --rm app load demo_csv"),
        (
            ["load", "--docker", "a", "b", "--full-refresh"],
            f"{COMPOSE} run --rm app load a b --full-refresh",
        ),
    ],
)
def test_container_shortcuts_make_the_compose_call(arguments: list[str], called: str) -> None:
    result = _udp(*arguments)

    assert (result.returncode, result.stdout.strip()) == (0, called), result.stderr


@pytest.mark.parametrize("arguments", [["up", "--demo", "--other"], ["down", "-v"]])
def test_container_shortcuts_refuse_what_they_do_not_know(arguments: list[str]) -> None:
    result = _udp(*arguments)

    # Exit 2 and no compose call: `down -v` would delete the data.
    assert (result.returncode, result.stdout) == (2, "")
    assert "usage: ./udp" in result.stderr
