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
# The variables ./udp hands on to Docker Compose when they are set, in the order it names them.
PASSED_ON = (
    "POSTGRES_PASSWORD",
    "UDP_API_KEYS",
    "UDP_PG_PORT",
    "UDP_SOURCE_PG_PORT",
    "UDP_MOCK_API_PORT",
    "UDP_API_PORT",
    "UDP_PROM_PORT",
    "DEMO_SCHEDULE",
)


def _udp(
    *arguments: str, dry: bool = True, exported: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    """Run ./udp. With `exported`, those are the only compose variables the shell has: the ones
    this machine happens to export are taken out first."""
    shell = shutil.which("sh")
    assert shell is not None, "./udp needs a POSIX shell on the PATH"
    # Plain text: on GitHub's runner the help came out in colour, and no name matched.
    env = {**os.environ, "UDP_DRY_RUN": "1" if dry else "", "NO_COLOR": "1", "TERM": "dumb"}
    if exported is not None:
        env = {name: value for name, value in env.items() if name not in PASSED_ON} | exported
    return subprocess.run(
        [shell, "udp", *arguments], cwd=ROOT, env=env, capture_output=True, text=True, timeout=120
    )


def _passing(result: subprocess.CompletedProcess[str]) -> str:
    lines = [line for line in result.stderr.splitlines() if line.startswith("passing:")]
    assert len(lines) == 1, result.stderr
    return lines[0]


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
        # `run` is another name for `up`, making the same call.
        (["run"], f"{COMPOSE} up -d --build --wait"),
        (["run", "--demo"], f"{COMPOSE} --profile demo up -d --build --wait"),
        (
            ["down"],
            f"{COMPOSE} --profile app --profile demo --profile monitoring --profile scale down",
        ),
        (
            ["status"],
            f"{COMPOSE} --profile app --profile demo --profile monitoring --profile scale ps",
        ),
        (["logs"], f"{COMPOSE} logs --tail 100"),
        (["logs", "api", "-f"], f"{COMPOSE} logs --tail 100 api -f"),
        (["restart"], f"{COMPOSE} restart api scheduler"),
        # --build: the app image is built before the load, so it never runs stale code.
        (["load", "demo_csv", "--docker"], f"{COMPOSE} run --rm --build app load demo_csv"),
        (
            ["load", "--docker", "a", "b", "--full-refresh"],
            f"{COMPOSE} run --rm --build app load a b --full-refresh",
        ),
    ],
)
def test_container_shortcuts_make_the_compose_call(arguments: list[str], called: str) -> None:
    result = _udp(*arguments)

    assert (result.returncode, result.stdout.strip()) == (0, called), result.stderr


def test_a_fresh_shell_passes_nothing_so_the_env_file_decides() -> None:
    result = _udp("up", exported={})

    # An empty POSTGRES_PASSWORD handed to Compose would win over the one in deploy/.env.
    assert result.returncode == 0, result.stderr
    assert _passing(result) == "passing:"
    assert "POSTGRES_PASSWORD" not in result.stderr
    assert "UDP_API_KEYS" not in result.stderr


def test_an_exported_variable_is_passed_by_name_and_its_value_is_never_printed() -> None:
    result = _udp("up", exported={"POSTGRES_PASSWORD": "a b"})

    assert result.returncode == 0, result.stderr
    assert _passing(result) == "passing: POSTGRES_PASSWORD"
    assert result.stdout.strip() == f"{COMPOSE} up -d --build --wait"
    assert "a b" not in result.stdout + result.stderr


@pytest.mark.parametrize("arguments", [["down"], ["load", "demo_csv", "--docker"]])
def test_a_variable_exported_empty_is_not_passed(arguments: list[str]) -> None:
    result = _udp(*arguments, exported={"UDP_API_KEYS": "", "UDP_API_PORT": "8001"})

    assert result.returncode == 0, result.stderr
    assert _passing(result) == "passing: UDP_API_PORT"


def test_backup_dumps_the_database_into_a_dated_file_under_backups() -> None:
    result = _udp("backup")

    assert result.returncode == 0, result.stderr
    dump = f"{COMPOSE} exec -T postgres pg_dump -U udp -Fc udp > backups/udp-"
    assert re.fullmatch(re.escape(dump) + r"\d{8}-\d{6}\.dump", result.stdout.strip()), (
        result.stdout
    )


def test_open_uses_the_port_this_shell_exports() -> None:
    result = _udp("open", exported={"UDP_API_PORT": "8123"})

    # The opener depends on the machine; the address it is handed does not.
    assert result.returncode == 0, result.stderr
    assert "http://127.0.0.1:8123" in result.stdout


@pytest.mark.parametrize(
    "arguments",
    [
        ["up", "--demo", "--other"],
        ["run", "--demo", "--other"],
        ["down", "-v"],
        ["restart", "x"],
        ["backup", "x"],
    ],
)
def test_container_shortcuts_refuse_what_they_do_not_know(arguments: list[str]) -> None:
    result = _udp(*arguments)

    # Exit 2 and no compose call: `down -v` would delete the data.
    assert (result.returncode, result.stdout) == (2, "")
    assert "usage: ./udp" in result.stderr
